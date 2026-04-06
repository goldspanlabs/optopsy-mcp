//! Handlers for forward testing (paper trading).
//!
//! - `start` — initialize a new forward test session
//! - `step` — process new bars and persist state
//! - `status` — view equity curve, drift detection

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use chrono::Utc;
use serde_json::Value;

use crate::data::cache::CachedStore;
use crate::data::forward_test_store::{
    ForwardTestSnapshot, ForwardTestTrade, SqliteForwardTestStore,
};
use crate::engine::types::{EquityPoint, TradeRecord};
use crate::scripting::engine::{CachingDataLoader, CancelCallback};
use crate::tools::response_types::forward_test::{
    DriftAnalysis, ForwardTestEquityPoint, ForwardTestStatusResponse, ForwardTestTradeEvent,
    StartForwardTestResponse, StepForwardTestResponse,
};

// ──────────────────────────────────────────────────────────────────────────────
// start_forward_test
// ──────────────────────────────────────────────────────────────────────────────

/// Parameters for creating a forward test session.
pub struct StartParams<'a> {
    pub store: &'a SqliteForwardTestStore,
    pub strategy_store: Option<&'a dyn crate::data::traits::StrategyStore>,
    pub strategy: &'a str,
    pub symbol: &'a str,
    pub capital: f64,
    pub params: &'a HashMap<String, Value>,
    pub start_date: Option<&'a str>,
    pub baseline_sharpe: Option<f64>,
    pub baseline_win_rate: Option<f64>,
    pub baseline_max_dd: Option<f64>,
}

/// Create a new forward test session with frozen parameters.
#[allow(clippy::too_many_lines)]
pub fn start(p: &StartParams<'_>) -> Result<StartForwardTestResponse> {
    if p.capital <= 0.0 {
        bail!("Capital must be positive, got {}", p.capital);
    }

    // Validate that the strategy exists
    let run_params = crate::tools::run_script::RunScriptParams {
        strategy: Some(p.strategy.to_string()),
        script: None,
        params: p.params.clone(),
        profile: None,
    };
    crate::tools::run_script::resolve_script_source(&run_params, p.strategy_store)?;

    let now = Utc::now().to_rfc3339();
    let session_id = uuid::Uuid::new_v4().to_string();

    let mut effective_params = p.params.clone();
    if let Some(sd) = p.start_date {
        effective_params.insert("START_DATE".to_string(), Value::String(sd.to_string()));
    }
    effective_params.insert("symbol".to_string(), Value::String(p.symbol.to_uppercase()));
    effective_params.insert("CAPITAL".to_string(), serde_json::json!(p.capital));

    let session = crate::data::forward_test_store::ForwardTestSession {
        id: session_id.clone(),
        strategy: p.strategy.to_string(),
        symbol: p.symbol.to_uppercase(),
        params: serde_json::to_value(&effective_params)?,
        status: "active".to_string(),
        capital: p.capital,
        current_equity: p.capital,
        last_bar_date: None,
        total_trades: 0,
        realized_pnl: 0.0,
        engine_state: serde_json::json!({}),
        baseline_sharpe: p.baseline_sharpe,
        baseline_win_rate: p.baseline_win_rate,
        baseline_max_dd: p.baseline_max_dd,
        created_at: now.clone(),
        updated_at: now,
    };

    p.store.create_session(&session)?;

    let mut key_findings = vec![format!(
        "Forward test session created for {} on {}",
        p.strategy,
        p.symbol.to_uppercase()
    )];
    if p.baseline_sharpe.is_some() || p.baseline_win_rate.is_some() {
        key_findings.push("Baseline metrics set — drift detection will be active".to_string());
    } else {
        key_findings.push(
            "No baseline metrics provided — drift detection will be unavailable. Pass baseline_sharpe/baseline_win_rate/baseline_max_dd from your backtest results.".to_string(),
        );
    }
    if let Some(sd) = p.start_date {
        key_findings.push(format!("Forward test starts from {sd}"));
    }

    let summary = format!(
        "Forward test session initialized for {} on {} with ${:.0} capital. Session ID: {}",
        p.strategy,
        p.symbol.to_uppercase(),
        p.capital,
        &session_id,
    );

    Ok(StartForwardTestResponse {
        summary,
        session_id: session_id.clone(),
        strategy: p.strategy.to_string(),
        symbol: p.symbol.to_uppercase(),
        capital: p.capital,
        status: "active".to_string(),
        baseline_sharpe: p.baseline_sharpe,
        baseline_win_rate: p.baseline_win_rate,
        baseline_max_dd: p.baseline_max_dd,
        key_findings,
        suggested_next_steps: vec![
            "Merge updated market data into your parquet files".to_string(),
            format!("[NEXT] Call step_forward_test(session_id=\"{session_id}\") to process available bars"),
            format!("[THEN] Call forward_test_status(session_id=\"{session_id}\") to view progress and drift analysis"),
        ],
    })
}

// ──────────────────────────────────────────────────────────────────────────────
// step_forward_test
// ──────────────────────────────────────────────────────────────────────────────

/// Process new bars for a forward test session.
///
/// Re-runs the engine from session start through all available data, then
/// compares with the previous state to identify new trades and equity changes.
#[allow(clippy::too_many_lines)]
pub async fn step(
    store: &SqliteForwardTestStore,
    strategy_store: Option<&dyn crate::data::traits::StrategyStore>,
    cache: &Arc<CachedStore>,
    adjustment_store: Option<Arc<crate::data::adjustment_store::SqliteAdjustmentStore>>,
    session_id: &str,
) -> Result<StepForwardTestResponse> {
    let session = store
        .get_session(session_id)?
        .ok_or_else(|| anyhow::anyhow!("Forward test session '{session_id}' not found"))?;

    if session.status != "active" {
        bail!(
            "Session is '{}' — only active sessions can be stepped",
            session.status
        );
    }

    // Reconstruct params — fail explicitly if corrupted
    let params: HashMap<String, Value> = serde_json::from_value(session.params.clone())
        .map_err(|e| anyhow::anyhow!("Failed to deserialize session params: {e}"))?;

    let run_params = crate::tools::run_script::RunScriptParams {
        strategy: Some(session.strategy.clone()),
        script: None,
        params: params.clone(),
        profile: None,
    };
    let (_resolved_id, source) =
        crate::tools::run_script::resolve_script_source(&run_params, strategy_store)?;

    let loader = CachingDataLoader::new(Arc::clone(cache), adjustment_store);
    let no_cancel: CancelCallback = Box::new(|| false);

    let script_result = crate::scripting::engine::run_script_backtest(
        &source,
        &params,
        &loader,
        None,
        None,
        Some(&no_cancel),
    )
    .await?;

    let result = &script_result.result;
    let trade_log = &result.trade_log;
    let equity_curve = &result.equity_curve;

    let previous_trade_count = session.total_trades as usize;
    if trade_log.len() < previous_trade_count {
        bail!(
            "Forward test session {session_id} is inconsistent: replay produced {} trades, \
             but persisted state expects at least {previous_trade_count}. \
             The underlying data or strategy likely changed. Delete this session and start fresh.",
            trade_log.len(),
        );
    }
    let previous_equity_len = session
        .engine_state
        .get("equity_curve_len")
        .and_then(Value::as_u64)
        .and_then(|len| usize::try_from(len).ok())
        .unwrap_or(0);
    if equity_curve.len() < previous_equity_len {
        bail!(
            "Forward test session {session_id} is inconsistent: replay produced {} equity points, \
             but persisted state expects at least {previous_equity_len}. \
             The underlying data or strategy likely changed. Delete this session and start fresh.",
            equity_curve.len(),
        );
    }
    let new_trades: Vec<_> = trade_log.iter().skip(previous_trade_count).collect();
    let total_trades = trade_log.len() as i64;

    let current_equity = equity_curve.last().map_or(session.capital, |e| e.equity);
    let realized_pnl: f64 = trade_log.iter().map(|t| t.pnl).sum();
    let cumulative_pnl = current_equity - session.capital;

    let last_bar_date = equity_curve
        .last()
        .map(|e| e.datetime.format("%Y-%m-%d").to_string())
        .unwrap_or_default();

    let previous_last_date = session.last_bar_date.as_deref().unwrap_or("");
    let first_new_bar_date = find_first_new_bar_date(equity_curve, previous_last_date, &session);
    let new_bars_count = count_new_bars(equity_curve, previous_last_date);

    if new_bars_count == 0 && new_trades.is_empty() {
        return Ok(StepForwardTestResponse {
            summary: format!(
                "No new bars available for session {session_id}. Last processed: {previous_last_date}. \
                 Merge updated data and try again."
            ),
            session_id: session_id.to_string(),
            bars_processed: 0,
            date_range: format!("{previous_last_date} → {previous_last_date}"),
            current_equity: session.current_equity,
            daily_pnl: 0.0,
            cumulative_pnl: session.current_equity - session.capital,
            trades: vec![],
            total_trades: session.total_trades,
            key_findings: vec!["No new data available — merge updated parquet files first".to_string()],
            suggested_next_steps: vec![
                "Merge new market data with your existing parquet files".to_string(),
                format!("[THEN] Call step_forward_test(session_id=\"{session_id}\") again"),
            ],
        });
    }

    let trade_events = record_trades(&new_trades, store, session_id, &session.symbol)?;
    persist_snapshots(store, session_id, equity_curve, &session, &new_trades)?;

    let daily_pnl = if equity_curve.len() >= 2 {
        equity_curve[equity_curve.len() - 1].equity - equity_curve[equity_curve.len() - 2].equity
    } else {
        0.0
    };

    store.update_session_state(
        session_id,
        current_equity,
        &last_bar_date,
        total_trades,
        realized_pnl,
        &serde_json::json!({
            "equity_curve_len": equity_curve.len(),
            "trade_count": total_trades,
        }),
    )?;

    let key_findings = build_step_findings(
        &new_trades,
        new_bars_count,
        &first_new_bar_date,
        &last_bar_date,
        current_equity,
        session.capital,
        total_trades,
    );

    let total_return_pct = (current_equity - session.capital) / session.capital * 100.0;
    let summary = format!(
        "Stepped forward test: {new_bars_count} new bars processed ({first_new_bar_date} → {last_bar_date}). \
         {} new trade(s). Equity: ${current_equity:.2} ({total_return_pct:+.2}%)",
        new_trades.len()
    );

    Ok(StepForwardTestResponse {
        summary,
        session_id: session_id.to_string(),
        bars_processed: new_bars_count,
        date_range: format!("{first_new_bar_date} → {last_bar_date}"),
        current_equity,
        daily_pnl,
        cumulative_pnl,
        trades: trade_events,
        total_trades,
        key_findings,
        suggested_next_steps: vec![
            format!("[NEXT] Call forward_test_status(session_id=\"{session_id}\") for full equity curve and drift analysis"),
            "Merge more data and step again when ready".to_string(),
        ],
    })
}

// ──────────────────────────────────────────────────────────────────────────────
// forward_test_status
// ──────────────────────────────────────────────────────────────────────────────

/// Get comprehensive status for a forward test session with drift detection.
#[allow(clippy::too_many_lines)]
pub fn status(
    store: &SqliteForwardTestStore,
    session_id: &str,
) -> Result<ForwardTestStatusResponse> {
    let session = store
        .get_session(session_id)?
        .ok_or_else(|| anyhow::anyhow!("Forward test session '{session_id}' not found"))?;

    let snapshots = store.get_snapshots(session_id)?;
    let trades = store.get_trades(session_id)?;

    let equity_curve: Vec<ForwardTestEquityPoint> = snapshots
        .iter()
        .map(|s| ForwardTestEquityPoint {
            date: s.date.clone(),
            equity: s.equity,
            daily_pnl: s.daily_pnl,
        })
        .collect();

    let recent_trades: Vec<ForwardTestTradeEvent> = trades
        .iter()
        .rev()
        .take(10)
        .map(|t| ForwardTestTradeEvent {
            date: t.date.clone(),
            action: t.action.clone(),
            description: t.description.clone().unwrap_or_default(),
            pnl: t.pnl,
            exit_type: t.exit_type.clone(),
        })
        .collect();

    let total_return_pct = (session.current_equity - session.capital) / session.capital * 100.0;
    let forward_sharpe = compute_forward_sharpe(&snapshots);
    let forward_win_rate = compute_forward_win_rate(&trades);
    let forward_max_dd = compute_forward_max_dd(&snapshots);

    let drift = compute_drift(
        forward_sharpe,
        session.baseline_sharpe,
        forward_win_rate,
        session.baseline_win_rate,
        forward_max_dd,
        session.baseline_max_dd,
    );

    let days_running =
        if let Ok(created) = chrono::DateTime::parse_from_rfc3339(&session.created_at) {
            (Utc::now() - created.with_timezone(&Utc)).num_days()
        } else {
            0
        };

    let confidence_level = match session.total_trades {
        n if n >= 30 => "comparable",
        n if n >= 20 => "preliminary",
        _ => "insufficient",
    };

    let key_findings = build_status_findings(
        days_running,
        session.total_trades,
        confidence_level,
        total_return_pct,
        session.current_equity,
        drift.as_ref(),
        forward_win_rate,
    );

    let summary = format!(
        "Forward test for {} on {}: {days_running} days, {} trades, {total_return_pct:+.2}% return. \
         Confidence: {confidence_level}.",
        session.strategy, session.symbol, session.total_trades,
    );

    let mut suggested_next_steps = vec![];
    if confidence_level == "insufficient" {
        suggested_next_steps.push(format!(
            "Need {} more trades for preliminary comparison (currently {})",
            20 - session.total_trades,
            session.total_trades
        ));
    }
    if drift
        .as_ref()
        .is_some_and(|d| d.status == "warning" || d.status == "alert")
    {
        suggested_next_steps.push(
            "Performance is drifting from backtest baseline — investigate or pause the session"
                .to_string(),
        );
    }
    suggested_next_steps.push(format!(
        "[NEXT] Call step_forward_test(session_id=\"{session_id}\") after merging new data"
    ));

    Ok(ForwardTestStatusResponse {
        summary,
        session_id: session_id.to_string(),
        strategy: session.strategy,
        symbol: session.symbol,
        status: session.status,
        capital: session.capital,
        current_equity: session.current_equity,
        total_return_pct,
        total_trades: session.total_trades,
        realized_pnl: session.realized_pnl,
        last_bar_date: session.last_bar_date,
        days_running,
        equity_curve,
        recent_trades,
        drift,
        confidence_level: confidence_level.to_string(),
        key_findings,
        suggested_next_steps,
    })
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

fn find_first_new_bar_date(
    equity_curve: &[EquityPoint],
    previous_last_date: &str,
    session: &crate::data::forward_test_store::ForwardTestSession,
) -> String {
    if previous_last_date.is_empty() {
        equity_curve
            .first()
            .map(|e| e.datetime.format("%Y-%m-%d").to_string())
            .unwrap_or_default()
    } else if let Ok(cutoff) = chrono::NaiveDate::parse_from_str(previous_last_date, "%Y-%m-%d") {
        match equity_curve.iter().find(|e| e.datetime.date() > cutoff) {
            Some(e) => e.datetime.format("%Y-%m-%d").to_string(),
            None => session.last_bar_date.clone().unwrap_or_default(),
        }
    } else {
        session.last_bar_date.clone().unwrap_or_default()
    }
}

fn count_new_bars(equity_curve: &[EquityPoint], previous_last_date: &str) -> usize {
    if let Ok(cutoff) = chrono::NaiveDate::parse_from_str(previous_last_date, "%Y-%m-%d") {
        equity_curve
            .iter()
            .filter(|e| e.datetime.date() > cutoff)
            .count()
    } else {
        equity_curve.len()
    }
}

fn record_trades(
    new_trades: &[&TradeRecord],
    store: &SqliteForwardTestStore,
    session_id: &str,
    symbol: &str,
) -> Result<Vec<ForwardTestTradeEvent>> {
    let mut events = Vec::new();
    for trade in new_trades {
        let entry_date = trade.entry_datetime.format("%Y-%m-%d").to_string();
        let exit_date = trade.exit_datetime.format("%Y-%m-%d").to_string();

        let open_desc = format!(
            "{:?} — entry cost: ${:.2}",
            trade.entry_label, trade.entry_cost
        );
        events.push(ForwardTestTradeEvent {
            date: entry_date,
            action: "open".to_string(),
            description: open_desc,
            pnl: None,
            exit_type: None,
        });

        let pnl_pct = if trade.entry_cost.abs() > f64::EPSILON {
            trade.pnl / trade.entry_cost.abs() * 100.0
        } else {
            0.0
        };
        let close_desc = format!(
            "{:?} — P&L: ${:.2} ({pnl_pct:.1}%)",
            trade.exit_label, trade.pnl
        );
        events.push(ForwardTestTradeEvent {
            date: exit_date.clone(),
            action: "close".to_string(),
            description: close_desc.clone(),
            pnl: Some(trade.pnl),
            exit_type: Some(format!("{:?}", trade.exit_type)),
        });

        store.insert_trade(
            session_id,
            &ForwardTestTrade {
                trade_id: trade.trade_id as i64,
                action: "close".to_string(),
                date: exit_date,
                symbol: symbol.to_string(),
                description: Some(close_desc),
                entry_cost: Some(trade.entry_cost),
                exit_proceeds: Some(trade.exit_proceeds),
                pnl: Some(trade.pnl),
                exit_type: Some(format!("{:?}", trade.exit_type)),
                details: serde_json::json!({}),
            },
        )?;
    }
    Ok(events)
}

fn persist_snapshots(
    store: &SqliteForwardTestStore,
    session_id: &str,
    equity_curve: &[EquityPoint],
    session: &crate::data::forward_test_store::ForwardTestSession,
    new_trades: &[&TradeRecord],
) -> Result<()> {
    let previous_len = session
        .engine_state
        .get("equity_curve_len")
        .and_then(Value::as_u64)
        .and_then(|len| usize::try_from(len).ok())
        .unwrap_or(0)
        .min(equity_curve.len());

    let mut trades_by_date: HashMap<String, i64> = HashMap::new();
    for trade in new_trades {
        let d = trade.exit_datetime.format("%Y-%m-%d").to_string();
        *trades_by_date.entry(d).or_insert(0) += 1;
    }

    for (idx, point) in equity_curve.iter().enumerate().skip(previous_len) {
        let pnl = if idx > 0 {
            point.equity - equity_curve[idx - 1].equity
        } else {
            0.0
        };
        let date = point.datetime.format("%Y-%m-%d").to_string();
        let trades_today = trades_by_date.get(&date).copied().unwrap_or(0);
        store.insert_snapshot(
            session_id,
            &ForwardTestSnapshot {
                date,
                equity: point.equity,
                daily_pnl: pnl,
                cumulative_pnl: point.equity - session.capital,
                open_positions: 0,
                trades_today,
                details: serde_json::json!({}),
            },
        )?;
    }
    Ok(())
}

fn build_step_findings(
    new_trades: &[&TradeRecord],
    new_bars_count: usize,
    first_date: &str,
    last_date: &str,
    current_equity: f64,
    capital: f64,
    total_trades: i64,
) -> Vec<String> {
    let mut findings = vec![format!(
        "Processed {new_bars_count} new bar(s) from {first_date} to {last_date}"
    )];
    if new_trades.is_empty() {
        findings.push("No new trades closed in this period".to_string());
    } else {
        let new_pnl: f64 = new_trades.iter().map(|t| t.pnl).sum();
        let winners = new_trades.iter().filter(|t| t.pnl > 0.0).count();
        findings.push(format!(
            "{} new trade(s) closed: {} winner(s), P&L ${new_pnl:.2}",
            new_trades.len(),
            winners,
        ));
    }
    let total_return_pct = (current_equity - capital) / capital * 100.0;
    findings.push(format!(
        "Equity: ${current_equity:.2} ({total_return_pct:+.2}%), {total_trades} total trades"
    ));
    findings
}

fn compute_forward_sharpe(snapshots: &[ForwardTestSnapshot]) -> Option<f64> {
    if snapshots.len() < 5 {
        return None;
    }
    let daily_returns: Vec<f64> = snapshots
        .windows(2)
        .map(|w| {
            if w[0].equity.abs() > f64::EPSILON {
                (w[1].equity - w[0].equity) / w[0].equity
            } else {
                0.0
            }
        })
        .collect();
    let n = daily_returns.len() as f64;
    let mean = daily_returns.iter().sum::<f64>() / n;
    // Use sample variance (n-1) to match engine's std_dev calculation
    let variance = daily_returns
        .iter()
        .map(|r| (r - mean).powi(2))
        .sum::<f64>()
        / (n - 1.0);
    let std_dev = variance.sqrt();
    if std_dev > f64::EPSILON {
        Some(mean / std_dev * (252.0_f64).sqrt())
    } else {
        None
    }
}

fn compute_forward_win_rate(trades: &[ForwardTestTrade]) -> Option<f64> {
    let (closed, winners) = trades.iter().fold((0_usize, 0_usize), |(c, w), t| {
        if t.action == "close" {
            (c + 1, w + usize::from(t.pnl.unwrap_or(0.0) > 0.0))
        } else {
            (c, w)
        }
    });
    if closed == 0 {
        return None;
    }
    Some(winners as f64 / closed as f64)
}

fn compute_forward_max_dd(snapshots: &[ForwardTestSnapshot]) -> Option<f64> {
    if snapshots.len() < 2 {
        return None;
    }
    let mut peak = snapshots[0].equity;
    let mut max_dd = 0.0_f64;
    for s in snapshots {
        if s.equity > peak {
            peak = s.equity;
        }
        if peak > f64::EPSILON {
            let dd = (peak - s.equity) / peak;
            if dd > max_dd {
                max_dd = dd;
            }
        }
    }
    Some(max_dd)
}

fn build_status_findings(
    days_running: i64,
    total_trades: i64,
    confidence_level: &str,
    total_return_pct: f64,
    current_equity: f64,
    drift: Option<&DriftAnalysis>,
    forward_win_rate: Option<f64>,
) -> Vec<String> {
    let mut findings = vec![
        format!("Running {days_running} day(s), {total_trades} trades closed ({confidence_level} confidence)"),
        format!("Return: {total_return_pct:+.2}%, equity: ${current_equity:.2}"),
    ];
    if let Some(d) = drift {
        findings.push(format!("Drift status: {} — {}", d.status, d.assessment));
    }
    if let Some(wr) = forward_win_rate {
        findings.push(format!("Forward win rate: {:.1}%", wr * 100.0));
    }
    findings
}

fn compute_drift(
    forward_sharpe: Option<f64>,
    baseline_sharpe: Option<f64>,
    forward_win_rate: Option<f64>,
    baseline_win_rate: Option<f64>,
    forward_max_dd: Option<f64>,
    baseline_max_dd: Option<f64>,
) -> Option<DriftAnalysis> {
    if baseline_sharpe.is_none() && baseline_win_rate.is_none() && baseline_max_dd.is_none() {
        return None;
    }

    let sharpe_drift = match (forward_sharpe, baseline_sharpe) {
        (Some(f), Some(b)) if b.abs() > f64::EPSILON => Some((f - b) / b.abs()),
        _ => None,
    };
    let win_rate_drift = match (forward_win_rate, baseline_win_rate) {
        (Some(f), Some(b)) if b.abs() > f64::EPSILON => Some((f - b) / b),
        _ => None,
    };
    let max_dd_drift = match (forward_max_dd, baseline_max_dd) {
        (Some(f), Some(b)) if b.abs() > f64::EPSILON => Some((f - b) / b),
        _ => None,
    };

    let mut alerts = 0;
    let mut warnings = 0;
    if let Some(sd) = sharpe_drift {
        if sd < -0.5 {
            alerts += 1;
        } else if sd < -0.25 {
            warnings += 1;
        }
    }
    if let Some(wd) = win_rate_drift {
        if wd < -0.3 {
            alerts += 1;
        } else if wd < -0.15 {
            warnings += 1;
        }
    }
    if let Some(dd) = max_dd_drift {
        if dd > 0.5 {
            alerts += 1;
        } else if dd > 0.25 {
            warnings += 1;
        }
    }

    let (status, assessment) = if alerts > 0 {
        ("alert".to_string(), "Significant performance degradation — forward test is underperforming backtest expectations. Consider pausing.".to_string())
    } else if warnings > 0 {
        ("warning".to_string(), "Moderate drift from backtest baseline — monitor closely. May be within normal variance.".to_string())
    } else {
        (
            "on_track".to_string(),
            "Forward performance is tracking backtest expectations within acceptable bounds."
                .to_string(),
        )
    };

    Some(DriftAnalysis {
        forward_sharpe,
        baseline_sharpe,
        sharpe_drift,
        forward_win_rate,
        baseline_win_rate,
        win_rate_drift,
        forward_max_dd,
        baseline_max_dd,
        max_dd_drift,
        status,
        assessment,
    })
}
