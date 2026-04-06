//! MCP tool handlers for forward testing (paper trading).
//!
//! Three tools:
//! - `start_forward_test` — initialize a new forward test session
//! - `step_forward_test` — process new bars and persist state
//! - `forward_test_status` — view equity curve, drift detection, open positions

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use chrono::Utc;
use serde_json::Value;

use crate::data::cache::CachedStore;
use crate::data::forward_test_store::{
    ForwardTestSnapshot, ForwardTestTrade, SqliteForwardTestStore,
};
use crate::scripting::engine::{CachingDataLoader, CancelCallback};
use crate::tools::response_types::forward_test::{
    DriftAnalysis, ForwardTestEquityPoint, ForwardTestStatusResponse, ForwardTestTradeEvent,
    StartForwardTestResponse, StepForwardTestResponse,
};

// ──────────────────────────────────────────────────────────────────────────────
// start_forward_test
// ──────────────────────────────────────────────────────────────────────────────

/// Create a new forward test session with frozen parameters.
pub async fn start(
    store: &SqliteForwardTestStore,
    strategy_store: Option<&dyn crate::data::traits::StrategyStore>,
    strategy: &str,
    symbol: &str,
    capital: f64,
    params: &HashMap<String, Value>,
    start_date: Option<&str>,
    baseline_sharpe: Option<f64>,
    baseline_win_rate: Option<f64>,
    baseline_max_dd: Option<f64>,
) -> Result<StartForwardTestResponse> {
    // Validate capital
    if capital <= 0.0 {
        bail!("Capital must be positive, got {capital}");
    }

    // Validate that the strategy exists
    let run_params = crate::tools::run_script::RunScriptParams {
        strategy: Some(strategy.to_string()),
        script: None,
        params: params.clone(),
        profile: None,
    };
    let (_resolved_id, _source) =
        crate::tools::run_script::resolve_script_source(&run_params, strategy_store)?;

    let now = Utc::now().to_rfc3339();
    let session_id = uuid::Uuid::new_v4().to_string();

    // Build params with start_date injected if provided
    let mut effective_params = params.clone();
    if let Some(sd) = start_date {
        effective_params.insert("START_DATE".to_string(), Value::String(sd.to_string()));
    }
    effective_params.insert("symbol".to_string(), Value::String(symbol.to_uppercase()));
    effective_params.insert("CAPITAL".to_string(), serde_json::json!(capital));

    let session = crate::data::forward_test_store::ForwardTestSession {
        id: session_id.clone(),
        strategy: strategy.to_string(),
        symbol: symbol.to_uppercase(),
        params: serde_json::to_value(&effective_params)?,
        status: "active".to_string(),
        capital,
        current_equity: capital,
        last_bar_date: None,
        total_trades: 0,
        realized_pnl: 0.0,
        engine_state: serde_json::json!({}),
        baseline_sharpe,
        baseline_win_rate,
        baseline_max_dd,
        created_at: now.clone(),
        updated_at: now,
    };

    store.create_session(&session)?;

    let mut key_findings = vec![format!(
        "Forward test session created for {strategy} on {}",
        symbol.to_uppercase()
    )];
    if baseline_sharpe.is_some() || baseline_win_rate.is_some() {
        key_findings.push("Baseline metrics set — drift detection will be active".to_string());
    } else {
        key_findings.push(
            "No baseline metrics provided — drift detection will be unavailable. Pass baseline_sharpe/baseline_win_rate/baseline_max_dd from your backtest results.".to_string(),
        );
    }
    if let Some(sd) = start_date {
        key_findings.push(format!("Forward test starts from {sd}"));
    }

    let summary = format!(
        "Forward test session initialized for {strategy} on {} with ${capital:.0} capital. \
         Session ID: {}",
        symbol.to_uppercase(),
        &session_id,
    );

    Ok(StartForwardTestResponse {
        summary,
        session_id: session_id.clone(),
        strategy: strategy.to_string(),
        symbol: symbol.to_uppercase(),
        capital,
        status: "active".to_string(),
        baseline_sharpe,
        baseline_win_rate,
        baseline_max_dd,
        key_findings,
        suggested_next_steps: vec![
            "Merge updated market data into your parquet files".to_string(),
            format!(
                "[NEXT] Call step_forward_test(session_id=\"{session_id}\") to process available bars"
            ),
            format!(
                "[THEN] Call forward_test_status(session_id=\"{session_id}\") to view progress and drift analysis"
            ),
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
/// This replay approach avoids Rhai scope serialization complexity while
/// guaranteeing identical behavior to a full backtest.
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

    // Reconstruct params from the stored session — fail explicitly if corrupted
    let params: HashMap<String, Value> = serde_json::from_value(session.params.clone())
        .map_err(|e| anyhow::anyhow!("Failed to deserialize session params: {e}"))?;

    // Resolve and run the strategy
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

    // Determine what's new since last step
    let previous_trade_count = session.total_trades as usize;
    let new_trades: Vec<_> = trade_log.iter().skip(previous_trade_count).collect();
    let total_trades = trade_log.len() as i64;

    // Calculate current equity and P&L
    let current_equity = equity_curve
        .last()
        .map(|e| e.equity)
        .unwrap_or(session.capital);
    let realized_pnl: f64 = trade_log.iter().map(|t| t.pnl).sum();
    let cumulative_pnl = current_equity - session.capital;

    // Determine the date range of new bars
    let last_bar_date = equity_curve
        .last()
        .map(|e| e.datetime.format("%Y-%m-%d").to_string())
        .unwrap_or_default();

    // Count bars processed beyond previous state
    let previous_last_date = session.last_bar_date.as_deref().unwrap_or("");
    let first_new_bar_date = if previous_last_date.is_empty() {
        equity_curve
            .first()
            .map(|e| e.datetime.format("%Y-%m-%d").to_string())
            .unwrap_or_default()
    } else {
        equity_curve
            .iter()
            .find(|e| e.datetime.format("%Y-%m-%d").to_string().as_str() > previous_last_date)
            .map(|e| e.datetime.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| session.last_bar_date.clone().unwrap_or_default())
    };

    let new_bars_count = equity_curve
        .iter()
        .filter(|e| e.datetime.format("%Y-%m-%d").to_string().as_str() > previous_last_date)
        .count();

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
            key_findings: vec![
                "No new data available — merge updated parquet files first".to_string(),
            ],
            suggested_next_steps: vec![
                "Merge new market data with your existing parquet files".to_string(),
                format!("[THEN] Call step_forward_test(session_id=\"{session_id}\") again"),
            ],
        });
    }

    // Record new trade events
    let mut trade_events = Vec::new();
    for trade in &new_trades {
        let entry_date = trade.entry_datetime.format("%Y-%m-%d").to_string();
        let exit_date = trade.exit_datetime.format("%Y-%m-%d").to_string();

        // Record open event
        let open_desc = format!(
            "{:?} — entry cost: ${:.2}",
            trade.entry_label, trade.entry_cost,
        );
        trade_events.push(ForwardTestTradeEvent {
            date: entry_date.clone(),
            action: "open".to_string(),
            description: open_desc.clone(),
            pnl: None,
            exit_type: None,
        });

        // Record close event
        let close_desc = format!(
            "{:?} — P&L: ${:.2} ({:.1}%)",
            trade.exit_label,
            trade.pnl,
            if trade.entry_cost.abs() > f64::EPSILON {
                trade.pnl / trade.entry_cost.abs() * 100.0
            } else {
                0.0
            }
        );
        trade_events.push(ForwardTestTradeEvent {
            date: exit_date.clone(),
            action: "close".to_string(),
            description: close_desc.clone(),
            pnl: Some(trade.pnl),
            exit_type: Some(format!("{:?}", trade.exit_type)),
        });

        // Persist trade to DB with close-specific description
        store.insert_trade(
            session_id,
            &ForwardTestTrade {
                trade_id: trade.trade_id as i64,
                action: "close".to_string(),
                date: exit_date,
                symbol: session.symbol.clone(),
                description: Some(close_desc),
                entry_cost: Some(trade.entry_cost),
                exit_proceeds: Some(trade.exit_proceeds),
                pnl: Some(trade.pnl),
                exit_type: Some(format!("{:?}", trade.exit_type)),
                details: serde_json::json!({}),
            },
        )?;
    }

    // Build per-bar snapshots for all new bars (not just the last one)
    let previous_equity_curve_len = session
        .engine_state
        .get("equity_curve_len")
        .and_then(Value::as_u64)
        .and_then(|len| usize::try_from(len).ok())
        .unwrap_or(0)
        .min(equity_curve.len());

    // Build a map of trade counts per date for snapshot details
    let mut trades_by_date: HashMap<String, i64> = HashMap::new();
    for trade in &new_trades {
        let trade_date = trade.exit_datetime.format("%Y-%m-%d").to_string();
        *trades_by_date.entry(trade_date).or_insert(0) += 1;
    }

    for (idx, point) in equity_curve
        .iter()
        .enumerate()
        .skip(previous_equity_curve_len)
    {
        let bar_daily_pnl = if idx > 0 {
            point.equity - equity_curve[idx - 1].equity
        } else {
            0.0
        };
        let snapshot_date = point.datetime.format("%Y-%m-%d").to_string();
        let trades_today = trades_by_date.get(&snapshot_date).copied().unwrap_or(0);

        store.insert_snapshot(
            session_id,
            &ForwardTestSnapshot {
                date: snapshot_date,
                equity: point.equity,
                daily_pnl: bar_daily_pnl,
                cumulative_pnl: point.equity - session.capital,
                open_positions: 0,
                trades_today,
                details: serde_json::json!({}),
            },
        )?;
    }

    // Daily PnL from equity curve for the latest bar
    let daily_pnl = if equity_curve.len() >= 2 {
        equity_curve[equity_curve.len() - 1].equity - equity_curve[equity_curve.len() - 2].equity
    } else {
        0.0
    };

    // Update session state
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

    // Build key findings
    let mut key_findings = Vec::new();
    key_findings.push(format!(
        "Processed {new_bars_count} new bar(s) from {first_new_bar_date} to {last_bar_date}"
    ));
    if !new_trades.is_empty() {
        let new_pnl: f64 = new_trades.iter().map(|t| t.pnl).sum();
        let winners = new_trades.iter().filter(|t| t.pnl > 0.0).count();
        key_findings.push(format!(
            "{} new trade(s) closed: {} winner(s), P&L ${new_pnl:.2}",
            new_trades.len(),
            winners,
        ));
    } else {
        key_findings.push("No new trades closed in this period".to_string());
    }
    let total_return_pct = (current_equity - session.capital) / session.capital * 100.0;
    key_findings.push(format!(
        "Equity: ${current_equity:.2} ({total_return_pct:+.2}%), {total_trades} total trades"
    ));

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
            format!(
                "[NEXT] Call forward_test_status(session_id=\"{session_id}\") for full equity curve and drift analysis"
            ),
            "Merge more data and step again when ready".to_string(),
        ],
    })
}

// ──────────────────────────────────────────────────────────────────────────────
// forward_test_status
// ──────────────────────────────────────────────────────────────────────────────

/// Get comprehensive status for a forward test session with drift detection.
pub async fn status(
    store: &SqliteForwardTestStore,
    session_id: &str,
) -> Result<ForwardTestStatusResponse> {
    let session = store
        .get_session(session_id)?
        .ok_or_else(|| anyhow::anyhow!("Forward test session '{session_id}' not found"))?;

    let snapshots = store.get_snapshots(session_id)?;
    let trades = store.get_trades(session_id)?;

    // Build equity curve from snapshots
    let equity_curve: Vec<ForwardTestEquityPoint> = snapshots
        .iter()
        .map(|s| ForwardTestEquityPoint {
            date: s.date.clone(),
            equity: s.equity,
            daily_pnl: s.daily_pnl,
        })
        .collect();

    // Recent trades (last 10)
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

    // Calculate forward test metrics
    let total_return_pct = (session.current_equity - session.capital) / session.capital * 100.0;

    // Compute forward Sharpe from daily P&L snapshots
    let forward_sharpe = if snapshots.len() >= 5 {
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
        let mean = daily_returns.iter().sum::<f64>() / daily_returns.len() as f64;
        let variance = daily_returns
            .iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>()
            / daily_returns.len() as f64;
        let std_dev = variance.sqrt();
        if std_dev > f64::EPSILON {
            Some(mean / std_dev * (252.0_f64).sqrt())
        } else {
            None
        }
    } else {
        None
    };

    // Forward win rate from closed trades
    let closed_trades: Vec<_> = trades.iter().filter(|t| t.action == "close").collect();
    let forward_win_rate = if !closed_trades.is_empty() {
        let winners = closed_trades
            .iter()
            .filter(|t| t.pnl.unwrap_or(0.0) > 0.0)
            .count();
        Some(winners as f64 / closed_trades.len() as f64)
    } else {
        None
    };

    // Forward max drawdown from equity snapshots (with zero guard)
    let forward_max_dd = if snapshots.len() >= 2 {
        let mut peak = snapshots[0].equity;
        let mut max_dd = 0.0_f64;
        for s in &snapshots {
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
    } else {
        None
    };

    // Drift detection
    let drift = compute_drift(
        forward_sharpe,
        session.baseline_sharpe,
        forward_win_rate,
        session.baseline_win_rate,
        forward_max_dd,
        session.baseline_max_dd,
    );

    // Days running
    let days_running =
        if let Ok(created) = chrono::DateTime::parse_from_rfc3339(&session.created_at) {
            (Utc::now() - created.with_timezone(&Utc)).num_days()
        } else {
            0
        };

    // Confidence level based on trade count
    let confidence_level = if session.total_trades >= 30 {
        "comparable".to_string()
    } else if session.total_trades >= 20 {
        "preliminary".to_string()
    } else {
        "insufficient".to_string()
    };

    // Build key findings
    let mut key_findings = Vec::new();
    key_findings.push(format!(
        "Running {days_running} day(s), {total_trades} trades closed ({confidence_level} confidence)",
        total_trades = session.total_trades,
    ));
    key_findings.push(format!(
        "Return: {total_return_pct:+.2}%, equity: ${:.2}",
        session.current_equity,
    ));
    if let Some(ref d) = drift {
        key_findings.push(format!("Drift status: {} — {}", d.status, d.assessment));
    }
    if let Some(wr) = forward_win_rate {
        key_findings.push(format!("Forward win rate: {:.1}%", wr * 100.0));
    }

    let summary = format!(
        "Forward test for {} on {}: {days_running} days, {} trades, {total_return_pct:+.2}% return. \
         Confidence: {confidence_level}.",
        session.strategy,
        session.symbol,
        session.total_trades,
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
        confidence_level,
        key_findings,
        suggested_next_steps,
    })
}

// ──────────────────────────────────────────────────────────────────────────────
// Drift detection helper
// ──────────────────────────────────────────────────────────────────────────────

fn compute_drift(
    forward_sharpe: Option<f64>,
    baseline_sharpe: Option<f64>,
    forward_win_rate: Option<f64>,
    baseline_win_rate: Option<f64>,
    forward_max_dd: Option<f64>,
    baseline_max_dd: Option<f64>,
) -> Option<DriftAnalysis> {
    // Need at least one baseline metric to compute drift
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

    // Determine overall status
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
        (
            "alert".to_string(),
            "Significant performance degradation — forward test is underperforming backtest expectations. Consider pausing.".to_string(),
        )
    } else if warnings > 0 {
        (
            "warning".to_string(),
            "Moderate drift from backtest baseline — monitor closely. May be within normal variance."
                .to_string(),
        )
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
