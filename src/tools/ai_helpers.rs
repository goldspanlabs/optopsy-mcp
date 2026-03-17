//! Shared helper functions and threshold constants for AI-enriched tool responses.
//!
//! Centralises assessment logic (Sharpe tiers, p-value interpretation, data quality
//! warnings) so that all formatting modules use consistent language and thresholds.

use std::collections::HashMap;

use crate::engine::types::{
    to_display_name, BacktestParams, BacktestQualityStats, ExitType, TradeRecord,
};

use super::response_types::{BacktestDataQuality, BacktestParamsSummary, TradeStat, TradeSummary};

// ── Assessment thresholds ────────────────────────────────────────────────────
// Centralised so they can be tuned (or made configurable) in one place.

/// Sharpe ratio tier boundaries (descending).
pub(crate) const SHARPE_EXCELLENT: f64 = 1.5;
pub(crate) const SHARPE_STRONG: f64 = 1.0;
pub(crate) const SHARPE_MODERATE: f64 = 0.5;

/// Profit factor thresholds.
pub(crate) const PF_CONSISTENTLY_PROFITABLE: f64 = 1.5;
pub(crate) const PF_MARGINALLY_PROFITABLE: f64 = 1.0;

/// Calmar ratio boundary: above = moderate drawdown, below = high drawdown.
pub(crate) const CALMAR_MODERATE: f64 = 1.0;

/// `VaR` 95% daily risk tiers (percentage).
pub(crate) const VAR_CONTAINED_PCT: f64 = 2.0;
pub(crate) const VAR_MODERATE_PCT: f64 = 5.0;

/// Data quality warning thresholds.
pub(crate) const PRICE_DATA_COVERAGE_WARN_PCT: f64 = 80.0;
pub(crate) const FILL_RATE_WARN_PCT: f64 = 50.0;

/// Backtest suggested-next-steps thresholds.
pub(crate) const SHARPE_NEEDS_IMPROVEMENT: f64 = 1.0;
pub(crate) const DRAWDOWN_HIGH: f64 = 0.15;

/// P-value significance tiers.
pub(crate) const P_HIGHLY_SIGNIFICANT: f64 = 0.01;
pub(crate) const P_SIGNIFICANT: f64 = 0.05;
pub(crate) const P_MARGINALLY_SIGNIFICANT: f64 = 0.10;

/// Walk-forward Sharpe decay thresholds.
pub(crate) const WF_SHARPE_DECAY_HIGH: f64 = 0.5;
pub(crate) const WF_SHARPE_DECAY_LOW: f64 = 0.1;

/// Walk-forward profitable window thresholds (percentage).
pub(crate) const WF_PROFITABLE_WINDOWS_GOOD: f64 = 70.0;
pub(crate) const WF_PROFITABLE_WINDOWS_BAD: f64 = 50.0;

/// Sweep overall score tiers.
pub(crate) const SWEEP_SCORE_WEAK: f64 = 0.5;
pub(crate) const SWEEP_SCORE_MODERATE: f64 = 0.7;

/// Build a serialisable parameter summary from backtest params for inclusion in responses.
pub(crate) fn build_params_summary(params: &BacktestParams) -> BacktestParamsSummary {
    BacktestParamsSummary {
        display_name: to_display_name(&params.strategy),
        strategy: params.strategy.clone(),
        leg_deltas: params.leg_deltas.clone(),
        entry_dte: params.entry_dte.clone(),
        exit_dte: params.exit_dte,
        slippage: params.slippage.clone(),
        commission: params.commission.clone(),
        capital: params.capital,
        quantity: params.quantity,
        multiplier: params.multiplier,
        max_positions: params.max_positions,
        stop_loss: params.stop_loss,
        take_profit: params.take_profit,
        max_hold_days: params.max_hold_days,
        selector: params.selector.clone(),
        entry_signal: params.entry_signal.as_ref().map(|s| {
            serde_json::to_value(s).unwrap_or_else(|e| {
                tracing::warn!(error = %e, "Failed to serialize entry_signal");
                serde_json::Value::Null
            })
        }),
        exit_signal: params.exit_signal.as_ref().map(|s| {
            serde_json::to_value(s).unwrap_or_else(|e| {
                tracing::warn!(error = %e, "Failed to serialize exit_signal");
                serde_json::Value::Null
            })
        }),
        min_net_premium: params.min_net_premium,
        max_net_premium: params.max_net_premium,
        min_net_delta: params.min_net_delta,
        max_net_delta: params.max_net_delta,
        min_days_between_entries: params.min_days_between_entries,
        expiration_filter: params.expiration_filter.clone(),
        exit_net_delta: params.exit_net_delta,
        sizing: params.sizing.clone(),
    }
}

/// Return a human-readable label for a Sharpe ratio value (e.g. "excellent", "poor").
pub(crate) fn assess_sharpe(sharpe: f64) -> &'static str {
    if sharpe >= SHARPE_EXCELLENT {
        "excellent"
    } else if sharpe >= SHARPE_STRONG {
        "strong"
    } else if sharpe >= SHARPE_MODERATE {
        "moderate"
    } else if sharpe >= 0.0 {
        "weak"
    } else {
        "poor"
    }
}

/// Format a P&L value as a signed dollar string (e.g. "+$150.00" or "-$42.50").
pub(crate) fn format_pnl(value: f64) -> String {
    if value >= 0.0 {
        format!("+${value:.2}")
    } else {
        format!("-${:.2}", value.abs())
    }
}

/// Map an `ExitType` enum variant to its display name string.
pub(crate) fn exit_type_name(exit_type: &ExitType) -> &'static str {
    match exit_type {
        ExitType::Expiration => "Expiration",
        ExitType::StopLoss => "StopLoss",
        ExitType::TakeProfit => "TakeProfit",
        ExitType::MaxHold => "MaxHold",
        ExitType::DteExit => "DteExit",
        ExitType::Adjustment => "Adjustment",
        ExitType::Signal => "Signal",
        ExitType::DeltaExit => "DeltaExit",
    }
}

/// Compute presentation-layer trade summary (winners/losers, exit breakdown, best/worst).
pub(crate) fn compute_trade_summary(
    trade_log: &[TradeRecord],
    metrics: &crate::engine::types::PerformanceMetrics,
) -> TradeSummary {
    let total = trade_log.len();

    // Only compute presentation-layer fields (best/worst trade, exit breakdown)
    // Trade-level averages come from PerformanceMetrics (already computed in metrics.rs)
    let mut winner_count: usize = 0;
    let mut loser_count: usize = 0;
    let mut exit_breakdown: HashMap<String, usize> = HashMap::new();
    let mut best: Option<&TradeRecord> = None;
    let mut worst: Option<&TradeRecord> = None;

    for t in trade_log {
        if t.pnl > 0.0 {
            winner_count += 1;
        } else if t.pnl < 0.0 {
            loser_count += 1;
        }
        // Zero-PnL (scratch) trades: neutral

        *exit_breakdown
            .entry(exit_type_name(&t.exit_type).to_string())
            .or_default() += 1;

        if best.is_none_or(|b| t.pnl > b.pnl) {
            best = Some(t);
        }
        if worst.is_none_or(|w| t.pnl < w.pnl) {
            worst = Some(t);
        }
    }

    let to_trade_stat = |t: Option<&TradeRecord>| {
        t.map(|t| TradeStat {
            pnl: t.pnl,
            date: t.entry_datetime.format("%Y-%m-%d").to_string(),
        })
    };

    TradeSummary {
        total,
        winners: winner_count,
        losers: loser_count,
        avg_pnl: metrics.avg_trade_pnl,
        avg_winner: metrics.avg_winner,
        avg_loser: metrics.avg_loser,
        avg_days_held: metrics.avg_days_held,
        exit_breakdown,
        best_trade: to_trade_stat(best),
        worst_trade: to_trade_stat(worst),
    }
}

/// Return the name of the most frequently occurring exit type in the trade log.
pub(crate) fn most_common_exit(trade_log: &[TradeRecord]) -> String {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for t in trade_log {
        *counts.entry(exit_type_name(&t.exit_type)).or_default() += 1;
    }
    counts
        .into_iter()
        .max_by_key(|(_, c)| *c)
        .map_or_else(|| "N/A".to_string(), |(name, _)| name.to_string())
}

/// Convert engine-level quality stats into a client-facing data quality report with warnings.
pub(crate) fn build_backtest_quality(quality: &BacktestQualityStats) -> BacktestDataQuality {
    let price_data_coverage_pct = if quality.trading_days_total > 0 {
        (quality.trading_days_with_data as f64 / quality.trading_days_total as f64) * 100.0
    } else {
        0.0
    };

    let fill_rate_pct = if quality.total_candidates > 0 {
        (quality.positions_opened as f64 / quality.total_candidates as f64) * 100.0
    } else {
        0.0
    };

    let median_entry_spread_pct = if quality.entry_spread_pcts.is_empty() {
        None
    } else {
        let mut sorted = quality.entry_spread_pcts.clone();
        sorted.sort_by(|a: &f64, b: &f64| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median_idx = sorted.len() / 2;
        Some(if sorted.len().is_multiple_of(2) {
            f64::midpoint(sorted[median_idx - 1], sorted[median_idx])
        } else {
            sorted[median_idx]
        })
    };

    let mut warnings = Vec::new();
    if price_data_coverage_pct < PRICE_DATA_COVERAGE_WARN_PCT {
        warnings.push(format!(
            "Price data missing for {:.0}% of trading days. Mark-to-market accuracy may be reduced.",
            100.0 - price_data_coverage_pct
        ));
    }
    if quality.total_candidates > 0 && fill_rate_pct < FILL_RATE_WARN_PCT {
        warnings.push(format!(
            "Only {fill_rate_pct:.0}% of entry candidates were opened. Consider increasing max_positions."
        ));
    }

    BacktestDataQuality {
        trading_days_total: quality.trading_days_total,
        trading_days_with_price_data: quality.trading_days_with_data,
        price_data_coverage_pct,
        total_entry_candidates: quality.total_candidates,
        total_positions_opened: quality.positions_opened,
        fill_rate_pct,
        median_entry_spread_pct,
        warnings,
    }
}

/// Generate human-readable key findings from backtest metrics and trade log.
pub(crate) fn backtest_key_findings(
    m: &crate::engine::types::PerformanceMetrics,
    trade_log: &[TradeRecord],
) -> Vec<String> {
    let mut findings = Vec::new();

    // Win rate + profit factor
    let win_pct = m.win_rate * 100.0;
    findings.push(format!(
        "Win rate of {win_pct:.0}% with profit factor {:.2}{}",
        m.profit_factor,
        if m.max_consecutive_losses == 0 && m.win_rate > 0.0 {
            " — no losing trades"
        } else if m.win_rate == 0.0 && m.avg_loser == 0.0 {
            " — all scratch trades"
        } else if m.profit_factor >= PF_CONSISTENTLY_PROFITABLE {
            " — consistently profitable"
        } else if m.profit_factor >= PF_MARGINALLY_PROFITABLE {
            " — marginally profitable"
        } else {
            " — losses exceed wins"
        }
    ));

    // CAGR + total return
    findings.push(format!(
        "Total return {:.1}%, CAGR {:.1}%",
        m.total_return_pct,
        m.cagr * 100.0,
    ));

    // Drawdown
    let dd_pct = m.max_drawdown * 100.0;
    findings.push(format!(
        "Max drawdown of {dd_pct:.1}%{}",
        if m.calmar > CALMAR_MODERATE {
            format!(" is moderate relative to returns (Calmar {:.2})", m.calmar)
        } else if m.calmar > 0.0 {
            format!(" is high relative to returns (Calmar {:.2})", m.calmar)
        } else {
            " with negative or zero returns".to_string()
        }
    ));

    // VaR
    let var_pct = m.var_95 * 100.0;
    findings.push(format!(
        "VaR 95% of {var_pct:.1}% — daily risk is {}",
        if var_pct < VAR_CONTAINED_PCT {
            "contained"
        } else if var_pct < VAR_MODERATE_PCT {
            "moderate"
        } else {
            "elevated"
        }
    ));

    // Expectancy + trade behavior
    let common_exit = most_common_exit(trade_log);
    findings.push(format!(
        "Expectancy {} per trade, avg hold {:.1} days, max losing streak: {}. Most common exit: {}",
        format_pnl(m.expectancy),
        m.avg_days_held,
        m.max_consecutive_losses,
        common_exit
    ));

    findings
}

/// Generate key findings from walk-forward aggregate statistics.
pub(crate) fn walk_forward_findings(
    agg: &crate::engine::walk_forward::WalkForwardAggregate,
) -> Vec<String> {
    let mut findings = Vec::new();
    if agg.failed_windows > 0 {
        findings.push(format!(
            "{} window(s) failed and were excluded from aggregate statistics",
            agg.failed_windows
        ));
    }
    if agg.avg_train_test_sharpe_decay > WF_SHARPE_DECAY_HIGH {
        findings.push(format!(
            "High train→test Sharpe decay ({:.2}) suggests overfitting risk",
            agg.avg_train_test_sharpe_decay
        ));
    } else if agg.avg_train_test_sharpe_decay < WF_SHARPE_DECAY_LOW {
        findings.push(format!(
            "Low train→test Sharpe decay ({:.2}) indicates robust strategy",
            agg.avg_train_test_sharpe_decay
        ));
    }
    if agg.pct_profitable_windows >= WF_PROFITABLE_WINDOWS_GOOD {
        findings.push(format!(
            "{:.0}% of test windows profitable — strong consistency",
            agg.pct_profitable_windows
        ));
    } else if agg.pct_profitable_windows < WF_PROFITABLE_WINDOWS_BAD {
        findings.push(format!(
            "Only {:.0}% of test windows profitable — strategy may be unreliable",
            agg.pct_profitable_windows
        ));
    }
    if agg.std_test_sharpe > 1.0 {
        findings.push(format!(
            "High variance in test Sharpe (σ={:.2}) — performance is inconsistent across windows",
            agg.std_test_sharpe
        ));
    }
    findings.push(format!(
        "Average out-of-sample Sharpe is {} ({:.2})",
        assess_sharpe(agg.avg_test_sharpe),
        agg.avg_test_sharpe
    ));
    findings
}

/// Convert an epoch timestamp (seconds) to a `YYYY-MM-DD` date string.
///
/// Returns the raw integer as a string if the timestamp is out of range.
pub(crate) fn epoch_to_date_string(epoch: i64) -> String {
    chrono::DateTime::from_timestamp(epoch, 0).map_or_else(
        || format!("{epoch}"),
        |dt| dt.naive_utc().format("%Y-%m-%d").to_string(),
    )
}

/// Return a significance label for a p-value (e.g. "highly significant", "not significant").
pub(crate) fn interpret_p_value(p: f64) -> &'static str {
    if p < P_HIGHLY_SIGNIFICANT {
        "highly significant"
    } else if p < P_SIGNIFICANT {
        "significant"
    } else if p < P_MARGINALLY_SIGNIFICANT {
        "marginally significant"
    } else {
        "not significant"
    }
}
