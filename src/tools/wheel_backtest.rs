//! Execute a wheel strategy backtest and return an AI-enriched response.
//!
//! Resolves wheel parameters, dispatches to `engine::wheel_sim::run_wheel_backtest`,
//! then enriches the raw result with summary text, key findings, cycle statistics,
//! data quality diagnostics, and suggested next steps.

use std::collections::BTreeMap;

use anyhow::Result;
use chrono::NaiveDate;

use crate::engine::metrics::calculate_metrics;
use crate::engine::wheel_sim::{self, WheelParams};
use crate::tools::ai_helpers::build_backtest_quality;
use crate::tools::response_types::wheel::{WheelBacktestResponse, WheelCycle, WheelCycleSummary};
use crate::tools::response_types::UnderlyingPrice;

use super::ai_format;

/// Execute the wheel backtest engine and format the result with metrics, cycles, and assessment.
pub fn execute(
    options_df: &polars::prelude::DataFrame,
    ohlcv_closes: &BTreeMap<NaiveDate, f64>,
    params: &WheelParams,
    entry_dates: Option<&std::collections::HashSet<NaiveDate>>,
    trading_days: &[NaiveDate],
    underlying_prices: Vec<UnderlyingPrice>,
) -> Result<WheelBacktestResponse> {
    let start = std::time::Instant::now();

    let result =
        wheel_sim::run_wheel_backtest(options_df, ohlcv_closes, params, entry_dates, trading_days)?;

    let metrics = calculate_metrics(
        &result.equity_curve,
        &result.trade_log,
        params.capital,
        252.0, // daily bars per year
    )?;

    let cycle_summary = build_cycle_summary(&result.cycles);

    // AI enrichment
    let (summary, assessment, key_findings, suggested_next_steps) =
        ai_format::format_wheel_backtest(&result, &metrics, &cycle_summary, params);

    let data_quality = build_backtest_quality(&result.quality);

    super::macros::log_elapsed!(
        start,
        "Wheel backtest finished",
        trades = result.trade_log.len()
    );

    Ok(WheelBacktestResponse {
        summary,
        assessment,
        key_findings,
        metrics,
        trade_log: result.trade_log,
        cycles: result.cycles,
        cycle_summary,
        data_quality,
        underlying_prices,
        suggested_next_steps,
    })
}

/// Aggregate per-cycle statistics into a summary.
fn build_cycle_summary(cycles: &[WheelCycle]) -> WheelCycleSummary {
    let total = cycles.len();
    if total == 0 {
        return WheelCycleSummary {
            total_cycles: 0,
            completed_cycles: 0,
            put_only_cycles: 0,
            stopped_out_cycles: 0,
            avg_cycle_pnl: 0.0,
            avg_cycle_days: 0.0,
            avg_calls_per_assignment: 0.0,
            total_put_premium: 0.0,
            total_call_premium: 0.0,
            total_premium_collected: 0.0,
            total_stock_pnl: 0.0,
            assignment_rate: 0.0,
        };
    }

    // Completed = assigned AND called away (full cycle)
    let completed = cycles
        .iter()
        .filter(|c| c.assigned && c.called_away_date.is_some())
        .count();
    let put_only = cycles.iter().filter(|c| !c.assigned).count();
    // Stopped out = assigned but not called away (stock was sold via stop loss or end-of-data)
    let stopped_out = total - completed - put_only;

    let assigned = cycles.iter().filter(|c| c.assigned).count();
    let avg_calls_per_assignment = if assigned > 0 {
        cycles
            .iter()
            .filter(|c| c.assigned)
            .map(|c| c.calls_sold as f64)
            .sum::<f64>()
            / assigned as f64
    } else {
        0.0
    };

    let total_put_premium: f64 = cycles.iter().map(|c| c.put_premium).sum();
    let total_call_premium: f64 = cycles.iter().flat_map(|c| &c.call_premiums).sum::<f64>();
    let total_stock_pnl: f64 = cycles.iter().filter_map(|c| c.stock_pnl).sum();
    let total_premium_collected = total_put_premium + total_call_premium;

    let avg_cycle_pnl: f64 = cycles.iter().map(|c| c.total_pnl).sum::<f64>() / total as f64;
    let avg_cycle_days: f64 = cycles
        .iter()
        .map(|c| f64::from(c.days_in_cycle))
        .sum::<f64>()
        / total as f64;

    let assignment_rate = if total > 0 {
        assigned as f64 / total as f64
    } else {
        0.0
    };

    WheelCycleSummary {
        total_cycles: total,
        completed_cycles: completed,
        put_only_cycles: put_only,
        stopped_out_cycles: stopped_out,
        avg_cycle_pnl,
        avg_cycle_days,
        avg_calls_per_assignment,
        total_put_premium,
        total_call_premium,
        total_premium_collected,
        total_stock_pnl,
        assignment_rate,
    }
}
