//! Format wheel backtest results into AI-enriched responses.
//!
//! Transforms raw `WheelResult` data into structured responses with
//! natural-language summaries, key findings, and actionable next steps.

use crate::engine::types::PerformanceMetrics;
use crate::engine::wheel_sim::{WheelParams, WheelResult};
use crate::tools::ai_helpers::{
    assess_sharpe, format_pnl, DRAWDOWN_HIGH, SHARPE_NEEDS_IMPROVEMENT,
};
use crate::tools::response_types::wheel::WheelCycleSummary;

/// Format a wheel backtest result into summary, assessment, key findings, and next steps.
///
/// Returns `(summary, assessment, key_findings, suggested_next_steps)`.
pub fn format_wheel_backtest(
    result: &WheelResult,
    metrics: &PerformanceMetrics,
    summary: &WheelCycleSummary,
    params: &WheelParams,
) -> (String, String, Vec<String>, Vec<String>) {
    // Zero-trade early branch
    if result.trade_log.is_empty() {
        return (
            "Wheel backtest: no trades were executed. Check delta/DTE filters and data coverage."
                .to_string(),
            "N/A".to_string(),
            vec![
                "No option candidates matched the entry criteria.".to_string(),
                "Consider widening put_delta/call_delta ranges or DTE ranges.".to_string(),
            ],
            vec![
                "Widen put_delta or put_dte ranges and re-run".to_string(),
                "Check that options data covers the desired date range".to_string(),
            ],
        );
    }

    let assessment = assess_sharpe(metrics.sharpe);
    let total_pnl: f64 = result.trade_log.iter().map(|t| t.pnl).sum();
    let pnl_str = format_pnl(total_pnl);

    let summary_text = format!(
        "Wheel strategy: {assessment} performance (Sharpe {:.2}). \
         {} cycles ({} completed, {} put-only, {} stopped out). \
         {} total P&L, {:.1}% win rate, {:.1}% max drawdown. \
         Total premium collected: {}.",
        metrics.sharpe,
        summary.total_cycles,
        summary.completed_cycles,
        summary.put_only_cycles,
        summary.stopped_out_cycles,
        pnl_str,
        metrics.win_rate * 100.0,
        metrics.max_drawdown * 100.0,
        format_pnl(summary.total_premium_collected),
    );

    let mut key_findings = vec![
        format!(
            "Assignment rate: {:.0}% ({} of {} cycles)",
            summary.assignment_rate * 100.0,
            (summary.assignment_rate * summary.total_cycles as f64).round() as usize,
            summary.total_cycles
        ),
        format!(
            "Avg {:.1} covered calls per assignment",
            summary.avg_calls_per_assignment
        ),
        format!(
            "Put premium: {}, Call premium: {}",
            format_pnl(summary.total_put_premium),
            format_pnl(summary.total_call_premium)
        ),
        format!(
            "Avg cycle P&L: {} over {:.0} days",
            format_pnl(summary.avg_cycle_pnl),
            summary.avg_cycle_days
        ),
    ];

    if summary.total_stock_pnl.abs() > 0.01 {
        key_findings.push(format!(
            "Stock P&L from assignments: {}",
            format_pnl(summary.total_stock_pnl)
        ));
    }

    if metrics.max_consecutive_losses > 2 {
        key_findings.push(format!(
            "Max {} consecutive losing trades",
            metrics.max_consecutive_losses
        ));
    }

    let mut suggested_next_steps = Vec::new();
    if metrics.sharpe < SHARPE_NEEDS_IMPROVEMENT {
        suggested_next_steps.push(
            "[ITERATE] Try different put/call delta targets or DTE ranges to improve Sharpe"
                .to_string(),
        );
    }
    if metrics.max_drawdown > DRAWDOWN_HIGH {
        suggested_next_steps.push(
            "[RISK] High drawdown detected -- consider adding stop_loss to cap assignment risk"
                .to_string(),
        );
        if params.stop_loss.is_none() {
            suggested_next_steps.push(
                "[RISK] No stop_loss set -- try adding one (e.g., stop_loss: 0.10) to limit stock losses"
                    .to_string(),
            );
        }
    }
    suggested_next_steps
        .push("[ITERATE] Try different put/call delta targets to tune risk/reward".to_string());
    suggested_next_steps.push(
        "[RISK] Add entry_signal (e.g., \"VIX / VIX3M < 1.0\") to filter entries by market regime"
            .to_string(),
    );
    suggested_next_steps
        .push("[VALIDATE] Use permutation_test to check statistical significance".to_string());

    (
        summary_text,
        assessment.to_string(),
        key_findings,
        suggested_next_steps,
    )
}
