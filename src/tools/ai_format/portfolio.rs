//! Format portfolio backtest results into an AI-enriched response.

use crate::engine::types::{EquityPoint, PerformanceMetrics};
use crate::tools::ai_helpers::{
    assess_sharpe, format_pnl, DRAWDOWN_HIGH, SHARPE_NEEDS_IMPROVEMENT,
};
use crate::tools::response_types::{
    CorrelationEntry, PortfolioBacktestResponse, PortfolioStrategyResult,
};

/// Format a portfolio backtest into a full AI-enriched response.
#[allow(clippy::too_many_lines)]
pub fn format_portfolio(
    capital: f64,
    portfolio_metrics: PerformanceMetrics,
    strategy_results: Vec<PortfolioStrategyResult>,
    correlation_matrix: Vec<CorrelationEntry>,
    equity_curve: Vec<EquityPoint>,
) -> PortfolioBacktestResponse {
    let m = &portfolio_metrics;
    let assessment = assess_sharpe(m.sharpe).to_string();
    let total_trades: usize = strategy_results.iter().map(|s| s.trade_summary.total).sum();
    let total_pnl: f64 = strategy_results.iter().map(|s| s.total_pnl).sum();

    let summary = format!(
        "Portfolio backtest ({} strategies, ${:.0} capital): {assessment} performance \
         (Sharpe {:.2}). {} total trades, {} total P&L, {:.1}% max drawdown.",
        strategy_results.len(),
        capital,
        m.sharpe,
        total_trades,
        format_pnl(total_pnl),
        m.max_drawdown * 100.0,
    );

    let mut key_findings = Vec::new();

    // Per-strategy highlights
    if let Some(best) = strategy_results
        .iter()
        .max_by(|a, b| a.metrics.sharpe.partial_cmp(&b.metrics.sharpe).unwrap())
    {
        key_findings.push(format!(
            "Best individual Sharpe: {} ({:.2})",
            best.label, best.metrics.sharpe
        ));
    }
    if let Some(worst) = strategy_results
        .iter()
        .min_by(|a, b| a.metrics.sharpe.partial_cmp(&b.metrics.sharpe).unwrap())
    {
        key_findings.push(format!(
            "Worst individual Sharpe: {} ({:.2})",
            worst.label, worst.metrics.sharpe
        ));
    }

    // Correlation insights
    if let Some(highest) = correlation_matrix
        .iter()
        .max_by(|a, b| a.correlation.partial_cmp(&b.correlation).unwrap())
    {
        if highest.correlation > 0.7 {
            key_findings.push(format!(
                "High correlation ({:.2}) between {} and {} — limited diversification benefit",
                highest.correlation, highest.strategy_a, highest.strategy_b
            ));
        }
    }
    if let Some(lowest) = correlation_matrix
        .iter()
        .min_by(|a, b| a.correlation.partial_cmp(&b.correlation).unwrap())
    {
        if lowest.correlation < 0.3 {
            key_findings.push(format!(
                "Low correlation ({:.2}) between {} and {} — good diversification",
                lowest.correlation, lowest.strategy_a, lowest.strategy_b
            ));
        }
    }

    // Portfolio-level metrics
    key_findings.push(format!(
        "Portfolio win rate: {:.1}%, profit factor: {:.2}",
        m.win_rate * 100.0,
        m.profit_factor
    ));

    if m.max_drawdown > DRAWDOWN_HIGH {
        key_findings.push(format!(
            "Portfolio max drawdown ({:.1}%) exceeds {:.0}% threshold",
            m.max_drawdown * 100.0,
            DRAWDOWN_HIGH * 100.0,
        ));
    }

    // Suggested next steps
    let mut suggested_next_steps = Vec::new();

    if m.sharpe < SHARPE_NEEDS_IMPROVEMENT {
        suggested_next_steps.push(
            "[ITERATE] Consider adjusting strategy weights or replacing weak strategies"
                .to_string(),
        );
    }
    if m.max_drawdown > DRAWDOWN_HIGH {
        suggested_next_steps.push(
            "[RISK] High portfolio drawdown — consider adding stop-losses or reducing allocation to volatile strategies".to_string(),
        );
    }

    // Check for over-concentrated allocations
    if let Some(max_alloc) = strategy_results
        .iter()
        .map(|s| s.allocation_pct)
        .max_by(|a, b| a.partial_cmp(b).unwrap())
    {
        if max_alloc > 60.0 {
            suggested_next_steps.push(format!(
                "[BALANCE] Largest allocation is {max_alloc:.0}% — consider more even weighting for diversification",
            ));
        }
    }

    suggested_next_steps.push(
        "[NEXT] Adjust allocation weights based on individual strategy Sharpe ratios".to_string(),
    );
    suggested_next_steps
        .push("[COMPARE] Try different entry signals to improve weaker strategies".to_string());

    PortfolioBacktestResponse {
        summary,
        assessment,
        key_findings,
        capital,
        portfolio_metrics,
        strategy_results,
        correlation_matrix,
        equity_curve,
        suggested_next_steps,
    }
}
