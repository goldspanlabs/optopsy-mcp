//! Handler body for the `portfolio_backtest` tool.

use crate::data::cache::validate_path_segment;
use crate::tools;
use crate::tools::response_types::PortfolioBacktestResponse;

use super::super::params::{tool_err, PortfolioBacktestParams};
use super::super::OptopsyServer;

/// Execute the `portfolio_backtest` tool logic.
///
/// Validates allocations, resolves OHLCV paths for each strategy,
/// and runs the portfolio backtest on a blocking thread.
pub async fn execute(
    server: &OptopsyServer,
    params: PortfolioBacktestParams,
) -> Result<PortfolioBacktestResponse, String> {
    // Validate allocation weights sum to ~100%
    let total_alloc: f64 = params.strategies.iter().map(|s| s.allocation_pct).sum();
    if (total_alloc - 100.0).abs() > 1.0 {
        return Err(format!(
            "Allocation percentages sum to {total_alloc:.1}% — must be approximately 100%"
        ));
    }

    tracing::info!(
        strategies = params.strategies.len(),
        capital = params.capital,
        "Portfolio backtest request received"
    );

    // Parse dates once
    let start_date = params
        .start_date
        .as_deref()
        .map(|s| {
            chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|_| format!("Invalid start_date \"{s}\": expected YYYY-MM-DD"))
        })
        .transpose()?;
    let end_date = params
        .end_date
        .as_deref()
        .map(|s| {
            chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|_| format!("Invalid end_date \"{s}\": expected YYYY-MM-DD"))
        })
        .transpose()?;

    // Resolve each strategy's OHLCV data path and cross-symbol paths
    let mut resolved = Vec::new();
    for config in &params.strategies {
        let symbol = config.symbol.to_uppercase();
        validate_path_segment(&symbol)
            .map_err(|e| format!("Invalid symbol \"{}\": {e}", config.symbol))?;

        let ohlcv_path = server.ensure_ohlcv(&symbol)?;

        let cross_ohlcv_paths = server.resolve_cross_ohlcv_paths(
            Some(&config.entry_signal),
            config.exit_signal.as_ref(),
            &[],
            &[],
        )?;

        resolved.push(tools::portfolio::ResolvedStrategy {
            symbol: symbol.clone(),
            side: config.side,
            entry_signal: config.entry_signal.clone(),
            exit_signal: config.exit_signal.clone(),
            allocation_pct: config.allocation_pct,
            quantity: config.quantity,
            stop_loss: config.stop_loss,
            take_profit: config.take_profit,
            max_hold_days: config.max_hold_days,
            slippage: config.slippage.clone(),
            ohlcv_path,
            cross_ohlcv_paths,
            start_date,
            end_date,
        });
    }

    let default_slippage = params.slippage.clone();
    let capital = params.capital;

    tokio::task::spawn_blocking(move || {
        tools::portfolio::execute(resolved, capital, &default_slippage)
    })
    .await
    .map_err(|e| format!("Portfolio backtest task panicked: {e}"))?
    .map_err(tool_err)
}
