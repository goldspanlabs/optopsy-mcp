//! Run a stock/equity backtest and return an AI-enriched response.
//!
//! Loads OHLCV data, evaluates signals, dispatches to `engine::stock_sim`,
//! then enriches the result with the same response format used by options backtests.

use anyhow::Result;

use crate::engine::stock_sim::{self, StockBacktestParams};

use super::ai_format;
use super::response_types::{StockBacktestResponse, UnderlyingPrice};

/// Execute the stock backtest engine and format the result.
pub fn execute(
    params: &StockBacktestParams,
    underlying_prices: Vec<UnderlyingPrice>,
) -> Result<StockBacktestResponse> {
    let start = std::time::Instant::now();

    // Parse OHLCV bars from the cached parquet file
    let ohlcv_path = params
        .ohlcv_path
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("ohlcv_path is required for stock backtest"))?;

    let bars = stock_sim::parse_ohlcv_bars(ohlcv_path, params.start_date, params.end_date)?;

    // Build signal date filters
    let (entry_dates, exit_dates) = stock_sim::build_stock_signal_filters(params)?;

    // Run the simulation
    let result =
        stock_sim::run_stock_backtest(&bars, params, entry_dates.as_ref(), exit_dates.as_ref())?;

    let elapsed = start.elapsed();
    tracing::info!(
        elapsed_ms = elapsed.as_millis(),
        trades = result.trade_count,
        "Stock backtest engine finished"
    );

    Ok(ai_format::format_stock_backtest(
        result,
        params,
        underlying_prices,
    ))
}
