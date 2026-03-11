//! Run a full event-driven backtest and return an AI-enriched response.

use anyhow::Result;
use polars::prelude::*;

use crate::engine::types::BacktestParams;

use super::ai_format;
use super::response_types::{BacktestResponse, UnderlyingPrice};

/// Execute the backtest engine and format the result with metrics, trade log, and assessment.
pub fn execute(
    df: &DataFrame,
    params: &BacktestParams,
    underlying_prices: Vec<UnderlyingPrice>,
) -> Result<BacktestResponse> {
    let start = std::time::Instant::now();
    let result = crate::engine::core::run_backtest(df, params)?;
    let elapsed = start.elapsed();
    tracing::info!(
        elapsed_ms = elapsed.as_millis(),
        trades = result.trade_count,
        "Backtest engine finished"
    );
    Ok(ai_format::format_backtest(
        result,
        params,
        underlying_prices,
    ))
}
