//! Run a full event-driven backtest and return an AI-enriched response.
//!
//! Resolves strategy parameters, dispatches to `engine::core::run_backtest`,
//! then enriches the raw result with summary text, key findings, trade
//! statistics, data quality diagnostics, and suggested next steps.

use anyhow::Result;
use polars::prelude::*;

use crate::engine::types::BacktestParams;
use crate::signals::helpers::IndicatorData;

use super::ai_format;
use super::response_types::{BacktestResponse, UnderlyingPrice};

/// Execute the backtest engine and format the result with metrics, trade log, and assessment.
pub fn execute(
    df: &DataFrame,
    params: &BacktestParams,
    underlying_prices: Vec<UnderlyingPrice>,
    ohlcv_df: Option<&DataFrame>,
) -> Result<BacktestResponse> {
    let start = std::time::Instant::now();
    let result = crate::engine::core::run_backtest(df, params)?;
    let elapsed = start.elapsed();
    tracing::info!(
        elapsed_ms = elapsed.as_millis(),
        trades = result.trade_count,
        "Backtest engine finished"
    );

    // Compute raw indicator data for charting from signals (if OHLCV data available)
    let mut indicator_data: Vec<IndicatorData> = vec![];
    if let Some(ohlcv) = ohlcv_df {
        if let Some(ref spec) = params.entry_signal {
            indicator_data.extend(crate::signals::indicators::compute_indicator_data(
                spec, ohlcv, "date",
            ));
        }
        if let Some(ref spec) = params.exit_signal {
            indicator_data.extend(crate::signals::indicators::compute_indicator_data(
                spec, ohlcv, "date",
            ));
        }
    }

    Ok(ai_format::format_backtest(
        result,
        params,
        underlying_prices,
        indicator_data,
    ))
}
