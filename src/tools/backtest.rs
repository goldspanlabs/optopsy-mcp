//! Run a full event-driven backtest and return an AI-enriched response.
//!
//! Resolves strategy parameters, dispatches to `engine::core::run_backtest`,
//! then enriches the raw result with summary text, key findings, trade
//! statistics, data quality diagnostics, and suggested next steps.

use anyhow::Result;
use polars::prelude::*;

use crate::engine::types::BacktestParams;
use crate::signals::helpers::{collect_indicator_data, IndicatorData};

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
    super::macros::log_elapsed!(
        start,
        "Backtest engine finished",
        trades = result.trade_count
    );

    // Compute raw indicator data for charting from signals (if OHLCV data available)
    let indicator_data: Vec<IndicatorData> = if let Some(ohlcv) = ohlcv_df {
        collect_indicator_data(
            params.entry_signal.as_ref(),
            params.exit_signal.as_ref(),
            ohlcv,
            "date",
            &[],
        )
    } else {
        vec![]
    };

    Ok(ai_format::format_backtest(
        result,
        params,
        underlying_prices,
        indicator_data,
    ))
}
