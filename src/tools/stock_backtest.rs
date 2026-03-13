//! Run a stock/equity backtest and return an AI-enriched response.
//!
//! Loads OHLCV data, evaluates signals, dispatches to `engine::stock_sim`,
//! then enriches the result with the same response format used by options backtests.

use anyhow::Result;

use crate::engine::stock_sim::{self, StockBacktestParams};
use crate::signals::helpers::IndicatorData;

use super::ai_format;
use super::response_types::{StockBacktestResponse, UnderlyingPrice};

/// Execute the stock backtest engine and format the result.
pub fn execute(
    params: &StockBacktestParams,
    underlying_prices: Vec<UnderlyingPrice>,
) -> Result<StockBacktestResponse> {
    let start = std::time::Instant::now();

    // Load OHLCV DataFrame once, then derive bars and signal filters from it
    let ohlcv_path = params
        .ohlcv_path
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("ohlcv_path is required for stock backtest"))?;

    let ohlcv_df = stock_sim::load_ohlcv_df(ohlcv_path, params.start_date, params.end_date)?;

    // Apply session filter BEFORE resampling so that out-of-session rows don't
    // pollute aggregated OHLC values. This applies whenever the *source* data is
    // intraday (has a Datetime column), regardless of target interval — e.g.
    // resampling 1-min data to daily should still exclude pre/post-market bars.
    let ohlcv_df = stock_sim::filter_session(&ohlcv_df, params.session_filter.as_ref())?;

    let ohlcv_df = stock_sim::resample_ohlcv(&ohlcv_df, params.interval)?;
    let bars = stock_sim::bars_from_df(&ohlcv_df)?;

    // Build signal date filters from the session-filtered, resampled DataFrame
    let date_col = stock_sim::detect_date_col(&ohlcv_df);
    let (entry_dates, exit_dates) = stock_sim::build_stock_signal_filters(params, &ohlcv_df)?;

    // Compute raw indicator data for charting from signals
    let mut indicator_data: Vec<IndicatorData> = vec![];
    if let Some(ref spec) = params.entry_signal {
        indicator_data.extend(crate::signals::indicators::compute_indicator_data(
            spec, &ohlcv_df, date_col,
        ));
    }
    if let Some(ref spec) = params.exit_signal {
        // Deduplicate: skip indicators already present from entry signal
        for ind in crate::signals::indicators::compute_indicator_data(spec, &ohlcv_df, date_col) {
            if !indicator_data
                .iter()
                .any(|existing| existing.name == ind.name)
            {
                indicator_data.push(ind);
            }
        }
    }

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
        indicator_data,
    ))
}
