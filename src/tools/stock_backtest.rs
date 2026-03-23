//! Run a stock/equity backtest and return an AI-enriched response.
//!
//! Loads OHLCV data, evaluates signals, dispatches to `engine::stock_sim`,
//! then enriches the result with the same response format used by options backtests.

use anyhow::Result;

use crate::engine::stock_sim::{self, StockBacktestParams};
use crate::signals::helpers::collect_indicator_data;

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

    // Apply a default lookback cap for intraday intervals when no start date is
    // specified, preventing loading millions of bars from multi-year datasets.
    // Anchored to end_date when present so historical runs don't produce an
    // effective_start that exceeds end_date.
    let effective_start =
        stock_sim::compute_effective_start(params.interval, params.start_date, params.end_date);

    let ohlcv_df = stock_sim::load_ohlcv_df(ohlcv_path, effective_start, params.end_date)?;

    // Apply session filter BEFORE resampling so that out-of-session rows don't
    // pollute aggregated OHLC values. This applies whenever the *source* data is
    // intraday (has a Datetime column), regardless of target interval — e.g.
    // resampling 1-min data to daily should still exclude pre/post-market bars.
    let ohlcv_df = stock_sim::filter_session(&ohlcv_df, params.session_filter.as_ref())?;

    let ohlcv_df = stock_sim::resample_ohlcv(&ohlcv_df, params.interval)?;
    let bars = stock_sim::bars_from_df(&ohlcv_df)?;

    // Build signal date filters from the session-filtered, resampled DataFrame
    let date_col = stock_sim::detect_date_col(&ohlcv_df);
    let (entry_dates, exit_dates) = stock_sim::build_stock_signal_filters(
        params,
        &ohlcv_df,
        stock_sim::ohlcv_path_to_cache_root(ohlcv_path),
    )?;

    // Compute raw indicator data for charting from signals
    let indicator_data = collect_indicator_data(
        params.entry_signal.as_ref(),
        params.exit_signal.as_ref(),
        &ohlcv_df,
        date_col,
        &[],
    );

    // Run the simulation
    let result =
        stock_sim::run_stock_backtest(&bars, params, entry_dates.as_ref(), exit_dates.as_ref())?;

    super::macros::log_elapsed!(
        start,
        "Stock backtest engine finished",
        trades = result.trade_count
    );

    Ok(ai_format::format_stock_backtest(
        result,
        params,
        underlying_prices,
        indicator_data,
    ))
}
