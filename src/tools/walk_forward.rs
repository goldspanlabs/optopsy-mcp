//! Run walk-forward analysis to detect overfitting by testing on rolling out-of-sample windows.
//!
//! Splits the data into train/test windows, runs a backtest on each, and
//! compares in-sample vs out-of-sample metrics to assess strategy robustness.

use std::collections::HashSet;

use anyhow::Result;
use chrono::NaiveDateTime;
use polars::prelude::*;

use crate::engine::stock_sim::{Bar, StockBacktestParams};
use crate::engine::types::BacktestParams;
use crate::engine::walk_forward;

use super::ai_format;
use super::response_types::WalkForwardResponse;

/// Execute walk-forward analysis and format per-window and aggregate results.
pub fn execute(
    df: &DataFrame,
    params: &BacktestParams,
    train_days: i32,
    test_days: i32,
    step_days: Option<i32>,
) -> Result<WalkForwardResponse> {
    let start = std::time::Instant::now();
    let result = walk_forward::run_walk_forward(df, params, train_days, test_days, step_days)?;
    let elapsed = start.elapsed();
    tracing::info!(
        elapsed_ms = elapsed.as_millis(),
        windows = result.windows.len(),
        "Walk-forward analysis finished"
    );
    Ok(ai_format::format_walk_forward(
        &result,
        &params.strategy,
        None,
        train_days,
        test_days,
        step_days,
    ))
}

/// Execute stock-mode walk-forward analysis on OHLCV bars.
#[allow(clippy::too_many_arguments, clippy::implicit_hasher)]
pub fn execute_stock(
    bars: &[Bar],
    params: &StockBacktestParams,
    entry_dates: &Option<HashSet<NaiveDateTime>>,
    exit_dates: &Option<HashSet<NaiveDateTime>>,
    label: &str,
    train_days: i32,
    test_days: i32,
    step_days: Option<i32>,
) -> Result<WalkForwardResponse> {
    let start = std::time::Instant::now();
    let result = walk_forward::run_walk_forward_stock(
        bars,
        params,
        entry_dates,
        exit_dates,
        train_days,
        test_days,
        step_days,
    )?;
    let elapsed = start.elapsed();
    tracing::info!(
        elapsed_ms = elapsed.as_millis(),
        windows = result.windows.len(),
        "Stock walk-forward analysis finished"
    );
    Ok(ai_format::format_walk_forward(
        &result,
        label,
        Some("stock"),
        train_days,
        test_days,
        step_days,
    ))
}
