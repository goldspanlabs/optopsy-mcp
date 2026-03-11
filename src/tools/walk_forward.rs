//! Run walk-forward analysis to detect overfitting by testing on rolling out-of-sample windows.
//!
//! Splits the data into train/test windows, runs a backtest on each, and
//! compares in-sample vs out-of-sample metrics to assess strategy robustness.

use anyhow::Result;
use polars::prelude::*;

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
        &result, params, train_days, test_days, step_days,
    ))
}
