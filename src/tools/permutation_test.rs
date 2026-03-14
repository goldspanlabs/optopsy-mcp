//! Run a permutation test to assess statistical significance of backtest results.
//!
//! Shuffles trade P&L values to build a null distribution, then computes
//! p-values for Sharpe, total P&L, and other metrics to determine whether
//! observed performance is statistically distinguishable from random noise.

use std::collections::HashSet;
use std::hash::BuildHasher;

use anyhow::Result;
use chrono::{NaiveDate, NaiveDateTime};
use polars::prelude::*;

use crate::engine::permutation::{
    run_permutation_test, run_stock_permutation_test, PermutationParams,
};
use crate::engine::stock_sim::{Bar, StockBacktestParams};
use crate::engine::types::BacktestParams;

use super::ai_format;
use super::response_types::PermutationTestResponse;

/// Execute the permutation test and format results with p-values and significance assessment.
pub fn execute<S1: BuildHasher, S2: BuildHasher>(
    df: &DataFrame,
    params: &BacktestParams,
    perm_params: &PermutationParams,
    entry_dates: &Option<HashSet<NaiveDate, S1>>,
    exit_dates: Option<&HashSet<NaiveDate, S2>>,
) -> Result<PermutationTestResponse> {
    let start = std::time::Instant::now();
    let output = run_permutation_test(df, params, perm_params, entry_dates, exit_dates)?;
    let elapsed = start.elapsed();
    tracing::info!(
        elapsed_ms = elapsed.as_millis(),
        permutations = output.num_completed,
        "Permutation test finished"
    );
    Ok(ai_format::format_permutation_test(output, params))
}

/// Execute a stock-mode permutation test on OHLCV bars.
#[allow(clippy::implicit_hasher)]
pub fn execute_stock(
    bars: &[Bar],
    params: &StockBacktestParams,
    entry_dates: &Option<HashSet<NaiveDateTime>>,
    exit_dates: &Option<HashSet<NaiveDateTime>>,
    perm_params: &PermutationParams,
    label: &str,
) -> Result<PermutationTestResponse> {
    let start = std::time::Instant::now();
    let output = run_stock_permutation_test(bars, params, entry_dates, exit_dates, perm_params)?;
    let elapsed = start.elapsed();
    tracing::info!(
        elapsed_ms = elapsed.as_millis(),
        permutations = output.num_completed,
        "Stock permutation test finished"
    );
    Ok(ai_format::format_permutation_test_stock(
        output, label, params,
    ))
}
