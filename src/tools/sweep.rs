//! Run a parameter sweep across delta/DTE grids and rank combinations by Sharpe.
//!
//! Delegates to `engine::sweep::run_sweep` for the heavy lifting, then
//! formats the ranked results, sensitivity analysis, stability scores,
//! and optional OOS validation into an AI-enriched response.

use anyhow::Result;
use polars::prelude::*;

use crate::engine::sweep::{StockSweepParams, SweepParams};

use super::ai_format;
use super::response_types::SweepResponse;

/// Execute the parameter sweep engine and format ranked results with sensitivity analysis.
pub fn execute(df: &DataFrame, params: &SweepParams) -> Result<SweepResponse> {
    let output = crate::engine::sweep::run_sweep(df, params)?;
    Ok(ai_format::format_sweep(output))
}

/// Execute a stock-mode parameter sweep and format results.
pub fn execute_stock(params: &StockSweepParams) -> Result<SweepResponse> {
    let start = std::time::Instant::now();
    let output = crate::engine::sweep::run_stock_sweep(params)?;
    super::macros::log_elapsed!(
        start,
        "Stock parameter sweep finished",
        combinations = output.combinations_run
    );
    Ok(ai_format::format_sweep(output))
}
