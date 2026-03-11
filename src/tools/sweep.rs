//! Run a parameter sweep across delta/DTE grids and rank combinations by Sharpe.
//!
//! Delegates to `engine::sweep::run_sweep` for the heavy lifting, then
//! formats the ranked results, sensitivity analysis, stability scores,
//! and optional OOS validation into an AI-enriched response.

use anyhow::Result;
use polars::prelude::*;

use crate::engine::sweep::SweepParams;

use super::ai_format;
use super::response_types::SweepResponse;

/// Execute the parameter sweep engine and format ranked results with sensitivity analysis.
pub fn execute(df: &DataFrame, params: &SweepParams) -> Result<SweepResponse> {
    let output = crate::engine::sweep::run_sweep(df, params)?;
    Ok(ai_format::format_sweep(output))
}
