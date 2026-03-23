//! Run Bayesian optimization over options parameter space.
//!
//! Delegates to `engine::bayesian::run_bayesian_optimization` and formats
//! the result into an AI-enriched `BayesianOptimizeResponse`.

use anyhow::Result;
use polars::prelude::*;

use crate::engine::bayesian::BayesianParams;

use super::ai_format;
use super::response_types::BayesianOptimizeResponse;

/// Execute Bayesian optimization and format results.
pub fn execute(df: &DataFrame, params: &BayesianParams) -> Result<BayesianOptimizeResponse> {
    let output = crate::engine::bayesian::run_bayesian_optimization(df, params)?;
    Ok(ai_format::format_bayesian(output))
}
