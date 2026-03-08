use std::collections::HashSet;
use std::hash::BuildHasher;

use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::*;

use crate::engine::permutation::{run_permutation_test, PermutationParams};
use crate::engine::types::BacktestParams;

use super::ai_format;
use super::response_types::PermutationTestResponse;

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
