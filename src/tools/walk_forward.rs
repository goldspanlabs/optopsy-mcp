use anyhow::Result;
use polars::prelude::*;

use crate::engine::types::BacktestParams;
use crate::engine::walk_forward;

use super::ai_format;
use super::response_types::WalkForwardResponse;

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
