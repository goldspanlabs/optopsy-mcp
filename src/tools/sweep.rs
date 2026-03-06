use anyhow::Result;
use polars::prelude::*;

use crate::engine::sweep::SweepParams;

use super::ai_format;
use super::response_types::SweepResponse;

pub fn execute(df: &DataFrame, params: &SweepParams) -> Result<SweepResponse> {
    let output = crate::engine::sweep::run_sweep(df, params)?;
    Ok(ai_format::format_sweep(output))
}
