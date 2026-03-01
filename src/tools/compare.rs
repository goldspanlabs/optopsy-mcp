use anyhow::Result;
use polars::prelude::*;

use crate::engine::types::CompareParams;

use super::ai_format;
use super::response_types::CompareResponse;

pub fn execute(df: &DataFrame, params: &CompareParams) -> Result<CompareResponse> {
    let result = crate::engine::core::compare_strategies(df, params)?;
    Ok(ai_format::format_compare(result))
}
