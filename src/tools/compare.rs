use anyhow::Result;
use polars::prelude::*;

use crate::engine::types::CompareParams;

pub fn execute(df: &DataFrame, params: &CompareParams) -> Result<String> {
    let result = crate::engine::core::compare_strategies(df, params)?;
    Ok(serde_json::to_string_pretty(&result)?)
}
