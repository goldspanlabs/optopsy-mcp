use anyhow::Result;
use polars::prelude::*;

use crate::engine::types::EvaluateParams;

pub fn execute(df: &DataFrame, params: &EvaluateParams) -> Result<String> {
    let result = crate::engine::core::evaluate_strategy(df, params)?;
    Ok(serde_json::to_string_pretty(&result)?)
}
