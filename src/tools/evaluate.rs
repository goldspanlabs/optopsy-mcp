use anyhow::Result;
use polars::prelude::*;

use crate::engine::types::EvaluateParams;

use super::ai_format;
use super::response_types::EvaluateResponse;

pub fn execute(df: &DataFrame, params: &EvaluateParams) -> Result<EvaluateResponse> {
    let result = crate::engine::core::evaluate_strategy(df, params)?;
    Ok(ai_format::format_evaluate(result, params))
}
