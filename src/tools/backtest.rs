use anyhow::Result;
use polars::prelude::*;

use crate::engine::types::BacktestParams;

use super::ai_format;
use super::response_types::BacktestResponse;

pub fn execute(df: &DataFrame, params: &BacktestParams) -> Result<BacktestResponse> {
    let result = crate::engine::core::run_backtest(df, params)?;
    Ok(ai_format::format_backtest(result, params))
}
