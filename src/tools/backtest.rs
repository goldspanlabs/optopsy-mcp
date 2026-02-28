use anyhow::Result;
use polars::prelude::*;

use crate::engine::types::BacktestParams;

pub fn execute(df: &DataFrame, params: &BacktestParams) -> Result<String> {
    let result = crate::engine::core::run_backtest(df, params)?;
    Ok(serde_json::to_string_pretty(&result)?)
}
