//! Compare multiple strategies side-by-side and rank by Sharpe and P&L.

use anyhow::Result;
use polars::prelude::*;

use crate::engine::types::CompareParams;

use super::ai_format;
use super::response_types::CompareResponse;

/// Run backtests for all strategies in `params` and return a ranked comparison response.
pub fn execute(df: &DataFrame, params: &CompareParams) -> Result<CompareResponse> {
    let (results, labeled_entries) = crate::engine::core::compare_strategies(df, params)?;
    Ok(ai_format::format_compare(results, &labeled_entries))
}
