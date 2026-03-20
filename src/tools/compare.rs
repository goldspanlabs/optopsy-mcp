//! Compare multiple strategies side-by-side and rank by Sharpe and P&L.
//!
//! Runs independent backtests for each strategy using shared simulation
//! parameters, then assembles a ranked comparison with per-strategy metrics
//! and trade logs for equity curve overlays.

use anyhow::Result;
use polars::prelude::*;

use crate::engine::core::StockCompareEntry;
use crate::engine::types::CompareParams;

use super::ai_format;
use super::response_types::CompareResponse;

/// Run backtests for all strategies in `params` and return a ranked comparison response.
pub fn execute(df: &DataFrame, params: &CompareParams) -> Result<CompareResponse> {
    let (results, labeled_entries) = crate::engine::core::compare_strategies(df, params)?;
    Ok(ai_format::format_compare(results, &labeled_entries))
}

/// Run stock backtests for all entries and return a ranked comparison response.
pub fn execute_stock(entries: &[StockCompareEntry]) -> Result<CompareResponse> {
    let start = std::time::Instant::now();
    let results = crate::engine::core::compare_stock_strategies(entries)?;
    super::macros::log_elapsed!(start, "Stock compare finished", entries = entries.len());
    Ok(ai_format::format_stock_compare(results, entries))
}
