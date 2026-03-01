use anyhow::Result;
use polars::prelude::*;

use crate::engine::types::EvaluateParams;

use super::ai_format;
use super::response_types::EvaluateResponse;

/// Compute median bid-ask spread % across the `DataFrame`
fn compute_median_spread_pct(df: &DataFrame) -> Option<f64> {
    let bid_col = df.column("bid").ok()?.f64().ok()?;
    let ask_col = df.column("ask").ok()?.f64().ok()?;

    let mut spreads = Vec::new();
    for i in 0..df.height() {
        let bid = bid_col.get(i).unwrap_or(0.0);
        let ask = ask_col.get(i).unwrap_or(0.0);
        if bid > 0.0 && ask > 0.0 {
            let mid = f64::midpoint(bid, ask);
            let spread_pct = (ask - bid) / mid * 100.0;
            spreads.push(spread_pct);
        }
    }

    if spreads.is_empty() {
        return None;
    }

    spreads.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let median_idx = spreads.len() / 2;
    Some(if spreads.len().is_multiple_of(2) {
        f64::midpoint(spreads[median_idx - 1], spreads[median_idx])
    } else {
        spreads[median_idx]
    })
}

pub fn execute(df: &DataFrame, params: &EvaluateParams) -> Result<EvaluateResponse> {
    let result = crate::engine::core::evaluate_strategy(df, params)?;
    let median_spread = compute_median_spread_pct(df);
    Ok(ai_format::format_evaluate(result, params, median_spread))
}
