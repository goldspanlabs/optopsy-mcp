use anyhow::Result;
use polars::prelude::*;

use super::response_types::SuggestResponse;
use crate::engine::suggest::{SuggestParams, SuggestResult};

pub fn execute(df: &DataFrame, params: &SuggestParams) -> Result<SuggestResponse> {
    let result = crate::engine::suggest::suggest_parameters(df, params)?;
    Ok(format_suggest(result))
}

fn format_suggest(result: SuggestResult) -> SuggestResponse {
    let liquidity_pct = if result.data_coverage.total_rows == 0 {
        0.0
    } else {
        (result.data_coverage.liquid_rows as f64 / result.data_coverage.total_rows as f64) * 100.0
    };

    let summary = format!(
        "Suggested parameters for '{}': max_entry_dte={}, exit_dte={}, confidence={:.1}%, liquidity={:.1}%",
        result.strategy,
        result.max_entry_dte,
        result.exit_dte,
        result.confidence * 100.0,
        liquidity_pct
    );

    let mut suggested_next_steps = vec![];
    suggested_next_steps.push(
        "Use these parameters in evaluate_strategy to test the suggested configuration".to_string(),
    );
    suggested_next_steps.push(
        "Run run_backtest with the suggested parameters for full simulation validation".to_string(),
    );

    if result.confidence < 0.5 {
        suggested_next_steps.push(
            "Consider calling download_options_data to fetch more historical data".to_string(),
        );
        suggested_next_steps.push(
            "Try adjusting risk_preference to Aggressive if Conservative/Moderate filters are too strict"
                .to_string(),
        );
    }

    if result.data_coverage.expiration_count < 3 {
        suggested_next_steps.push(
            "Expand date range to include more expiration cycles for better coverage".to_string(),
        );
    }

    SuggestResponse {
        summary,
        strategy: result.strategy,
        leg_deltas: result.leg_deltas,
        max_entry_dte: result.max_entry_dte,
        exit_dte: result.exit_dte,
        slippage: result.slippage,
        rationale: result.rationale,
        confidence: result.confidence,
        data_coverage: super::response_types::DataCoverage {
            total_rows: result.data_coverage.total_rows,
            liquid_rows: result.data_coverage.liquid_rows,
            dte_range: result.data_coverage.dte_range,
            expiration_count: result.data_coverage.expiration_count,
            warnings: result.data_coverage.warnings,
        },
        suggested_next_steps,
    }
}
