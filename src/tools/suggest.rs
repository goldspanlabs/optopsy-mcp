use anyhow::Result;
use polars::prelude::*;

use super::response_types::SuggestResponse;
use crate::engine::suggest::{SuggestParams, SuggestResult};

pub fn execute(df: &DataFrame, params: &SuggestParams) -> Result<SuggestResponse> {
    let result = crate::engine::suggest::suggest_parameters(df, params)?;
    Ok(format_suggest(result))
}

fn format_suggest(result: SuggestResult) -> SuggestResponse {
    let summary = format!(
        "Suggested parameters for '{}': max_entry_dte={}, exit_dte={}, confidence={:.1}%",
        result.strategy,
        result.max_entry_dte,
        result.exit_dte,
        result.confidence * 100.0
    );

    let mut key_findings = vec![];
    key_findings.push(format!(
        "Liquidity analysis: {} liquid rows out of {} total ({:.1}%)",
        result.data_coverage.liquid_rows,
        result.data_coverage.total_rows,
        (result.data_coverage.liquid_rows as f64 / result.data_coverage.total_rows as f64) * 100.0
    ));

    key_findings.push(format!(
        "DTE coverage: {} with {} unique expirations detected",
        result.data_coverage.dte_range, result.data_coverage.expiration_count
    ));

    key_findings.push(format!(
        "Recommended slippage model: {}",
        format_slippage(&result.slippage)
    ));

    if result.confidence < 0.5 {
        key_findings.push(
            "⚠️ Low confidence: consider fetching more data or adjusting risk preference"
                .to_string(),
        );
    }

    for warning in &result.data_coverage.warnings {
        key_findings.push(format!("⚠️ {warning}"));
    }

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

fn format_slippage(slippage: &crate::engine::types::Slippage) -> String {
    match slippage {
        crate::engine::types::Slippage::Mid => "Mid".to_string(),
        crate::engine::types::Slippage::Spread => "Spread".to_string(),
        crate::engine::types::Slippage::Liquidity {
            fill_ratio,
            ref_volume,
        } => format!(
            "Liquidity (fill_ratio={:.0}%, ref_volume={ref_volume})",
            fill_ratio * 100.0
        ),
        crate::engine::types::Slippage::PerLeg { per_leg } => {
            format!("PerLeg (${per_leg:.2})")
        }
    }
}
