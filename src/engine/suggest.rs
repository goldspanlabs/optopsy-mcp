use anyhow::{bail, Result};
use polars::prelude::*;
use std::fmt::Write as _;

use super::types::{Slippage, StrategyDef, TargetRange};
use crate::strategies;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskPreference {
    Conservative,
    Moderate,
    Aggressive,
}

#[derive(Debug, Clone)]
pub struct SuggestParams {
    pub strategy: String,
    pub risk_preference: RiskPreference,
    pub target_win_rate: Option<f64>,
    pub target_sharpe: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct SuggestResult {
    pub strategy: String,
    pub leg_deltas: Vec<TargetRange>,
    pub max_entry_dte: i32,
    pub exit_dte: i32,
    pub slippage: Slippage,
    pub rationale: String,
    pub confidence: f64,
    pub data_coverage: DataCoverage,
}

#[derive(Debug, Clone)]
pub struct DataCoverage {
    pub total_rows: usize,
    pub liquid_rows: usize,
    pub dte_range: String,
    pub expiration_count: usize,
    pub warnings: Vec<String>,
}

/// Analyze the loaded options chain and suggest optimal parameters for a strategy.
///
/// Performs an analysis of the `DataFrame` to:
/// 1. Compute DTE and `spread_ratio` per row (filters to bid > 0 && ask > 0).
/// 2. Apply liquidity filters based on `risk_preference`.
/// 3. Identify candidate DTE regions using clustering.
/// 4. Select leg delta target ranges using quantile-based analysis within the chosen DTE region.
/// 5. Infer slippage model from median spread quality of retained rows.
/// 6. Compute confidence score from data coverage (row density, DTE span, expiration cycles).
pub fn suggest_parameters(df: &DataFrame, params: &SuggestParams) -> Result<SuggestResult> {
    // Validate strategy exists and get leg definitions
    let strategy_def = strategies::find_strategy(&params.strategy)
        .ok_or_else(|| anyhow::anyhow!("Unknown strategy: {}", params.strategy))?;

    let is_multi_exp = strategy_def.is_multi_expiration();

    // Compute DTE and spread_ratio columns
    let df = crate::engine::filters::compute_dte(df)?;
    let df = compute_spread_ratio(&df)?;

    // Get unique quote dates to understand DTE range
    let total_rows = df.height();

    // Apply liquidity filter
    let (liquid_rows, liquid_df) = apply_liquidity_filter(&df, params.risk_preference)?;

    if liquid_rows == 0 {
        return Err(anyhow::anyhow!(
            "No rows pass liquidity filter for {:?}",
            params.risk_preference
        ));
    }

    // Get unique expiration dates from the liquid subset
    let exp_dates = liquid_df.column("expiration")?.unique()?.len();
    // Analyze DTE distribution for the best cluster
    let (max_entry_dte, exit_dte, dte_range_str) = find_best_dte_cluster(&liquid_df)?;

    // Analyze delta distribution for each leg's option type
    let leg_deltas = extract_leg_deltas(&liquid_df, &strategy_def)?;

    // Infer slippage from median spread_ratio
    let slippage = infer_slippage(&liquid_df)?;

    // Compute confidence score
    let confidence = compute_confidence(
        liquid_rows,
        total_rows,
        exp_dates,
        is_multi_exp,
        liquid_df.column("dte")?.i32()?.unique()?.len(),
    );

    // Build rationale text
    let rationale = build_rationale(
        &strategy_def,
        max_entry_dte,
        exit_dte,
        liquid_rows,
        total_rows,
        &dte_range_str,
        &leg_deltas,
        &slippage,
        params,
    );

    // Collect warnings
    let mut warnings = Vec::new();
    if liquid_rows < 30 {
        warnings.push(
            "Limited data: fewer than 30 liquid rows. Results may not be reliable.".to_string(),
        );
    }
    if liquid_df.column("dte")?.i32()?.unique()?.len() < 3 {
        warnings.push(
            "Sparse DTE coverage: fewer than 3 distinct DTE values in liquid zone.".to_string(),
        );
    }
    if is_multi_exp && exp_dates < 2 {
        warnings.push(format!(
            "Calendar/diagonal strategy requires 2+ expiration cycles; found {exp_dates}."
        ));
    }

    let data_coverage = DataCoverage {
        total_rows,
        liquid_rows,
        dte_range: dte_range_str,
        expiration_count: exp_dates,
        warnings,
    };

    Ok(SuggestResult {
        strategy: params.strategy.clone(),
        leg_deltas,
        max_entry_dte,
        exit_dte,
        slippage,
        rationale,
        confidence,
        data_coverage,
    })
}

/// Compute `spread_ratio` = (ask - bid) / mid price.
/// Filters to bid > 0 && ask > 0 to avoid NaN/infinity values.
fn compute_spread_ratio(df: &DataFrame) -> Result<DataFrame> {
    let result = df
        .clone()
        .lazy()
        .filter(col("bid").gt(lit(0.0)))
        .filter(col("ask").gt(lit(0.0)))
        .with_column(
            ((col("ask") - col("bid")) / ((col("ask") + col("bid")) / lit(2.0)))
                .alias("spread_ratio"),
        )
        .collect()?;
    Ok(result)
}

/// Apply liquidity filter based on `risk_preference` and return (`liquid_row_count`, `filtered_df`).
fn apply_liquidity_filter(
    df: &DataFrame,
    risk_preference: RiskPreference,
) -> Result<(usize, DataFrame)> {
    let max_spread = match risk_preference {
        RiskPreference::Conservative => 0.05,
        RiskPreference::Moderate => 0.15,
        RiskPreference::Aggressive => 0.30,
    };

    // Filter by spread_ratio according to the selected risk preference
    let filtered = df
        .clone()
        .lazy()
        .filter(col("spread_ratio").gt_eq(0.0)) // Ensure valid spreads
        .filter(col("spread_ratio").lt_eq(max_spread))
        .collect()?;

    let liquid_count = filtered.height();
    Ok((liquid_count, filtered))
}

/// Find the best DTE cluster (most continuous coverage) and suggest `exit_dte`.
/// Returns (`max_entry_dte`, `exit_dte`, `dte_range_string`).
fn find_best_dte_cluster(df: &DataFrame) -> Result<(i32, i32, String)> {
    let dte_col = df.column("dte")?.i32()?;
    #[allow(clippy::filter_map_identity)]
    let mut dtes: Vec<i32> = dte_col
        .iter()
        .filter_map(|x| x)
        .filter(|d| *d > 0) // Filter to strictly positive DTEs
        .collect();

    if dtes.is_empty() {
        bail!("No strictly positive DTE values in filtered data");
    }

    // Use unique DTEs to avoid duplicates dominating the cluster
    dtes.sort_unstable();
    dtes.dedup();

    // Find the largest contiguous cluster
    let mut best_cluster = vec![];
    let mut current_cluster = vec![dtes[0]];

    for &dte in &dtes[1..] {
        if dte - current_cluster.last().unwrap() <= 15 {
            // Allow gaps up to 15 days
            current_cluster.push(dte);
        } else {
            if current_cluster.len() > best_cluster.len() {
                best_cluster.clone_from(&current_cluster);
            }
            current_cluster = vec![dte];
        }
    }
    if current_cluster.len() > best_cluster.len() {
        best_cluster.clone_from(&current_cluster);
    }

    let min_dte = *best_cluster.first().unwrap_or(&1);
    let max_dte = *best_cluster.last().unwrap_or(&60);
    let dte_range_str = format!("{min_dte}-{max_dte} days");

    // Suggest exit_dte as 30% of max_entry_dte, floored to natural cluster boundary
    let suggested_exit = (max_dte as f32 * 0.3).floor() as i32;
    let exit_dte = suggested_exit.max(min_dte / 2).min(min_dte);

    Ok((max_dte, exit_dte, dte_range_str))
}

/// Extract recommended delta ranges for each leg based on option type analysis.
fn extract_leg_deltas(df: &DataFrame, strategy_def: &StrategyDef) -> Result<Vec<TargetRange>> {
    let mut leg_deltas = Vec::new();

    for leg in &strategy_def.legs {
        let option_type_str = match leg.option_type {
            crate::engine::types::OptionType::Call => "call",
            crate::engine::types::OptionType::Put => "put",
        };

        // Filter by option type
        let leg_df = df
            .clone()
            .lazy()
            .filter(col("option_type").eq(lit(option_type_str)))
            .with_column(col("delta").abs().alias("abs_delta"))
            .collect()?;

        if leg_df.height() == 0 {
            // No rows for this option type; use defaults
            leg_deltas.push(TargetRange {
                target: 0.5,
                min: 0.2,
                max: 0.8,
            });
            continue;
        }

        // Analyze delta distribution
        let delta_col = leg_df.column("abs_delta")?.f64()?;
        let mut deltas: Vec<f64> = delta_col
            .iter()
            .filter_map(|x| x.filter(|v| v.is_finite()))
            .collect();

        if deltas.is_empty() {
            leg_deltas.push(TargetRange {
                target: 0.5,
                min: 0.2,
                max: 0.8,
            });
            continue;
        }

        deltas.sort_by(f64::total_cmp);

        let q25_idx = (deltas.len() as f64 * 0.25) as usize;
        let q50_idx = (deltas.len() as f64 * 0.50) as usize;
        let q75_idx = (deltas.len() as f64 * 0.75) as usize;

        let min_delta = deltas[q25_idx];
        let target_delta = deltas[q50_idx];
        let max_delta = deltas[q75_idx];

        leg_deltas.push(TargetRange {
            target: target_delta.clamp(0.1, 0.9),
            min: min_delta.clamp(0.0, 0.5),
            max: max_delta.clamp(0.5, 1.0),
        });
    }

    Ok(leg_deltas)
}

/// Infer slippage model from median `spread_ratio` in the liquid subset.
fn infer_slippage(df: &DataFrame) -> Result<Slippage> {
    let spread_col = df.column("spread_ratio")?.f64()?;
    let mut spreads: Vec<f64> = spread_col
        .iter()
        .filter_map(|x| x.filter(|v| v.is_finite()))
        .collect();

    if spreads.is_empty() {
        return Ok(Slippage::Mid);
    }

    spreads.sort_by(f64::total_cmp);
    let median_spread = spreads[spreads.len() / 2];

    if median_spread < 0.05 {
        Ok(Slippage::Mid)
    } else if median_spread < 0.15 {
        Ok(Slippage::Spread)
    } else {
        // Get reference volume from the data
        let vol_col = df.column("volume")?;
        let ref_volume = if let Ok(vol_s) = vol_col.u64() {
            #[allow(clippy::filter_map_identity)]
            let vols: Vec<u64> = vol_s.iter().filter_map(|x| x).collect();
            if vols.is_empty() {
                1_000
            } else {
                let mut vols = vols;
                vols.sort_unstable();
                vols[vols.len() / 2]
            }
        } else {
            1_000
        };

        Ok(Slippage::Liquidity {
            fill_ratio: 0.7,
            ref_volume,
        })
    }
}

/// Compute confidence score based on data coverage.
fn compute_confidence(
    liquid_rows: usize,
    total_rows: usize,
    expiration_count: usize,
    is_multi_exp: bool,
    dte_cluster_count: usize,
) -> f64 {
    let base = liquid_rows as f64 / total_rows.max(1) as f64;
    let dte_bonus = (dte_cluster_count as f64 / 4.0).min(1.0);
    let exp_bonus = (expiration_count as f64 / 6.0).min(1.0);
    let calendar_penalty = if is_multi_exp && expiration_count < 2 {
        -0.3
    } else {
        0.0
    };

    (base * 0.5 + dte_bonus * 0.25 + exp_bonus * 0.25 + calendar_penalty).clamp(0.0, 1.0)
}

/// Build a human-readable rationale text.
#[allow(clippy::too_many_arguments)]
fn build_rationale(
    strategy_def: &StrategyDef,
    max_entry_dte: i32,
    exit_dte: i32,
    liquid_rows: usize,
    total_rows: usize,
    dte_range_str: &str,
    leg_deltas: &[TargetRange],
    slippage: &Slippage,
    params: &SuggestParams,
) -> String {
    let mut text = format!(
        "Strategy '{}' analysis based on {} liquid rows out of {} total. \
         Best DTE zone: {}. \
         Max entry DTE set to {} days; exit DTE set to {} days for tighter management.\n",
        strategy_def.name, liquid_rows, total_rows, dte_range_str, max_entry_dte, exit_dte
    );

    let _ = writeln!(text, "Leg delta targets (count={}):", leg_deltas.len());
    for (i, target) in leg_deltas.iter().enumerate() {
        let _ = writeln!(
            text,
            "  Leg {}: target δ={:.2}, range [{:.2}, {:.2}]",
            i, target.target, target.min, target.max
        );
    }

    match slippage {
        Slippage::Mid => {
            let _ = writeln!(
                text,
                "Slippage model: Mid — optimal execution assumed, no slippage cost."
            );
        }
        Slippage::Spread => {
            let _ = writeln!(
                text,
                "Slippage model: Spread — fills at mid + typical bid/ask cost."
            );
        }
        Slippage::Liquidity {
            fill_ratio,
            ref_volume,
        } => {
            let _ = writeln!(
                text,
                "Slippage: Liquidity model with {:.0}% fill ratio and reference volume {}.",
                fill_ratio * 100.0,
                ref_volume
            );
        }
        Slippage::PerLeg { .. } => {
            let _ = writeln!(
                text,
                "Slippage model: PerLeg — custom per-leg slippage model."
            );
        }
    }

    text.push_str(&add_target_context(params));

    text
}

fn add_target_context(params: &SuggestParams) -> String {
    let mut text = String::new();

    if let Some(wr) = params.target_win_rate {
        let _ = writeln!(
            text,
            "Target win rate: {:.1}% (informational only).",
            wr * 100.0
        );
    }

    if let Some(sharpe) = params.target_sharpe {
        let _ = writeln!(
            text,
            "Target Sharpe ratio: {sharpe:.2} (informational only)."
        );
    }

    if text.is_empty() {
        text.push_str(
            "No specific win rate or Sharpe target provided; defaults based on liquidity analysis.\n",
        );
    }

    text
}
