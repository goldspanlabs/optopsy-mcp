//! Sweep analysis types and helper functions: combination building, sensitivity
//! analysis, parameter stability scoring, signal labeling, and data splitting.

use std::collections::{HashMap, HashSet};

use anyhow::{bail, Result};
use polars::prelude::*;

use super::types::{DteRange, Slippage, SweepResult, TargetRange};
use crate::data::parquet::QUOTE_DATETIME_COL;
use crate::signals::registry::SignalSpec;
use crate::strategies;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Per-strategy delta sweep spec: which delta targets to sweep for each leg.
#[derive(Debug, Clone)]
pub struct SweepStrategyEntry {
    /// Strategy name (e.g. `"iron_condor"`).
    pub name: String,
    /// Per-leg arrays of delta targets to enumerate.
    pub leg_delta_targets: Vec<Vec<f64>>,
}

/// Shared sweep dimensions: the grid of DTE, exit DTE, and slippage values to enumerate.
#[derive(Debug, Clone)]
pub struct SweepDimensions {
    /// Entry DTE target values to sweep.
    pub entry_dte_targets: Vec<i32>,
    /// Exit DTE values to sweep.
    pub exit_dtes: Vec<i32>,
    /// Slippage models to include in the sweep.
    pub slippage_models: Vec<Slippage>,
}

/// Full sweep input combining strategy entries, dimension grid, simulation
/// parameters, and optional out-of-sample / permutation settings.
#[derive(Debug, Clone)]
pub struct SweepParams {
    /// Strategy entries with per-leg delta targets.
    pub strategies: Vec<SweepStrategyEntry>,
    /// Grid dimensions (DTE, exit DTE, slippage) to enumerate.
    pub sweep: SweepDimensions,
    /// Shared simulation parameters (capital, quantity, etc.).
    pub sim_params: super::types::SimParams,
    /// Fraction of data reserved for out-of-sample validation (0 = disabled).
    pub out_of_sample_pct: f64,
    /// Optional directional filter for strategy selection.
    pub direction: Option<super::types::Direction>,
    /// Entry signal variants to sweep. Empty = use `sim_params.entry_signal` as-is.
    pub entry_signals: Vec<SignalSpec>,
    /// Exit signal variants to sweep. Empty = use `sim_params.exit_signal` as-is.
    pub exit_signals: Vec<SignalSpec>,
    /// If set, run this many permutations per combination to compute Sharpe p-values,
    /// then apply Bonferroni and BH-FDR multiple comparisons corrections.
    pub num_permutations: Option<usize>,
    /// Optional RNG seed for reproducible permutation tests.
    pub permutation_seed: Option<u64>,
}

/// Per-dimension-value averages used for sensitivity analysis.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct DimensionStats {
    /// Average Sharpe ratio across results sharing this dimension value.
    pub avg_sharpe: f64,
    /// Average P&L across results sharing this dimension value.
    pub avg_pnl: f64,
    /// Number of results contributing to these averages.
    pub count: usize,
}

/// Out-of-sample validation result comparing in-sample vs hold-out performance.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct OosResult {
    /// Combination label identifying this result.
    pub label: String,
    /// Sharpe ratio on the training (in-sample) set.
    pub train_sharpe: f64,
    /// Sharpe ratio on the test (out-of-sample) set.
    pub test_sharpe: f64,
    /// P&L on the training set.
    pub train_pnl: f64,
    /// P&L on the test set.
    pub test_pnl: f64,
}

/// Per-dimension stability for a single top result, measuring sensitivity to neighbor changes.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct DimensionStability {
    /// Name of the dimension (e.g. `"leg_1_delta"`, `"entry_dte"`).
    pub dimension: String,
    /// Stability score in [0, 1] where 1 = perfectly stable.
    pub score: f64,
    /// Largest relative Sharpe change among grid neighbors.
    pub max_sharpe_change: f64,
    /// Number of neighboring results found for this dimension.
    pub neighbor_count: usize,
}

/// Overall stability assessment for a top sweep result across all dimensions.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct StabilityScore {
    /// Combination label.
    pub label: String,
    /// Average stability score across all dimensions in [0, 1].
    pub overall_score: f64,
    /// True if `overall_score >= 0.7`.
    pub is_stable: bool,
    /// Human-readable warning when performance is fragile.
    pub warning: Option<String>,
    /// Stability breakdown for each swept dimension.
    pub per_dimension: Vec<DimensionStability>,
}

/// Complete output of a parameter sweep: ranked results, sensitivity, stability, and OOS data.
#[derive(Debug, Clone)]
pub struct SweepOutput {
    /// Total combinations enumerated (including skipped and failed).
    pub combinations_total: usize,
    /// Combinations that were actually backtested.
    pub combinations_run: usize,
    /// Pre-filter skips (delta ordering, deduplication)
    pub combinations_skipped: usize,
    /// Backtests that errored at runtime (after being selected to run)
    pub combinations_failed: usize,
    /// Number of signal combinations swept (entry × exit), if > 1
    pub signal_combinations: Option<usize>,
    /// Results sorted by Sharpe ratio (descending).
    pub ranked_results: Vec<SweepResult>,
    /// Sensitivity analysis: dimension name -> value -> aggregate stats.
    pub dimension_sensitivity: HashMap<String, HashMap<String, DimensionStats>>,
    /// Out-of-sample validation results for top combinations.
    pub oos_results: Vec<OosResult>,
    /// Parameter stability scores for top combinations.
    pub stability_scores: Vec<StabilityScore>,
    /// Multiple comparisons correction results, populated when `num_permutations` is set.
    /// Tuple of (Bonferroni, Benjamini-Hochberg) corrections applied to Sharpe p-values.
    pub multiple_comparisons: Option<(
        super::multiple_comparisons::MultipleComparisonsResult,
        super::multiple_comparisons::MultipleComparisonsResult,
    )>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Generate cartesian product of multiple arrays of f64 values.
pub fn cartesian_product(arrays: &[Vec<f64>]) -> Vec<Vec<f64>> {
    if arrays.is_empty() {
        return vec![vec![]];
    }
    let mut result = vec![vec![]];
    for arr in arrays {
        if arr.is_empty() {
            return vec![];
        }
        let mut next = Vec::with_capacity(result.len() * arr.len());
        for existing in &result {
            for val in arr {
                let mut combo = existing.clone();
                combo.push(*val);
                next.push(combo);
            }
        }
        result = next;
    }
    result
}

/// Convert a single delta target to a `TargetRange` (± 0.05, clamped [0.01, 0.99]).
pub fn delta_target_to_range(target: f64) -> TargetRange {
    TargetRange {
        target: target.clamp(0.01, 0.99),
        min: (target - 0.05).clamp(0.01, 0.99),
        max: (target + 0.05).clamp(0.01, 0.99),
    }
}

/// Convert a DTE target to a `DteRange` (± 30%, min clamped to 1).
pub fn dte_target_to_range(target: i32) -> DteRange {
    let margin = (f64::from(target) * 0.3).round() as i32;
    let margin = margin.max(1);
    DteRange {
        target,
        min: (target - margin).max(1),
        max: target + margin,
    }
}

/// Split a `DataFrame` by date for out-of-sample validation.
/// `oos_pct` is the fraction of data to use as test set (e.g. 0.3 = 30%).
pub fn split_by_date(df: &DataFrame, oos_pct: f64) -> Result<(DataFrame, DataFrame)> {
    if oos_pct <= 0.0 || oos_pct >= 1.0 {
        bail!("out_of_sample_pct must be between 0 and 1 (exclusive)");
    }

    let date_col = QUOTE_DATETIME_COL;
    let sorted = df
        .clone()
        .lazy()
        .sort([date_col], SortMultipleOptions::default())
        .collect()?;

    let n = sorted.height();
    if n < 2 {
        bail!("out-of-sample validation requires at least 2 rows of data");
    }
    let split_idx = ((n as f64) * (1.0 - oos_pct)).round() as usize;
    let split_idx = split_idx.clamp(1, n - 1);

    let train = sorted.slice(0, split_idx);
    let test = sorted.slice(split_idx as i64, n - split_idx);

    Ok((train, test))
}

/// Count distinct entry dates in a trade log.
pub fn count_independent_entry_periods(trade_log: &[super::types::TradeRecord]) -> usize {
    let dates: HashSet<_> = trade_log.iter().map(|t| t.entry_datetime.date()).collect();
    dates.len()
}

/// Check if delta ordering violates the strategy's default ordering.
///
/// For example, `bull_call_spread` defaults are [0.50, 0.10] (leg0 > leg1),
/// so a combo [0.10, 0.50] is inverted and gets skipped.
pub fn violates_delta_ordering(strategy_name: &str, delta_targets: &[f64]) -> bool {
    if delta_targets.len() <= 1 {
        return false;
    }

    let Some(strategy_def) = strategies::find_strategy(strategy_name) else {
        return false;
    };

    let defaults = strategy_def.default_deltas();
    if defaults.len() != delta_targets.len() {
        return false;
    }

    // Check pairwise ordering: for every pair of legs, the relative ordering
    // of the sweep deltas must match the defaults.
    for i in 0..delta_targets.len() {
        for j in (i + 1)..delta_targets.len() {
            let default_order = defaults[i].target.partial_cmp(&defaults[j].target);
            let sweep_order = delta_targets[i].partial_cmp(&delta_targets[j]);
            match (default_order, sweep_order) {
                (Some(std::cmp::Ordering::Greater), Some(std::cmp::Ordering::Less))
                | (Some(std::cmp::Ordering::Less), Some(std::cmp::Ordering::Greater)) => {
                    return true;
                }
                _ => {}
            }
        }
    }

    false
}

/// Generate a short human-readable label for a `SignalSpec`.
pub(crate) fn signal_spec_label(spec: &SignalSpec) -> String {
    match spec {
        SignalSpec::RsiBelow { threshold, .. } => format!("RsiBelow(t={threshold})"),
        SignalSpec::RsiAbove { threshold, .. } => format!("RsiAbove(t={threshold})"),
        SignalSpec::MacdBullish { .. } => "MacdBullish".to_string(),
        SignalSpec::MacdBearish { .. } => "MacdBearish".to_string(),
        SignalSpec::MacdCrossover { .. } => "MacdCrossover".to_string(),
        SignalSpec::StochasticBelow {
            period, threshold, ..
        } => format!("StochBelow(p={period},t={threshold})"),
        SignalSpec::StochasticAbove {
            period, threshold, ..
        } => format!("StochAbove(p={period},t={threshold})"),
        SignalSpec::PriceAboveSma { period, .. } => format!("AboveSma(p={period})"),
        SignalSpec::PriceBelowSma { period, .. } => format!("BelowSma(p={period})"),
        SignalSpec::PriceAboveEma { period, .. } => format!("AboveEma(p={period})"),
        SignalSpec::PriceBelowEma { period, .. } => format!("BelowEma(p={period})"),
        SignalSpec::SmaCrossover {
            fast_period,
            slow_period,
            ..
        } => format!("SmaCross(f={fast_period},s={slow_period})"),
        SignalSpec::SmaCrossunder {
            fast_period,
            slow_period,
            ..
        } => format!("SmaXunder(f={fast_period},s={slow_period})"),
        SignalSpec::EmaCrossover {
            fast_period,
            slow_period,
            ..
        } => format!("EmaCross(f={fast_period},s={slow_period})"),
        SignalSpec::EmaCrossunder {
            fast_period,
            slow_period,
            ..
        } => format!("EmaXunder(f={fast_period},s={slow_period})"),
        SignalSpec::BollingerLowerTouch { period, .. } => format!("BollLower(p={period})"),
        SignalSpec::BollingerUpperTouch { period, .. } => format!("BollUpper(p={period})"),
        SignalSpec::ConsecutiveUp { count, .. } => format!("ConsecUp(n={count})"),
        SignalSpec::ConsecutiveDown { count, .. } => format!("ConsecDown(n={count})"),
        SignalSpec::RateOfChange {
            period, threshold, ..
        } => format!("RoC(p={period},t={threshold})"),
        SignalSpec::And { .. } => "And(…)".to_string(),
        SignalSpec::Or { .. } => "Or(…)".to_string(),
        // Fallback: use Debug variant name truncated
        other => {
            let dbg = format!("{other:?}");
            // Take up to the first '{' or 100 chars
            let end = dbg.find('{').unwrap_or(dbg.len()).min(100);
            dbg[..end].trim().to_string()
        }
    }
}

/// A single entry×exit signal combination for the sweep.
pub(crate) struct SignalCombo {
    pub(crate) entry: Option<SignalSpec>,
    pub(crate) exit: Option<SignalSpec>,
    /// Human-readable label for display (may not be unique for complex signals)
    pub(crate) label: String,
    /// Full-precision key for deduplication (uses `Debug` representation)
    pub(crate) dedup_key: String,
    pub(crate) dim_keys: Vec<(String, String)>,
}

/// Build the cartesian product of entry × exit signal lists.
/// If a list is empty, use `None` (1 variant) for that slot.
pub(crate) fn build_signal_combos(
    entry_signals: &[SignalSpec],
    exit_signals: &[SignalSpec],
) -> Vec<SignalCombo> {
    let entry_variants: Vec<Option<&SignalSpec>> = if entry_signals.is_empty() {
        vec![None]
    } else {
        entry_signals.iter().map(Some).collect()
    };
    let exit_variants: Vec<Option<&SignalSpec>> = if exit_signals.is_empty() {
        vec![None]
    } else {
        exit_signals.iter().map(Some).collect()
    };

    let mut combos = Vec::with_capacity(entry_variants.len() * exit_variants.len());
    for entry in &entry_variants {
        for exit in &exit_variants {
            let mut parts = Vec::new();
            let mut dim_keys = Vec::new();

            if let Some(e) = entry {
                let lbl = signal_spec_label(e);
                dim_keys.push(("entry_signal".to_string(), lbl.clone()));
                parts.push(format!("ent={lbl}"));
            }
            if let Some(x) = exit {
                let lbl = signal_spec_label(x);
                dim_keys.push(("exit_signal".to_string(), lbl.clone()));
                parts.push(format!("ext={lbl}"));
            }

            let label = if parts.is_empty() {
                String::new()
            } else {
                format!("[{}]", parts.join(","))
            };

            // Use Debug representation for dedup key (fully unique, unlike display labels)
            let dedup_key = format!("{entry:?}|{exit:?}");

            combos.push(SignalCombo {
                entry: entry.cloned(),
                exit: exit.cloned(),
                label,
                dedup_key,
                dim_keys,
            });
        }
    }
    combos
}

/// Build a label for a sweep combination (reuses compare labeling pattern).
pub(crate) fn build_sweep_label(
    strategy_name: &str,
    deltas: &[TargetRange],
    dte: i32,
    exit_dte: i32,
    slippage: &Slippage,
) -> String {
    let delta_str: Vec<String> = deltas.iter().map(|d| format!("{:.2}", d.target)).collect();
    let slippage_suffix = match slippage {
        Slippage::Spread => String::new(),
        Slippage::Mid => ", mid".to_string(),
        Slippage::Liquidity {
            fill_ratio,
            ref_volume,
        } => format!(", liq(fr={fill_ratio:.2}, rv={ref_volume})"),
        Slippage::PerLeg { per_leg } => format!(", pleg({per_leg:.2})"),
        Slippage::BidAskTravel { pct } => format!(", bat({pct:.2})"),
    };
    format!(
        "{}(Δ{}, DTE {}, Exit {}{})",
        strategy_name,
        delta_str.join("/"),
        dte,
        exit_dte,
        slippage_suffix
    )
}

/// Compute dimension sensitivity: group results by each dimension value,
/// compute average Sharpe and `PnL`.
/// Dimensions covered: `strategy`, `entry_dte`, `exit_dte`, `slippage`, and per-leg delta targets.
pub fn compute_sensitivity(
    results: &[SweepResult],
) -> HashMap<String, HashMap<String, DimensionStats>> {
    let mut sensitivity: HashMap<String, HashMap<String, Vec<(f64, f64)>>> = HashMap::new();

    for r in results {
        // Strategy dimension
        sensitivity
            .entry("strategy".to_string())
            .or_default()
            .entry(r.strategy.clone())
            .or_default()
            .push((r.sharpe, r.pnl));

        // Entry DTE dimension
        sensitivity
            .entry("entry_dte".to_string())
            .or_default()
            .entry(r.entry_dte.target.to_string())
            .or_default()
            .push((r.sharpe, r.pnl));

        // Exit DTE dimension
        sensitivity
            .entry("exit_dte".to_string())
            .or_default()
            .entry(r.exit_dte.to_string())
            .or_default()
            .push((r.sharpe, r.pnl));

        // Slippage dimension
        let slippage_key = match &r.slippage {
            Slippage::Spread => "spread".to_string(),
            Slippage::Mid => "mid".to_string(),
            Slippage::Liquidity {
                fill_ratio,
                ref_volume,
            } => {
                format!("liquidity(fill_ratio={fill_ratio:.2},ref_volume={ref_volume})")
            }
            Slippage::PerLeg { per_leg } => format!("per_leg({per_leg:.2})"),
            Slippage::BidAskTravel { pct } => format!("bid_ask_travel({pct:.2})"),
        };
        sensitivity
            .entry("slippage".to_string())
            .or_default()
            .entry(slippage_key)
            .or_default()
            .push((r.sharpe, r.pnl));

        // Per-leg delta dimensions
        for (i, leg) in r.leg_deltas.iter().enumerate() {
            let dim_key = format!("leg_{}_delta", i + 1);
            let delta_key = format!("{:.2}", leg.target);
            sensitivity
                .entry(dim_key)
                .or_default()
                .entry(delta_key)
                .or_default()
                .push((r.sharpe, r.pnl));
        }

        // Signal dimensions (entry_signal, exit_signal)
        for (dim_name, dim_value) in &r.signal_dim_keys {
            sensitivity
                .entry(dim_name.clone())
                .or_default()
                .entry(dim_value.clone())
                .or_default()
                .push((r.sharpe, r.pnl));
        }
    }

    sensitivity
        .into_iter()
        .map(|(dim, values)| {
            let stats = values
                .into_iter()
                .map(|(key, pairs)| {
                    let count = pairs.len();
                    let avg_sharpe = pairs.iter().map(|(s, _)| s).sum::<f64>() / count as f64;
                    let avg_pnl = pairs.iter().map(|(_, p)| p).sum::<f64>() / count as f64;
                    (
                        key,
                        DimensionStats {
                            avg_sharpe,
                            avg_pnl,
                            count,
                        },
                    )
                })
                .collect();
            (dim, stats)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Parameter stability
// ---------------------------------------------------------------------------

/// Build a fingerprint key for a `SweepResult` that identifies its exact
/// parameter combination (strategy, leg deltas, entry DTE, exit DTE).
/// Signals and slippage are excluded from neighbor search (categorical).
fn stability_fingerprint(r: &SweepResult) -> (String, Vec<String>, i32, i32) {
    let delta_keys: Vec<String> = r
        .leg_deltas
        .iter()
        .map(|d| format!("{:.4}", d.target))
        .collect();
    (
        r.strategy.clone(),
        delta_keys,
        r.entry_dte.target,
        r.exit_dte,
    )
}

/// Compute parameter stability scores for the top results in a sweep.
///
/// For each top result, checks neighbors (results differing in exactly one
/// orderable dimension by one grid step) and measures how much Sharpe changes.
#[allow(clippy::too_many_lines)]
pub fn compute_stability(results: &[SweepResult], params: &SweepParams) -> Vec<StabilityScore> {
    if results.is_empty() {
        return vec![];
    }

    // Build sorted grids for orderable dimensions
    let entry_dte_grid: Vec<i32> = {
        let mut v = params.sweep.entry_dte_targets.clone();
        v.sort_unstable();
        v.dedup();
        v
    };
    let exit_dte_grid: Vec<i32> = {
        let mut v = params.sweep.exit_dtes.clone();
        v.sort_unstable();
        v.dedup();
        v
    };

    // Per-strategy sorted delta grids (one Vec<f64> per leg)
    let mut strategy_delta_grids: HashMap<String, Vec<Vec<f64>>> = HashMap::new();
    for strat in &params.strategies {
        let grids: Vec<Vec<f64>> = strat
            .leg_delta_targets
            .iter()
            .map(|vals| {
                let mut sorted = vals.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                sorted.dedup_by(|a, b| (*a - *b).abs() < 1e-9);
                sorted
            })
            .collect();
        strategy_delta_grids.insert(strat.name.clone(), grids);
    }

    // Build lookup: fingerprint → Sharpe
    let mut sharpe_map: HashMap<(String, Vec<String>, i32, i32), f64> = HashMap::new();
    for r in results {
        let fp = stability_fingerprint(r);
        // If multiple results share the same fingerprint (different slippage/signals),
        // keep the first (highest Sharpe since results are sorted).
        sharpe_map.entry(fp).or_insert(r.sharpe);
    }

    let top_n = results.len().min(5);
    let mut scores = Vec::with_capacity(top_n);

    for r in results.iter().take(top_n) {
        let fp = stability_fingerprint(r);
        let mut dim_stabilities = Vec::new();

        // Check leg delta dimensions
        if let Some(grids) = strategy_delta_grids.get(&r.strategy) {
            for (leg_idx, grid) in grids.iter().enumerate() {
                if grid.len() <= 1 {
                    continue;
                }
                let current_delta = r.leg_deltas[leg_idx].target;
                let pos = grid.iter().position(|&v| (v - current_delta).abs() < 1e-9);
                if let Some(pos) = pos {
                    let neighbors: Vec<f64> = [pos.wrapping_sub(1), pos + 1]
                        .iter()
                        .filter_map(|&idx| grid.get(idx).copied())
                        .collect();

                    let mut max_change = 0.0f64;
                    let mut neighbor_count = 0usize;
                    for neighbor_delta in &neighbors {
                        let mut neighbor_fp = fp.clone();
                        neighbor_fp.1[leg_idx] = format!("{neighbor_delta:.4}");
                        if let Some(&neighbor_sharpe) = sharpe_map.get(&neighbor_fp) {
                            let rel_change =
                                (neighbor_sharpe - r.sharpe).abs() / r.sharpe.abs().max(0.01);
                            max_change = max_change.max(rel_change);
                            neighbor_count += 1;
                        }
                    }

                    if neighbor_count > 0 {
                        dim_stabilities.push(DimensionStability {
                            dimension: format!("leg_{}_delta", leg_idx + 1),
                            score: 1.0 - max_change.clamp(0.0, 1.0),
                            max_sharpe_change: max_change,
                            neighbor_count,
                        });
                    }
                }
            }
        }

        // Check entry DTE dimension
        if entry_dte_grid.len() > 1 {
            let pos = entry_dte_grid.iter().position(|&v| v == r.entry_dte.target);
            if let Some(pos) = pos {
                let neighbors: Vec<i32> = [pos.wrapping_sub(1), pos + 1]
                    .iter()
                    .filter_map(|&idx| entry_dte_grid.get(idx).copied())
                    .collect();

                let mut max_change = 0.0f64;
                let mut neighbor_count = 0usize;
                for &neighbor_dte in &neighbors {
                    let neighbor_fp = (fp.0.clone(), fp.1.clone(), neighbor_dte, fp.3);
                    if let Some(&neighbor_sharpe) = sharpe_map.get(&neighbor_fp) {
                        let rel_change =
                            (neighbor_sharpe - r.sharpe).abs() / r.sharpe.abs().max(0.01);
                        max_change = max_change.max(rel_change);
                        neighbor_count += 1;
                    }
                }

                if neighbor_count > 0 {
                    dim_stabilities.push(DimensionStability {
                        dimension: "entry_dte".to_string(),
                        score: 1.0 - max_change.clamp(0.0, 1.0),
                        max_sharpe_change: max_change,
                        neighbor_count,
                    });
                }
            }
        }

        // Check exit DTE dimension
        if exit_dte_grid.len() > 1 {
            let pos = exit_dte_grid.iter().position(|&v| v == r.exit_dte);
            if let Some(pos) = pos {
                let neighbors: Vec<i32> = [pos.wrapping_sub(1), pos + 1]
                    .iter()
                    .filter_map(|&idx| exit_dte_grid.get(idx).copied())
                    .collect();

                let mut max_change = 0.0f64;
                let mut neighbor_count = 0usize;
                for &neighbor_exit in &neighbors {
                    let neighbor_fp = (fp.0.clone(), fp.1.clone(), fp.2, neighbor_exit);
                    if let Some(&neighbor_sharpe) = sharpe_map.get(&neighbor_fp) {
                        let rel_change =
                            (neighbor_sharpe - r.sharpe).abs() / r.sharpe.abs().max(0.01);
                        max_change = max_change.max(rel_change);
                        neighbor_count += 1;
                    }
                }

                if neighbor_count > 0 {
                    dim_stabilities.push(DimensionStability {
                        dimension: "exit_dte".to_string(),
                        score: 1.0 - max_change.clamp(0.0, 1.0),
                        max_sharpe_change: max_change,
                        neighbor_count,
                    });
                }
            }
        }

        // Compute overall score
        let (overall_score, warning) = if dim_stabilities.is_empty() {
            (
                1.0,
                Some("Single-point sweep — no neighbors to compare.".to_string()),
            )
        } else {
            let avg =
                dim_stabilities.iter().map(|d| d.score).sum::<f64>() / dim_stabilities.len() as f64;
            let warn = if avg < 0.5 {
                Some(format!(
                    "UNSTABLE — performance is fragile across neighboring parameters (score: {avg:.2})."
                ))
            } else if avg < 0.7 {
                Some(format!(
                    "CAUTION — moderate sensitivity to parameter changes (score: {avg:.2})."
                ))
            } else {
                None
            };
            (avg, warn)
        };

        scores.push(StabilityScore {
            label: r.label.clone(),
            overall_score,
            is_stable: overall_score >= 0.7,
            warning,
            per_dimension: dim_stabilities,
        });
    }

    scores
}
