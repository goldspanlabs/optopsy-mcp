use std::collections::{HashMap, HashSet};

use anyhow::{bail, Result};
use polars::prelude::*;

use super::core::run_backtest;
use super::types::{
    to_display_name, BacktestParams, Direction, DteRange, ExpirationFilter, SimParams, Slippage,
    SweepResult, TargetRange,
};
use crate::data::parquet::QUOTE_DATETIME_COL;
use crate::engine::types::default_min_bid_ask;
use crate::signals::registry::SignalSpec;
use crate::strategies;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Per-strategy delta sweep spec
#[derive(Debug, Clone)]
pub struct SweepStrategyEntry {
    pub name: String,
    pub leg_delta_targets: Vec<Vec<f64>>,
}

/// Shared sweep dimensions
#[derive(Debug, Clone)]
pub struct SweepDimensions {
    pub entry_dte_targets: Vec<i32>,
    pub exit_dtes: Vec<i32>,
    pub slippage_models: Vec<Slippage>,
}

/// Full sweep input
#[derive(Debug, Clone)]
pub struct SweepParams {
    pub strategies: Vec<SweepStrategyEntry>,
    pub sweep: SweepDimensions,
    pub sim_params: SimParams,
    pub out_of_sample_pct: f64,
    pub direction: Option<Direction>,
    /// Entry signal variants to sweep. Empty = use `sim_params.entry_signal` as-is.
    pub entry_signals: Vec<SignalSpec>,
    /// Exit signal variants to sweep. Empty = use `sim_params.exit_signal` as-is.
    pub exit_signals: Vec<SignalSpec>,
}

/// Per-dimension-value averages
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct DimensionStats {
    pub avg_sharpe: f64,
    pub avg_pnl: f64,
    pub count: usize,
}

/// OOS validation row
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct OosResult {
    pub label: String,
    pub train_sharpe: f64,
    pub test_sharpe: f64,
    pub train_pnl: f64,
    pub test_pnl: f64,
}

/// Per-dimension stability for a single top result
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct DimensionStability {
    pub dimension: String,
    pub score: f64,
    pub max_sharpe_change: f64,
    pub neighbor_count: usize,
}

/// Stability score for a single top result
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct StabilityScore {
    pub label: String,
    pub overall_score: f64,
    pub is_stable: bool,
    pub warning: Option<String>,
    pub per_dimension: Vec<DimensionStability>,
}

/// Full sweep output
#[derive(Debug, Clone)]
pub struct SweepOutput {
    pub combinations_total: usize,
    pub combinations_run: usize,
    /// Pre-filter skips (delta ordering, deduplication)
    pub combinations_skipped: usize,
    /// Backtests that errored at runtime (after being selected to run)
    pub combinations_failed: usize,
    /// Number of signal combinations swept (entry × exit), if > 1
    pub signal_combinations: Option<usize>,
    pub ranked_results: Vec<SweepResult>,
    pub dimension_sensitivity: HashMap<String, HashMap<String, DimensionStats>>,
    pub oos_results: Vec<OosResult>,
    pub stability_scores: Vec<StabilityScore>,
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
fn signal_spec_label(spec: &SignalSpec) -> String {
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
struct SignalCombo {
    entry: Option<SignalSpec>,
    exit: Option<SignalSpec>,
    /// Human-readable label for display (may not be unique for complex signals)
    label: String,
    /// Full-precision key for deduplication (uses `Debug` representation)
    dedup_key: String,
    dim_keys: Vec<(String, String)>,
}

/// Build the cartesian product of entry × exit signal lists.
/// If a list is empty, use `None` (1 variant) for that slot.
fn build_signal_combos(
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
fn build_sweep_label(
    strategy_name: &str,
    deltas: &[TargetRange],
    dte: i32,
    exit_dte: i32,
    slippage: &Slippage,
) -> String {
    let delta_str: Vec<String> = deltas.iter().map(|d| format!("{:.2}", d.target)).collect();
    let slippage_suffix = match slippage {
        Slippage::Spread => String::new(),
        Slippage::Mid => ",mid".to_string(),
        Slippage::Liquidity {
            fill_ratio,
            ref_volume,
        } => format!(",liq(fr={fill_ratio:.2},rv={ref_volume})"),
        Slippage::PerLeg { per_leg } => format!(",pleg({per_leg:.2})"),
        Slippage::BidAskTravel { pct } => format!(",bat({pct:.2})"),
    };
    format!(
        "{}(Δ{},DTE{},exit{}{})",
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

// ---------------------------------------------------------------------------
// Main orchestrator
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
pub fn run_sweep(df: &DataFrame, params: &SweepParams) -> Result<SweepOutput> {
    // 1. Build all combinations
    struct Combo {
        strategy_name: String,
        leg_deltas: Vec<TargetRange>,
        entry_dte: DteRange,
        exit_dte: i32,
        slippage: Slippage,
        label: String,
        entry_signal: Option<SignalSpec>,
        exit_signal: Option<SignalSpec>,
        signal_dim_keys: Vec<(String, String)>,
    }

    // Build signal combos (cartesian product of entry × exit signal lists)
    let signal_combos = build_signal_combos(&params.entry_signals, &params.exit_signals);
    let signal_count = signal_combos.len();

    let mut combos: Vec<Combo> = Vec::new();
    let mut seen_dedup_keys: HashSet<String> = HashSet::new();
    let mut skipped = 0usize;

    for strat in &params.strategies {
        let delta_combos = cartesian_product(&strat.leg_delta_targets);

        for delta_targets in &delta_combos {
            // Pre-filter: check delta ordering
            if violates_delta_ordering(&strat.name, delta_targets) {
                skipped += 1;
                continue;
            }

            let leg_deltas: Vec<TargetRange> = delta_targets
                .iter()
                .map(|&d| delta_target_to_range(d))
                .collect();

            for &dte_target in &params.sweep.entry_dte_targets {
                let entry_dte = dte_target_to_range(dte_target);

                for &exit_dte in &params.sweep.exit_dtes {
                    // Filter: exit_dte must be < entry_dte.min
                    if exit_dte >= entry_dte.min {
                        skipped += 1;
                        continue;
                    }

                    for slippage in &params.sweep.slippage_models {
                        for sig_combo in &signal_combos {
                            let base_label = build_sweep_label(
                                &strat.name,
                                &leg_deltas,
                                dte_target,
                                exit_dte,
                                slippage,
                            );
                            let label = if sig_combo.label.is_empty() {
                                base_label
                            } else {
                                format!("{base_label}{}", sig_combo.label)
                            };

                            // Build a full-precision dedup key to avoid false collisions
                            let delta_key: Vec<String> = leg_deltas
                                .iter()
                                .map(|d| format!("{:.6}:{:.6}:{:.6}", d.target, d.min, d.max))
                                .collect();
                            let slippage_key = match slippage {
                                Slippage::Spread => "spread".to_string(),
                                Slippage::Mid => "mid".to_string(),
                                Slippage::Liquidity {
                                    fill_ratio,
                                    ref_volume,
                                } => format!("liq:{fill_ratio:.6}:{ref_volume}"),
                                Slippage::PerLeg { per_leg } => format!("pleg:{per_leg:.6}"),
                                Slippage::BidAskTravel { pct } => format!("bat:{pct:.6}"),
                            };
                            let dedup_key = format!(
                                "{}|{}|{}|{}|{}|{}",
                                strat.name,
                                delta_key.join(","),
                                dte_target,
                                exit_dte,
                                slippage_key,
                                sig_combo.dedup_key,
                            );

                            // Deduplicate on the precise key; keep label only for display
                            if !seen_dedup_keys.insert(dedup_key) {
                                skipped += 1;
                                continue;
                            }

                            combos.push(Combo {
                                strategy_name: strat.name.clone(),
                                leg_deltas: leg_deltas.clone(),
                                entry_dte: entry_dte.clone(),
                                exit_dte,
                                slippage: slippage.clone(),
                                label,
                                entry_signal: sig_combo.entry.clone(),
                                exit_signal: sig_combo.exit.clone(),
                                signal_dim_keys: sig_combo.dim_keys.clone(),
                            });

                            // Enforce cap early to avoid materializing an enormous Vec
                            if combos.len() > 100 {
                                bail!(
                                    "Parameter sweep exceeds the 100-combination limit (max 100). \
                                     Reduce delta targets, DTE values, exit_dtes, slippage models, \
                                     or signal variants \
                                     (currently: {} strategies, {} entry DTE values, {} exit DTE values, \
                                     {} slippage models, {} signal combos).",
                                    params.strategies.len(),
                                    params.sweep.entry_dte_targets.len(),
                                    params.sweep.exit_dtes.len(),
                                    params.sweep.slippage_models.len(),
                                    signal_count,
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    let total = combos.len() + skipped;

    // 2. Split data if OOS enabled
    let (train_df, test_df) = if params.out_of_sample_pct > 0.0 {
        let (train, test) = split_by_date(df, params.out_of_sample_pct)?;
        (train, Some(test))
    } else {
        (df.clone(), None)
    };

    // 3. Run backtests on training set
    let mut results: Vec<SweepResult> = Vec::new();
    let mut failed: usize = 0;

    for combo in &combos {
        // Use per-combo signals if signal sweep is active; otherwise fall back to sim_params
        let entry_signal = combo
            .entry_signal
            .clone()
            .or_else(|| params.sim_params.entry_signal.clone());
        let exit_signal = combo
            .exit_signal
            .clone()
            .or_else(|| params.sim_params.exit_signal.clone());

        let backtest_params = BacktestParams {
            strategy: combo.strategy_name.clone(),
            leg_deltas: combo.leg_deltas.clone(),
            entry_dte: combo.entry_dte.clone(),
            exit_dte: combo.exit_dte,
            slippage: combo.slippage.clone(),
            commission: None,
            min_bid_ask: default_min_bid_ask(),
            stop_loss: params.sim_params.stop_loss,
            take_profit: params.sim_params.take_profit,
            max_hold_days: params.sim_params.max_hold_days,
            capital: params.sim_params.capital,
            quantity: params.sim_params.quantity,
            multiplier: params.sim_params.multiplier,
            max_positions: params.sim_params.max_positions,
            selector: params.sim_params.selector.clone(),
            adjustment_rules: vec![],
            entry_signal,
            exit_signal,
            ohlcv_path: params.sim_params.ohlcv_path.clone(),
            cross_ohlcv_paths: params.sim_params.cross_ohlcv_paths.clone(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: params.sim_params.min_days_between_entries,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: params.sim_params.exit_net_delta,
        };

        match run_backtest(&train_df, &backtest_params) {
            Ok(bt) => {
                let independent_periods = count_independent_entry_periods(&bt.trade_log);
                results.push(SweepResult {
                    label: combo.label.clone(),
                    strategy: combo.strategy_name.clone(),
                    display_name: to_display_name(&combo.strategy_name),
                    leg_deltas: combo.leg_deltas.clone(),
                    entry_dte: combo.entry_dte.clone(),
                    exit_dte: combo.exit_dte,
                    slippage: combo.slippage.clone(),
                    trades: bt.trade_count,
                    pnl: bt.total_pnl,
                    sharpe: bt.metrics.sharpe,
                    sortino: bt.metrics.sortino,
                    max_dd: bt.metrics.max_drawdown,
                    win_rate: bt.metrics.win_rate,
                    profit_factor: bt.metrics.profit_factor,
                    calmar: bt.metrics.calmar,
                    total_return_pct: bt.metrics.total_return_pct,
                    independent_entry_periods: independent_periods,
                    entry_signal: combo.entry_signal.clone(),
                    exit_signal: combo.exit_signal.clone(),
                    signal_dim_keys: combo.signal_dim_keys.clone(),
                });
            }
            Err(e) => {
                failed += 1;
                tracing::warn!("Sweep combo '{}' failed: {e}", combo.label);
            }
        }
    }

    // 4. Sort by Sharpe descending
    results.sort_by(|a, b| {
        b.sharpe
            .partial_cmp(&a.sharpe)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // 5. Compute dimension sensitivity
    let dimension_sensitivity = compute_sensitivity(&results);

    // 6. OOS validation on top 3
    let mut oos_results = Vec::new();
    if let Some(ref test_df) = test_df {
        let top_n = results.len().min(3);
        for r in results.iter().take(top_n) {
            // Use per-result signals for OOS re-run (may differ across combos)
            let entry_signal = r
                .entry_signal
                .clone()
                .or_else(|| params.sim_params.entry_signal.clone());
            let exit_signal = r
                .exit_signal
                .clone()
                .or_else(|| params.sim_params.exit_signal.clone());

            let backtest_params = BacktestParams {
                strategy: r.strategy.clone(),
                leg_deltas: r.leg_deltas.clone(),
                entry_dte: r.entry_dte.clone(),
                exit_dte: r.exit_dte,
                slippage: r.slippage.clone(),
                commission: None,
                min_bid_ask: default_min_bid_ask(),
                stop_loss: params.sim_params.stop_loss,
                take_profit: params.sim_params.take_profit,
                max_hold_days: params.sim_params.max_hold_days,
                capital: params.sim_params.capital,
                quantity: params.sim_params.quantity,
                multiplier: params.sim_params.multiplier,
                max_positions: params.sim_params.max_positions,
                selector: params.sim_params.selector.clone(),
                adjustment_rules: vec![],
                entry_signal,
                exit_signal,
                ohlcv_path: params.sim_params.ohlcv_path.clone(),
                cross_ohlcv_paths: params.sim_params.cross_ohlcv_paths.clone(),
                min_net_premium: None,
                max_net_premium: None,
                min_net_delta: None,
                max_net_delta: None,
                min_days_between_entries: params.sim_params.min_days_between_entries,
                expiration_filter: ExpirationFilter::Any,
                exit_net_delta: params.sim_params.exit_net_delta,
            };

            match run_backtest(test_df, &backtest_params) {
                Ok(test_bt) => {
                    oos_results.push(OosResult {
                        label: r.label.clone(),
                        train_sharpe: r.sharpe,
                        test_sharpe: test_bt.metrics.sharpe,
                        train_pnl: r.pnl,
                        test_pnl: test_bt.total_pnl,
                    });
                }
                Err(e) => {
                    tracing::warn!("OOS validation for '{}' failed: {e}", r.label);
                }
            }
        }
    }

    let combinations_run = results.len();

    // 7. Compute parameter stability for top results
    let stability_scores = compute_stability(&results, params);

    Ok(SweepOutput {
        combinations_total: total,
        combinations_run,
        combinations_skipped: skipped,
        combinations_failed: failed,
        signal_combinations: if signal_count > 1 {
            Some(signal_count)
        } else {
            None
        },
        ranked_results: results,
        dimension_sensitivity,
        oos_results,
        stability_scores,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::parquet::QUOTE_DATETIME_COL;
    use crate::engine::types::TradeSelector;
    use chrono::NaiveDate;

    #[test]
    fn cartesian_product_single() {
        let result = cartesian_product(&[vec![1.0, 2.0]]);
        assert_eq!(result, vec![vec![1.0], vec![2.0]]);
    }

    #[test]
    fn cartesian_product_multi() {
        let result = cartesian_product(&[vec![1.0, 2.0], vec![3.0, 4.0]]);
        assert_eq!(
            result,
            vec![
                vec![1.0, 3.0],
                vec![1.0, 4.0],
                vec![2.0, 3.0],
                vec![2.0, 4.0],
            ]
        );
    }

    #[test]
    fn cartesian_product_empty() {
        let result = cartesian_product(&[vec![1.0], vec![]]);
        assert!(result.is_empty());
    }

    #[test]
    fn cartesian_product_no_arrays() {
        let result = cartesian_product(&[]);
        let expected: Vec<Vec<f64>> = vec![vec![]];
        assert_eq!(result, expected);
    }

    #[test]
    fn delta_target_to_range_normal() {
        let r = delta_target_to_range(0.30);
        assert!((r.target - 0.30).abs() < 1e-10);
        assert!((r.min - 0.25).abs() < 1e-10);
        assert!((r.max - 0.35).abs() < 1e-10);
    }

    #[test]
    fn delta_target_to_range_clamped_low() {
        let r = delta_target_to_range(0.02);
        assert!((r.target - 0.02).abs() < 1e-10);
        assert!((r.min - 0.01).abs() < 1e-10);
        assert!((r.max - 0.07).abs() < 1e-10);
    }

    #[test]
    fn delta_target_to_range_clamped_high() {
        let r = delta_target_to_range(0.98);
        assert!((r.target - 0.98).abs() < 1e-10);
        assert!((r.min - 0.93).abs() < 1e-10);
        assert!((r.max - 0.99).abs() < 1e-10);
    }

    #[test]
    fn dte_target_to_range_normal() {
        let r = dte_target_to_range(45);
        assert_eq!(r.target, 45);
        assert_eq!(r.min, 45 - 14); // round(45*0.3) = 14
        assert_eq!(r.max, 45 + 14);
    }

    #[test]
    fn dte_target_to_range_small() {
        let r = dte_target_to_range(2);
        assert_eq!(r.target, 2);
        assert_eq!(r.min, 1); // 2 - 1 = 1, clamped to 1
        assert_eq!(r.max, 3); // 2 + 1
    }

    #[test]
    fn exit_dte_filtering() {
        // exit_dte=30, entry_dte_range min=31 → valid (30 < 31)
        let entry = dte_target_to_range(45); // min=31
        assert!(30 < entry.min, "30 should be < entry.min={}", entry.min);

        // exit_dte=35 should be invalid for entry_dte_target=45 (min=31)
        assert!(35 >= entry.min);
    }

    #[test]
    fn violates_delta_ordering_inverted() {
        // bull_call_spread defaults: [0.50, 0.10] (leg0 > leg1)
        assert!(violates_delta_ordering("bull_call_spread", &[0.10, 0.50]));
    }

    #[test]
    fn violates_delta_ordering_valid() {
        // Same ordering as defaults: leg0 > leg1
        assert!(!violates_delta_ordering("bull_call_spread", &[0.50, 0.10]));
    }

    #[test]
    fn violates_delta_ordering_single_leg() {
        // Single-leg strategies always pass
        assert!(!violates_delta_ordering("long_call", &[0.30]));
    }

    #[test]
    fn violates_delta_ordering_unknown_strategy() {
        assert!(!violates_delta_ordering("nonexistent", &[0.30, 0.10]));
    }

    #[test]
    fn count_independent_entry_periods_distinct() {
        use chrono::NaiveDateTime;
        let dt1 =
            NaiveDateTime::parse_from_str("2024-01-15 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let dt2 =
            NaiveDateTime::parse_from_str("2024-01-16 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let dt3 =
            NaiveDateTime::parse_from_str("2024-01-17 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let trades = vec![
            super::super::types::TradeRecord::new(
                1,
                dt1,
                dt2,
                100.0,
                110.0,
                10.0,
                1,
                super::super::types::ExitType::Expiration,
                vec![],
            ),
            super::super::types::TradeRecord::new(
                2,
                dt1, // same date as trade 1
                dt3,
                100.0,
                90.0,
                -10.0,
                2,
                super::super::types::ExitType::Expiration,
                vec![],
            ),
            super::super::types::TradeRecord::new(
                3,
                dt2,
                dt3,
                100.0,
                105.0,
                5.0,
                1,
                super::super::types::ExitType::Expiration,
                vec![],
            ),
        ];
        assert_eq!(count_independent_entry_periods(&trades), 2);
    }

    #[test]
    fn combination_cap_error() {
        // Build params that would exceed 100 combos
        let strategies = vec![SweepStrategyEntry {
            name: "long_call".to_string(),
            leg_delta_targets: vec![vec![
                0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60,
            ]],
        }];
        let sweep = SweepDimensions {
            entry_dte_targets: vec![30, 45, 60],
            exit_dtes: vec![0, 5, 10, 15],
            slippage_models: vec![Slippage::Mid],
        };
        let sim_params = SimParams {
            capital: 10000.0,
            quantity: 1,
            multiplier: 100,
            max_positions: 3,
            selector: TradeSelector::default(),
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_days_between_entries: None,
            exit_net_delta: None,
        };
        let params = SweepParams {
            strategies,
            sweep,
            sim_params,
            out_of_sample_pct: 0.0,
            direction: None,
            entry_signals: vec![],
            exit_signals: vec![],
        };

        // Use a minimal DataFrame
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let qt = date.and_hms_opt(0, 0, 0).unwrap();
        let mut df = df! {
            QUOTE_DATETIME_COL => &[qt],
            "option_type" => &["call"],
            "strike" => &[100.0f64],
            "bid" => &[5.0f64],
            "ask" => &[5.5f64],
            "delta" => &[0.5f64],
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), vec![exp]).into_column(),
        )
        .unwrap();

        let result = run_sweep(&df, &params);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("max 100"));
    }

    #[test]
    fn deduplication_collapses_identical() {
        // Two identical strategy entries should produce only one combo
        let strategies = vec![
            SweepStrategyEntry {
                name: "long_call".to_string(),
                leg_delta_targets: vec![vec![0.30]],
            },
            SweepStrategyEntry {
                name: "long_call".to_string(),
                leg_delta_targets: vec![vec![0.30]],
            },
        ];
        let sweep = SweepDimensions {
            entry_dte_targets: vec![45],
            exit_dtes: vec![0],
            slippage_models: vec![Slippage::Spread],
        };
        let sim_params = SimParams {
            capital: 10000.0,
            quantity: 1,
            multiplier: 100,
            max_positions: 3,
            selector: TradeSelector::default(),
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_days_between_entries: None,
            exit_net_delta: None,
        };
        let params = SweepParams {
            strategies,
            sweep,
            sim_params,
            out_of_sample_pct: 0.0,
            direction: None,
            entry_signals: vec![],
            exit_signals: vec![],
        };

        // Use minimal df
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let qt = date.and_hms_opt(0, 0, 0).unwrap();
        let mut df = df! {
            QUOTE_DATETIME_COL => &[qt],
            "option_type" => &["call"],
            "strike" => &[100.0f64],
            "bid" => &[5.0f64],
            "ask" => &[5.5f64],
            "delta" => &[0.5f64],
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), vec![exp]).into_column(),
        )
        .unwrap();

        let result = run_sweep(&df, &params).unwrap();
        // Should have 1 skipped duplicate, leaving 1 combo run (or 0 if no trades)
        assert!(result.combinations_skipped >= 1);
    }

    #[test]
    fn split_by_date_correct() {
        let dates: Vec<_> = (1..=10)
            .map(|d| {
                NaiveDate::from_ymd_opt(2024, 1, d)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
            })
            .collect();

        let df = df! {
            QUOTE_DATETIME_COL => &dates,
            "value" => &(1..=10).collect::<Vec<i32>>(),
        }
        .unwrap();

        let (train, test) = split_by_date(&df, 0.3).unwrap();
        assert_eq!(train.height(), 7);
        assert_eq!(test.height(), 3);
    }

    // --- Signal sweep tests ---

    #[test]
    fn signal_combos_entry_only() {
        let entries = vec![
            SignalSpec::RsiBelow {
                column: "close".into(),
                threshold: 25.0,
            },
            SignalSpec::RsiBelow {
                column: "close".into(),
                threshold: 30.0,
            },
            SignalSpec::RsiBelow {
                column: "close".into(),
                threshold: 35.0,
            },
        ];
        let combos = build_signal_combos(&entries, &[]);
        assert_eq!(combos.len(), 3);
        for c in &combos {
            assert!(c.entry.is_some());
            assert!(c.exit.is_none());
            assert!(!c.label.is_empty());
            assert_eq!(c.dim_keys.len(), 1);
            assert_eq!(c.dim_keys[0].0, "entry_signal");
        }
    }

    #[test]
    fn signal_combos_entry_and_exit() {
        let entries = vec![
            SignalSpec::RsiBelow {
                column: "close".into(),
                threshold: 25.0,
            },
            SignalSpec::RsiBelow {
                column: "close".into(),
                threshold: 30.0,
            },
            SignalSpec::RsiBelow {
                column: "close".into(),
                threshold: 35.0,
            },
        ];
        let exits = vec![
            SignalSpec::ConsecutiveDown {
                column: "close".into(),
                count: 2,
            },
            SignalSpec::ConsecutiveDown {
                column: "close".into(),
                count: 3,
            },
        ];
        let combos = build_signal_combos(&entries, &exits);
        assert_eq!(combos.len(), 6); // 3 × 2
        for c in &combos {
            assert!(c.entry.is_some());
            assert!(c.exit.is_some());
            assert_eq!(c.dim_keys.len(), 2);
        }
    }

    #[test]
    fn signal_combos_empty_lists() {
        let combos = build_signal_combos(&[], &[]);
        assert_eq!(combos.len(), 1);
        assert!(combos[0].entry.is_none());
        assert!(combos[0].exit.is_none());
        assert!(combos[0].label.is_empty());
        assert!(combos[0].dim_keys.is_empty());
    }

    #[test]
    fn sweep_with_signals_multiplies_combos() {
        // 1 strategy × 1 delta × 1 DTE × 1 exit × 1 slippage × 3 signals = 3 combos
        let strategies = vec![SweepStrategyEntry {
            name: "long_call".to_string(),
            leg_delta_targets: vec![vec![0.50]],
        }];
        let sweep = SweepDimensions {
            entry_dte_targets: vec![45],
            exit_dtes: vec![0],
            slippage_models: vec![Slippage::Mid],
        };
        let sim_params = SimParams {
            capital: 10000.0,
            quantity: 1,
            multiplier: 100,
            max_positions: 3,
            selector: TradeSelector::default(),
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_days_between_entries: None,
            exit_net_delta: None,
        };
        let params = SweepParams {
            strategies,
            sweep,
            sim_params,
            out_of_sample_pct: 0.0,
            direction: None,
            entry_signals: vec![
                SignalSpec::RsiBelow {
                    column: "close".into(),
                    threshold: 25.0,
                },
                SignalSpec::RsiBelow {
                    column: "close".into(),
                    threshold: 30.0,
                },
                SignalSpec::RsiBelow {
                    column: "close".into(),
                    threshold: 35.0,
                },
            ],
            exit_signals: vec![],
        };

        // Use a minimal DataFrame
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let qt = date.and_hms_opt(0, 0, 0).unwrap();
        let mut df = df! {
            QUOTE_DATETIME_COL => &[qt],
            "option_type" => &["call"],
            "strike" => &[100.0f64],
            "bid" => &[5.0f64],
            "ask" => &[5.5f64],
            "delta" => &[0.5f64],
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), vec![exp]).into_column(),
        )
        .unwrap();

        let output = run_sweep(&df, &params).unwrap();
        // Total should be 3 (1×1×1×1×3), though they may all fail (no ohlcv)
        assert_eq!(output.combinations_total, 3);
        assert_eq!(output.signal_combinations, Some(3));
    }

    #[test]
    fn signal_spec_label_covers_common_variants() {
        assert_eq!(
            signal_spec_label(&SignalSpec::RsiBelow {
                column: "close".into(),
                threshold: 25.0,
            }),
            "RsiBelow(t=25)"
        );
        assert_eq!(
            signal_spec_label(&SignalSpec::SmaCrossover {
                column: "close".into(),
                fast_period: 10,
                slow_period: 20,
            }),
            "SmaCross(f=10,s=20)"
        );
        assert_eq!(
            signal_spec_label(&SignalSpec::ConsecutiveUp {
                column: "close".into(),
                count: 3,
            }),
            "ConsecUp(n=3)"
        );
    }

    // --- compute_stability tests ---

    fn make_sweep_result(
        strategy: &str,
        delta_target: f64,
        entry_dte: i32,
        exit_dte: i32,
        sharpe: f64,
    ) -> SweepResult {
        SweepResult {
            label: format!("{strategy}(Δ{delta_target:.2},DTE{entry_dte},exit{exit_dte})"),
            strategy: strategy.to_string(),
            display_name: to_display_name(strategy),
            leg_deltas: vec![delta_target_to_range(delta_target)],
            entry_dte: dte_target_to_range(entry_dte),
            exit_dte,
            slippage: Slippage::Mid,
            trades: 10,
            pnl: sharpe * 100.0,
            sharpe,
            sortino: sharpe * 1.2,
            max_dd: 0.05,
            win_rate: 0.6,
            profit_factor: 1.5,
            calmar: 1.0,
            total_return_pct: 5.0,
            independent_entry_periods: 8,
            entry_signal: None,
            exit_signal: None,
            signal_dim_keys: vec![],
        }
    }

    fn make_sweep_params(
        strategy: &str,
        deltas: Vec<f64>,
        entry_dtes: Vec<i32>,
        exit_dtes: Vec<i32>,
    ) -> SweepParams {
        SweepParams {
            strategies: vec![SweepStrategyEntry {
                name: strategy.to_string(),
                leg_delta_targets: vec![deltas],
            }],
            sweep: SweepDimensions {
                entry_dte_targets: entry_dtes,
                exit_dtes,
                slippage_models: vec![Slippage::Mid],
            },
            sim_params: SimParams {
                capital: 10000.0,
                quantity: 1,
                multiplier: 100,
                max_positions: 5,
                selector: TradeSelector::Nearest,
                stop_loss: None,
                take_profit: None,
                max_hold_days: None,
                entry_signal: None,
                exit_signal: None,
                ohlcv_path: None,
                cross_ohlcv_paths: HashMap::new(),
                min_days_between_entries: None,
                exit_net_delta: None,
            },
            out_of_sample_pct: 0.0,
            direction: None,
            entry_signals: vec![],
            exit_signals: vec![],
        }
    }

    #[test]
    fn stability_all_neighbors_same_sharpe() {
        let results = vec![
            make_sweep_result("long_call", 0.30, 45, 0, 1.0),
            make_sweep_result("long_call", 0.40, 45, 0, 1.0),
            make_sweep_result("long_call", 0.50, 45, 0, 1.0),
        ];
        let params = make_sweep_params("long_call", vec![0.30, 0.40, 0.50], vec![45], vec![0]);

        let scores = compute_stability(&results, &params);
        assert_eq!(scores.len(), 3);
        assert!((scores[0].overall_score - 1.0).abs() < 1e-10);
        assert!(scores[0].is_stable);
        assert!(scores[0].warning.is_none());
    }

    #[test]
    fn stability_sharp_drop_in_neighbor() {
        let results = vec![
            make_sweep_result("long_call", 0.40, 45, 0, 2.0),
            make_sweep_result("long_call", 0.30, 45, 0, 0.1),
            make_sweep_result("long_call", 0.50, 45, 0, 0.1),
        ];
        let params = make_sweep_params("long_call", vec![0.30, 0.40, 0.50], vec![45], vec![0]);

        let scores = compute_stability(&results, &params);
        assert!(!scores.is_empty());
        // Top result (Sharpe 2.0 at 0.40) has neighbors at 0.1 → relative change = 1.9/2.0 = 0.95
        let top = &scores[0];
        assert!(
            top.overall_score < 0.5,
            "Expected unstable, got {}",
            top.overall_score
        );
        assert!(!top.is_stable);
        assert!(top.warning.is_some());
        assert!(top.warning.as_ref().unwrap().contains("UNSTABLE"));
    }

    #[test]
    fn stability_single_value_dimension_excluded() {
        // Only one delta value → no neighbors possible → score 1.0 with note
        let results = vec![make_sweep_result("long_call", 0.30, 45, 0, 1.5)];
        let params = make_sweep_params("long_call", vec![0.30], vec![45], vec![0]);

        let scores = compute_stability(&results, &params);
        assert_eq!(scores.len(), 1);
        assert!((scores[0].overall_score - 1.0).abs() < 1e-10);
        assert!(scores[0].warning.is_some());
        assert!(scores[0].warning.as_ref().unwrap().contains("Single-point"));
    }

    #[test]
    fn stability_empty_results() {
        let params = make_sweep_params("long_call", vec![0.30], vec![45], vec![0]);
        let scores = compute_stability(&[], &params);
        assert!(scores.is_empty());
    }

    #[test]
    fn stability_multi_dimension() {
        // Sweep across deltas AND entry DTEs
        let results = vec![
            make_sweep_result("long_call", 0.30, 30, 0, 1.0),
            make_sweep_result("long_call", 0.30, 45, 0, 1.0),
            make_sweep_result("long_call", 0.40, 30, 0, 1.0),
            make_sweep_result("long_call", 0.40, 45, 0, 1.0),
        ];
        let params = make_sweep_params("long_call", vec![0.30, 0.40], vec![30, 45], vec![0]);

        let scores = compute_stability(&results, &params);
        assert!(!scores.is_empty());
        // All same Sharpe → all stable
        assert!((scores[0].overall_score - 1.0).abs() < 1e-10);
        // Should have 2 per-dimension entries (leg_1_delta + entry_dte)
        assert_eq!(scores[0].per_dimension.len(), 2);
    }
}
