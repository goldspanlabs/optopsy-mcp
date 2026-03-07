use std::collections::{HashMap, HashSet};

use anyhow::{bail, Result};
use polars::prelude::*;

use super::core::run_backtest;
use super::types::{
    BacktestParams, Direction, DteRange, ExpirationFilter, SimParams, Slippage, SweepResult,
    TargetRange,
};
use crate::data::parquet::QUOTE_DATETIME_COL;
use crate::engine::types::default_min_bid_ask;
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

/// Full sweep output
#[derive(Debug, Clone)]
pub struct SweepOutput {
    pub combinations_total: usize,
    pub combinations_run: usize,
    /// Pre-filter skips (delta ordering, deduplication)
    pub combinations_skipped: usize,
    /// Backtests that errored at runtime (after being selected to run)
    pub combinations_failed: usize,
    pub ranked_results: Vec<SweepResult>,
    pub dimension_sensitivity: HashMap<String, HashMap<String, DimensionStats>>,
    pub oos_results: Vec<OosResult>,
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
    }

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
                        let label = build_sweep_label(
                            &strat.name,
                            &leg_deltas,
                            dte_target,
                            exit_dte,
                            slippage,
                        );

                        // Build a full-precision dedup key to avoid false collisions
                        // from the rounded display label (e.g. 0.301 vs 0.302 → same label).
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
                            "{}|{}|{}|{}|{}",
                            strat.name,
                            delta_key.join(","),
                            dte_target,
                            exit_dte,
                            slippage_key,
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
                        });

                        // Enforce cap early to avoid materializing an enormous Vec
                        if combos.len() > 100 {
                            bail!(
                                "Parameter sweep exceeds the 100-combination limit (max 100). \
                                 Reduce delta targets, DTE values, exit_dtes, or slippage models \
                                 (currently: {} strategies, {} entry DTE values, {} exit DTE values, \
                                 {} slippage models).",
                                params.strategies.len(),
                                params.sweep.entry_dte_targets.len(),
                                params.sweep.exit_dtes.len(),
                                params.sweep.slippage_models.len(),
                            );
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
            entry_signal: params.sim_params.entry_signal.clone(),
            exit_signal: params.sim_params.exit_signal.clone(),
            ohlcv_path: params.sim_params.ohlcv_path.clone(),
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
                entry_signal: params.sim_params.entry_signal.clone(),
                exit_signal: params.sim_params.exit_signal.clone(),
                ohlcv_path: params.sim_params.ohlcv_path.clone(),
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

    Ok(SweepOutput {
        combinations_total: total,
        combinations_run,
        combinations_skipped: skipped,
        combinations_failed: failed,
        ranked_results: results,
        dimension_sensitivity,
        oos_results,
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
            super::super::types::TradeRecord {
                trade_id: 1,
                entry_datetime: dt1,
                exit_datetime: dt2,
                entry_cost: 100.0,
                exit_proceeds: 110.0,
                pnl: 10.0,
                days_held: 1,
                exit_type: super::super::types::ExitType::Expiration,
            },
            super::super::types::TradeRecord {
                trade_id: 2,
                entry_datetime: dt1, // same date as trade 1
                exit_datetime: dt3,
                entry_cost: 100.0,
                exit_proceeds: 90.0,
                pnl: -10.0,
                days_held: 2,
                exit_type: super::super::types::ExitType::Expiration,
            },
            super::super::types::TradeRecord {
                trade_id: 3,
                entry_datetime: dt2,
                exit_datetime: dt3,
                entry_cost: 100.0,
                exit_proceeds: 105.0,
                pnl: 5.0,
                days_held: 1,
                exit_type: super::super::types::ExitType::Expiration,
            },
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
            min_days_between_entries: None,
            exit_net_delta: None,
        };
        let params = SweepParams {
            strategies,
            sweep,
            sim_params,
            out_of_sample_pct: 0.0,
            direction: None,
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
            min_days_between_entries: None,
            exit_net_delta: None,
        };
        let params = SweepParams {
            strategies,
            sweep,
            sim_params,
            out_of_sample_pct: 0.0,
            direction: None,
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
}
