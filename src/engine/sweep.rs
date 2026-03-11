//! Parameter sweep orchestrator: enumerates strategy/delta/DTE/slippage/signal
//! combinations, runs backtests, ranks results, computes sensitivity and
//! stability scores, and optionally performs out-of-sample validation and
//! permutation-based multiple comparisons correction.

use std::collections::HashSet;

use anyhow::{bail, Result};
use polars::prelude::*;

use super::core::run_backtest;
use super::multiple_comparisons::{self, MultipleComparisonsResult};
use super::sweep_analysis::{build_signal_combos, build_sweep_label};
use super::types::{
    to_display_name, BacktestParams, DteRange, ExpirationFilter, SimParams, Slippage, SweepResult,
    TargetRange,
};
use crate::engine::permutation::{run_permutation_test, PermutationParams};
use crate::engine::types::default_min_bid_ask;
use crate::signals::registry::SignalSpec;

// Re-export everything from sweep_analysis so existing `crate::engine::sweep::X` imports still work
pub use super::sweep_analysis::{
    cartesian_product, compute_sensitivity, compute_stability, count_independent_entry_periods,
    delta_target_to_range, dte_target_to_range, split_by_date, violates_delta_ordering,
    DimensionStability, DimensionStats, OosResult, StabilityScore, SweepDimensions, SweepOutput,
    SweepParams, SweepStrategyEntry,
};

// ---------------------------------------------------------------------------
// Main orchestrator
// ---------------------------------------------------------------------------

/// Execute a parameter sweep over all strategy/delta/DTE/slippage/signal combinations.
///
/// Returns ranked results with sensitivity analysis, stability scores, and
/// optional out-of-sample validation and multiple comparisons correction.
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
        let delta_combos = super::sweep_analysis::cartesian_product(&strat.leg_delta_targets);

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

        let backtest_params = build_backtest_params_for_combo(
            combo.strategy_name.clone(),
            combo.leg_deltas.clone(),
            combo.entry_dte.clone(),
            combo.exit_dte,
            combo.slippage.clone(),
            entry_signal,
            exit_signal,
            &params.sim_params,
        );

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
                    p_value: None,
                    sizing: None,
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

            let backtest_params = build_backtest_params_for_combo(
                r.strategy.clone(),
                r.leg_deltas.clone(),
                r.entry_dte.clone(),
                r.exit_dte,
                r.slippage.clone(),
                entry_signal,
                exit_signal,
                &params.sim_params,
            );

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

    // 8. Optional: run permutation tests per combo and apply multiple comparisons correction
    let multiple_comparisons = run_multiple_comparisons(&train_df, &mut results, params);

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
        multiple_comparisons,
    })
}

// ---------------------------------------------------------------------------
// Multiple comparisons helper
// ---------------------------------------------------------------------------

/// Build a [`BacktestParams`] for a single sweep combination.
///
/// Centralises construction so that all three call sites (train run, OOS re-run,
/// permutation test) stay in sync if sweep defaults ever change.
#[allow(clippy::too_many_arguments)]
fn build_backtest_params_for_combo(
    strategy: String,
    leg_deltas: Vec<TargetRange>,
    entry_dte: DteRange,
    exit_dte: i32,
    slippage: Slippage,
    entry_signal: Option<SignalSpec>,
    exit_signal: Option<SignalSpec>,
    sim_params: &SimParams,
) -> BacktestParams {
    BacktestParams {
        strategy,
        leg_deltas,
        entry_dte,
        exit_dte,
        slippage,
        commission: None,
        min_bid_ask: default_min_bid_ask(),
        stop_loss: sim_params.stop_loss,
        take_profit: sim_params.take_profit,
        max_hold_days: sim_params.max_hold_days,
        capital: sim_params.capital,
        quantity: sim_params.quantity,
        sizing: sim_params.sizing.clone(),
        multiplier: sim_params.multiplier,
        max_positions: sim_params.max_positions,
        selector: sim_params.selector.clone(),
        adjustment_rules: vec![],
        entry_signal,
        exit_signal,
        ohlcv_path: sim_params.ohlcv_path.clone(),
        cross_ohlcv_paths: sim_params.cross_ohlcv_paths.clone(),
        min_net_premium: None,
        max_net_premium: None,
        min_net_delta: None,
        max_net_delta: None,
        min_days_between_entries: sim_params.min_days_between_entries,
        expiration_filter: ExpirationFilter::Any,
        exit_net_delta: sim_params.exit_net_delta,
    }
}

/// Run permutation tests for each sweep result (if `params.num_permutations` is set),
/// populate `p_value` on each result, and apply Bonferroni + BH-FDR corrections.
///
/// Always populates `result.p_value` when `num_permutations` is set (even for a single result).
/// Only returns a correction tuple when there are ≥2 results (multiple comparisons require ≥2 tests).
/// Returns `None` if permutation testing is not requested.
fn run_multiple_comparisons(
    train_df: &DataFrame,
    results: &mut [SweepResult],
    params: &SweepParams,
) -> Option<(MultipleComparisonsResult, MultipleComparisonsResult)> {
    let num_permutations = params.num_permutations?;

    let perm_params = PermutationParams {
        num_permutations,
        seed: params.permutation_seed,
    };

    // Always compute p-values when permutation testing is requested
    for result in results.iter_mut() {
        let entry_signal = result
            .entry_signal
            .clone()
            .or_else(|| params.sim_params.entry_signal.clone());
        let exit_signal = result
            .exit_signal
            .clone()
            .or_else(|| params.sim_params.exit_signal.clone());

        let backtest_params = build_backtest_params_for_combo(
            result.strategy.clone(),
            result.leg_deltas.clone(),
            result.entry_dte.clone(),
            result.exit_dte,
            result.slippage.clone(),
            entry_signal,
            exit_signal,
            &params.sim_params,
        );

        match run_permutation_test(
            train_df,
            &backtest_params,
            &perm_params,
            &None::<HashSet<chrono::NaiveDate>>,
            None::<&HashSet<chrono::NaiveDate>>,
        ) {
            Ok(output) => {
                let sharpe_p = output
                    .metric_results
                    .iter()
                    .find(|m| m.metric_name == "sharpe")
                    .map_or(1.0, |m| m.p_value);
                result.p_value = Some(sharpe_p);
            }
            Err(e) => {
                tracing::warn!(
                    "Permutation test for sweep combo '{}' failed: {e}",
                    result.label
                );
                result.p_value = Some(1.0);
            }
        }
    }

    // Multiple comparisons correction requires ≥2 tests
    if results.len() < 2 {
        return None;
    }

    // Collect labels and p-values
    let labels: Vec<String> = results.iter().map(|r| r.label.clone()).collect();
    let p_values: Vec<f64> = results.iter().map(|r| r.p_value.unwrap_or(1.0)).collect();

    let bon = multiple_comparisons::bonferroni(&labels, &p_values, 0.05);
    let bh = multiple_comparisons::benjamini_hochberg(&labels, &p_values, 0.05);
    Some((bon, bh))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::data::parquet::QUOTE_DATETIME_COL;
    use crate::engine::sweep_analysis::{
        build_signal_combos, cartesian_product, signal_spec_label,
    };
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
            crate::engine::types::TradeRecord::new(
                1,
                dt1,
                dt2,
                100.0,
                110.0,
                10.0,
                1,
                crate::engine::types::ExitType::Expiration,
                vec![],
            ),
            crate::engine::types::TradeRecord::new(
                2,
                dt1, // same date as trade 1
                dt3,
                100.0,
                90.0,
                -10.0,
                2,
                crate::engine::types::ExitType::Expiration,
                vec![],
            ),
            crate::engine::types::TradeRecord::new(
                3,
                dt2,
                dt3,
                100.0,
                105.0,
                5.0,
                1,
                crate::engine::types::ExitType::Expiration,
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
            sizing: None,
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
            num_permutations: None,
            permutation_seed: None,
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
            sizing: None,
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
            num_permutations: None,
            permutation_seed: None,
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
            sizing: None,
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
            num_permutations: None,
            permutation_seed: None,
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
            p_value: None,
            sizing: None,
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
                sizing: None,
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
            num_permutations: None,
            permutation_seed: None,
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
