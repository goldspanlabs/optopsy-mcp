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
        mode: None,
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
// Stock sweep
// ---------------------------------------------------------------------------

use super::stock_sim::{self, StockBacktestParams};
use super::types::{Interval, SessionFilter, Side};

/// A single stock sweep combination descriptor.
struct StockCombo {
    label: String,
    entry_signal: SignalSpec,
    exit_signal: Option<SignalSpec>,
    interval: Interval,
    side: Side,
    session_filter: Option<SessionFilter>,
    stop_loss: Option<f64>,
    take_profit: Option<f64>,
    slippage: Slippage,
    /// Dimension key pairs for sensitivity analysis.
    dim_keys: Vec<(String, String)>,
}

/// Filter an optional signal date set to the date range of the given bars.
fn filter_signals_to_bar_range(
    dates: Option<&HashSet<chrono::NaiveDateTime>>,
    bars: &[stock_sim::Bar],
) -> Option<HashSet<chrono::NaiveDateTime>> {
    dates.map(|d| {
        if let (Some(first), Some(last)) = (bars.first(), bars.last()) {
            stock_sim::filter_datetime_set(
                d,
                first.datetime.date(),
                last.datetime.date() + chrono::Duration::days(1),
            )
        } else {
            HashSet::new()
        }
    })
}

/// Build a `StockBacktestParams` from a base and a combo, overriding signal/side/slippage fields.
fn build_stock_params_for_combo(
    base: &StockBacktestParams,
    combo: &StockCombo,
) -> StockBacktestParams {
    let mut p = base.clone();
    p.entry_signal = Some(combo.entry_signal.clone());
    p.exit_signal.clone_from(&combo.exit_signal);
    p.side = combo.side;
    p.interval = combo.interval;
    p.session_filter = combo.session_filter;
    p.stop_loss = combo.stop_loss;
    p.take_profit = combo.take_profit;
    p.slippage = combo.slippage.clone();
    p
}

/// Parameters for a stock sweep.
pub struct StockSweepParams {
    pub entry_signals: Vec<SignalSpec>,
    pub exit_signals: Vec<SignalSpec>,
    pub intervals: Vec<Interval>,
    pub sides: Vec<Side>,
    pub session_filters: Vec<Option<SessionFilter>>,
    pub stop_losses: Vec<Option<f64>>,
    pub take_profits: Vec<Option<f64>>,
    pub slippage_models: Vec<Slippage>,
    /// Shared stock backtest params (template — `entry_signal`, `exit_signal`, interval,
    /// side, `session_filter`, `stop_loss`, `take_profit`, slippage are overridden per combo).
    pub base_params: StockBacktestParams,
    pub out_of_sample_pct: f64,
    pub num_permutations: Option<usize>,
    pub permutation_seed: Option<u64>,
}

/// Execute a stock parameter sweep over signal/interval/side/slippage/stop/TP combinations.
#[allow(clippy::too_many_lines)]
pub fn run_stock_sweep(params: &StockSweepParams) -> Result<SweepOutput> {
    // 1. Build all combos (cartesian product)
    let mut combos: Vec<StockCombo> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut skipped: usize = 0;

    for entry_sig in &params.entry_signals {
        for exit_sig_opt in if params.exit_signals.is_empty() {
            vec![None]
        } else {
            params.exit_signals.iter().map(Some).collect()
        } {
            for interval in &params.intervals {
                for side in &params.sides {
                    for session in &params.session_filters {
                        for stop_loss in &params.stop_losses {
                            for take_profit in &params.take_profits {
                                for slippage in &params.slippage_models {
                                    // Build dedup key
                                    let dedup_key = format!(
                                        "{}|{}|{:?}|{:?}|{:?}|{:?}|{:?}|{:?}",
                                        super::sweep_analysis::signal_spec_label(entry_sig),
                                        exit_sig_opt.map_or(
                                            "none".to_string(),
                                            super::sweep_analysis::signal_spec_label
                                        ),
                                        interval,
                                        side,
                                        session,
                                        stop_loss,
                                        take_profit,
                                        slippage,
                                    );
                                    if !seen.insert(dedup_key) {
                                        skipped += 1;
                                        continue;
                                    }

                                    // Build label and dim_keys
                                    let entry_label =
                                        super::sweep_analysis::signal_spec_label(entry_sig);
                                    let mut parts = vec![entry_label.clone()];
                                    let mut dim_keys =
                                        vec![("entry_signal".to_string(), entry_label)];

                                    if let Some(exit_sig) = exit_sig_opt {
                                        let exit_label =
                                            super::sweep_analysis::signal_spec_label(exit_sig);
                                        parts.push(format!("exit:{exit_label}"));
                                        dim_keys.push(("exit_signal".to_string(), exit_label));
                                    }

                                    if params.intervals.len() > 1 {
                                        let interval_str = format!("{interval:?}");
                                        parts.push(interval_str.clone());
                                        dim_keys.push(("interval".to_string(), interval_str));
                                    }

                                    if params.sides.len() > 1 {
                                        let side_str = format!("{side:?}");
                                        parts.push(side_str.clone());
                                        dim_keys.push(("side".to_string(), side_str));
                                    }

                                    if params.session_filters.len() > 1 {
                                        let sf_str = session
                                            .as_ref()
                                            .map_or("None".to_string(), |sf| format!("{sf:?}"));
                                        parts.push(format!("session:{sf_str}"));
                                        dim_keys.push(("session_filter".to_string(), sf_str));
                                    }

                                    if params.stop_losses.len() > 1 {
                                        let sl_str = stop_loss
                                            .map_or("None".to_string(), |v| format!("{v:.2}"));
                                        parts.push(format!("SL:{sl_str}"));
                                        dim_keys.push(("stop_loss".to_string(), sl_str));
                                    }

                                    if params.take_profits.len() > 1 {
                                        let tp_str = take_profit
                                            .map_or("None".to_string(), |v| format!("{v:.2}"));
                                        parts.push(format!("TP:{tp_str}"));
                                        dim_keys.push(("take_profit".to_string(), tp_str));
                                    }

                                    if params.slippage_models.len() > 1 {
                                        let slip_str = match slippage {
                                            Slippage::Mid => "mid".to_string(),
                                            Slippage::Spread => "spread".to_string(),
                                            _ => format!("{slippage:?}"),
                                        };
                                        parts.push(format!("Slip:{slip_str}"));
                                        dim_keys.push(("slippage".to_string(), slip_str));
                                    }

                                    let label = parts.join(" | ");

                                    combos.push(StockCombo {
                                        label,
                                        entry_signal: entry_sig.clone(),
                                        exit_signal: exit_sig_opt.cloned(),
                                        interval: *interval,
                                        side: *side,
                                        session_filter: *session,
                                        stop_loss: *stop_loss,
                                        take_profit: *take_profit,
                                        slippage: slippage.clone(),
                                        dim_keys,
                                    });

                                    if combos.len() > 100 {
                                        bail!(
                                            "Stock parameter sweep exceeds the 100-combination limit. \
                                             Reduce entry_signals ({}), exit_signals ({}), intervals ({}), \
                                             sides ({}), session_filters ({}), stop_losses ({}), \
                                             take_profits ({}), or slippage_models ({}).",
                                            params.entry_signals.len(),
                                            params.exit_signals.len(),
                                            params.intervals.len(),
                                            params.sides.len(),
                                            params.session_filters.len(),
                                            params.stop_losses.len(),
                                            params.take_profits.len(),
                                            params.slippage_models.len(),
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let total = combos.len() + skipped;

    // 2. Group combos by (interval, session_filter) to reuse prepared data
    let mut grouped: std::collections::BTreeMap<String, Vec<usize>> =
        std::collections::BTreeMap::new();
    for (idx, combo) in combos.iter().enumerate() {
        let group_key = format!("{:?}|{:?}", combo.interval, combo.session_filter);
        grouped.entry(group_key).or_default().push(idx);
    }

    // 3. Determine OOS date split (needs global date bounds)
    let ohlcv_path = params
        .base_params
        .ohlcv_path
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("ohlcv_path is required for stock sweep"))?;

    // 4. Run backtests grouped by data-prep key, caching prepared data for OOS reuse
    let mut results: Vec<SweepResult> = Vec::new();
    let mut combo_indices: Vec<usize> = Vec::new();
    let mut failed: usize = 0;

    // Cache (bars, ohlcv_df) by group key to avoid re-reading parquet during OOS pass.
    // Only populated when out_of_sample_pct > 0.0; otherwise data is used directly per group
    // and released, keeping peak memory bounded to a single group's data at a time.
    let mut data_cache: std::collections::BTreeMap<
        String,
        (Vec<stock_sim::Bar>, polars::prelude::DataFrame),
    > = std::collections::BTreeMap::new();

    for (group_key, indices) in &grouped {
        // Use the first combo in the group to prep data
        let representative = &combos[indices[0]];
        let (all_bars, ohlcv_df) = stock_sim::prepare_stock_data(
            ohlcv_path,
            representative.interval,
            representative.session_filter.as_ref(),
            params.base_params.start_date,
            params.base_params.end_date,
        )?;

        // Only cache when the OOS pass will need the data later. This avoids accumulating
        // all groups in memory when OOS is disabled (one group at a time instead).
        if params.out_of_sample_pct > 0.0 {
            data_cache.insert(group_key.clone(), (all_bars.clone(), ohlcv_df.clone()));
        }

        // Determine OOS split on bars. The test slice is fetched separately in the OOS pass
        // below to avoid a redundant allocation here.
        let train_bars: &[stock_sim::Bar] = if params.out_of_sample_pct > 0.0 && all_bars.len() > 10
        {
            let split_idx =
                ((1.0 - params.out_of_sample_pct) * all_bars.len() as f64).round() as usize;
            let split_idx = split_idx.clamp(1, all_bars.len() - 1);
            &all_bars[..split_idx]
        } else {
            &all_bars
        };

        for &idx in indices {
            let combo = &combos[idx];

            // Build params for this combo
            let combo_params = build_stock_params_for_combo(&params.base_params, combo);

            // Build signal filters for train data
            let (entry_dates, exit_dates) = stock_sim::build_stock_signal_filters(
                &combo_params,
                &ohlcv_df,
                stock_sim::ohlcv_path_to_cache_root(ohlcv_path),
            )?;

            // Filter signal dates to train window
            let train_entry = filter_signals_to_bar_range(entry_dates.as_ref(), train_bars);
            let train_exit = filter_signals_to_bar_range(exit_dates.as_ref(), train_bars);

            match stock_sim::run_stock_backtest(
                train_bars,
                &combo_params,
                train_entry.as_ref(),
                train_exit.as_ref(),
            ) {
                Ok(bt) => {
                    let independent_periods = count_independent_entry_periods(&bt.trade_log);
                    combo_indices.push(idx);
                    results.push(SweepResult {
                        label: combo.label.clone(),
                        strategy: "stock".to_string(),
                        display_name: format!("{} Stock", params.base_params.symbol),
                        leg_deltas: vec![],
                        entry_dte: DteRange {
                            target: 0,
                            min: 0,
                            max: 0,
                        },
                        exit_dte: 0,
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
                        entry_signal: Some(combo.entry_signal.clone()),
                        exit_signal: combo.exit_signal.clone(),
                        signal_dim_keys: combo.dim_keys.clone(),
                        p_value: None,
                        sizing: None,
                    });
                }
                Err(e) => {
                    failed += 1;
                    tracing::warn!("Stock sweep combo '{}' failed: {e}", combo.label);
                }
            }
        }
    }

    // 4. Sort by Sharpe descending (keep combo_indices in sync)
    {
        let mut paired: Vec<(SweepResult, usize)> =
            results.into_iter().zip(combo_indices).collect();
        paired.sort_by(|a, b| {
            b.0.sharpe
                .partial_cmp(&a.0.sharpe)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let (sorted_results, sorted_indices) = paired.into_iter().unzip();
        results = sorted_results;
        combo_indices = sorted_indices;
    }

    // 5. Compute dimension sensitivity (uses signal_dim_keys)
    let dimension_sensitivity = compute_sensitivity(&results);

    // 6. OOS validation on top 3 — reuse cached data from step 4
    let mut oos_results = Vec::new();
    if params.out_of_sample_pct > 0.0 {
        let top_n = results.len().min(3);
        for (ri, r) in results.iter().take(top_n).enumerate() {
            let combo = &combos[combo_indices[ri]];

            // Look up cached data instead of re-reading parquet
            let group_key = format!("{:?}|{:?}", combo.interval, combo.session_filter);
            let (all_bars, oos_ohlcv_df) = data_cache
                .get(&group_key)
                .ok_or_else(|| anyhow::anyhow!("Missing cached data for OOS group {group_key}"))?;

            if all_bars.len() < 2 {
                // Not enough data for a train/test split; skip OOS for this combo.
                continue;
            }

            let split_idx =
                ((1.0 - params.out_of_sample_pct) * all_bars.len() as f64).round() as usize;
            let split_idx = split_idx.clamp(1, all_bars.len() - 1);
            let test_bars = &all_bars[split_idx..];

            let combo_params = build_stock_params_for_combo(&params.base_params, combo);

            let (entry_dates, exit_dates) = stock_sim::build_stock_signal_filters(
                &combo_params,
                oos_ohlcv_df,
                stock_sim::ohlcv_path_to_cache_root(ohlcv_path),
            )?;

            let test_entry = filter_signals_to_bar_range(entry_dates.as_ref(), test_bars);
            let test_exit = filter_signals_to_bar_range(exit_dates.as_ref(), test_bars);

            match stock_sim::run_stock_backtest(
                test_bars,
                &combo_params,
                test_entry.as_ref(),
                test_exit.as_ref(),
            ) {
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
                    tracing::warn!("Stock OOS validation for '{}' failed: {e}", r.label);
                }
            }
        }
    }

    let combinations_run = results.len();

    // 7. Stability — use empty grids since stock dimensions are in signal_dim_keys
    let dummy_sweep_params = SweepParams {
        strategies: vec![],
        sweep: SweepDimensions {
            entry_dte_targets: vec![],
            exit_dtes: vec![],
            slippage_models: vec![],
        },
        sim_params: SimParams {
            capital: params.base_params.capital,
            quantity: params.base_params.quantity,
            sizing: params.base_params.sizing.clone(),
            multiplier: 100,
            max_positions: params.base_params.max_positions,
            selector: super::types::TradeSelector::default(),
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            max_hold_bars: None,
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: std::collections::HashMap::new(),
            min_days_between_entries: None,
            min_bars_between_entries: None,
            conflict_resolution: None,
            exit_net_delta: None,
        },
        out_of_sample_pct: 0.0,
        direction: None,
        entry_signals: vec![],
        exit_signals: vec![],
        num_permutations: None,
        permutation_seed: None,
    };
    let stability_scores = compute_stability(&results, &dummy_sweep_params);

    // 8. Optional permutation tests for multiple comparisons
    let multiple_comparisons =
        run_stock_multiple_comparisons(ohlcv_path, &mut results, params, &combos, &combo_indices);

    Ok(SweepOutput {
        mode: Some("stock".to_string()),
        combinations_total: total,
        combinations_run,
        combinations_skipped: skipped,
        combinations_failed: failed,
        signal_combinations: None,
        ranked_results: results,
        dimension_sensitivity,
        oos_results,
        stability_scores,
        multiple_comparisons,
    })
}

/// Cache key for `run_stock_multiple_comparisons`: unique per (interval, session, date-range).
type DataPrepKey = (
    Interval,
    Option<SessionFilter>,
    Option<chrono::NaiveDate>,
    Option<chrono::NaiveDate>,
);

/// Run permutation tests for each stock sweep result and apply multiple comparisons correction.
fn run_stock_multiple_comparisons(
    ohlcv_path: &str,
    results: &mut [SweepResult],
    params: &StockSweepParams,
    combos: &[StockCombo],
    combo_indices: &[usize],
) -> Option<(
    super::multiple_comparisons::MultipleComparisonsResult,
    super::multiple_comparisons::MultipleComparisonsResult,
)> {
    let num_permutations = params.num_permutations?;

    let perm_params = PermutationParams {
        num_permutations,
        seed: params.permutation_seed,
    };

    // Cache prepared (bars, df) by (interval, session_filter, start_date, end_date) key to
    // avoid re-reading and re-resampling the same OHLCV parquet for every combo.
    // The cache holds at most one entry per unique (interval, session, date-range) group,
    // so memory usage is bounded by the number of distinct data-prep configurations.
    let mut data_cache: std::collections::HashMap<
        DataPrepKey,
        (Vec<stock_sim::Bar>, polars::prelude::DataFrame),
    > = std::collections::HashMap::new();

    for (ri, result) in results.iter_mut().enumerate() {
        let combo = &combos[combo_indices[ri]];

        let cache_key: DataPrepKey = (
            combo.interval,
            combo.session_filter,
            params.base_params.start_date,
            params.base_params.end_date,
        );

        let entry = data_cache.entry(cache_key);
        let cached = match entry {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => {
                if let Ok(prepared) = stock_sim::prepare_stock_data(
                    ohlcv_path,
                    combo.interval,
                    combo.session_filter.as_ref(),
                    params.base_params.start_date,
                    params.base_params.end_date,
                ) {
                    e.insert(prepared)
                } else {
                    result.p_value = Some(1.0);
                    continue;
                }
            }
        };
        let (bars, ohlcv_df) = cached;

        let combo_params = build_stock_params_for_combo(&params.base_params, combo);

        let Ok((entry_dates, exit_dates)) = stock_sim::build_stock_signal_filters(
            &combo_params,
            ohlcv_df,
            stock_sim::ohlcv_path_to_cache_root(ohlcv_path),
        ) else {
            result.p_value = Some(1.0);
            continue;
        };

        match crate::engine::permutation::run_stock_permutation_test(
            bars,
            &combo_params,
            &entry_dates,
            &exit_dates,
            &perm_params,
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
                    "Stock permutation test for combo '{}' failed: {e}",
                    result.label
                );
                result.p_value = Some(1.0);
            }
        }
    }

    if results.len() < 2 {
        return None;
    }

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
    use crate::data::parquet::DATETIME_COL;
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
            max_hold_bars: None,
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_days_between_entries: None,
            min_bars_between_entries: None,
            conflict_resolution: None,
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
            DATETIME_COL => &[qt],
            "option_type" => &["c"],
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
            max_hold_bars: None,
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_days_between_entries: None,
            min_bars_between_entries: None,
            conflict_resolution: None,
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
            DATETIME_COL => &[qt],
            "option_type" => &["c"],
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
            DATETIME_COL => &dates,
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
            SignalSpec::Formula {
                formula: "rsi(close, 14) < 25".into(),
            },
            SignalSpec::Formula {
                formula: "rsi(close, 14) < 30".into(),
            },
            SignalSpec::Formula {
                formula: "rsi(close, 14) < 35".into(),
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
            SignalSpec::Formula {
                formula: "rsi(close, 14) < 25".into(),
            },
            SignalSpec::Formula {
                formula: "rsi(close, 14) < 30".into(),
            },
            SignalSpec::Formula {
                formula: "rsi(close, 14) < 35".into(),
            },
        ];
        let exits = vec![
            SignalSpec::Formula {
                formula: "consecutive_down(close) >= 2".into(),
            },
            SignalSpec::Formula {
                formula: "consecutive_down(close) >= 3".into(),
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
            max_hold_bars: None,
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_days_between_entries: None,
            min_bars_between_entries: None,
            conflict_resolution: None,
            exit_net_delta: None,
        };
        let params = SweepParams {
            strategies,
            sweep,
            sim_params,
            out_of_sample_pct: 0.0,
            direction: None,
            entry_signals: vec![
                SignalSpec::Formula {
                    formula: "rsi(close, 14) < 25".into(),
                },
                SignalSpec::Formula {
                    formula: "rsi(close, 14) < 30".into(),
                },
                SignalSpec::Formula {
                    formula: "rsi(close, 14) < 35".into(),
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
            DATETIME_COL => &[qt],
            "option_type" => &["c"],
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
    fn signal_spec_label_covers_all_variants() {
        assert_eq!(
            signal_spec_label(&SignalSpec::Formula {
                formula: "rsi(close, 14) < 30".into()
            }),
            "rsi(close, 14) < 30"
        );
        assert_eq!(
            signal_spec_label(&SignalSpec::Saved {
                name: "my_signal".into(),
            }),
            "Saved(my_signal)"
        );
        assert_eq!(
            signal_spec_label(&SignalSpec::And {
                left: Box::new(SignalSpec::Formula {
                    formula: "rsi(close,14) < 30".into()
                }),
                right: Box::new(SignalSpec::Formula {
                    formula: "close > sma(close,20)".into()
                }),
            }),
            "And(…)"
        );
        assert_eq!(
            signal_spec_label(&SignalSpec::Or {
                left: Box::new(SignalSpec::Formula {
                    formula: "rsi(close,14) < 30".into()
                }),
                right: Box::new(SignalSpec::Formula {
                    formula: "close > sma(close,20)".into()
                }),
            }),
            "Or(…)"
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
                max_hold_bars: None,
                entry_signal: None,
                exit_signal: None,
                ohlcv_path: None,
                cross_ohlcv_paths: HashMap::new(),
                min_days_between_entries: None,
                min_bars_between_entries: None,
                conflict_resolution: None,
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
    fn stability_exact_asymmetric_sharpe() {
        // Sweep grid: deltas [0.20, 0.30, 0.40], entry_dtes [30, 45], exit_dtes [0]
        //
        // Center strategy: delta=0.30, dte=45, exit=0 → Sharpe=2.0
        // Neighbors:
        //   leg_1_delta dim: delta=0.20 (Sharpe=1.5), delta=0.40 (Sharpe=0.5)
        //   entry_dte dim:   dte=30 (Sharpe=1.8)
        //
        // ── leg_1_delta dimension ──
        //   left neighbor (0.20): rel_change = |1.5 - 2.0| / max(|2.0|, 0.01) = 0.5/2.0 = 0.25
        //   right neighbor (0.40): rel_change = |0.5 - 2.0| / max(|2.0|, 0.01) = 1.5/2.0 = 0.75
        //   max_change = 0.75
        //   dim_score = 1.0 - clamp(0.75, 0, 1) = 0.25
        //
        // ── entry_dte dimension ──
        //   neighbor (30): rel_change = |1.8 - 2.0| / max(|2.0|, 0.01) = 0.2/2.0 = 0.10
        //   max_change = 0.10
        //   dim_score = 1.0 - 0.10 = 0.90
        //
        // overall_score = (0.25 + 0.90) / 2 = 0.575
        // is_stable = 0.575 >= 0.7 → false

        let results = vec![
            make_sweep_result("long_call", 0.30, 45, 0, 2.0), // center (top by Sharpe)
            make_sweep_result("long_call", 0.20, 45, 0, 1.5), // delta neighbor left
            make_sweep_result("long_call", 0.30, 30, 0, 1.8), // DTE neighbor
            make_sweep_result("long_call", 0.40, 45, 0, 0.5), // delta neighbor right
            // Fill out grid so all combos exist:
            make_sweep_result("long_call", 0.20, 30, 0, 1.0),
            make_sweep_result("long_call", 0.40, 30, 0, 0.3),
        ];
        let params = make_sweep_params("long_call", vec![0.20, 0.30, 0.40], vec![30, 45], vec![0]);

        let scores = compute_stability(&results, &params);
        assert!(!scores.is_empty());

        let top = &scores[0];

        // Verify per-dimension scores
        let delta_dim = top
            .per_dimension
            .iter()
            .find(|d| d.dimension == "leg_1_delta")
            .expect("should have leg_1_delta dimension");
        // max_sharpe_change = 0.75
        assert!(
            (delta_dim.max_sharpe_change - 0.75).abs() < 1e-10,
            "delta max_sharpe_change: expected 0.75, got {}",
            delta_dim.max_sharpe_change
        );
        // score = 1.0 - 0.75 = 0.25
        assert!(
            (delta_dim.score - 0.25).abs() < 1e-10,
            "delta score: expected 0.25, got {}",
            delta_dim.score
        );
        assert_eq!(delta_dim.neighbor_count, 2);

        let dte_dim = top
            .per_dimension
            .iter()
            .find(|d| d.dimension == "entry_dte")
            .expect("should have entry_dte dimension");
        // max_sharpe_change = |1.8 - 2.0| / 2.0 = 0.10
        assert!(
            (dte_dim.max_sharpe_change - 0.10).abs() < 1e-10,
            "dte max_sharpe_change: expected 0.10, got {}",
            dte_dim.max_sharpe_change
        );
        // score = 1.0 - 0.10 = 0.90
        assert!(
            (dte_dim.score - 0.90).abs() < 1e-10,
            "dte score: expected 0.90, got {}",
            dte_dim.score
        );
        assert_eq!(dte_dim.neighbor_count, 1);

        // overall_score = (0.25 + 0.90) / 2 = 0.575
        assert!(
            (top.overall_score - 0.575).abs() < 1e-10,
            "overall_score: expected 0.575, got {}",
            top.overall_score
        );

        // is_stable = 0.575 >= 0.7 → false
        assert!(!top.is_stable, "should not be stable at 0.575");

        // Warning should contain "CAUTION" (score between 0.5 and 0.7)
        assert!(
            top.warning.as_ref().unwrap().contains("CAUTION"),
            "expected CAUTION warning, got: {:?}",
            top.warning
        );
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
