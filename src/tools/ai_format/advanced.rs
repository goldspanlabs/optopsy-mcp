//! Format advanced analysis results (sweep, walk-forward, permutation test) into AI-enriched responses.

use crate::engine::permutation::PermutationOutput;
use crate::engine::sweep::SweepOutput;
use crate::engine::types::BacktestParams;
use crate::engine::walk_forward::WalkForwardResult;
use crate::tools::ai_helpers::{
    build_params_summary, format_pnl, interpret_p_value, walk_forward_findings, P_SIGNIFICANT,
    SWEEP_SCORE_MODERATE, SWEEP_SCORE_WEAK,
};
use crate::tools::response_types::{
    MultipleComparisonsCorrection, OosValidation, PermutationTestResponse, SweepResponse,
    WalkForwardAggregate, WalkForwardResponse, WalkForwardWindowResult,
};

/// Format parameter sweep output into a ranked response with stability and OOS validation.
#[allow(clippy::too_many_lines)]
pub fn format_sweep(output: SweepOutput) -> SweepResponse {
    let best = output.ranked_results.first().cloned();

    let signal_suffix = output
        .signal_combinations
        .map(|n| format!(" (incl. {n} signal variants)"))
        .unwrap_or_default();

    let summary = if let Some(ref b) = best {
        format!(
            "Swept {} combinations{signal_suffix}; ran {} ({} skipped, {} failed). Best: {} (Sharpe {:.2}, {}).",
            output.combinations_total,
            output.combinations_run,
            output.combinations_skipped,
            output.combinations_failed,
            b.label,
            b.sharpe,
            format_pnl(b.pnl),
        )
    } else {
        format!(
            "Swept {} combinations{signal_suffix} but none produced results ({} skipped, {} failed).",
            output.combinations_total, output.combinations_skipped, output.combinations_failed,
        )
    };

    let out_of_sample = if output.oos_results.is_empty() {
        None
    } else {
        Some(OosValidation {
            top_n_validated: output.oos_results.len(),
            results: output.oos_results,
        })
    };

    // Stability analysis
    let stability = if output.stability_scores.is_empty() {
        None
    } else {
        Some(output.stability_scores.clone())
    };

    // Append stability info to summary
    let summary = if let Some(ref scores) = stability {
        if let Some(top) = scores.first() {
            if let Some(ref warn) = top.warning {
                format!("{summary} ⚠ Stability: {warn}")
            } else {
                format!(
                    "{summary} Parameter stability: GOOD (score: {:.2}).",
                    top.overall_score
                )
            }
        } else {
            summary
        }
    } else {
        summary
    };

    let mut suggested_next_steps = Vec::new();
    if let Some(ref b) = best {
        suggested_next_steps.push(format!(
            "[NEXT] Use run_backtest(strategy=\"{}\") on best combo \"{}\" for detailed trade-level analysis",
            b.strategy, b.label,
        ));
        suggested_next_steps.push(
            "[ITERATE] Narrow delta/DTE ranges around the best combo and re-sweep".to_string(),
        );
    }
    if output.combinations_run == 0 {
        suggested_next_steps.push(
            "[FIX] No valid combinations — widen DTE ranges, reduce exit_dtes, or add more strategies"
                .to_string(),
        );
    }

    // Add stability-specific next steps
    if let Some(ref scores) = stability {
        if let Some(top) = scores.first() {
            if top.overall_score < SWEEP_SCORE_WEAK {
                suggested_next_steps.push(format!(
                    "[UNSTABLE] Best combination has low parameter stability ({:.2}). Performance may be fragile — consider a more stable alternative.",
                    top.overall_score,
                ));
            } else if top.overall_score < SWEEP_SCORE_MODERATE {
                suggested_next_steps.push(format!(
                    "[CAUTION] Best combination has moderate parameter stability ({:.2}). Re-sweep with a narrower grid around this region to confirm robustness before deploying.",
                    top.overall_score,
                ));
            } else {
                suggested_next_steps.push(format!(
                    "[GOOD] Best combination shows stable performance across neighboring parameters (stability: {:.2}).",
                    top.overall_score,
                ));
            }
        }
    }

    // Build multiple comparisons correction response
    let multiple_comparisons = output.multiple_comparisons.map(|(bon, bh)| {
        let bon_sig = bon.num_significant;
        let bh_sig = bh.num_significant;
        let num_tests = bon.num_tests;

        // Add multiple comparisons next steps
        if bon_sig == 0 && bh_sig == 0 {
            suggested_next_steps.push(format!(
                "[MC] Multiple comparisons correction: 0/{num_tests} configurations survive \
                 significance after Bonferroni or BH-FDR correction (α=0.05). \
                 Results may be noise — consider more data or fewer parameter combinations."
            ));
        } else if bh_sig > bon_sig {
            suggested_next_steps.push(format!(
                "[MC] Multiple comparisons: {bh_sig}/{num_tests} combinations survive BH-FDR, \
                 {bon_sig}/{num_tests} survive the stricter Bonferroni correction (α=0.05)."
            ));
        } else {
            suggested_next_steps.push(format!(
                "[MC] Multiple comparisons: {bon_sig}/{num_tests} combinations remain significant \
                 after Bonferroni and BH-FDR correction (α=0.05) — strong evidence of a real edge."
            ));
        }

        MultipleComparisonsCorrection {
            bonferroni: bon,
            benjamini_hochberg: bh,
        }
    });

    SweepResponse {
        summary,
        combinations_total: output.combinations_total,
        combinations_run: output.combinations_run,
        combinations_skipped: output.combinations_skipped,
        combinations_failed: output.combinations_failed,
        signal_combinations: output.signal_combinations,
        best_combination: best,
        dimension_sensitivity: output.dimension_sensitivity,
        out_of_sample,
        stability,
        multiple_comparisons,
        ranked_results: output.ranked_results,
        suggested_next_steps,
    }
}

/// Format walk-forward analysis results with per-window details and aggregate statistics.
pub fn format_walk_forward(
    result: &WalkForwardResult,
    params: &BacktestParams,
    train_days: i32,
    test_days: i32,
    step_days: Option<i32>,
) -> WalkForwardResponse {
    let agg = &result.aggregate;
    let step = step_days.unwrap_or(test_days);

    let attempted_windows = agg.successful_windows + agg.failed_windows;
    let window_desc = if agg.failed_windows > 0 {
        format!(
            "{} of {} attempted windows ({} failed)",
            agg.successful_windows, attempted_windows, agg.failed_windows
        )
    } else {
        format!("{} windows", agg.successful_windows)
    };

    let summary = format!(
        "Walk-forward analysis for {} across {} (train={}d, test={}d, step={}d): \
         avg test Sharpe {:.2} (±{:.2}), {:.0}% profitable windows, total test P&L {}",
        params.strategy,
        window_desc,
        train_days,
        test_days,
        step,
        agg.avg_test_sharpe,
        agg.std_test_sharpe,
        agg.pct_profitable_windows,
        format_pnl(agg.total_test_pnl),
    );

    let windows: Vec<WalkForwardWindowResult> = result
        .windows
        .iter()
        .map(|w| WalkForwardWindowResult {
            window_number: w.window_number,
            train_start: w.train_start.to_string(),
            train_end: w.train_end.to_string(),
            test_start: w.test_start.to_string(),
            test_end: w.test_end.to_string(),
            train_sharpe: w.train_sharpe,
            test_sharpe: w.test_sharpe,
            train_pnl: w.train_pnl,
            test_pnl: w.test_pnl,
            train_trades: w.train_trades,
            test_trades: w.test_trades,
            train_win_rate: w.train_win_rate,
            test_win_rate: w.test_win_rate,
        })
        .collect();

    let key_findings = walk_forward_findings(agg);

    let mut suggested_next_steps = vec![];
    if agg.avg_train_test_sharpe_decay > 0.5 {
        suggested_next_steps
            .push("Consider simplifying strategy parameters to reduce overfitting".to_string());
    }
    if agg.pct_profitable_windows < 50.0 {
        suggested_next_steps
            .push("Try different strategy parameters or a different strategy entirely".to_string());
    }
    suggested_next_steps.push(
        "Use `parameter_sweep` to find optimal parameters, then validate with `walk_forward`"
            .to_string(),
    );
    suggested_next_steps.push(
        "Try different train/test window sizes to check robustness of the walk-forward results"
            .to_string(),
    );

    WalkForwardResponse {
        summary,
        windows,
        aggregate: WalkForwardAggregate {
            successful_windows: agg.successful_windows,
            failed_windows: agg.failed_windows,
            avg_test_sharpe: agg.avg_test_sharpe,
            std_test_sharpe: agg.std_test_sharpe,
            avg_test_pnl: agg.avg_test_pnl,
            pct_profitable_windows: agg.pct_profitable_windows,
            avg_train_test_sharpe_decay: agg.avg_train_test_sharpe_decay,
            total_test_pnl: agg.total_test_pnl,
        },
        key_findings,
        suggested_next_steps,
    }
}

/// Format permutation test output with significance assessment and per-metric p-values.
pub fn format_permutation_test(
    output: PermutationOutput,
    params: &BacktestParams,
) -> PermutationTestResponse {
    let real = &output.real_result;
    let real_total_pnl: f64 = real.trade_log.iter().map(|t| t.pnl).sum();

    let sharpe_p = output
        .metric_results
        .iter()
        .find(|m| m.metric_name == "sharpe")
        .map_or(1.0, |m| m.p_value);
    let pnl_p = output
        .metric_results
        .iter()
        .find(|m| m.metric_name == "total_pnl")
        .map_or(1.0, |m| m.p_value);
    let is_significant = sharpe_p < P_SIGNIFICANT && pnl_p < P_SIGNIFICANT;

    let sig_label = if is_significant {
        "statistically significant"
    } else {
        "NOT statistically significant"
    };
    let pnl_str = format_pnl(real_total_pnl);

    let summary = format!(
        "Permutation test for {} ({} permutations, {} completed): results are {sig_label}. \
         Real Sharpe {:.2} (p={sharpe_p:.3}), real PnL {pnl_str} (p={pnl_p:.3}).",
        params.strategy, output.num_permutations, output.num_completed, real.metrics.sharpe,
    );

    let assessment = if is_significant {
        let sig = interpret_p_value(sharpe_p.max(pnl_p));
        format!("The strategy shows a {sig} edge over random entry timing (Sharpe p={sharpe_p:.3}, PnL p={pnl_p:.3})")
    } else {
        format!(
            "The strategy does NOT show a significant edge over random entry timing \
                 (Sharpe p={sharpe_p:.3}, PnL p={pnl_p:.3}). Results could be due to chance."
        )
    };

    let key_findings: Vec<String> = output
        .metric_results
        .iter()
        .map(|m| {
            let sig = interpret_p_value(m.p_value);
            format!(
                "{}: real={:.4}, permuted mean={:.4} (±{:.4}), p={:.3} ({sig})",
                m.metric_name, m.real_value, m.mean_permuted, m.std_permuted, m.p_value,
            )
        })
        .collect();

    let suggested_next_steps = if is_significant {
        vec![
            format!(
                "[NEXT] Run parameter_sweep to optimize {} parameters further",
                params.strategy
            ),
            "[VALIDATE] Run with more permutations (500-1000) for tighter p-value estimates"
                .to_string(),
        ]
    } else {
        vec![
            "[ITERATE] Try different delta targets, DTE ranges, or entry signals to find a significant edge".to_string(),
            "[COMPARE] Use compare_strategies to test alternative strategies".to_string(),
        ]
    };

    PermutationTestResponse {
        summary,
        assessment,
        key_findings,
        parameters: build_params_summary(params),
        num_permutations: output.num_permutations,
        num_completed: output.num_completed,
        real_metrics: real.metrics.clone(),
        real_trade_count: real.trade_count,
        real_total_pnl,
        metric_tests: output.metric_results,
        is_significant,
        suggested_next_steps,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::sweep::SweepOutput;
    use crate::engine::types::{DteRange, Slippage, SweepResult, TargetRange};
    use std::collections::HashMap;

    #[test]
    fn format_sweep_with_results() {
        let results = vec![
            SweepResult {
                label: "long_call(Δ0.50, DTE 45, Exit 0)".to_string(),
                strategy: "long_call".to_string(),
                display_name: "Long Call".to_string(),
                leg_deltas: vec![TargetRange {
                    target: 0.50,
                    min: 0.45,
                    max: 0.55,
                }],
                entry_dte: DteRange {
                    target: 45,
                    min: 31,
                    max: 59,
                },
                exit_dte: 0,
                slippage: Slippage::Mid,
                trades: 10,
                pnl: 500.0,
                sharpe: 1.5,
                sortino: 2.0,
                max_dd: 0.05,
                win_rate: 0.7,
                profit_factor: 2.5,
                calmar: 2.0,
                total_return_pct: 5.0,
                independent_entry_periods: 8,
                entry_signal: None,
                exit_signal: None,
                signal_dim_keys: vec![],
                p_value: None,
            },
            SweepResult {
                label: "long_call(Δ0.35, DTE 45, Exit 0)".to_string(),
                strategy: "long_call".to_string(),
                display_name: "Long Call".to_string(),
                leg_deltas: vec![TargetRange {
                    target: 0.35,
                    min: 0.30,
                    max: 0.40,
                }],
                entry_dte: DteRange {
                    target: 45,
                    min: 31,
                    max: 59,
                },
                exit_dte: 0,
                slippage: Slippage::Mid,
                trades: 8,
                pnl: 200.0,
                sharpe: 0.8,
                sortino: 1.0,
                max_dd: 0.08,
                win_rate: 0.6,
                profit_factor: 1.5,
                calmar: 1.0,
                total_return_pct: 2.0,
                independent_entry_periods: 6,
                entry_signal: None,
                exit_signal: None,
                signal_dim_keys: vec![],
                p_value: None,
            },
        ];

        let output = SweepOutput {
            combinations_total: 5,
            combinations_run: 2,
            combinations_skipped: 3,
            combinations_failed: 0,
            signal_combinations: None,
            ranked_results: results,
            dimension_sensitivity: HashMap::new(),
            oos_results: vec![],
            stability_scores: vec![],
            multiple_comparisons: None,
        };

        let response = format_sweep(output);
        assert!(response
            .summary
            .contains("long_call(Δ0.50, DTE 45, Exit 0)"));
        assert!(response.summary.contains("1.50"));
        assert!(response.best_combination.is_some());
        assert_eq!(
            response.best_combination.unwrap().label,
            "long_call(Δ0.50, DTE 45, Exit 0)"
        );
        assert_eq!(response.combinations_run, 2);
        assert_eq!(response.combinations_skipped, 3);
        assert!(response.out_of_sample.is_none());
        assert!(response
            .suggested_next_steps
            .iter()
            .any(|s| s.contains("run_backtest")));
    }

    #[test]
    fn format_sweep_no_results() {
        let output = SweepOutput {
            combinations_total: 10,
            combinations_run: 0,
            combinations_skipped: 10,
            combinations_failed: 0,
            signal_combinations: None,
            ranked_results: vec![],
            dimension_sensitivity: HashMap::new(),
            oos_results: vec![],
            stability_scores: vec![],
            multiple_comparisons: None,
        };

        let response = format_sweep(output);
        assert!(response.summary.contains("none produced results"));
        assert!(response.best_combination.is_none());
        assert!(response
            .suggested_next_steps
            .iter()
            .any(|s| s.contains("widen DTE")));
    }

    #[test]
    fn format_sweep_with_oos() {
        use crate::engine::sweep::OosResult;

        let output = SweepOutput {
            combinations_total: 2,
            combinations_run: 1,
            combinations_skipped: 1,
            combinations_failed: 0,
            signal_combinations: None,
            ranked_results: vec![SweepResult {
                label: "test_combo".to_string(),
                strategy: "long_call".to_string(),
                display_name: "Long Call".to_string(),
                leg_deltas: vec![TargetRange {
                    target: 0.50,
                    min: 0.45,
                    max: 0.55,
                }],
                entry_dte: DteRange {
                    target: 45,
                    min: 31,
                    max: 59,
                },
                exit_dte: 0,
                slippage: Slippage::Mid,
                trades: 5,
                pnl: 100.0,
                sharpe: 1.0,
                sortino: 1.2,
                max_dd: 0.05,
                win_rate: 0.6,
                profit_factor: 1.5,
                calmar: 1.0,
                total_return_pct: 1.0,
                independent_entry_periods: 4,
                entry_signal: None,
                exit_signal: None,
                signal_dim_keys: vec![],
                p_value: None,
            }],
            dimension_sensitivity: HashMap::new(),
            oos_results: vec![OosResult {
                label: "test_combo".to_string(),
                train_sharpe: 1.0,
                test_sharpe: 0.8,
                train_pnl: 100.0,
                test_pnl: 50.0,
            }],
            stability_scores: vec![],
            multiple_comparisons: None,
        };

        let response = format_sweep(output);
        assert!(response.out_of_sample.is_some());
        let oos = response.out_of_sample.unwrap();
        assert_eq!(oos.top_n_validated, 1);
        assert_eq!(oos.results.len(), 1);
        assert!((oos.results[0].train_sharpe - 1.0).abs() < 1e-10);
        assert!((oos.results[0].test_sharpe - 0.8).abs() < 1e-10);
    }
}
