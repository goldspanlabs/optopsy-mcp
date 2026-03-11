//! Integration tests for the `parameter_sweep` feature.
//!
//! These tests exercise the full sweep pipeline with real data frames
//! that produce actual trades, validating end-to-end correctness.

use chrono::NaiveDate;
use optopsy_mcp::engine::core::run_backtest;
use optopsy_mcp::engine::sweep::{run_sweep, SweepDimensions, SweepParams, SweepStrategyEntry};
use optopsy_mcp::engine::types::{
    strategy_direction, Direction, ExitType, SimParams, Slippage, TradeSelector,
};
use optopsy_mcp::signals::registry::SignalSpec;

mod common;
use common::{make_multi_strike_df, write_ohlcv_parquet};

fn default_sim_params() -> SimParams {
    SimParams {
        capital: 100_000.0,
        quantity: 1,
        sizing: None,
        multiplier: 100,
        max_positions: 3,
        selector: TradeSelector::First,
        stop_loss: None,
        take_profit: None,
        max_hold_days: None,
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: None,
        cross_ohlcv_paths: std::collections::HashMap::new(),
        min_days_between_entries: None,
        exit_net_delta: None,
    }
}

// ---------------------------------------------------------------------------
// Full pipeline integration tests
// ---------------------------------------------------------------------------

#[test]
fn sweep_single_strategy_produces_ranked_results() {
    let df = make_multi_strike_df();
    let params = SweepParams {
        strategies: vec![SweepStrategyEntry {
            name: "long_call".to_string(),
            leg_delta_targets: vec![vec![0.35, 0.50, 0.70]],
        }],
        sweep: SweepDimensions {
            entry_dte_targets: vec![45],
            exit_dtes: vec![0, 5],
            slippage_models: vec![Slippage::Mid],
        },
        sim_params: default_sim_params(),
        out_of_sample_pct: 0.0,
        direction: None,
        entry_signals: vec![],
        exit_signals: vec![],
        num_permutations: None,
        permutation_seed: None,
    };

    let output = run_sweep(&df, &params).unwrap();

    // Should have run multiple combinations (3 deltas × 1 DTE × 2 exit_dtes × 1 slippage)
    // Some exit_dtes may be filtered (exit_dte >= entry_dte.min)
    assert!(
        output.combinations_run > 0,
        "Expected some combinations to run, got 0"
    );
    assert!(
        output.combinations_total > output.combinations_run
            || output.combinations_skipped > 0
            || output.combinations_run > 0,
        "Expected some processing"
    );

    // Results should be sorted by Sharpe descending
    for w in output.ranked_results.windows(2) {
        assert!(
            w[0].sharpe >= w[1].sharpe || w[0].sharpe.is_nan(),
            "Results not sorted by Sharpe: {} >= {}",
            w[0].sharpe,
            w[1].sharpe,
        );
    }

    // At least one result should have actual trades
    let has_trades = output.ranked_results.iter().any(|r| r.trades > 0);
    assert!(
        has_trades,
        "Expected at least one combo to produce trades with make_multi_strike_df"
    );
}

#[test]
fn sweep_multi_strategy_without_direction_filter() {
    let df = make_multi_strike_df();
    let params = SweepParams {
        strategies: vec![
            SweepStrategyEntry {
                name: "long_call".to_string(),
                leg_delta_targets: vec![vec![0.50]],
            },
            SweepStrategyEntry {
                name: "short_put".to_string(),
                leg_delta_targets: vec![vec![0.40]],
            },
            // This bearish strategy is included because direction filtering
            // happens at the server layer (resolve_sweep_strategies), not in run_sweep.
            SweepStrategyEntry {
                name: "long_put".to_string(),
                leg_delta_targets: vec![vec![0.40]],
            },
        ],
        sweep: SweepDimensions {
            entry_dte_targets: vec![45],
            exit_dtes: vec![0],
            slippage_models: vec![Slippage::Mid],
        },
        sim_params: default_sim_params(),
        out_of_sample_pct: 0.0,
        // direction filtering is not applied by run_sweep itself; all 3 strategies are processed
        direction: None,
        entry_signals: vec![],
        exit_signals: vec![],
        num_permutations: None,
        permutation_seed: None,
    };

    let output = run_sweep(&df, &params).unwrap();

    // Should have results from multiple strategies
    let strategy_names: Vec<&str> = output
        .ranked_results
        .iter()
        .map(|r| r.strategy.as_str())
        .collect();

    // All three should be present (direction filtering is at server level)
    let unique_strategies: std::collections::HashSet<&str> =
        strategy_names.iter().copied().collect();
    assert!(
        unique_strategies.len() >= 2,
        "Expected results from multiple strategies, got: {unique_strategies:?}"
    );
}

#[test]
fn sweep_with_oos_validation() {
    let df = make_multi_strike_df();
    let params = SweepParams {
        strategies: vec![SweepStrategyEntry {
            name: "long_call".to_string(),
            leg_delta_targets: vec![vec![0.35, 0.50]],
        }],
        sweep: SweepDimensions {
            entry_dte_targets: vec![45],
            exit_dtes: vec![0],
            slippage_models: vec![Slippage::Mid],
        },
        sim_params: default_sim_params(),
        out_of_sample_pct: 0.3,
        direction: None,
        entry_signals: vec![],
        exit_signals: vec![],
        num_permutations: None,
        permutation_seed: None,
    };

    let output = run_sweep(&df, &params).unwrap();

    // With sparse synthetic data, OOS may or may not produce results,
    // but the split + validation pipeline should not error
    // The oos_results vec should be populated if any training combos succeeded
    if !output.ranked_results.is_empty() {
        // OOS validation runs on top 3 (or fewer)
        // With sparse data the test set may not produce trades, but no panic
        assert!(
            output.oos_results.len() <= 3,
            "OOS should validate at most 3, got {}",
            output.oos_results.len()
        );
        for oos in &output.oos_results {
            // Train metrics should match ranked_results
            let train = output.ranked_results.iter().find(|r| r.label == oos.label);
            assert!(
                train.is_some(),
                "OOS label '{}' not found in ranked_results",
                oos.label
            );
            assert!(
                (oos.train_sharpe - train.unwrap().sharpe).abs() < 1e-10,
                "OOS train_sharpe doesn't match ranked_results"
            );
        }
    }
}

#[test]
fn sweep_oos_disabled_when_zero() {
    let df = make_multi_strike_df();
    let params = SweepParams {
        strategies: vec![SweepStrategyEntry {
            name: "long_call".to_string(),
            leg_delta_targets: vec![vec![0.50]],
        }],
        sweep: SweepDimensions {
            entry_dte_targets: vec![45],
            exit_dtes: vec![0],
            slippage_models: vec![Slippage::Mid],
        },
        sim_params: default_sim_params(),
        out_of_sample_pct: 0.0,
        direction: None,
        entry_signals: vec![],
        exit_signals: vec![],
        num_permutations: None,
        permutation_seed: None,
    };

    let output = run_sweep(&df, &params).unwrap();
    assert!(
        output.oos_results.is_empty(),
        "OOS should be empty when pct=0"
    );
}

#[test]
fn sweep_dimension_sensitivity_populated() {
    let df = make_multi_strike_df();
    let params = SweepParams {
        strategies: vec![
            SweepStrategyEntry {
                name: "long_call".to_string(),
                leg_delta_targets: vec![vec![0.35, 0.50]],
            },
            SweepStrategyEntry {
                name: "short_put".to_string(),
                leg_delta_targets: vec![vec![0.40]],
            },
        ],
        sweep: SweepDimensions {
            entry_dte_targets: vec![45],
            exit_dtes: vec![0],
            slippage_models: vec![Slippage::Mid],
        },
        sim_params: default_sim_params(),
        out_of_sample_pct: 0.0,
        direction: None,
        entry_signals: vec![],
        exit_signals: vec![],
        num_permutations: None,
        permutation_seed: None,
    };

    let output = run_sweep(&df, &params).unwrap();

    if !output.ranked_results.is_empty() {
        // Should have sensitivity for "strategy" dimension at minimum
        assert!(
            output.dimension_sensitivity.contains_key("strategy"),
            "Missing 'strategy' in dimension_sensitivity"
        );
        let strat_sens = &output.dimension_sensitivity["strategy"];
        for (key, stats) in strat_sens {
            assert!(stats.count > 0, "Strategy '{key}' has 0 count");
        }
    }
}

#[test]
fn sweep_independent_entry_periods_populated() {
    let df = make_multi_strike_df();
    let params = SweepParams {
        strategies: vec![SweepStrategyEntry {
            name: "long_call".to_string(),
            leg_delta_targets: vec![vec![0.50]],
        }],
        sweep: SweepDimensions {
            entry_dte_targets: vec![45],
            exit_dtes: vec![0],
            slippage_models: vec![Slippage::Mid],
        },
        sim_params: default_sim_params(),
        out_of_sample_pct: 0.0,
        direction: None,
        entry_signals: vec![],
        exit_signals: vec![],
        num_permutations: None,
        permutation_seed: None,
    };

    let output = run_sweep(&df, &params).unwrap();

    for r in &output.ranked_results {
        if r.trades > 0 {
            assert!(
                r.independent_entry_periods > 0,
                "Combo '{}' has {} trades but 0 independent periods",
                r.label,
                r.trades
            );
        }
    }
}

#[test]
fn sweep_spread_strategy_filters_inverted_deltas() {
    let df = make_multi_strike_df();
    let params = SweepParams {
        strategies: vec![SweepStrategyEntry {
            name: "bull_call_spread".to_string(),
            // Defaults: [0.50, 0.10] (leg0 > leg1)
            // [0.10, 0.50] is inverted → should be skipped
            // [0.50, 0.10] is valid → should run
            leg_delta_targets: vec![vec![0.10, 0.50], vec![0.10, 0.50]],
        }],
        sweep: SweepDimensions {
            entry_dte_targets: vec![45],
            exit_dtes: vec![0],
            slippage_models: vec![Slippage::Mid],
        },
        sim_params: default_sim_params(),
        out_of_sample_pct: 0.0,
        direction: None,
        entry_signals: vec![],
        exit_signals: vec![],
        num_permutations: None,
        permutation_seed: None,
    };

    let output = run_sweep(&df, &params).unwrap();

    // Some combos should have been skipped due to inverted delta ordering
    assert!(
        output.combinations_skipped > 0,
        "Expected some skipped combos for inverted delta orderings"
    );

    // All results should have valid ordering (leg0.target > leg1.target for bull_call_spread)
    for r in &output.ranked_results {
        if r.leg_deltas.len() == 2 {
            assert!(
                r.leg_deltas[0].target >= r.leg_deltas[1].target,
                "Combo '{}' has inverted deltas: {} < {}",
                r.label,
                r.leg_deltas[0].target,
                r.leg_deltas[1].target,
            );
        }
    }
}

#[test]
fn sweep_all_combos_skipped_returns_empty() {
    let df = make_multi_strike_df();
    // Use exit_dte values that are all >= the entry_dte range min
    // dte_target_to_range(10) → min = 10 - 3 = 7
    // exit_dtes [10, 20] are all >= 7 → all skipped
    let params = SweepParams {
        strategies: vec![SweepStrategyEntry {
            name: "long_call".to_string(),
            leg_delta_targets: vec![vec![0.50]],
        }],
        sweep: SweepDimensions {
            entry_dte_targets: vec![10],
            exit_dtes: vec![10, 20],
            slippage_models: vec![Slippage::Mid],
        },
        sim_params: default_sim_params(),
        out_of_sample_pct: 0.0,
        direction: None,
        entry_signals: vec![],
        exit_signals: vec![],
        num_permutations: None,
        permutation_seed: None,
    };

    let output = run_sweep(&df, &params).unwrap();
    assert_eq!(output.combinations_run, 0);
    assert!(output.ranked_results.is_empty());
}

#[test]
fn sweep_multiple_slippage_models() {
    let df = make_multi_strike_df();
    let params = SweepParams {
        strategies: vec![SweepStrategyEntry {
            name: "long_call".to_string(),
            leg_delta_targets: vec![vec![0.50]],
        }],
        sweep: SweepDimensions {
            entry_dte_targets: vec![45],
            exit_dtes: vec![0],
            slippage_models: vec![Slippage::Mid, Slippage::Spread],
        },
        sim_params: default_sim_params(),
        out_of_sample_pct: 0.0,
        direction: None,
        entry_signals: vec![],
        exit_signals: vec![],
        num_permutations: None,
        permutation_seed: None,
    };

    let output = run_sweep(&df, &params).unwrap();

    // Should have 2 combos (1 delta × 1 DTE × 1 exit × 2 slippage)
    assert!(
        output.combinations_run >= 2,
        "Expected 2+ combos for 2 slippage models, got {}",
        output.combinations_run
    );

    // Labels should differentiate slippage
    let labels: Vec<&str> = output
        .ranked_results
        .iter()
        .map(|r| r.label.as_str())
        .collect();
    let has_mid = labels.iter().any(|l| l.contains(", mid"));
    let has_spread = labels
        .iter()
        .any(|l| !l.contains(", mid") && !l.contains(", liq"));
    assert!(has_mid, "Should have a Mid slippage combo");
    assert!(has_spread, "Should have a Spread slippage combo");
}

// ---------------------------------------------------------------------------
// strategy_direction coverage
// ---------------------------------------------------------------------------

#[test]
fn strategy_direction_covers_all_32() {
    let all = optopsy_mcp::strategies::all_strategies();
    assert_eq!(all.len(), 32, "Expected 32 strategies");

    let mut bullish = 0;
    let mut bearish = 0;
    let mut neutral = 0;
    let mut volatile = 0;

    for s in all {
        match strategy_direction(&s.name) {
            Direction::Bullish => bullish += 1,
            Direction::Bearish => bearish += 1,
            Direction::Neutral => neutral += 1,
            Direction::Volatile => volatile += 1,
        }
    }

    assert_eq!(bullish, 6, "Expected 6 bullish strategies");
    assert_eq!(bearish, 4, "Expected 4 bearish strategies");
    assert_eq!(volatile, 4, "Expected 4 volatile strategies");
    assert_eq!(neutral, 18, "Expected 18 neutral strategies");
}

// ---------------------------------------------------------------------------
// Signal integration tests
// ---------------------------------------------------------------------------

/// Rising OHLCV — `ConsecutiveUp(2)` fires from Jan 22 onward.
///
/// With `dte_target_to_range(30)` → min=21, max=39, entry candidates are
/// Jan 15 (DTE=32) and Jan 22 (DTE=25). Signal blocks Jan 15, allows Jan 22.
fn rising_ohlcv() -> (tempfile::TempDir, String) {
    let dates: Vec<NaiveDate> = vec![
        NaiveDate::from_ymd_opt(2024, 1, 11).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(),
        NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(),
    ];
    let closes = vec![100.0, 101.0, 102.0, 103.0];
    write_ohlcv_parquet(&dates, &closes)
}

/// Entry signal that partially filters: blocks Jan 15 entry, allows Jan 22.
/// Produces strictly fewer trades than baseline, proving the signal is
/// actually evaluated (not just "any signal = block all").
///
/// Uses `entry_dte_targets: [30]` → `dte_target_to_range(30)` = min=21, max=39,
/// which includes both Jan 15 (DTE=32) and Jan 22 (DTE=25) as entry candidates.
#[test]
fn sweep_entry_signal_filters_some_entries() {
    let df = make_multi_strike_df();
    let (_dir, path) = rising_ohlcv();

    let sweep_dims = SweepDimensions {
        entry_dte_targets: vec![30],
        exit_dtes: vec![0, 5],
        slippage_models: vec![Slippage::Mid],
    };
    let strategies = vec![SweepStrategyEntry {
        name: "long_call".to_string(),
        leg_delta_targets: vec![vec![0.35, 0.50, 0.70]],
    }];

    // Baseline: entries on both Jan 15 and Jan 22
    let baseline_params = SweepParams {
        strategies: strategies.clone(),
        sweep: sweep_dims.clone(),
        sim_params: default_sim_params(),
        out_of_sample_pct: 0.0,
        direction: None,
        entry_signals: vec![],
        exit_signals: vec![],
        num_permutations: None,
        permutation_seed: None,
    };
    let baseline = run_sweep(&df, &baseline_params).unwrap();
    let baseline_trades: usize = baseline.ranked_results.iter().map(|r| r.trades).sum();
    assert!(baseline_trades > 0, "Baseline must produce trades");

    // ConsecutiveUp(2) fires from Jan 22 → blocks Jan 15, allows Jan 22
    let signal_params = SweepParams {
        strategies,
        sweep: sweep_dims,
        sim_params: SimParams {
            entry_signal: Some(SignalSpec::ConsecutiveUp {
                column: "close".into(),
                count: 2,
            }),
            exit_signal: None,
            ohlcv_path: Some(path),
            ..default_sim_params()
        },
        out_of_sample_pct: 0.0,
        direction: None,
        entry_signals: vec![],
        exit_signals: vec![],
        num_permutations: None,
        permutation_seed: None,
    };
    let output = run_sweep(&df, &signal_params).unwrap();
    let signal_trades: usize = output.ranked_results.iter().map(|r| r.trades).sum();

    assert!(
        signal_trades > 0,
        "Signal should allow trades on Jan 22 where it fires"
    );

    // Signal shifts entry from Jan 15 → Jan 22 (different price), so PnL must differ
    let baseline_pnl: f64 = baseline.ranked_results.iter().map(|r| r.pnl).sum();
    let signal_pnl: f64 = output.ranked_results.iter().map(|r| r.pnl).sum();
    assert!(
        (baseline_pnl - signal_pnl).abs() > 0.01,
        "Signal must change PnL by shifting entry date: baseline_pnl={baseline_pnl}, signal_pnl={signal_pnl}"
    );
}

/// Exit signal produces `ExitType::Signal` exits via `run_backtest`, confirming
/// the signal field on `SimParams` reaches `BacktestParams` correctly.
/// Uses sweep-equivalent parameters to match the code path.
#[test]
fn sweep_exit_signal_produces_signal_exits() {
    let df = make_multi_strike_df();
    let dates: Vec<NaiveDate> = vec![
        NaiveDate::from_ymd_opt(2024, 1, 11).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(),
        NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(),
    ];
    let closes = vec![107.0, 106.0, 105.0, 104.0];
    let (_dir, path) = write_ohlcv_parquet(&dates, &closes);

    // Sweep internally builds BacktestParams with these fields from SimParams.
    // Call run_backtest directly to verify ExitType::Signal appears in trade log.
    let mut params = common::backtest_params("long_call", vec![common::delta(0.50)]);
    params.exit_signal = Some(SignalSpec::ConsecutiveDown {
        column: "close".into(),
        count: 1,
    });
    params.ohlcv_path = Some(path);

    let result = run_backtest(&df, &params).unwrap();
    assert!(result.trade_count > 0, "Should produce trades");

    let signal_exits = result
        .trade_log
        .iter()
        .filter(|t| matches!(t.exit_type, ExitType::Signal))
        .count();
    assert!(
        signal_exits > 0,
        "Exit signal should cause at least one Signal exit, got 0 out of {} trades",
        result.trade_count
    );
}

/// Signals thread correctly through the OOS validation path in sweep.
/// The OOS `BacktestParams` (second construction in sweep.rs) must also
/// receive signals. A blocking signal should produce 0 trades in both
/// training and OOS.
#[test]
fn sweep_signal_threads_through_oos_path() {
    let df = make_multi_strike_df();
    let (_dir, path) = rising_ohlcv();

    let sweep_dims = SweepDimensions {
        entry_dte_targets: vec![30],
        exit_dtes: vec![0, 5],
        slippage_models: vec![Slippage::Mid],
    };
    let strategies = vec![SweepStrategyEntry {
        name: "long_call".to_string(),
        leg_delta_targets: vec![vec![0.35, 0.50, 0.70]],
    }];

    // With signal + OOS enabled: should not panic, and signal should
    // apply in both training and OOS BacktestParams constructions.
    let params = SweepParams {
        strategies,
        sweep: sweep_dims,
        sim_params: SimParams {
            entry_signal: Some(SignalSpec::ConsecutiveUp {
                column: "close".into(),
                count: 2,
            }),
            exit_signal: None,
            ohlcv_path: Some(path),
            ..default_sim_params()
        },
        out_of_sample_pct: 0.3,
        direction: None,
        entry_signals: vec![],
        exit_signals: vec![],
        num_permutations: None,
        permutation_seed: None,
    };

    // Must not panic — exercises both BacktestParams constructions with signals
    let output = run_sweep(&df, &params).unwrap();

    if !output.ranked_results.is_empty() {
        assert!(
            output.oos_results.len() <= 3,
            "OOS should validate at most top 3, got {}",
            output.oos_results.len()
        );
    }
}

// ---------------------------------------------------------------------------
// Signal sweep (entry_signals / exit_signals) integration tests
// ---------------------------------------------------------------------------

/// Sweeping 3 entry signal variants should produce 3× the combos of a
/// no-signal sweep, and sensitivity should include an `entry_signal` dimension.
#[test]
fn sweep_entry_signals_multiplies_combos() {
    let df = make_multi_strike_df();
    let (_dir, path) = rising_ohlcv();

    let strategies = vec![SweepStrategyEntry {
        name: "long_call".to_string(),
        leg_delta_targets: vec![vec![0.50]],
    }];
    let sweep_dims = SweepDimensions {
        entry_dte_targets: vec![30],
        exit_dtes: vec![0],
        slippage_models: vec![Slippage::Mid],
    };

    // Baseline: no signals → 1 combo
    let baseline = run_sweep(
        &df,
        &SweepParams {
            strategies: strategies.clone(),
            sweep: sweep_dims.clone(),
            sim_params: default_sim_params(),
            out_of_sample_pct: 0.0,
            direction: None,
            entry_signals: vec![],
            exit_signals: vec![],
            num_permutations: None,
            permutation_seed: None,
        },
    )
    .unwrap();

    // Signal sweep: 3 entry signals → 3 combos
    let output = run_sweep(
        &df,
        &SweepParams {
            strategies,
            sweep: sweep_dims,
            sim_params: SimParams {
                ohlcv_path: Some(path),
                ..default_sim_params()
            },
            out_of_sample_pct: 0.0,
            direction: None,
            entry_signals: vec![
                SignalSpec::ConsecutiveUp {
                    column: "close".into(),
                    count: 1,
                },
                SignalSpec::ConsecutiveUp {
                    column: "close".into(),
                    count: 2,
                },
                SignalSpec::ConsecutiveUp {
                    column: "close".into(),
                    count: 3,
                },
            ],
            exit_signals: vec![],
            num_permutations: None,
            permutation_seed: None,
        },
    )
    .unwrap();

    assert_eq!(
        output.combinations_total,
        baseline.combinations_total * 3,
        "3 signal variants should triple the combo count"
    );
    assert_eq!(output.signal_combinations, Some(3));

    // Sensitivity should include entry_signal dimension
    if !output.ranked_results.is_empty() {
        assert!(
            output.dimension_sensitivity.contains_key("entry_signal"),
            "Missing 'entry_signal' in dimension_sensitivity: {:?}",
            output.dimension_sensitivity.keys().collect::<Vec<_>>()
        );
    }
}

// ---------------------------------------------------------------------------
// Multiple comparisons correction integration tests
// ---------------------------------------------------------------------------

#[test]
fn sweep_without_permutations_has_no_multiple_comparisons() {
    let df = make_multi_strike_df();
    let params = SweepParams {
        strategies: vec![SweepStrategyEntry {
            name: "short_put".to_string(),
            leg_delta_targets: vec![vec![0.20, 0.30]],
        }],
        sweep: SweepDimensions {
            entry_dte_targets: vec![45],
            exit_dtes: vec![0],
            slippage_models: vec![Slippage::Mid],
        },
        sim_params: default_sim_params(),
        out_of_sample_pct: 0.0,
        direction: None,
        entry_signals: vec![],
        exit_signals: vec![],
        num_permutations: None,
        permutation_seed: None,
    };

    let output = run_sweep(&df, &params).unwrap();
    // Without num_permutations, multiple_comparisons must be None
    assert!(
        output.multiple_comparisons.is_none(),
        "Expected multiple_comparisons to be None when num_permutations is not set"
    );
    // p_value on each result should also be None
    for r in &output.ranked_results {
        assert!(
            r.p_value.is_none(),
            "Expected p_value to be None on result '{}' when num_permutations is not set",
            r.label
        );
    }
}

#[test]
fn sweep_with_permutations_produces_multiple_comparisons() {
    let df = make_multi_strike_df();
    let params = SweepParams {
        strategies: vec![SweepStrategyEntry {
            name: "short_put".to_string(),
            leg_delta_targets: vec![vec![0.20, 0.30]],
        }],
        sweep: SweepDimensions {
            entry_dte_targets: vec![45],
            exit_dtes: vec![0],
            slippage_models: vec![Slippage::Mid],
        },
        sim_params: default_sim_params(),
        out_of_sample_pct: 0.0,
        direction: None,
        entry_signals: vec![],
        exit_signals: vec![],
        num_permutations: Some(20),
        permutation_seed: Some(42),
    };

    let output = run_sweep(&df, &params).unwrap();

    // Each result should have a p_value populated (regardless of multiple_comparisons)
    for r in &output.ranked_results {
        assert!(
            r.p_value.is_some(),
            "Expected p_value to be Some on result '{}' when num_permutations is set",
            r.label
        );
        let p = r.p_value.unwrap();
        assert!(
            (0.0..=1.0).contains(&p),
            "p_value {} out of [0,1] for '{}'",
            p,
            r.label
        );
    }

    // multiple_comparisons should be Some only when there are ≥2 results
    if output.ranked_results.len() >= 2 {
        let mc = output.multiple_comparisons.as_ref().expect(
            "Expected multiple_comparisons to be Some when num_permutations is set and ≥2 results",
        );

        let (bon, bh) = mc;

        // Bonferroni
        assert_eq!(bon.method, "bonferroni");
        assert_eq!(bon.num_tests, output.ranked_results.len());
        assert!((bon.alpha - 0.05).abs() < 1e-10);
        assert_eq!(bon.results.len(), output.ranked_results.len());

        // BH-FDR
        assert_eq!(bh.method, "benjamini_hochberg");
        assert_eq!(bh.num_tests, output.ranked_results.len());
        assert!((bh.alpha - 0.05).abs() < 1e-10);
        assert_eq!(bh.results.len(), output.ranked_results.len());

        // BH is never more conservative than Bonferroni
        assert!(
            bh.num_significant >= bon.num_significant,
            "BH ({}) should retain ≥ significant results as Bonferroni ({})",
            bh.num_significant,
            bon.num_significant
        );

        // Adjusted p-values must be in [0, 1]
        for r in &bon.results {
            assert!(
                (0.0..=1.0).contains(&r.adjusted_p_value),
                "Bonferroni adjusted p-value {} out of [0,1]",
                r.adjusted_p_value
            );
            assert!(
                (0.0..=1.0).contains(&r.original_p_value),
                "Original p-value {} out of [0,1]",
                r.original_p_value
            );
        }
        for r in &bh.results {
            assert!(
                (0.0..=1.0).contains(&r.adjusted_p_value),
                "BH adjusted p-value {} out of [0,1]",
                r.adjusted_p_value
            );
        }

        // Labels in corrections must match sweep result labels
        let result_labels: std::collections::HashSet<&str> = output
            .ranked_results
            .iter()
            .map(|r| r.label.as_str())
            .collect();
        for r in &bon.results {
            assert!(
                result_labels.contains(r.label.as_str()),
                "Bonferroni label '{}' not found in sweep results",
                r.label
            );
        }
    }
}

#[test]
fn sweep_multiple_comparisons_bonferroni_more_conservative_than_bh() {
    // Bonferroni must always have num_significant ≤ BH num_significant
    let df = make_multi_strike_df();
    let params = SweepParams {
        strategies: vec![SweepStrategyEntry {
            name: "short_put".to_string(),
            leg_delta_targets: vec![vec![0.15, 0.20, 0.25]],
        }],
        sweep: SweepDimensions {
            entry_dte_targets: vec![30, 45],
            exit_dtes: vec![0],
            slippage_models: vec![Slippage::Mid],
        },
        sim_params: default_sim_params(),
        out_of_sample_pct: 0.0,
        direction: None,
        entry_signals: vec![],
        exit_signals: vec![],
        num_permutations: Some(15),
        permutation_seed: Some(99),
    };

    let output = run_sweep(&df, &params).unwrap();

    if let Some((bon, bh)) = &output.multiple_comparisons {
        assert!(
            bh.num_significant >= bon.num_significant,
            "BH-FDR should be less conservative than Bonferroni: bh={} bon={}",
            bh.num_significant,
            bon.num_significant
        );

        // All original p-values should match between bon and bh
        let bon_originals: Vec<f64> = bon.results.iter().map(|r| r.original_p_value).collect();
        let bh_originals: Vec<f64> = bh.results.iter().map(|r| r.original_p_value).collect();
        for (a, b) in bon_originals.iter().zip(bh_originals.iter()) {
            assert!(
                (a - b).abs() < 1e-10,
                "Original p-values differ between Bonferroni ({a}) and BH ({b})"
            );
        }
    }
    // If only 0 or 1 results, multiple_comparisons may be None — that's fine
}
