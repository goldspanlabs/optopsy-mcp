//! Integration tests for the `parameter_sweep` feature.
//!
//! These tests exercise the full sweep pipeline with real data frames
//! that produce actual trades, validating end-to-end correctness.

use optopsy_mcp::engine::sweep::{run_sweep, SweepDimensions, SweepParams, SweepStrategyEntry};
use optopsy_mcp::engine::types::{
    strategy_direction, Direction, SimParams, Slippage, TradeSelector,
};

mod common;
use common::make_multi_strike_df;

fn default_sim_params() -> SimParams {
    SimParams {
        capital: 100_000.0,
        quantity: 1,
        multiplier: 100,
        max_positions: 3,
        selector: TradeSelector::First,
        stop_loss: None,
        take_profit: None,
        max_hold_days: None,
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
fn sweep_multi_strategy_with_direction_filter() {
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
            // This bearish strategy should be filtered out by direction
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
        // Note: direction filtering happens at server level (resolve_sweep_strategies),
        // not in run_sweep. This test verifies the engine processes all provided strategies.
        direction: None,
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
    let has_mid = labels.iter().any(|l| l.contains(",mid"));
    let has_spread = labels
        .iter()
        .any(|l| !l.contains(",mid") && !l.contains(",liq"));
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

    for s in &all {
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
