//! Integration tests for the `bayesian_optimize` feature.
//!
//! These tests exercise the Bayesian optimization pipeline with the same
//! synthetic data used by the parameter sweep tests, validating the GP-guided
//! search, convergence, OOS validation, and edge-case handling.

use optopsy_mcp::engine::bayesian::{run_bayesian_optimization, BayesianParams, Objective};
use optopsy_mcp::engine::types::{Slippage, TradeSelector};

mod common;
use common::make_multi_strike_df;

fn default_sim_params() -> optopsy_mcp::engine::types::SimParams {
    optopsy_mcp::engine::types::SimParams {
        capital: 100_000.0,
        quantity: 1,
        sizing: None,
        multiplier: 100,
        max_positions: 3,
        selector: TradeSelector::First,
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
    }
}

// ---------------------------------------------------------------------------
// Basic pipeline tests
// ---------------------------------------------------------------------------

#[test]
fn bayesian_single_leg_produces_ranked_results() {
    let df = make_multi_strike_df();
    let params = BayesianParams {
        strategy: "long_call".to_string(),
        leg_delta_bounds: vec![(0.20, 0.70)],
        entry_dte_bounds: (30, 60),
        exit_dtes: vec![0, 5],
        slippage_models: vec![Slippage::Mid],
        sim_params: default_sim_params(),
        max_evaluations: 15,
        initial_samples: 8,
        out_of_sample_pct: 0.0,
        seed: Some(42),
        objective: Objective::Sharpe,
    };

    let output = run_bayesian_optimization(&df, &params).unwrap();

    // Should have evaluated configurations
    assert!(
        !output.ranked_results.is_empty(),
        "Expected at least one successful evaluation"
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

    // Convergence trace has one entry per successful evaluation (non-finite entries
    // from failed evaluations are filtered out before returning).
    assert!(
        output.convergence_trace.len() <= params.max_evaluations,
        "Convergence trace length should be <= max_evaluations"
    );

    // Convergence trace should be monotonically non-decreasing
    for w in output.convergence_trace.windows(2) {
        assert!(
            w[1] >= w[0],
            "Convergence trace not monotonic: {} -> {}",
            w[0],
            w[1],
        );
    }

    // At least one result should have actual trades
    let has_trades = output.ranked_results.iter().any(|r| r.trades > 0);
    assert!(
        has_trades,
        "Expected at least one evaluation to produce trades"
    );
}

#[test]
fn bayesian_two_leg_spread() {
    let df = make_multi_strike_df();
    let params = BayesianParams {
        strategy: "bull_call_spread".to_string(),
        leg_delta_bounds: vec![(0.40, 0.70), (0.15, 0.40)],
        entry_dte_bounds: (30, 60),
        exit_dtes: vec![0],
        slippage_models: vec![Slippage::Mid],
        sim_params: default_sim_params(),
        max_evaluations: 12,
        initial_samples: 6,
        out_of_sample_pct: 0.0,
        seed: Some(123),
        objective: Objective::Sharpe,
    };

    let output = run_bayesian_optimization(&df, &params).unwrap();

    // All results should be for the correct strategy
    for r in &output.ranked_results {
        assert_eq!(r.strategy, "bull_call_spread");
        assert_eq!(r.leg_deltas.len(), 2, "Bull call spread should have 2 legs");
    }
}

#[test]
fn bayesian_with_oos_validation() {
    let df = make_multi_strike_df();
    let params = BayesianParams {
        strategy: "long_call".to_string(),
        leg_delta_bounds: vec![(0.30, 0.70)],
        entry_dte_bounds: (30, 60),
        exit_dtes: vec![0, 5],
        slippage_models: vec![Slippage::Mid],
        sim_params: default_sim_params(),
        max_evaluations: 12,
        initial_samples: 6,
        out_of_sample_pct: 0.3,
        seed: Some(42),
        objective: Objective::Sharpe,
    };

    let output = run_bayesian_optimization(&df, &params).unwrap();

    // OOS validation should run on top results (up to 3)
    // With sparse synthetic data, some OOS backtests may fail silently,
    // but the pipeline should not error.
    if !output.ranked_results.is_empty() {
        assert!(
            output.oos_results.len() <= 3,
            "OOS should validate at most 3, got {}",
            output.oos_results.len()
        );
        for oos in &output.oos_results {
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

// ---------------------------------------------------------------------------
// Objective variants
// ---------------------------------------------------------------------------

#[test]
fn bayesian_sortino_objective() {
    let df = make_multi_strike_df();
    let params = BayesianParams {
        strategy: "long_call".to_string(),
        leg_delta_bounds: vec![(0.30, 0.60)],
        entry_dte_bounds: (30, 60),
        exit_dtes: vec![0],
        slippage_models: vec![Slippage::Mid],
        sim_params: default_sim_params(),
        max_evaluations: 10,
        initial_samples: 5,
        out_of_sample_pct: 0.0,
        seed: Some(99),
        objective: Objective::Sortino,
    };

    let output = run_bayesian_optimization(&df, &params).unwrap();
    assert_eq!(output.objective, "Sortino");

    // Results should be sorted by Sortino descending
    for w in output.ranked_results.windows(2) {
        assert!(
            w[0].sortino >= w[1].sortino || w[0].sortino.is_nan(),
            "Results not sorted by Sortino: {} >= {}",
            w[0].sortino,
            w[1].sortino,
        );
    }
}

#[test]
fn bayesian_calmar_objective() {
    let df = make_multi_strike_df();
    let params = BayesianParams {
        strategy: "long_call".to_string(),
        leg_delta_bounds: vec![(0.30, 0.60)],
        entry_dte_bounds: (30, 60),
        exit_dtes: vec![0],
        slippage_models: vec![Slippage::Mid],
        sim_params: default_sim_params(),
        max_evaluations: 10,
        initial_samples: 5,
        out_of_sample_pct: 0.0,
        seed: Some(77),
        objective: Objective::Calmar,
    };

    let output = run_bayesian_optimization(&df, &params).unwrap();
    assert_eq!(output.objective, "Calmar");
}

#[test]
fn bayesian_profit_factor_objective() {
    let df = make_multi_strike_df();
    let params = BayesianParams {
        strategy: "long_call".to_string(),
        leg_delta_bounds: vec![(0.30, 0.60)],
        entry_dte_bounds: (30, 60),
        exit_dtes: vec![0],
        slippage_models: vec![Slippage::Mid],
        sim_params: default_sim_params(),
        max_evaluations: 10,
        initial_samples: 5,
        out_of_sample_pct: 0.0,
        seed: Some(55),
        objective: Objective::ProfitFactor,
    };

    let output = run_bayesian_optimization(&df, &params).unwrap();
    assert_eq!(output.objective, "Profit Factor");
}

// ---------------------------------------------------------------------------
// Multiple slippage models (categorical sweep)
// ---------------------------------------------------------------------------

#[test]
fn bayesian_multiple_slippage_models() {
    let df = make_multi_strike_df();
    let params = BayesianParams {
        strategy: "long_call".to_string(),
        leg_delta_bounds: vec![(0.30, 0.60)],
        entry_dte_bounds: (30, 60),
        exit_dtes: vec![0, 5],
        slippage_models: vec![Slippage::Mid, Slippage::Spread],
        sim_params: default_sim_params(),
        max_evaluations: 12,
        initial_samples: 6,
        out_of_sample_pct: 0.0,
        seed: Some(42),
        objective: Objective::Sharpe,
    };

    let output = run_bayesian_optimization(&df, &params).unwrap();
    assert!(!output.ranked_results.is_empty());

    // Dimension sensitivity should have entries for slippage
    // (may not be present if all evaluations used the same slippage)
    // Just verify no panic and results are valid
    for r in &output.ranked_results {
        assert!(
            matches!(r.slippage, Slippage::Mid | Slippage::Spread),
            "Unexpected slippage model in result"
        );
    }
}

// ---------------------------------------------------------------------------
// Sensitivity analysis
// ---------------------------------------------------------------------------

#[test]
fn bayesian_produces_dimension_sensitivity() {
    let df = make_multi_strike_df();
    let params = BayesianParams {
        strategy: "long_call".to_string(),
        leg_delta_bounds: vec![(0.20, 0.70)],
        entry_dte_bounds: (30, 60),
        exit_dtes: vec![0, 5],
        slippage_models: vec![Slippage::Mid],
        sim_params: default_sim_params(),
        max_evaluations: 15,
        initial_samples: 8,
        out_of_sample_pct: 0.0,
        seed: Some(42),
        objective: Objective::Sharpe,
    };

    let output = run_bayesian_optimization(&df, &params).unwrap();

    // Sensitivity should have at least some dimensions if there are results
    if output.ranked_results.len() >= 2 {
        assert!(
            !output.dimension_sensitivity.is_empty(),
            "Expected non-empty dimension sensitivity with multiple results"
        );
    }
}

// ---------------------------------------------------------------------------
// Determinism with seed
// ---------------------------------------------------------------------------

#[test]
fn bayesian_deterministic_with_same_seed() {
    let df = make_multi_strike_df();
    let make_params = || BayesianParams {
        strategy: "long_call".to_string(),
        leg_delta_bounds: vec![(0.30, 0.60)],
        entry_dte_bounds: (30, 60),
        exit_dtes: vec![0],
        slippage_models: vec![Slippage::Mid],
        sim_params: default_sim_params(),
        max_evaluations: 10,
        initial_samples: 5,
        out_of_sample_pct: 0.0,
        seed: Some(42),
        objective: Objective::Sharpe,
    };

    let output1 = run_bayesian_optimization(&df, &make_params()).unwrap();
    let output2 = run_bayesian_optimization(&df, &make_params()).unwrap();

    assert_eq!(
        output1.ranked_results.len(),
        output2.ranked_results.len(),
        "Same seed should produce same number of results"
    );

    // Convergence traces should be identical
    assert_eq!(
        output1.convergence_trace, output2.convergence_trace,
        "Same seed should produce identical convergence traces"
    );
}

// ---------------------------------------------------------------------------
// Validation / error cases
// ---------------------------------------------------------------------------

#[test]
fn bayesian_fails_when_max_evals_lt_initial_samples() {
    let df = make_multi_strike_df();
    let params = BayesianParams {
        strategy: "long_call".to_string(),
        leg_delta_bounds: vec![(0.30, 0.60)],
        entry_dte_bounds: (30, 60),
        exit_dtes: vec![0],
        slippage_models: vec![Slippage::Mid],
        sim_params: default_sim_params(),
        max_evaluations: 3,
        initial_samples: 10,
        out_of_sample_pct: 0.0,
        seed: Some(42),
        objective: Objective::Sharpe,
    };

    let result = run_bayesian_optimization(&df, &params);
    assert!(
        result.is_err(),
        "Should fail when max_evaluations < initial_samples"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("max_evaluations") && err.contains("initial_samples"),
        "Error should mention the constraint: {err}"
    );
}

#[test]
fn bayesian_fails_with_empty_delta_bounds() {
    let df = make_multi_strike_df();
    let params = BayesianParams {
        strategy: "long_call".to_string(),
        leg_delta_bounds: vec![],
        entry_dte_bounds: (30, 60),
        exit_dtes: vec![0],
        slippage_models: vec![Slippage::Mid],
        sim_params: default_sim_params(),
        max_evaluations: 10,
        initial_samples: 5,
        out_of_sample_pct: 0.0,
        seed: Some(42),
        objective: Objective::Sharpe,
    };

    let result = run_bayesian_optimization(&df, &params);
    assert!(result.is_err(), "Should fail with empty leg_delta_bounds");
}

#[test]
fn bayesian_fails_with_empty_exit_dtes() {
    let df = make_multi_strike_df();
    let params = BayesianParams {
        strategy: "long_call".to_string(),
        leg_delta_bounds: vec![(0.30, 0.60)],
        entry_dte_bounds: (30, 60),
        exit_dtes: vec![],
        slippage_models: vec![Slippage::Mid],
        sim_params: default_sim_params(),
        max_evaluations: 10,
        initial_samples: 5,
        out_of_sample_pct: 0.0,
        seed: Some(42),
        objective: Objective::Sharpe,
    };

    let result = run_bayesian_optimization(&df, &params);
    assert!(result.is_err(), "Should fail with empty exit_dtes");
}

#[test]
fn bayesian_fails_with_empty_slippage() {
    let df = make_multi_strike_df();
    let params = BayesianParams {
        strategy: "long_call".to_string(),
        leg_delta_bounds: vec![(0.30, 0.60)],
        entry_dte_bounds: (30, 60),
        exit_dtes: vec![0],
        slippage_models: vec![],
        sim_params: default_sim_params(),
        max_evaluations: 10,
        initial_samples: 5,
        out_of_sample_pct: 0.0,
        seed: Some(42),
        objective: Objective::Sharpe,
    };

    let result = run_bayesian_optimization(&df, &params);
    assert!(result.is_err(), "Should fail with empty slippage_models");
}

#[test]
fn bayesian_fails_with_inverted_dte_bounds() {
    let df = make_multi_strike_df();
    let params = BayesianParams {
        strategy: "long_call".to_string(),
        leg_delta_bounds: vec![(0.30, 0.60)],
        entry_dte_bounds: (60, 30), // inverted
        exit_dtes: vec![0],
        slippage_models: vec![Slippage::Mid],
        sim_params: default_sim_params(),
        max_evaluations: 10,
        initial_samples: 5,
        out_of_sample_pct: 0.0,
        seed: Some(42),
        objective: Objective::Sharpe,
    };

    let result = run_bayesian_optimization(&df, &params);
    assert!(
        result.is_err(),
        "Should fail with inverted entry_dte_bounds"
    );
}

// ---------------------------------------------------------------------------
// Tool layer (format_bayesian)
// ---------------------------------------------------------------------------

#[test]
fn bayesian_tool_execute_formats_response() {
    let df = make_multi_strike_df();
    let params = BayesianParams {
        strategy: "long_call".to_string(),
        leg_delta_bounds: vec![(0.30, 0.60)],
        entry_dte_bounds: (30, 60),
        exit_dtes: vec![0],
        slippage_models: vec![Slippage::Mid],
        sim_params: default_sim_params(),
        max_evaluations: 10,
        initial_samples: 5,
        out_of_sample_pct: 0.0,
        seed: Some(42),
        objective: Objective::Sharpe,
    };

    let response = optopsy_mcp::tools::bayesian_optimize::execute(&df, &params).unwrap();

    // AI-enriched fields should be populated
    assert!(!response.summary.is_empty(), "Summary should not be empty");
    assert_eq!(response.objective, "Sharpe");
    assert_eq!(response.total_evaluations, 10);
    assert!(!response.convergence_trace.is_empty());
    assert!(!response.ranked_results.is_empty());
    assert!(
        !response.key_findings.is_empty(),
        "Key findings should not be empty"
    );
    assert!(
        !response.suggested_next_steps.is_empty(),
        "Suggested next steps should not be empty"
    );
}

// ---------------------------------------------------------------------------
// Put strategies
// ---------------------------------------------------------------------------

#[test]
fn bayesian_short_put_strategy() {
    let df = make_multi_strike_df();
    let params = BayesianParams {
        strategy: "short_put".to_string(),
        leg_delta_bounds: vec![(0.20, 0.55)],
        entry_dte_bounds: (30, 60),
        exit_dtes: vec![0, 5],
        slippage_models: vec![Slippage::Mid],
        sim_params: default_sim_params(),
        max_evaluations: 12,
        initial_samples: 6,
        out_of_sample_pct: 0.0,
        seed: Some(42),
        objective: Objective::Sharpe,
    };

    let output = run_bayesian_optimization(&df, &params).unwrap();

    for r in &output.ranked_results {
        assert_eq!(r.strategy, "short_put");
    }
}
