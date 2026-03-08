//! Integration tests for the `permutation_test` feature.
//!
//! These tests exercise the full permutation pipeline with real data frames
//! that produce actual trades, validating end-to-end correctness of shuffled
//! reruns and p-value computation.

use optopsy_mcp::engine::core::run_backtest;
use optopsy_mcp::engine::permutation::{run_permutation_test, PermutationParams};

mod common;
use common::{backtest_params, delta, make_multi_strike_df};

// ---------------------------------------------------------------------------
// Full pipeline integration tests
// ---------------------------------------------------------------------------

#[test]
fn permutation_test_produces_metric_results() {
    let df = make_multi_strike_df();
    let params = backtest_params("short_put", vec![delta(0.20)]);

    // Verify the strategy produces trades before running permutation test
    let real = run_backtest(&df, &params).unwrap();
    assert!(real.trade_count > 0, "Need trades for permutation test");

    let perm_params = PermutationParams {
        num_permutations: 20,
        seed: Some(42),
    };

    let output = run_permutation_test(&df, &params, &perm_params, &None::<std::collections::HashSet<chrono::NaiveDate>>, None::<&std::collections::HashSet<chrono::NaiveDate>>).unwrap();

    // Should complete all permutations (or most)
    assert!(
        output.num_completed > 0,
        "Expected some permutations to complete, got 0"
    );
    assert_eq!(output.num_permutations, 20);

    // Should have 5 metric results: sharpe, total_pnl, win_rate, profit_factor, cagr
    assert_eq!(
        output.metric_results.len(),
        5,
        "Expected 5 metrics, got {}",
        output.metric_results.len()
    );

    let metric_names: Vec<&str> = output.metric_results.iter().map(|m| m.metric_name.as_str()).collect();
    assert!(metric_names.contains(&"sharpe"));
    assert!(metric_names.contains(&"total_pnl"));
    assert!(metric_names.contains(&"win_rate"));
    assert!(metric_names.contains(&"profit_factor"));
    assert!(metric_names.contains(&"cagr"));

    // Each metric should have valid statistical properties
    for m in &output.metric_results {
        assert!(
            (0.0..=1.0).contains(&m.p_value),
            "{}: p_value {} out of [0,1]",
            m.metric_name,
            m.p_value,
        );
        assert!(
            m.std_permuted >= 0.0,
            "{}: negative std_permuted {}",
            m.metric_name,
            m.std_permuted,
        );
        assert!(
            m.percentile_5 <= m.percentile_95,
            "{}: p5 ({}) > p95 ({})",
            m.metric_name,
            m.percentile_5,
            m.percentile_95,
        );
        // Histogram should have non-zero total count matching num_completed
        if !m.histogram.is_empty() {
            let total: usize = m.histogram.iter().map(|b| b.count).sum();
            assert_eq!(
                total, output.num_completed,
                "{}: histogram total {} != num_completed {}",
                m.metric_name, total, output.num_completed
            );
        }
    }

    // Real result should match the standalone backtest
    assert_eq!(output.real_result.trade_count, real.trade_count);
    assert!(
        (output.real_result.total_pnl - real.total_pnl).abs() < f64::EPSILON,
        "Real result PnL should match standalone backtest"
    );
}

#[test]
fn permutation_test_deterministic_with_seed() {
    let df = make_multi_strike_df();
    let params = backtest_params("short_put", vec![delta(0.20)]);

    let perm_params = PermutationParams {
        num_permutations: 10,
        seed: Some(123),
    };

    let out1 = run_permutation_test(&df, &params, &perm_params, &None::<std::collections::HashSet<chrono::NaiveDate>>, None::<&std::collections::HashSet<chrono::NaiveDate>>).unwrap();
    let out2 = run_permutation_test(&df, &params, &perm_params, &None::<std::collections::HashSet<chrono::NaiveDate>>, None::<&std::collections::HashSet<chrono::NaiveDate>>).unwrap();

    assert_eq!(out1.num_completed, out2.num_completed);
    for (m1, m2) in out1.metric_results.iter().zip(out2.metric_results.iter()) {
        assert!(
            (m1.p_value - m2.p_value).abs() < f64::EPSILON,
            "{}: p_value differs between runs ({} vs {})",
            m1.metric_name,
            m1.p_value,
            m2.p_value,
        );
        assert!(
            (m1.mean_permuted - m2.mean_permuted).abs() < f64::EPSILON,
            "{}: mean_permuted differs between runs",
            m1.metric_name,
        );
    }
}

#[test]
fn permutation_test_unknown_strategy_errors() {
    let df = make_multi_strike_df();
    let params = backtest_params("nonexistent_strategy", vec![delta(0.30)]);

    let perm_params = PermutationParams {
        num_permutations: 5,
        seed: Some(1),
    };

    let result = run_permutation_test(&df, &params, &perm_params, &None::<std::collections::HashSet<chrono::NaiveDate>>, None::<&std::collections::HashSet<chrono::NaiveDate>>);
    assert!(result.is_err(), "Should fail for unknown strategy");
    assert!(
        result.unwrap_err().to_string().contains("Unknown strategy"),
        "Error should mention unknown strategy"
    );
}

#[test]
fn permutation_test_no_candidates_returns_empty_metrics() {
    let df = make_multi_strike_df();
    // Use a delta that won't match any options in the data (delta 0.99)
    let mut params = backtest_params("short_put", vec![delta(0.99)]);
    // Narrow delta range so nothing matches
    params.leg_deltas[0].min = 0.98;
    params.leg_deltas[0].max = 0.99;

    let perm_params = PermutationParams {
        num_permutations: 5,
        seed: Some(1),
    };

    let result = run_permutation_test(&df, &params, &perm_params, &None::<std::collections::HashSet<chrono::NaiveDate>>, None::<&std::collections::HashSet<chrono::NaiveDate>>);
    // Should either error (no trades in real backtest) or return empty metrics (no candidates)
    if let Ok(output) = result {
        assert_eq!(output.num_completed, 0);
        assert!(output.metric_results.is_empty());
    }
    // Err is also acceptable — real backtest may fail
}

#[test]
fn permutation_test_with_entry_date_filter() {
    let df = make_multi_strike_df();
    let params = backtest_params("short_put", vec![delta(0.20)]);

    let perm_params = PermutationParams {
        num_permutations: 10,
        seed: Some(42),
    };

    // Only allow entry on Jan 15
    let mut allowed = std::collections::HashSet::new();
    allowed.insert(chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());

    let output = run_permutation_test(&df, &params, &perm_params, &Some(allowed), None::<&std::collections::HashSet<chrono::NaiveDate>>).unwrap();

    // Should still produce results (Jan 15 is a valid entry date in the test data)
    assert!(output.real_result.trade_count > 0 || output.num_completed == 0);
}

#[test]
fn permutation_test_multi_leg_strategy() {
    let df = make_multi_strike_df();
    // Bull put spread: short higher delta put, long lower delta put
    let params = backtest_params("bull_put_spread", vec![delta(0.40), delta(0.20)]);

    let perm_params = PermutationParams {
        num_permutations: 15,
        seed: Some(99),
    };

    let result = run_permutation_test(&df, &params, &perm_params, &None::<std::collections::HashSet<chrono::NaiveDate>>, None::<&std::collections::HashSet<chrono::NaiveDate>>);

    // Multi-leg strategy should work through the pipeline without panicking.
    // May produce 0 candidates if spreads don't match in synthetic data.
    if let Ok(output) = result {
        assert_eq!(output.num_permutations, 15);
        if output.num_completed > 0 {
            assert_eq!(output.metric_results.len(), 5);
            for m in &output.metric_results {
                assert!(
                    (0.0..=1.0).contains(&m.p_value),
                    "{}: p_value {} out of [0,1]",
                    m.metric_name,
                    m.p_value,
                );
            }
        }
    }
    // Err is also acceptable — spread may not match in synthetic data
}
