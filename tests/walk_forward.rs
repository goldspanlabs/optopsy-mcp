//! Integration tests for the `walk_forward` analysis feature.
//!
//! These tests exercise the full walk-forward pipeline with real data frames
//! that produce actual trades, validating window generation, backtest execution
//! across windows, and aggregate computation.

use optopsy_mcp::engine::walk_forward::run_walk_forward;

mod common;
use common::{backtest_params, delta, make_multi_strike_df};

// ---------------------------------------------------------------------------
// Full pipeline integration tests
// ---------------------------------------------------------------------------

#[test]
fn walk_forward_produces_windows_and_aggregate() {
    let df = make_multi_strike_df();
    // Data spans Jan 15 to Feb 11 = 28 days inclusive.
    // Use train=14, test=7 (21 total, fits in 28 days).
    // step defaults to test_days=7.
    let params = backtest_params("short_put", vec![delta(0.20)]);

    let result = run_walk_forward(&df, &params, 14, 7, None).unwrap();

    // Should have at least 1 window
    assert!(
        !result.windows.is_empty(),
        "Expected at least 1 walk-forward window"
    );

    // Aggregate should reflect the windows
    let agg = &result.aggregate;
    assert_eq!(agg.successful_windows, result.windows.len());

    // Window numbering should be sequential starting at 1
    for (i, w) in result.windows.iter().enumerate() {
        assert_eq!(
            w.window_number,
            i + 1,
            "Window numbering should be sequential"
        );
    }

    // Train end == test start (contiguous windows)
    for w in &result.windows {
        assert_eq!(
            w.train_end, w.test_start,
            "Train end should equal test start for contiguous windows"
        );
        assert!(
            w.train_start < w.train_end,
            "Train start should be before train end"
        );
        assert!(
            w.test_start < w.test_end,
            "Test start should be before test end"
        );
    }

    // Pct profitable should be in [0, 100]
    assert!(
        (0.0..=100.0).contains(&agg.pct_profitable_windows),
        "pct_profitable_windows {} out of [0, 100]",
        agg.pct_profitable_windows,
    );

    // std_test_sharpe should be non-negative
    assert!(
        agg.std_test_sharpe >= 0.0,
        "std_test_sharpe should be non-negative"
    );

    // total_test_pnl should equal sum of window test_pnls
    let sum_test_pnl: f64 = result.windows.iter().map(|w| w.test_pnl).sum();
    assert!(
        (agg.total_test_pnl - sum_test_pnl).abs() < 1e-6,
        "total_test_pnl ({}) should equal sum of window test_pnls ({})",
        agg.total_test_pnl,
        sum_test_pnl,
    );
}

#[test]
fn walk_forward_insufficient_data_errors() {
    let df = make_multi_strike_df();
    // Data spans 28 days, request train=20 + test=15 = 35
    let params = backtest_params("short_put", vec![delta(0.20)]);

    let result = run_walk_forward(&df, &params, 20, 15, None);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("walk-forward requires at least"),
        "Should mention minimum data requirement"
    );
}

#[test]
fn walk_forward_step_days_too_small_errors() {
    let df = make_multi_strike_df();
    let params = backtest_params("short_put", vec![delta(0.20)]);

    // Explicit step_days < 5 should error
    let result = run_walk_forward(&df, &params, 14, 7, Some(3));
    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains("step_days"),
        "Should mention step_days in error"
    );
}

#[test]
fn walk_forward_test_days_as_step_too_small_errors() {
    let df = make_multi_strike_df();
    let params = backtest_params("short_put", vec![delta(0.20)]);

    // test_days=3 used as step when step_days is None → should error (< 5)
    let result = run_walk_forward(&df, &params, 14, 3, None);
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("test_days"), "Should mention test_days: {msg}");
}

#[test]
fn walk_forward_train_days_zero_errors() {
    let df = make_multi_strike_df();
    let params = backtest_params("short_put", vec![delta(0.20)]);

    let result = run_walk_forward(&df, &params, 0, 7, None);
    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains("train_days"),
        "Should mention train_days"
    );
}

#[test]
fn walk_forward_custom_step_days() {
    let df = make_multi_strike_df();
    // Data spans 28 days. train=10, test=7, step=5
    // Should produce more windows than default step=test_days=7
    let params = backtest_params("short_put", vec![delta(0.20)]);

    let result_default = run_walk_forward(&df, &params, 10, 7, None);
    let result_step5 = run_walk_forward(&df, &params, 10, 7, Some(5));

    // Both should succeed or both fail (same data), but step=5 should have >= as many windows
    match (result_default, result_step5) {
        (Ok(r_default), Ok(r_step5)) => {
            assert!(
                r_step5.windows.len() >= r_default.windows.len(),
                "Smaller step should produce at least as many windows ({} vs {})",
                r_step5.windows.len(),
                r_default.windows.len(),
            );
        }
        (Err(_), Err(_)) => {} // Both failed due to no valid windows — acceptable
        (Ok(_), Err(e)) => panic!("step=5 failed but default succeeded: {e}"),
        (Err(e), Ok(_)) => panic!("default failed but step=5 succeeded: {e}"),
    }
}

#[test]
fn walk_forward_sharpe_decay_is_coherent() {
    let df = make_multi_strike_df();
    let params = backtest_params("short_put", vec![delta(0.20)]);

    let result = run_walk_forward(&df, &params, 14, 7, None).unwrap();
    let agg = &result.aggregate;

    // avg_train_test_sharpe_decay should equal mean(train_sharpe - test_sharpe)
    let expected_decay: f64 = result
        .windows
        .iter()
        .map(|w| w.train_sharpe - w.test_sharpe)
        .sum::<f64>()
        / result.windows.len() as f64;

    assert!(
        (agg.avg_train_test_sharpe_decay - expected_decay).abs() < 1e-6,
        "Sharpe decay ({}) should match manual calculation ({})",
        agg.avg_train_test_sharpe_decay,
        expected_decay,
    );
}

#[test]
fn walk_forward_multi_leg_strategy() {
    let df = make_multi_strike_df();
    let params = backtest_params("bull_put_spread", vec![delta(0.40), delta(0.20)]);

    let result = run_walk_forward(&df, &params, 14, 7, None);

    // Multi-leg strategy should work through the walk-forward pipeline
    // May produce windows with 0 trades if spreads don't match, but should not error
    match result {
        Ok(r) => {
            assert!(!r.windows.is_empty());
            assert_eq!(r.aggregate.successful_windows, r.windows.len());
        }
        Err(e) => {
            // "No valid windows" is acceptable if spread legs don't match in small windows
            assert!(e.to_string().contains("No valid"), "Unexpected error: {e}");
        }
    }
}
