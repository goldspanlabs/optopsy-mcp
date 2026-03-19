//! Integration tests for the `walk_forward` analysis feature.
//!
//! These tests exercise the full walk-forward pipeline with real data frames
//! that produce actual trades, validating window generation, backtest execution
//! across windows, and aggregate computation.

use chrono::Datelike;
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

#[test]
fn walk_forward_aggregate_stats_match_window_data() {
    // Verify every aggregate statistic is correctly derived from individual windows.
    // This catches bugs in the aggregate computation (compute_aggregate).
    let df = make_multi_strike_df();
    let params = backtest_params("short_put", vec![delta(0.20)]);

    let result = run_walk_forward(&df, &params, 14, 7, None).unwrap();
    let agg = &result.aggregate;
    let windows = &result.windows;
    let n = windows.len() as f64;

    // 1. successful_windows == windows.len()
    assert_eq!(agg.successful_windows, windows.len());

    // 2. total_test_pnl == sum of window test_pnls
    let sum_test_pnl: f64 = windows.iter().map(|w| w.test_pnl).sum();
    assert!(
        (agg.total_test_pnl - sum_test_pnl).abs() < 1e-10,
        "total_test_pnl ({}) != sum of window test_pnls ({sum_test_pnl})",
        agg.total_test_pnl,
    );

    // 3. avg_test_pnl == total_test_pnl / n
    let expected_avg_pnl = sum_test_pnl / n;
    assert!(
        (agg.avg_test_pnl - expected_avg_pnl).abs() < 1e-10,
        "avg_test_pnl ({}) != expected ({expected_avg_pnl})",
        agg.avg_test_pnl,
    );

    // 4. avg_test_sharpe == mean of window test_sharpes
    let sum_test_sharpe: f64 = windows.iter().map(|w| w.test_sharpe).sum();
    let expected_avg_sharpe = sum_test_sharpe / n;
    assert!(
        (agg.avg_test_sharpe - expected_avg_sharpe).abs() < 1e-10,
        "avg_test_sharpe ({}) != expected ({expected_avg_sharpe})",
        agg.avg_test_sharpe,
    );

    // 5. std_test_sharpe == population stddev of test sharpes
    //    Hand formula: sqrt( sum((s_i - mean)^2) / n )
    let variance: f64 = windows
        .iter()
        .map(|w| (w.test_sharpe - expected_avg_sharpe).powi(2))
        .sum::<f64>()
        / n;
    let expected_std = variance.sqrt();
    assert!(
        (agg.std_test_sharpe - expected_std).abs() < 1e-10,
        "std_test_sharpe ({}) != expected ({expected_std})",
        agg.std_test_sharpe,
    );

    // 6. pct_profitable_windows == (count of windows with test_pnl > 0) / n * 100
    let profitable_count = windows.iter().filter(|w| w.test_pnl > 0.0).count();
    let expected_pct = (profitable_count as f64 / n) * 100.0;
    assert!(
        (agg.pct_profitable_windows - expected_pct).abs() < 1e-10,
        "pct_profitable_windows ({}) != expected ({expected_pct})",
        agg.pct_profitable_windows,
    );

    // 7. avg_train_test_sharpe_decay == mean(train_sharpe - test_sharpe)
    let expected_decay: f64 = windows
        .iter()
        .map(|w| w.train_sharpe - w.test_sharpe)
        .sum::<f64>()
        / n;
    assert!(
        (agg.avg_train_test_sharpe_decay - expected_decay).abs() < 1e-10,
        "avg_train_test_sharpe_decay ({}) != expected ({expected_decay})",
        agg.avg_train_test_sharpe_decay,
    );
}

#[test]
fn walk_forward_windows_non_overlapping_test_periods() {
    // When step_days == test_days (the default), consecutive windows' test periods
    // should be contiguous and non-overlapping:
    //   window[i].test_end == window[i+1].test_start
    //
    // This catches bugs where the walk-forward driver incorrectly advances
    // the cursor or computes window boundaries.
    let df = make_multi_strike_df();
    let params = backtest_params("short_put", vec![delta(0.20)]);

    let result = run_walk_forward(&df, &params, 14, 7, None).unwrap();
    let windows = &result.windows;

    if windows.len() >= 2 {
        for pair in windows.windows(2) {
            let prev = &pair[0];
            let next = &pair[1];

            // With default step = test_days, consecutive test periods should be contiguous
            assert_eq!(
                prev.test_end, next.test_start,
                "Window {} test_end ({}) should equal Window {} test_start ({}) \
                 for contiguous, non-overlapping test periods",
                prev.window_number, prev.test_end, next.window_number, next.test_start,
            );

            // Train periods should advance by step_days (== test_days == 7)
            let train_advance = (next.train_start - prev.train_start).num_days();
            assert_eq!(
                train_advance, 7,
                "Train starts should advance by step_days=7, but Window {} to {} advanced by {}",
                prev.window_number, next.window_number, train_advance,
            );
        }
    }

    // Window dates should be monotonically increasing
    for w in windows {
        assert!(w.train_start < w.train_end, "train_start < train_end");
        assert!(w.train_end <= w.test_start, "train_end <= test_start");
        assert!(w.test_start < w.test_end, "test_start < test_end");
    }
}

// ---------------------------------------------------------------------------
// Stock walk-forward tests
// ---------------------------------------------------------------------------

/// Build 60 weekday bars of synthetic OHLCV data for stock walk-forward tests.
/// Returns (`TempDir`, `path_string`, `Vec<Bar>`).
fn make_stock_wf_fixture() -> (
    tempfile::TempDir,
    String,
    Vec<optopsy_mcp::engine::stock_sim::Bar>,
) {
    use chrono::NaiveDate;
    use polars::prelude::*;

    let start = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
    let mut dates = Vec::new();
    let mut closes: Vec<f64> = Vec::new();
    let mut price = 100.0_f64;
    let mut d = start;
    while dates.len() < 60 {
        if d.weekday() != chrono::Weekday::Sat && d.weekday() != chrono::Weekday::Sun {
            dates.push(d);
            closes.push(price);
            price += 0.3;
        }
        d += chrono::Duration::days(1);
    }

    let n = dates.len();
    let mut df = df! {
        "open"     => closes.clone(),
        "high"     => closes.iter().map(|c| c + 1.0).collect::<Vec<_>>(),
        "low"      => closes.iter().map(|c| c - 1.0).collect::<Vec<_>>(),
        "close"    => closes.clone(),
        "adjclose" => closes.clone(),
        "volume"   => vec![1_000_000i64; n],
    }
    .unwrap();
    df.with_column(
        DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()).into_column(),
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ohlcv.parquet");
    let file = std::fs::File::create(&path).unwrap();
    ParquetWriter::new(file).finish(&mut df).unwrap();
    let path_str = path.to_string_lossy().to_string();

    let bars = optopsy_mcp::engine::stock_sim::parse_ohlcv_bars(&path_str, None, None).unwrap();
    (dir, path_str, bars)
}

#[test]
fn stock_walk_forward_produces_windows_and_aggregate() {
    use optopsy_mcp::engine::stock_sim::StockBacktestParams;
    use optopsy_mcp::engine::types::{Interval, Side, Slippage};
    use optopsy_mcp::engine::walk_forward::run_walk_forward_stock;
    use std::collections::HashSet;

    let (_dir, path, bars) = make_stock_wf_fixture();

    // Entry every 5th bar so there are trades in each window.
    let mut entry_dates: HashSet<chrono::NaiveDateTime> = HashSet::new();
    for (i, bar) in bars.iter().enumerate() {
        if i % 5 == 0 {
            entry_dates.insert(bar.datetime);
        }
    }

    let params = StockBacktestParams {
        symbol: "TEST".to_string(),
        side: Side::Long,
        capital: 100_000.0,
        quantity: 10,
        sizing: None,
        max_positions: 1,
        slippage: Slippage::Mid,
        commission: None,
        stop_loss: None,
        take_profit: None,
        max_hold_days: Some(4),
        max_hold_bars: None,
        min_days_between_entries: None,
        min_bars_between_entries: None,
        conflict_resolution: optopsy_mcp::engine::types::ConflictResolution::default(),
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: Some(path),
        cross_ohlcv_paths: std::collections::HashMap::new(),
        start_date: None,
        end_date: None,
        interval: Interval::Daily,
        session_filter: None,
    };

    // 60 bars ≈ 84 calendar days; train=30, test=10 → at least 2 windows.
    let result =
        run_walk_forward_stock(&bars, &params, &Some(entry_dates), &None, 30, 10, None).unwrap();

    assert!(
        !result.windows.is_empty(),
        "Expected at least 1 walk-forward window, got 0"
    );
    assert_eq!(result.aggregate.successful_windows, result.windows.len());

    // Window numbers are sequential starting at 1.
    for (i, w) in result.windows.iter().enumerate() {
        assert_eq!(w.window_number, i + 1);
        assert!(w.train_start < w.train_end);
        assert!(w.test_start < w.test_end);
    }
}

#[test]
fn stock_walk_forward_rejects_insufficient_train_days() {
    use optopsy_mcp::engine::stock_sim::StockBacktestParams;
    use optopsy_mcp::engine::types::{Interval, Side, Slippage};
    use optopsy_mcp::engine::walk_forward::run_walk_forward_stock;

    let (_dir, path, bars) = make_stock_wf_fixture();

    let params = StockBacktestParams {
        symbol: "TEST".to_string(),
        side: Side::Long,
        capital: 100_000.0,
        quantity: 10,
        sizing: None,
        max_positions: 1,
        slippage: Slippage::Mid,
        commission: None,
        stop_loss: None,
        take_profit: None,
        max_hold_days: None,
        max_hold_bars: None,
        min_days_between_entries: None,
        min_bars_between_entries: None,
        conflict_resolution: optopsy_mcp::engine::types::ConflictResolution::default(),
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: Some(path),
        cross_ohlcv_paths: std::collections::HashMap::new(),
        start_date: None,
        end_date: None,
        interval: Interval::Daily,
        session_filter: None,
    };

    // train_days = 0 → should fail validation.
    let err = run_walk_forward_stock(&bars, &params, &None, &None, 0, 10, None).unwrap_err();
    assert!(
        err.to_string().contains("train_days"),
        "Expected train_days error, got: {err}"
    );
}

#[test]
fn stock_walk_forward_rejects_small_step_days() {
    use optopsy_mcp::engine::stock_sim::StockBacktestParams;
    use optopsy_mcp::engine::types::{Interval, Side, Slippage};
    use optopsy_mcp::engine::walk_forward::run_walk_forward_stock;

    let (_dir, path, bars) = make_stock_wf_fixture();

    let params = StockBacktestParams {
        symbol: "TEST".to_string(),
        side: Side::Long,
        capital: 100_000.0,
        quantity: 10,
        sizing: None,
        max_positions: 1,
        slippage: Slippage::Mid,
        commission: None,
        stop_loss: None,
        take_profit: None,
        max_hold_days: None,
        max_hold_bars: None,
        min_days_between_entries: None,
        min_bars_between_entries: None,
        conflict_resolution: optopsy_mcp::engine::types::ConflictResolution::default(),
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: Some(path),
        cross_ohlcv_paths: std::collections::HashMap::new(),
        start_date: None,
        end_date: None,
        interval: Interval::Daily,
        session_filter: None,
    };

    // step_days = 2 < 5 → should fail validation.
    let err = run_walk_forward_stock(&bars, &params, &None, &None, 20, 10, Some(2)).unwrap_err();
    assert!(
        err.to_string().contains("step_days"),
        "Expected step_days error, got: {err}"
    );
}
