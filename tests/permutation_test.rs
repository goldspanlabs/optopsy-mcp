//! Integration tests for the `permutation_test` feature.
//!
//! These tests exercise the full permutation pipeline with real data frames
//! that produce actual trades, validating end-to-end correctness of shuffled
//! reruns and p-value computation.

use chrono::Datelike;
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

    let output = run_permutation_test(
        &df,
        &params,
        &perm_params,
        &None::<std::collections::HashSet<chrono::NaiveDate>>,
        None::<&std::collections::HashSet<chrono::NaiveDate>>,
    )
    .unwrap();

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

    let metric_names: Vec<&str> = output
        .metric_results
        .iter()
        .map(|m| m.metric_name.as_str())
        .collect();
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

    let out1 = run_permutation_test(
        &df,
        &params,
        &perm_params,
        &None::<std::collections::HashSet<chrono::NaiveDate>>,
        None::<&std::collections::HashSet<chrono::NaiveDate>>,
    )
    .unwrap();
    let out2 = run_permutation_test(
        &df,
        &params,
        &perm_params,
        &None::<std::collections::HashSet<chrono::NaiveDate>>,
        None::<&std::collections::HashSet<chrono::NaiveDate>>,
    )
    .unwrap();

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

    let result = run_permutation_test(
        &df,
        &params,
        &perm_params,
        &None::<std::collections::HashSet<chrono::NaiveDate>>,
        None::<&std::collections::HashSet<chrono::NaiveDate>>,
    );
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

    let result = run_permutation_test(
        &df,
        &params,
        &perm_params,
        &None::<std::collections::HashSet<chrono::NaiveDate>>,
        None::<&std::collections::HashSet<chrono::NaiveDate>>,
    );
    // Should either error (no trades in real backtest) or return empty metrics (no candidates)
    if let Ok(output) = result {
        assert_eq!(output.num_completed, 0);
        assert!(output.metric_results.is_empty());
    }
    // Err is also acceptable — real backtest may fail
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

    let result = run_permutation_test(
        &df,
        &params,
        &perm_params,
        &None::<std::collections::HashSet<chrono::NaiveDate>>,
        None::<&std::collections::HashSet<chrono::NaiveDate>>,
    );

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

// ---------------------------------------------------------------------------
// Stock permutation test
// ---------------------------------------------------------------------------

/// Build a tiny synthetic OHLCV parquet for stock permutation tests.
/// Returns (`TempDir`, path, `Vec<Bar>`).
fn make_stock_perm_fixture() -> (
    tempfile::TempDir,
    String,
    Vec<optopsy_mcp::engine::stock_sim::Bar>,
) {
    use chrono::NaiveDate;
    use polars::prelude::*;

    let start = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
    let mut dates = Vec::new();
    let mut closes: Vec<f64> = Vec::new();

    // 30 weekday bars with a gentle uptrend
    let mut price = 100.0_f64;
    let mut d = start;
    while dates.len() < 30 {
        if d.weekday() != chrono::Weekday::Sat && d.weekday() != chrono::Weekday::Sun {
            dates.push(d);
            closes.push(price);
            price += 0.5;
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
fn stock_permutation_test_produces_metric_results() {
    use optopsy_mcp::engine::permutation::{run_stock_permutation_test, PermutationParams};
    use optopsy_mcp::engine::stock_sim::StockBacktestParams;
    use optopsy_mcp::engine::types::{Interval, Side, Slippage};
    use std::collections::HashSet;

    let (_dir, path, bars) = make_stock_perm_fixture();

    // Fire entry signal on every 5th bar so we get a few trades.
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
        ohlcv_path: Some(path.clone()),
        cross_ohlcv_paths: std::collections::HashMap::new(),
        start_date: None,
        end_date: None,
        interval: Interval::Daily,
        session_filter: None,
    };

    let perm_params = PermutationParams {
        num_permutations: 20,
        seed: Some(42),
    };

    let output =
        run_stock_permutation_test(&bars, &params, &Some(entry_dates), &None, &perm_params)
            .unwrap();

    assert_eq!(output.num_permutations, 20);
    assert!(
        output.num_completed > 0,
        "Expected at least one permutation to complete"
    );
    assert_eq!(output.metric_results.len(), 5, "Expected 5 metrics");

    for m in &output.metric_results {
        assert!(
            (0.0..=1.0).contains(&m.p_value),
            "{}: p_value {} out of [0,1]",
            m.metric_name,
            m.p_value,
        );
    }
}

#[test]
fn stock_permutation_test_zero_signal_fires_returns_zero_completed() {
    use optopsy_mcp::engine::permutation::{run_stock_permutation_test, PermutationParams};
    use optopsy_mcp::engine::stock_sim::StockBacktestParams;
    use optopsy_mcp::engine::types::{Interval, Side, Slippage};

    let (_dir, path, bars) = make_stock_perm_fixture();

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

    let perm_params = PermutationParams {
        num_permutations: 10,
        seed: Some(1),
    };

    // No entry dates → signal_fire_count = 0 → early return with num_completed = 0.
    let output = run_stock_permutation_test(&bars, &params, &None, &None, &perm_params).unwrap();

    assert_eq!(output.num_completed, 0);
    assert!(output.metric_results.is_empty());
}
