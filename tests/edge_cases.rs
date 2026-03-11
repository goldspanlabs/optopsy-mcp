//! Edge-case tests: empty `DataFrames`, NaN/Infinity in metrics,
//! zero-width DTE/delta ranges, concurrent loading races,
//! max position capacity management, and signal-triggered exits mid-position.

mod common;

use chrono::NaiveDate;
use optopsy_mcp::engine::core::run_backtest;
use optopsy_mcp::engine::types::{BacktestParams, DteRange, ExitType, Slippage, TradeSelector};
use polars::prelude::*;

// ── Empty DataFrame tests ────────────────────────────────────────────────────

#[test]
fn filter_dte_range_on_empty_df() {
    let df = df! {
        "dte" => Vec::<i32>::new(),
        "value" => Vec::<i32>::new(),
    }
    .unwrap();
    let result = optopsy_mcp::engine::filters::filter_dte_range(&df, 60, 30).unwrap();
    assert_eq!(result.height(), 0);
}

#[test]
fn filter_valid_quotes_on_empty_df() {
    let df = df! {
        "bid" => Vec::<f64>::new(),
        "ask" => Vec::<f64>::new(),
    }
    .unwrap();
    let result = optopsy_mcp::engine::filters::filter_valid_quotes(&df, 0.05).unwrap();
    assert_eq!(result.height(), 0);
}

#[test]
fn filter_option_type_on_empty_df() {
    let df = df! {
        "option_type" => Vec::<&str>::new(),
    }
    .unwrap();
    let result = optopsy_mcp::engine::filters::filter_option_type(&df, "call").unwrap();
    assert_eq!(result.height(), 0);
}

#[test]
fn select_closest_delta_on_empty_df() {
    let dt_col: Vec<chrono::NaiveDateTime> = vec![];
    let exp_col: Vec<chrono::NaiveDateTime> = vec![];
    let df = df! {
        "quote_datetime" => &dt_col,
        "expiration" => &exp_col,
        "delta" => Vec::<f64>::new(),
        "strike" => Vec::<f64>::new(),
        "bid" => Vec::<f64>::new(),
        "ask" => Vec::<f64>::new(),
    }
    .unwrap();
    let target = optopsy_mcp::engine::types::TargetRange {
        target: 0.30,
        min: 0.20,
        max: 0.40,
    };
    let result = optopsy_mcp::engine::filters::select_closest_delta(&df, &target).unwrap();
    assert_eq!(result.height(), 0);
}

// ── Metrics edge cases: NaN/Infinity handling ────────────────────────────────

#[test]
fn metrics_with_no_trades_returns_defaults() {
    let result = optopsy_mcp::engine::metrics::calculate_metrics(&[], &[], 10_000.0).unwrap();
    assert!((result.sharpe - 0.0).abs() < f64::EPSILON);
    assert!((result.win_rate - 0.0).abs() < f64::EPSILON);
    assert!((result.profit_factor - 0.0).abs() < f64::EPSILON);
}

#[test]
fn metrics_with_zero_capital_returns_defaults() {
    let result = optopsy_mcp::engine::metrics::calculate_metrics(&[], &[], 0.0).unwrap();
    assert!((result.sharpe - 0.0).abs() < f64::EPSILON);
    assert!((result.cagr - 0.0).abs() < f64::EPSILON);
}

#[test]
fn metrics_with_negative_capital_returns_defaults() {
    let result = optopsy_mcp::engine::metrics::calculate_metrics(&[], &[], -100.0).unwrap();
    assert!((result.max_drawdown - 0.0).abs() < f64::EPSILON);
}

#[test]
fn metrics_all_fields_are_finite() {
    // Single winning trade
    let entry_dt = NaiveDate::from_ymd_opt(2024, 1, 15)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let exit_dt = NaiveDate::from_ymd_opt(2024, 2, 15)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let trade = optopsy_mcp::engine::types::TradeRecord::new(
        1,
        entry_dt,
        exit_dt,
        -100.0, // debit
        150.0,  // credit
        50.0,   // profit
        31,
        optopsy_mcp::engine::types::ExitType::DteExit,
        vec![],
    );
    let equity = vec![
        optopsy_mcp::engine::types::EquityPoint {
            datetime: entry_dt,
            equity: 10_000.0,
        },
        optopsy_mcp::engine::types::EquityPoint {
            datetime: exit_dt,
            equity: 10_050.0,
        },
    ];
    let m = optopsy_mcp::engine::metrics::calculate_metrics(&equity, &[trade], 10_000.0).unwrap();
    assert!(m.sharpe.is_finite(), "sharpe must be finite");
    assert!(m.sortino.is_finite(), "sortino must be finite");
    assert!(m.cagr.is_finite(), "cagr must be finite");
    assert!(m.calmar.is_finite(), "calmar must be finite");
    assert!(m.var_95.is_finite(), "var_95 must be finite");
    assert!(m.profit_factor.is_finite(), "profit_factor must be finite");
    assert!(m.expectancy.is_finite(), "expectancy must be finite");
}

// ── Zero-width DTE/delta ranges ──────────────────────────────────────────────

#[test]
fn zero_width_dte_range_returns_exact_match() {
    let df = df! {
        "dte" => &[29i32, 30, 31],
        "value" => &[1, 2, 3],
    }
    .unwrap();
    let result = optopsy_mcp::engine::filters::filter_dte_range(&df, 30, 30).unwrap();
    assert_eq!(result.height(), 1);
}

#[test]
fn zero_width_delta_range() {
    use garde::Validate;
    let tr = optopsy_mcp::engine::types::TargetRange {
        target: 0.30,
        min: 0.30,
        max: 0.30,
    };
    // Should be valid — min == max is a point, not inverted
    assert!(tr.validate().is_ok());
}

#[test]
fn zero_width_dte_range_validation() {
    use garde::Validate;
    let dte = optopsy_mcp::engine::types::DteRange {
        target: 45,
        min: 45,
        max: 45,
    };
    assert!(dte.validate().is_ok());
}

// ── Concurrent loading: verify Arc<RwLock> data map is safe ──────────────────

#[tokio::test]
async fn concurrent_read_access_does_not_panic() {
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    let data: Arc<RwLock<HashMap<String, DataFrame>>> = Arc::new(RwLock::new(HashMap::new()));

    // Insert a small DataFrame
    {
        let mut w = data.write().await;
        let df = df! { "x" => &[1i32, 2, 3] }.unwrap();
        w.insert("TEST".to_string(), df);
    }

    // Spawn many concurrent readers
    let mut handles = Vec::new();
    for _ in 0..50 {
        let data_clone = Arc::clone(&data);
        handles.push(tokio::spawn(async move {
            let r = data_clone.read().await;
            assert!(r.contains_key("TEST"));
            assert_eq!(r["TEST"].height(), 3);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn concurrent_write_then_read_consistent() {
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    let data: Arc<RwLock<HashMap<String, DataFrame>>> = Arc::new(RwLock::new(HashMap::new()));

    // Writer task
    let data_w = Arc::clone(&data);
    let writer = tokio::spawn(async move {
        for i in 0..10 {
            let mut w = data_w.write().await;
            let df = df! { "val" => &[i] }.unwrap();
            w.insert(format!("SYM{i}"), df);
        }
    });

    writer.await.unwrap();

    let r = data.read().await;
    assert_eq!(r.len(), 10);
}

// ── Max position capacity management ─────────────────────────────────────────

/// Build synthetic data with many distinct entry dates to test max_positions.
/// 5 trading dates, each with a distinct near-term expiration so the engine
/// can open separate positions per date (duplicate expirations are deduplicated).
fn make_many_entry_dates_df() -> DataFrame {
    let dates = [
        NaiveDate::from_ymd_opt(2024, 1, 10).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 11).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 12).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 16).unwrap(),
        NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(), // exit date (DTE=0..5 for all exps)
    ];

    // Each date gets a different expiration so the engine doesn't dedup them
    let exps = [
        NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(), // DTE ~65
        NaiveDate::from_ymd_opt(2024, 3, 16).unwrap(),
        NaiveDate::from_ymd_opt(2024, 3, 17).unwrap(),
        NaiveDate::from_ymd_opt(2024, 3, 18).unwrap(),
        NaiveDate::from_ymd_opt(2024, 3, 19).unwrap(),
    ];

    let mut quote_dates = Vec::new();
    let mut expirations_vec = Vec::new();
    let mut option_types = Vec::new();
    let mut strikes = Vec::new();
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    let mut deltas = Vec::new();

    // For each entry date, create a call option with a unique expiration
    for (i, (_date, exp)) in dates[..5].iter().zip(exps.iter()).enumerate() {
        // Entry date row
        let price = 5.0 - (i as f64) * 0.5; // decreasing price
        for d in &dates {
            quote_dates.push(d.and_hms_opt(0, 0, 0).unwrap());
            expirations_vec.push(*exp);
            option_types.push("call");
            strikes.push(100.0);
            bids.push(price - 0.25);
            asks.push(price + 0.25);
            deltas.push(0.50);
        }
    }

    let mut df = df! {
        "quote_datetime" => &quote_dates,
        "option_type" => &option_types,
        "strike" => &strikes,
        "bid" => &bids,
        "ask" => &asks,
        "delta" => &deltas,
    }
    .unwrap();

    df.with_column(
        DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations_vec).into_column(),
    )
    .unwrap();

    df
}

fn capacity_params(max_positions: i32) -> BacktestParams {
    BacktestParams {
        strategy: "long_call".to_string(),
        leg_deltas: vec![common::delta(0.50)],
        entry_dte: DteRange {
            target: 65,
            min: 60,
            max: 70,
        },
        exit_dte: 5,
        slippage: Slippage::Mid,
        commission: None,
        min_bid_ask: 0.0,
        stop_loss: None,
        take_profit: None,
        max_hold_days: None,
        capital: 100_000.0,
        quantity: 1,
        multiplier: 100,
        max_positions,
        selector: TradeSelector::First,
        adjustment_rules: vec![],
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: None,
        cross_ohlcv_paths: std::collections::HashMap::new(),
        min_net_premium: None,
        max_net_premium: None,
        min_net_delta: None,
        max_net_delta: None,
        min_days_between_entries: None,
        expiration_filter: optopsy_mcp::engine::types::ExpirationFilter::Any,
        exit_net_delta: None,
    }
}

#[test]
fn max_positions_one_caps_concurrent_trades() {
    let df = make_many_entry_dates_df();
    let params = capacity_params(1);

    let result = run_backtest(&df, &params).expect("backtest failed");

    // With max_positions=1, only one position at a time. Since all positions
    // expire near end of data, at most 1 trade should be opened.
    assert!(
        result.trade_count <= 1,
        "max_positions=1 should cap concurrent trades to 1, got {}",
        result.trade_count
    );
}

#[test]
fn max_positions_higher_allows_more_trades() {
    let df = make_many_entry_dates_df();

    let result_1 = run_backtest(&df, &capacity_params(1)).expect("backtest failed");
    let result_5 = run_backtest(&df, &capacity_params(5)).expect("backtest failed");

    // Higher max_positions should allow more or equal trades
    assert!(
        result_5.trade_count >= result_1.trade_count,
        "max_positions=5 ({}) should allow >= trades than max_positions=1 ({})",
        result_5.trade_count,
        result_1.trade_count
    );
}

// ── Signal-triggered exits mid-position ──────────────────────────────────────

#[test]
fn signal_exit_fires_mid_position() {
    // Entry on Jan 15 (DTE=32). Exit signal fires on Jan 22 (consecutive down).
    // Without signal, trade would exit on Feb 11 (DTE exit). Signal should cut it short.
    let df = common::make_multi_strike_df();

    // OHLCV with declining prices: signal fires from Jan 22 onward
    let dates = vec![
        NaiveDate::from_ymd_opt(2024, 1, 11).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(),
        NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(),
    ];
    let closes = vec![105.0, 104.0, 103.0, 102.0]; // consecutive down
    let (_dir, path) = common::write_ohlcv_parquet(&dates, &closes);

    let mut params = common::backtest_params("long_call", vec![common::delta(0.50)]);
    params.exit_signal = Some(
        optopsy_mcp::signals::registry::SignalSpec::ConsecutiveDown {
            column: "close".into(),
            count: 1,
        },
    );
    params.ohlcv_path = Some(path);

    let result = run_backtest(&df, &params).expect("backtest failed");

    assert!(result.trade_count > 0, "should produce at least 1 trade");
    let trade = &result.trade_log[0];

    // Signal exit should fire before the DTE exit
    assert!(
        matches!(trade.exit_type, ExitType::Signal),
        "expected Signal exit, got {:?}",
        trade.exit_type
    );
    assert!(
        trade.days_held < 27,
        "signal exit should be earlier than DTE exit (27 days), got {}",
        trade.days_held
    );
}

#[test]
fn signal_exit_has_priority_over_dte_when_same_day() {
    // Set up so DTE exit and signal exit could both fire on Feb 11.
    // Signal has higher priority in the event loop (checked first).
    let df = common::make_multi_strike_df();

    // OHLCV declining: signal fires from Jan 22 onward (well before DTE exit)
    let dates = vec![
        NaiveDate::from_ymd_opt(2024, 1, 11).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(),
        NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(),
    ];
    let closes = vec![100.0, 101.0, 102.0, 99.0]; // only last bar is down
    let (_dir, path) = common::write_ohlcv_parquet(&dates, &closes);

    let mut params = common::backtest_params("long_call", vec![common::delta(0.50)]);
    params.exit_signal = Some(
        optopsy_mcp::signals::registry::SignalSpec::ConsecutiveDown {
            column: "close".into(),
            count: 1,
        },
    );
    params.ohlcv_path = Some(path);

    let result = run_backtest(&df, &params).expect("backtest failed");

    assert!(result.trade_count > 0, "should produce at least 1 trade");

    // Feb 11: both DTE exit (DTE=5) and signal (consecutive down) fire.
    // Signal takes priority in event loop.
    let trade = &result.trade_log[0];
    assert!(
        matches!(trade.exit_type, ExitType::Signal),
        "signal exit should have priority over DTE exit, got {:?}",
        trade.exit_type
    );
}

// ── Pathological sweep inputs ────────────────────────────────────────────────

#[test]
fn sweep_empty_strategies_returns_empty() {
    use optopsy_mcp::engine::sweep::{run_sweep, SweepDimensions, SweepParams};
    use optopsy_mcp::engine::types::SimParams;

    let df = common::make_multi_strike_df();
    let params = SweepParams {
        strategies: vec![],
        sweep: SweepDimensions {
            entry_dte_targets: vec![45],
            exit_dtes: vec![0],
            slippage_models: vec![Slippage::Mid],
        },
        sim_params: SimParams {
            capital: 100_000.0,
            quantity: 1,
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
        },
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
fn sweep_empty_delta_grid_returns_empty() {
    use optopsy_mcp::engine::sweep::{run_sweep, SweepDimensions, SweepParams, SweepStrategyEntry};
    use optopsy_mcp::engine::types::SimParams;

    let df = common::make_multi_strike_df();
    let params = SweepParams {
        strategies: vec![SweepStrategyEntry {
            name: "long_call".to_string(),
            leg_delta_targets: vec![], // no delta grid
        }],
        sweep: SweepDimensions {
            entry_dte_targets: vec![45],
            exit_dtes: vec![0],
            slippage_models: vec![Slippage::Mid],
        },
        sim_params: SimParams {
            capital: 100_000.0,
            quantity: 1,
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
        },
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
fn sweep_empty_dte_targets_returns_empty() {
    use optopsy_mcp::engine::sweep::{run_sweep, SweepDimensions, SweepParams, SweepStrategyEntry};
    use optopsy_mcp::engine::types::SimParams;

    let df = common::make_multi_strike_df();
    let params = SweepParams {
        strategies: vec![SweepStrategyEntry {
            name: "long_call".to_string(),
            leg_delta_targets: vec![vec![0.50]],
        }],
        sweep: SweepDimensions {
            entry_dte_targets: vec![], // no DTE targets
            exit_dtes: vec![0],
            slippage_models: vec![Slippage::Mid],
        },
        sim_params: SimParams {
            capital: 100_000.0,
            quantity: 1,
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
        },
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
fn sweep_nonexistent_strategy_produces_error_in_results() {
    use optopsy_mcp::engine::sweep::{run_sweep, SweepDimensions, SweepParams, SweepStrategyEntry};
    use optopsy_mcp::engine::types::SimParams;

    let df = common::make_multi_strike_df();
    let params = SweepParams {
        strategies: vec![SweepStrategyEntry {
            name: "nonexistent_strategy_xyz".to_string(),
            leg_delta_targets: vec![vec![0.50]],
        }],
        sweep: SweepDimensions {
            entry_dte_targets: vec![45],
            exit_dtes: vec![0],
            slippage_models: vec![Slippage::Mid],
        },
        sim_params: SimParams {
            capital: 100_000.0,
            quantity: 1,
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
        },
        out_of_sample_pct: 0.0,
        direction: None,
        entry_signals: vec![],
        exit_signals: vec![],
        num_permutations: None,
        permutation_seed: None,
    };

    // Should not panic — errors are captured per-combo
    let output = run_sweep(&df, &params).unwrap();
    assert_eq!(
        output.combinations_run, 0,
        "nonexistent strategy should produce 0 successful runs"
    );
}
