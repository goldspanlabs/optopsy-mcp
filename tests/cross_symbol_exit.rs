//! Tests for cross-symbol exit signal resolution in options backtests.
//!
//! Validates that cross-symbol formula references (e.g., `VIX > 30`) evaluate
//! against the cross-symbol's OHLCV data, and that exit signals only close
//! positions that are actually open.
//!
//! Uses `make_multi_strike_df()` from common: 3 dates (Jan 15, Jan 22, Feb 11 2024),
//! near-term exp Feb 16 (DTE=32 from Jan 15), `exit_dte=5` → DTE exit on Feb 11.

use chrono::NaiveDate;
use optopsy_mcp::engine::core::run_backtest;
use optopsy_mcp::engine::types::{
    BacktestParams, DteRange, ExitType, Slippage, TargetRange, TradeSelector,
};
use optopsy_mcp::signals::registry::SignalSpec;
use std::collections::HashMap;

mod common;
use common::{delta, make_multi_strike_df, write_ohlcv_parquet};

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// The 3 trading dates matching `make_multi_strike_df`.
fn ohlcv_dates() -> Vec<NaiveDate> {
    vec![
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(),
        NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(),
    ]
}

fn base_params(strategy: &str, leg_deltas: Vec<TargetRange>) -> BacktestParams {
    BacktestParams {
        strategy: strategy.to_string(),
        leg_deltas,
        entry_dte: DteRange {
            target: 45,
            min: 10,
            max: 45,
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
        sizing: None,
        multiplier: 100,
        max_positions: 5,
        selector: TradeSelector::First,
        adjustment_rules: vec![],
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: None,
        cross_ohlcv_paths: HashMap::new(),
        min_net_premium: None,
        max_net_premium: None,
        min_net_delta: None,
        max_net_delta: None,
        min_days_between_entries: None,
        expiration_filter: optopsy_mcp::engine::types::ExpirationFilter::Any,
        exit_net_delta: None,
    }
}

// ─── Test: Cross-symbol VIX exit fires on correct date ──────────────────────

#[test]
fn cross_symbol_vix_exit_fires_when_signal_active_and_position_open() {
    // Setup: short_put enters Jan 15 (DTE=32), normal DTE exit would be Feb 11 (DTE=5).
    //
    // VIX closes: Jan 15=18, Jan 22=35, Feb 11=20
    // Exit signal: VIX > 30 (a "VIX spike" condition)
    //
    // Expected: VIX > 30 fires on Jan 22. Position is open since Jan 15.
    // → Signal exit on Jan 22 (7 days held), NOT the normal DTE exit on Feb 11.
    let df = make_multi_strike_df();
    let dates = ohlcv_dates();

    let (_spy_dir, spy_path) = write_ohlcv_parquet(&dates, &[450.0, 450.0, 450.0]);
    let (_vix_dir, vix_path) = write_ohlcv_parquet(&dates, &[18.0, 35.0, 20.0]);

    let mut cross_paths = HashMap::new();
    cross_paths.insert("VIX".to_string(), vix_path);

    let mut params = base_params("short_put", vec![delta(0.40)]);
    params.ohlcv_path = Some(spy_path);
    params.cross_ohlcv_paths = cross_paths;
    params.exit_signal = Some(SignalSpec::Formula {
        formula: "VIX > 30".into(),
    });

    let result = run_backtest(&df, &params).expect("backtest should succeed");

    assert!(!result.trade_log.is_empty(), "expected at least 1 trade");
    let trade = &result.trade_log[0];

    assert!(
        matches!(trade.exit_type, ExitType::Signal),
        "expected Signal exit (VIX spike on Jan 22), got {:?}",
        trade.exit_type
    );
    assert_eq!(
        trade.days_held, 7,
        "expected 7 days held (Jan 15 → Jan 22), got {}",
        trade.days_held
    );
}

// ─── Test: Cross-symbol exit evaluates VIX data, not SPY data ───────────────

#[test]
fn cross_symbol_exit_uses_cross_data_not_primary() {
    // Signal: VIX > 400
    // - On VIX data (18/19/20): never true → no signal exit
    // - If incorrectly evaluated on SPY (450): would be true → signal exit
    //
    // Expected: no signal exit. Trade exits via normal DTE exit on Feb 11.
    let df = make_multi_strike_df();
    let dates = ohlcv_dates();

    let (_spy_dir, spy_path) = write_ohlcv_parquet(&dates, &[450.0, 450.0, 450.0]);
    let (_vix_dir, vix_path) = write_ohlcv_parquet(&dates, &[18.0, 19.0, 20.0]);

    let mut cross_paths = HashMap::new();
    cross_paths.insert("VIX".to_string(), vix_path);

    let mut params = base_params("short_put", vec![delta(0.40)]);
    params.ohlcv_path = Some(spy_path);
    params.cross_ohlcv_paths = cross_paths;
    params.exit_signal = Some(SignalSpec::Formula {
        formula: "VIX > 400".into(), // true for SPY (450), false for VIX (18-20)
    });

    let result = run_backtest(&df, &params).expect("backtest should succeed");

    assert_eq!(result.trade_count, 1, "expected exactly 1 trade");
    let trade = &result.trade_log[0];

    assert!(
        matches!(trade.exit_type, ExitType::DteExit),
        "expected DteExit (VIX signal should not fire), got {:?}",
        trade.exit_type
    );
    assert_eq!(
        trade.days_held, 27,
        "expected 27 days held (Jan 15 → Feb 11 DTE exit), got {}",
        trade.days_held
    );
}

// ─── Test: Signal exit only closes OPEN positions ───────────────────────────

#[test]
fn cross_symbol_exit_only_closes_open_positions() {
    // Setup: max_positions=1, short_put, VIX signal fires on ALL 3 dates (close > 30).
    //
    // The engine processes exits before entries on each day:
    //   Jan 15: enter position, signal exit same day (7 days held to Jan 22)
    //   Jan 22: exit trade 1, re-enter new position, signal exit (20 days held to Feb 11)
    //   Feb 11: exit trade 2
    //
    // Exactly 2 trades, both Signal exits. No phantom trades from signal firing
    // on days without open positions.
    let df = make_multi_strike_df();
    let dates = ohlcv_dates();

    let (_spy_dir, spy_path) = write_ohlcv_parquet(&dates, &[450.0, 450.0, 450.0]);
    let (_vix_dir, vix_path) = write_ohlcv_parquet(&dates, &[35.0, 35.0, 35.0]);

    let mut cross_paths = HashMap::new();
    cross_paths.insert("VIX".to_string(), vix_path);

    let mut params = base_params("short_put", vec![delta(0.40)]);
    params.ohlcv_path = Some(spy_path);
    params.cross_ohlcv_paths = cross_paths;
    params.max_positions = 1;
    params.exit_signal = Some(SignalSpec::Formula {
        formula: "VIX > 30".into(),
    });

    let result = run_backtest(&df, &params).expect("backtest should succeed");

    assert_eq!(result.trade_count, 2, "expected exactly 2 trades");

    for trade in &result.trade_log {
        assert!(
            matches!(trade.exit_type, ExitType::Signal),
            "expected Signal exit, got {:?}",
            trade.exit_type
        );
    }

    // Trade 1: Jan 15 → Jan 22 (7 days), Trade 2: Jan 22 → Feb 11 (20 days)
    assert_eq!(result.trade_log[0].days_held, 7);
    assert_eq!(result.trade_log[1].days_held, 20);
}

// ─── Test: Missing cross_ohlcv_paths errors cleanly ─────────────────────────

#[test]
fn missing_cross_ohlcv_paths_errors() {
    // Exit signal references VIX via formula syntax, but cross_ohlcv_paths is empty.
    // The engine should error with a clear message, not panic.
    let df = make_multi_strike_df();
    let dates = ohlcv_dates();

    let (_spy_dir, spy_path) = write_ohlcv_parquet(&dates, &[450.0, 450.0, 450.0]);

    let mut params = base_params("short_put", vec![delta(0.40)]);
    params.ohlcv_path = Some(spy_path);
    // cross_ohlcv_paths is empty — no VIX data
    params.exit_signal = Some(SignalSpec::Formula {
        formula: "VIX > 30".into(),
    });

    let result = run_backtest(&df, &params);

    assert!(
        result.is_err(),
        "expected error when cross_ohlcv_paths is missing VIX data"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("VIX") && err_msg.contains("no OHLCV data loaded"),
        "error should mention VIX and explain missing data, got: {err_msg}"
    );
}
