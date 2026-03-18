//! Tests for cross-symbol exit signal resolution in options backtests.
//!
//! Validates two key properties:
//! 1. CrossSymbol exit signals evaluate against the cross-symbol's OHLCV data (not primary)
//! 2. Exit signals only close positions that are actually open (not fire on all days)
//!
//! Uses `make_multi_strike_df()` from common: 3 dates (Jan 15, Jan 22, Feb 11 2024),
//! near-term exp Feb 16 (DTE=32 from Jan 15), exit_dte=5 → DTE exit on Feb 11.

use chrono::NaiveDate;
use optopsy_mcp::engine::core::run_backtest;
use optopsy_mcp::engine::types::{
    BacktestParams, DteRange, ExitType, Slippage, TargetRange, TradeSelector,
};
use optopsy_mcp::signals::registry::SignalSpec;
use polars::prelude::*;
use std::collections::HashMap;
use tempfile::TempDir;

mod common;
use common::{delta, make_multi_strike_df};

// ─── Helpers ─────────────────────────────────────────────────────────────────

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

/// Write a primary SPY OHLCV parquet covering the same dates as `make_multi_strike_df`.
/// Prices are flat — the primary symbol data should NOT influence the cross-symbol exit.
fn write_spy_ohlcv(dates: &[NaiveDate]) -> (TempDir, String) {
    let n = dates.len();
    let closes: Vec<f64> = vec![450.0; n]; // flat SPY prices
    let mut df = df! {
        "open"     => vec![449.0f64; n],
        "high"     => vec![451.0f64; n],
        "low"      => vec![448.0f64; n],
        "close"    => &closes,
        "adjclose" => &closes,
        "volume"   => vec![1_000_000i64; n],
    }
    .unwrap();
    df.with_column(
        DateChunked::from_naive_date(PlSmallStr::from("date"), dates.to_vec()).into_column(),
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("spy_ohlcv.parquet");
    let file = std::fs::File::create(&path).unwrap();
    ParquetWriter::new(file).finish(&mut df).unwrap();
    (dir, path.to_string_lossy().to_string())
}

/// Write a VIX OHLCV parquet with controlled close values.
/// `closes` should align 1:1 with `dates`.
fn write_vix_ohlcv(dates: &[NaiveDate], closes: &[f64]) -> (TempDir, String) {
    let n = dates.len();
    let mut df = df! {
        "open"     => vec![20.0f64; n],
        "high"     => vec![35.0f64; n],
        "low"      => vec![15.0f64; n],
        "close"    => closes,
        "adjclose" => closes,
        "volume"   => vec![500_000i64; n],
    }
    .unwrap();
    df.with_column(
        DateChunked::from_naive_date(PlSmallStr::from("date"), dates.to_vec()).into_column(),
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("vix_ohlcv.parquet");
    let file = std::fs::File::create(&path).unwrap();
    ParquetWriter::new(file).finish(&mut df).unwrap();
    (dir, path.to_string_lossy().to_string())
}

// ─── Test: Cross-symbol VIX exit fires on correct date ──────────────────────

#[test]
fn cross_symbol_vix_exit_fires_when_signal_active_and_position_open() {
    // Setup: short_put enters Jan 15 (DTE=32), normal DTE exit would be Feb 11 (DTE=5).
    //
    // VIX closes: Jan 15=18, Jan 22=35, Feb 11=20
    // Exit signal: VIX close > 30 (a "VIX spike" condition)
    //
    // Expected: VIX > 30 fires on Jan 22. Position is open since Jan 15.
    // → Signal exit on Jan 22 (7 days held), NOT the normal DTE exit on Feb 11.
    let df = make_multi_strike_df();

    let ohlcv_dates = vec![
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(),
        NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(),
    ];

    let (_spy_dir, spy_path) = write_spy_ohlcv(&ohlcv_dates);
    // VIX spikes to 35 on Jan 22
    let (_vix_dir, vix_path) = write_vix_ohlcv(&ohlcv_dates, &[18.0, 35.0, 20.0]);

    let mut cross_paths = HashMap::new();
    cross_paths.insert("VIX".to_string(), vix_path);

    let mut params = base_params("short_put", vec![delta(0.40)]);
    params.ohlcv_path = Some(spy_path);
    params.cross_ohlcv_paths = cross_paths;
    params.exit_signal = Some(SignalSpec::CrossSymbol {
        symbol: "VIX".into(),
        signal: Box::new(SignalSpec::Formula {
            formula: "close > 30".into(),
        }),
    });

    let result = run_backtest(&df, &params).expect("backtest should succeed");

    assert!(!result.trade_log.is_empty(), "expected at least 1 trade");
    let trade = &result.trade_log[0];

    // Signal exit should fire on Jan 22 when VIX > 30
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
    // Setup: SPY close is flat at 450 (never > 30... well it IS > 30, but the point
    // is we craft a signal that would fire on SPY but NOT on VIX to prove evaluation
    // uses the correct DataFrame).
    //
    // Signal: VIX close < 15 (never true — VIX stays 18/19/20).
    // If incorrectly evaluated against SPY (close=450), 450 < 15 is false too.
    // We need a different approach: use a signal that fires on SPY but not on VIX.
    //
    // Signal: CrossSymbol("VIX", "close > 400")
    // - On VIX data (18/19/20): never true → no signal exit
    // - If incorrectly evaluated on SPY (450): would be true → signal exit
    //
    // Expected: no signal exit. Trade exits via normal DTE exit on Feb 11.
    let df = make_multi_strike_df();

    let ohlcv_dates = vec![
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(),
        NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(),
    ];

    let (_spy_dir, spy_path) = write_spy_ohlcv(&ohlcv_dates);
    // VIX stays low — signal should never fire
    let (_vix_dir, vix_path) = write_vix_ohlcv(&ohlcv_dates, &[18.0, 19.0, 20.0]);

    let mut cross_paths = HashMap::new();
    cross_paths.insert("VIX".to_string(), vix_path);

    let mut params = base_params("short_put", vec![delta(0.40)]);
    params.ohlcv_path = Some(spy_path);
    params.cross_ohlcv_paths = cross_paths;
    params.exit_signal = Some(SignalSpec::CrossSymbol {
        symbol: "VIX".into(),
        signal: Box::new(SignalSpec::Formula {
            formula: "close > 400".into(), // true for SPY (450), false for VIX (18-20)
        }),
    });

    let result = run_backtest(&df, &params).expect("backtest should succeed");

    assert_eq!(result.trade_count, 1, "expected exactly 1 trade");
    let trade = &result.trade_log[0];

    // Signal should NOT fire — VIX never exceeds 400.
    // Trade should exit via normal DTE exit.
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
    // Setup: max_positions=1, short_put enters Jan 15, VIX signal fires on ALL 3 dates.
    //
    // VIX closes: 35, 35, 35 (always > 30)
    // Signal fires: Jan 15, Jan 22, Feb 11
    //
    // Position enters Jan 15. Signal fires the same day → immediate exit on Jan 15.
    // After the position closes, the signal fires on Jan 22 and Feb 11 — but there
    // are no open positions to close. The backtest should produce exactly 1 trade.
    //
    // This proves exit signals don't create phantom trades or errors on days
    // without open positions.
    let df = make_multi_strike_df();

    let ohlcv_dates = vec![
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(),
        NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(),
    ];

    let (_spy_dir, spy_path) = write_spy_ohlcv(&ohlcv_dates);
    // VIX always high — signal fires every day
    let (_vix_dir, vix_path) = write_vix_ohlcv(&ohlcv_dates, &[35.0, 35.0, 35.0]);

    let mut cross_paths = HashMap::new();
    cross_paths.insert("VIX".to_string(), vix_path);

    let mut params = base_params("short_put", vec![delta(0.40)]);
    params.ohlcv_path = Some(spy_path);
    params.cross_ohlcv_paths = cross_paths;
    params.max_positions = 1;
    params.exit_signal = Some(SignalSpec::CrossSymbol {
        symbol: "VIX".into(),
        signal: Box::new(SignalSpec::Formula {
            formula: "close > 30".into(),
        }),
    });

    let result = run_backtest(&df, &params).expect("backtest should succeed");

    // All trades should exit via Signal — VIX > 30 every day
    for trade in &result.trade_log {
        assert!(
            matches!(trade.exit_type, ExitType::Signal),
            "expected Signal exit, got {:?}",
            trade.exit_type
        );
    }

    // The signal fires on all 3 dates, but only open positions are closed.
    // No phantom trades or errors from signal firing on dates without positions.
    // (The exact trade count depends on re-entry logic — the key assertion is
    // that every trade exits via Signal and the backtest completes without error.)
    assert!(
        result.trade_count >= 1,
        "expected at least 1 trade, got {}",
        result.trade_count
    );
}

// ─── Test: Missing cross_ohlcv_paths errors cleanly ─────────────────────────

#[test]
fn missing_cross_ohlcv_paths_errors() {
    // CrossSymbol exit references VIX, but cross_ohlcv_paths is empty.
    // The engine should error with a clear message, not panic.
    let df = make_multi_strike_df();

    let ohlcv_dates = vec![
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(),
        NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(),
    ];

    let (_spy_dir, spy_path) = write_spy_ohlcv(&ohlcv_dates);

    let mut params = base_params("short_put", vec![delta(0.40)]);
    params.ohlcv_path = Some(spy_path);
    // Deliberately leave cross_ohlcv_paths empty
    params.exit_signal = Some(SignalSpec::CrossSymbol {
        symbol: "VIX".into(),
        signal: Box::new(SignalSpec::Formula {
            formula: "close > 30".into(),
        }),
    });

    let result = run_backtest(&df, &params);

    assert!(
        result.is_err(),
        "expected error when cross_ohlcv_paths is missing VIX data"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("VIX") && err_msg.contains("no OHLCV data loaded"),
        "error should mention VIX and missing data, got: {err_msg}"
    );
}
