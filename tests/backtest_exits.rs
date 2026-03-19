//! Tests for exit priority ordering and trading-day gap handling.
//!
//! These tests use `make_multi_strike_df()` from the common module for data
//! consistency, and focus on asserting exit type priorities.

use chrono::NaiveDate;
use optopsy_mcp::data::parquet::DATETIME_COL;
use optopsy_mcp::engine::core::run_backtest;
use optopsy_mcp::engine::types::{
    BacktestParams, DteRange, ExitType, Slippage, TargetRange, TradeSelector,
};
use polars::prelude::*;

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

// ─── Weekend Gap Test ────────────────────────────────────────────────────────

#[test]
fn weekend_gap_max_hold_exits_correctly() {
    // Using make_multi_strike_df: 3 dates (Jan 15, Jan 22, Feb 11), near-term exp Feb 16.
    // Entry Jan 15 (DTE=32), exit_dte=5 → DTE exit on Feb 11 (DTE=5).
    // max_hold_days=5 → fires on Jan 22 (7 calendar days > 5).
    // Jan 15 → Jan 22 is a 7-day gap but only 2 trading days in the data.
    // The MaxHold check uses calendar days (not trading days), so it fires on Jan 22.
    let df = make_multi_strike_df();

    let mut params = base_params("long_call", vec![delta(0.50)]);
    params.max_hold_days = Some(5);

    let result = run_backtest(&df, &params).expect("backtest failed");

    assert!(!result.trade_log.is_empty(), "expected at least 1 trade");
    let trade = &result.trade_log[0];

    // First trade should exit via MaxHold on Jan 22 (before DTE exit on Feb 11)
    assert!(
        matches!(trade.exit_type, ExitType::MaxHold),
        "expected MaxHold exit, got {:?}",
        trade.exit_type
    );
    assert_eq!(
        trade.days_held, 7,
        "expected 7 days held (Jan 15 → Jan 22), got {}",
        trade.days_held
    );
}

// ─── Exit Priority Tests ────────────────────────────────────────────────────

#[test]
fn max_hold_beats_stop_loss_same_day() {
    // Using make_multi_strike_df: entry Jan 15, intermediate Jan 22, exit Feb 11.
    // Long call@100: entry mid=5.25, Jan 22 mid=4.25 → MTM=(4.25-5.25)*100=-100.
    // SL at 10% of entry cost: threshold = 5.25*100*0.10 = 52.5. MTM=-100 < -52.5 → SL fires.
    // MaxHold(5) also fires on Jan 22 (7 days > 5).
    // MaxHold has higher priority than StopLoss in both paths.
    let df = make_multi_strike_df();

    let mut params = base_params("long_call", vec![delta(0.50)]);
    params.max_hold_days = Some(5);
    params.stop_loss = Some(0.10); // Low threshold so SL also fires on Jan 22

    let result = run_backtest(&df, &params).expect("backtest failed");

    assert!(!result.trade_log.is_empty(), "expected at least 1 trade");
    let trade = &result.trade_log[0];

    // MaxHold has higher priority than StopLoss
    assert!(
        matches!(trade.exit_type, ExitType::MaxHold),
        "expected MaxHold (higher priority than StopLoss), got {:?}",
        trade.exit_type
    );
}

#[test]
fn stop_loss_exits_before_dte() {
    // Long call@100: entry mid=5.25, Jan 22 mid=4.25, Feb 11 mid=2.25.
    // SL at 10%: threshold = 525*0.10 = 52.5. Jan 22 MTM=-100 < -52.5 → SL fires.
    // SL should fire on Jan 22, before the DTE exit on Feb 11.
    let df = make_multi_strike_df();

    let mut params = base_params("long_call", vec![delta(0.50)]);
    params.stop_loss = Some(0.10);

    let result = run_backtest(&df, &params).expect("backtest failed");

    assert!(!result.trade_log.is_empty(), "expected at least 1 trade");
    let trade = &result.trade_log[0];

    assert!(
        matches!(trade.exit_type, ExitType::StopLoss),
        "expected StopLoss, got {:?}",
        trade.exit_type
    );
    assert_eq!(
        trade.days_held, 7,
        "expected 7 days held (Jan 15 → Jan 22), got {}",
        trade.days_held
    );
}

// ─── Entry DTE Min Floor Tests ──────────────────────────────────────────────
//
// These tests prove that entry_dte.min prevents the "1-day trade" bug.
//
// Previously, the entry DTE floor was `exit_dte + 1`. With exit_dte=5, a
// candidate at DTE=6 could enter and exit the next day at DTE=5 — producing
// a 1-day trade even when the user expected ~40-day trades.
//
// Now `entry_dte.min` is an explicit floor. Setting min=30 with exit_dte=5
// guarantees trades hold for at least ~25 days.

/// Build synthetic data with consecutive dates and a near-expiry option to
/// reproduce the 1-day trade scenario.
///
/// Dates: Jan 10, Jan 11, Jan 12, Feb 20
/// Short exp: Jan 16 (DTE = 6, 5, 4, expired)
/// Long exp:  Feb 25 (DTE = 46, 45, 44, 5)
/// Single call strike at 100, delta 0.50
fn make_short_dte_df() -> DataFrame {
    let exp_short = NaiveDate::from_ymd_opt(2024, 1, 16).unwrap();
    let exp_long = NaiveDate::from_ymd_opt(2024, 2, 25).unwrap();

    let dates = [
        NaiveDate::from_ymd_opt(2024, 1, 10).unwrap(), // DTE: short=6, long=46
        NaiveDate::from_ymd_opt(2024, 1, 11).unwrap(), // DTE: short=5, long=45
        NaiveDate::from_ymd_opt(2024, 1, 12).unwrap(), // DTE: short=4, long=44
        NaiveDate::from_ymd_opt(2024, 2, 20).unwrap(), // DTE: short=expired, long=5
    ];

    let mut quote_dates = Vec::new();
    let mut expirations = Vec::new();
    let mut option_types = Vec::new();
    let mut strikes = Vec::new();
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    let mut deltas = Vec::new();

    // Short-exp call: prices decay quickly
    let short_bids = [3.00, 2.50, 2.00, 0.0]; // expired by Feb 20
    let short_asks = [3.50, 3.00, 2.50, 0.0];
    // Long-exp call: slower decay
    let long_bids = [7.00, 6.90, 6.80, 2.00];
    let long_asks = [7.50, 7.40, 7.30, 2.50];

    for (i, date) in dates.iter().enumerate() {
        // Short-exp rows (skip expired date)
        if i < 3 {
            quote_dates.push(date.and_hms_opt(0, 0, 0).unwrap());
            expirations.push(exp_short);
            option_types.push("c");
            strikes.push(100.0);
            bids.push(short_bids[i]);
            asks.push(short_asks[i]);
            deltas.push(0.50);
        }

        // Long-exp rows (all dates)
        quote_dates.push(date.and_hms_opt(0, 0, 0).unwrap());
        expirations.push(exp_long);
        option_types.push("c");
        strikes.push(100.0);
        bids.push(long_bids[i]);
        asks.push(long_asks[i]);
        deltas.push(0.50);
    }

    let mut df = df! {
        DATETIME_COL => &quote_dates,
        "option_type" => &option_types,
        "strike" => &strikes,
        "bid" => &bids,
        "ask" => &asks,
        "delta" => &deltas,
    }
    .unwrap();

    df.with_column(
        DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations).into_column(),
    )
    .unwrap();

    df
}

#[test]
fn low_dte_floor_produces_one_day_trade() {
    // Reproduce the old behavior: with min=6 (simulating exit_dte+1 floor),
    // the short-exp candidate (DTE=6 on Jan 10) qualifies.
    // Trade enters Jan 10, exits Jan 11 (DTE=5) = 1 day held.
    // This is the bug we're fixing — user expected ~40-day trades but got 1-day trades.
    let df = make_short_dte_df();

    let mut params = base_params("long_call", vec![delta(0.50)]);
    params.entry_dte = DteRange {
        target: 6,
        min: 6, // old floor: exit_dte + 1
        max: 50,
    };
    params.exit_dte = 5;
    params.selector = TradeSelector::Nearest; // picks DTE closest to target=6

    let result = run_backtest(&df, &params).expect("backtest failed");

    assert!(!result.trade_log.is_empty(), "expected at least 1 trade");
    let trade = &result.trade_log[0];

    // With the low floor, we get a 1-day trade — this is the problem
    assert_eq!(
        trade.days_held, 1,
        "low DTE floor should produce a 1-day trade (Jan 10→Jan 11), got {}",
        trade.days_held
    );
}

#[test]
fn entry_dte_min_prevents_one_day_trade() {
    // Same data as above, but with min=30.
    // Short-exp DTE=6 < 30 → rejected. Only long-exp DTE=46 qualifies.
    // Trade enters Jan 10 (DTE=46), exits Feb 20 (DTE=5) = 41 days held.
    // The min floor guarantees trades last at least (min - exit_dte) ≈ 25 days.
    let df = make_short_dte_df();

    let mut params = base_params("long_call", vec![delta(0.50)]);
    params.entry_dte = DteRange {
        target: 45,
        min: 30,
        max: 50,
    };
    params.exit_dte = 5;

    let result = run_backtest(&df, &params).expect("backtest failed");

    assert_eq!(result.trade_count, 1, "expected exactly 1 trade");
    let trade = &result.trade_log[0];

    // With min=30, the short-exp DTE=6 is rejected. Long-exp enters at DTE=46.
    // Trade holds for 41 days (Jan 10 → Feb 20), matching the user's expectation.
    assert_eq!(
        trade.days_held, 41,
        "expected 41 days held (Jan 10→Feb 20), got {}",
        trade.days_held
    );
    assert!(
        trade.days_held >= 25,
        "entry_dte.min=30 guarantees trades hold at least ~25 days, got {}",
        trade.days_held
    );
}

#[test]
fn wide_dte_range_produces_proportional_hold_duration() {
    // The user's core expectation: with entry_dte={target:45, min:30, max:50}
    // and exit_dte=5, trades should hold for roughly 25-45 days, not 1-2 days.
    //
    // Using make_multi_strike_df: near-term DTE=32, exit_dte=5.
    // Expected hold: ~27 days (DTE 32→5).
    // Minimum guaranteed hold: entry_dte.min - exit_dte = 30 - 5 = 25 days.
    let df = make_multi_strike_df();

    let mut params = base_params("long_call", vec![delta(0.50)]);
    params.entry_dte = DteRange {
        target: 45,
        min: 30,
        max: 45,
    };

    let result = run_backtest(&df, &params).expect("backtest failed");

    assert_eq!(result.trade_count, 1);
    let trade = &result.trade_log[0];

    let min_expected_hold = i64::from(params.entry_dte.min - params.exit_dte);
    assert!(
        trade.days_held >= min_expected_hold,
        "wide DTE range (min={}, exit={}) should guarantee ≥{} day hold, got {}",
        params.entry_dte.min,
        params.exit_dte,
        min_expected_hold,
        trade.days_held,
    );
}

#[test]
fn take_profit_exits_before_dte() {
    // Short call@100: entry mid=5.25 (credit), Jan 22 mid=4.25.
    // MTM = (5.25 - 4.25) * 100 = 100 (profit from decay).
    // TP at 10%: threshold = 525*0.10 = 52.5. MTM=100 > 52.5 → TP fires.
    let df = make_multi_strike_df();

    let mut params = base_params("short_call", vec![delta(0.50)]);
    params.take_profit = Some(0.10);

    let result = run_backtest(&df, &params).expect("backtest failed");

    assert!(!result.trade_log.is_empty(), "expected at least 1 trade");
    let trade = &result.trade_log[0];

    assert!(
        matches!(trade.exit_type, ExitType::TakeProfit),
        "expected TakeProfit, got {:?}",
        trade.exit_type
    );
}
