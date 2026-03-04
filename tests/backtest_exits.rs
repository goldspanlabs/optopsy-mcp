//! Tests for exit priority ordering and trading-day gap handling.
//!
//! These tests use `make_multi_strike_df()` from the common module for data
//! consistency, and focus on asserting exit type priorities.

use optopsy_mcp::engine::core::run_backtest;
use optopsy_mcp::engine::types::{
    BacktestParams, DteRange, ExitType, Slippage, TargetRange, TradeSelector,
};

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
        multiplier: 100,
        max_positions: 5,
        selector: TradeSelector::First,
        adjustment_rules: vec![],
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: None,
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
fn dte_exit_is_baseline_exit() {
    // Verify that DTE exit works as the baseline exit mechanism.
    // No early exit conditions — trade should exit via DTE on Feb 11 (DTE=5).
    // Long call@100: entry mid=5.25 on Jan 15, exit mid=2.25 on Feb 11.
    // PnL = (2.25-5.25)*100 = -300
    let df = make_multi_strike_df();
    let params = base_params("long_call", vec![delta(0.50)]);

    let result = run_backtest(&df, &params).expect("backtest failed");

    assert_eq!(result.trade_count, 1, "expected exactly 1 trade");
    let trade = &result.trade_log[0];

    assert!(
        matches!(trade.exit_type, ExitType::DteExit),
        "expected DteExit, got {:?}",
        trade.exit_type
    );
    assert_eq!(
        trade.days_held, 27,
        "expected 27 days held (Jan 15 → Feb 11)"
    );
    assert!(
        (trade.pnl - (-300.0)).abs() < 0.01,
        "expected PnL -300.0, got {}",
        trade.pnl
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
