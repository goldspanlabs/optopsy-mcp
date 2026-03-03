//! Parity tests verifying that the vectorized and event-loop backtest paths
//! produce identical results for the same inputs.
//!
//! The vectorized path is used when `adjustment_rules` is empty.
//! The event-loop path is used when `adjustment_rules` is non-empty.
//! A dummy rule with an impossibly high threshold ensures the event loop is
//! exercised without actually triggering any adjustment.

use chrono::NaiveDate;
use optopsy_mcp::engine::core::run_backtest;
use optopsy_mcp::engine::types::{
    AdjustmentAction, AdjustmentRule, AdjustmentTrigger, BacktestParams, ExitType, Slippage,
    TargetRange, TradeSelector,
};

mod common;
use common::{delta, make_multi_strike_df};

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Build default backtest params (vectorized path — empty `adjustment_rules`).
fn base_params(strategy: &str, leg_deltas: Vec<TargetRange>) -> BacktestParams {
    BacktestParams {
        strategy: strategy.to_string(),
        leg_deltas,
        max_entry_dte: 45,
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

/// Return a dummy adjustment rule that never fires (loss threshold impossibly high).
fn noop_adjustment_rule() -> AdjustmentRule {
    AdjustmentRule {
        trigger: AdjustmentTrigger::DefensiveRoll {
            loss_threshold: 999.0,
        },
        action: AdjustmentAction::Close {
            position_id: 0,
            leg_index: 0,
        },
    }
}

/// Run backtest via both paths and assert parity on trade count, `PnL`, exit types, and days held.
fn assert_parity(mut params: BacktestParams) {
    let df = make_multi_strike_df();

    // Vectorized path (empty adjustment_rules)
    params.adjustment_rules = vec![];
    let vec_result = run_backtest(&df, &params).expect("vectorized backtest failed");

    // Event-loop path (non-empty adjustment_rules with no-op rule)
    params.adjustment_rules = vec![noop_adjustment_rule()];
    let evt_result = run_backtest(&df, &params).expect("event-loop backtest failed");

    // Same trade count
    assert_eq!(
        vec_result.trade_count, evt_result.trade_count,
        "trade count mismatch: vectorized={}, event={}",
        vec_result.trade_count, evt_result.trade_count
    );

    // Same total PnL (within epsilon for floating point)
    assert!(
        (vec_result.total_pnl - evt_result.total_pnl).abs() < 0.01,
        "PnL mismatch: vectorized={}, event={}",
        vec_result.total_pnl,
        evt_result.total_pnl
    );

    // Same exit types and days held per trade
    assert_eq!(
        vec_result.trade_log.len(),
        evt_result.trade_log.len(),
        "trade log length mismatch"
    );
    for (i, (v, e)) in vec_result
        .trade_log
        .iter()
        .zip(evt_result.trade_log.iter())
        .enumerate()
    {
        assert_eq!(
            std::mem::discriminant(&v.exit_type),
            std::mem::discriminant(&e.exit_type),
            "trade {i}: exit type mismatch: vectorized={:?}, event={:?}",
            v.exit_type,
            e.exit_type
        );
        assert_eq!(
            v.days_held, e.days_held,
            "trade {i}: days held mismatch: vectorized={}, event={}",
            v.days_held, e.days_held
        );
        assert!(
            (v.pnl - e.pnl).abs() < 0.01,
            "trade {i}: PnL mismatch: vectorized={}, event={}",
            v.pnl,
            e.pnl
        );
    }
}

// ─── Parity Tests ────────────────────────────────────────────────────────────

#[test]
fn parity_basic_long_call() {
    // Baseline: DTE exit only, no early-exit conditions
    let params = base_params("long_call", vec![delta(0.50)]);
    assert_parity(params);
}

#[test]
fn parity_with_stop_loss() {
    // Long call loses value → both paths should trigger SL on same day
    let mut params = base_params("long_call", vec![delta(0.50)]);
    params.stop_loss = Some(0.30); // 30% of entry cost
    assert_parity(params);
}

#[test]
fn parity_with_take_profit() {
    // Short call gains value → both paths should trigger TP on same day
    let mut params = base_params("short_call", vec![delta(0.50)]);
    params.take_profit = Some(0.30); // 30% of entry cost
    assert_parity(params);
}

#[test]
fn parity_with_max_hold_days() {
    // Force exit after 10 days → both paths should trigger MaxHold
    let mut params = base_params("long_call", vec![delta(0.50)]);
    params.max_hold_days = Some(10);
    assert_parity(params);
}

#[test]
fn parity_spread_strategy() {
    // Bull call spread (2-leg): both paths must agree on multi-leg P&L
    let params = base_params("bull_call_spread", vec![delta(0.50), delta(0.35)]);
    assert_parity(params);
}

#[test]
fn event_loop_dispatch_with_adjustment_rules() {
    // Verify that run_backtest with non-empty adjustment_rules doesn't error
    // and returns a valid result (exercises event-loop dispatch path)
    let df = make_multi_strike_df();
    let mut params = base_params("long_call", vec![delta(0.50)]);
    params.adjustment_rules = vec![noop_adjustment_rule()];

    let result = run_backtest(&df, &params);
    assert!(
        result.is_ok(),
        "event-loop dispatch failed: {:?}",
        result.err()
    );

    let bt = result.unwrap();
    assert!(bt.trade_count > 0, "expected at least 1 trade");
    assert!(!bt.trade_log.is_empty(), "expected non-empty trade log");
}

// ─── Trade Log Correctness Tests ─────────────────────────────────────────────
//
// These tests verify every field of TradeRecord against hand-calculated values,
// running both vectorized (empty adjustment_rules) and event-loop paths.

/// Assert every field of a single-trade backtest result against expected values.
fn assert_trade_correctness(
    label: &str,
    params: &BacktestParams,
    expected_entry_cost: f64,
    expected_exit_proceeds: f64,
    expected_pnl: f64,
) {
    let df = make_multi_strike_df();
    let bt = run_backtest(&df, params).unwrap_or_else(|e| panic!("{label}: backtest failed: {e}"));

    assert_eq!(bt.trade_count, 1, "{label}: expected 1 trade");
    let trade = &bt.trade_log[0];

    let entry_date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
    let exit_date = NaiveDate::from_ymd_opt(2024, 2, 11).unwrap();

    assert_eq!(
        trade.entry_datetime.date(),
        entry_date,
        "{label}: entry date"
    );
    assert_eq!(trade.exit_datetime.date(), exit_date, "{label}: exit date");
    assert_eq!(trade.days_held, 27, "{label}: days_held");
    assert!(
        matches!(trade.exit_type, ExitType::DteExit),
        "{label}: expected DteExit, got {:?}",
        trade.exit_type
    );
    assert!(
        (trade.entry_cost - expected_entry_cost).abs() < 0.01,
        "{label}: entry_cost expected {expected_entry_cost}, got {}",
        trade.entry_cost
    );
    assert!(
        (trade.exit_proceeds - expected_exit_proceeds).abs() < 0.01,
        "{label}: exit_proceeds expected {expected_exit_proceeds}, got {}",
        trade.exit_proceeds
    );
    assert!(
        (trade.pnl - expected_pnl).abs() < 0.01,
        "{label}: pnl expected {expected_pnl}, got {}",
        trade.pnl
    );
    // Internal consistency
    assert!(
        (trade.exit_proceeds - (trade.entry_cost + trade.pnl)).abs() < 0.01,
        "{label}: exit_proceeds != entry_cost + pnl"
    );
}

#[test]
fn correctness_long_call_trade_log() {
    // L Call@100: entry_mid=5.25, exit_mid=2.25
    // entry_cost = 5.25 × 1 × 100 = 525.0
    // exit_proceeds = 2.25 × 1 × 100 = 225.0
    // pnl = 225.0 - 525.0 = -300.0
    let vectorized = base_params("long_call", vec![delta(0.50)]);
    assert_trade_correctness("long_call[vec]", &vectorized, 525.0, 225.0, -300.0);

    let mut event_loop = vectorized;
    event_loop.adjustment_rules = vec![noop_adjustment_rule()];
    assert_trade_correctness("long_call[evt]", &event_loop, 525.0, 225.0, -300.0);
}

#[test]
fn correctness_short_call_trade_log() {
    // S Call@100: entry_mid=5.25, exit_mid=2.25
    // entry_cost = 5.25 × (-1) × 100 = -525.0
    // exit_proceeds = 2.25 × (-1) × 100 = -225.0
    // pnl = -225.0 - (-525.0) = 300.0
    let vectorized = base_params("short_call", vec![delta(0.50)]);
    assert_trade_correctness("short_call[vec]", &vectorized, -525.0, -225.0, 300.0);

    let mut event_loop = vectorized;
    event_loop.adjustment_rules = vec![noop_adjustment_rule()];
    assert_trade_correctness("short_call[evt]", &event_loop, -525.0, -225.0, 300.0);
}

#[test]
fn correctness_bull_call_spread_trade_log() {
    // L Call@100 (δ0.50) + S Call@105 (δ0.35)
    // L Call@100: entry_mid=5.25, exit_mid=2.25 → cost=525, proceeds=225
    // S Call@105: entry_mid=3.25, exit_mid=1.25 → cost=-325, proceeds=-125
    // entry_cost = 525 + (-325) = 200.0
    // exit_proceeds = 225 + (-125) = 100.0
    // pnl = 100.0 - 200.0 = -100.0
    let vectorized = base_params("bull_call_spread", vec![delta(0.50), delta(0.35)]);
    assert_trade_correctness("bull_call_spread[vec]", &vectorized, 200.0, 100.0, -100.0);

    let mut event_loop = vectorized;
    event_loop.adjustment_rules = vec![noop_adjustment_rule()];
    assert_trade_correctness("bull_call_spread[evt]", &event_loop, 200.0, 100.0, -100.0);
}
