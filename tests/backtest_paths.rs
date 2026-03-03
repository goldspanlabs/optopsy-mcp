//! Parity tests verifying that the vectorized and event-loop backtest paths
//! produce identical results for the same inputs.
//!
//! The vectorized path is used when `adjustment_rules` is empty.
//! The event-loop path is used when `adjustment_rules` is non-empty.
//! A dummy rule with an impossibly high threshold ensures the event loop is
//! exercised without actually triggering any adjustment.

use optopsy_mcp::engine::core::run_backtest;
use optopsy_mcp::engine::types::{
    AdjustmentAction, AdjustmentRule, AdjustmentTrigger, BacktestParams, Slippage, TargetRange,
    TradeSelector,
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
