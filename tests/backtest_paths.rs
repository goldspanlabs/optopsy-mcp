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
    AdjustmentAction, AdjustmentRule, AdjustmentTrigger, BacktestParams, ExitType, TargetRange,
};

mod common;
use common::{backtest_params, delta, make_multi_strike_df};

// ─── Helpers ─────────────────────────────────────────────────────────────────

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

/// Run backtest via both paths and assert parity on all trade log fields.
fn assert_parity(mut params: BacktestParams) {
    let df = make_multi_strike_df();
    let strategy = params.strategy.clone();

    // Vectorized path (empty adjustment_rules)
    params.adjustment_rules = vec![];
    let vec_result = run_backtest(&df, &params).expect("vectorized backtest failed");

    // Event-loop path (non-empty adjustment_rules with no-op rule)
    params.adjustment_rules = vec![noop_adjustment_rule()];
    let evt_result = run_backtest(&df, &params).expect("event-loop backtest failed");

    // Same trade count
    assert_eq!(
        vec_result.trade_count, evt_result.trade_count,
        "{strategy}: trade count mismatch: vectorized={}, event={}",
        vec_result.trade_count, evt_result.trade_count
    );

    // Same total PnL (within epsilon for floating point)
    assert!(
        (vec_result.total_pnl - evt_result.total_pnl).abs() < 0.01,
        "{strategy}: PnL mismatch: vectorized={}, event={}",
        vec_result.total_pnl,
        evt_result.total_pnl
    );

    // Same trade log fields per trade
    assert_eq!(
        vec_result.trade_log.len(),
        evt_result.trade_log.len(),
        "{strategy}: trade log length mismatch"
    );
    for (i, (v, e)) in vec_result
        .trade_log
        .iter()
        .zip(evt_result.trade_log.iter())
        .enumerate()
    {
        assert_eq!(
            v.entry_datetime, e.entry_datetime,
            "{strategy} trade {i}: entry_datetime mismatch"
        );
        assert_eq!(
            v.exit_datetime, e.exit_datetime,
            "{strategy} trade {i}: exit_datetime mismatch"
        );
        assert!(
            (v.entry_cost - e.entry_cost).abs() < 0.01,
            "{strategy} trade {i}: entry_cost mismatch: vectorized={}, event={}",
            v.entry_cost,
            e.entry_cost
        );
        assert!(
            (v.exit_proceeds - e.exit_proceeds).abs() < 0.01,
            "{strategy} trade {i}: exit_proceeds mismatch: vectorized={}, event={}",
            v.exit_proceeds,
            e.exit_proceeds
        );
        assert!(
            (v.pnl - e.pnl).abs() < 0.01,
            "{strategy} trade {i}: PnL mismatch: vectorized={}, event={}",
            v.pnl,
            e.pnl
        );
        assert_eq!(
            v.days_held, e.days_held,
            "{strategy} trade {i}: days held mismatch: vectorized={}, event={}",
            v.days_held, e.days_held
        );
        assert_eq!(
            std::mem::discriminant(&v.exit_type),
            std::mem::discriminant(&e.exit_type),
            "{strategy} trade {i}: exit type mismatch: vectorized={:?}, event={:?}",
            v.exit_type,
            e.exit_type
        );
    }
}

// ─── Parity Tests ────────────────────────────────────────────────────────────

#[test]
fn parity_basic_long_call() {
    // Baseline: DTE exit only, no early-exit conditions
    let params = backtest_params("long_call", vec![delta(0.50)]);
    assert_parity(params);
}

#[test]
fn parity_with_stop_loss() {
    // Long call loses value → both paths should trigger SL on same day
    let mut params = backtest_params("long_call", vec![delta(0.50)]);
    params.stop_loss = Some(0.30); // 30% of entry cost
    assert_parity(params);
}

#[test]
fn parity_with_take_profit() {
    // Short call gains value → both paths should trigger TP on same day
    let mut params = backtest_params("short_call", vec![delta(0.50)]);
    params.take_profit = Some(0.30); // 30% of entry cost
    assert_parity(params);
}

#[test]
fn parity_with_max_hold_days() {
    // Force exit after 10 days → both paths should trigger MaxHold
    let mut params = backtest_params("long_call", vec![delta(0.50)]);
    params.max_hold_days = Some(10);
    assert_parity(params);
}

#[test]
fn parity_spread_strategy() {
    // Bull call spread (2-leg): both paths must agree on multi-leg P&L
    let params = backtest_params("bull_call_spread", vec![delta(0.50), delta(0.35)]);
    assert_parity(params);
}

#[test]
fn parity_short_put() {
    // Single-leg put credit: ensures put-side handling agrees
    let params = backtest_params("short_put", vec![delta(0.40)]);
    assert_parity(params);
}

#[test]
fn parity_bear_put_spread() {
    // 2-leg put spread: both paths must agree on put multi-leg P&L
    let params = backtest_params("bear_put_spread", vec![delta(0.40), delta(0.55)]);
    assert_parity(params);
}

#[test]
fn parity_butterfly() {
    // 3-leg (middle ×2): both paths must agree on butterfly cost aggregation
    let params = backtest_params(
        "long_call_butterfly",
        vec![delta(0.50), delta(0.35), delta(0.20)],
    );
    assert_parity(params);
}

#[test]
fn parity_iron_condor() {
    // 4-leg mixed puts+calls: full multi-leg parity
    let params = backtest_params(
        "iron_condor",
        vec![delta(0.20), delta(0.40), delta(0.35), delta(0.20)],
    );
    assert_parity(params);
}

#[test]
fn parity_calendar_spread() {
    // Multi-expiration: near+far term must agree across paths
    let params = backtest_params("call_calendar_spread", vec![delta(0.50), delta(0.50)]);
    assert_parity(params);
}

#[test]
fn event_loop_dispatch_with_adjustment_rules() {
    // Verify that run_backtest with non-empty adjustment_rules doesn't error
    // and returns a valid result (exercises event-loop dispatch path)
    let df = make_multi_strike_df();
    let mut params = backtest_params("long_call", vec![delta(0.50)]);
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
//
// Mid prices from synthetic data (entry=Jan 15, exit=Feb 11, Slippage::Mid):
//   Near-term calls: @95=8.25/5.25, @100=5.25/2.25, @105=3.25/1.25, @110=1.75/0.55
//   Near-term puts:  @95=1.25/0.45, @100=2.75/1.25, @105=4.75/2.75, @110=7.25/4.75
//   Far-term calls:  @100=7.25/4.75, @105=4.75/3.05

/// Assert every `TradeRecord` field on a single path.
fn assert_trade_fields(
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
        "{label}: exit_proceeds ({}) != entry_cost ({}) + pnl ({})",
        trade.exit_proceeds,
        trade.entry_cost,
        trade.pnl
    );
}

/// Run correctness checks on both vectorized and event-loop paths.
fn assert_correctness_both_paths(
    strategy: &str,
    deltas: Vec<TargetRange>,
    expected_entry_cost: f64,
    expected_exit_proceeds: f64,
    expected_pnl: f64,
) {
    let params = backtest_params(strategy, deltas);
    assert_trade_fields(
        &format!("{strategy}[vec]"),
        &params,
        expected_entry_cost,
        expected_exit_proceeds,
        expected_pnl,
    );

    let mut evt_params = params;
    evt_params.adjustment_rules = vec![noop_adjustment_rule()];
    assert_trade_fields(
        &format!("{strategy}[evt]"),
        &evt_params,
        expected_entry_cost,
        expected_exit_proceeds,
        expected_pnl,
    );
}

#[test]
fn correctness_long_call() {
    // L Call@100: cost = 5.25×1×100 = 525, proceeds = 2.25×1×100 = 225, pnl = -300
    assert_correctness_both_paths("long_call", vec![delta(0.50)], 525.0, 225.0, -300.0);
}

#[test]
fn correctness_short_call() {
    // S Call@100: cost = 5.25×(-1)×100 = -525, proceeds = 2.25×(-1)×100 = -225, pnl = 300
    assert_correctness_both_paths("short_call", vec![delta(0.50)], -525.0, -225.0, 300.0);
}

#[test]
fn correctness_short_put() {
    // S Put@100: cost = 2.75×(-1)×100 = -275, proceeds = 1.25×(-1)×100 = -125, pnl = 150
    assert_correctness_both_paths("short_put", vec![delta(0.40)], -275.0, -125.0, 150.0);
}

#[test]
fn correctness_bull_call_spread() {
    // L Call@100 + S Call@105
    // entry = 525 + (-325) = 200, exit = 225 + (-125) = 100, pnl = -100
    assert_correctness_both_paths(
        "bull_call_spread",
        vec![delta(0.50), delta(0.35)],
        200.0,
        100.0,
        -100.0,
    );
}

#[test]
fn correctness_bear_put_spread() {
    // L Put@100 (δ0.40) + S Put@105 (δ0.55)
    // L Put@100: cost=275, proceeds=125; S Put@105: cost=-475, proceeds=-275
    // entry = 275 - 475 = -200, exit = 125 - 275 = -150, pnl = 50
    assert_correctness_both_paths(
        "bear_put_spread",
        vec![delta(0.40), delta(0.55)],
        -200.0,
        -150.0,
        50.0,
    );
}

#[test]
fn correctness_long_call_butterfly() {
    // L Call@100 + S Call@105 ×2 + L Call@110
    // entry = 525 - 650 + 175 = 50, exit = 225 - 250 + 55 = 30, pnl = -20
    assert_correctness_both_paths(
        "long_call_butterfly",
        vec![delta(0.50), delta(0.35), delta(0.20)],
        50.0,
        30.0,
        -20.0,
    );
}

#[test]
fn correctness_iron_condor() {
    // L Put@95 + S Put@100 + S Call@105 + L Call@110
    // entry = 125 - 275 - 325 + 175 = -300, exit = 45 - 125 - 125 + 55 = -150, pnl = 150
    assert_correctness_both_paths(
        "iron_condor",
        vec![delta(0.20), delta(0.40), delta(0.35), delta(0.20)],
        -300.0,
        -150.0,
        150.0,
    );
}

#[test]
fn correctness_call_calendar_spread() {
    // S near Call@100 + L far Call@100
    // S near: cost=-525, proceeds=-225; L far: cost=725, proceeds=475
    // entry = -525 + 725 = 200, exit = -225 + 475 = 250, pnl = 50
    assert_correctness_both_paths(
        "call_calendar_spread",
        vec![delta(0.50), delta(0.50)],
        200.0,
        250.0,
        50.0,
    );
}
