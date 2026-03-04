//! Tests for adjustment rules: defensive roll, calendar roll, delta drift triggers
//! with Close, Roll, and Add actions.
//!
//! Uses `make_multi_strike_df()` from the common module and runs through `run_backtest()`
//! for full end-to-end dispatch.

use chrono::NaiveDate;
use optopsy_mcp::engine::core::run_backtest;
use optopsy_mcp::engine::types::{
    AdjustmentAction, AdjustmentRule, AdjustmentTrigger, BacktestParams, DteRange, ExitType,
    Slippage, TargetRange, TradeSelector,
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

// ─── DefensiveRoll: Close action triggers on MTM loss ────────────────────────

#[test]
fn defensive_roll_closes_losing_position() {
    // Long call@100: entry mid=5.25 on Jan 15, Jan 22 mid=4.25.
    // MTM on Jan 22 = (4.25 - 5.25) * 100 = -100.
    // entry_cost = 525. loss_threshold=0.10 → threshold = 52.5.
    // -100 < -52.5 → DefensiveRoll fires on Jan 22, before DTE exit on Feb 11.
    let df = make_multi_strike_df();

    let mut params = base_params("long_call", vec![delta(0.50)]);
    params.adjustment_rules = vec![AdjustmentRule {
        trigger: AdjustmentTrigger::DefensiveRoll {
            loss_threshold: 0.10,
        },
        action: AdjustmentAction::Close {
            position_id: 0,
            leg_index: 0,
        },
    }];

    let result = run_backtest(&df, &params).expect("backtest failed");

    assert!(!result.trade_log.is_empty(), "expected at least 1 trade");
    let trade = &result.trade_log[0];

    assert!(
        matches!(trade.exit_type, ExitType::Adjustment),
        "expected Adjustment exit, got {:?}",
        trade.exit_type
    );
    // Should exit on Jan 22 (7 days), not Feb 11 (27 days)
    assert_eq!(
        trade.days_held, 7,
        "expected 7 days held (Jan 15 → Jan 22), got {}",
        trade.days_held
    );
}

// ─── CalendarRoll: Close action triggers on DTE threshold ────────────────────

#[test]
fn calendar_roll_closes_at_dte_trigger() {
    // Long call@100: entry Jan 15, exp Feb 16 (DTE=32).
    // Jan 22: DTE=25, Feb 11: DTE=5.
    // dte_trigger=25 → fires on Jan 22 (DTE=25 ≤ 25), before DTE exit at exit_dte=5.
    let df = make_multi_strike_df();

    let mut params = base_params("long_call", vec![delta(0.50)]);
    params.adjustment_rules = vec![AdjustmentRule {
        trigger: AdjustmentTrigger::CalendarRoll {
            dte_trigger: 25,
            new_dte: 45,
        },
        action: AdjustmentAction::Close {
            position_id: 0,
            leg_index: 0,
        },
    }];

    let result = run_backtest(&df, &params).expect("backtest failed");

    assert!(!result.trade_log.is_empty(), "expected at least 1 trade");
    let trade = &result.trade_log[0];

    assert!(
        matches!(trade.exit_type, ExitType::Adjustment),
        "expected Adjustment exit, got {:?}",
        trade.exit_type
    );
    assert_eq!(
        trade.days_held, 7,
        "expected 7 days held (Jan 15 → Jan 22), got {}",
        trade.days_held
    );
}

// ─── DeltaDrift: Close action triggers on high delta ─────────────────────────

#[test]
fn delta_drift_triggers_on_high_delta() {
    // Long call@100: delta=0.50 in the data.
    // max_delta=0.40 → 0.50 > 0.40 → rule is eligible on the first adjustment
    // phase after the position is opened.
    // The event loop runs adjustments before processing new entries, so a
    // position opened on Jan 15 cannot be adjusted until the next trading day.
    let df = make_multi_strike_df();

    let mut params = base_params("long_call", vec![delta(0.50)]);
    params.adjustment_rules = vec![AdjustmentRule {
        trigger: AdjustmentTrigger::DeltaDrift {
            leg_index: 0,
            max_delta: 0.40,
        },
        action: AdjustmentAction::Close {
            position_id: 0,
            leg_index: 0,
        },
    }];

    let result = run_backtest(&df, &params).expect("backtest failed");

    assert!(!result.trade_log.is_empty(), "expected at least 1 trade");
    let trade = &result.trade_log[0];

    assert!(
        matches!(trade.exit_type, ExitType::Adjustment),
        "expected Adjustment exit from DeltaDrift, got {:?}",
        trade.exit_type
    );
}

// ─── Roll action: replaces leg, position stays open ──────────────────────────

#[test]
fn roll_action_replaces_leg() {
    // Long call@100: entry Jan 15. DefensiveRoll at low threshold fires Jan 22.
    // Roll action: close leg 0, add new leg at strike 105, same near-term exp.
    // Position should stay open (has an open leg) and eventually exit via DTE on Feb 11.
    let df = make_multi_strike_df();
    let exp_near = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();

    let mut params = base_params("long_call", vec![delta(0.50)]);
    params.adjustment_rules = vec![AdjustmentRule {
        trigger: AdjustmentTrigger::DefensiveRoll {
            loss_threshold: 0.10,
        },
        action: AdjustmentAction::Roll {
            position_id: 0,
            leg_index: 0,
            new_strike: 105.0,
            new_expiration: exp_near,
        },
    }];

    let result = run_backtest(&df, &params).expect("backtest failed");

    assert!(!result.trade_log.is_empty(), "expected at least 1 trade");
    let trade = &result.trade_log[0];

    // After roll, position stays open with new leg. Should exit via DTE on Feb 11.
    assert!(
        matches!(trade.exit_type, ExitType::DteExit),
        "expected DteExit after roll, got {:?}",
        trade.exit_type
    );
    // Entry Jan 15 → exit Feb 11 = 27 days
    assert_eq!(
        trade.days_held, 27,
        "expected 27 days held (full duration after roll), got {}",
        trade.days_held
    );
}

// ─── No-fire: high threshold means adjustment never triggers ─────────────────

#[test]
fn adjustment_no_fire_below_threshold() {
    // DefensiveRoll with loss_threshold=999.0 → threshold = 999 * 525 = huge.
    // MTM never reaches that, so adjustment never fires. Normal DTE exit on Feb 11.
    let df = make_multi_strike_df();

    let mut params = base_params("long_call", vec![delta(0.50)]);
    params.adjustment_rules = vec![AdjustmentRule {
        trigger: AdjustmentTrigger::DefensiveRoll {
            loss_threshold: 999.0,
        },
        action: AdjustmentAction::Close {
            position_id: 0,
            leg_index: 0,
        },
    }];

    let result = run_backtest(&df, &params).expect("backtest failed");

    assert!(!result.trade_log.is_empty(), "expected at least 1 trade");
    let trade = &result.trade_log[0];

    assert!(
        matches!(trade.exit_type, ExitType::DteExit),
        "expected normal DteExit (adjustment should not fire), got {:?}",
        trade.exit_type
    );
    assert_eq!(
        trade.days_held, 27,
        "expected 27 days held (Jan 15 → Feb 11)"
    );
}

// ─── Parity: unreachable adjustment preserves same results as no adjustment ──

#[test]
fn noop_rule_preserves_parity() {
    // With an unreachable rule (threshold=999), results should match no-rule backtest.
    let df = make_multi_strike_df();

    let params_no_adj = base_params("long_call", vec![delta(0.50)]);
    let mut params_with_adj = base_params("long_call", vec![delta(0.50)]);
    params_with_adj.adjustment_rules = vec![AdjustmentRule {
        trigger: AdjustmentTrigger::DefensiveRoll {
            loss_threshold: 999.0,
        },
        action: AdjustmentAction::Close {
            position_id: 0,
            leg_index: 0,
        },
    }];

    let result_no = run_backtest(&df, &params_no_adj).expect("backtest failed");
    let result_with = run_backtest(&df, &params_with_adj).expect("backtest failed");

    assert_eq!(result_no.trade_log.len(), result_with.trade_log.len());

    for (t1, t2) in result_no.trade_log.iter().zip(result_with.trade_log.iter()) {
        assert!(
            (t1.pnl - t2.pnl).abs() < 0.01,
            "PnL mismatch: {} vs {}",
            t1.pnl,
            t2.pnl
        );
        assert_eq!(t1.days_held, t2.days_held);
    }
}
