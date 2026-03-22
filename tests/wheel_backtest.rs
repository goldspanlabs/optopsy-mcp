//! Integration tests for the wheel strategy backtest engine.
//!
//! These tests exercise `run_wheel_backtest()` end-to-end with synthetic
//! options `DataFrame`s and OHLCV close maps, covering full cycles, OTM puts,
//! multiple rotations, cost basis verification, and signal filtering.

use std::collections::{BTreeMap, HashSet};

use chrono::NaiveDate;
use polars::prelude::*;

use optopsy_mcp::data::parquet::DATETIME_COL;
use optopsy_mcp::engine::types::{DteRange, ExitType, Slippage, TargetRange};
use optopsy_mcp::engine::wheel_sim::{run_wheel_backtest, WheelParams};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn d(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

fn dt(y: i32, m: u32, day: u32) -> chrono::NaiveDateTime {
    NaiveDate::from_ymd_opt(y, m, day)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

/// Build an options `DataFrame` from rows.
///
/// Each row: (datetime, expiration, `option_type`, strike, bid, ask, delta)
fn make_options_df(
    rows: &[(chrono::NaiveDateTime, NaiveDate, &str, f64, f64, f64, f64)],
) -> DataFrame {
    let dates: Vec<chrono::NaiveDateTime> = rows.iter().map(|r| r.0).collect();
    let expirations: Vec<NaiveDate> = rows.iter().map(|r| r.1).collect();
    let opt_types: Vec<&str> = rows.iter().map(|r| r.2).collect();
    let strikes: Vec<f64> = rows.iter().map(|r| r.3).collect();
    let bids: Vec<f64> = rows.iter().map(|r| r.4).collect();
    let asks: Vec<f64> = rows.iter().map(|r| r.5).collect();
    let deltas: Vec<f64> = rows.iter().map(|r| r.6).collect();

    let mut df = df! {
        DATETIME_COL => &dates,
        "option_type" => &opt_types,
        "strike" => &strikes,
        "bid" => &bids,
        "ask" => &asks,
        "delta" => &deltas,
    }
    .unwrap();

    let exp_col =
        DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations).into_column();
    df.with_column(exp_col).unwrap();
    df
}

fn default_params() -> WheelParams {
    WheelParams {
        put_delta: TargetRange {
            target: 0.30,
            min: 0.10,
            max: 0.50,
        },
        put_dte: DteRange {
            target: 45,
            min: 20,
            max: 60,
        },
        call_delta: TargetRange {
            target: 0.30,
            min: 0.10,
            max: 0.50,
        },
        call_dte: DteRange {
            target: 30,
            min: 15,
            max: 60,
        },
        min_call_strike_at_cost: false,
        capital: 100_000.0,
        quantity: 1,
        multiplier: 100,
        slippage: Slippage::Mid,
        commission: None,
        stop_loss: None,
        min_bid_ask: 0.0,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Full wheel rotation: put expires ITM -> assignment -> sell covered call ->
/// call expires ITM -> called away.
#[test]
fn wheel_full_cycle() {
    let entry_date = d(2024, 1, 2);
    let put_exp = d(2024, 2, 16); // 45 DTE from entry
    let call_exp = d(2024, 3, 15); // 28 DTE from put_exp

    // Row layout:
    // 1. Entry day: put at strike 100, bid 3.00 / ask 3.50, delta -0.30, exp Feb 16
    // 2. Put exp day: call at strike 102, bid 2.00 / ask 2.50, delta 0.30, exp Mar 15
    //    (engine will see put expired ITM on this day, then look for call to sell)
    // 3. Call exp day: call row for closing reference
    let df = make_options_df(&[
        (dt(2024, 1, 2), put_exp, "p", 100.0, 3.00, 3.50, -0.30),
        (dt(2024, 2, 16), call_exp, "c", 102.0, 2.00, 2.50, 0.30),
        (dt(2024, 3, 15), call_exp, "c", 102.0, 0.10, 0.20, 0.02),
    ]);

    let mut closes = BTreeMap::new();
    closes.insert(entry_date, 101.0);
    closes.insert(put_exp, 98.0); // below 100 -> put ITM -> assigned
    closes.insert(call_exp, 105.0); // above 102 -> call ITM -> called away

    let trading_days = vec![entry_date, put_exp, call_exp];
    let params = default_params();
    let result = run_wheel_backtest(&df, &closes, &params, None, &trading_days).unwrap();

    // Should have 2 trades: put assignment + call called-away
    assert_eq!(result.trade_log.len(), 2, "Expected 2 trades in full cycle");
    assert_eq!(result.trade_log[0].exit_type, ExitType::Assignment);
    assert_eq!(result.trade_log[1].exit_type, ExitType::CalledAway);

    // Should have 1 completed cycle
    assert_eq!(result.cycles.len(), 1);
    let cycle = &result.cycles[0];
    assert!(cycle.assigned, "Cycle should be assigned");
    assert_eq!(cycle.calls_sold, 1);
    assert!(
        cycle.called_away_date.is_some(),
        "Should have called_away_date"
    );

    // Rolling cost basis = strike - put premium = 100 - 3.25 = 96.75
    let cost_basis = cycle.cost_basis.unwrap();
    assert!(
        (cost_basis - 96.75).abs() < 1e-10,
        "Cost basis was {cost_basis}, expected 96.75"
    );

    // Stock PnL = (call_strike - entry_price) * qty * mult = (102 - 100) * 1 * 100 = 200
    let stock_pnl = cycle.stock_pnl.unwrap();
    assert!(
        (stock_pnl - 200.0).abs() < 1e-10,
        "Stock PnL was {stock_pnl}, expected 200.0"
    );
}

/// Put expires OTM: stock stays above strike, premium collected as profit.
#[test]
fn wheel_put_expires_otm() {
    let entry_date = d(2024, 1, 2);
    let put_exp = d(2024, 2, 16);

    let df = make_options_df(&[(dt(2024, 1, 2), put_exp, "p", 100.0, 3.00, 3.50, -0.30)]);

    let mut closes = BTreeMap::new();
    closes.insert(entry_date, 105.0);
    closes.insert(put_exp, 105.0); // above strike -> OTM

    let trading_days = vec![entry_date, put_exp];
    let params = default_params();
    let result = run_wheel_backtest(&df, &closes, &params, None, &trading_days).unwrap();

    // 1 trade: the put expiring OTM
    assert_eq!(result.trade_log.len(), 1);
    assert_eq!(result.trade_log[0].exit_type, ExitType::Expiration);

    // Premium = mid(3.00, 3.50) * 1 * 100 = 325.0
    let pnl = result.trade_log[0].pnl;
    assert!((pnl - 325.0).abs() < 1e-10, "PnL was {pnl}, expected 325.0");

    // 1 cycle, put-only, not assigned
    assert_eq!(result.cycles.len(), 1);
    assert!(!result.cycles[0].assigned);
    assert_eq!(result.cycles[0].calls_sold, 0);
    assert!(
        result.cycles[0].cost_basis.is_none(),
        "OTM put should have no cost basis"
    );
}

/// Data long enough for 2+ full rotations: put ITM -> call ITM -> put ITM -> call ITM.
#[test]
fn wheel_multiple_cycles() {
    // Cycle 1: put entry Jan 2, exp Feb 16 (45 DTE), call exp Mar 15 (27 DTE)
    // Cycle 2: put entry Mar 15, exp Apr 29 (45 DTE), call exp May 27 (28 DTE)
    let d1 = d(2024, 1, 2);
    let put_exp1 = d(2024, 2, 16);
    let call_exp1 = d(2024, 3, 15);
    let put_exp2 = d(2024, 4, 29);
    let call_exp2 = d(2024, 5, 27);

    let df = make_options_df(&[
        // Cycle 1: put
        (dt(2024, 1, 2), put_exp1, "p", 100.0, 3.00, 3.50, -0.30),
        // Cycle 1: call (entered on put_exp1 day after assignment)
        (dt(2024, 2, 16), call_exp1, "c", 102.0, 2.00, 2.50, 0.30),
        // Cycle 2: put (entered on call_exp1 day after being called away)
        (dt(2024, 3, 15), put_exp2, "p", 100.0, 2.80, 3.20, -0.28),
        // Cycle 2: call
        (dt(2024, 4, 29), call_exp2, "c", 103.0, 1.80, 2.20, 0.25),
    ]);

    let mut closes = BTreeMap::new();
    closes.insert(d1, 101.0);
    closes.insert(put_exp1, 98.0); // ITM -> assigned
    closes.insert(call_exp1, 105.0); // ITM -> called away
    closes.insert(put_exp2, 97.0); // ITM -> assigned
    closes.insert(call_exp2, 106.0); // ITM -> called away

    let trading_days = vec![d1, put_exp1, call_exp1, put_exp2, call_exp2];
    let params = default_params();
    let result = run_wheel_backtest(&df, &closes, &params, None, &trading_days).unwrap();

    // At least 2 cycles
    assert!(
        result.cycles.len() >= 2,
        "Expected >= 2 cycles, got {}",
        result.cycles.len()
    );

    // Both cycles should be assigned and called away
    for (i, cycle) in result.cycles.iter().enumerate() {
        assert!(cycle.assigned, "Cycle {i} should be assigned");
        assert!(
            cycle.called_away_date.is_some(),
            "Cycle {i} should have called_away_date"
        );
    }

    // Equity curve should be continuous (one point per trading day)
    assert_eq!(result.equity_curve.len(), trading_days.len());

    // Equity should be monotonically changing (not necessarily increasing, but continuous)
    for window in result.equity_curve.windows(2) {
        assert!(
            window[1].equity.is_finite(),
            "Equity point should be finite"
        );
    }
}

/// Verify rolling cost basis = put strike - put premium per share.
#[test]
fn wheel_cost_basis_correct() {
    let entry_date = d(2024, 1, 2);
    let put_exp = d(2024, 2, 16);

    // Put strike = 100, bid = 3.00, ask = 3.50 -> mid = 3.25
    let df = make_options_df(&[(dt(2024, 1, 2), put_exp, "p", 100.0, 3.00, 3.50, -0.30)]);

    let mut closes = BTreeMap::new();
    closes.insert(entry_date, 101.0);
    closes.insert(put_exp, 95.0); // ITM -> assigned

    let trading_days = vec![entry_date, put_exp];
    let params = default_params();
    let result = run_wheel_backtest(&df, &closes, &params, None, &trading_days).unwrap();

    assert!(!result.cycles.is_empty());
    let cycle = &result.cycles[0];
    assert!(cycle.assigned);

    // Rolling cost basis = strike - put premium = 100 - 3.25 = 96.75
    let cost_basis = cycle.cost_basis.unwrap();
    assert!(
        (cost_basis - 96.75).abs() < 1e-10,
        "Cost basis was {cost_basis}, expected 96.75"
    );

    // Also verify the cycle's put_premium = 3.25 * 1 * 100 = 325.0
    assert!(
        (cycle.put_premium - 325.0).abs() < 1e-10,
        "Put premium was {}, expected 325.0",
        cycle.put_premium
    );
}

/// Signal filter: trades should only open on days in the `entry_dates` set.
#[test]
fn wheel_with_signal_filter() {
    let no_signal_day = d(2024, 1, 2);
    let signal_day = d(2024, 1, 15);
    let put_exp = d(2024, 3, 1); // 45 DTE from signal_day

    // Put candidates on both days
    let df = make_options_df(&[
        // Day without signal — should be skipped
        (dt(2024, 1, 2), put_exp, "p", 100.0, 3.00, 3.50, -0.30),
        // Day with signal — should open trade
        (dt(2024, 1, 15), put_exp, "p", 100.0, 2.80, 3.20, -0.28),
    ]);

    let mut closes = BTreeMap::new();
    closes.insert(no_signal_day, 105.0);
    closes.insert(signal_day, 105.0);
    closes.insert(put_exp, 105.0); // OTM -> keep premium

    let trading_days = vec![no_signal_day, signal_day, put_exp];

    // Only allow entry on signal_day
    let mut entry_dates = HashSet::new();
    entry_dates.insert(signal_day);

    let params = default_params();
    let result =
        run_wheel_backtest(&df, &closes, &params, Some(&entry_dates), &trading_days).unwrap();

    // Should have exactly 1 trade, entered on the signal day
    assert_eq!(
        result.trade_log.len(),
        1,
        "Expected 1 trade with signal filter"
    );

    // The trade entry should be on the signal day
    let trade = &result.trade_log[0];
    let entry_date_from_trade = trade.entry_datetime.date();
    assert_eq!(
        entry_date_from_trade, signal_day,
        "Trade should have opened on signal day {signal_day}, got {entry_date_from_trade}"
    );

    // Should have 1 cycle
    assert_eq!(result.cycles.len(), 1);
    assert_eq!(
        result.cycles[0].put_entry_date,
        signal_day.to_string(),
        "Cycle put_entry_date should match signal day"
    );
}
