//! Integration tests verifying `PnL` correctness for all 32 option strategies.
//!
//! Each test uses a synthetic `DataFrame` with 4 strikes (95/100/105/110),
//! both calls and puts, 3 quote dates (Jan 15, Jan 22, Feb 11), and a
//! single expiration (Feb 16, 2024). Prices decay over time so that a
//! DTE-based exit on Feb 11 (DTE=5) produces deterministic, hand-calculated `PnL`.

use chrono::NaiveDate;
use optopsy_mcp::engine::core::{evaluate_strategy, run_backtest};
use optopsy_mcp::engine::types::{
    BacktestParams, EvaluateParams, Slippage, TargetRange, TradeSelector,
};
use polars::prelude::*;

// ─── Constants ───────────────────────────────────────────────────────────────

const QUOTE_DATETIME_COL: &str = "quote_datetime";

// ─── Synthetic DataFrame Builder ─────────────────────────────────────────────

/// Build a rich synthetic options `DataFrame` with calls+puts at 4 strikes across 3 dates.
///
/// Calls (strikes 95/100/105/110):
///   | Strike | Jan 15 bid/ask | Jan 22 bid/ask | Feb 11 bid/ask | Delta |
///   |--------|---------------|---------------|---------------|-------|
///   | 95     | 8.00/8.50     | 7.00/7.50     | 5.00/5.50     | 0.70  |
///   | 100    | 5.00/5.50     | 4.00/4.50     | 2.00/2.50     | 0.50  |
///   | 105    | 3.00/3.50     | 2.20/2.70     | 1.00/1.50     | 0.35  |
///   | 110    | 1.50/2.00     | 1.00/1.50     | 0.30/0.80     | 0.20  |
///
/// Puts (strikes 95/100/105/110):
///   | Strike | Jan 15 bid/ask | Jan 22 bid/ask | Feb 11 bid/ask | Delta  |
///   |--------|---------------|---------------|---------------|--------|
///   | 95     | 1.00/1.50     | 0.80/1.30     | 0.20/0.70     | -0.20  |
///   | 100    | 2.50/3.00     | 2.00/2.50     | 1.00/1.50     | -0.40  |
///   | 105    | 4.50/5.00     | 3.80/4.30     | 2.50/3.00     | -0.55  |
///   | 110    | 7.00/7.50     | 6.20/6.70     | 4.50/5.00     | -0.70  |
fn make_multi_strike_df() -> DataFrame {
    let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();

    let dates = [
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), // DTE=32, entry
        NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(), // DTE=25, mid
        NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(), // DTE=5, exit
    ];

    // Call data per strike: (strike, delta, bids, asks)
    let call_data: Vec<(f64, f64, [f64; 3], [f64; 3])> = vec![
        (95.0, 0.70, [8.00, 7.00, 5.00], [8.50, 7.50, 5.50]),
        (100.0, 0.50, [5.00, 4.00, 2.00], [5.50, 4.50, 2.50]),
        (105.0, 0.35, [3.00, 2.20, 1.00], [3.50, 2.70, 1.50]),
        (110.0, 0.20, [1.50, 1.00, 0.30], [2.00, 1.50, 0.80]),
    ];

    // Put data per strike: (strike, delta, bids, asks)
    let put_data: Vec<(f64, f64, [f64; 3], [f64; 3])> = vec![
        (95.0, -0.20, [1.00, 0.80, 0.20], [1.50, 1.30, 0.70]),
        (100.0, -0.40, [2.50, 2.00, 1.00], [3.00, 2.50, 1.50]),
        (105.0, -0.55, [4.50, 3.80, 2.50], [5.00, 4.30, 3.00]),
        (110.0, -0.70, [7.00, 6.20, 4.50], [7.50, 6.70, 5.00]),
    ];

    let mut quote_dates = Vec::new();
    let mut expirations_vec = Vec::new();
    let mut option_types = Vec::new();
    let mut strikes = Vec::new();
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    let mut deltas = Vec::new();

    // Add call rows
    for (strike, delta, bid_arr, ask_arr) in &call_data {
        for (i, date) in dates.iter().enumerate() {
            quote_dates.push(date.and_hms_opt(0, 0, 0).unwrap());
            expirations_vec.push(exp);
            option_types.push("call");
            strikes.push(*strike);
            bids.push(bid_arr[i]);
            asks.push(ask_arr[i]);
            deltas.push(*delta);
        }
    }

    // Add put rows
    for (strike, delta, bid_arr, ask_arr) in &put_data {
        for (i, date) in dates.iter().enumerate() {
            quote_dates.push(date.and_hms_opt(0, 0, 0).unwrap());
            expirations_vec.push(exp);
            option_types.push("put");
            strikes.push(*strike);
            bids.push(bid_arr[i]);
            asks.push(ask_arr[i]);
            deltas.push(*delta);
        }
    }

    let mut df = df! {
        QUOTE_DATETIME_COL => &quote_dates,
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

// ─── Parameter Helpers ───────────────────────────────────────────────────────

fn delta(target: f64) -> TargetRange {
    TargetRange {
        target,
        min: 0.01,
        max: 0.99,
    }
}

fn backtest_params(strategy: &str, leg_deltas: Vec<TargetRange>) -> BacktestParams {
    BacktestParams {
        strategy: strategy.to_string(),
        leg_deltas,
        max_entry_dte: 45,
        exit_dte: 5,
        slippage: Slippage::Mid,
        commission: None,
        stop_loss: None,
        take_profit: None,
        max_hold_days: None,
        capital: 100_000.0,
        quantity: 1,
        multiplier: 100,
        max_positions: 5,
        selector: TradeSelector::First,
        adjustment_rules: vec![],
    }
}

fn evaluate_params(strategy: &str, leg_deltas: Vec<TargetRange>) -> EvaluateParams {
    EvaluateParams {
        strategy: strategy.to_string(),
        leg_deltas,
        max_entry_dte: 45,
        exit_dte: 5,
        dte_interval: 10,
        delta_interval: 0.10,
        slippage: Slippage::Mid,
        commission: None,
    }
}

/// Run a backtest and assert on trade count, `PnL`, and days held.
fn assert_backtest(strategy: &str, deltas: Vec<TargetRange>, expected_pnl: f64) {
    let df = make_multi_strike_df();
    let params = backtest_params(strategy, deltas);

    let result = run_backtest(&df, &params);
    assert!(
        result.is_ok(),
        "{strategy}: run_backtest failed: {:?}",
        result.err()
    );

    let bt = result.unwrap();
    assert_eq!(bt.trade_count, 1, "{strategy}: expected 1 trade");
    assert_eq!(bt.trade_log.len(), 1, "{strategy}: expected 1 trade log");
    assert!(
        (bt.total_pnl - expected_pnl).abs() < 1.0,
        "{strategy}: expected PnL {expected_pnl}, got {}",
        bt.total_pnl
    );
    assert_eq!(
        bt.trade_log[0].days_held, 27,
        "{strategy}: expected 27 days held (Jan 15 → Feb 11)"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// BACKTEST TESTS — Singles (6)
// All use strike 100 (call δ0.50, put δ0.40)
// Call@100: entry_mid=5.25, exit_mid=2.25
// Put@100: entry_mid=2.75, exit_mid=1.25
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn backtest_long_call() {
    // L Call@100: (2.25-5.25)×1×100 = -300
    assert_backtest("long_call", vec![delta(0.50)], -300.0);
}

#[test]
fn backtest_short_call() {
    // S Call@100: (2.25-5.25)×(-1)×100 = +300
    assert_backtest("short_call", vec![delta(0.50)], 300.0);
}

#[test]
fn backtest_long_put() {
    // L Put@100: (1.25-2.75)×1×100 = -150
    assert_backtest("long_put", vec![delta(0.40)], -150.0);
}

#[test]
fn backtest_short_put() {
    // S Put@100: (1.25-2.75)×(-1)×100 = +150
    assert_backtest("short_put", vec![delta(0.40)], 150.0);
}

#[test]
fn backtest_covered_call() {
    // S Call@100: same as short_call = +300
    assert_backtest("covered_call", vec![delta(0.50)], 300.0);
}

#[test]
fn backtest_cash_secured_put() {
    // S Put@100: same as short_put = +150
    assert_backtest("cash_secured_put", vec![delta(0.40)], 150.0);
}

// ═══════════════════════════════════════════════════════════════════════════════
// BACKTEST TESTS — Spreads (8)
// Strike ordering: strike_0 < strike_1
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn backtest_bull_call_spread() {
    // L Call@100 (δ0.50) + S Call@105 (δ0.35)
    // -300 + 200 = -100
    assert_backtest("bull_call_spread", vec![delta(0.50), delta(0.35)], -100.0);
}

#[test]
fn backtest_bear_call_spread() {
    // S Call@100 (δ0.50) + L Call@105 (δ0.35)
    // +300 + (-200) = +100
    assert_backtest("bear_call_spread", vec![delta(0.50), delta(0.35)], 100.0);
}

#[test]
fn backtest_bull_put_spread() {
    // S Put@100 (δ0.40) + L Put@105 (δ0.55)
    // +150 + (-200) = -50
    assert_backtest("bull_put_spread", vec![delta(0.40), delta(0.55)], -50.0);
}

#[test]
fn backtest_bear_put_spread() {
    // L Put@100 (δ0.40) + S Put@105 (δ0.55)
    // -150 + 200 = +50
    assert_backtest("bear_put_spread", vec![delta(0.40), delta(0.55)], 50.0);
}

#[test]
fn backtest_long_straddle() {
    // L Call@100 (δ0.50) + L Put@105 (δ0.55)
    // Call: (2.25-5.25)×1×100 = -300
    // Put:  (2.75-4.75)×1×100 = -200
    // Total: -500
    assert_backtest("long_straddle", vec![delta(0.50), delta(0.55)], -500.0);
}

#[test]
fn backtest_short_straddle() {
    // S Call@100 (δ0.50) + S Put@105 (δ0.55)
    // Call: +300, Put: +200 = +500
    assert_backtest("short_straddle", vec![delta(0.50), delta(0.55)], 500.0);
}

#[test]
fn backtest_long_strangle() {
    // L Call@105 (δ0.35) + L Put@110 (δ0.70)
    // Call: (1.25-3.25)×1×100 = -200
    // Put:  (4.75-7.25)×1×100 = -250
    // Total: -450
    assert_backtest("long_strangle", vec![delta(0.35), delta(0.70)], -450.0);
}

#[test]
fn backtest_short_strangle() {
    // S Call@105 (δ0.35) + S Put@110 (δ0.70)
    // Call: +200, Put: +250 = +450
    assert_backtest("short_strangle", vec![delta(0.35), delta(0.70)], 450.0);
}

// ═══════════════════════════════════════════════════════════════════════════════
// BACKTEST TESTS — Butterflies (4)
// 3 legs at strikes 100/105/110
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn backtest_long_call_butterfly() {
    // L Call@100, S Call@105 ×2, L Call@110
    // -300 + 400 - 120 = -20
    assert_backtest(
        "long_call_butterfly",
        vec![delta(0.50), delta(0.35), delta(0.20)],
        -20.0,
    );
}

#[test]
fn backtest_short_call_butterfly() {
    // S Call@100, L Call@105 ×2, S Call@110
    // +300 - 400 + 120 = +20
    assert_backtest(
        "short_call_butterfly",
        vec![delta(0.50), delta(0.35), delta(0.20)],
        20.0,
    );
}

#[test]
fn backtest_long_put_butterfly() {
    // L Put@100, S Put@105 ×2, L Put@110
    // -150 + 400 - 250 = 0
    assert_backtest(
        "long_put_butterfly",
        vec![delta(0.40), delta(0.55), delta(0.70)],
        0.0,
    );
}

#[test]
fn backtest_short_put_butterfly() {
    // S Put@100, L Put@105 ×2, S Put@110
    // +150 - 400 + 250 = 0
    assert_backtest(
        "short_put_butterfly",
        vec![delta(0.40), delta(0.55), delta(0.70)],
        0.0,
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// BACKTEST TESTS — Condors (4)
// 4 legs at strikes 95/100/105/110
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn backtest_long_call_condor() {
    // L Call@95, S Call@100, S Call@105, L Call@110
    // -300 + 300 + 200 - 120 = 80
    assert_backtest(
        "long_call_condor",
        vec![delta(0.70), delta(0.50), delta(0.35), delta(0.20)],
        80.0,
    );
}

#[test]
fn backtest_short_call_condor() {
    // S Call@95, L Call@100, L Call@105, S Call@110
    // +300 - 300 - 200 + 120 = -80
    assert_backtest(
        "short_call_condor",
        vec![delta(0.70), delta(0.50), delta(0.35), delta(0.20)],
        -80.0,
    );
}

#[test]
fn backtest_long_put_condor() {
    // L Put@95, S Put@100, S Put@105, L Put@110
    // -80 + 150 + 200 - 250 = 20
    assert_backtest(
        "long_put_condor",
        vec![delta(0.20), delta(0.40), delta(0.55), delta(0.70)],
        20.0,
    );
}

#[test]
fn backtest_short_put_condor() {
    // S Put@95, L Put@100, L Put@105, S Put@110
    // +80 - 150 - 200 + 250 = -20
    assert_backtest(
        "short_put_condor",
        vec![delta(0.20), delta(0.40), delta(0.55), delta(0.70)],
        -20.0,
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// BACKTEST TESTS — Iron (4)
// 4 legs: [Put, Put, Call, Call] at strikes 95/100/105/110
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn backtest_iron_condor() {
    // L Put@95 (δ0.20), S Put@100 (δ0.40), S Call@105 (δ0.35), L Call@110 (δ0.20)
    // -80 + 150 + 200 - 120 = 150
    assert_backtest(
        "iron_condor",
        vec![delta(0.20), delta(0.40), delta(0.35), delta(0.20)],
        150.0,
    );
}

#[test]
fn backtest_reverse_iron_condor() {
    // S Put@95, L Put@100, L Call@105, S Call@110
    // +80 - 150 - 200 + 120 = -150
    assert_backtest(
        "reverse_iron_condor",
        vec![delta(0.20), delta(0.40), delta(0.35), delta(0.20)],
        -150.0,
    );
}

#[test]
fn backtest_iron_butterfly() {
    // L Put@95 (δ0.20), S Put@100 (δ0.40), S Call@105 (δ0.35), L Call@110 (δ0.20)
    // Same strikes as iron_condor → same PnL: 150
    assert_backtest(
        "iron_butterfly",
        vec![delta(0.20), delta(0.40), delta(0.35), delta(0.20)],
        150.0,
    );
}

#[test]
fn backtest_reverse_iron_butterfly() {
    // S Put@95, L Put@100, L Call@105, S Call@110
    // Same as reverse_iron_condor: -150
    assert_backtest(
        "reverse_iron_butterfly",
        vec![delta(0.20), delta(0.40), delta(0.35), delta(0.20)],
        -150.0,
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// BACKTEST TESTS — Calendar / Diagonal (6)
// Since all legs join on same expiration, these behave like spreads.
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn backtest_call_calendar_spread() {
    // S Call@100 (δ0.50) + L Call@105 (δ0.35)
    // +300 + (-200) = +100
    assert_backtest(
        "call_calendar_spread",
        vec![delta(0.50), delta(0.35)],
        100.0,
    );
}

#[test]
fn backtest_put_calendar_spread() {
    // S Put@100 (δ0.40) + L Put@105 (δ0.55)
    // +150 + (-200) = -50
    assert_backtest(
        "put_calendar_spread",
        vec![delta(0.40), delta(0.55)],
        -50.0,
    );
}

#[test]
fn backtest_call_diagonal_spread() {
    // S Call@100 (δ0.50) + L Call@105 (δ0.35)
    // Same as call_calendar: +100
    assert_backtest(
        "call_diagonal_spread",
        vec![delta(0.50), delta(0.35)],
        100.0,
    );
}

#[test]
fn backtest_put_diagonal_spread() {
    // S Put@100 (δ0.40) + L Put@105 (δ0.55)
    // Same as put_calendar: -50
    assert_backtest(
        "put_diagonal_spread",
        vec![delta(0.40), delta(0.55)],
        -50.0,
    );
}

#[test]
fn backtest_double_calendar() {
    // S Call@95 (δ0.70), L Call@100 (δ0.50), S Put@105 (δ0.55), L Put@110 (δ0.70)
    // S Call@95:  (5.25-8.25)×(-1)×100 = +300
    // L Call@100: (2.25-5.25)×1×100    = -300
    // S Put@105:  (2.75-4.75)×(-1)×100 = +200
    // L Put@110:  (4.75-7.25)×1×100    = -250
    // Total: 300 - 300 + 200 - 250 = -50
    assert_backtest(
        "double_calendar",
        vec![delta(0.70), delta(0.50), delta(0.55), delta(0.70)],
        -50.0,
    );
}

#[test]
fn backtest_double_diagonal() {
    // Same structure as double_calendar: -50
    assert_backtest(
        "double_diagonal",
        vec![delta(0.70), delta(0.50), delta(0.55), delta(0.70)],
        -50.0,
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// EVALUATE TESTS — one per category
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn evaluate_singles() {
    let df = make_multi_strike_df();
    let params = evaluate_params("long_call", vec![delta(0.50)]);
    let result = evaluate_strategy(&df, &params);
    assert!(result.is_ok(), "evaluate long_call failed: {:?}", result.err());
    let stats = result.unwrap();
    assert!(!stats.is_empty(), "Expected at least one group stat for long_call");
}

#[test]
fn evaluate_spreads() {
    let df = make_multi_strike_df();
    let params = evaluate_params("bull_call_spread", vec![delta(0.50), delta(0.35)]);
    let result = evaluate_strategy(&df, &params);
    assert!(result.is_ok(), "evaluate bull_call_spread failed: {:?}", result.err());
    let stats = result.unwrap();
    assert!(!stats.is_empty(), "Expected at least one group stat for bull_call_spread");
}

#[test]
fn evaluate_butterflies() {
    let df = make_multi_strike_df();
    let params = evaluate_params(
        "long_call_butterfly",
        vec![delta(0.50), delta(0.35), delta(0.20)],
    );
    let result = evaluate_strategy(&df, &params);
    assert!(result.is_ok(), "evaluate long_call_butterfly failed: {:?}", result.err());
    let stats = result.unwrap();
    assert!(!stats.is_empty(), "Expected at least one group stat for long_call_butterfly");
}

#[test]
fn evaluate_condors() {
    let df = make_multi_strike_df();
    let params = evaluate_params(
        "long_call_condor",
        vec![delta(0.70), delta(0.50), delta(0.35), delta(0.20)],
    );
    let result = evaluate_strategy(&df, &params);
    assert!(result.is_ok(), "evaluate long_call_condor failed: {:?}", result.err());
    let stats = result.unwrap();
    assert!(!stats.is_empty(), "Expected at least one group stat for long_call_condor");
}

#[test]
fn evaluate_iron() {
    let df = make_multi_strike_df();
    let params = evaluate_params(
        "iron_condor",
        vec![delta(0.20), delta(0.40), delta(0.35), delta(0.20)],
    );
    let result = evaluate_strategy(&df, &params);
    assert!(result.is_ok(), "evaluate iron_condor failed: {:?}", result.err());
    let stats = result.unwrap();
    assert!(!stats.is_empty(), "Expected at least one group stat for iron_condor");
}

#[test]
fn evaluate_calendar() {
    let df = make_multi_strike_df();
    let params = evaluate_params("call_calendar_spread", vec![delta(0.50), delta(0.35)]);
    let result = evaluate_strategy(&df, &params);
    assert!(result.is_ok(), "evaluate call_calendar_spread failed: {:?}", result.err());
    let stats = result.unwrap();
    assert!(!stats.is_empty(), "Expected at least one group stat for call_calendar_spread");
}
