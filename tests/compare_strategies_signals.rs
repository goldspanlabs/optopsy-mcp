//! Integration tests for `compare_strategies` with entry/exit signals.
//!
//! These tests verify that signals set on `SimParams` are correctly threaded
//! through `compare_strategies()` to the underlying `run_backtest()` calls.

use chrono::NaiveDate;
use optopsy_mcp::engine::core::compare_strategies;
use optopsy_mcp::engine::types::{
    CompareEntry, CompareParams, DteRange, SimParams, Slippage, TradeSelector,
};
use optopsy_mcp::signals::registry::SignalSpec;

mod common;
use common::{make_multi_strike_df, write_ohlcv_parquet};

fn default_sim_params() -> SimParams {
    SimParams {
        capital: 100_000.0,
        quantity: 1,
        sizing: None,
        multiplier: 100,
        max_positions: 3,
        selector: TradeSelector::First,
        stop_loss: None,
        take_profit: None,
        max_hold_days: None,
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: None,
        cross_ohlcv_paths: std::collections::HashMap::new(),
        min_days_between_entries: None,
        exit_net_delta: None,
    }
}

fn default_strategies() -> Vec<CompareEntry> {
    vec![
        CompareEntry {
            name: "long_call".to_string(),
            leg_deltas: vec![common::delta(0.50)],
            entry_dte: DteRange {
                target: 45,
                min: 10,
                max: 45,
            },
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
        },
        CompareEntry {
            name: "long_put".to_string(),
            leg_deltas: vec![common::delta(0.40)],
            entry_dte: DteRange {
                target: 45,
                min: 10,
                max: 45,
            },
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
        },
    ]
}

/// Rising OHLCV — `ConsecutiveUp(2)` fires from Jan 22 onward.
///
/// With compare's `entry_dte: {min:10, max:45}`, entry candidates are Jan 15 (DTE=32)
/// and Jan 22 (DTE=25). The signal blocks Jan 15 but allows Jan 22, shifting entry
/// to a later date with different option prices → different P&L.
fn rising_ohlcv() -> (tempfile::TempDir, String) {
    let dates: Vec<NaiveDate> = vec![
        NaiveDate::from_ymd_opt(2024, 1, 11).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(),
        NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(),
    ];
    let closes = vec![100.0, 101.0, 102.0, 103.0];
    write_ohlcv_parquet(&dates, &closes)
}

/// Entry signal shifts entry date, producing different P&L.
///
/// `ConsecutiveUp(2)` blocks Jan 15, allows Jan 22. The trade enters Jan 22
/// instead of Jan 15, at a different mid price. Both baseline and signal produce
/// the same number of trades (1 per strategy), but with different P&L because
/// the entry price changes.
#[test]
fn compare_entry_signal_changes_entry_date_and_pnl() {
    let df = make_multi_strike_df();
    let (_dir, path) = rising_ohlcv();

    // Baseline: enters Jan 15 at mid 5.25 for long_call, exits Feb 11 at mid 2.25
    let baseline_params = CompareParams {
        strategies: default_strategies(),
        sim_params: default_sim_params(),
    };
    let (baseline, _) = compare_strategies(&df, &baseline_params).unwrap();
    let baseline_trades: usize = baseline.iter().map(|r| r.trades).sum();
    assert!(baseline_trades > 0, "Baseline must produce trades");

    // Signal: enters Jan 22 at mid 4.25 for long_call, exits Feb 11 at mid 2.25
    // PnL changes: baseline = (2.25 - 5.25)*100 = -300, signal = (2.25 - 4.25)*100 = -200
    let signal_params = CompareParams {
        strategies: default_strategies(),
        sim_params: SimParams {
            entry_signal: Some(SignalSpec::Formula {
                formula: "consecutive_up(close) >= 2".into(),
            }),
            exit_signal: None,
            ohlcv_path: Some(path),
            ..default_sim_params()
        },
    };
    let (results, _) = compare_strategies(&df, &signal_params).unwrap();
    let signal_trades: usize = results.iter().map(|r| r.trades).sum();

    assert!(signal_trades > 0, "Signal should allow trades on Jan 22");

    // PnL must differ — different entry date means different entry price
    let any_pnl_differs = baseline
        .iter()
        .zip(results.iter())
        .filter(|(b, s)| b.strategy == s.strategy)
        .any(|(b, s)| (b.pnl - s.pnl).abs() > 0.01);
    assert!(
        any_pnl_differs,
        "Entry signal should change PnL by shifting entry date: baseline={:?}, signal={:?}",
        baseline
            .iter()
            .map(|r| (&r.strategy, r.pnl))
            .collect::<Vec<_>>(),
        results
            .iter()
            .map(|r| (&r.strategy, r.pnl))
            .collect::<Vec<_>>(),
    );
}
