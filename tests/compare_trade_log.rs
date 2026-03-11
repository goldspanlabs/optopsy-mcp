//! Tests verifying that `compare_strategies` returns trade logs in each `CompareResult`.

use chrono::NaiveDate;
use optopsy_mcp::engine::core::compare_strategies;
use optopsy_mcp::engine::types::{
    CompareEntry, CompareParams, DteRange, ExitType, SimParams, Slippage, TradeSelector,
};

mod common;
use common::{delta, make_multi_strike_df};

fn default_sim_params() -> SimParams {
    SimParams {
        capital: 100_000.0,
        quantity: 1,
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

fn two_strategies() -> Vec<CompareEntry> {
    vec![
        CompareEntry {
            name: "long_call".to_string(),
            leg_deltas: vec![delta(0.50)],
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
            leg_deltas: vec![delta(0.40)],
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

/// Each `CompareResult` must contain a non-empty `trade_log` when trades are produced.
#[test]
fn compare_results_include_trade_logs() {
    let df = make_multi_strike_df();
    let params = CompareParams {
        strategies: two_strategies(),
        sim_params: default_sim_params(),
    };

    let (results, _) = compare_strategies(&df, &params).unwrap();
    assert_eq!(results.len(), 2, "Should have results for both strategies");

    for result in &results {
        assert!(
            result.trades > 0,
            "Strategy '{}' should produce trades",
            result.strategy
        );
        assert_eq!(
            result.trade_log.len(),
            result.trades,
            "Strategy '{}': trade_log length should match trades count",
            result.strategy
        );
    }
}

/// Trade log entries have valid structure: sequential IDs, positive `days_held`,
/// entry before exit, and non-empty legs.
#[test]
fn compare_trade_log_entries_have_valid_structure() {
    let df = make_multi_strike_df();
    let params = CompareParams {
        strategies: two_strategies(),
        sim_params: default_sim_params(),
    };

    let (results, _) = compare_strategies(&df, &params).unwrap();

    for result in &results {
        for (i, trade) in result.trade_log.iter().enumerate() {
            assert_eq!(
                trade.trade_id,
                i + 1,
                "Strategy '{}': trade_id should be sequential starting at 1",
                result.strategy
            );
            assert!(
                trade.entry_datetime < trade.exit_datetime,
                "Strategy '{}' trade {}: entry must be before exit",
                result.strategy,
                trade.trade_id
            );
            assert!(
                trade.days_held > 0,
                "Strategy '{}' trade {}: days_held must be positive",
                result.strategy,
                trade.trade_id
            );
            assert!(
                !trade.legs.is_empty(),
                "Strategy '{}' trade {}: must have at least one leg",
                result.strategy,
                trade.trade_id
            );
        }
    }
}

/// Trade log P&L is internally consistent: pnl = `exit_proceeds` - `entry_cost`.
#[test]
fn compare_trade_log_pnl_consistency() {
    let df = make_multi_strike_df();
    let params = CompareParams {
        strategies: two_strategies(),
        sim_params: default_sim_params(),
    };

    let (results, _) = compare_strategies(&df, &params).unwrap();

    for result in &results {
        let sum_pnl: f64 = result.trade_log.iter().map(|t| t.pnl).sum();
        assert!(
            (sum_pnl - result.pnl).abs() < 0.01,
            "Strategy '{}': sum of trade_log pnl ({:.2}) should match result pnl ({:.2})",
            result.strategy,
            sum_pnl,
            result.pnl
        );

        for trade in &result.trade_log {
            let expected_pnl = trade.exit_proceeds - trade.entry_cost;
            assert!(
                (trade.pnl - expected_pnl).abs() < 0.01,
                "Strategy '{}' trade {}: pnl ({:.2}) should equal exit_proceeds - entry_cost ({:.2})",
                result.strategy,
                trade.trade_id,
                trade.pnl,
                expected_pnl
            );
        }
    }
}

/// Trade logs match what `run_backtest` produces independently for the same strategy.
#[test]
fn compare_trade_log_matches_standalone_backtest() {
    let df = make_multi_strike_df();

    let params = CompareParams {
        strategies: two_strategies(),
        sim_params: default_sim_params(),
    };
    let (results, _) = compare_strategies(&df, &params).unwrap();

    // Run standalone backtest for long_call with the same params
    let standalone_params = common::backtest_params("long_call", vec![delta(0.50)]);
    let standalone = optopsy_mcp::engine::core::run_backtest(&df, &standalone_params).unwrap();

    let compare_long_call = results.iter().find(|r| r.strategy == "long_call").unwrap();

    assert_eq!(
        compare_long_call.trade_log.len(),
        standalone.trade_log.len(),
        "Compare and standalone should produce same number of trades"
    );

    for (ct, st) in compare_long_call
        .trade_log
        .iter()
        .zip(standalone.trade_log.iter())
    {
        assert_eq!(ct.trade_id, st.trade_id, "Trade IDs should match");
        assert_eq!(
            ct.entry_datetime, st.entry_datetime,
            "Entry dates should match"
        );
        assert_eq!(
            ct.exit_datetime, st.exit_datetime,
            "Exit dates should match"
        );
        assert!(
            (ct.pnl - st.pnl).abs() < 0.01,
            "PnL should match: compare={:.2}, standalone={:.2}",
            ct.pnl,
            st.pnl
        );
        assert_eq!(ct.days_held, st.days_held, "Days held should match");
        assert!(
            std::mem::discriminant(&ct.exit_type) == std::mem::discriminant(&st.exit_type),
            "Exit types should match"
        );
    }
}

/// Compare works with more than two strategies.
#[test]
fn compare_accepts_three_or_more_strategies() {
    let df = make_multi_strike_df();

    let strategies = vec![
        CompareEntry {
            name: "long_call".to_string(),
            leg_deltas: vec![delta(0.50)],
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
            leg_deltas: vec![delta(0.40)],
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
            name: "short_call".to_string(),
            leg_deltas: vec![delta(0.35)],
            entry_dte: DteRange {
                target: 45,
                min: 10,
                max: 45,
            },
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
        },
    ];

    let params = CompareParams {
        strategies,
        sim_params: default_sim_params(),
    };

    let (results, _) = compare_strategies(&df, &params).unwrap();
    assert_eq!(results.len(), 3, "Should have results for all 3 strategies");

    for result in &results {
        assert!(
            !result.trade_log.is_empty(),
            "Strategy '{}' should have trade logs",
            result.strategy
        );
        assert_eq!(
            result.trade_log.len(),
            result.trades,
            "Strategy '{}': trade_log length must match trades count",
            result.strategy
        );
    }
}

/// Trade log entries have correct entry dates from the synthetic data.
#[test]
fn compare_trade_log_entry_dates_match_synthetic_data() {
    let df = make_multi_strike_df();
    let params = CompareParams {
        strategies: two_strategies(),
        sim_params: default_sim_params(),
    };

    let (results, _) = compare_strategies(&df, &params).unwrap();

    // Synthetic data has entries on Jan 15 (DTE=32 for near-term exp Feb 16)
    let expected_entry = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
    // Exit at DTE=5 means Feb 11
    let expected_exit = NaiveDate::from_ymd_opt(2024, 2, 11).unwrap();

    for result in &results {
        for trade in &result.trade_log {
            assert_eq!(
                trade.entry_datetime.date(),
                expected_entry,
                "Strategy '{}': entry date should be Jan 15",
                result.strategy
            );
            assert_eq!(
                trade.exit_datetime.date(),
                expected_exit,
                "Strategy '{}': exit date should be Feb 11 (DTE=5)",
                result.strategy
            );
            assert!(
                matches!(trade.exit_type, ExitType::DteExit),
                "Strategy '{}': exit type should be DteExit, got {:?}",
                result.strategy,
                trade.exit_type
            );
        }
    }
}
