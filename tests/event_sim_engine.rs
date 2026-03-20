use chrono::NaiveDate;
use optopsy_mcp::data::parquet::DATETIME_COL;
use optopsy_mcp::engine::event_sim::{find_entry_candidates, run_event_loop};
use optopsy_mcp::engine::price_table::build_date_index;
use optopsy_mcp::engine::sim_types::{
    CandidateLeg, DateIndex, EntryCandidate, PriceTable, QuoteSnapshot, SimContext,
};
use optopsy_mcp::engine::types::{
    BacktestParams, DteRange, ExitType, ExpirationFilter, OptionType, Slippage, TargetRange,
    TradeSelector,
};
use optopsy_mcp::strategies::find_strategy;
use ordered_float::OrderedFloat;
use rustc_hash::FxBuildHasher;
use std::collections::{BTreeMap, HashMap};

use polars::prelude::*;

fn make_price_table_simple() -> (PriceTable, Vec<NaiveDate>, DateIndex) {
    let mut table = PriceTable::with_hasher(FxBuildHasher);
    let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
    let d2 = NaiveDate::from_ymd_opt(2024, 1, 22).unwrap();
    let d3 = NaiveDate::from_ymd_opt(2024, 1, 29).unwrap();
    let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
    let strike = 100.0;

    table.insert(
        (d1, exp, OrderedFloat(strike), OptionType::Call),
        QuoteSnapshot {
            bid: 5.0,
            ask: 5.50,
            delta: 0.50,
        },
    );
    table.insert(
        (d2, exp, OrderedFloat(strike), OptionType::Call),
        QuoteSnapshot {
            bid: 3.0,
            ask: 3.50,
            delta: 0.35,
        },
    );
    table.insert(
        (d3, exp, OrderedFloat(strike), OptionType::Call),
        QuoteSnapshot {
            bid: 2.0,
            ask: 2.50,
            delta: 0.25,
        },
    );

    let days = vec![d1, d2, d3];
    let date_index = build_date_index(&table);
    (table, days, date_index)
}

/// Helper: build a synthetic daily options `DataFrame` for testing.
fn make_daily_df() -> DataFrame {
    let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
    let d2 = NaiveDate::from_ymd_opt(2024, 1, 22).unwrap();
    let d3 = NaiveDate::from_ymd_opt(2024, 1, 29).unwrap();
    let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();

    let quote_dates = vec![
        d1.and_hms_opt(0, 0, 0).unwrap(),
        d2.and_hms_opt(0, 0, 0).unwrap(),
        d3.and_hms_opt(0, 0, 0).unwrap(),
    ];
    let expirations = [exp, exp, exp];

    let mut df = df! {
        DATETIME_COL => &quote_dates,
        "option_type" => &["c", "c", "c"],
        "strike" => &[100.0f64, 100.0, 100.0],
        "bid" => &[5.0f64, 3.0, 2.0],
        "ask" => &[5.50f64, 3.50, 2.50],
        "delta" => &[0.50f64, 0.35, 0.25],
    }
    .unwrap();
    df.with_column(
        DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations).into_column(),
    )
    .unwrap();
    df
}

#[test]
fn run_event_loop_single_trade() {
    let (table, days, date_idx) = make_price_table_simple();
    let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
    let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();

    let mut candidates = BTreeMap::new();
    candidates.insert(
        d1,
        vec![EntryCandidate {
            entry_date: d1,
            expiration: exp,
            secondary_expiration: None,
            legs: vec![CandidateLeg {
                option_type: OptionType::Call,
                strike: 100.0,
                expiration: exp,
                bid: 5.0,
                ask: 5.50,
                delta: 0.50,
            }],
            net_premium: -5.25,
            net_delta: 0.50,
        }],
    );

    let strategy_def = find_strategy("long_call").unwrap();

    let params = BacktestParams {
        strategy: "long_call".to_string(),
        leg_deltas: vec![TargetRange {
            target: 0.50,
            min: 0.20,
            max: 0.80,
        }],
        entry_dte: DteRange {
            target: 45,
            min: 10,
            max: 60,
        },
        exit_dte: 15,
        slippage: Slippage::Mid,
        commission: None,
        min_bid_ask: 0.0,
        stop_loss: None,
        take_profit: None,
        max_hold_days: None,
        capital: 10000.0,
        quantity: 1,
        sizing: None,
        multiplier: 100,
        max_positions: 5,
        selector: TradeSelector::First,
        adjustment_rules: vec![],
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: None,
        cross_ohlcv_paths: HashMap::new(),
        min_net_premium: None,
        max_net_premium: None,
        min_net_delta: None,
        max_net_delta: None,
        min_days_between_entries: None,
        expiration_filter: ExpirationFilter::Any,
        exit_net_delta: None,
    };

    let (_trade_log, equity_curve, _) = {
        let ctx = SimContext {
            price_table: &table,
            params: &params,
            strategy_def: &strategy_def,
            ohlcv_closes: None,
        };
        run_event_loop(&ctx, &candidates, &days, None, &date_idx)
    };

    assert_eq!(equity_curve.len(), 3, "Should have 3 equity points");
}

#[test]
#[allow(clippy::too_many_lines)]
fn run_event_loop_stop_loss() {
    let mut table = PriceTable::with_hasher(FxBuildHasher);
    let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
    let d2 = NaiveDate::from_ymd_opt(2024, 1, 16).unwrap();
    let d3 = NaiveDate::from_ymd_opt(2024, 1, 17).unwrap();
    let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();

    table.insert(
        (d1, exp, OrderedFloat(100.0), OptionType::Call),
        QuoteSnapshot {
            bid: 5.0,
            ask: 5.50,
            delta: 0.50,
        },
    );
    table.insert(
        (d2, exp, OrderedFloat(100.0), OptionType::Call),
        QuoteSnapshot {
            bid: 4.0,
            ask: 4.50,
            delta: 0.45,
        },
    );
    table.insert(
        (d3, exp, OrderedFloat(100.0), OptionType::Call),
        QuoteSnapshot {
            bid: 1.0,
            ask: 1.50,
            delta: 0.15,
        },
    );

    let days = vec![d1, d2, d3];
    let date_idx = build_date_index(&table);
    let mut candidates = BTreeMap::new();
    candidates.insert(
        d1,
        vec![EntryCandidate {
            entry_date: d1,
            expiration: exp,
            secondary_expiration: None,
            legs: vec![CandidateLeg {
                option_type: OptionType::Call,
                strike: 100.0,
                expiration: exp,
                bid: 5.0,
                ask: 5.50,
                delta: 0.50,
            }],
            net_premium: -5.25,
            net_delta: 0.50,
        }],
    );

    let strategy_def = find_strategy("long_call").unwrap();

    let params = BacktestParams {
        strategy: "long_call".to_string(),
        leg_deltas: vec![TargetRange {
            target: 0.50,
            min: 0.20,
            max: 0.80,
        }],
        entry_dte: DteRange {
            target: 45,
            min: 10,
            max: 60,
        },
        exit_dte: 5,
        slippage: Slippage::Mid,
        commission: None,
        min_bid_ask: 0.0,
        stop_loss: Some(0.50),
        take_profit: None,
        max_hold_days: None,
        capital: 10000.0,
        quantity: 1,
        sizing: None,
        multiplier: 100,
        max_positions: 5,
        selector: TradeSelector::First,
        adjustment_rules: vec![],
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: None,
        cross_ohlcv_paths: HashMap::new(),
        min_net_premium: None,
        max_net_premium: None,
        min_net_delta: None,
        max_net_delta: None,
        min_days_between_entries: None,
        expiration_filter: ExpirationFilter::Any,
        exit_net_delta: None,
    };

    let (trade_log, _, _) = {
        let ctx = SimContext {
            price_table: &table,
            params: &params,
            strategy_def: &strategy_def,
            ohlcv_closes: None,
        };
        run_event_loop(&ctx, &candidates, &days, None, &date_idx)
    };

    assert_eq!(trade_log.len(), 1);
    assert!(
        matches!(trade_log[0].exit_type, ExitType::StopLoss),
        "Expected StopLoss, got {:?}",
        trade_log[0].exit_type
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn run_event_loop_take_profit() {
    let mut table = PriceTable::with_hasher(FxBuildHasher);
    let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
    let d2 = NaiveDate::from_ymd_opt(2024, 1, 16).unwrap();
    let d3 = NaiveDate::from_ymd_opt(2024, 1, 17).unwrap();
    let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();

    table.insert(
        (d1, exp, OrderedFloat(100.0), OptionType::Call),
        QuoteSnapshot {
            bid: 5.0,
            ask: 5.50,
            delta: 0.50,
        },
    );
    table.insert(
        (d2, exp, OrderedFloat(100.0), OptionType::Call),
        QuoteSnapshot {
            bid: 6.0,
            ask: 6.50,
            delta: 0.55,
        },
    );
    table.insert(
        (d3, exp, OrderedFloat(100.0), OptionType::Call),
        QuoteSnapshot {
            bid: 10.0,
            ask: 10.50,
            delta: 0.70,
        },
    );

    let days = vec![d1, d2, d3];
    let date_idx = build_date_index(&table);
    let mut candidates = BTreeMap::new();
    candidates.insert(
        d1,
        vec![EntryCandidate {
            entry_date: d1,
            expiration: exp,
            secondary_expiration: None,
            legs: vec![CandidateLeg {
                option_type: OptionType::Call,
                strike: 100.0,
                expiration: exp,
                bid: 5.0,
                ask: 5.50,
                delta: 0.50,
            }],
            net_premium: -5.25,
            net_delta: 0.50,
        }],
    );

    let strategy_def = find_strategy("long_call").unwrap();

    let params = BacktestParams {
        strategy: "long_call".to_string(),
        leg_deltas: vec![TargetRange {
            target: 0.50,
            min: 0.20,
            max: 0.80,
        }],
        entry_dte: DteRange {
            target: 45,
            min: 10,
            max: 60,
        },
        exit_dte: 5,
        slippage: Slippage::Mid,
        commission: None,
        min_bid_ask: 0.0,
        stop_loss: None,
        take_profit: Some(0.50),
        max_hold_days: None,
        capital: 10000.0,
        quantity: 1,
        sizing: None,
        multiplier: 100,
        max_positions: 5,
        selector: TradeSelector::First,
        adjustment_rules: vec![],
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: None,
        cross_ohlcv_paths: HashMap::new(),
        min_net_premium: None,
        max_net_premium: None,
        min_net_delta: None,
        max_net_delta: None,
        min_days_between_entries: None,
        expiration_filter: ExpirationFilter::Any,
        exit_net_delta: None,
    };

    let (trade_log, _, _) = {
        let ctx = SimContext {
            price_table: &table,
            params: &params,
            strategy_def: &strategy_def,
            ohlcv_closes: None,
        };
        run_event_loop(&ctx, &candidates, &days, None, &date_idx)
    };

    assert_eq!(trade_log.len(), 1);
    assert!(
        matches!(trade_log[0].exit_type, ExitType::TakeProfit),
        "Expected TakeProfit, got {:?}",
        trade_log[0].exit_type
    );
}

#[test]
fn run_event_loop_max_positions() {
    let mut table = PriceTable::with_hasher(FxBuildHasher);
    let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
    let d2 = NaiveDate::from_ymd_opt(2024, 1, 16).unwrap();
    let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();

    for d in [d1, d2] {
        table.insert(
            (d, exp, OrderedFloat(100.0), OptionType::Call),
            QuoteSnapshot {
                bid: 5.0,
                ask: 5.50,
                delta: 0.50,
            },
        );
        table.insert(
            (d, exp, OrderedFloat(105.0), OptionType::Call),
            QuoteSnapshot {
                bid: 3.0,
                ask: 3.50,
                delta: 0.40,
            },
        );
    }

    let days = vec![d1, d2];
    let date_idx = build_date_index(&table);

    let make_cand = |date: NaiveDate, strike: f64, bid: f64, ask: f64| -> EntryCandidate {
        EntryCandidate {
            entry_date: date,
            expiration: exp,
            secondary_expiration: None,
            legs: vec![CandidateLeg {
                option_type: OptionType::Call,
                strike,
                expiration: exp,
                bid,
                ask,
                delta: 0.50,
            }],
            net_premium: -(bid + ask) / 2.0,
            net_delta: 0.50,
        }
    };

    let mut candidates = BTreeMap::new();
    candidates.insert(d1, vec![make_cand(d1, 100.0, 5.0, 5.50)]);
    candidates.insert(d2, vec![make_cand(d2, 105.0, 3.0, 3.50)]);

    let strategy_def = find_strategy("long_call").unwrap();

    let params = BacktestParams {
        strategy: "long_call".to_string(),
        leg_deltas: vec![TargetRange {
            target: 0.50,
            min: 0.20,
            max: 0.80,
        }],
        entry_dte: DteRange {
            target: 45,
            min: 10,
            max: 60,
        },
        exit_dte: 5,
        slippage: Slippage::Mid,
        commission: None,
        min_bid_ask: 0.0,
        stop_loss: None,
        take_profit: None,
        max_hold_days: None,
        capital: 10000.0,
        quantity: 1,
        sizing: None,
        multiplier: 100,
        max_positions: 1,
        selector: TradeSelector::First,
        adjustment_rules: vec![],
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: None,
        cross_ohlcv_paths: HashMap::new(),
        min_net_premium: None,
        max_net_premium: None,
        min_net_delta: None,
        max_net_delta: None,
        min_days_between_entries: None,
        expiration_filter: ExpirationFilter::Any,
        exit_net_delta: None,
    };

    let (trade_log, _, _) = {
        let ctx = SimContext {
            price_table: &table,
            params: &params,
            strategy_def: &strategy_def,
            ohlcv_closes: None,
        };
        run_event_loop(&ctx, &candidates, &days, None, &date_idx)
    };

    assert_eq!(trade_log.len(), 0, "No trades should close in 2 days");
}

#[test]
fn run_event_loop_daily_equity() {
    let (table, days, date_idx) = make_price_table_simple();
    let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
    let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();

    let mut candidates = BTreeMap::new();
    candidates.insert(
        d1,
        vec![EntryCandidate {
            entry_date: d1,
            expiration: exp,
            secondary_expiration: None,
            legs: vec![CandidateLeg {
                option_type: OptionType::Call,
                strike: 100.0,
                expiration: exp,
                bid: 5.0,
                ask: 5.50,
                delta: 0.50,
            }],
            net_premium: -5.25,
            net_delta: 0.50,
        }],
    );

    let strategy_def = find_strategy("long_call").unwrap();

    let params = BacktestParams {
        strategy: "long_call".to_string(),
        leg_deltas: vec![TargetRange {
            target: 0.50,
            min: 0.20,
            max: 0.80,
        }],
        entry_dte: DteRange {
            target: 45,
            min: 10,
            max: 60,
        },
        exit_dte: 5,
        slippage: Slippage::Mid,
        commission: None,
        min_bid_ask: 0.0,
        stop_loss: None,
        take_profit: None,
        max_hold_days: None,
        capital: 10000.0,
        quantity: 1,
        sizing: None,
        multiplier: 100,
        max_positions: 5,
        selector: TradeSelector::First,
        adjustment_rules: vec![],
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: None,
        cross_ohlcv_paths: HashMap::new(),
        min_net_premium: None,
        max_net_premium: None,
        min_net_delta: None,
        max_net_delta: None,
        min_days_between_entries: None,
        expiration_filter: ExpirationFilter::Any,
        exit_net_delta: None,
    };

    let (_, equity_curve, _) = {
        let ctx = SimContext {
            price_table: &table,
            params: &params,
            strategy_def: &strategy_def,
            ohlcv_closes: None,
        };
        run_event_loop(&ctx, &candidates, &days, None, &date_idx)
    };

    assert_eq!(
        equity_curve.len(),
        days.len(),
        "One equity point per trading day"
    );

    assert!(
        (equity_curve[0].equity - 10000.0).abs() < 1e-10,
        "Day 1 equity should be 10000, got {}",
        equity_curve[0].equity
    );

    assert!(
        (equity_curve[1].equity - 9800.0).abs() < 1e-10,
        "Day 2 equity should be 9800, got {}",
        equity_curve[1].equity
    );

    assert!(
        (equity_curve[2].equity - 9700.0).abs() < 1e-10,
        "Day 3 equity should be 9700, got {}",
        equity_curve[2].equity
    );
}

#[test]
fn find_entry_candidates_single_leg() {
    let df = make_daily_df();
    let strategy_def = find_strategy("long_call").unwrap();

    let params = BacktestParams {
        strategy: "long_call".to_string(),
        leg_deltas: vec![TargetRange {
            target: 0.50,
            min: 0.10,
            max: 0.80,
        }],
        entry_dte: DteRange {
            target: 45,
            min: 10,
            max: 60,
        },
        exit_dte: 5,
        slippage: Slippage::Mid,
        commission: None,
        min_bid_ask: 0.0,
        stop_loss: None,
        take_profit: None,
        max_hold_days: None,
        capital: 10000.0,
        quantity: 1,
        sizing: None,
        multiplier: 100,
        max_positions: 5,
        selector: TradeSelector::First,
        adjustment_rules: vec![],
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: None,
        cross_ohlcv_paths: HashMap::new(),
        min_net_premium: None,
        max_net_premium: None,
        min_net_delta: None,
        max_net_delta: None,
        min_days_between_entries: None,
        expiration_filter: ExpirationFilter::Any,
        exit_net_delta: None,
    };

    let candidates = find_entry_candidates(&df, &strategy_def, &params).unwrap();
    assert!(
        !candidates.is_empty(),
        "Should find at least one date with candidates"
    );

    for cands in candidates.values() {
        assert_eq!(cands[0].legs.len(), 1);
    }
}

#[test]
fn find_entry_candidates_three_legs_no_duplicate_columns() {
    let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
    let d1 = NaiveDate::from_ymd_opt(2024, 1, 15)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();

    let mut df = df! {
        DATETIME_COL => &[d1, d1, d1],
        "option_type" => &["c", "c", "c"],
        "strike" => &[100.0f64, 105.0, 110.0],
        "bid" => &[5.0f64, 3.0, 1.5],
        "ask" => &[5.50f64, 3.50, 2.0],
        "delta" => &[0.50f64, 0.35, 0.20],
    }
    .unwrap();
    df.with_column(
        DateChunked::from_naive_date(PlSmallStr::from("expiration"), [exp, exp, exp]).into_column(),
    )
    .unwrap();

    let strategy_def = find_strategy("long_call_butterfly").unwrap();
    let params = BacktestParams {
        strategy: "long_call_butterfly".to_string(),
        leg_deltas: vec![
            TargetRange {
                target: 0.50,
                min: 0.01,
                max: 0.99,
            },
            TargetRange {
                target: 0.35,
                min: 0.01,
                max: 0.99,
            },
            TargetRange {
                target: 0.20,
                min: 0.01,
                max: 0.99,
            },
        ],
        entry_dte: DteRange {
            target: 45,
            min: 10,
            max: 60,
        },
        exit_dte: 5,
        slippage: Slippage::Mid,
        commission: None,
        min_bid_ask: 0.0,
        stop_loss: None,
        take_profit: None,
        max_hold_days: None,
        capital: 10000.0,
        quantity: 1,
        sizing: None,
        multiplier: 100,
        max_positions: 5,
        selector: TradeSelector::First,
        adjustment_rules: vec![],
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: None,
        cross_ohlcv_paths: HashMap::new(),
        min_net_premium: None,
        max_net_premium: None,
        min_net_delta: None,
        max_net_delta: None,
        min_days_between_entries: None,
        expiration_filter: ExpirationFilter::Any,
        exit_net_delta: None,
    };

    let candidates = find_entry_candidates(&df, &strategy_def, &params).unwrap();
    assert!(
        !candidates.is_empty(),
        "Should find entry candidates for 3-leg butterfly"
    );
    for cands in candidates.values() {
        assert_eq!(cands[0].legs.len(), 3, "Butterfly should have 3 legs");
    }
}

#[test]
fn find_entry_candidates_skips_rows_with_null_strike() {
    let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
    let d1 = NaiveDate::from_ymd_opt(2024, 1, 15)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();

    let null_strike: Vec<Option<f64>> = vec![None];
    let strike_col = Series::new("strike".into(), null_strike).into_column();

    let mut df = df! {
        DATETIME_COL => &[d1],
        "option_type" => &["c"],
        "bid" => &[5.0f64],
        "ask" => &[5.50f64],
        "delta" => &[0.50f64],
    }
    .unwrap();
    df.with_column(strike_col).unwrap();
    df.with_column(
        DateChunked::from_naive_date(PlSmallStr::from("expiration"), [exp]).into_column(),
    )
    .unwrap();

    let strategy_def = find_strategy("long_call").unwrap();
    let params = BacktestParams {
        strategy: "long_call".to_string(),
        leg_deltas: vec![TargetRange {
            target: 0.50,
            min: 0.10,
            max: 0.90,
        }],
        entry_dte: DteRange {
            target: 45,
            min: 10,
            max: 60,
        },
        exit_dte: 5,
        slippage: Slippage::Mid,
        commission: None,
        min_bid_ask: 0.0,
        stop_loss: None,
        take_profit: None,
        max_hold_days: None,
        capital: 10000.0,
        quantity: 1,
        sizing: None,
        multiplier: 100,
        max_positions: 5,
        selector: TradeSelector::First,
        adjustment_rules: vec![],
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: None,
        cross_ohlcv_paths: HashMap::new(),
        min_net_premium: None,
        max_net_premium: None,
        min_net_delta: None,
        max_net_delta: None,
        min_days_between_entries: None,
        expiration_filter: ExpirationFilter::Any,
        exit_net_delta: None,
    };

    let candidates = find_entry_candidates(&df, &strategy_def, &params).unwrap();
    assert!(
        candidates.is_empty(),
        "Row with null strike should be skipped; expected no candidates"
    );
}

#[test]
fn net_premium_filter_excludes_low_premium() {
    let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
    let d1 = NaiveDate::from_ymd_opt(2024, 1, 15)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();

    let mut df = df! {
        DATETIME_COL => &[d1],
        "option_type" => &["c"],
        "strike" => &[100.0f64],
        "bid" => &[0.30f64],
        "ask" => &[0.40f64],
        "delta" => &[0.50f64],
    }
    .unwrap();
    df.with_column(
        DateChunked::from_naive_date(PlSmallStr::from("expiration"), [exp]).into_column(),
    )
    .unwrap();

    let strategy_def = find_strategy("long_call").unwrap();
    let params = BacktestParams {
        strategy: "long_call".to_string(),
        leg_deltas: vec![TargetRange {
            target: 0.50,
            min: 0.10,
            max: 0.99,
        }],
        entry_dte: DteRange {
            target: 45,
            min: 10,
            max: 60,
        },
        exit_dte: 5,
        slippage: Slippage::Mid,
        commission: None,
        min_bid_ask: 0.0,
        stop_loss: None,
        take_profit: None,
        max_hold_days: None,
        capital: 10000.0,
        quantity: 1,
        sizing: None,
        multiplier: 100,
        max_positions: 5,
        selector: TradeSelector::First,
        adjustment_rules: vec![],
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: None,
        cross_ohlcv_paths: HashMap::new(),
        min_net_premium: Some(1.0),
        max_net_premium: None,
        min_net_delta: None,
        max_net_delta: None,
        min_days_between_entries: None,
        expiration_filter: ExpirationFilter::Any,
        exit_net_delta: None,
    };

    let candidates = find_entry_candidates(&df, &strategy_def, &params).unwrap();
    assert!(
        candidates.is_empty(),
        "Candidates with premium < min_net_premium should be excluded"
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn stagger_days_asserts_trade_count() {
    let mut table = PriceTable::with_hasher(FxBuildHasher);
    let exp = NaiveDate::from_ymd_opt(2024, 1, 19).unwrap();
    let days: Vec<NaiveDate> = (0..10)
        .map(|i| NaiveDate::from_ymd_opt(2024, 1, 10 + i).unwrap())
        .collect();

    for &d in &days {
        table.insert(
            (d, exp, OrderedFloat(100.0), OptionType::Call),
            QuoteSnapshot {
                bid: 5.0,
                ask: 5.50,
                delta: 0.50,
            },
        );
    }

    let date_idx = build_date_index(&table);
    let mut candidates = BTreeMap::new();
    for &d in &days {
        candidates.insert(
            d,
            vec![EntryCandidate {
                entry_date: d,
                expiration: exp,
                secondary_expiration: None,
                legs: vec![CandidateLeg {
                    option_type: OptionType::Call,
                    strike: 100.0,
                    expiration: exp,
                    bid: 5.0,
                    ask: 5.50,
                    delta: 0.50,
                }],
                net_premium: -5.25,
                net_delta: 0.50,
            }],
        );
    }

    let strategy_def = find_strategy("long_call").unwrap();

    let params_no_stagger = BacktestParams {
        strategy: "long_call".to_string(),
        leg_deltas: vec![TargetRange {
            target: 0.50,
            min: 0.20,
            max: 0.80,
        }],
        entry_dte: DteRange {
            target: 45,
            min: 10,
            max: 60,
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
        max_positions: 10,
        selector: TradeSelector::First,
        adjustment_rules: vec![],
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: None,
        cross_ohlcv_paths: HashMap::new(),
        min_net_premium: None,
        max_net_premium: None,
        min_net_delta: None,
        max_net_delta: None,
        min_days_between_entries: None,
        expiration_filter: ExpirationFilter::Any,
        exit_net_delta: None,
    };
    let (trades_no_stagger, _, _) = {
        let ctx = SimContext {
            price_table: &table,
            params: &params_no_stagger,
            strategy_def: &strategy_def,
            ohlcv_closes: None,
        };
        run_event_loop(&ctx, &candidates, &days, None, &date_idx)
    };

    let params_stagger = BacktestParams {
        min_days_between_entries: Some(3),
        ..params_no_stagger.clone()
    };
    let (trades_stagger, _, _) = {
        let ctx = SimContext {
            price_table: &table,
            params: &params_stagger,
            strategy_def: &strategy_def,
            ohlcv_closes: None,
        };
        run_event_loop(&ctx, &candidates, &days, None, &date_idx)
    };

    assert!(
        trades_no_stagger.len() > trades_stagger.len(),
        "Stagger should reduce trade count: {} without vs {} with",
        trades_no_stagger.len(),
        trades_stagger.len(),
    );
    assert!(
        trades_stagger.len() <= 4,
        "With 10 days and stagger=3, at most 4 entries: got {}",
        trades_stagger.len(),
    );
}

#[test]
fn max_net_premium_filter_excludes_expensive() {
    let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
    let d1 = NaiveDate::from_ymd_opt(2024, 1, 15)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();

    let mut df = df! {
        DATETIME_COL => &[d1],
        "option_type" => &["c"],
        "strike" => &[100.0f64],
        "bid" => &[5.0f64],
        "ask" => &[5.50f64],
        "delta" => &[0.50f64],
    }
    .unwrap();
    df.with_column(
        DateChunked::from_naive_date(PlSmallStr::from("expiration"), [exp]).into_column(),
    )
    .unwrap();

    let strategy_def = find_strategy("long_call").unwrap();
    let params = BacktestParams {
        strategy: "long_call".to_string(),
        leg_deltas: vec![TargetRange {
            target: 0.50,
            min: 0.10,
            max: 0.99,
        }],
        entry_dte: DteRange {
            target: 45,
            min: 10,
            max: 60,
        },
        exit_dte: 5,
        slippage: Slippage::Mid,
        commission: None,
        min_bid_ask: 0.0,
        stop_loss: None,
        take_profit: None,
        max_hold_days: None,
        capital: 10000.0,
        quantity: 1,
        sizing: None,
        multiplier: 100,
        max_positions: 5,
        selector: TradeSelector::First,
        adjustment_rules: vec![],
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: None,
        cross_ohlcv_paths: HashMap::new(),
        min_net_premium: None,
        max_net_premium: Some(1.0),
        min_net_delta: None,
        max_net_delta: None,
        min_days_between_entries: None,
        expiration_filter: ExpirationFilter::Any,
        exit_net_delta: None,
    };

    let candidates = find_entry_candidates(&df, &strategy_def, &params).unwrap();
    assert!(
        candidates.is_empty(),
        "Candidates with premium > max_net_premium should be excluded"
    );
}

#[test]
fn net_delta_filter_excludes_high_delta() {
    let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
    let d1 = NaiveDate::from_ymd_opt(2024, 1, 15)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();

    let mut df = df! {
        DATETIME_COL => &[d1],
        "option_type" => &["c"],
        "strike" => &[100.0f64],
        "bid" => &[5.0f64],
        "ask" => &[5.50f64],
        "delta" => &[0.70f64],
    }
    .unwrap();
    df.with_column(
        DateChunked::from_naive_date(PlSmallStr::from("expiration"), [exp]).into_column(),
    )
    .unwrap();

    let strategy_def = find_strategy("long_call").unwrap();

    let params = BacktestParams {
        strategy: "long_call".to_string(),
        leg_deltas: vec![TargetRange {
            target: 0.70,
            min: 0.10,
            max: 0.99,
        }],
        entry_dte: DteRange {
            target: 45,
            min: 10,
            max: 60,
        },
        exit_dte: 5,
        slippage: Slippage::Mid,
        commission: None,
        min_bid_ask: 0.0,
        stop_loss: None,
        take_profit: None,
        max_hold_days: None,
        capital: 10000.0,
        quantity: 1,
        sizing: None,
        multiplier: 100,
        max_positions: 5,
        selector: TradeSelector::First,
        adjustment_rules: vec![],
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: None,
        cross_ohlcv_paths: HashMap::new(),
        min_net_premium: None,
        max_net_premium: None,
        min_net_delta: None,
        max_net_delta: Some(0.50),
        min_days_between_entries: None,
        expiration_filter: ExpirationFilter::Any,
        exit_net_delta: None,
    };

    let candidates = find_entry_candidates(&df, &strategy_def, &params).unwrap();
    assert!(
        candidates.is_empty(),
        "Candidates with net_delta > max_net_delta should be excluded"
    );
}

#[test]
fn net_delta_filter_passes_within_range() {
    let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
    let d1 = NaiveDate::from_ymd_opt(2024, 1, 15)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();

    let mut df = df! {
        DATETIME_COL => &[d1],
        "option_type" => &["c"],
        "strike" => &[100.0f64],
        "bid" => &[5.0f64],
        "ask" => &[5.50f64],
        "delta" => &[0.30f64],
    }
    .unwrap();
    df.with_column(
        DateChunked::from_naive_date(PlSmallStr::from("expiration"), [exp]).into_column(),
    )
    .unwrap();

    let strategy_def = find_strategy("long_call").unwrap();
    let params = BacktestParams {
        strategy: "long_call".to_string(),
        leg_deltas: vec![TargetRange {
            target: 0.30,
            min: 0.10,
            max: 0.99,
        }],
        entry_dte: DteRange {
            target: 45,
            min: 10,
            max: 60,
        },
        exit_dte: 5,
        slippage: Slippage::Mid,
        commission: None,
        min_bid_ask: 0.0,
        stop_loss: None,
        take_profit: None,
        max_hold_days: None,
        capital: 10000.0,
        quantity: 1,
        sizing: None,
        multiplier: 100,
        max_positions: 5,
        selector: TradeSelector::First,
        adjustment_rules: vec![],
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: None,
        cross_ohlcv_paths: HashMap::new(),
        min_net_premium: None,
        max_net_premium: None,
        min_net_delta: Some(0.10),
        max_net_delta: Some(0.50),
        min_days_between_entries: None,
        expiration_filter: ExpirationFilter::Any,
        exit_net_delta: None,
    };

    let candidates = find_entry_candidates(&df, &strategy_def, &params).unwrap();
    assert!(
        !candidates.is_empty(),
        "Candidate with net_delta=0.30 should pass [0.10, 0.50] filter"
    );
}

#[test]
fn entry_candidate_net_delta_computed_correctly() {
    let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
    let d1 = NaiveDate::from_ymd_opt(2024, 1, 15)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();

    let mut df = df! {
        DATETIME_COL => &[d1],
        "option_type" => &["c"],
        "strike" => &[100.0f64],
        "bid" => &[5.0f64],
        "ask" => &[5.50f64],
        "delta" => &[0.45f64],
    }
    .unwrap();
    df.with_column(
        DateChunked::from_naive_date(PlSmallStr::from("expiration"), [exp]).into_column(),
    )
    .unwrap();

    let strategy_def = find_strategy("long_call").unwrap();
    let params = BacktestParams {
        strategy: "long_call".to_string(),
        leg_deltas: vec![TargetRange {
            target: 0.45,
            min: 0.10,
            max: 0.99,
        }],
        entry_dte: DteRange {
            target: 45,
            min: 10,
            max: 60,
        },
        exit_dte: 5,
        slippage: Slippage::Mid,
        commission: None,
        min_bid_ask: 0.0,
        stop_loss: None,
        take_profit: None,
        max_hold_days: None,
        capital: 10000.0,
        quantity: 1,
        sizing: None,
        multiplier: 100,
        max_positions: 5,
        selector: TradeSelector::First,
        adjustment_rules: vec![],
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: None,
        cross_ohlcv_paths: HashMap::new(),
        min_net_premium: None,
        max_net_premium: None,
        min_net_delta: None,
        max_net_delta: None,
        min_days_between_entries: None,
        expiration_filter: ExpirationFilter::Any,
        exit_net_delta: None,
    };

    let candidates = find_entry_candidates(&df, &strategy_def, &params).unwrap();
    assert!(!candidates.is_empty());
    let cand = &candidates.values().next().unwrap()[0];
    assert!(
        (cand.net_delta - 0.45).abs() < 1e-10,
        "Expected net_delta=0.45, got {}",
        cand.net_delta,
    );
}

#[test]
fn delta_exit_triggers_when_threshold_exceeded() {
    let mut table = PriceTable::with_hasher(FxBuildHasher);
    let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
    let days: Vec<NaiveDate> = (0..5)
        .map(|i| NaiveDate::from_ymd_opt(2024, 1, 15 + i).unwrap())
        .collect();

    for (i, &d) in days.iter().enumerate() {
        let delta = if i < 2 { 0.30 } else { 0.80 };
        table.insert(
            (d, exp, OrderedFloat(100.0), OptionType::Call),
            QuoteSnapshot {
                bid: 5.0,
                ask: 5.50,
                delta,
            },
        );
    }

    let mut candidates = BTreeMap::new();
    let date_idx = build_date_index(&table);
    candidates.insert(
        days[0],
        vec![EntryCandidate {
            entry_date: days[0],
            expiration: exp,
            secondary_expiration: None,
            legs: vec![CandidateLeg {
                option_type: OptionType::Call,
                strike: 100.0,
                expiration: exp,
                bid: 5.0,
                ask: 5.50,
                delta: 0.30,
            }],
            net_premium: -5.25,
            net_delta: 0.30,
        }],
    );

    let strategy_def = find_strategy("long_call").unwrap();
    let params = BacktestParams {
        strategy: "long_call".to_string(),
        leg_deltas: vec![TargetRange {
            target: 0.30,
            min: 0.10,
            max: 0.99,
        }],
        entry_dte: DteRange {
            target: 45,
            min: 10,
            max: 60,
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
        cross_ohlcv_paths: HashMap::new(),
        min_net_premium: None,
        max_net_premium: None,
        min_net_delta: None,
        max_net_delta: None,
        min_days_between_entries: None,
        expiration_filter: ExpirationFilter::Any,
        exit_net_delta: Some(0.50),
    };

    let (trade_log, _, _) = {
        let ctx = SimContext {
            price_table: &table,
            params: &params,
            strategy_def: &strategy_def,
            ohlcv_closes: None,
        };
        run_event_loop(&ctx, &candidates, &days, None, &date_idx)
    };

    assert_eq!(trade_log.len(), 1, "Should have exactly 1 trade");
    assert_eq!(
        trade_log[0].exit_type,
        ExitType::DeltaExit,
        "Trade should exit via DeltaExit, got {:?}",
        trade_log[0].exit_type,
    );
    assert_eq!(
        trade_log[0].days_held, 2,
        "Should exit on day 2 (delta spikes)"
    );
}

#[test]
fn delta_exit_does_not_trigger_within_threshold() {
    let mut table = PriceTable::with_hasher(FxBuildHasher);
    let exp = NaiveDate::from_ymd_opt(2024, 1, 18).unwrap();
    let days: Vec<NaiveDate> = (0..5)
        .map(|i| NaiveDate::from_ymd_opt(2024, 1, 15 + i).unwrap())
        .collect();

    for &d in &days {
        table.insert(
            (d, exp, OrderedFloat(100.0), OptionType::Call),
            QuoteSnapshot {
                bid: 5.0,
                ask: 5.50,
                delta: 0.30,
            },
        );
    }

    let mut candidates = BTreeMap::new();
    let date_idx = build_date_index(&table);
    candidates.insert(
        days[0],
        vec![EntryCandidate {
            entry_date: days[0],
            expiration: exp,
            secondary_expiration: None,
            legs: vec![CandidateLeg {
                option_type: OptionType::Call,
                strike: 100.0,
                expiration: exp,
                bid: 5.0,
                ask: 5.50,
                delta: 0.30,
            }],
            net_premium: -5.25,
            net_delta: 0.30,
        }],
    );

    let strategy_def = find_strategy("long_call").unwrap();
    let params = BacktestParams {
        strategy: "long_call".to_string(),
        leg_deltas: vec![TargetRange {
            target: 0.30,
            min: 0.10,
            max: 0.99,
        }],
        entry_dte: DteRange {
            target: 45,
            min: 1,
            max: 60,
        },
        exit_dte: 0,
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
        cross_ohlcv_paths: HashMap::new(),
        min_net_premium: None,
        max_net_premium: None,
        min_net_delta: None,
        max_net_delta: None,
        min_days_between_entries: None,
        expiration_filter: ExpirationFilter::Any,
        exit_net_delta: Some(0.50),
    };

    let (trade_log, _, _) = {
        let ctx = SimContext {
            price_table: &table,
            params: &params,
            strategy_def: &strategy_def,
            ohlcv_closes: None,
        };
        run_event_loop(&ctx, &candidates, &days, None, &date_idx)
    };

    assert!(!trade_log.is_empty(), "Should have at least 1 closed trade");
    for trade in &trade_log {
        assert_ne!(
            trade.exit_type,
            ExitType::DeltaExit,
            "Should NOT trigger DeltaExit when delta stays below threshold",
        );
    }
}
