use std::collections::HashMap;

use chrono::NaiveDate;
use polars::prelude::*;

use optopsy_mcp::data::parquet::DATETIME_COL;
use optopsy_mcp::engine::core::{build_signal_filters, run_backtest};
use optopsy_mcp::engine::types::{
    BacktestParams, DteRange, ExitType, ExpirationFilter, Slippage, TargetRange, TradeSelector,
};
use optopsy_mcp::signals::registry::SignalSpec;

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Build a daily options `DataFrame` with intermediate dates for event-driven backtest.
/// Shows price decay from entry to exit for a long call.
fn make_daily_options_df() -> DataFrame {
    let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
    // 6 days of data: entry at Jan 15 (DTE=32), decay through to Feb 11 (DTE=5)
    let dates: Vec<_> = vec![
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), // DTE=32
        NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(), // DTE=25
        NaiveDate::from_ymd_opt(2024, 1, 29).unwrap(), // DTE=18
        NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),  // DTE=15
        NaiveDate::from_ymd_opt(2024, 2, 5).unwrap(),  // DTE=11
        NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(), // DTE=5
    ];

    let quote_dates: Vec<_> = dates
        .iter()
        .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
        .collect();
    let expirations: Vec<_> = dates.iter().map(|_| exp).collect();

    // Simulate time decay: option losing value over time
    let bids = vec![5.00, 4.50, 3.80, 3.20, 2.60, 2.00f64];
    let asks = vec![5.50, 5.00, 4.30, 3.70, 3.10, 2.50f64];
    let deltas = vec![0.50, 0.47, 0.42, 0.38, 0.33, 0.25f64];

    let n = dates.len();
    let mut df = df! {
        DATETIME_COL => &quote_dates,
        "option_type" => vec!["c"; n],
        "strike" => vec![100.0f64; n],
        "bid" => &bids,
        "ask" => &asks,
        "delta" => &deltas,
    }
    .unwrap();
    df.with_column(
        DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations).into_column(),
    )
    .unwrap();
    df
}

/// Build daily data where price drops sharply to trigger stop loss.
fn make_stop_loss_df() -> DataFrame {
    let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
    let dates: Vec<_> = vec![
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), // DTE=32, entry
        NaiveDate::from_ymd_opt(2024, 1, 16).unwrap(), // DTE=31
        NaiveDate::from_ymd_opt(2024, 1, 17).unwrap(), // DTE=30, big drop → SL
        NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(), // DTE=25
        NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(), // DTE=5
    ];

    let quote_dates: Vec<_> = dates
        .iter()
        .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
        .collect();
    let expirations: Vec<_> = dates.iter().map(|_| exp).collect();

    // Sharp drop on day 3: entry mid=5.25, day 3 mid=1.25 → loss = 400 > 50% of 525 = 262.5
    let bids = vec![5.00, 4.00, 1.00, 0.80, 0.50f64];
    let asks = vec![5.50, 4.50, 1.50, 1.30, 1.00f64];
    let deltas = vec![0.50, 0.45, 0.15, 0.12, 0.08f64];

    let n = dates.len();
    let mut df = df! {
        DATETIME_COL => &quote_dates,
        "option_type" => vec!["c"; n],
        "strike" => vec![100.0f64; n],
        "bid" => &bids,
        "ask" => &asks,
        "delta" => &deltas,
    }
    .unwrap();
    df.with_column(
        DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations).into_column(),
    )
    .unwrap();
    df
}

fn default_backtest_params() -> BacktestParams {
    BacktestParams {
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
    }
}

/// Write a minimal OHLCV parquet to a temp file for signal tests.
/// Returns a `TempDir` that keeps the file alive until dropped.
fn write_ohlcv_parquet(dates: &[NaiveDate], closes: &[f64]) -> (tempfile::TempDir, String) {
    let n = dates.len();
    let mut df = df! {
        "open" => vec![100.0f64; n],
        "high" => vec![105.0f64; n],
        "low" => vec![95.0f64; n],
        "close" => closes,
        "adjclose" => closes,
        "volume" => vec![1_000_000i64; n],
    }
    .unwrap();
    df.with_column(
        DateChunked::from_naive_date(PlSmallStr::from("date"), dates.to_vec()).into_column(),
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ohlcv.parquet");
    let file = std::fs::File::create(&path).unwrap();
    polars::prelude::ParquetWriter::new(file)
        .finish(&mut df)
        .unwrap();
    let path_str = path.to_string_lossy().to_string();
    (dir, path_str)
}

/// Write an intraday OHLCV Parquet to a temporary file.
///
/// `bars_for_date(date_index, close_ref) → Vec<(hour, minute, close)>` is called per date to
/// generate that date's bar rows. All bars share open=100, high=105, low=95.
fn write_intraday_ohlcv_parquet_raw(
    dates: &[NaiveDate],
    close_refs: &[f64],
    filename: &str,
    bars_for_date: impl Fn(usize, f64) -> Vec<(u32, u32, f64)>,
) -> (tempfile::TempDir, String) {
    let mut datetimes = Vec::new();
    let mut opens = Vec::new();
    let mut highs = Vec::new();
    let mut lows = Vec::new();
    let mut closes = Vec::new();
    let mut volumes = Vec::new();

    for (i, date) in dates.iter().enumerate() {
        for (hour, minute, close_val) in bars_for_date(i, close_refs[i]) {
            datetimes.push(date.and_hms_opt(hour, minute, 0).unwrap());
            opens.push(100.0);
            highs.push(105.0);
            lows.push(95.0);
            closes.push(close_val);
            volumes.push(1_000_000i64);
        }
    }

    let dt_series = DatetimeChunked::from_naive_datetime(
        PlSmallStr::from("datetime"),
        datetimes,
        TimeUnit::Milliseconds,
    )
    .into_column();

    let mut df = df! {
        "open" => &opens,
        "high" => &highs,
        "low" => &lows,
        "close" => &closes,
        "adjclose" => &closes,
        "volume" => &volumes,
    }
    .unwrap();
    df.with_column(dt_series).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(filename);
    let file = std::fs::File::create(&path).unwrap();
    polars::prelude::ParquetWriter::new(file)
        .finish(&mut df)
        .unwrap();
    (dir, path.to_string_lossy().to_string())
}

/// Write an intraday OHLCV parquet with a `datetime` (Datetime) column.
/// Each date gets 3 bars: 10:00, 13:00, 15:59. Only the 15:59 bar uses the
/// specified close; the others use offset values to distinguish them.
fn write_intraday_ohlcv_parquet(
    dates: &[NaiveDate],
    closes_at_1559: &[f64],
) -> (tempfile::TempDir, String) {
    write_intraday_ohlcv_parquet_raw(dates, closes_at_1559, "ohlcv_intraday.parquet", |_, c| {
        vec![
            (10, 0, c + 5.0), // morning bar — different close
            (13, 0, c - 2.0), // midday bar — different close
            (15, 59, c),      // EOD bar — the one options see
        ]
    })
}

/// Write an intraday OHLCV parquet where **no bar is at 15:59**.
/// Each date gets bars at 13:00 and 16:00 only, to exercise the fallback
/// path that selects the last bar per day when 15:59 is absent.
fn write_intraday_ohlcv_parquet_no_1559(
    dates: &[NaiveDate],
    closes_at_last_bar: &[f64],
) -> (tempfile::TempDir, String) {
    write_intraday_ohlcv_parquet_raw(
        dates,
        closes_at_last_bar,
        "ohlcv_intraday_no_1559.parquet",
        |_, c| {
            vec![
                (13, 0, c + 3.0), // earlier bar — different close
                (16, 0, c),       // latest bar — the fallback should pick this
            ]
        },
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn run_backtest_e2e_long_call() {
    let df = make_daily_options_df();
    let params = default_backtest_params();

    let result = run_backtest(&df, &params);
    assert!(result.is_ok(), "run_backtest failed: {:?}", result.err());
    let bt = result.unwrap();

    assert_eq!(bt.trade_count, 1);
    // Long call: buy at mid 5.25 on Jan 15, DTE exit triggers on Feb 11 (DTE=5)
    // Sell at mid 2.25 → loss = (2.25 - 5.25) * 100 = -300
    assert!(
        (bt.total_pnl - (-300.0)).abs() < 1.0,
        "Expected ~-300 PnL, got {}",
        bt.total_pnl
    );
    assert!(!bt.equity_curve.is_empty());
    assert_eq!(bt.trade_log.len(), 1);
    // Entry Jan 15, exit Feb 11 = 27 days
    assert_eq!(bt.trade_log[0].days_held, 27);
}

#[test]
fn run_backtest_daily_equity_curve_has_all_days() {
    let df = make_daily_options_df();
    let params = default_backtest_params();

    let result = run_backtest(&df, &params).unwrap();

    // Should have one equity point per trading day (6 days)
    assert_eq!(
        result.equity_curve.len(),
        6,
        "Expected 6 equity points (one per trading day), got {}",
        result.equity_curve.len()
    );

    // First day equity should include unrealized (entry at mid 5.25, current mid 5.25 → 0 unrealized)
    assert!(
        (result.equity_curve[0].equity - 10000.0).abs() < 1.0,
        "Day 1 equity should be ~10000, got {}",
        result.equity_curve[0].equity
    );
}

#[test]
fn run_backtest_e2e_with_stop_loss() {
    let df = make_stop_loss_df();
    let mut params = default_backtest_params();
    params.stop_loss = Some(0.50); // 50% stop loss

    let result = run_backtest(&df, &params);
    assert!(
        result.is_ok(),
        "run_backtest with stop loss failed: {:?}",
        result.err()
    );
    let bt = result.unwrap();
    assert_eq!(bt.trade_count, 1);
    // Stop loss fires on day 3 (Jan 17) at real market prices
    assert!(
        matches!(bt.trade_log[0].exit_type, ExitType::StopLoss),
        "Expected StopLoss exit, got {:?}",
        bt.trade_log[0].exit_type
    );
    // Exit on Jan 17 = 2 days held
    assert_eq!(bt.trade_log[0].days_held, 2);
}

#[test]
fn run_backtest_unknown_strategy_errors() {
    let df = make_daily_options_df();
    let mut params = default_backtest_params();
    params.strategy = "nonexistent".to_string();

    let result = run_backtest(&df, &params);
    assert!(result.is_err());
}

#[test]
fn run_backtest_wrong_leg_count_errors() {
    let df = make_daily_options_df();
    let mut params = default_backtest_params();
    params.leg_deltas = vec![]; // long_call needs 1 delta, providing 0

    let result = run_backtest(&df, &params);
    assert!(result.is_err());
}

#[test]
fn run_backtest_signal_without_ohlcv_path_errors() {
    let df = make_daily_options_df();
    let mut params = default_backtest_params();
    params.entry_signal = Some(SignalSpec::Formula {
        formula: "consecutive_up(close) >= 2".into(),
    });
    // ohlcv_path intentionally left None
    let result = run_backtest(&df, &params);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("ohlcv_path is required"),);
}

#[test]
fn run_backtest_entry_signal_filters_candidates() {
    let df = make_daily_options_df();
    // Options dates: Jan 15, 22, 29, Feb 1, 5, 11 — all are entry candidates (DTE > exit_dte=5)
    //
    // OHLCV: closes decline throughout, so ConsecutiveUp(2) never fires.
    // All entry candidates should be blocked → 0 trades.
    let ohlcv_dates: Vec<NaiveDate> = vec![
        NaiveDate::from_ymd_opt(2024, 1, 11).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 29).unwrap(),
        NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),
        NaiveDate::from_ymd_opt(2024, 2, 5).unwrap(),
        NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(),
    ];
    // Monotonically decreasing → ConsecutiveUp(2) never fires
    let closes = vec![107.0, 106.0, 105.0, 104.0, 103.0, 102.0, 101.0];
    let (_dir, path) = write_ohlcv_parquet(&ohlcv_dates, &closes);

    let mut params = default_backtest_params();
    params.entry_signal = Some(SignalSpec::Formula {
        formula: "consecutive_up(close) >= 2".into(),
    });
    params.ohlcv_path = Some(path);

    let result = run_backtest(&df, &params).unwrap();
    // All entry dates blocked since close never goes up twice in a row
    assert_eq!(result.trade_count, 0);

    // Verify baseline without signal would have produced a trade
    let mut baseline = default_backtest_params();
    baseline.entry_signal = None;
    let baseline_result = run_backtest(&df, &baseline).unwrap();
    assert!(
        baseline_result.trade_count > 0,
        "Baseline without signal should produce trades"
    );
}

#[test]
fn run_backtest_exit_signal_triggers_early_close() {
    let df = make_daily_options_df();
    // Options dates: Jan 15, 22, 29, Feb 1, 5, 11
    // Without exit signal, trade closes on Feb 11 (DTE=5 exit).
    // With exit signal on Jan 29, trade should close there instead.
    let ohlcv_dates: Vec<NaiveDate> = vec![
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 29).unwrap(), // exit signal fires here
        NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),
        NaiveDate::from_ymd_opt(2024, 2, 5).unwrap(),
        NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(),
    ];
    // ConsecutiveUp(2): fires when 2 consecutive up closes
    // Make close go up on Jan 22 and Jan 29 → signal fires on Jan 29
    let closes = vec![100.0, 101.0, 102.0, 99.0, 98.0, 97.0];
    let (_dir, path) = write_ohlcv_parquet(&ohlcv_dates, &closes);

    let mut params = default_backtest_params();
    params.max_positions = 1; // prevent re-entry after signal exit
    params.exit_signal = Some(SignalSpec::Formula {
        formula: "consecutive_up(close) >= 2".into(),
    });
    params.ohlcv_path = Some(path);

    let result = run_backtest(&df, &params).unwrap();
    // First trade: entry Jan 15, signal exit Jan 29 (ConsecutiveUp fires)
    // With max_positions=1, a second trade may open after exit.
    // Verify the first trade was closed by signal.
    assert!(
        result.trade_count >= 1,
        "Expected at least 1 trade, got {}",
        result.trade_count
    );
    assert!(
        matches!(result.trade_log[0].exit_type, ExitType::Signal),
        "Expected Signal exit on first trade, got {:?}",
        result.trade_log[0].exit_type
    );
    // Entry Jan 15, exit Jan 29 = 14 days
    assert_eq!(result.trade_log[0].days_held, 14);
}

#[test]
#[allow(clippy::too_many_lines)]
fn run_backtest_spread_strategy() {
    // Build data for a bull call spread: long call at lower strike, short call at higher
    let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
    let dates = [
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(),
        NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(),
    ];

    // Two strikes per date: 100 and 105
    let mut quote_dates = Vec::new();
    let mut expirations_vec = Vec::new();
    let mut option_types = Vec::new();
    let mut strikes = Vec::new();
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    let mut deltas = Vec::new();

    // Strike 100 data
    let bids_100 = [5.00, 4.00, 2.00f64];
    let asks_100 = [5.50, 4.50, 2.50f64];
    let deltas_100 = [0.50, 0.42, 0.25f64];

    // Strike 105 data
    let bids_105 = [3.00, 2.20, 1.00f64];
    let asks_105 = [3.50, 2.70, 1.50f64];
    let deltas_105 = [0.35, 0.28, 0.15f64];

    for (i, date) in dates.iter().enumerate() {
        // Strike 100
        quote_dates.push(date.and_hms_opt(0, 0, 0).unwrap());
        expirations_vec.push(exp);
        option_types.push("c");
        strikes.push(100.0f64);
        bids.push(bids_100[i]);
        asks.push(asks_100[i]);
        deltas.push(deltas_100[i]);

        // Strike 105
        quote_dates.push(date.and_hms_opt(0, 0, 0).unwrap());
        expirations_vec.push(exp);
        option_types.push("c");
        strikes.push(105.0f64);
        bids.push(bids_105[i]);
        asks.push(asks_105[i]);
        deltas.push(deltas_105[i]);
    }

    let mut df = df! {
        DATETIME_COL => &quote_dates,
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

    let params = BacktestParams {
        strategy: "bull_call_spread".to_string(),
        leg_deltas: vec![
            TargetRange {
                target: 0.50,
                min: 0.20,
                max: 0.80,
            },
            TargetRange {
                target: 0.35,
                min: 0.10,
                max: 0.60,
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

    let result = run_backtest(&df, &params);
    assert!(
        result.is_ok(),
        "run_backtest for spread failed: {:?}",
        result.err()
    );
    let bt = result.unwrap();
    assert_eq!(bt.trade_count, 1);
    // Both legs should be present in the trade
    assert_eq!(bt.trade_log.len(), 1);
}

#[test]
fn signal_filters_intraday_ohlcv_uses_1559_bar() {
    // Options data with entry on Jan 15 (DTE=32)
    let df = make_daily_options_df();

    let ohlcv_dates: Vec<NaiveDate> = vec![
        NaiveDate::from_ymd_opt(2024, 1, 11).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 29).unwrap(),
        NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),
        NaiveDate::from_ymd_opt(2024, 2, 5).unwrap(),
        NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(),
    ];
    // 15:59 closes: monotonically decreasing → ConsecutiveUp(2) never fires
    // But the 10:00 and 13:00 bars have different values that WOULD fire
    // if not filtered — proving the 15:59 filter is working.
    let closes_at_1559 = vec![107.0, 106.0, 105.0, 104.0, 103.0, 102.0, 101.0];
    let (_dir, path) = write_intraday_ohlcv_parquet(&ohlcv_dates, &closes_at_1559);

    let mut params = default_backtest_params();
    params.entry_signal = Some(SignalSpec::Formula {
        formula: "consecutive_up(close) >= 2".into(),
    });
    params.ohlcv_path = Some(path);

    let result = run_backtest(&df, &params).unwrap();
    // Signal should evaluate against 15:59 bars only (monotonic decrease → no fires)
    assert_eq!(
        result.trade_count, 0,
        "Intraday OHLCV should be filtered to 15:59 bars; signal should not fire"
    );
}

#[test]
fn day_of_week_signal_works_with_intraday_ohlcv() {
    let df = make_daily_options_df();
    // Options dates: Jan 15 (Mon), 22 (Mon), 29 (Mon), Feb 1 (Thu), 5 (Mon), 11 (Sun)

    let ohlcv_dates: Vec<NaiveDate> = vec![
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), // Monday
        NaiveDate::from_ymd_opt(2024, 1, 16).unwrap(), // Tuesday
        NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(), // Monday
        NaiveDate::from_ymd_opt(2024, 1, 29).unwrap(), // Monday
        NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),  // Thursday
        NaiveDate::from_ymd_opt(2024, 2, 5).unwrap(),  // Monday
        NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(), // Sunday
    ];
    let closes = vec![100.0; 7];
    let (_dir, path) = write_intraday_ohlcv_parquet(&ohlcv_dates, &closes);

    let mut params = default_backtest_params();
    // day_of_week() == 4 → Thursday only → only Feb 1 fires
    params.entry_signal = Some(SignalSpec::Formula {
        formula: "day_of_week() == 4".into(),
    });
    params.ohlcv_path = Some(path);

    // build_signal_filters should succeed (not crash on datetime column)
    let filters = build_signal_filters(&params, &df, None);
    assert!(
        filters.is_ok(),
        "day_of_week() with intraday OHLCV should not error: {:?}",
        filters.err()
    );

    let (entry_dates, _) = filters.unwrap();
    let entry_dates = entry_dates.expect("entry filter should be Some");
    // Only Thursday Feb 1 should be in the set
    assert!(
        entry_dates.contains(&NaiveDate::from_ymd_opt(2024, 2, 1).unwrap()),
        "Thursday Feb 1 should fire"
    );
    assert!(
        !entry_dates.contains(&NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()),
        "Monday Jan 15 should not fire for day_of_week() == 4"
    );
}

#[test]
fn intraday_ohlcv_fallback_to_last_bar_per_day_when_no_1559() {
    // When intraday OHLCV has no 15:59 bar, load_signal_ohlcv must fall back to the
    // last available bar per calendar date (preserving one-row-per-day semantics).
    let df = make_daily_options_df();

    let ohlcv_dates: Vec<NaiveDate> = vec![
        NaiveDate::from_ymd_opt(2024, 1, 11).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 29).unwrap(),
        NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),
        NaiveDate::from_ymd_opt(2024, 2, 5).unwrap(),
        NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(),
    ];
    // 16:00 closes (the last bar per day): monotonically increasing → ConsecutiveUp(2) fires.
    // The 13:00 bars have close = last_close + 3.0, which is also increasing but differently.
    // The key assertion is that signal evaluation fires (proving the fallback data is used
    // and that it still yields exactly one bar per day).
    let closes_at_last_bar = vec![100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0];
    let (_dir, path) = write_intraday_ohlcv_parquet_no_1559(&ohlcv_dates, &closes_at_last_bar);

    let mut params = default_backtest_params();
    params.entry_signal = Some(SignalSpec::Formula {
        formula: "consecutive_up(close) >= 2".into(),
    });
    params.ohlcv_path = Some(path);

    // build_signal_filters should succeed — the fallback produces one row per day
    let filters = build_signal_filters(&params, &df, None);
    assert!(
        filters.is_ok(),
        "build_signal_filters should succeed when intraday OHLCV has no 15:59 bars: {:?}",
        filters.err()
    );

    let (entry_dates, _) = filters.unwrap();
    let entry_dates = entry_dates.expect("entry filter should be Some");
    // Monotonically increasing closes → consecutive_up(2) fires from Jan 22 onward;
    // at least one date should be in the entry set (proving the fallback was used).
    assert!(
        !entry_dates.is_empty(),
        "Signal should fire on increasing closes from last-bar-per-day fallback"
    );
    // Verify one-row-per-day: each calendar date appears at most once (no bar duplication).
    assert!(
        entry_dates.len() <= ohlcv_dates.len(),
        "Fallback must not duplicate rows across dates"
    );
}
