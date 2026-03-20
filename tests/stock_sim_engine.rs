use std::collections::HashSet;

use chrono::{NaiveDate, NaiveDateTime};
use optopsy_mcp::engine::stock_sim::{
    bars_from_df, detect_date_col, filter_session, resample_ohlcv, run_stock_backtest, Bar,
    StockBacktestParams,
};
use optopsy_mcp::engine::types::{
    Commission, ConflictResolution, ExitType, Interval, SessionFilter, Side, Slippage,
};

/// Helper: create a midnight `NaiveDateTime` from y/m/d for test bars.
fn dt(y: i32, m: u32, d: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

fn make_bars() -> Vec<Bar> {
    vec![
        Bar {
            datetime: dt(2024, 1, 2),
            open: 100.0,
            high: 102.0,
            low: 99.0,
            close: 101.0,
        },
        Bar {
            datetime: dt(2024, 1, 3),
            open: 101.0,
            high: 103.0,
            low: 100.0,
            close: 102.0,
        },
        Bar {
            datetime: dt(2024, 1, 4),
            open: 102.0,
            high: 104.0,
            low: 101.0,
            close: 103.0,
        },
        Bar {
            datetime: dt(2024, 1, 5),
            open: 103.0,
            high: 105.0,
            low: 102.0,
            close: 104.0,
        },
        Bar {
            datetime: dt(2024, 1, 8),
            open: 104.0,
            high: 106.0,
            low: 103.0,
            close: 105.0,
        },
    ]
}

fn default_params() -> StockBacktestParams {
    StockBacktestParams {
        symbol: "SPY".to_string(),
        side: Side::Long,
        capital: 100_000.0,
        quantity: 100,
        sizing: None,
        max_positions: 1,
        slippage: Slippage::Mid,
        commission: None,
        stop_loss: None,
        take_profit: None,
        max_hold_days: None,
        max_hold_bars: None,
        min_days_between_entries: None,
        min_bars_between_entries: None,
        conflict_resolution: ConflictResolution::default(),
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: None,
        cross_ohlcv_paths: std::collections::HashMap::new(),
        start_date: None,
        end_date: None,
        interval: Interval::Daily,
        session_filter: None,
    }
}

#[test]
fn empty_bars_returns_zero_trades() {
    let params = default_params();
    let result = run_stock_backtest(&[], &params, None, None).unwrap();
    assert_eq!(result.trade_count, 0);
    assert!((result.total_pnl - 0.0).abs() < f64::EPSILON);
}

#[test]
fn no_entry_signal_no_trades() {
    let bars = make_bars();
    let params = default_params();
    let result = run_stock_backtest(&bars, &params, None, None).unwrap();
    assert_eq!(result.trade_count, 0);
}

#[test]
fn entry_on_first_bar_close_at_end() {
    let bars = make_bars();
    let params = default_params();
    let mut entry_dates = HashSet::new();
    entry_dates.insert(bars[0].datetime);

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
    assert_eq!(result.trade_count, 1);
    // Entered at open 100, closed at close 105 → pnl = 5 * 100 = 500
    assert!((result.total_pnl - 500.0).abs() < 1e-6);
}

#[test]
fn short_position_profits_on_decline() {
    let bars = vec![
        Bar {
            datetime: dt(2024, 1, 2),
            open: 105.0,
            high: 106.0,
            low: 104.0,
            close: 104.0,
        },
        Bar {
            datetime: dt(2024, 1, 3),
            open: 104.0,
            high: 105.0,
            low: 100.0,
            close: 100.0,
        },
    ];
    let mut params = default_params();
    params.side = Side::Short;
    let mut entry_dates = HashSet::new();
    entry_dates.insert(bars[0].datetime);

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
    assert_eq!(result.trade_count, 1);
    // Short at 105, close at 100 → pnl = (105-100) * 100 = 500
    assert!((result.total_pnl - 500.0).abs() < 1e-6);
}

#[test]
fn stop_loss_fills_at_trigger_price() {
    // Entry at open 100. SL at 5% → trigger price = 95.0
    // Bar low = 94.0 (triggers SL), close = 90.0
    // P&L should use SL price 95.0, NOT close 90.0
    let bars = vec![
        Bar {
            datetime: dt(2024, 1, 2),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
        },
        Bar {
            datetime: dt(2024, 1, 3),
            open: 100.5,
            high: 101.0,
            low: 94.0,
            close: 90.0, // Close is much lower than SL price
        },
    ];
    let mut params = default_params();
    params.stop_loss = Some(0.05);
    let mut entry_dates = HashSet::new();
    entry_dates.insert(bars[0].datetime);

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
    assert_eq!(result.trade_count, 1);
    assert_eq!(result.trade_log[0].exit_type, ExitType::StopLoss);
    // P&L = (95.0 - 100.0) * 100 = -500.0 (filled at SL price, not close of 90)
    assert!(
        (result.total_pnl - (-500.0)).abs() < 1e-6,
        "SL should fill at 95.0 not close 90.0, got pnl={}",
        result.total_pnl
    );
}

#[test]
fn take_profit_fills_at_trigger_price() {
    // Entry at open 100. TP at 10% → trigger price = 110.0
    // Bar high = 112.0 (triggers TP), close = 111.0
    // P&L should use TP price 110.0, NOT close 111.0
    let bars = vec![
        Bar {
            datetime: dt(2024, 1, 2),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
        },
        Bar {
            datetime: dt(2024, 1, 3),
            open: 101.0,
            high: 112.0,
            low: 100.0,
            close: 111.0,
        },
    ];
    let mut params = default_params();
    params.take_profit = Some(0.10);
    let mut entry_dates = HashSet::new();
    entry_dates.insert(bars[0].datetime);

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
    assert_eq!(result.trade_count, 1);
    assert_eq!(result.trade_log[0].exit_type, ExitType::TakeProfit);
    // P&L = (110.0 - 100.0) * 100 = 1000.0 (filled at TP price, not close 111)
    assert!(
        (result.total_pnl - 1000.0).abs() < 1e-6,
        "TP should fill at 110.0 not close 111.0, got pnl={}",
        result.total_pnl
    );
}

#[test]
fn max_hold_days_triggers() {
    let bars = make_bars();
    let mut params = default_params();
    params.max_hold_days = Some(2);
    let mut entry_dates = HashSet::new();
    entry_dates.insert(bars[0].datetime);

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
    assert_eq!(result.trade_count, 1);
    assert_eq!(result.trade_log[0].exit_type, ExitType::MaxHold);
}

#[test]
fn exit_signal_closes_position() {
    let bars = make_bars();
    let params = default_params();
    let mut entry_dates = HashSet::new();
    entry_dates.insert(bars[0].datetime);
    let mut exit_date_set = HashSet::new();
    exit_date_set.insert(bars[2].datetime);

    let result =
        run_stock_backtest(&bars, &params, Some(&entry_dates), Some(&exit_date_set)).unwrap();
    assert_eq!(result.trade_count, 1);
    assert_eq!(result.trade_log[0].exit_type, ExitType::Signal);
}

#[test]
fn commission_in_trade_pnl() {
    let bars = make_bars();
    let mut params = default_params();
    params.commission = Some(Commission {
        per_contract: 0.01,
        base_fee: 1.0,
        min_fee: 0.0,
    });
    let mut entry_dates = HashSet::new();
    entry_dates.insert(bars[0].datetime);

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
    // Commission per side: 1.0 + 0.01*100 = 2.0
    // Gross PnL: (105-100)*100 = 500
    // Net: 500 - 2.0 (entry) - 2.0 (exit) = 496.0
    assert!(
        (result.total_pnl - 496.0).abs() < 1e-6,
        "Both entry and exit commission should be in trade P&L, got {}",
        result.total_pnl
    );
}

#[test]
fn equity_curve_has_points_for_each_bar() {
    let bars = make_bars();
    let params = default_params();
    let mut entry_dates = HashSet::new();
    entry_dates.insert(bars[0].datetime);

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
    assert_eq!(result.equity_curve.len(), bars.len());
}

#[test]
fn max_positions_respected() {
    let bars = make_bars();
    let mut params = default_params();
    params.max_positions = 1;
    let mut entry_dates = HashSet::new();
    entry_dates.insert(bars[0].datetime);
    entry_dates.insert(bars[1].datetime);

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
    assert_eq!(result.trade_count, 1);
}

#[test]
fn short_entry_rejected_insufficient_margin() {
    let bars = make_bars();
    let mut params = default_params();
    params.side = Side::Short;
    params.capital = 5_000.0; // Not enough for 100 shares at ~100
    let mut entry_dates = HashSet::new();
    entry_dates.insert(bars[0].datetime);

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
    assert_eq!(
        result.trade_count, 0,
        "Short with insufficient margin should not open"
    );
}

#[test]
fn short_entry_accepted_sufficient_margin() {
    let bars = make_bars();
    let mut params = default_params();
    params.side = Side::Short;
    params.capital = 15_000.0; // Enough for 100 shares at ~100
    let mut entry_dates = HashSet::new();
    entry_dates.insert(bars[0].datetime);

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
    assert_eq!(
        result.trade_count, 1,
        "Short with sufficient margin should open"
    );
}

// ── Resample tests ──────────────────────────────────────────────────

fn make_daily_df() -> polars::prelude::DataFrame {
    use polars::prelude::*;
    // 10 trading days across 2 weeks: Jan 6-10 (week 2) and Jan 13-17 (week 3)
    let dates = vec![
        NaiveDate::from_ymd_opt(2025, 1, 6).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 7).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 8).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 9).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 14).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 15).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 16).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 17).unwrap(),
    ];
    let date_col =
        DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()).into_column();

    df! {
        "open" =>    &[100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0],
        "high" =>    &[102.0, 103.0, 105.0, 106.0, 107.0, 108.0, 109.0, 110.0, 111.0, 112.0],
        "low" =>     &[ 99.0, 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0],
        "close" =>   &[101.0, 102.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0, 110.0, 111.0],
        "adjclose" => &[101.0, 102.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0, 110.0, 111.0],
        "volume" =>  &[1000_i64, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900],
    }
    .unwrap()
    .hstack(&[date_col])
    .unwrap()
    .select(["date", "open", "high", "low", "close", "adjclose", "volume"])
    .unwrap()
}

#[test]
fn resample_daily_returns_same() {
    let df = make_daily_df();
    let result = resample_ohlcv(&df, Interval::Daily).unwrap();
    assert_eq!(result.height(), df.height());
}

#[test]
fn resample_weekly_groups_by_week() {
    let df = make_daily_df();
    let result = resample_ohlcv(&df, Interval::Weekly).unwrap();
    // 2 ISO weeks → 2 bars
    assert_eq!(result.height(), 2);

    let opens = result.column("open").unwrap().f64().unwrap();
    let highs = result.column("high").unwrap().f64().unwrap();
    let lows = result.column("low").unwrap().f64().unwrap();
    let closes = result.column("close").unwrap().f64().unwrap();
    let volumes = result.column("volume").unwrap().i64().unwrap();

    // Week 1: open=100 (first), high=107 (max), low=99 (min), close=106 (last)
    assert!((opens.get(0).unwrap() - 100.0).abs() < 1e-6);
    assert!((highs.get(0).unwrap() - 107.0).abs() < 1e-6);
    assert!((lows.get(0).unwrap() - 99.0).abs() < 1e-6);
    assert!((closes.get(0).unwrap() - 106.0).abs() < 1e-6);
    // Volume: 1000+1100+1200+1300+1400 = 6000
    assert_eq!(volumes.get(0).unwrap(), 6000);

    // Week 2: open=105 (first), high=112 (max), low=104 (min), close=111 (last)
    assert!((opens.get(1).unwrap() - 105.0).abs() < 1e-6);
    assert!((highs.get(1).unwrap() - 112.0).abs() < 1e-6);
    assert!((lows.get(1).unwrap() - 104.0).abs() < 1e-6);
    assert!((closes.get(1).unwrap() - 111.0).abs() < 1e-6);
    // Volume: 1500+1600+1700+1800+1900 = 8500
    assert_eq!(volumes.get(1).unwrap(), 8500);
}

#[test]
fn resample_monthly_groups_by_month() {
    let df = make_daily_df();
    let result = resample_ohlcv(&df, Interval::Monthly).unwrap();
    // All 10 days in Jan 2025 → 1 bar
    assert_eq!(result.height(), 1);

    let opens = result.column("open").unwrap().f64().unwrap();
    let closes = result.column("close").unwrap().f64().unwrap();

    assert!((opens.get(0).unwrap() - 100.0).abs() < 1e-6);
    assert!((closes.get(0).unwrap() - 111.0).abs() < 1e-6);
}

#[test]
fn resample_preserves_date_column_type() {
    let df = make_daily_df();
    let result = resample_ohlcv(&df, Interval::Weekly).unwrap();
    // bars_from_df should work on the resampled output
    let bars = bars_from_df(&result).unwrap();
    assert_eq!(bars.len(), 2);
}

#[test]
fn resample_weekly_year_boundary() {
    // Dec 30, 2024 is ISO week 1 of 2025; Jan 3, 2025 is also ISO week 1 of 2025.
    // They should group together in weekly resampling.
    use polars::prelude::*;
    let dates = vec![
        NaiveDate::from_ymd_opt(2024, 12, 30).unwrap(), // ISO week 1, 2025
        NaiveDate::from_ymd_opt(2024, 12, 31).unwrap(), // ISO week 1, 2025
        NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),   // ISO week 1, 2025
        NaiveDate::from_ymd_opt(2025, 1, 3).unwrap(),   // ISO week 1, 2025
        NaiveDate::from_ymd_opt(2025, 1, 6).unwrap(),   // ISO week 2, 2025
    ];
    let date_col = DateChunked::from_naive_date(PlSmallStr::from("date"), dates).into_column();
    let df = df! {
        "open" => &[100.0, 101.0, 102.0, 103.0, 104.0],
        "high" => &[102.0, 103.0, 104.0, 105.0, 106.0],
        "low" => &[99.0, 100.0, 101.0, 102.0, 103.0],
        "close" => &[101.0, 102.0, 103.0, 104.0, 105.0],
        "volume" => &[1000_i64, 1100, 1200, 1300, 1400],
    }
    .unwrap()
    .hstack(&[date_col])
    .unwrap()
    .select(["date", "open", "high", "low", "close", "volume"])
    .unwrap();

    let result = resample_ohlcv(&df, Interval::Weekly).unwrap();
    // Dec 30 + Dec 31 + Jan 2 + Jan 3 → ISO week 1 → 1 bar
    // Jan 6 → ISO week 2 → 1 bar
    assert_eq!(result.height(), 2);
    let volumes = result.column("volume").unwrap().i64().unwrap();
    assert_eq!(volumes.get(0).unwrap(), 1000 + 1100 + 1200 + 1300);
    assert_eq!(volumes.get(1).unwrap(), 1400);
}

#[test]
fn resample_empty_dataframe() {
    use polars::prelude::*;
    let dates: Vec<NaiveDate> = vec![];
    let date_col = DateChunked::from_naive_date(PlSmallStr::from("date"), dates).into_column();
    let df = df! {
        "open" => Vec::<f64>::new(),
        "high" => Vec::<f64>::new(),
        "low" => Vec::<f64>::new(),
        "close" => Vec::<f64>::new(),
        "volume" => Vec::<i64>::new(),
    }
    .unwrap()
    .hstack(&[date_col])
    .unwrap()
    .select(["date", "open", "high", "low", "close", "volume"])
    .unwrap();

    let result = resample_ohlcv(&df, Interval::Weekly).unwrap();
    assert_eq!(result.height(), 0);
}

#[test]
fn resample_without_adjclose() {
    use polars::prelude::*;
    let dates = vec![
        NaiveDate::from_ymd_opt(2025, 1, 6).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 7).unwrap(),
    ];
    let date_col = DateChunked::from_naive_date(PlSmallStr::from("date"), dates).into_column();
    let df = df! {
        "open" => &[100.0, 101.0],
        "high" => &[102.0, 103.0],
        "low" => &[99.0, 100.0],
        "close" => &[101.0, 102.0],
        "volume" => &[1000_i64, 1100],
    }
    .unwrap()
    .hstack(&[date_col])
    .unwrap()
    .select(["date", "open", "high", "low", "close", "volume"])
    .unwrap();

    let result = resample_ohlcv(&df, Interval::Weekly).unwrap();
    assert_eq!(result.height(), 1);
    // Should NOT have adjclose column
    assert!(result.column("adjclose").is_err());
}

#[test]
fn resample_single_row_per_group() {
    use polars::prelude::*;
    // Two dates in different weeks, one bar each
    let dates = vec![
        NaiveDate::from_ymd_opt(2025, 1, 6).unwrap(),  // Week 2
        NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(), // Week 3
    ];
    let date_col = DateChunked::from_naive_date(PlSmallStr::from("date"), dates).into_column();
    let df = df! {
        "open" => &[100.0, 110.0],
        "high" => &[102.0, 112.0],
        "low" => &[99.0, 109.0],
        "close" => &[101.0, 111.0],
        "volume" => &[1000_i64, 2000],
    }
    .unwrap()
    .hstack(&[date_col])
    .unwrap()
    .select(["date", "open", "high", "low", "close", "volume"])
    .unwrap();

    let result = resample_ohlcv(&df, Interval::Weekly).unwrap();
    assert_eq!(result.height(), 2);
    // Each group has one bar, so OHLCV should be unchanged
    let opens = result.column("open").unwrap().f64().unwrap();
    let closes = result.column("close").unwrap().f64().unwrap();
    assert!((opens.get(0).unwrap() - 100.0).abs() < 1e-6);
    assert!((closes.get(0).unwrap() - 101.0).abs() < 1e-6);
    assert!((opens.get(1).unwrap() - 110.0).abs() < 1e-6);
    assert!((closes.get(1).unwrap() - 111.0).abs() < 1e-6);
}

// --- Intraday resampling tests ---

/// Build a synthetic intraday `DataFrame` with `"datetime"` (Datetime) column.
/// 12 one-minute bars starting at 2025-01-06 09:30:00.
#[allow(clippy::let_and_return)]
fn make_intraday_df() -> polars::prelude::DataFrame {
    use polars::prelude::*;

    let base = NaiveDate::from_ymd_opt(2025, 1, 6)
        .unwrap()
        .and_hms_opt(9, 30, 0)
        .unwrap();
    let timestamps_us: Vec<i64> = (0..12)
        .map(|i| {
            let dt = base + chrono::Duration::minutes(i);
            dt.and_utc().timestamp_micros()
        })
        .collect();

    let dt_series = Series::new("datetime".into(), &timestamps_us)
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
        .unwrap();

    let df = df! {
        "open" =>    &[100.0, 101.0, 102.0, 103.0, 104.0, 105.0,
                       106.0, 107.0, 108.0, 109.0, 110.0, 111.0],
        "high" =>    &[101.0, 102.0, 103.0, 104.0, 105.0, 106.0,
                       107.0, 108.0, 109.0, 110.0, 111.0, 112.0],
        "low" =>     &[ 99.0, 100.0, 101.0, 102.0, 103.0, 104.0,
                       105.0, 106.0, 107.0, 108.0, 109.0, 110.0],
        "close" =>   &[100.5, 101.5, 102.5, 103.5, 104.5, 105.5,
                       106.5, 107.5, 108.5, 109.5, 110.5, 111.5],
        "adjclose" => &[100.5, 101.5, 102.5, 103.5, 104.5, 105.5,
                       106.5, 107.5, 108.5, 109.5, 110.5, 111.5],
        "volume" =>  &[1000_i64, 1100, 1200, 1300, 1400, 1500,
                       1600, 1700, 1800, 1900, 2000, 2100],
    }
    .unwrap()
    .hstack(&[dt_series.into()])
    .unwrap()
    .select([
        "datetime", "open", "high", "low", "close", "adjclose", "volume",
    ])
    .unwrap();

    df
}

#[test]
fn resample_intraday_min1_passthrough() {
    let df = make_intraday_df();
    let result = resample_ohlcv(&df, Interval::Min1).unwrap();
    assert_eq!(result.height(), df.height());
}

#[test]
fn resample_intraday_1m_to_5m() {
    let df = make_intraday_df();
    let result = resample_ohlcv(&df, Interval::Min5).unwrap();
    // 12 bars at 09:30..09:41 → 5-min groups: [09:30-09:34], [09:35-09:39], [09:40-09:41]
    // Group 1: min 30-34 (truncate to 30) → 5 bars
    // Group 2: min 35-39 (truncate to 35) → 5 bars
    // Group 3: min 40-41 (truncate to 40) → 2 bars
    assert_eq!(result.height(), 3);

    // Output should have "datetime" column (intraday target)
    assert!(result.column("datetime").is_ok());
    assert!(result.column("date").is_err());

    let opens = result.column("open").unwrap().f64().unwrap();
    let closes = result.column("close").unwrap().f64().unwrap();
    let highs = result.column("high").unwrap().f64().unwrap();
    let lows = result.column("low").unwrap().f64().unwrap();
    let volumes = result.column("volume").unwrap().i64().unwrap();

    // Group 1 (09:30-09:34): open=100, high=max(101..105)=105, low=min(99..103)=99, close=104.5
    assert!((opens.get(0).unwrap() - 100.0).abs() < 1e-6);
    assert!((highs.get(0).unwrap() - 105.0).abs() < 1e-6);
    assert!((lows.get(0).unwrap() - 99.0).abs() < 1e-6);
    assert!((closes.get(0).unwrap() - 104.5).abs() < 1e-6);
    // Volume: 1000+1100+1200+1300+1400 = 6000
    assert_eq!(volumes.get(0).unwrap(), 6000);

    // Group 3 (09:40-09:41): open=110, close=111.5, 2 bars
    assert!((opens.get(2).unwrap() - 110.0).abs() < 1e-6);
    assert!((closes.get(2).unwrap() - 111.5).abs() < 1e-6);
    assert_eq!(volumes.get(2).unwrap(), 2000 + 2100);
}

#[test]
fn resample_intraday_1m_to_30m() {
    let df = make_intraday_df();
    let result = resample_ohlcv(&df, Interval::Min30).unwrap();
    // 12 bars from 09:30-09:41 all fall in the 09:30 30-min bucket
    assert_eq!(result.height(), 1);

    let opens = result.column("open").unwrap().f64().unwrap();
    let closes = result.column("close").unwrap().f64().unwrap();
    assert!((opens.get(0).unwrap() - 100.0).abs() < 1e-6);
    assert!((closes.get(0).unwrap() - 111.5).abs() < 1e-6);
}

#[test]
fn resample_intraday_1m_to_hourly() {
    let df = make_intraday_df();
    let result = resample_ohlcv(&df, Interval::Hour1).unwrap();
    // All 12 bars are in the 09:xx hour → 1 group
    assert_eq!(result.height(), 1);

    let volumes = result.column("volume").unwrap().i64().unwrap();
    let expected_vol: i64 = (1000..=2100).step_by(100).sum();
    assert_eq!(volumes.get(0).unwrap(), expected_vol);
}

#[test]
fn resample_intraday_to_daily() {
    let df = make_intraday_df();
    let result = resample_ohlcv(&df, Interval::Daily).unwrap();
    // All bars on 2025-01-06 → 1 daily bar
    assert_eq!(result.height(), 1);

    // Output should have "date" column (daily target), not "datetime"
    assert!(result.column("date").is_ok());
    assert!(result.column("datetime").is_err());

    let opens = result.column("open").unwrap().f64().unwrap();
    let highs = result.column("high").unwrap().f64().unwrap();
    let lows = result.column("low").unwrap().f64().unwrap();
    let closes = result.column("close").unwrap().f64().unwrap();
    assert!((opens.get(0).unwrap() - 100.0).abs() < 1e-6);
    assert!((highs.get(0).unwrap() - 112.0).abs() < 1e-6);
    assert!((lows.get(0).unwrap() - 99.0).abs() < 1e-6);
    assert!((closes.get(0).unwrap() - 111.5).abs() < 1e-6);
}

#[test]
fn resample_intraday_to_weekly() {
    let df = make_intraday_df();
    let result = resample_ohlcv(&df, Interval::Weekly).unwrap();
    // All bars on same day → 1 weekly bar
    assert_eq!(result.height(), 1);
    assert!(result.column("date").is_ok());
}

#[test]
fn resample_daily_to_intraday_errors() {
    let df = make_daily_df();
    let result = resample_ohlcv(&df, Interval::Min5);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Cannot resample daily data to intraday"));
}

#[test]
fn resample_intraday_to_daily_feeds_bars_from_df() {
    // Verify that intraday→daily resampled output can be consumed by bars_from_df
    let df = make_intraday_df();
    let daily = resample_ohlcv(&df, Interval::Daily).unwrap();
    let bars = bars_from_df(&daily).unwrap();
    assert_eq!(bars.len(), 1);
    assert!((bars[0].open - 100.0).abs() < 1e-6);
}

// --- Real fixture tests (SPY 1-min Parquet) ---

fn load_fixture_df() -> polars::prelude::DataFrame {
    use polars::prelude::*;
    LazyFrame::scan_parquet(
        "tests/fixtures/SPY_1min_sample.parquet".into(),
        ScanArgsParquet::default(),
    )
    .expect("scan parquet")
    .collect()
    .expect("collect")
}

#[test]
fn fixture_bars_from_df_reads_intraday() {
    let df = load_fixture_df();
    let bars = bars_from_df(&df).unwrap();
    assert!(bars.len() > 1000, "expected many bars, got {}", bars.len());
    // Bars should have sub-day precision
    assert_ne!(
        bars[0].datetime.time(),
        chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap()
    );
}

#[test]
fn fixture_resample_1m_to_5m() {
    let df = load_fixture_df();
    let result = resample_ohlcv(&df, Interval::Min5).unwrap();
    // 10269 1-min bars → ~2054 5-min bars
    assert!(
        result.height() > 2000 && result.height() < 2200,
        "unexpected 5m bar count: {}",
        result.height()
    );
    assert!(result.column("datetime").is_ok());
}

#[test]
fn fixture_resample_1m_to_hourly() {
    let df = load_fixture_df();
    let result = resample_ohlcv(&df, Interval::Hour1).unwrap();
    assert!(
        result.height() > 100 && result.height() < 200,
        "unexpected hourly bar count: {}",
        result.height()
    );
    assert!(result.column("datetime").is_ok());
}

#[test]
fn fixture_resample_1m_to_daily() {
    let df = load_fixture_df();
    let result = resample_ohlcv(&df, Interval::Daily).unwrap();
    // Multi-day dataset → several daily bars
    assert!(
        result.height() >= 2,
        "expected multiple daily bars, got {}",
        result.height()
    );
    assert!(result.column("date").is_ok());

    // OHLCV invariants: high >= open, high >= close, low <= open, low <= close
    let opens = result.column("open").unwrap().f64().unwrap();
    let highs = result.column("high").unwrap().f64().unwrap();
    let lows = result.column("low").unwrap().f64().unwrap();
    let closes = result.column("close").unwrap().f64().unwrap();
    for i in 0..result.height() {
        let (o, h, l, c) = (
            opens.get(i).unwrap(),
            highs.get(i).unwrap(),
            lows.get(i).unwrap(),
            closes.get(i).unwrap(),
        );
        assert!(h >= o && h >= c, "high < open or close at row {i}");
        assert!(l <= o && l <= c, "low > open or close at row {i}");
    }
}

#[test]
fn fixture_resample_to_daily_feeds_bars_from_df() {
    let df = load_fixture_df();
    let daily = resample_ohlcv(&df, Interval::Daily).unwrap();
    let bars = bars_from_df(&daily).unwrap();
    assert!(bars.len() >= 2);
    // Daily bars should be at midnight
    assert_eq!(
        bars[0].datetime.time(),
        chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap()
    );
}

#[test]
fn fixture_resample_to_weekly() {
    let df = load_fixture_df();
    let result = resample_ohlcv(&df, Interval::Weekly).unwrap();
    assert!(result.height() >= 1);
    assert!(result.column("date").is_ok());

    // Volume should sum correctly — total should match source
    let src_vol: i64 = df
        .column("volume")
        .unwrap()
        .i64()
        .unwrap()
        .into_iter()
        .flatten()
        .sum();
    let dst_vol: i64 = result
        .column("volume")
        .unwrap()
        .i64()
        .unwrap()
        .into_iter()
        .flatten()
        .sum();
    assert_eq!(src_vol, dst_vol, "volume mismatch after weekly resample");
}

#[test]
fn fixture_session_filter_premarket() {
    let df = load_fixture_df();
    let mut bars = bars_from_df(&df).unwrap();
    let before = bars.len();
    let (start, end) = SessionFilter::Premarket.time_range();
    bars.retain(|b| {
        let t = b.datetime.time();
        t >= start && t < end
    });
    // Premarket = 04:00-09:30 — fixture starts at 04:00 so should have premarket bars
    assert!(!bars.is_empty(), "no premarket bars found");
    assert!(bars.len() < before, "filter should reduce bar count");
    // All bars within premarket window
    for b in &bars {
        let t = b.datetime.time();
        assert!(t >= start && t < end, "bar at {t} outside premarket");
    }
}

#[test]
fn filter_session_regular_hours() {
    let df = load_fixture_df();
    let before = df.height();

    let filtered = filter_session(&df, Some(&SessionFilter::RegularHours)).unwrap();

    assert!(filtered.height() > 0, "should have regular hours bars");
    assert!(filtered.height() < before, "filter should reduce row count");

    // Verify all rows are within 09:30-16:00
    let bars = bars_from_df(&filtered).unwrap();
    let start = chrono::NaiveTime::from_hms_opt(9, 30, 0).unwrap();
    let end = chrono::NaiveTime::from_hms_opt(16, 0, 0).unwrap();
    for b in &bars {
        let t = b.datetime.time();
        assert!(
            t >= start && t < end,
            "bar at {t} outside regular hours [09:30, 16:00)"
        );
    }
}

#[test]
fn filter_session_none_is_passthrough() {
    let df = load_fixture_df();
    let filtered = filter_session(&df, None).unwrap();
    assert_eq!(filtered.height(), df.height());
}

// ── AfterHours + ExtendedHours session filter tests ─────────────────

#[test]
fn filter_session_after_hours() {
    let df = load_fixture_df();
    let before = df.height();
    let filtered = filter_session(&df, Some(&SessionFilter::AfterHours)).unwrap();

    // After hours = 16:00-20:00
    if filtered.height() > 0 {
        assert!(filtered.height() < before);
        let bars = bars_from_df(&filtered).unwrap();
        let start = chrono::NaiveTime::from_hms_opt(16, 0, 0).unwrap();
        let end = chrono::NaiveTime::from_hms_opt(20, 0, 0).unwrap();
        for b in &bars {
            let t = b.datetime.time();
            assert!(
                t >= start && t < end,
                "bar at {t} outside after hours [16:00, 20:00)"
            );
        }
    }
    // If no after-hours bars in fixture, that's OK — just verify no crash
}

#[test]
fn filter_session_extended_hours() {
    let df = load_fixture_df();
    let filtered = filter_session(&df, Some(&SessionFilter::ExtendedHours)).unwrap();

    // Extended = 04:00-20:00 — should include nearly all bars in fixture
    assert!(filtered.height() > 0);
    let bars = bars_from_df(&filtered).unwrap();
    let start = chrono::NaiveTime::from_hms_opt(4, 0, 0).unwrap();
    let end = chrono::NaiveTime::from_hms_opt(20, 0, 0).unwrap();
    for b in &bars {
        let t = b.datetime.time();
        assert!(
            t >= start && t < end,
            "bar at {t} outside extended hours [04:00, 20:00)"
        );
    }
}

#[test]
fn filter_session_on_daily_data_is_noop() {
    use polars::prelude::*;
    let dates = vec![
        NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
    ];
    let date_col = DateChunked::from_naive_date(PlSmallStr::from("date"), dates).into_column();
    let df = polars::prelude::df! {
        "open" => &[100.0, 101.0],
        "high" => &[102.0, 103.0],
        "low" => &[99.0, 100.0],
        "close" => &[101.0, 102.0],
        "volume" => &[1000_i64, 1100],
    }
    .unwrap()
    .hstack(&[date_col])
    .unwrap();

    // Session filter on daily (no "datetime" column) should be a no-op
    let filtered = filter_session(&df, Some(&SessionFilter::RegularHours)).unwrap();
    assert_eq!(filtered.height(), df.height());
}

// ── detect_date_col tests ───────────────────────────────────────────

#[test]
fn detect_date_col_with_datetime() {
    use polars::prelude::*;
    let datetimes =
        vec![
            chrono::NaiveDateTime::parse_from_str("2024-01-02 09:30:00", "%Y-%m-%d %H:%M:%S")
                .unwrap(),
        ];
    let dt_chunked: DatetimeChunked =
        DatetimeChunked::new(PlSmallStr::from("datetime"), &datetimes);
    let df = DataFrame::new(
        1,
        vec![
            dt_chunked.into_series().into(),
            Series::new("close".into(), &[100.0]).into(),
        ],
    )
    .unwrap();
    assert_eq!(detect_date_col(&df), "datetime");
}

#[test]
fn detect_date_col_with_date_only() {
    use polars::prelude::*;
    let dates = vec![NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()];
    let date_col = DateChunked::from_naive_date(PlSmallStr::from("date"), dates).into_column();
    let df = DataFrame::new(
        1,
        vec![date_col, Series::new("close".into(), &[100.0]).into()],
    )
    .unwrap();
    assert_eq!(detect_date_col(&df), "date");
}

#[test]
fn detect_date_col_prefers_datetime_over_date() {
    use polars::prelude::*;
    // DataFrame with BOTH "date" and "datetime" columns
    let dates = vec![NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()];
    let date_col = DateChunked::from_naive_date(PlSmallStr::from("date"), dates).into_column();
    let datetimes =
        vec![
            chrono::NaiveDateTime::parse_from_str("2024-01-02 09:30:00", "%Y-%m-%d %H:%M:%S")
                .unwrap(),
        ];
    let dt_chunked: DatetimeChunked =
        DatetimeChunked::new(PlSmallStr::from("datetime"), &datetimes);
    let df = DataFrame::new(
        1,
        vec![
            date_col,
            dt_chunked.into_series().into(),
            Series::new("close".into(), &[100.0]).into(),
        ],
    )
    .unwrap();
    assert_eq!(detect_date_col(&df), "datetime");
}

#[test]
fn detect_date_col_string_datetime_column_falls_back_to_date() {
    use polars::prelude::*;
    // A "datetime" column that is String type, not Datetime — should fall back to "date"
    let dates = vec![NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()];
    let date_col = DateChunked::from_naive_date(PlSmallStr::from("date"), dates).into_column();
    let df = DataFrame::new(
        1,
        vec![
            date_col,
            Series::new("datetime".into(), &["2024-01-02 09:30:00"]).into(),
            Series::new("close".into(), &[100.0]).into(),
        ],
    )
    .unwrap();
    assert_eq!(detect_date_col(&df), "date");
}

// ── Session filter + resampling combo ───────────────────────────────

#[test]
fn filter_session_then_resample_to_5m() {
    let df = load_fixture_df();
    // Filter to regular hours first
    let filtered = filter_session(&df, Some(&SessionFilter::RegularHours)).unwrap();
    assert!(filtered.height() > 0);

    // Resample filtered data to 5-min
    let resampled = resample_ohlcv(&filtered, Interval::Min5).unwrap();
    assert!(resampled.height() > 0);

    // All resampled bars should be within regular hours
    let bars = bars_from_df(&resampled).unwrap();
    let start = chrono::NaiveTime::from_hms_opt(9, 30, 0).unwrap();
    let end = chrono::NaiveTime::from_hms_opt(16, 0, 0).unwrap();
    for b in &bars {
        let t = b.datetime.time();
        assert!(
            t >= start && t < end,
            "resampled bar at {t} outside regular hours"
        );
    }
}

#[test]
fn filter_session_then_resample_to_hourly() {
    use optopsy_mcp::engine::stock_sim::volume_as_i64;

    let df = load_fixture_df();
    let filtered = filter_session(&df, Some(&SessionFilter::RegularHours)).unwrap();

    let resampled = resample_ohlcv(&filtered, Interval::Hour1).unwrap();
    assert!(resampled.height() > 0);

    // Volume should be preserved: filtered sum == resampled sum
    let filtered_vol: i64 = volume_as_i64(&filtered)
        .unwrap()
        .into_iter()
        .flatten()
        .sum();
    let resampled_vol: i64 = filtered
        .column("volume")
        .ok()
        .and_then(|_| volume_as_i64(&resampled).ok())
        .map_or(0, |v| v.into_iter().flatten().sum());
    assert_eq!(
        filtered_vol, resampled_vol,
        "volume mismatch after session filter + resample"
    );
}

// ── Intraday position sizing ────────────────────────────────────────

#[test]
fn intraday_sizing_uses_correct_bars_per_year() {
    // Verify that volatility target sizing with intraday bars produces a different
    // quantity than with daily bars, due to different annualization factor.
    use optopsy_mcp::engine::sizing;

    // Constant returns: daily vs 5-min should both get None vol (constant),
    // so both fall back to fixed quantity — this verifies the plumbing doesn't crash.
    let closes = vec![100.0; 100];
    let vol_daily = sizing::compute_realized_vol(&closes, 30, Interval::Daily.bars_per_year());
    let vol_5m = sizing::compute_realized_vol(&closes, 30, Interval::Min5.bars_per_year());
    // Both should be zero/None for constant prices
    assert!(vol_daily.is_none() || vol_daily.unwrap().abs() < 1e-10);
    assert!(vol_5m.is_none() || vol_5m.unwrap().abs() < 1e-10);
}

#[test]
fn intraday_vol_target_sizing_scales_with_bars_per_year() {
    use optopsy_mcp::engine::sizing;

    // Varying prices: realized vol should differ based on annualization factor
    let closes: Vec<f64> = (0..100)
        .map(|i| 100.0 + (f64::from(i) * 0.1).sin() * 2.0)
        .collect();

    let vol_daily = sizing::compute_realized_vol(&closes, 60, Interval::Daily.bars_per_year());
    let vol_5m = sizing::compute_realized_vol(&closes, 60, Interval::Min5.bars_per_year());

    // Both should be Some and positive
    assert!(vol_daily.is_some());
    assert!(vol_5m.is_some());

    // 5-min annualized vol should be much larger than daily annualized vol
    // because bars_per_year(Min5) = 252*78 >> 252
    let vd = vol_daily.unwrap();
    let v5 = vol_5m.unwrap();
    assert!(
        v5 > vd,
        "5-min annualized vol ({v5}) should exceed daily ({vd})"
    );
}

#[test]
fn intraday_backtest_with_sizing_doesnt_crash() {
    use optopsy_mcp::engine::types::{PositionSizing, SizingConfig, SizingConstraints};

    // Build a few intraday bars
    let bars = vec![
        Bar {
            datetime: chrono::NaiveDateTime::parse_from_str(
                "2024-01-02 09:30:00",
                "%Y-%m-%d %H:%M:%S",
            )
            .unwrap(),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
        },
        Bar {
            datetime: chrono::NaiveDateTime::parse_from_str(
                "2024-01-02 09:31:00",
                "%Y-%m-%d %H:%M:%S",
            )
            .unwrap(),
            open: 100.5,
            high: 102.0,
            low: 100.0,
            close: 101.5,
        },
        Bar {
            datetime: chrono::NaiveDateTime::parse_from_str(
                "2024-01-02 09:32:00",
                "%Y-%m-%d %H:%M:%S",
            )
            .unwrap(),
            open: 101.5,
            high: 103.0,
            low: 101.0,
            close: 102.0,
        },
    ];

    let mut params = default_params();
    params.interval = Interval::Min1;
    params.stop_loss = Some(0.05);
    params.sizing = Some(SizingConfig {
        method: PositionSizing::FixedFractional { risk_pct: 0.02 },
        constraints: SizingConstraints {
            min_quantity: 1,
            max_quantity: Some(1000),
        },
    });

    let mut entry_dates = HashSet::new();
    entry_dates.insert(bars[0].datetime);

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
    assert_eq!(result.trade_count, 1);
    // Should have used dynamic sizing
    let trade = &result.trade_log[0];
    assert!(trade.computed_quantity.is_some());
}

// ── Intraday helpers ──────────────────────────────────────────────────

/// Build intraday bars at 5-minute intervals within a single day.
fn make_intraday_bars(count: usize) -> Vec<Bar> {
    let base = NaiveDate::from_ymd_opt(2024, 3, 15)
        .unwrap()
        .and_hms_opt(9, 30, 0)
        .unwrap();
    (0..count)
        .map(|i| {
            let dt = base + chrono::Duration::minutes(5 * i as i64);
            let price = 100.0 + i as f64;
            Bar {
                datetime: dt,
                open: price,
                high: price + 1.0,
                low: price - 0.5,
                close: price + 0.5,
            }
        })
        .collect()
}

fn intraday_params() -> StockBacktestParams {
    StockBacktestParams {
        interval: Interval::Min5,
        ..default_params()
    }
}

// ── Fix 1: min_bars_between_entries ────────────────────────────────

#[test]
fn intraday_min_bars_between_entries_enforces_cooldown() {
    let bars = make_intraday_bars(10);
    let mut params = intraday_params();
    params.min_bars_between_entries = Some(3);
    params.max_hold_bars = Some(1); // exit after 1 bar so position frees up

    // Signal fires on every bar
    let entry_dates: HashSet<NaiveDateTime> = bars.iter().map(|b| b.datetime).collect();

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
    // Entry at bar 0, exit at bar 1. Cooldown=3, so next entry at bar 3, exit at bar 4.
    // Next at bar 6, exit at bar 7. Next at bar 9, force-close at end.
    // Without cooldown we'd get ~10 trades; with cooldown=3 we get exactly 4.
    assert_eq!(
        result.trade_count, 4,
        "With 10 bars, hold=1, cooldown=3: expected 4 trades; got {}",
        result.trade_count
    );
    // Verify entry bar spacing: bars 0, 3, 6, 9
    let entry_dts: Vec<_> = result.trade_log.iter().map(|t| t.entry_datetime).collect();
    assert_eq!(entry_dts[0], bars[0].datetime);
    assert_eq!(entry_dts[1], bars[3].datetime);
    assert_eq!(entry_dts[2], bars[6].datetime);
    assert_eq!(entry_dts[3], bars[9].datetime);
}

#[test]
fn intraday_min_bars_cooldown_allows_reentry_after_gap() {
    let bars = make_intraday_bars(10);
    let mut params = intraday_params();
    params.min_bars_between_entries = Some(3);
    params.max_hold_bars = Some(2); // exit after 2 bars

    let entry_dates: HashSet<NaiveDateTime> = bars.iter().map(|b| b.datetime).collect();

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
    // Entry at bar 0, exit at bar 2. Cooldown=3, next entry at bar 3, exit at bar 5.
    // Next at bar 6, exit at bar 8. Next at bar 9, force-close at end. = 4 trades.
    assert_eq!(
        result.trade_count, 4,
        "With 10 bars, hold=2, cooldown=3: expected 4 trades; got {}",
        result.trade_count
    );
    let entry_dts: Vec<_> = result.trade_log.iter().map(|t| t.entry_datetime).collect();
    assert_eq!(entry_dts[0], bars[0].datetime);
    assert_eq!(entry_dts[1], bars[3].datetime);
    assert_eq!(entry_dts[2], bars[6].datetime);
    assert_eq!(entry_dts[3], bars[9].datetime);
}

// ── Fix 2: max_hold_bars ──────────────────────────────────────────

#[test]
fn intraday_max_hold_bars_closes_after_n_bars() {
    let bars = make_intraday_bars(10);
    let mut params = intraday_params();
    params.max_hold_bars = Some(3);
    params.max_positions = 1;

    let mut entry_dates = HashSet::new();
    entry_dates.insert(bars[0].datetime);

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
    assert_eq!(result.trade_count, 1);
    assert_eq!(result.trade_log[0].exit_type, ExitType::MaxHold);
    // Entry at bar 0 (09:30), exit at bar 3 (09:45) — held exactly 3 bars
    assert_eq!(result.trade_log[0].entry_datetime, bars[0].datetime);
    assert_eq!(result.trade_log[0].exit_datetime, bars[3].datetime);
}

#[test]
fn daily_max_hold_days_still_works() {
    let bars = make_bars();
    let mut params = default_params();
    params.max_hold_days = Some(2);

    let mut entry_dates = HashSet::new();
    entry_dates.insert(bars[0].datetime);

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
    assert_eq!(result.trade_count, 1);
    assert_eq!(result.trade_log[0].exit_type, ExitType::MaxHold);
}

// ── Fix 3: same-bar entry+exit guard ──────────────────────────────

#[test]
fn same_bar_entry_exit_signal_prevents_trade() {
    let bars = make_bars();
    let params = default_params();

    let mut entry_dates = HashSet::new();
    entry_dates.insert(bars[0].datetime);
    let mut exit_dates = HashSet::new();
    exit_dates.insert(bars[0].datetime); // same bar

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), Some(&exit_dates)).unwrap();
    assert_eq!(
        result.trade_count, 0,
        "Should not open a trade when exit signal fires on same bar"
    );
}

// ── Fix 4: ConflictResolution ─────────────────────────────────────

#[test]
fn conflict_resolution_stop_loss_first() {
    // Bar where both SL and TP trigger: wide range bar
    let bars = vec![
        Bar {
            datetime: dt(2024, 1, 2),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
        },
        Bar {
            datetime: dt(2024, 1, 3),
            open: 100.0,
            high: 115.0, // hits TP at 110
            low: 85.0,   // hits SL at 90
            close: 100.0,
        },
    ];
    let mut params = default_params();
    params.stop_loss = Some(0.10);
    params.take_profit = Some(0.10);
    params.conflict_resolution = ConflictResolution::StopLossFirst;

    let mut entry_dates = HashSet::new();
    entry_dates.insert(bars[0].datetime);

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
    assert_eq!(result.trade_log[0].exit_type, ExitType::StopLoss);
}

#[test]
fn conflict_resolution_take_profit_first() {
    let bars = vec![
        Bar {
            datetime: dt(2024, 1, 2),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
        },
        Bar {
            datetime: dt(2024, 1, 3),
            open: 100.0,
            high: 115.0,
            low: 85.0,
            close: 100.0,
        },
    ];
    let mut params = default_params();
    params.stop_loss = Some(0.10);
    params.take_profit = Some(0.10);
    params.conflict_resolution = ConflictResolution::TakeProfitFirst;

    let mut entry_dates = HashSet::new();
    entry_dates.insert(bars[0].datetime);

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
    assert_eq!(result.trade_log[0].exit_type, ExitType::TakeProfit);
}

#[test]
fn conflict_resolution_nearest_picks_closer_to_open() {
    // SL at 90 (distance 10 from open 100), TP at 105 (distance 5 from open)
    let bars = vec![
        Bar {
            datetime: dt(2024, 1, 2),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
        },
        Bar {
            datetime: dt(2024, 1, 3),
            open: 100.0,
            high: 115.0,
            low: 85.0,
            close: 100.0,
        },
    ];
    let mut params = default_params();
    params.stop_loss = Some(0.10); // SL at 90, dist=10
    params.take_profit = Some(0.05); // TP at 105, dist=5
    params.conflict_resolution = ConflictResolution::Nearest;

    let mut entry_dates = HashSet::new();
    entry_dates.insert(bars[0].datetime);

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
    // TP is closer to open (5 vs 10), so TP wins
    assert_eq!(result.trade_log[0].exit_type, ExitType::TakeProfit);
}

// ── Fix 5: gap-through fill ───────────────────────────────────────

#[test]
fn gap_through_stop_loss_fills_at_open() {
    // Long position, stop at 95 (5%), but next bar opens at 92 (gap through)
    let bars = vec![
        Bar {
            datetime: dt(2024, 1, 2),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
        },
        Bar {
            datetime: dt(2024, 1, 3),
            open: 92.0, // gap through SL at 95
            high: 94.0,
            low: 91.0,
            close: 93.0,
        },
    ];
    let mut params = default_params();
    params.stop_loss = Some(0.05); // SL at 95

    let mut entry_dates = HashSet::new();
    entry_dates.insert(bars[0].datetime);

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
    assert_eq!(result.trade_log[0].exit_type, ExitType::StopLoss);
    // PnL should be based on fill at 92 (open), not 95 (stop level)
    // Entry at 100, exit at 92, 100 shares = -800
    assert!(
        (result.total_pnl - (-800.0)).abs() < 1e-6,
        "Gap-through should fill at open (92), not stop (95); pnl={}",
        result.total_pnl
    );
}

#[test]
fn gap_through_take_profit_fills_at_open() {
    // Long position, TP at 105 (5%), but next bar opens at 108 (gap through)
    let bars = vec![
        Bar {
            datetime: dt(2024, 1, 2),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
        },
        Bar {
            datetime: dt(2024, 1, 3),
            open: 108.0, // gap through TP at 105
            high: 110.0,
            low: 107.0,
            close: 109.0,
        },
    ];
    let mut params = default_params();
    params.take_profit = Some(0.05); // TP at 105

    let mut entry_dates = HashSet::new();
    entry_dates.insert(bars[0].datetime);

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
    assert_eq!(result.trade_log[0].exit_type, ExitType::TakeProfit);
    // PnL should be based on fill at 108 (open), not 105 (TP level)
    // Entry at 100, exit at 108, 100 shares = 800
    assert!(
        (result.total_pnl - 800.0).abs() < 1e-6,
        "Gap-through should fill at open (108), not TP (105); pnl={}",
        result.total_pnl
    );
}
