//! Integration tests for the stock/equity backtesting pipeline.
//!
//! Tests the full flow: write OHLCV parquet → parse bars → build signal filters →
//! run simulation → verify trades, metrics, and AI-formatted response.

use chrono::{Datelike, NaiveDate};
use optopsy_mcp::engine::stock_sim::{
    build_stock_signal_filters, load_ohlcv_df, parse_ohlcv_bars, run_stock_backtest,
    StockBacktestParams,
};
use optopsy_mcp::engine::types::{Commission, ExitType, Side, Slippage};
use optopsy_mcp::signals::registry::SignalSpec;
use optopsy_mcp::tools::response_types::UnderlyingPrice;
use polars::prelude::*;
use tempfile::TempDir;

/// OHLCV data tuple: (dates, opens, highs, lows, closes).
type OhlcvData = (Vec<NaiveDate>, Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>);

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Default stock backtest params for tests.
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
        entry_signal: None,
        exit_signal: None,
        ohlcv_path: None,
        cross_ohlcv_paths: std::collections::HashMap::new(),
        start_date: None,
        end_date: None,
        interval: optopsy_mcp::engine::types::Interval::Daily,
    }
}

/// Write synthetic OHLCV data to a temp parquet file.
/// Returns the `TempDir` (must stay alive) and the file path string.
fn write_ohlcv(
    dates: &[NaiveDate],
    opens: &[f64],
    highs: &[f64],
    lows: &[f64],
    closes: &[f64],
) -> (TempDir, String) {
    let n = dates.len();
    let volumes: Vec<i64> = vec![1_000_000; n];
    let adjcloses: Vec<f64> = closes.to_vec();

    let mut df = df! {
        "open" => opens,
        "high" => highs,
        "low" => lows,
        "close" => closes,
        "adjclose" => &adjcloses,
        "volume" => &volumes,
    }
    .unwrap();
    df.with_column(
        DateChunked::from_naive_date(PlSmallStr::from("date"), dates.to_vec()).into_column(),
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ohlcv.parquet");
    let file = std::fs::File::create(&path).unwrap();
    ParquetWriter::new(file).finish(&mut df).unwrap();
    (dir, path.to_string_lossy().to_string())
}

/// Generate 60 trading days of synthetic trending-up data.
/// Starts at 100, drifts +0.5/day with some noise.
fn trending_up_data() -> OhlcvData {
    let start = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
    let mut dates = Vec::new();
    let mut opens = Vec::new();
    let mut highs = Vec::new();
    let mut lows = Vec::new();
    let mut closes = Vec::new();

    let mut price = 100.0;
    for i in 0..60 {
        let date = start + chrono::Duration::days(i);
        // Skip weekends
        if date.weekday() == chrono::Weekday::Sat || date.weekday() == chrono::Weekday::Sun {
            continue;
        }
        let open = price;
        let high = price + 2.0;
        let low = price - 1.0;
        let close = price + 0.5;

        dates.push(date);
        opens.push(open);
        highs.push(high);
        lows.push(low);
        closes.push(close);

        price = close;
    }
    (dates, opens, highs, lows, closes)
}

/// Generate 60 trading days of synthetic trending-down data.
/// Starts at 150, drifts -0.5/day.
fn trending_down_data() -> OhlcvData {
    let start = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
    let mut dates = Vec::new();
    let mut opens = Vec::new();
    let mut highs = Vec::new();
    let mut lows = Vec::new();
    let mut closes = Vec::new();

    let mut price = 150.0;
    for i in 0..60 {
        let date = start + chrono::Duration::days(i);
        if date.weekday() == chrono::Weekday::Sat || date.weekday() == chrono::Weekday::Sun {
            continue;
        }
        let open = price;
        let high = price + 1.0;
        let low = price - 2.0;
        let close = price - 0.5;

        dates.push(date);
        opens.push(open);
        highs.push(high);
        lows.push(low);
        closes.push(close);

        price = close;
    }
    (dates, opens, highs, lows, closes)
}

/// Generate 80 trading days of choppy/mean-reverting data.
/// Oscillates between 95 and 105 with a sine-like pattern.
fn choppy_data() -> OhlcvData {
    let start = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
    let mut dates = Vec::new();
    let mut opens = Vec::new();
    let mut highs = Vec::new();
    let mut lows = Vec::new();
    let mut closes = Vec::new();

    for i in 0..80 {
        let date = start + chrono::Duration::days(i);
        if date.weekday() == chrono::Weekday::Sat || date.weekday() == chrono::Weekday::Sun {
            continue;
        }
        let t = i as f64;
        let mid = 100.0 + 5.0 * (t * 0.3).sin();
        let open = mid - 0.2;
        let high = mid + 1.5;
        let low = mid - 1.5;
        let close = mid + 0.2;

        dates.push(date);
        opens.push(open);
        highs.push(high);
        lows.push(low);
        closes.push(close);
    }
    (dates, opens, highs, lows, closes)
}

// ─── Pipeline: parquet → parse → simulate ────────────────────────────────────

#[test]
fn parse_ohlcv_bars_from_parquet() {
    let (dates, opens, highs, lows, closes) = trending_up_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let bars = parse_ohlcv_bars(&path, None, None).unwrap();
    assert_eq!(bars.len(), dates.len());
}

#[test]
fn parse_ohlcv_bars_with_date_filter() {
    let (dates, opens, highs, lows, closes) = trending_up_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let start = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
    let end = NaiveDate::from_ymd_opt(2024, 1, 31).unwrap();
    let bars = parse_ohlcv_bars(&path, Some(start), Some(end)).unwrap();

    assert!(!bars.is_empty());
    assert!(
        bars.len() < dates.len(),
        "date filter should reduce bar count"
    );
}

#[test]
fn full_pipeline_trending_up_long() {
    let (dates, opens, highs, lows, closes) = trending_up_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let bars = parse_ohlcv_bars(&path, None, None).unwrap();
    let bar_count = bars.len();

    // Entry on day 0 with ConsecutiveUp (count=1 → fires when close > prev close)
    // In trending-up data, this fires on every bar after the first
    let mut entry_dates = std::collections::HashSet::new();
    entry_dates.insert(dates[0]);

    let params = default_params();
    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();

    assert_eq!(result.trade_count, 1);
    // Long position entered at open of first bar (100.0), closed at close of last bar
    // In trending-up data, close increases ~0.5/bar so last close ≈ 100 + 0.5*(n-1)
    assert!(
        result.total_pnl > 0.0,
        "Long in uptrend should be profitable"
    );
    assert_eq!(result.equity_curve.len(), bar_count);
}

#[test]
fn full_pipeline_trending_down_short() {
    let (dates, opens, highs, lows, closes) = trending_down_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let bars = parse_ohlcv_bars(&path, None, None).unwrap();

    let mut entry_dates = std::collections::HashSet::new();
    entry_dates.insert(dates[0]);

    let mut params = default_params();
    params.side = Side::Short;

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();

    assert_eq!(result.trade_count, 1);
    assert!(
        result.total_pnl > 0.0,
        "Short in downtrend should be profitable, got {}",
        result.total_pnl
    );
}

#[test]
fn full_pipeline_long_in_downtrend_loses() {
    let (dates, opens, highs, lows, closes) = trending_down_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let bars = parse_ohlcv_bars(&path, None, None).unwrap();

    let mut entry_dates = std::collections::HashSet::new();
    entry_dates.insert(dates[0]);

    let params = default_params();
    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();

    assert_eq!(result.trade_count, 1);
    assert!(
        result.total_pnl < 0.0,
        "Long in downtrend should lose money, got {}",
        result.total_pnl
    );
}

// ─── Signal-driven entry/exit via build_stock_signal_filters ─────────────────

#[test]
fn signal_driven_entry_consecutive_up() {
    let (dates, opens, highs, lows, closes) = trending_up_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let mut params = default_params();
    params.ohlcv_path = Some(path.clone());
    params.entry_signal = Some(SignalSpec::Formula {
        formula: "consecutive_up(close) >= 3".into(),
    });

    let ohlcv_df = load_ohlcv_df(&path, None, None).unwrap();
    let (entry_dates, exit_dates) = build_stock_signal_filters(&params, &ohlcv_df).unwrap();

    assert!(entry_dates.is_some(), "Should find entry signal dates");
    let entry_set = entry_dates.as_ref().unwrap();
    assert!(
        !entry_set.is_empty(),
        "Trending-up data should have ConsecutiveUp(3) signal dates"
    );
    assert!(exit_dates.is_none(), "No exit signal configured");
}

#[test]
fn signal_driven_entry_and_exit() {
    let (dates, opens, highs, lows, closes) = choppy_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let mut params = default_params();
    params.ohlcv_path = Some(path.clone());
    params.entry_signal = Some(SignalSpec::Formula {
        formula: "consecutive_up(close) >= 2".into(),
    });
    params.exit_signal = Some(SignalSpec::Formula {
        formula: "consecutive_down(close) >= 2".into(),
    });

    let ohlcv_df = load_ohlcv_df(&path, None, None).unwrap();
    let (entry_dates, exit_dates) = build_stock_signal_filters(&params, &ohlcv_df).unwrap();

    assert!(entry_dates.is_some());
    assert!(exit_dates.is_some());
    let entry_set = entry_dates.as_ref().unwrap();
    let exit_set = exit_dates.as_ref().unwrap();
    assert!(
        !entry_set.is_empty(),
        "Choppy data should have ConsecutiveUp(2) signals"
    );
    assert!(
        !exit_set.is_empty(),
        "Choppy data should have ConsecutiveDown(2) signals"
    );
}

#[test]
fn signal_filters_into_full_backtest() {
    let (dates, opens, highs, lows, closes) = trending_up_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let mut params = default_params();
    params.ohlcv_path = Some(path.clone());
    params.entry_signal = Some(SignalSpec::Formula {
        formula: "consecutive_up(close) >= 3".into(),
    });

    let ohlcv_df = load_ohlcv_df(&path, None, None).unwrap();
    let (entry_dates, exit_dates) = build_stock_signal_filters(&params, &ohlcv_df).unwrap();
    let bars = parse_ohlcv_bars(&path, None, None).unwrap();

    let result =
        run_stock_backtest(&bars, &params, entry_dates.as_ref(), exit_dates.as_ref()).unwrap();

    assert!(
        result.trade_count >= 1,
        "Should have at least 1 trade from ConsecutiveUp(3) signal"
    );
    // In a consistently trending-up market with max_positions=1, the first entry
    // stays open (no exit signal) until force-closed at end
    assert!(
        result.total_pnl > 0.0,
        "Signal-driven long in uptrend should profit"
    );
}

#[test]
fn signal_entry_and_exit_produces_multiple_trades() {
    let (dates, opens, highs, lows, closes) = choppy_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let mut params = default_params();
    params.ohlcv_path = Some(path.clone());
    params.entry_signal = Some(SignalSpec::Formula {
        formula: "consecutive_up(close) >= 2".into(),
    });
    params.exit_signal = Some(SignalSpec::Formula {
        formula: "consecutive_down(close) >= 2".into(),
    });

    let ohlcv_df = load_ohlcv_df(&path, None, None).unwrap();
    let (entry_dates, exit_dates) = build_stock_signal_filters(&params, &ohlcv_df).unwrap();
    let bars = parse_ohlcv_bars(&path, None, None).unwrap();

    let result =
        run_stock_backtest(&bars, &params, entry_dates.as_ref(), exit_dates.as_ref()).unwrap();

    // In choppy data with entry/exit signals, we should see multiple trades
    // as positions open on up-streaks and close on down-streaks
    assert!(
        result.trade_count >= 2,
        "Choppy data with entry+exit signals should produce multiple trades, got {}",
        result.trade_count
    );

    // All closed trades (except possibly last) should be Signal exits
    let signal_exits = result
        .trade_log
        .iter()
        .filter(|t| matches!(t.exit_type, ExitType::Signal))
        .count();
    assert!(
        signal_exits >= 1,
        "Should have at least 1 Signal exit in choppy data"
    );
}

// ─── Exit scenarios with realistic multi-bar data ────────────────────────────

#[test]
fn stop_loss_triggers_in_downtrend() {
    let (dates, opens, highs, lows, closes) = trending_down_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let bars = parse_ohlcv_bars(&path, None, None).unwrap();

    let mut entry_dates = std::collections::HashSet::new();
    entry_dates.insert(dates[0]);

    let mut params = default_params();
    params.stop_loss = Some(0.02); // 2% stop loss — should trigger quickly in downtrend

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();

    assert_eq!(result.trade_count, 1);
    assert_eq!(
        result.trade_log[0].exit_type,
        ExitType::StopLoss,
        "2% stop loss should trigger in downtrend"
    );
    assert!(
        result.total_pnl < 0.0,
        "Stop loss exit should be a loss, got {}",
        result.total_pnl
    );
}

#[test]
fn take_profit_triggers_in_uptrend() {
    let (dates, opens, highs, lows, closes) = trending_up_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let bars = parse_ohlcv_bars(&path, None, None).unwrap();

    let mut entry_dates = std::collections::HashSet::new();
    entry_dates.insert(dates[0]);

    let mut params = default_params();
    params.take_profit = Some(0.02); // 2% take profit — should trigger quickly in uptrend

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();

    assert_eq!(result.trade_count, 1);
    assert_eq!(
        result.trade_log[0].exit_type,
        ExitType::TakeProfit,
        "2% take profit should trigger in uptrend"
    );
    assert!(result.total_pnl > 0.0, "Take profit exit should be a gain");
}

#[test]
fn max_hold_forces_exit() {
    let (dates, opens, highs, lows, closes) = trending_up_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let bars = parse_ohlcv_bars(&path, None, None).unwrap();

    let mut entry_dates = std::collections::HashSet::new();
    entry_dates.insert(dates[0]);

    let mut params = default_params();
    params.max_hold_days = Some(5);

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();

    assert_eq!(result.trade_count, 1);
    assert_eq!(result.trade_log[0].exit_type, ExitType::MaxHold);
    assert!(
        result.trade_log[0].days_held <= 7,
        "Should exit within ~5 calendar days (may include weekend gap), got {}",
        result.trade_log[0].days_held
    );
}

#[test]
fn stop_loss_beats_max_hold_same_bar() {
    // Build data where price drops enough to trigger both SL and max_hold on the same day
    let dates = vec![
        NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(), // Day 2 (calendar days = 2)
    ];
    let opens = vec![100.0, 100.5, 99.0];
    let highs = vec![101.0, 101.0, 99.5];
    let lows = vec![99.5, 99.0, 89.0]; // Day 3: low = 89 triggers 5% SL
    let closes = vec![100.5, 99.5, 90.0];

    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);
    let bars = parse_ohlcv_bars(&path, None, None).unwrap();

    let mut entry_dates = std::collections::HashSet::new();
    entry_dates.insert(dates[0]);

    let mut params = default_params();
    params.stop_loss = Some(0.05); // 5% SL
    params.max_hold_days = Some(2); // Also fires on day 3

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();

    assert_eq!(result.trade_count, 1);
    // StopLoss is checked before MaxHold in the exit priority chain
    assert_eq!(
        result.trade_log[0].exit_type,
        ExitType::StopLoss,
        "StopLoss should have priority over MaxHold"
    );
}

// ─── Slippage models ─────────────────────────────────────────────────────────

#[test]
fn mid_vs_spread_slippage_different_pnl() {
    let (dates, opens, highs, lows, closes) = trending_up_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let bars = parse_ohlcv_bars(&path, None, None).unwrap();

    let mut entry_dates = std::collections::HashSet::new();
    entry_dates.insert(dates[0]);

    let mut params_mid = default_params();
    params_mid.slippage = Slippage::Mid;
    let result_mid = run_stock_backtest(&bars, &params_mid, Some(&entry_dates), None).unwrap();

    let mut params_spread = default_params();
    params_spread.slippage = Slippage::Spread;
    let result_spread =
        run_stock_backtest(&bars, &params_spread, Some(&entry_dates), None).unwrap();

    assert!(
        (result_mid.total_pnl - result_spread.total_pnl).abs() > f64::EPSILON,
        "Mid ({}) and Spread ({}) should produce different P&L",
        result_mid.total_pnl,
        result_spread.total_pnl
    );
    // Spread should be worse (higher entry cost, lower exit proceeds for long)
    assert!(
        result_mid.total_pnl > result_spread.total_pnl,
        "Mid should yield better P&L than Spread for longs"
    );
}

#[test]
fn per_leg_slippage_worse_than_mid() {
    let (dates, opens, highs, lows, closes) = trending_up_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let bars = parse_ohlcv_bars(&path, None, None).unwrap();

    let mut entry_dates = std::collections::HashSet::new();
    entry_dates.insert(dates[0]);

    let mut params_mid = default_params();
    params_mid.slippage = Slippage::Mid;
    let result_mid = run_stock_backtest(&bars, &params_mid, Some(&entry_dates), None).unwrap();

    let mut params_per_leg = default_params();
    params_per_leg.slippage = Slippage::PerLeg { per_leg: 0.20 };
    let result_pl = run_stock_backtest(&bars, &params_per_leg, Some(&entry_dates), None).unwrap();

    assert!(
        result_mid.total_pnl > result_pl.total_pnl,
        "PerLeg slippage should produce worse P&L than Mid"
    );
}

// ─── Commission integration ──────────────────────────────────────────────────

#[test]
fn commission_reduces_equity() {
    let (dates, opens, highs, lows, closes) = trending_up_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let bars = parse_ohlcv_bars(&path, None, None).unwrap();

    let mut entry_dates = std::collections::HashSet::new();
    entry_dates.insert(dates[0]);

    let params_no_comm = default_params();
    let result_no_comm =
        run_stock_backtest(&bars, &params_no_comm, Some(&entry_dates), None).unwrap();

    let mut params_comm = default_params();
    params_comm.commission = Some(Commission {
        per_contract: 0.01,
        base_fee: 5.0,
        min_fee: 0.0,
    });
    let result_comm = run_stock_backtest(&bars, &params_comm, Some(&entry_dates), None).unwrap();

    assert!(
        result_no_comm.total_pnl > result_comm.total_pnl,
        "Commission should reduce P&L: no_comm={}, comm={}",
        result_no_comm.total_pnl,
        result_comm.total_pnl
    );
}

// ─── Multiple positions ──────────────────────────────────────────────────────

#[test]
fn multiple_entries_with_max_positions() {
    let (dates, opens, highs, lows, closes) = trending_up_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let bars = parse_ohlcv_bars(&path, None, None).unwrap();

    // Signal fires on first 5 days
    let mut entry_dates = std::collections::HashSet::new();
    for date in dates.iter().take(5) {
        entry_dates.insert(*date);
    }

    // max_positions=1 → only 1 position opened
    let mut params_1 = default_params();
    params_1.max_positions = 1;
    let result_1 = run_stock_backtest(&bars, &params_1, Some(&entry_dates), None).unwrap();

    // max_positions=3 → up to 3 positions opened
    let mut params_3 = default_params();
    params_3.max_positions = 3;
    let result_3 = run_stock_backtest(&bars, &params_3, Some(&entry_dates), None).unwrap();

    assert_eq!(result_1.trade_count, 1);
    assert!(
        result_3.trade_count > 1 && result_3.trade_count <= 3,
        "max_positions=3 with 5 signal days should produce 2-3 trades, got {}",
        result_3.trade_count
    );
}

// ─── Short positions with exit scenarios ─────────────────────────────────────

#[test]
fn short_stop_loss_triggers_on_rally() {
    let (dates, opens, highs, lows, closes) = trending_up_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let bars = parse_ohlcv_bars(&path, None, None).unwrap();

    let mut entry_dates = std::collections::HashSet::new();
    entry_dates.insert(dates[0]);

    let mut params = default_params();
    params.side = Side::Short;
    params.stop_loss = Some(0.02); // 2% SL — triggers when high >= entry * 1.02

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();

    assert_eq!(result.trade_count, 1);
    assert_eq!(
        result.trade_log[0].exit_type,
        ExitType::StopLoss,
        "Short with 2% SL should stop out in uptrend"
    );
    assert!(
        result.total_pnl < 0.0,
        "Short stopped out in uptrend should lose money"
    );
}

#[test]
fn short_take_profit_triggers_on_decline() {
    let (dates, opens, highs, lows, closes) = trending_down_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let bars = parse_ohlcv_bars(&path, None, None).unwrap();

    let mut entry_dates = std::collections::HashSet::new();
    entry_dates.insert(dates[0]);

    let mut params = default_params();
    params.side = Side::Short;
    params.take_profit = Some(0.02); // 2% TP — triggers when low <= entry * 0.98

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();

    assert_eq!(result.trade_count, 1);
    assert_eq!(
        result.trade_log[0].exit_type,
        ExitType::TakeProfit,
        "Short with 2% TP should take profit in downtrend"
    );
    assert!(
        result.total_pnl > 0.0,
        "Short TP in downtrend should be profitable"
    );
}

// ─── Tool layer: execute() end-to-end ────────────────────────────────────────

#[test]
fn tool_execute_end_to_end() {
    let (dates, opens, highs, lows, closes) = trending_up_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let mut params = default_params();
    params.ohlcv_path = Some(path);
    params.entry_signal = Some(SignalSpec::Formula {
        formula: "consecutive_up(close) >= 3".into(),
    });

    let underlying_prices: Vec<UnderlyingPrice> = dates
        .iter()
        .enumerate()
        .map(|(i, d)| UnderlyingPrice {
            date: d.format("%Y-%m-%d").to_string(),
            open: opens[i],
            high: highs[i],
            low: lows[i],
            close: closes[i],
            volume: None,
        })
        .collect();

    let response = optopsy_mcp::tools::stock_backtest::execute(&params, underlying_prices).unwrap();

    // Verify response shape
    assert!(!response.summary.is_empty());
    assert!(!response.assessment.is_empty());
    assert!(!response.key_findings.is_empty());
    assert!(!response.suggested_next_steps.is_empty());
    assert_eq!(response.parameters.symbol, "SPY");
    assert_eq!(response.parameters.quantity, 100);
    assert!(!response.underlying_prices.is_empty());

    // Verify trade data
    assert!(
        response.trade_summary.total >= 1,
        "Should have at least 1 trade"
    );
    assert!(!response.trade_log.is_empty());
}

#[test]
fn tool_execute_zero_trades_response() {
    let (dates, opens, highs, lows, closes) = trending_down_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let mut params = default_params();
    params.ohlcv_path = Some(path);
    // ConsecutiveUp with high count in downtrend → no signals fire
    params.entry_signal = Some(SignalSpec::Formula {
        formula: "consecutive_up(close) >= 20".into(),
    });

    let response = optopsy_mcp::tools::stock_backtest::execute(&params, vec![]).unwrap();

    assert!(
        response.summary.contains("no trades"),
        "Zero-trade response should mention 'no trades', got: {}",
        response.summary
    );
    assert_eq!(response.assessment, "N/A");
    assert!(response.trade_log.is_empty());
}

// ─── Equity curve and metrics integrity ──────────────────────────────────────

#[test]
fn equity_curve_monotonic_start_for_profitable_trend() {
    let (dates, opens, highs, lows, closes) = trending_up_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let bars = parse_ohlcv_bars(&path, None, None).unwrap();

    let mut entry_dates = std::collections::HashSet::new();
    entry_dates.insert(dates[0]);

    let params = default_params();
    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();

    assert!(!result.equity_curve.is_empty());
    // In a smooth uptrend, equity should generally increase
    let first_equity = result.equity_curve[0].equity;
    let last_equity = result.equity_curve.last().unwrap().equity;
    assert!(
        last_equity > first_equity,
        "Equity should increase in uptrend: first={first_equity}, last={last_equity}"
    );
}

#[test]
fn metrics_populated_for_multi_trade_backtest() {
    let (dates, opens, highs, lows, closes) = choppy_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let bars = parse_ohlcv_bars(&path, None, None).unwrap();

    // Entry on every 5th trading day
    let mut entry_dates = std::collections::HashSet::new();
    for (i, date) in dates.iter().enumerate() {
        if i % 5 == 0 {
            entry_dates.insert(*date);
        }
    }

    let mut params = default_params();
    params.max_hold_days = Some(3); // Short hold → multiple trades

    let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();

    assert!(
        result.trade_count >= 3,
        "Should have at least 3 trades with max_hold=3 and entries every 5 days, got {}",
        result.trade_count
    );

    // Metrics should be meaningful (not default/zero)
    let m = &result.metrics;
    // Win rate should be between 0 and 1
    assert!(m.win_rate >= 0.0 && m.win_rate <= 1.0);
    // Profit factor should be non-negative
    assert!(m.profit_factor >= 0.0);
    // Max drawdown should be non-negative
    assert!(m.max_drawdown >= 0.0);
}

// ─── Date range filtering at sim level ───────────────────────────────────────

#[test]
fn date_range_limits_bars_used() {
    let (dates, opens, highs, lows, closes) = trending_up_data();
    let (_dir, path) = write_ohlcv(&dates, &opens, &highs, &lows, &closes);

    let all_bars = parse_ohlcv_bars(&path, None, None).unwrap();

    let start = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
    let end = NaiveDate::from_ymd_opt(2024, 2, 1).unwrap();
    let filtered_bars = parse_ohlcv_bars(&path, Some(start), Some(end)).unwrap();

    assert!(
        filtered_bars.len() < all_bars.len(),
        "Date filter should reduce bars: all={}, filtered={}",
        all_bars.len(),
        filtered_bars.len()
    );

    // Run backtest on filtered subset
    let mut entry_dates = std::collections::HashSet::new();
    entry_dates.insert(start);

    let params = default_params();
    let result = run_stock_backtest(&filtered_bars, &params, Some(&entry_dates), None).unwrap();

    assert_eq!(result.equity_curve.len(), filtered_bars.len());
}
