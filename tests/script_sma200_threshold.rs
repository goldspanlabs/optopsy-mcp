//! Dry-run integration test for the `sma200_threshold` strategy with `ctx.plot()`.

use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::*;
use std::collections::HashMap;

use optopsy_mcp::scripting::engine::{run_script_backtest, DataLoader};
use optopsy_mcp::scripting::types::OhlcvBar;

fn bars_to_df(bars: &[OhlcvBar]) -> DataFrame {
    let datetimes: Vec<chrono::NaiveDateTime> = bars.iter().map(|b| b.datetime).collect();
    let opens: Vec<f64> = bars.iter().map(|b| b.open).collect();
    let highs: Vec<f64> = bars.iter().map(|b| b.high).collect();
    let lows: Vec<f64> = bars.iter().map(|b| b.low).collect();
    let closes: Vec<f64> = bars.iter().map(|b| b.close).collect();
    let volumes: Vec<f64> = bars.iter().map(|b| b.volume).collect();

    df! {
        "datetime" => DatetimeChunked::from_naive_datetime(
            PlSmallStr::from("datetime"),
            datetimes,
            TimeUnit::Microseconds,
        ).into_column().take_materialized_series(),
        "open" => &opens,
        "high" => &highs,
        "low" => &lows,
        "close" => &closes,
        "volume" => &volumes,
    }
    .unwrap()
}

struct TestDataLoader {
    ohlcv_df: DataFrame,
}

#[async_trait::async_trait]
impl DataLoader for TestDataLoader {
    async fn load_ohlcv(
        &self,
        _symbol: &str,
        _start: Option<NaiveDate>,
        _end: Option<NaiveDate>,
    ) -> Result<DataFrame> {
        Ok(self.ohlcv_df.clone())
    }

    async fn load_options(
        &self,
        _symbol: &str,
        _start: Option<NaiveDate>,
        _end: Option<NaiveDate>,
    ) -> Result<DataFrame> {
        Ok(DataFrame::empty())
    }

    fn load_splits(
        &self,
        _symbol: &str,
    ) -> Result<Vec<optopsy_mcp::data::adjustment_store::SplitRow>> {
        Ok(Vec::new())
    }

    fn load_dividends(
        &self,
        _symbol: &str,
    ) -> Result<Vec<optopsy_mcp::data::adjustment_store::DividendRow>> {
        Ok(Vec::new())
    }
}

fn sma200_params() -> HashMap<String, serde_json::Value> {
    let mut params = HashMap::new();
    params.insert("SYMBOL".to_string(), serde_json::json!("TEST"));
    params.insert("CAPITAL".to_string(), serde_json::json!(100_000));
    params
}

/// Generate 250 daily bars with an uptrend that eventually triggers
/// entry (close > SMA200 * 1.04) and then a drop for exit.
fn make_sma200_test_bars() -> Vec<OhlcvBar> {
    let mut bars = Vec::new();
    let start = NaiveDate::from_ymd_opt(2023, 1, 2).unwrap();

    for i in 0..250 {
        let day = start + chrono::Duration::days(i);
        let dt = day.and_hms_opt(0, 0, 0).unwrap();

        // Steady uptrend so SMA(200) builds up, then a sharp rally
        let close = if i < 210 {
            100.0 + (i as f64) * 0.3 // 100 → 163 over 210 bars
        } else if i < 235 {
            // Sharp rally to break above SMA(200) * 1.04
            170.0 + ((i - 210) as f64) * 3.0 // 170 → 245
        } else {
            // Crash below SMA(200) * 0.97
            90.0
        };

        bars.push(OhlcvBar {
            datetime: dt,
            open: close - 0.5,
            high: close + 1.0,
            low: close - 1.0,
            close,
            volume: 1_000_000.0,
        });
    }
    bars
}

#[tokio::test]
async fn sma200_threshold_runs_and_produces_trades() {
    let bars = make_sma200_test_bars();
    let df = bars_to_df(&bars);
    let loader = TestDataLoader { ohlcv_df: df };

    let script = std::fs::read_to_string("scripts/strategies/sma200_threshold.rhai")
        .expect("sma200_threshold.rhai should exist");

    let result = run_script_backtest(&script, &sma200_params(), &loader, None, None, None)
        .await
        .expect("backtest should complete without error");

    // Should have at least one trade (entry when close > SMA*1.04, exit when close < SMA*0.97)
    assert!(
        !result.result.trade_log.is_empty(),
        "expected at least one trade, got none. Warnings: {:?}",
        result.result.warnings
    );

    // Equity curve should be populated
    assert!(
        !result.result.equity_curve.is_empty(),
        "equity curve should not be empty"
    );

    // ctx.plot() should have produced custom series
    assert!(
        !result.custom_series.series.is_empty(),
        "expected custom_series from ctx.plot() calls, got none"
    );

    // Check that the two threshold series exist
    let series_names: Vec<&str> = result
        .custom_series
        .series
        .keys()
        .map(String::as_str)
        .collect();
    assert!(
        series_names.iter().any(|n| n.contains("Entry Threshold")),
        "expected 'Entry Threshold' series, found: {series_names:?}"
    );
    assert!(
        series_names.iter().any(|n| n.contains("Exit Threshold")),
        "expected 'Exit Threshold' series, found: {series_names:?}"
    );

    eprintln!(
        "✓ sma200_threshold: {} trades, {} equity points, {} custom series",
        result.result.trade_log.len(),
        result.result.equity_curve.len(),
        result.custom_series.series.len()
    );
}

#[tokio::test]
async fn sma200_threshold_no_trades_before_warmup() {
    // Only 100 bars — not enough for SMA(200) to compute
    let bars: Vec<OhlcvBar> = make_sma200_test_bars().into_iter().take(100).collect();
    let df = bars_to_df(&bars);
    let loader = TestDataLoader { ohlcv_df: df };

    let script = std::fs::read_to_string("scripts/strategies/sma200_threshold.rhai")
        .expect("sma200_threshold.rhai should exist");

    let result = run_script_backtest(&script, &sma200_params(), &loader, None, None, None)
        .await
        .expect("backtest should complete without error");

    // No trades: SMA(200) requires 200 bars to produce a value
    assert!(
        result.result.trade_log.is_empty(),
        "expected no trades during warmup period, got {}",
        result.result.trade_log.len()
    );
}
