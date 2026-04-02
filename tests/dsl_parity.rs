//! Parity tests: verify .trading DSL strategies produce equivalent results to .rhai originals.

use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::*;
use std::collections::HashMap;

use optopsy_mcp::scripting::dsl;
use optopsy_mcp::scripting::engine::{run_script_backtest, DataLoader, ScriptBacktestResult};
use optopsy_mcp::scripting::types::OhlcvBar;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// SMA 200 Threshold fixture (identical to tests/script_sma200_threshold.rs)
// ---------------------------------------------------------------------------

fn make_sma200_test_bars() -> Vec<OhlcvBar> {
    let mut bars = Vec::new();
    let start = NaiveDate::from_ymd_opt(2023, 1, 2).unwrap();

    for i in 0..250 {
        let day = start + chrono::Duration::days(i);
        let dt = day.and_hms_opt(0, 0, 0).unwrap();

        let close = if i < 210 {
            100.0 + (i as f64) * 0.3
        } else if i < 235 {
            170.0 + ((i - 210) as f64) * 3.0
        } else {
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

fn sma200_params() -> HashMap<String, serde_json::Value> {
    let mut params = HashMap::new();
    params.insert("SYMBOL".to_string(), serde_json::json!("TEST"));
    params.insert("CAPITAL".to_string(), serde_json::json!(100_000));
    params
}

// ---------------------------------------------------------------------------
// BB Mean Reversion fixture (identical to tests/script_bb_mean_reversion.rs)
// ---------------------------------------------------------------------------

fn make_bb_test_bars() -> Vec<OhlcvBar> {
    let mut bars = Vec::new();

    let stable_closes = [
        100.0, 100.2, 99.8, 100.1, 99.9, 100.0, 100.1, 99.9, 100.0, 100.2, 99.8, 100.1, 99.9,
        100.0, 100.2, 99.8, 100.1, 99.9, 100.0, 100.1, 99.9, 100.0, 100.2, 99.8, 100.1, 99.9,
        100.0, 100.1, 99.9, 100.0,
    ];

    for (i, &close) in stable_closes.iter().enumerate() {
        let date = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap() + chrono::Duration::days(i as i64);
        bars.push(OhlcvBar {
            datetime: date.and_hms_opt(0, 0, 0).unwrap(),
            open: close - 0.1,
            high: close + 0.3,
            low: close - 0.3,
            close,
            volume: 1_000_000.0,
        });
    }

    // Bar 30: massive spike above upper BB
    let spike_date = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap() + chrono::Duration::days(30);
    bars.push(OhlcvBar {
        datetime: spike_date.and_hms_opt(0, 0, 0).unwrap(),
        open: 100.0,
        high: 106.0,
        low: 99.8,
        close: 105.0,
        volume: 2_000_000.0,
    });

    // Bars 31-35: price stays above SMA (no exit yet)
    for i in 1..=5 {
        let date = spike_date + chrono::Duration::days(i);
        let close = 103.0 - (i as f64 * 0.3);
        bars.push(OhlcvBar {
            datetime: date.and_hms_opt(0, 0, 0).unwrap(),
            open: close + 0.1,
            high: close + 0.3,
            low: close - 0.3,
            close,
            volume: 1_000_000.0,
        });
    }

    // Bar 36: price drops well below SMA(20) -> take profit exit
    let exit_date = spike_date + chrono::Duration::days(6);
    bars.push(OhlcvBar {
        datetime: exit_date.and_hms_opt(0, 0, 0).unwrap(),
        open: 100.0,
        high: 100.2,
        low: 97.0,
        close: 97.0,
        volume: 1_000_000.0,
    });

    bars
}

fn bb_params() -> HashMap<String, serde_json::Value> {
    let mut params = HashMap::new();
    params.insert("SYMBOL".to_string(), serde_json::json!("SPY"));
    params.insert("CAPITAL".to_string(), serde_json::json!(100_000));
    params
}

// ---------------------------------------------------------------------------
// Runner helper: run either .rhai or .trading script and return result
// ---------------------------------------------------------------------------

async fn run_strategy(
    script_path: &str,
    params: &HashMap<String, serde_json::Value>,
    bars: &[OhlcvBar],
) -> ScriptBacktestResult {
    let source = std::fs::read_to_string(script_path)
        .unwrap_or_else(|e| panic!("Failed to read {script_path}: {e}"));

    // If it's a .trading file, transpile to Rhai first
    let script = if dsl::is_trading_dsl(&source) {
        dsl::transpile(&source)
            .unwrap_or_else(|e| panic!("DSL transpile failed for {script_path}: {e}"))
    } else {
        source
    };

    let loader = TestDataLoader {
        ohlcv_df: bars_to_df(bars),
    };

    run_script_backtest(&script, params, &loader, None, None, None)
        .await
        .unwrap_or_else(|e| panic!("Backtest failed for {script_path}: {e}"))
}

// ===== SMA 200 THRESHOLD PARITY TESTS =====

#[tokio::test]
async fn parity_sma200_threshold_trade_count() {
    let bars = make_sma200_test_bars();
    let params = sma200_params();

    let rhai_result =
        run_strategy("scripts/strategies/sma200_threshold.rhai", &params, &bars).await;
    let dsl_result = run_strategy(
        "scripts/strategies/sma200_threshold.trading",
        &params,
        &bars,
    )
    .await;

    assert!(
        rhai_result.result.trade_count > 0,
        "Rhai should produce trades"
    );
    assert_eq!(
        rhai_result.result.trade_count, dsl_result.result.trade_count,
        "Trade count mismatch: rhai={}, dsl={}",
        rhai_result.result.trade_count, dsl_result.result.trade_count
    );

    eprintln!(
        "SMA200 parity: both produced {} trade(s)",
        rhai_result.result.trade_count
    );
}

#[tokio::test]
async fn parity_sma200_threshold_equity_curve() {
    let bars = make_sma200_test_bars();
    let params = sma200_params();

    let rhai_result =
        run_strategy("scripts/strategies/sma200_threshold.rhai", &params, &bars).await;
    let dsl_result = run_strategy(
        "scripts/strategies/sma200_threshold.trading",
        &params,
        &bars,
    )
    .await;

    assert_eq!(
        rhai_result.result.equity_curve.len(),
        dsl_result.result.equity_curve.len(),
        "Equity curve length mismatch: rhai={}, dsl={}",
        rhai_result.result.equity_curve.len(),
        dsl_result.result.equity_curve.len()
    );

    // Compare final equity (allow tiny float difference)
    let rhai_final = rhai_result.result.equity_curve.last().unwrap().equity;
    let dsl_final = dsl_result.result.equity_curve.last().unwrap().equity;
    assert!(
        (rhai_final - dsl_final).abs() < 0.01,
        "Final equity mismatch: rhai={rhai_final}, dsl={dsl_final}"
    );

    eprintln!("SMA200 equity parity: rhai={rhai_final:.2}, dsl={dsl_final:.2}");
}

#[tokio::test]
async fn parity_sma200_threshold_custom_series() {
    let bars = make_sma200_test_bars();
    let params = sma200_params();

    let rhai_result =
        run_strategy("scripts/strategies/sma200_threshold.rhai", &params, &bars).await;
    let dsl_result = run_strategy(
        "scripts/strategies/sma200_threshold.trading",
        &params,
        &bars,
    )
    .await;

    // Both should have the same custom series keys
    let rhai_keys: std::collections::BTreeSet<&String> =
        rhai_result.custom_series.series.keys().collect();
    let dsl_keys: std::collections::BTreeSet<&String> =
        dsl_result.custom_series.series.keys().collect();

    assert_eq!(
        rhai_keys, dsl_keys,
        "Custom series keys mismatch.\n  rhai: {rhai_keys:?}\n  dsl:  {dsl_keys:?}"
    );

    // Check that the series values match (same length, same non-None values)
    for key in &rhai_keys {
        let rhai_vals = &rhai_result.custom_series.series[*key];
        let dsl_vals = &dsl_result.custom_series.series[*key];
        assert_eq!(
            rhai_vals.len(),
            dsl_vals.len(),
            "Series '{key}' length mismatch"
        );

        for (i, (rv, dv)) in rhai_vals.iter().zip(dsl_vals.iter()).enumerate() {
            match (rv, dv) {
                (Some(r), Some(d)) => {
                    assert!(
                        (r - d).abs() < 0.01,
                        "Series '{key}' value mismatch at index {i}: rhai={r}, dsl={d}"
                    );
                }
                (None, None) => {}
                _ => {
                    panic!(
                        "Series '{key}' presence mismatch at index {i}: rhai={rv:?}, dsl={dv:?}"
                    );
                }
            }
        }
    }

    eprintln!("SMA200 custom series parity: {rhai_keys:?}");
}

// ===== BB MEAN REVERSION PARITY TESTS =====

#[tokio::test]
async fn parity_bb_mean_reversion_trade_count() {
    let bars = make_bb_test_bars();
    let params = bb_params();

    let rhai_result =
        run_strategy("scripts/strategies/bb_mean_reversion.rhai", &params, &bars).await;
    let dsl_result = run_strategy(
        "scripts/strategies/bb_mean_reversion.trading",
        &params,
        &bars,
    )
    .await;

    assert!(
        rhai_result.result.trade_count > 0,
        "Rhai should produce trades"
    );
    assert_eq!(
        rhai_result.result.trade_count, dsl_result.result.trade_count,
        "Trade count mismatch: rhai={}, dsl={}",
        rhai_result.result.trade_count, dsl_result.result.trade_count
    );

    eprintln!(
        "BB parity: both produced {} trade(s)",
        rhai_result.result.trade_count
    );
}

#[tokio::test]
async fn parity_bb_mean_reversion_exit_types() {
    let bars = make_bb_test_bars();
    let params = bb_params();

    let rhai_result =
        run_strategy("scripts/strategies/bb_mean_reversion.rhai", &params, &bars).await;
    let dsl_result = run_strategy(
        "scripts/strategies/bb_mean_reversion.trading",
        &params,
        &bars,
    )
    .await;

    let rhai_exits: Vec<String> = rhai_result
        .result
        .trade_log
        .iter()
        .map(|t| format!("{:?}", t.exit_type))
        .collect();
    let dsl_exits: Vec<String> = dsl_result
        .result
        .trade_log
        .iter()
        .map(|t| format!("{:?}", t.exit_type))
        .collect();

    assert_eq!(
        rhai_exits, dsl_exits,
        "Exit types mismatch.\n  rhai: {rhai_exits:?}\n  dsl:  {dsl_exits:?}"
    );

    eprintln!("BB exit type parity: {rhai_exits:?}");
}

#[tokio::test]
async fn parity_bb_mean_reversion_equity_curve() {
    let bars = make_bb_test_bars();
    let params = bb_params();

    let rhai_result =
        run_strategy("scripts/strategies/bb_mean_reversion.rhai", &params, &bars).await;
    let dsl_result = run_strategy(
        "scripts/strategies/bb_mean_reversion.trading",
        &params,
        &bars,
    )
    .await;

    assert_eq!(
        rhai_result.result.equity_curve.len(),
        dsl_result.result.equity_curve.len(),
        "Equity curve length mismatch"
    );

    let rhai_final = rhai_result.result.equity_curve.last().unwrap().equity;
    let dsl_final = dsl_result.result.equity_curve.last().unwrap().equity;
    assert!(
        (rhai_final - dsl_final).abs() < 0.01,
        "Final equity mismatch: rhai={rhai_final}, dsl={dsl_final}"
    );

    eprintln!("BB equity parity: rhai={rhai_final:.2}, dsl={dsl_final:.2}");
}
