//! Integration tests: verify .trading DSL strategies produce correct backtest results.

use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::*;
use std::collections::{BTreeMap, HashMap};

use optopsy_mcp::data::parquet::DATETIME_COL;
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
    params.insert("symbol".to_string(), serde_json::json!("TEST"));
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
    params.insert("symbol".to_string(), serde_json::json!("SPY"));
    params.insert("CAPITAL".to_string(), serde_json::json!(100_000));
    params
}

// ---------------------------------------------------------------------------
// Runner helper: run a .trading script and return result
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

// ===== SMA 200 THRESHOLD TESTS =====

#[tokio::test]
async fn sma200_threshold_trade_count() {
    let bars = make_sma200_test_bars();
    let params = sma200_params();

    let result = run_strategy(
        "scripts/strategies/sma200_threshold.trading",
        &params,
        &bars,
    )
    .await;

    assert!(result.result.trade_count > 0, "Should produce trades");

    eprintln!("SMA200: produced {} trade(s)", result.result.trade_count);
}

#[tokio::test]
async fn sma200_threshold_equity_curve() {
    let bars = make_sma200_test_bars();
    let params = sma200_params();

    let result = run_strategy(
        "scripts/strategies/sma200_threshold.trading",
        &params,
        &bars,
    )
    .await;

    assert!(
        !result.result.equity_curve.is_empty(),
        "Equity curve should not be empty"
    );

    let final_equity = result.result.equity_curve.last().unwrap().equity;
    eprintln!("SMA200 final equity: {final_equity:.2}");
}

#[tokio::test]
async fn sma200_threshold_custom_series() {
    let bars = make_sma200_test_bars();
    let params = sma200_params();

    let result = run_strategy(
        "scripts/strategies/sma200_threshold.trading",
        &params,
        &bars,
    )
    .await;

    let keys: std::collections::BTreeSet<&String> = result.custom_series.series.keys().collect();

    assert!(
        !keys.is_empty(),
        "Should have custom series from ctx.plot() calls"
    );

    eprintln!("SMA200 custom series: {keys:?}");
}

// ===== BB MEAN REVERSION TESTS =====

#[tokio::test]
async fn bb_mean_reversion_trade_count() {
    let bars = make_bb_test_bars();
    let params = bb_params();

    let result = run_strategy(
        "scripts/strategies/bb_mean_reversion.trading",
        &params,
        &bars,
    )
    .await;

    assert!(result.result.trade_count > 0, "Should produce trades");

    eprintln!("BB: produced {} trade(s)", result.result.trade_count);
}

#[tokio::test]
async fn bb_mean_reversion_exit_types() {
    let bars = make_bb_test_bars();
    let params = bb_params();

    let result = run_strategy(
        "scripts/strategies/bb_mean_reversion.trading",
        &params,
        &bars,
    )
    .await;

    let exits: Vec<String> = result
        .result
        .trade_log
        .iter()
        .map(|t| format!("{:?}", t.exit_type))
        .collect();

    assert!(!exits.is_empty(), "Should have exit types");

    eprintln!("BB exit types: {exits:?}");
}

#[tokio::test]
async fn bb_mean_reversion_equity_curve() {
    let bars = make_bb_test_bars();
    let params = bb_params();

    let result = run_strategy(
        "scripts/strategies/bb_mean_reversion.trading",
        &params,
        &bars,
    )
    .await;

    assert!(
        !result.result.equity_curve.is_empty(),
        "Equity curve should not be empty"
    );

    let final_equity = result.result.equity_curve.last().unwrap().equity;
    eprintln!("BB final equity: {final_equity:.2}");
}

// ===========================================================================
// Wheel strategy parity tests
// ===========================================================================

// ---------------------------------------------------------------------------
// Helpers for options-based tests
// ---------------------------------------------------------------------------

fn d(y: i32, m: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, day).unwrap()
}

fn dt(y: i32, m: u32, day: u32) -> chrono::NaiveDateTime {
    d(y, m, day).and_hms_opt(0, 0, 0).unwrap()
}

/// Build a synthetic options `DataFrame`.
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

/// Test `DataLoader` that returns pre-built OHLCV and options `DataFrame`s.
struct OptionsTestDataLoader {
    ohlcv_df: DataFrame,
    options_df: DataFrame,
}

#[async_trait::async_trait]
impl DataLoader for OptionsTestDataLoader {
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
        Ok(self.options_df.clone())
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

fn make_bars_from_closes(closes: &BTreeMap<NaiveDate, f64>) -> Vec<OhlcvBar> {
    closes
        .iter()
        .map(|(&date, &close)| OhlcvBar {
            datetime: date.and_hms_opt(0, 0, 0).unwrap(),
            open: close,
            high: close * 1.01,
            low: close * 0.99,
            close,
            volume: 1_000_000.0,
        })
        .collect()
}

fn wheel_params() -> HashMap<String, serde_json::Value> {
    let mut params = HashMap::new();
    params.insert("symbol".to_string(), serde_json::json!("SPY"));
    params.insert("CAPITAL".to_string(), serde_json::json!(100_000));
    params.insert("PUT_DELTA".to_string(), serde_json::json!(0.30));
    params.insert("PUT_DTE".to_string(), serde_json::json!(45));
    params.insert("CALL_DELTA".to_string(), serde_json::json!(0.30));
    params.insert("CALL_DTE".to_string(), serde_json::json!(30));
    params.insert("EXIT_DTE".to_string(), serde_json::json!(5));
    params.insert("SLIPPAGE".to_string(), serde_json::json!("mid"));
    params.insert("MULTIPLIER".to_string(), serde_json::json!(100));
    params.insert("STOP_LOSS".to_string(), serde_json::json!(null));
    params.insert("TAKE_PROFIT".to_string(), serde_json::json!(null));
    params
}

/// Run a .trading script with options data and return result.
async fn run_options_strategy(
    script_path: &str,
    params: &HashMap<String, serde_json::Value>,
    bars: &[OhlcvBar],
    options_df: &DataFrame,
) -> ScriptBacktestResult {
    let source = std::fs::read_to_string(script_path)
        .unwrap_or_else(|e| panic!("Failed to read {script_path}: {e}"));

    let script = if dsl::is_trading_dsl(&source) {
        dsl::transpile(&source)
            .unwrap_or_else(|e| panic!("DSL transpile failed for {script_path}: {e}"))
    } else {
        source
    };

    let loader = OptionsTestDataLoader {
        ohlcv_df: bars_to_df(bars),
        options_df: options_df.clone(),
    };

    run_script_backtest(&script, params, &loader, None, None, None)
        .await
        .unwrap_or_else(|e| panic!("Backtest failed for {script_path}: {e}"))
}

// ===== WHEEL PUT EXPIRES OTM =====

#[tokio::test(flavor = "multi_thread")]
async fn wheel_put_expires_otm() {
    let put_exp = d(2024, 2, 16);

    let options_df = make_options_df(&[
        (dt(2024, 1, 2), put_exp, "p", 100.0, 3.00, 3.50, -0.30),
        (dt(2024, 1, 3), put_exp, "p", 100.0, 3.00, 3.50, -0.30),
    ]);

    let mut closes = BTreeMap::new();
    closes.insert(d(2024, 1, 2), 105.0);
    closes.insert(d(2024, 1, 3), 105.0);
    closes.insert(put_exp, 105.0); // above strike -> OTM

    let bars = make_bars_from_closes(&closes);
    let params = wheel_params();

    let result = run_options_strategy(
        "scripts/strategies/wheel.trading",
        &params,
        &bars,
        &options_df,
    )
    .await;

    assert!(
        result.result.trade_count > 0,
        "Should produce trades. Warnings: {:?}",
        result.result.warnings,
    );

    assert!(
        !result.result.equity_curve.is_empty(),
        "Equity curve should not be empty",
    );

    eprintln!(
        "Wheel put OTM: produced {} trade(s), {} equity points",
        result.result.trade_count,
        result.result.equity_curve.len(),
    );
}

// ===== WHEEL FULL CYCLE =====

#[tokio::test(flavor = "multi_thread")]
async fn wheel_full_cycle() {
    let put_exp = d(2024, 2, 16);
    let call_exp = d(2024, 3, 15);

    let options_df = make_options_df(&[
        (dt(2024, 1, 2), put_exp, "p", 100.0, 3.00, 3.50, -0.30),
        (dt(2024, 1, 3), put_exp, "p", 100.0, 3.00, 3.50, -0.30),
        (dt(2024, 2, 16), call_exp, "c", 102.0, 2.00, 2.50, 0.30),
        (dt(2024, 2, 17), call_exp, "c", 102.0, 2.00, 2.50, 0.30),
        (dt(2024, 3, 15), call_exp, "c", 102.0, 0.10, 0.20, 0.02),
    ]);

    let bars = vec![
        OhlcvBar {
            datetime: dt(2024, 1, 2),
            open: 101.0,
            high: 102.0,
            low: 100.0,
            close: 101.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 3),
            open: 101.0,
            high: 102.0,
            low: 100.0,
            close: 101.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 2, 16),
            open: 98.0,
            high: 99.0,
            low: 97.0,
            close: 98.0, // below 100 strike -> put ITM -> assignment
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 2, 17),
            open: 98.0,
            high: 99.0,
            low: 97.0,
            close: 98.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 3, 15),
            open: 105.0,
            high: 106.0,
            low: 104.0,
            close: 105.0, // above 102 strike -> call ITM -> called away
            volume: 1e6,
        },
    ];

    let params = wheel_params();

    let result = run_options_strategy(
        "scripts/strategies/wheel.trading",
        &params,
        &bars,
        &options_df,
    )
    .await;

    assert!(
        result.result.trade_count >= 2,
        "Should have at least 2 trades (put assignment + stock called away), got {}. Warnings: {:?}",
        result.result.trade_count,
        result.result.warnings,
    );

    let exits: Vec<String> = result
        .result
        .trade_log
        .iter()
        .map(|t| format!("{:?}", t.exit_type))
        .collect();

    assert_eq!(
        result.result.equity_curve.len(),
        5,
        "Should have 5 equity points",
    );

    eprintln!(
        "Wheel full cycle: produced {} trade(s), exit types: {:?}",
        result.result.trade_count, exits,
    );
}
