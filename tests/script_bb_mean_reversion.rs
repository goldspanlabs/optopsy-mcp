//! Integration tests for the Bollinger Band mean reversion strategy script.

use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::*;

use optopsy_mcp::scripting::engine::{run_script_backtest, DataLoader};
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

fn bb_params() -> std::collections::HashMap<String, serde_json::Value> {
    let mut params = std::collections::HashMap::new();
    params.insert("SYMBOL".to_string(), serde_json::json!("SPY"));
    params.insert("CAPITAL".to_string(), serde_json::json!(100_000));
    params
}

/// Generate bars: 30 bars of stable prices around 100, then a massive spike
/// above the upper Bollinger Band, then a reversion back to the mean.
fn make_bb_test_bars() -> Vec<OhlcvBar> {
    let mut bars = Vec::new();

    // 30 bars of very stable prices (tight range around 100)
    // This ensures SMA(20) is well-established and std dev is small
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
    // SMA(20) ≈ 100.0, std ≈ 0.1, upper BB ≈ 100.2
    // Close at 105.0 — way above the band
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
        let close = 103.0 - (i as f64 * 0.3); // 102.7, 102.4, 102.1, 101.8, 101.5
        bars.push(OhlcvBar {
            datetime: date.and_hms_opt(0, 0, 0).unwrap(),
            open: close + 0.1,
            high: close + 0.3,
            low: close - 0.3,
            close,
            volume: 1_000_000.0,
        });
    }

    // Bar 36: price drops well below SMA(20) → take profit exit
    let exit_date = spike_date + chrono::Duration::days(6);
    bars.push(OhlcvBar {
        datetime: exit_date.and_hms_opt(0, 0, 0).unwrap(),
        open: 100.0,
        high: 100.2,
        low: 97.0,
        close: 97.0, // clearly below SMA → exit
        volume: 1_000_000.0,
    });

    bars
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Verify the script compiles and `config()` returns valid settings.
#[tokio::test]
async fn bb_script_compiles_and_configures() {
    let script_source = std::fs::read_to_string("scripts/strategies/bb_mean_reversion.rhai")
        .expect("bb_mean_reversion.rhai should exist");
    let params = bb_params();

    let mut engine = optopsy_mcp::scripting::registration::build_engine();
    let params_clone = params.clone();
    engine.register_fn(
        "extern",
        move |name: &str, default: rhai::Dynamic, _desc: &str| -> rhai::Dynamic {
            if let Some(value) = params_clone.get(name) {
                optopsy_mcp::scripting::stdlib::json_to_dynamic(value)
            } else {
                default
            }
        },
    );
    let params_clone4 = params.clone();
    engine.register_fn(
        "extern",
        move |name: &str,
              default: rhai::Dynamic,
              _desc: &str,
              _opts: rhai::Array|
              -> rhai::Dynamic {
            if let Some(value) = params_clone4.get(name) {
                optopsy_mcp::scripting::stdlib::json_to_dynamic(value)
            } else {
                default
            }
        },
    );
    let ast = engine.compile(&script_source);
    assert!(
        ast.is_ok(),
        "bb_mean_reversion.rhai should compile: {:?}",
        ast.err()
    );

    let ast = ast.unwrap();
    let mut scope = rhai::Scope::new();
    optopsy_mcp::scripting::stdlib::inject_params_map(&mut scope, &params);
    let _ = engine
        .eval_ast_with_scope::<rhai::Dynamic>(&mut scope, &ast)
        .unwrap();

    let options = rhai::CallFnOptions::new()
        .eval_ast(false)
        .rewind_scope(false);
    let config: rhai::Dynamic = engine
        .call_fn_with_options(options, &mut scope, &ast, "config", ())
        .unwrap();

    let config_map = config.cast::<rhai::Map>();
    assert_eq!(
        config_map
            .get("symbol")
            .unwrap()
            .clone()
            .into_immutable_string()
            .unwrap()
            .as_str(),
        "SPY"
    );
}

/// Run the strategy: enter on BB breakout, exit when price reverts below SMA.
#[tokio::test(flavor = "multi_thread")]
async fn bb_entry_on_breakout_exit_on_reversion() {
    let bars = make_bb_test_bars();
    let loader = TestDataLoader {
        ohlcv_df: bars_to_df(&bars),
    };

    let script_source =
        std::fs::read_to_string("scripts/strategies/bb_mean_reversion.rhai").unwrap();
    let params = bb_params();

    let result = run_script_backtest(&script_source, &params, &loader).await;
    assert!(
        result.is_ok(),
        "BB mean reversion backtest should succeed: {:?}",
        result.err()
    );

    let result = result.unwrap();

    // Should have at least 1 trade (entered on the spike bar)
    assert!(
        result.result.trade_count >= 1,
        "Expected at least 1 trade, got {}. Warnings: {:?}",
        result.result.trade_count,
        result.result.warnings,
    );

    // The trade should have been closed with take_profit (price dropped below SMA)
    let exit_types: Vec<String> = result
        .result
        .trade_log
        .iter()
        .map(|t| format!("{:?}", t.exit_type))
        .collect();

    assert!(
        exit_types
            .iter()
            .any(|t| t.contains("TakeProfit") || t.contains("take_profit")),
        "Should have a take_profit exit. Got: {exit_types:?}"
    );
}

/// Test max hold exit: price stays above SMA for 10+ bars.
#[tokio::test(flavor = "multi_thread")]
async fn bb_max_hold_exit() {
    let mut bars = Vec::new();

    // 30 bars of stable prices for indicator warmup
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

    // Bars 31-42: price stays ABOVE SMA for 12 bars (forces max_hold at day 10)
    for i in 1..=12 {
        let date = spike_date + chrono::Duration::days(i);
        let close = 103.0 + (i as f64 * 0.1); // slowly rising, stays well above SMA
        bars.push(OhlcvBar {
            datetime: date.and_hms_opt(0, 0, 0).unwrap(),
            open: close - 0.1,
            high: close + 0.3,
            low: close - 0.3,
            close,
            volume: 1_000_000.0,
        });
    }

    let loader = TestDataLoader {
        ohlcv_df: bars_to_df(&bars),
    };

    let script_source =
        std::fs::read_to_string("scripts/strategies/bb_mean_reversion.rhai").unwrap();
    let params = bb_params();

    let result = run_script_backtest(&script_source, &params, &loader).await;
    assert!(
        result.is_ok(),
        "BB max-hold backtest should succeed: {:?}",
        result.err()
    );

    let result = result.unwrap();

    assert!(
        result.result.trade_count >= 1,
        "Expected at least 1 trade, got {}. Warnings: {:?}",
        result.result.trade_count,
        result.result.warnings,
    );

    // Should exit via max_hold
    let exit_types: Vec<String> = result
        .result
        .trade_log
        .iter()
        .map(|t| format!("{:?}", t.exit_type))
        .collect();

    assert!(
        exit_types
            .iter()
            .any(|t| t.contains("MaxHold") || t.contains("max_hold")),
        "Should have a max_hold exit. Got: {exit_types:?}"
    );
}
