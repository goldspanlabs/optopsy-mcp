//! Integration tests for the wheel strategy Rhai script.
//!
//! Verifies that `scripts/strategies/wheel.rhai` produces correct backtest
//! results using synthetic options data, and that the output format matches
//! what the frontend expects.

use std::collections::BTreeMap;

use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::*;

use optopsy_mcp::data::parquet::DATETIME_COL;
use optopsy_mcp::scripting::engine::{run_script_backtest, DataLoader, ScriptBacktestResult};
use optopsy_mcp::scripting::stdlib;
use optopsy_mcp::scripting::types::{Interval, OhlcvBar};

// ---------------------------------------------------------------------------
// Helpers
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

/// Test `DataLoader` that returns pre-built OHLCV bars and an options `DataFrame`.
struct TestDataLoader {
    bars: Vec<OhlcvBar>,
    options_df: DataFrame,
}

#[async_trait::async_trait]
impl DataLoader for TestDataLoader {
    async fn load_ohlcv(
        &self,
        _symbol: &str,
        _start: Option<NaiveDate>,
        _end: Option<NaiveDate>,
        _interval: Interval,
    ) -> Result<Vec<OhlcvBar>> {
        Ok(self.bars.clone())
    }

    async fn load_options(
        &self,
        _symbol: &str,
        _start: Option<NaiveDate>,
        _end: Option<NaiveDate>,
    ) -> Result<DataFrame> {
        Ok(self.options_df.clone())
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Verify `wheel.rhai` compiles and `config()` returns valid settings.
#[tokio::test]
async fn wheel_script_compiles_and_configures() {
    let script_source =
        std::fs::read_to_string("scripts/strategies/wheel.rhai").expect("wheel.rhai should exist");

    let mut params = std::collections::HashMap::new();
    params.insert("SYMBOL".to_string(), serde_json::json!("SPY"));
    params.insert("CAPITAL".to_string(), serde_json::json!(100_000));
    params.insert("PUT_DELTA".to_string(), serde_json::json!(0.30));
    params.insert("PUT_DTE".to_string(), serde_json::json!(45));
    params.insert("CALL_DELTA".to_string(), serde_json::json!(0.30));
    params.insert("CALL_DTE".to_string(), serde_json::json!(30));
    params.insert("EXIT_DTE".to_string(), serde_json::json!(5));
    params.insert("SLIPPAGE".to_string(), serde_json::json!("mid"));
    params.insert("MULTIPLIER".to_string(), serde_json::json!(100));
    params.insert("STOP_LOSS".to_string(), serde_json::json!(null));
    params.insert("TAKE_PROFIT".to_string(), serde_json::json!(0.50));

    let full_source = stdlib::inject_as_const(&script_source, &params);

    // Verify it compiles
    let engine = optopsy_mcp::scripting::registration::build_engine();
    let ast = engine.compile(&full_source);
    assert!(ast.is_ok(), "wheel.rhai should compile: {:?}", ast.err());

    // Verify config() can be called
    let ast = ast.unwrap();
    let mut scope = rhai::Scope::new();
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
    let capital_val = config_map.get("capital").unwrap();
    let capital = capital_val
        .as_float()
        .or_else(|_| capital_val.as_int().map(|i| i as f64))
        .unwrap();
    assert!(
        (capital - 100_000.0).abs() < f64::EPSILON,
        "Expected 100000.0, got {capital}"
    );
}

/// Run wheel.rhai with synthetic data: put expires OTM (premium collected).
#[tokio::test(flavor = "multi_thread")]
async fn wheel_script_put_expires_otm() {
    let entry_date = d(2024, 1, 2);
    let put_exp = d(2024, 2, 16);

    let options_df = make_options_df(&[(dt(2024, 1, 2), put_exp, "p", 100.0, 3.00, 3.50, -0.30)]);

    let mut closes = BTreeMap::new();
    closes.insert(entry_date, 105.0);
    closes.insert(put_exp, 105.0); // above strike → OTM

    let bars = make_bars_from_closes(&closes);
    let loader = TestDataLoader {
        bars,
        options_df: options_df.clone(),
    };

    let script_source = std::fs::read_to_string("scripts/strategies/wheel.rhai").unwrap();
    let mut params = std::collections::HashMap::new();
    params.insert("SYMBOL".to_string(), serde_json::json!("SPY"));
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

    let full_source = stdlib::inject_as_const(&script_source, &params);
    let result = run_script_backtest(&full_source, &loader).await;
    assert!(
        result.is_ok(),
        "Script backtest should succeed: {:?}",
        result.err()
    );

    let result = result.unwrap();

    // Should have opened a short put on bar 0 and it expired on bar 1
    assert_eq!(
        result.result.trade_count, 1,
        "Should have 1 trade (put expiration). Warnings: {:?}",
        result.result.warnings,
    );
    assert_eq!(result.result.equity_curve.len(), 2);

    // Equity curve should have entries for each bar
    assert_eq!(
        result.result.equity_curve.len(),
        2,
        "Should have 2 equity points"
    );
}

/// Minimal test: just open a stock position to verify the engine loop works.
#[tokio::test(flavor = "multi_thread")]
async fn wheel_script_stock_only_smoke_test() {
    let bars = vec![
        OhlcvBar {
            datetime: dt(2024, 1, 2),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 3),
            open: 100.0,
            high: 102.0,
            low: 99.0,
            close: 101.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 4),
            open: 101.0,
            high: 103.0,
            low: 100.0,
            close: 102.0,
            volume: 1e6,
        },
    ];

    let loader = TestDataLoader {
        bars,
        options_df: DataFrame::empty(),
    };

    let script = r#"
        const SYMBOL = "SPY";
        const CAPITAL = 100000.0;

        fn config() {
            #{
                symbol: SYMBOL,
                capital: CAPITAL,
                interval: "daily",
                data: #{ ohlcv: true, options: false },
                engine: #{ slippage: "mid" },
            }
        }

        fn on_bar(ctx) {
            if !ctx.has_positions() {
                return [#{ action: "open_stock", side: "long", qty: 100 }];
            }
            []
        }

        fn on_exit_check(ctx, pos) {
            if pos.days_held >= 1 {
                return #{ action: "close", reason: "max_hold" };
            }
            #{ action: "hold" }
        }
    "#;

    let result = run_script_backtest(script, &loader).await.unwrap();

    assert_eq!(result.result.trade_count, 1, "Should have 1 closed trade");
    assert_eq!(
        result.result.equity_curve.len(),
        3,
        "Should have 3 equity points"
    );

    // P&L: bought at 100 on bar 0, closed at 102 on bar 2 (days_held becomes 1 after bar 1)
    // Exit fires on bar 2 when days_held=1 (updated at end of bar 1)
    // P&L = (102 - 100) * 100 = 200
    let pnl = result.result.trade_log[0].pnl;
    assert!((pnl - 200.0).abs() < 1.0, "Expected ~200 PnL, got {pnl}");
}

/// Open an options position via inline script with synthetic data.
#[tokio::test(flavor = "multi_thread")]
async fn script_opens_options_position() {
    let put_exp = d(2024, 2, 16);
    let options_df = make_options_df(&[(dt(2024, 1, 2), put_exp, "p", 100.0, 3.00, 3.50, -0.30)]);

    let bars = vec![
        OhlcvBar {
            datetime: dt(2024, 1, 2),
            open: 105.0,
            high: 106.0,
            low: 104.0,
            close: 105.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 2, 16),
            open: 105.0,
            high: 106.0,
            low: 104.0,
            close: 105.0,
            volume: 1e6,
        },
    ];

    let loader = TestDataLoader { bars, options_df };

    // Minimal script that opens a short put on first bar
    let script = r#"
        const SYMBOL = "SPY";
        const CAPITAL = 100000.0;

        fn config() {
            #{
                symbol: SYMBOL,
                capital: CAPITAL,
                interval: "daily",
                data: #{ ohlcv: true, options: true },
                engine: #{ slippage: "mid" },
            }
        }

        fn on_bar(ctx) {
            if ctx.has_positions() { return []; }
            let spread = ctx.build_strategy([
                #{ side: "short", option_type: "put", delta: 0.30, dte: 45 },
            ]);
            if spread == () { return []; }
            [#{ action: "open_spread", spread: spread }]
        }
    "#;

    let result = run_script_backtest(script, &loader).await.unwrap();

    // Should have opened a position on bar 0
    assert!(
        result.result.equity_curve.len() == 2,
        "Should have 2 equity points"
    );
}

/// Verify `BacktestResult` has all fields the frontend expects.
#[tokio::test(flavor = "multi_thread")]
async fn wheel_script_result_has_expected_fields() {
    let entry_date = d(2024, 1, 2);
    let put_exp = d(2024, 2, 16);

    let options_df = make_options_df(&[(dt(2024, 1, 2), put_exp, "p", 100.0, 3.00, 3.50, -0.30)]);

    let mut closes = BTreeMap::new();
    closes.insert(entry_date, 105.0);
    closes.insert(put_exp, 105.0);

    let bars = make_bars_from_closes(&closes);
    let loader = TestDataLoader { bars, options_df };

    let script_source = std::fs::read_to_string("scripts/strategies/wheel.rhai").unwrap();
    let mut params = std::collections::HashMap::new();
    params.insert("SYMBOL".to_string(), serde_json::json!("SPY"));
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

    let full_source = stdlib::inject_as_const(&script_source, &params);
    let ScriptBacktestResult { result, .. } =
        run_script_backtest(&full_source, &loader).await.unwrap();

    // Verify all fields the FE needs are present and serializable
    let json = serde_json::to_value(&result.metrics).unwrap();
    assert!(json.get("sharpe").is_some(), "metrics should have sharpe");
    assert!(json.get("sortino").is_some(), "metrics should have sortino");
    assert!(
        json.get("max_drawdown").is_some(),
        "metrics should have max_drawdown"
    );
    assert!(
        json.get("win_rate").is_some(),
        "metrics should have win_rate"
    );
    assert!(json.get("cagr").is_some(), "metrics should have cagr");

    // Equity curve points should have datetime and equity
    for point in &result.equity_curve {
        assert!(point.equity > 0.0, "Equity should be positive");
    }

    // Trade log entries should have required fields
    for trade in &result.trade_log {
        assert!(trade.trade_id > 0);
        assert!(!trade.legs.is_empty(), "Trade should have legs");
    }
}
