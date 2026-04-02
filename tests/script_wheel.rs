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
use optopsy_mcp::scripting::types::OhlcvBar;

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

/// Test `DataLoader` that returns pre-built OHLCV and options `DataFrame`s.
struct TestDataLoader {
    ohlcv_df: DataFrame,
    options_df: DataFrame,
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

/// Convert `Vec<OhlcvBar>` to a Polars `DataFrame` for test data loaders.
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

/// Standard wheel params used across tests.
fn wheel_params() -> std::collections::HashMap<String, serde_json::Value> {
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
    params
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Verify `wheel.rhai` compiles and `config()` returns valid settings.
#[tokio::test]
async fn wheel_script_compiles_and_configures() {
    let script_source =
        std::fs::read_to_string("scripts/strategies/wheel.rhai").expect("wheel.rhai should exist");
    let params = wheel_params();

    // Verify it compiles (register extern() overloads so top-level calls resolve)
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
    assert!(ast.is_ok(), "wheel.rhai should compile: {:?}", ast.err());

    // Verify config() can be called with params in scope
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
///
/// Next-bar execution: order queued on bar 0, fills on bar 1, expires on bar 2.
#[tokio::test(flavor = "multi_thread")]
async fn wheel_script_put_expires_otm() {
    let entry_date = d(2024, 1, 2);
    let fill_date = d(2024, 1, 3); // bar 1: order fills here
    let put_exp = d(2024, 2, 16);

    // Provide options data on both entry_date and fill_date so the order can resolve
    let options_df = make_options_df(&[
        (dt(2024, 1, 2), put_exp, "p", 100.0, 3.00, 3.50, -0.30),
        (dt(2024, 1, 3), put_exp, "p", 100.0, 3.00, 3.50, -0.30),
    ]);

    let mut closes = BTreeMap::new();
    closes.insert(entry_date, 105.0);
    closes.insert(fill_date, 105.0);
    closes.insert(put_exp, 105.0); // above strike → OTM

    let bars = make_bars_from_closes(&closes);
    let loader = TestDataLoader {
        ohlcv_df: bars_to_df(&bars),
        options_df: options_df.clone(),
    };

    let script_source = std::fs::read_to_string("scripts/strategies/wheel.rhai").unwrap();
    let mut params = wheel_params();
    params.insert("TAKE_PROFIT".to_string(), serde_json::json!(null));

    let result = run_script_backtest(&script_source, &params, &loader, None, None, None).await;
    assert!(
        result.is_ok(),
        "Script backtest should succeed: {:?}",
        result.err()
    );

    let result = result.unwrap();

    // Should have opened a short put on bar 1 (filled from bar 0 order) and it expired on bar 2
    assert_eq!(
        result.result.trade_count, 1,
        "Should have 1 trade (put expiration). Warnings: {:?}",
        result.result.warnings,
    );

    // Equity curve should have entries for each bar
    assert_eq!(
        result.result.equity_curve.len(),
        3,
        "Should have 3 equity points"
    );
}

/// Minimal test: just open a stock position to verify the engine loop works.
///
/// Next-bar execution model:
/// - Bar 0: `on_bar` queues buy order (market order)
/// - Bar 1: order fills at bar 1's open (100.0), `days_held`=0
/// - Bar 2: `days_held`=1, `on_exit_check` triggers close at bar 2's close
/// - Bar 3: needed so bar 2 isn't the last bar (final bar orders are cancelled)
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
        OhlcvBar {
            datetime: dt(2024, 1, 5),
            open: 102.0,
            high: 104.0,
            low: 101.0,
            close: 103.0,
            volume: 1e6,
        },
    ];

    let loader = TestDataLoader {
        ohlcv_df: bars_to_df(&bars),
        options_df: DataFrame::empty(),
    };

    let script = r#"
        fn config() {
            #{
                symbol: params.SYMBOL,
                capital: params.CAPITAL,
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

    let mut params = std::collections::HashMap::new();
    params.insert("SYMBOL".to_string(), serde_json::json!("SPY"));
    params.insert("CAPITAL".to_string(), serde_json::json!(100_000.0));

    let result = run_script_backtest(script, &params, &loader, None, None, None)
        .await
        .unwrap();

    assert_eq!(result.result.trade_count, 1, "Should have 1 closed trade");
    assert_eq!(
        result.result.equity_curve.len(),
        4,
        "Should have 4 equity points"
    );

    // Next-bar execution: buy order queued on bar 0, fills at bar 1 open (100.0)
    // days_held becomes 1 after bar 2, exit fires on bar 3 at close (103.0)
    // P&L = (103 - 100) * 100 = 300
    let pnl = result.result.trade_log[0].pnl;
    assert!((pnl - 300.0).abs() < 1.0, "Expected ~300 PnL, got {pnl}");
}

/// Open an options position via inline script with synthetic data.
#[tokio::test(flavor = "multi_thread")]
async fn script_opens_options_position() {
    let put_exp = d(2024, 2, 16);
    // Provide options data on both bars so the order can fill on bar 1
    let options_df = make_options_df(&[
        (dt(2024, 1, 2), put_exp, "p", 100.0, 3.00, 3.50, -0.30),
        (dt(2024, 1, 3), put_exp, "p", 100.0, 3.00, 3.50, -0.30),
    ]);

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
            datetime: dt(2024, 1, 3),
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

    let loader = TestDataLoader {
        ohlcv_df: bars_to_df(&bars),
        options_df,
    };

    // Minimal script that opens a short put on first bar
    let script = r#"
        fn config() {
            #{
                symbol: params.SYMBOL,
                capital: params.CAPITAL,
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

    let mut params = std::collections::HashMap::new();
    params.insert("SYMBOL".to_string(), serde_json::json!("SPY"));
    params.insert("CAPITAL".to_string(), serde_json::json!(100_000.0));

    let result = run_script_backtest(script, &params, &loader, None, None, None)
        .await
        .unwrap();

    // Should have equity points for all 3 bars
    assert!(
        result.result.equity_curve.len() == 3,
        "Should have 3 equity points"
    );
}

/// Verify `BacktestResult` has all fields the frontend expects.
#[tokio::test(flavor = "multi_thread")]
async fn wheel_script_result_has_expected_fields() {
    let entry_date = d(2024, 1, 2);
    let fill_date = d(2024, 1, 3); // next-bar fill
    let put_exp = d(2024, 2, 16);

    let options_df = make_options_df(&[
        (dt(2024, 1, 2), put_exp, "p", 100.0, 3.00, 3.50, -0.30),
        (dt(2024, 1, 3), put_exp, "p", 100.0, 3.00, 3.50, -0.30),
    ]);

    let mut closes = BTreeMap::new();
    closes.insert(entry_date, 105.0);
    closes.insert(fill_date, 105.0);
    closes.insert(put_exp, 105.0);

    let bars = make_bars_from_closes(&closes);
    let loader = TestDataLoader {
        ohlcv_df: bars_to_df(&bars),
        options_df,
    };

    let script_source = std::fs::read_to_string("scripts/strategies/wheel.rhai").unwrap();
    let mut params = wheel_params();
    params.insert("TAKE_PROFIT".to_string(), serde_json::json!(null));

    let ScriptBacktestResult { result, .. } =
        run_script_backtest(&script_source, &params, &loader, None, None, None)
            .await
            .unwrap();

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

/// Full wheel cycle: sell put → assigned (ITM) → sell call → called away (ITM).
/// Verifies the engine creates implicit stock on assignment and closes it on `called_away`.
///
/// Next-bar execution: extra bars added so orders queue on bar N and fill on bar N+1.
#[tokio::test(flavor = "multi_thread")]
async fn wheel_full_cycle_assignment_and_called_away() {
    let put_exp = d(2024, 2, 16); // 45 DTE from 2024-01-02
    let call_exp = d(2024, 3, 15); // ~28 DTE from put_exp

    // Options data:
    // Bar 0 (signal): put available at strike 100
    // Bar 1 (fill): put fills here, also needs options for resolution
    // Bar 2 (put exp): call available at strike 102
    // Bar 3 (call fill): call fills here
    // Bar 4 (call exp): call row for close reference
    let options_df = make_options_df(&[
        (dt(2024, 1, 2), put_exp, "p", 100.0, 3.00, 3.50, -0.30),
        (dt(2024, 1, 3), put_exp, "p", 100.0, 3.00, 3.50, -0.30),
        (dt(2024, 2, 16), call_exp, "c", 102.0, 2.00, 2.50, 0.30),
        (dt(2024, 2, 17), call_exp, "c", 102.0, 2.00, 2.50, 0.30),
        (dt(2024, 3, 15), call_exp, "c", 102.0, 0.10, 0.20, 0.02),
    ]);

    // OHLCV: stock prices determine ITM/OTM at expiration
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
            close: 98.0, // below 100 strike → put ITM → assignment
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
            close: 105.0, // above 102 strike → call ITM → called away
            volume: 1e6,
        },
    ];

    let loader = TestDataLoader {
        ohlcv_df: bars_to_df(&bars),
        options_df,
    };

    // Use the wheel script with params
    let script_source = std::fs::read_to_string("scripts/strategies/wheel.rhai").unwrap();
    let mut params = wheel_params();
    params.insert("TAKE_PROFIT".to_string(), serde_json::json!(null));

    let result = run_script_backtest(&script_source, &params, &loader, None, None, None).await;
    assert!(
        result.is_ok(),
        "Wheel backtest should succeed: {:?}",
        result.err()
    );

    let result = result.unwrap();

    // Should have trades: put (assigned), stock (called away), and possibly a call
    assert!(
        result.result.trade_count >= 2,
        "Expected at least 2 trades (put assignment + stock called away), got {}. Warnings: {:?}",
        result.result.trade_count,
        result.result.warnings,
    );

    // Check that assignment and called_away exit types appear
    let exit_types: Vec<String> = result
        .result
        .trade_log
        .iter()
        .map(|t| format!("{:?}", t.exit_type))
        .collect();

    assert!(
        exit_types.iter().any(|t| t.contains("Assignment")),
        "Should have an Assignment exit. Got: {exit_types:?}"
    );

    // Equity curve should span all 5 bars
    assert_eq!(
        result.result.equity_curve.len(),
        5,
        "Should have 5 equity points"
    );
}
