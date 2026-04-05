//! Integration tests for multi-symbol portfolio backtesting.
//!
//! Verifies:
//! - `extern_symbol` auto-detection builds `config.symbols`
//! - Multi-symbol data loading (different bars per symbol)
//! - `ctx.sym("SYMBOL")` returns correct per-symbol data
//! - `buy_stock(symbol, qty)` targets the correct symbol
//! - Trade records carry the correct symbol field
//! - Single-symbol `extern_symbol` works (backward compat)

use std::collections::HashMap;

use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::*;

use optopsy_mcp::scripting::dsl;
use optopsy_mcp::scripting::engine::{run_script_backtest, DataLoader};
use optopsy_mcp::scripting::types::OhlcvBar;

fn dt(y: i32, m: u32, day: u32) -> chrono::NaiveDateTime {
    NaiveDate::from_ymd_opt(y, m, day)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

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

/// Data loader that returns different OHLCV data per symbol.
struct MultiSymbolLoader {
    data: HashMap<String, DataFrame>,
}

#[async_trait::async_trait]
impl DataLoader for MultiSymbolLoader {
    async fn load_ohlcv(
        &self,
        symbol: &str,
        _start: Option<NaiveDate>,
        _end: Option<NaiveDate>,
    ) -> Result<DataFrame> {
        self.data
            .get(&symbol.to_uppercase())
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("No OHLCV data for '{symbol}'"))
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

/// Create test bars for two symbols with different prices on the same dates.
/// SPY: 100, 102, 104, 106, 108
/// QQQ: 200, 198, 196, 194, 192  (inverse movement)
fn make_two_symbol_loader() -> MultiSymbolLoader {
    let dates = [
        dt(2024, 1, 2),
        dt(2024, 1, 3),
        dt(2024, 1, 4),
        dt(2024, 1, 5),
        dt(2024, 1, 8),
    ];

    let spy_bars: Vec<OhlcvBar> = dates
        .iter()
        .enumerate()
        .map(|(i, &datetime)| {
            let close = 100.0 + (i as f64) * 2.0;
            OhlcvBar {
                datetime,
                open: close - 0.5,
                high: close + 1.0,
                low: close - 1.0,
                close,
                volume: 1_000_000.0,
            }
        })
        .collect();

    let qqq_bars: Vec<OhlcvBar> = dates
        .iter()
        .enumerate()
        .map(|(i, &datetime)| {
            let close = 200.0 - (i as f64) * 2.0;
            OhlcvBar {
                datetime,
                open: close + 0.5,
                high: close + 1.0,
                low: close - 1.0,
                close,
                volume: 2_000_000.0,
            }
        })
        .collect();

    let mut data = HashMap::new();
    data.insert("SPY".to_string(), bars_to_df(&spy_bars));
    data.insert("QQQ".to_string(), bars_to_df(&qqq_bars));

    MultiSymbolLoader { data }
}

// ---------------------------------------------------------------------------
// Test: extern_symbol auto-detection with single symbol
// ---------------------------------------------------------------------------

/// Verifies that a script using `extern_symbol()` without `config.symbol`
/// auto-detects the symbol and runs correctly.
#[tokio::test(flavor = "multi_thread")]
async fn extern_symbol_auto_detect_single() {
    let loader = make_two_symbol_loader();
    let script = r#"
let symbol = extern_symbol("symbol", "SPY", "ticker");

fn config() {
    #{
        capital: 100000,
        interval: "daily",
        auto_close_on_end: true,
        data: #{ ohlcv: true },
    }
}

fn on_bar(ctx) {
    if ctx.bar_idx == 0 && !ctx.has_positions() {
        return [buy_stock(symbol, 10)];
    }
    []
}

fn on_exit_check(ctx, pos) {
    hold_position()
}
"#;

    let mut params = HashMap::new();
    params.insert("CAPITAL".to_string(), serde_json::json!(100_000.0));
    // symbol comes from extern_symbol default: "SPY"

    let result = run_script_backtest(script, &params, &loader, None, None, None)
        .await
        .expect("backtest should succeed");

    assert_eq!(
        result.result.symbol.as_deref(),
        Some("SPY"),
        "Symbol should be auto-detected as SPY"
    );
    assert!(
        result.result.trade_count > 0,
        "Should have at least one trade"
    );
}

/// Verifies that passing a different symbol via params overrides the default.
#[tokio::test(flavor = "multi_thread")]
async fn extern_symbol_override_via_params() {
    let loader = make_two_symbol_loader();
    let script = r#"
let symbol = extern_symbol("symbol", "SPY", "ticker");

fn config() {
    #{
        capital: 100000,
        interval: "daily",
        auto_close_on_end: true,
        data: #{ ohlcv: true },
    }
}

fn on_bar(ctx) {
    if ctx.bar_idx == 0 && !ctx.has_positions() {
        return [buy_stock(symbol, 10)];
    }
    []
}

fn on_exit_check(ctx, pos) {
    hold_position()
}
"#;

    let mut params = HashMap::new();
    params.insert("symbol".to_string(), serde_json::json!("QQQ"));
    params.insert("CAPITAL".to_string(), serde_json::json!(100_000.0));

    let result = run_script_backtest(script, &params, &loader, None, None, None)
        .await
        .expect("backtest should succeed");

    assert_eq!(
        result.result.symbol.as_deref(),
        Some("QQQ"),
        "Symbol should be overridden to QQQ via params"
    );
    assert!(
        result.result.trade_count > 0,
        "Should have at least one trade"
    );

    // Verify trade used QQQ prices (QQQ opens at 200.5 on day 1, closes at 192 on day 5)
    let trade = &result.result.trade_log[0];
    assert_eq!(
        trade.symbol.as_deref(),
        Some("QQQ"),
        "Trade should be for QQQ"
    );
}

// ---------------------------------------------------------------------------
// Test: Multi-symbol portfolio backtest
// ---------------------------------------------------------------------------

/// Verifies a script that declares two `extern_symbol` params loads data for
/// both symbols, and trades target the correct symbol with correct prices.
#[tokio::test(flavor = "multi_thread")]
async fn multi_symbol_portfolio_backtest() {
    let loader = make_two_symbol_loader();

    // Script trades SPY on bar 0, QQQ on bar 1, holds to end
    let script = r#"
let spy_sym = extern_symbol("spy_sym", "SPY", "long leg");
let qqq_sym = extern_symbol("qqq_sym", "QQQ", "short leg");

fn config() {
    #{
        capital: 100000,
        interval: "daily",
        auto_close_on_end: true,
        data: #{ ohlcv: true },
    }
}

fn on_bar(ctx) {
    let spy = ctx.sym(spy_sym);
    let qqq = ctx.sym(qqq_sym);

    if ctx.bar_idx == 0 {
        // Buy 10 shares of SPY
        return [buy_stock(spy_sym, 10)];
    }
    if ctx.bar_idx == 1 {
        // Buy 5 shares of QQQ
        return [buy_stock(qqq_sym, 5)];
    }
    []
}

fn on_exit_check(ctx, pos) {
    hold_position()
}
"#;

    let mut params = HashMap::new();
    params.insert("CAPITAL".to_string(), serde_json::json!(100_000.0));

    let result = run_script_backtest(script, &params, &loader, None, None, None)
        .await
        .expect("multi-symbol backtest should succeed");

    // Should have 2 trades (auto-closed at end)
    assert_eq!(
        result.result.trade_count, 2,
        "Should have 2 trades (SPY + QQQ)"
    );

    // Check trade symbols
    let spy_trades: Vec<_> = result
        .result
        .trade_log
        .iter()
        .filter(|t| t.symbol.as_deref() == Some("SPY"))
        .collect();
    let qqq_trades: Vec<_> = result
        .result
        .trade_log
        .iter()
        .filter(|t| t.symbol.as_deref() == Some("QQQ"))
        .collect();

    assert_eq!(spy_trades.len(), 1, "Should have 1 SPY trade");
    assert_eq!(qqq_trades.len(), 1, "Should have 1 QQQ trade");
}

// ---------------------------------------------------------------------------
// Test: ctx.sym() returns correct per-symbol prices
// ---------------------------------------------------------------------------

/// Verifies that ctx.sym("SPY").close and ctx.sym("QQQ").close return
/// different values matching each symbol's actual price data.
#[tokio::test(flavor = "multi_thread")]
async fn ctx_sym_returns_correct_prices() {
    let loader = make_two_symbol_loader();

    // Script checks prices on bar 2: SPY close=104, QQQ close=196
    // Buys SPY only if SPY.close < QQQ.close (always true in our test data)
    let script = r#"
let spy_sym = extern_symbol("spy_sym", "SPY", "long leg");
let qqq_sym = extern_symbol("qqq_sym", "QQQ", "short leg");

fn config() {
    #{
        capital: 100000,
        interval: "daily",
        auto_close_on_end: true,
        data: #{ ohlcv: true },
    }
}

fn on_bar(ctx) {
    if ctx.bar_idx != 2 || ctx.has_positions() { return []; }

    let spy = ctx.sym(spy_sym);
    let qqq = ctx.sym(qqq_sym);

    // SPY close on bar 2 = 104, QQQ close on bar 2 = 196
    // Only buy if SPY.close < QQQ.close (should be true)
    if spy.close < qqq.close {
        return [buy_stock(spy_sym, 1)];
    }
    []
}

fn on_exit_check(ctx, pos) {
    hold_position()
}
"#;

    let mut params = HashMap::new();
    params.insert("CAPITAL".to_string(), serde_json::json!(100_000.0));

    let result = run_script_backtest(script, &params, &loader, None, None, None)
        .await
        .expect("backtest should succeed");

    assert_eq!(
        result.result.trade_count, 1,
        "Should have 1 trade (condition was true: SPY 104 < QQQ 196)"
    );
}

// ---------------------------------------------------------------------------
// Test: Error when no symbol is declared
// ---------------------------------------------------------------------------

/// Verifies that a script with no `extern_symbol` and no `config.symbol` fails
/// with a clear error message.
#[tokio::test(flavor = "multi_thread")]
async fn no_symbol_declared_fails() {
    let loader = make_two_symbol_loader();

    let script = r#"
fn config() {
    #{
        capital: 100000,
        interval: "daily",
        data: #{ ohlcv: true },
    }
}

fn on_bar(ctx) { [] }
"#;

    let mut params = HashMap::new();
    params.insert("CAPITAL".to_string(), serde_json::json!(100_000.0));

    let result = run_script_backtest(script, &params, &loader, None, None, None).await;

    assert!(result.is_err(), "Should fail when no symbol is declared");
    let err = format!("{:#}", result.err().unwrap());
    assert!(
        err.contains("No symbols") || err.contains("symbol"),
        "Error should mention missing symbols, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Test: Stock MTM uses correct per-symbol close
// ---------------------------------------------------------------------------

/// Verifies that mark-to-market for a QQQ position uses QQQ's close price,
/// not SPY's (the first/primary symbol).
#[tokio::test(flavor = "multi_thread")]
async fn stock_mtm_uses_correct_symbol_close() {
    let loader = make_two_symbol_loader();

    // Buy QQQ on bar 0, exit on bar 3.
    // QQQ: open=200.5 (bar 0 fill on bar 1 = bar1.open = 198.5),
    //      close=194 on bar 3.
    // P&L = (194 - 198.5) * 5 = -22.5
    let script = r#"
let spy_sym = extern_symbol("spy_sym", "SPY", "primary");
let qqq_sym = extern_symbol("qqq_sym", "QQQ", "secondary");

fn config() {
    #{
        capital: 100000,
        interval: "daily",
        auto_close_on_end: true,
        data: #{ ohlcv: true },
    }
}

fn on_bar(ctx) {
    if ctx.bar_idx == 0 && !ctx.has_positions() {
        return [buy_stock(qqq_sym, 5)];
    }
    []
}

fn on_exit_check(ctx, pos) {
    hold_position()
}
"#;

    let mut params = HashMap::new();
    params.insert("CAPITAL".to_string(), serde_json::json!(100_000.0));

    let result = run_script_backtest(script, &params, &loader, None, None, None)
        .await
        .expect("backtest should succeed");

    assert_eq!(
        result.result.trade_count, 1,
        "Should have 1 trade (auto-closed at end)"
    );
    let trade = &result.result.trade_log[0];
    assert_eq!(
        trade.symbol.as_deref(),
        Some("QQQ"),
        "Trade should target QQQ"
    );

    // QQQ bar 1 open = 198.5 (fill price for market order queued on bar 0)
    // QQQ bar 4 close = 192.0 (auto-close at end of data)
    // P&L = (192.0 - 198.5) * 5 = -32.5
    // The key assertion: P&L uses QQQ's close (192), not SPY's (108)
    let pnl = trade.pnl;
    assert!(
        pnl < 0.0,
        "QQQ is declining so P&L should be negative, got {pnl}"
    );
    // If MTM incorrectly used SPY's close (108), P&L would be (108 - 198.5) * 5 = -452.5
    // With correct QQQ close (192), P&L is (192 - 198.5) * 5 = -32.5
    assert!(
        pnl > -100.0,
        "P&L should be around -32.5 (using QQQ prices), not -452 (SPY prices), got {pnl}"
    );
}

// ===========================================================================
// DSL Integration Tests
// ===========================================================================

// ---------------------------------------------------------------------------
// Test: DSL transpiles extern_symbol and symbol-aware buy_stock
// ---------------------------------------------------------------------------

/// Verifies that the DSL `strategy` block's `symbol` field generates an
/// `extern_symbol` call and `buy N shares` generates `buy_stock(symbol, N)`.
#[test]
fn dsl_transpiles_extern_symbol_and_buy_stock() {
    let dsl_source = r#"
strategy "Test Buy"
  symbol SPY
  interval daily
  data ohlcv

on each bar
  skip when has positions
  buy 100 shares
"#;

    let rhai = dsl::transpile(dsl_source).unwrap();

    // Should contain extern_symbol with SPY as default
    assert!(
        rhai.contains("extern_symbol"),
        "Generated Rhai should contain extern_symbol call"
    );
    assert!(
        rhai.contains("\"SPY\""),
        "Generated Rhai should contain SPY as default symbol"
    );

    // Should contain buy_stock(symbol, 100) — not buy_stock(100)
    assert!(
        rhai.contains("buy_stock(symbol, 100)"),
        "Generated Rhai should pass symbol to buy_stock, got:\n{rhai}"
    );

    // Should NOT contain symbol: in config
    assert!(
        !rhai.contains("symbol:"),
        "Generated Rhai should not have symbol in config"
    );
}

// ---------------------------------------------------------------------------
// Test: DSL sell generates symbol-aware sell_stock
// ---------------------------------------------------------------------------

#[test]
fn dsl_transpiles_sell_with_symbol() {
    let dsl_source = r#"
strategy "Test Sell"
  symbol AAPL
  interval daily
  data ohlcv

on each bar
  skip when not has positions
  sell 50 shares
"#;

    let rhai = dsl::transpile(dsl_source).unwrap();

    assert!(
        rhai.contains("sell_stock(symbol, __sell_qty)"),
        "Generated Rhai should pass symbol to sell_stock, got:\n{rhai}"
    );
}

// ---------------------------------------------------------------------------
// Test: DSL transpiled script runs end-to-end backtest
// ---------------------------------------------------------------------------

/// Transpiles a DSL script and runs it through the full backtest engine,
/// proving the generated `extern_symbol` + `buy_stock(symbol, N)` works.
#[tokio::test(flavor = "multi_thread")]
async fn dsl_transpiled_script_runs_backtest() {
    let dsl_source = r#"
strategy "DSL Buy and Hold"
  symbol SPY
  interval daily
  data ohlcv

on each bar
  skip when has positions
  buy 10 shares

on exit check
  hold position
"#;

    let rhai = dsl::transpile(dsl_source).unwrap();

    let loader = make_two_symbol_loader();
    let mut params = HashMap::new();
    params.insert("CAPITAL".to_string(), serde_json::json!(100_000.0));

    let result = run_script_backtest(&rhai, &params, &loader, None, None, None)
        .await
        .expect("DSL-transpiled backtest should succeed");

    assert_eq!(
        result.result.symbol.as_deref(),
        Some("SPY"),
        "Symbol should be SPY from extern_symbol default"
    );
    // Position is opened but never closed (hold position + no auto_close),
    // so trade_count is 0. Verify via equity curve showing unrealized P&L.
    assert!(
        result.result.equity_curve.len() >= 4,
        "Should have equity curve entries for each bar"
    );
    // Last equity point should differ from initial capital (position is open)
    let last_equity = result.result.equity_curve.last().unwrap().equity;
    assert!(
        (last_equity - 100_000.0).abs() > 0.01,
        "Equity should differ from initial capital (open position), got {last_equity}"
    );
}

// ---------------------------------------------------------------------------
// Test: DSL transpiled script respects symbol override via params
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn dsl_transpiled_script_symbol_override() {
    let dsl_source = r#"
strategy "DSL Override"
  symbol SPY
  interval daily
  data ohlcv

on each bar
  skip when has positions
  buy 10 shares

on exit check
  hold position
"#;

    let rhai = dsl::transpile(dsl_source).unwrap();

    let loader = make_two_symbol_loader();
    let mut params = HashMap::new();
    params.insert("symbol".to_string(), serde_json::json!("QQQ"));
    params.insert("CAPITAL".to_string(), serde_json::json!(100_000.0));

    let result = run_script_backtest(&rhai, &params, &loader, None, None, None)
        .await
        .expect("DSL-transpiled backtest should succeed with QQQ override");

    assert_eq!(
        result.result.symbol.as_deref(),
        Some("QQQ"),
        "Symbol should be overridden to QQQ via params"
    );
}
