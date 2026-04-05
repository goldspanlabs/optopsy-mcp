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

use optopsy_mcp::data::parquet::DATETIME_COL;
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

    // Verify trade symbol and that QQQ prices were used (not SPY's).
    // QQQ is declining (200→192), so auto-closed P&L should be negative.
    // SPY is rising (100→108), so if SPY prices were used, P&L would be positive.
    let trade = &result.result.trade_log[0];
    assert_eq!(
        trade.symbol.as_deref(),
        Some("QQQ"),
        "Trade should be for QQQ"
    );
    assert!(
        trade.pnl < 0.0,
        "QQQ is declining, P&L should be negative (proves QQQ prices used), got {}",
        trade.pnl
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

    // Verify P&L uses correct per-symbol prices (not primary symbol for both).
    // SPY: rising (100→108). Buy queued bar 0, fill bar 1 open (101.5), auto-close bar 4 close (108).
    // SPY P&L = (108 - 101.5) * 10 = 65.0
    let spy_pnl = spy_trades[0].pnl;
    assert!(
        spy_pnl > 0.0,
        "SPY is rising, P&L should be positive, got {spy_pnl}"
    );

    // QQQ: falling (200→192). Buy queued bar 1, fill bar 2 open (196.5), auto-close bar 4 close (192).
    // QQQ P&L = (192 - 196.5) * 5 = -22.5
    let qqq_pnl = qqq_trades[0].pnl;
    assert!(
        qqq_pnl < 0.0,
        "QQQ is falling, P&L should be negative, got {qqq_pnl}"
    );

    // Cross-check: if engine used SPY prices for QQQ's P&L, it would be
    // (108 - 101.5) * 5 = +32.5, which is positive. The negative assertion above
    // catches this bug.
}

// ---------------------------------------------------------------------------
// Test: ctx.sym() returns correct per-symbol prices
// ---------------------------------------------------------------------------

/// Verifies that ctx.sym("SPY").close and ctx.sym("QQQ").close return
/// different values matching each symbol's actual price data.
#[tokio::test(flavor = "multi_thread")]
async fn ctx_sym_returns_correct_prices() {
    let loader = make_two_symbol_loader();

    // Script uses ctx.sym() to read per-symbol prices and stores them in metadata.
    // On bar 2: SPY close=104, QQQ close=196. We verify both values.
    let script = r#"
let spy_sym = extern_symbol("spy_sym", "SPY", "long leg");
let qqq_sym = extern_symbol("qqq_sym", "QQQ", "short leg");

let spy_close_bar2 = 0.0;
let qqq_close_bar2 = 0.0;

fn config() {
    #{
        capital: 100000,
        interval: "daily",
        auto_close_on_end: true,
        data: #{ ohlcv: true },
    }
}

fn on_bar(ctx) {
    if ctx.bar_idx == 2 {
        let spy = ctx.sym(spy_sym);
        let qqq = ctx.sym(qqq_sym);
        spy_close_bar2 = spy.close;
        qqq_close_bar2 = qqq.close;

        // Only buy if SPY.close < QQQ.close (104 < 196 = true)
        if spy.close < qqq.close {
            return [buy_stock(spy_sym, 1)];
        }
    }
    []
}

fn on_exit_check(ctx, pos) {
    hold_position()
}

fn on_end(ctx) {
    #{
        spy_close: spy_close_bar2,
        qqq_close: qqq_close_bar2,
    }
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

    // Verify the actual close prices returned by ctx.sym()
    let metadata = result.metadata.expect("on_end should return metadata");
    let spy_close = metadata
        .get("spy_close")
        .expect("metadata should have spy_close")
        .as_float()
        .expect("spy_close should be a float");
    let qqq_close = metadata
        .get("qqq_close")
        .expect("metadata should have qqq_close")
        .as_float()
        .expect("qqq_close should be a float");

    // SPY bar 2 close = 100 + 2*2 = 104.0
    assert!(
        (spy_close - 104.0).abs() < 0.01,
        "ctx.sym(SPY).close on bar 2 should be 104.0, got {spy_close}"
    );
    // QQQ bar 2 close = 200 - 2*2 = 196.0
    assert!(
        (qqq_close - 196.0).abs() < 0.01,
        "ctx.sym(QQQ).close on bar 2 should be 196.0, got {qqq_close}"
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

    // Buy QQQ on bar 0; the order fills on bar 1 at QQQ's open (198.5).
    // There is no explicit exit in the script, so with `auto_close_on_end: true`
    // the position remains open until the final bar and should use QQQ's own
    // close for MTM/closing rather than SPY's.
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
  interval daily
  data ohlcv

asset symbol = "SPY"

on each bar
  skip when has positions
  buy 100 shares of symbol
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
  interval daily
  data ohlcv

asset symbol = "AAPL"

on each bar
  skip when not has positions
  sell 50 shares of symbol
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
  interval daily
  data ohlcv

asset symbol = "SPY"

on each bar
  skip when has positions
  buy 10 shares of symbol

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
    // SPY is rising (100→108), so equity should be ABOVE initial capital.
    // If no position was opened, equity would equal capital exactly.
    // If QQQ prices were used instead (declining), equity would be below capital.
    let last_equity = result.result.equity_curve.last().unwrap().equity;
    assert!(
        last_equity > 100_000.0,
        "SPY is rising, equity should be above initial capital (proves position opened with SPY data), got {last_equity}"
    );
}

// ---------------------------------------------------------------------------
// Test: DSL transpiled script respects symbol override via params
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn dsl_transpiled_script_symbol_override() {
    let dsl_source = r#"
strategy "DSL Override"
  interval daily
  data ohlcv

asset symbol = "SPY"

on each bar
  skip when has positions
  buy 10 shares of symbol

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
    // QQQ is declining (200→192), so equity should be BELOW initial capital.
    // If SPY data was used instead (rising), equity would be above capital.
    let last_equity = result.result.equity_curve.last().unwrap().equity;
    assert!(
        last_equity < 100_000.0,
        "QQQ is declining, equity should be below initial capital (proves QQQ data used), got {last_equity}"
    );
}

// ===========================================================================
// Options Multi-Symbol Tests
// ===========================================================================

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
        DATETIME_COL => DatetimeChunked::from_naive_datetime(
            PlSmallStr::from(DATETIME_COL),
            dates,
            TimeUnit::Microseconds,
        ).into_column().take_materialized_series(),
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

/// Data loader that returns different OHLCV AND options data per symbol.
struct MultiSymbolOptionsLoader {
    ohlcv: HashMap<String, DataFrame>,
    options: HashMap<String, DataFrame>,
}

#[async_trait::async_trait]
impl DataLoader for MultiSymbolOptionsLoader {
    async fn load_ohlcv(
        &self,
        symbol: &str,
        _start: Option<NaiveDate>,
        _end: Option<NaiveDate>,
    ) -> Result<DataFrame> {
        self.ohlcv
            .get(&symbol.to_uppercase())
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("No OHLCV data for '{symbol}'"))
    }

    async fn load_options(
        &self,
        symbol: &str,
        _start: Option<NaiveDate>,
        _end: Option<NaiveDate>,
    ) -> Result<DataFrame> {
        self.options
            .get(&symbol.to_uppercase())
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("No options data for '{symbol}'"))
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

/// Build test data for two symbols with options.
/// SPY: close 100 on all days, put strike 100 at delta -0.30
/// QQQ: close 200 on all days, put strike 200 at delta -0.30
/// Both have puts expiring on 2024-02-16.
fn make_options_loader() -> MultiSymbolOptionsLoader {
    let dates = [
        dt(2024, 1, 2),
        dt(2024, 1, 3),
        dt(2024, 1, 4),
        dt(2024, 2, 16), // expiration day
    ];
    let put_exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();

    let spy_bars: Vec<OhlcvBar> = dates
        .iter()
        .map(|&datetime| OhlcvBar {
            datetime,
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
            volume: 1_000_000.0,
        })
        .collect();

    let qqq_bars: Vec<OhlcvBar> = dates
        .iter()
        .map(|&datetime| OhlcvBar {
            datetime,
            open: 200.0,
            high: 201.0,
            low: 199.0,
            close: 200.0,
            volume: 2_000_000.0,
        })
        .collect();

    // SPY options: put at strike 100, bid/ask 3.00/3.50
    let spy_opts = make_options_df(
        &dates
            .iter()
            .map(|&d| (d, put_exp, "p", 100.0, 3.00, 3.50, -0.30))
            .collect::<Vec<_>>(),
    );

    // QQQ options: put at strike 200, bid/ask 5.00/5.50
    let qqq_opts = make_options_df(
        &dates
            .iter()
            .map(|&d| (d, put_exp, "p", 200.0, 5.00, 5.50, -0.30))
            .collect::<Vec<_>>(),
    );

    let mut ohlcv = HashMap::new();
    ohlcv.insert("SPY".to_string(), bars_to_df(&spy_bars));
    ohlcv.insert("QQQ".to_string(), bars_to_df(&qqq_bars));

    let mut options = HashMap::new();
    options.insert("SPY".to_string(), spy_opts);
    options.insert("QQQ".to_string(), qqq_opts);

    MultiSymbolOptionsLoader { ohlcv, options }
}

// ---------------------------------------------------------------------------
// Test: Options entry resolves from correct symbol's options chain
// ---------------------------------------------------------------------------

/// Two symbols with different strike prices. Script sells puts on QQQ.
/// Verifies the trade uses QQQ's strike (200), not SPY's (100).
#[tokio::test(flavor = "multi_thread")]
async fn options_entry_uses_correct_symbol_chain() {
    let loader = make_options_loader();

    let script = r#"
let spy_sym = extern_symbol("spy_sym", "SPY", "primary");
let qqq_sym = extern_symbol("qqq_sym", "QQQ", "secondary");

fn config() {
    #{
        capital: 100000,
        interval: "daily",
        data: #{ ohlcv: true, options: true },
        engine: #{ slippage: "mid", expiration_filter: "any" },
    }
}

fn on_bar(ctx) {
    if ctx.bar_idx == 0 && !ctx.has_positions() {
        // Sell a put on QQQ using QQQ's SymbolContext
        let qqq = ctx.sym(qqq_sym);
        let spread = qqq.short_put(0.30, 45);
        if spread != () { return [spread]; }
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
        .expect("options backtest should succeed");

    // Should have at least 1 trade (put expires at end or is still open)
    assert!(
        result.result.trade_count >= 1,
        "Should have at least 1 trade, got {}. Warnings: {:?}",
        result.result.trade_count,
        result.result.warnings,
    );

    // The trade should reference QQQ's strike (200), not SPY's (100)
    let trade = &result.result.trade_log[0];
    assert_eq!(
        trade.symbol.as_deref(),
        Some("QQQ"),
        "Trade should be for QQQ"
    );

    // Check legs: the put's strike should be 200 (QQQ), not 100 (SPY)
    assert!(
        !trade.legs.is_empty(),
        "Trade should have legs (it's an options trade)"
    );
    let strike = trade.legs[0].strike;
    assert!(
        (strike - 200.0).abs() < 1.0,
        "Put strike should be ~200 (QQQ strike), not ~100 (SPY), got {strike}"
    );
}

// ---------------------------------------------------------------------------
// Test: Options expiration ITM detection uses correct symbol's close
// ---------------------------------------------------------------------------

/// SPY close = 100. QQQ close = 210 on expiration day (above QQQ put strike 200).
/// A short put on QQQ at strike 200 with QQQ close at 210 should expire OTM.
/// If the engine incorrectly used SPY's close (100), the put at strike 200
/// would be deep ITM (assigned).
#[tokio::test(flavor = "multi_thread")]
async fn options_expiration_uses_correct_symbol_close() {
    // Custom loader: QQQ close = 210 on expiration day (above put strike 200 = OTM)
    let dates = [
        dt(2024, 1, 2),
        dt(2024, 1, 3),
        dt(2024, 1, 4),
        dt(2024, 2, 16), // expiration day
    ];
    let put_exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();

    let spy_bars: Vec<OhlcvBar> = dates
        .iter()
        .map(|&datetime| OhlcvBar {
            datetime,
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
            volume: 1_000_000.0,
        })
        .collect();

    // QQQ rises to 210 by expiration → put at 200 expires OTM
    let qqq_closes = [200.0, 202.0, 205.0, 210.0];
    let qqq_bars: Vec<OhlcvBar> = dates
        .iter()
        .zip(qqq_closes.iter())
        .map(|(&datetime, &close)| OhlcvBar {
            datetime,
            open: close - 1.0,
            high: close + 1.0,
            low: close - 2.0,
            close,
            volume: 2_000_000.0,
        })
        .collect();

    let spy_opts = make_options_df(
        &dates
            .iter()
            .map(|&d| (d, put_exp, "p", 100.0, 3.00, 3.50, -0.30))
            .collect::<Vec<_>>(),
    );
    let qqq_opts = make_options_df(
        &dates
            .iter()
            .map(|&d| (d, put_exp, "p", 200.0, 5.00, 5.50, -0.30))
            .collect::<Vec<_>>(),
    );

    let mut ohlcv = HashMap::new();
    ohlcv.insert("SPY".to_string(), bars_to_df(&spy_bars));
    ohlcv.insert("QQQ".to_string(), bars_to_df(&qqq_bars));
    let mut options = HashMap::new();
    options.insert("SPY".to_string(), spy_opts);
    options.insert("QQQ".to_string(), qqq_opts);

    let loader = MultiSymbolOptionsLoader { ohlcv, options };

    // Script sells a QQQ put, holds until expiration
    let script = r#"
let spy_sym = extern_symbol("spy_sym", "SPY", "primary");
let qqq_sym = extern_symbol("qqq_sym", "QQQ", "secondary");

fn config() {
    #{
        capital: 100000,
        interval: "daily",
        data: #{ ohlcv: true, options: true },
        engine: #{ slippage: "mid", expiration_filter: "any" },
    }
}

fn on_bar(ctx) {
    if ctx.bar_idx == 0 && !ctx.has_positions() {
        let qqq = ctx.sym(qqq_sym);
        let spread = qqq.short_put(0.30, 45);
        if spread != () { return [spread]; }
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
        .expect("options backtest should succeed");

    assert!(
        result.result.trade_count >= 1,
        "Should have at least 1 trade. Warnings: {:?}",
        result.result.warnings,
    );

    let trade = &result.result.trade_log[0];
    // QQQ close = 200, put strike = 200 → OTM/ATM → should expire, NOT be assigned.
    // If SPY close (100) was used: put strike 200 vs close 100 → deep ITM → assignment.
    let exit_reason = &trade.exit_type;
    assert!(
        !format!("{exit_reason:?}").contains("Assignment"),
        "QQQ put at strike 200 with QQQ close 200 should NOT be assignment. \
         If this is Assignment, the engine used SPY's close (100) instead of QQQ's (200). \
         Got: {exit_reason:?}"
    );
}

// ===========================================================================
// Sweep Integration Test
// ===========================================================================

// ---------------------------------------------------------------------------
// Test: Sweep with symbol in param grid
// ---------------------------------------------------------------------------

/// Verifies that a sweep with `symbol: ["SPY", "QQQ"]` in the param grid
/// runs the strategy for both symbols and produces different results.
#[tokio::test(flavor = "multi_thread")]
async fn sweep_with_symbol_array() {
    use optopsy_mcp::engine::sweep;

    let loader = std::sync::Arc::new(make_two_symbol_loader());

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

    let mut base_params = HashMap::new();
    base_params.insert("CAPITAL".to_string(), serde_json::json!(100_000.0));

    let mut param_grid = HashMap::new();
    param_grid.insert(
        "symbol".to_string(),
        vec![serde_json::json!("SPY"), serde_json::json!("QQQ")],
    );

    let config = sweep::GridSweepConfig {
        script_source: script.to_string(),
        base_params,
        param_grid,
        objective: "sharpe".to_string(),
    };

    let no_cancel: Box<dyn Fn() -> bool + Send + Sync> = Box::new(|| false);
    let result = sweep::run_grid_sweep(&config, loader, &no_cancel, |_, _| {})
        .await
        .expect("Sweep should succeed");

    assert_eq!(
        result.combinations_run, 2,
        "Should have run 2 combos (SPY + QQQ)"
    );

    // Both combos should produce results
    assert_eq!(
        result.ranked_results.len(),
        2,
        "Should have 2 ranked results"
    );

    // SPY is rising → positive P&L. QQQ is falling → negative P&L.
    // The ranked_results carry the combo params, so we can identify each.
    let spy_result = result
        .ranked_results
        .iter()
        .find(|r| r.params.get("symbol").and_then(|v| v.as_str()) == Some("SPY"))
        .expect("Should have SPY result");
    let qqq_result = result
        .ranked_results
        .iter()
        .find(|r| r.params.get("symbol").and_then(|v| v.as_str()) == Some("QQQ"))
        .expect("Should have QQQ result");

    assert!(
        spy_result.pnl > 0.0,
        "SPY is rising, sweep P&L should be positive, got {}",
        spy_result.pnl,
    );
    assert!(
        qqq_result.pnl < 0.0,
        "QQQ is declining, sweep P&L should be negative, got {}",
        qqq_result.pnl,
    );
}

// ===========================================================================
// Walk-Forward Integration Test
// ===========================================================================

// ---------------------------------------------------------------------------
// Test: Walk-forward injects symbol correctly
// ---------------------------------------------------------------------------

/// Date-filtering loader for walk-forward tests (filters by start/end date).
struct DateFilteringLoader {
    ohlcv_df: DataFrame,
}

#[async_trait::async_trait]
impl DataLoader for DateFilteringLoader {
    async fn load_ohlcv(
        &self,
        _symbol: &str,
        start: Option<NaiveDate>,
        end: Option<NaiveDate>,
    ) -> Result<DataFrame> {
        use polars::prelude::*;
        let mut df = self.ohlcv_df.clone();
        if let Some(s) = start {
            let start_dt = s.and_hms_opt(0, 0, 0).unwrap();
            df = df
                .lazy()
                .filter(col("datetime").gt_eq(lit(start_dt)))
                .collect()?;
        }
        if let Some(e) = end {
            let end_dt = e.and_hms_opt(23, 59, 59).unwrap();
            df = df
                .lazy()
                .filter(col("datetime").lt_eq(lit(end_dt)))
                .collect()?;
        }
        Ok(df)
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

/// Verifies that walk-forward passes the symbol to `run_script_backtest`
/// so that `extern_symbol("symbol", ...)` resolves correctly.
#[tokio::test(flavor = "multi_thread")]
async fn walk_forward_with_symbol() {
    use optopsy_mcp::engine::walk_forward::{self as wf, WalkForwardParams, WfMode, WfObjective};

    let bars: Vec<OhlcvBar> = (0..20)
        .map(|i| {
            let day = 2 + (i % 28);
            let month = 1 + i / 28;
            let close = 100.0 + f64::from(i) * 0.5;
            OhlcvBar {
                datetime: dt(2024, month as u32, day as u32),
                open: close - 0.2,
                high: close + 0.5,
                low: close - 0.5,
                close,
                volume: 1_000_000.0,
            }
        })
        .collect();

    let loader = DateFilteringLoader {
        ohlcv_df: bars_to_df(&bars),
    };

    let script = r#"
let symbol = extern_symbol("symbol", "SPY", "ticker");
let THRESHOLD = extern("THRESHOLD", 0.04, "threshold");

fn config() {
    #{
        capital: 100000,
        interval: "daily",
        auto_close_on_end: true,
        data: #{ ohlcv: true, indicators: ["sma:5"] },
    }
}

fn on_bar(ctx) {
    if ctx.bar_idx < 5 || ctx.has_positions() { return []; }
    let sma = ctx.sma(5);
    if sma == () { return []; }
    if ctx.close > sma * (1.0 + THRESHOLD) {
        return [buy_stock(symbol, 10)];
    }
    []
}

fn on_exit_check(ctx, pos) {
    if pos.days_held >= 2 {
        return close_position("target");
    }
    hold_position()
}
"#;

    let wf_params = WalkForwardParams {
        strategy: "inline".to_string(),
        symbol: "TEST".to_string(),
        capital: 100_000.0,
        params_grid: HashMap::from([(
            "THRESHOLD".to_string(),
            vec![serde_json::json!(0.01), serde_json::json!(0.02)],
        )]),
        objective: WfObjective::Sharpe,
        n_windows: 2,
        mode: WfMode::Rolling,
        train_pct: 0.70,
        start_date: None,
        end_date: None,
        profile: None,
        script_source: Some(script.to_string()),
        base_params: None,
    };

    let no_cancel: Box<dyn Fn() -> bool + Send + Sync> = Box::new(|| false);
    let result = wf::execute(wf_params, &loader, &no_cancel, |_, _| {}).await;

    // Walk-forward should succeed (not error out on missing symbol)
    assert!(
        result.is_ok(),
        "Walk-forward should succeed with extern_symbol script: {:#}",
        result.err().unwrap()
    );

    let wf_result = result.unwrap();
    assert!(
        !wf_result.windows.is_empty(),
        "Should have at least one walk-forward window"
    );
}
