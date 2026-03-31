//! End-to-end tests: DSL source → transpiled Rhai.

use super::*;

#[test]
fn test_transpile_minimal_stock_strategy() {
    let dsl = r#"
strategy "Buy and Hold"
  symbol AAPL
  interval daily
  data ohlcv

on each bar
  skip when has positions
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();

    // Should contain config function
    assert!(rhai.contains("fn config()"));
    assert!(rhai.contains("symbol: \"AAPL\""));
    assert!(rhai.contains("interval: \"daily\""));
    assert!(rhai.contains("ohlcv: true"));

    // Should contain on_bar with action accumulation
    assert!(rhai.contains("fn on_bar(ctx)"));
    assert!(rhai.contains("let __actions = [];"));
    assert!(rhai.contains("ctx.has_positions()"));
    assert!(rhai.contains("buy_stock(100)"));
    assert!(rhai.contains("__actions"));
}

#[test]
fn test_transpile_with_params_and_state() {
    let dsl = r#"
strategy "SMA Crossover"
  symbol SPY
  interval daily
  data ohlcv
  indicators sma:50, sma:200

param THRESHOLD = 0.04 "Entry threshold"
state wins = 0
state losses = 0

on each bar
  require sma:50, sma:200
  skip when has positions
  when close > sma(200) * (1 + THRESHOLD) then
    buy size_by_equity(1.0) shares

on exit check
  when close < sma(200) then
    close position "below_sma"
  otherwise
    hold position

on position closed
  when pos.pnl > 0 then
    add 1 to wins
  otherwise
    add 1 to losses
"#;

    let rhai = transpile(dsl).unwrap();

    // Params
    assert!(rhai.contains("extern(\"THRESHOLD\", 0.04, \"Entry threshold\")"));

    // State
    assert!(rhai.contains("let wins = 0;"));
    assert!(rhai.contains("let losses = 0;"));

    // Indicators in config
    assert!(rhai.contains("\"sma:50\""));
    assert!(rhai.contains("\"sma:200\""));

    // on_bar: require should generate indicators_ready
    assert!(rhai.contains("ctx.indicators_ready("));

    // Expression rewriting
    assert!(rhai.contains("ctx.close > ctx.sma(200)"));
    assert!(rhai.contains("ctx.size_by_equity(1.0)"));

    // on_exit_check
    assert!(rhai.contains("fn on_exit_check(ctx, pos)"));
    assert!(rhai.contains("close_position(\"below_sma\")"));
    assert!(rhai.contains("hold_position()"));

    // on_position_closed
    assert!(rhai.contains("fn on_position_closed(ctx, pos, exit_type)"));
    assert!(rhai.contains("wins += 1"));
    assert!(rhai.contains("losses += 1"));
}

#[test]
fn test_transpile_options_strategy() {
    let dsl = r#"
strategy "Iron Condor Income"
  symbol SPY
  interval daily
  data ohlcv, options
  indicators rsi:14
  slippage mid
  expiration_filter monthly
  max_positions 1

param PUT_DELTA = 0.30 "Short put delta"
param CALL_DELTA = 0.30 "Short call delta"
param DTE = 45 "Target DTE"
param PROFIT_TARGET = 0.50 "Take profit percentage"

on each bar
  require rsi:14
  skip when has positions
  skip when rsi(14) > 70
  open iron_condor(PUT_DELTA, CALL_DELTA, DTE)

on exit check
  when pos.pnl_pct > PROFIT_TARGET then
    close position "take_profit"
  when pos.days_held > 30 then
    close position "max_hold"
  otherwise
    hold position
"#;

    let rhai = transpile(dsl).unwrap();

    // Options data
    assert!(rhai.contains("options: true"));
    assert!(rhai.contains("ohlcv: true"));

    // Engine settings
    assert!(rhai.contains("slippage: \"mid\""));
    assert!(rhai.contains("expiration_filter: \"monthly\""));
    assert!(rhai.contains("max_positions: 1"));

    // Strategy call should be ctx-qualified
    assert!(rhai.contains("ctx.iron_condor(PUT_DELTA, CALL_DELTA, DTE)"));

    // Chained when/otherwise should generate if/else if/else
    assert!(rhai.contains("if pos.pnl_pct > PROFIT_TARGET"));
    assert!(rhai.contains("} else if pos.days_held > 30 {"));
    assert!(rhai.contains("} else {"));
}

#[test]
fn test_transpile_sell_validation() {
    let dsl = r#"
strategy "Sell Test"
  symbol AAPL
  interval daily
  data ohlcv

on each bar
  sell 50 shares
"#;

    let rhai = transpile(dsl).unwrap();

    // Sell should include quantity validation guard
    assert!(rhai.contains("let __sell_qty = 50;"));
    assert!(rhai.contains("if __sell_qty > 0"));
    assert!(rhai.contains("sell_stock(__sell_qty)"));
}

#[test]
fn test_transpile_plot_statement() {
    let dsl = r#"
strategy "Plot Test"
  symbol SPY
  interval daily
  data ohlcv
  indicators sma:200

on each bar
  set upper to sma(200) * 1.05
  plot "Upper Band" at upper
  plot "RSI" at rsi(14) as subchart
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("ctx.plot(\"Upper Band\", upper)"));
    assert!(rhai.contains("ctx.plot_with(\"RSI\", ctx.rsi(14), \"subchart\")"));
}

#[test]
fn test_transpile_boolean_operators() {
    let dsl = r#"
strategy "Boolean Test"
  symbol SPY
  interval daily
  data ohlcv

on each bar
  skip when has positions
  when close > 100 and rsi(14) < 30 or volume > 1000000 then
    buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("ctx.close > 100 && ctx.rsi(14) < 30 || ctx.volume > 1000000"));
}

#[test]
fn test_is_trading_dsl_detection() {
    assert!(is_trading_dsl("strategy \"Test\"\n  symbol SPY\n"));
    assert!(is_trading_dsl("# comment\nstrategy \"Test\"\n"));
    assert!(!is_trading_dsl("fn config() {\n  #{}"));
    assert!(!is_trading_dsl("let x = 42;"));
}

#[test]
fn test_error_on_bad_indent() {
    let dsl = "  strategy \"Test\"";
    let err = transpile(dsl).unwrap_err();
    assert!(err.message.contains("unexpected indentation"));
}

#[test]
fn test_error_on_when_without_then() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  when close > 100
    buy 100 shares
"#;
    let err = transpile(dsl).unwrap_err();
    assert!(err.message.contains("then"));
}

#[test]
fn test_transpile_set_and_add() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily
  data ohlcv

state counter = 0

on each bar
  set counter to counter + 1
  add 5 to counter
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("counter = counter + 1;"));
    assert!(rhai.contains("counter += 5;"));
}

#[test]
fn test_transpile_cross_symbols() {
    let dsl = r#"
strategy "Pairs"
  symbol SPY
  interval daily
  data ohlcv
  cross_symbols QQQ, IWM

on each bar
  when price_of("QQQ") > close then
    buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("cross_symbols: [\"QQQ\", \"IWM\"]"));
    assert!(rhai.contains("ctx.price_of(\"QQQ\") > ctx.close"));
}

#[test]
fn test_transpile_param_with_choices() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

param MODE = "fast" "Execution mode" choices fast, slow

on each bar
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("extern(\"MODE\", \"fast\", \"Execution mode\", [\"fast\", \"slow\"])"));
}

#[test]
fn test_transpile_raw_escape_hatch() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  raw let x = 42;
  buy x shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("let x = 42;"));
}

#[test]
fn test_transpile_on_end_callback() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

state total_bars = 0

on each bar
  add 1 to total_bars
  buy 100 shares

on end
  raw print("Done: " + total_bars);
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("fn on_end(ctx)"));
}
