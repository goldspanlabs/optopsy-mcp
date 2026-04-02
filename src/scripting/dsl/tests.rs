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

extern THRESHOLD = 0.04 "Entry threshold"
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

extern PUT_DELTA = 0.30 "Short put delta"
extern CALL_DELTA = 0.30 "Short call delta"
extern DTE = 45 "Target DTE"
extern PROFIT_TARGET = 0.50 "Take profit percentage"

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
fn test_transpile_extern_with_choices() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

extern MODE = "fast" "Execution mode" choices fast, slow

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

#[test]
fn test_independent_when_blocks_without_otherwise() {
    // Two consecutive `when` blocks WITHOUT `otherwise` should produce
    // two separate `if` statements, not an if/else-if chain.
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  when close > 100 then
    buy 50 shares
  when rsi(14) < 30 then
    buy 50 shares
"#;

    let rhai = transpile(dsl).unwrap();
    // Should have two separate `if` blocks, not `else if`
    assert!(
        !rhai.contains("else if"),
        "Independent when blocks should NOT produce else-if.\nGenerated:\n{rhai}"
    );
    // Both conditions should appear as separate if statements
    let if_count =
        rhai.matches("if ctx.close > 100").count() + rhai.matches("if ctx.rsi(14) < 30").count();
    assert_eq!(if_count, 2, "Expected 2 separate if blocks");
}

#[test]
fn test_when_chain_with_otherwise_produces_else_if() {
    // Consecutive `when` blocks WITH `otherwise` should chain into if/else-if/else.
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on exit check
  when pos.pnl_pct > 0.50 then
    close position "take_profit"
  when pos.days_held > 30 then
    close position "max_hold"
  when pos.pnl_pct < -1.0 then
    close position "stop_loss"
  otherwise
    hold position
"#;

    let rhai = transpile(dsl).unwrap();
    // Should have a fully flattened else-if chain
    assert!(
        rhai.contains("else if pos.days_held > 30"),
        "Chained when with otherwise should produce else-if.\nGenerated:\n{rhai}"
    );
    assert!(
        rhai.contains("else if pos.pnl_pct < -1.0"),
        "Third when in chain should also be else-if.\nGenerated:\n{rhai}"
    );
    // The otherwise should be a flat `else`, not nested
    let nested_else_if = rhai.contains("else {\n        if");
    assert!(
        !nested_else_if,
        "Chain should be fully flattened, not nested.\nGenerated:\n{rhai}"
    );
}

// ---------------------------------------------------------------------------
// New feature tests
// ---------------------------------------------------------------------------

#[test]
fn test_for_each_loop() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  for each pos in positions()
    when pos.pnl_pct < -0.5 then
      close position pos.id "stop_loss"
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("for pos in ctx.positions()"));
    assert!(rhai.contains("if pos.pnl_pct < -0.5"));
}

#[test]
fn test_subtract_from() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

state balance = 100

on each bar
  subtract 10 from balance
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("balance -= 10;"));
}

#[test]
fn test_multiply_and_divide() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

state factor = 1.0

on each bar
  multiply factor by 1.05
  divide factor by 2
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("factor *= 1.05;"));
    assert!(rhai.contains("factor /= 2;"));
}

#[test]
fn test_array_indexing_in_expressions() {
    // Verify that pos.legs[0].strike passes through rewriting unchanged
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily
  data ohlcv, options

on exit check
  when pos.legs[0].strike > close then
    close position "itm"
  otherwise
    hold position
"#;

    let rhai = transpile(dsl).unwrap();
    // legs[0].strike should NOT be rewritten — it's after a dot
    assert!(
        rhai.contains("pos.legs[0].strike > ctx.close"),
        "Array indexing should pass through.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_underscore_group_variable() {
    // Verify that _group works as a state variable
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

state _group = "Cycle 1"

on each bar
  buy 100 shares

on position closed
  set _group to "Cycle 2"
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("let _group = \"Cycle 1\";"));
    // _group is in scope_vars, so `set` should use bare assignment (no let)
    assert!(rhai.contains("_group = \"Cycle 2\";"));
    assert!(!rhai.contains("let _group = \"Cycle 2\""));
}

#[test]
fn test_for_each_with_complex_body() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

state total_pnl = 0.0

on each bar
  for each pos in positions()
    add pos.unrealized_pnl to total_pnl
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("for pos in ctx.positions()"));
    assert!(rhai.contains("total_pnl += pos.unrealized_pnl;"));
}

#[test]
fn test_mixed_dsl_and_raw_for_complex_patterns() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily
  data ohlcv

on each bar
  raw let bb_width = (ctx.bbands_upper(20) - ctx.bbands_lower(20)) / ctx.sma(20);
  raw let regime = if bb_width > 0.08 { "volatile" } else { "normal" };
  when regime == "normal" then
    buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("let bb_width = (ctx.bbands_upper(20)"));
    assert!(rhai.contains("let regime = if bb_width > 0.08"));
}

// ---------------------------------------------------------------------------
// Next-bar execution / order type tests
// ---------------------------------------------------------------------------

#[test]
fn test_buy_limit_order() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  buy 100 shares at 150.00 limit
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("buy_limit(100, 150.00)"),
        "Should generate buy_limit call.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_buy_stop_order() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  buy 100 shares at 155.00 stop
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("buy_stop(100, 155.00)"),
        "Should generate buy_stop call.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_sell_limit_order() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  sell 50 shares at 200.00 limit
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("sell_limit(__sell_qty, 200.00)"),
        "Should generate sell_limit call with guard.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_sell_stop_order() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  sell 50 shares at entry_price - 2.0 stop
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("sell_stop("),
        "Should generate sell_stop call.\nGenerated:\n{rhai}"
    );
    assert!(
        rhai.contains("ctx.entry_price - 2.0"),
        "entry_price should be rewritten to ctx.entry_price.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_buy_at_market_explicit() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  buy 100 shares at market
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("buy_stock(100)"),
        "Explicit 'at market' should generate buy_stock.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_cancel_all_orders() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  cancel all orders
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("cancel_orders()"),
        "Should generate cancel_orders().\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_cancel_orders_by_signal() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  cancel orders "old_entry"
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("cancel_orders(\"old_entry\")"),
        "Should generate cancel_orders with signal.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_position_awareness_properties() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  skip when market_position != 0
  when entry_price > 0 and bars_since_entry > 5 then
    sell current_shares shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("ctx.market_position != 0"));
    assert!(rhai.contains("ctx.entry_price > 0"));
    assert!(rhai.contains("ctx.bars_since_entry > 5"));
    assert!(rhai.contains("ctx.current_shares"));
}

#[test]
fn test_dynamic_limit_price_expression() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  buy 100 shares at close - atr(14) * 2 limit
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("buy_limit(100, ctx.close - ctx.atr(14) * 2)"),
        "Dynamic limit price should be rewritten.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_capitalized_buy_next_bar_at_market() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  Buy 100 shares next bar at market
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("buy_stock(100)"),
        "Capitalized Buy + next bar at market should work.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_capitalized_sell_next_bar_at_limit() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  Sell 50 shares next bar at 200.00 limit
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("sell_limit("),
        "Capitalized Sell + next bar at limit should work.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_both_cases_accepted() {
    // Lowercase still works (backward compat)
    let dsl_lower = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  buy 100 shares
"#;

    // Uppercase (TradeStation-style)
    let dsl_upper = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  Buy 100 shares next bar at market
"#;

    let rhai_lower = transpile(dsl_lower).unwrap();
    let rhai_upper = transpile(dsl_upper).unwrap();

    // Both should generate buy_stock
    assert!(rhai_lower.contains("buy_stock(100)"));
    assert!(rhai_upper.contains("buy_stock(100)"));
}

#[test]
fn test_transpile_lookback_strategy() {
    let dsl = r#"
strategy "Lookback Test"
  symbol SPY
  interval daily
  data ohlcv
  indicators sma:50, rsi:14

on each bar
  require sma:50
  when close[1] > sma(50)[1] and close < sma(50) then
    Buy size_by_equity(1.0) shares next bar at market
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("ctx.close(1) > ctx.sma_at(50, 1)"),
        "Lookback syntax should be transpiled.\nGenerated:\n{rhai}"
    );
    assert!(
        rhai.contains("ctx.close < ctx.sma(50)"),
        "Non-lookback should remain normal.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_transpile_crosses_in_when() {
    let dsl = r#"
strategy "Cross Test"
  symbol SPY
  interval daily
  indicators sma:50, sma:200

on each bar
  require sma:50, sma:200
  when sma(50) crosses above sma(200) then
    Buy 100 shares next bar at market
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("ctx.crossed_above(\"sma:50\", \"sma:200\")"),
        "Should contain crossed_above call.\nGenerated:\n{rhai}"
    );
}
