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

// ---------------------------------------------------------------------------
// Procedural mode tests
// ---------------------------------------------------------------------------

#[test]
fn test_transpile_procedural_mode() {
    let dsl = r#"
strategy "SMA Cross" procedural
  symbol SYMBOL
  capital CAPITAL
  interval daily
  indicators sma:50, sma:200

extern FAST = 50 "Fast MA"

require sma:50, sma:200

when no positions and sma(50) crosses above sma(200) then
  Buy size_by_equity(1.0) shares next bar at market

when has positions and close crosses below sma(50) then
  close position "signal_exit"
"#;

    let rhai = transpile(dsl).unwrap();
    // Should have procedural flag in config
    assert!(
        rhai.contains("procedural: true"),
        "Missing procedural flag.\nGenerated:\n{rhai}"
    );
    // Should have single on_bar function
    assert!(
        rhai.contains("fn on_bar(ctx)"),
        "Missing on_bar.\nGenerated:\n{rhai}"
    );
    // Should NOT have on_exit_check
    assert!(
        !rhai.contains("fn on_exit_check"),
        "Should not have on_exit_check.\nGenerated:\n{rhai}"
    );
    // Body should be inside on_bar
    assert!(
        rhai.contains("crossed_above"),
        "Missing crossed_above.\nGenerated:\n{rhai}"
    );
    assert!(
        rhai.contains("close_position(\"signal_exit\")"),
        "Missing close_position.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_procedural_rejects_event_blocks() {
    let dsl = r#"
strategy "Test" procedural
  symbol SPY
  interval daily

on each bar
  buy 100 shares
"#;

    let err = transpile(dsl).unwrap_err();
    assert!(
        err.message.contains("procedural"),
        "Error should mention procedural: {}",
        err.message
    );
}

#[test]
fn test_callback_rejects_bare_statements() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

buy 100 shares
"#;

    let err = transpile(dsl).unwrap_err();
    assert!(
        err.message.contains("unrecognized"),
        "Error should mention unrecognized: {}",
        err.message
    );
}

#[test]
fn test_procedural_with_state() {
    let dsl = r#"
strategy "Test" procedural
  symbol SPY
  interval daily

state counter = 0

add 1 to counter
when counter > 10 and no positions then
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("let counter = 0;"));
    assert!(rhai.contains("fn on_bar(ctx)"));
    assert!(rhai.contains("counter += 1;"));
}

// ---------------------------------------------------------------------------
// Auto-detect indicators tests
// ---------------------------------------------------------------------------

#[test]
fn test_auto_detect_indicators_from_body() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  when sma(200) > ema(50) and rsi(14) < 30 then
    buy 100 shares
"#;
    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("\"sma:200\""), "Missing sma:200.\n{rhai}");
    assert!(rhai.contains("\"ema:50\""), "Missing ema:50.\n{rhai}");
    assert!(rhai.contains("\"rsi:14\""), "Missing rsi:14.\n{rhai}");
}

#[test]
fn test_auto_detect_merges_with_explicit() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily
  indicators atr:14

on each bar
  when sma(200) > 0 then
    buy 100 shares
"#;
    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("\"atr:14\""),
        "Missing explicit atr:14.\n{rhai}"
    );
    assert!(
        rhai.contains("\"sma:200\""),
        "Missing auto-detected sma:200.\n{rhai}"
    );
}

#[test]
fn test_auto_detect_from_lookback_and_crossover() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  when sma(50)[1] > sma(200)[1] and rsi(14) crosses above 30 then
    buy 100 shares
"#;
    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("\"sma:50\""), "Missing sma:50.\n{rhai}");
    assert!(rhai.contains("\"sma:200\""), "Missing sma:200.\n{rhai}");
    assert!(rhai.contains("\"rsi:14\""), "Missing rsi:14.\n{rhai}");
}

#[test]
fn test_no_indicators_line_needed_procedural() {
    let dsl = r#"
strategy "Test" procedural
  symbol SPY
  interval daily

require sma:50
when sma(50) > close then
  buy 100 shares
"#;
    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("\"sma:50\""),
        "Missing auto-detected sma:50.\n{rhai}"
    );
}

#[test]
fn test_transpile_per_order_stop_loss() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  Buy 100 shares next bar at market
    stop_loss 5%
    profit_target 10%
"#;
    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("let __order = buy_stock(100)"),
        "Should use __order.\n{rhai}"
    );
    assert!(
        rhai.contains("__order.stop_loss_pct = 0.05"),
        "Missing stop_loss_pct.\n{rhai}"
    );
    assert!(
        rhai.contains("__order.profit_target_pct = 0.1"),
        "Missing profit_target_pct.\n{rhai}"
    );
    assert!(
        rhai.contains("__actions.push(__order)"),
        "Should push __order.\n{rhai}"
    );
}

#[test]
fn test_transpile_per_order_dollar_stop() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  Buy 100 shares next bar at market
    stop_loss $500
"#;
    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("__order.stop_loss_dollar = 500"),
        "Missing stop_loss_dollar.\n{rhai}"
    );
}

#[test]
fn test_transpile_per_order_trailing_stop() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  Buy 100 shares next bar at market
    trailing_stop 3%
"#;
    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("__order.trailing_stop_pct = 0.03"),
        "Missing trailing_stop_pct.\n{rhai}"
    );
}

#[test]
fn test_transpile_buy_without_modifiers_unchanged() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  Buy 100 shares next bar at market
"#;
    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("__actions.push(buy_stock(100))"),
        "Should use direct push.\n{rhai}"
    );
    assert!(!rhai.contains("__order"), "Should not use __order.\n{rhai}");
}

#[test]
fn test_transpile_procedural_with_per_order_stops() {
    let dsl = r#"
strategy "Test" procedural
  symbol SPY
  interval daily

when no positions and close > sma(200) then
  Buy 100 shares next bar at market
    stop_loss 5%
    profit_target 10%
    trailing_stop 3%
"#;
    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("__order.stop_loss_pct = 0.05"));
    assert!(rhai.contains("__order.profit_target_pct = 0.1"));
    assert!(rhai.contains("__order.trailing_stop_pct = 0.03"));
}

// ---------------------------------------------------------------------------
// Metadata keyword tests
// ---------------------------------------------------------------------------

#[test]
fn test_transpile_metadata() {
    let dsl = r#"
strategy "SMA Threshold"
  symbol SYMBOL
  capital CAPITAL
  interval daily
  category stock
  description "Enter when close > SMA(200)"
  hypothesis "Momentum above 200-day SMA signals continuation"
  tags trend_following, momentum, sma
  regime trending, bullish

on each bar
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("//! name: SMA Threshold"),
        "Missing name.\n{rhai}"
    );
    assert!(
        rhai.contains("//! description: Enter when close > SMA(200)"),
        "Missing description.\n{rhai}"
    );
    assert!(
        rhai.contains("//! category: stock"),
        "Missing category.\n{rhai}"
    );
    assert!(
        rhai.contains("//! hypothesis: Momentum above 200-day SMA signals continuation"),
        "Missing hypothesis.\n{rhai}"
    );
    assert!(
        rhai.contains("//! tags: trend_following, momentum, sma"),
        "Missing tags.\n{rhai}"
    );
    assert!(
        rhai.contains("//! regime: trending, bullish"),
        "Missing regime.\n{rhai}"
    );
}

#[test]
fn test_transpile_metadata_optional() {
    let dsl = r#"
strategy "Minimal"
  symbol SYMBOL
  capital CAPITAL
  interval daily

on each bar
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    // Name is always emitted
    assert!(rhai.contains("//! name: Minimal"), "Missing name.\n{rhai}");
    // Optional fields should not appear
    assert!(
        !rhai.contains("//! description:"),
        "Should not have description.\n{rhai}"
    );
    assert!(
        !rhai.contains("//! category:"),
        "Should not have category.\n{rhai}"
    );
}

// ---------------------------------------------------------------------------
// Dotted-path support tests
// ---------------------------------------------------------------------------

#[test]
fn test_set_dotted_path() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

state stock = #{price: 0.0, basis: 0.0}

on each bar
  set stock.price to close
  set stock.basis to close * 0.95
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    // Dotted path should be assignment, not let declaration
    assert!(
        rhai.contains("stock.price = ctx.close;"),
        "Should assign dotted path.\n{rhai}"
    );
    assert!(
        !rhai.contains("let stock.price"),
        "Should NOT declare dotted path.\n{rhai}"
    );
}

#[test]
fn test_compound_assignment_dotted_path() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

state totals = #{premium: 0.0, count: 0}

on each bar
  add 1.5 to totals.premium
  add 1 to totals.count
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("totals.premium += 1.5;"),
        "Should compound-assign dotted path.\n{rhai}"
    );
    assert!(
        rhai.contains("totals.count += 1;"),
        "Should compound-assign dotted path.\n{rhai}"
    );
}

#[test]
fn test_transpile_try_open() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily
  data ohlcv, options

state min_strike = 100.0

on each bar
  skip when has positions
  try open covered_call(0.30, 30) as spread
    skip when spread.spread.legs[0].strike < min_strike
    set min_strike to spread.spread.legs[0].strike
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("let spread = ctx.covered_call(0.30, 30)"),
        "Missing spread binding.\n{rhai}"
    );
    assert!(
        rhai.contains("if spread != ()"),
        "Missing null check.\n{rhai}"
    );
    assert!(
        rhai.contains("spread.spread.legs[0].strike < min_strike"),
        "Missing body condition.\n{rhai}"
    );
    assert!(
        rhai.contains("__actions.push(spread)"),
        "Missing auto-push.\n{rhai}"
    );
}

#[test]
fn test_transpile_try_open_procedural() {
    let dsl = r#"
strategy "Test" procedural
  symbol SPY
  interval daily
  data ohlcv, options

try open short_put(0.30, 45) as order
  skip when close < sma(200)
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("let order = ctx.short_put(0.30, 45)"),
        "Missing order binding.\n{rhai}"
    );
    assert!(
        rhai.contains("if order != ()"),
        "Missing null check.\n{rhai}"
    );
    assert!(
        rhai.contains("__actions.push(order)"),
        "Missing auto-push.\n{rhai}"
    );
}

// ---------------------------------------------------------------------------
// Time-based DSL features
// ---------------------------------------------------------------------------

#[test]
fn test_day_of_week_with_name() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  skip when day_of_week == monday
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("ctx.day_of_week() == 1"),
        "day_of_week should map to ctx method, monday to 1.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_month_name_mapping() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  skip when month == january
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("ctx.month() == 1"),
        "month should map to ctx method, january to 1.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_time_literal_intraday() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval 5m

on each bar
  skip when time < 10:00
  skip when time > 15:30
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains(r#"ctx.time() < "10:00""#),
        "time literal should be quoted string.\nGenerated:\n{rhai}"
    );
    assert!(
        rhai.contains(r#"ctx.time() > "15:30""#),
        "time literal should be quoted string.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_is_first_bar_intraday() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval 15m

on each bar
  skip when is_first_bar
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("ctx.is_first_bar()"),
        "is_first_bar should map to ctx method.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_is_expiry_week_daily() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  skip when is_expiry_week
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("ctx.is_expiry_week()"),
        "is_expiry_week should work on daily.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_trading_days_left_daily() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  skip when trading_days_left < 3
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("ctx.trading_days_left() < 3"),
        "trading_days_left should map to ctx method.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_is_quarter_end_daily() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  when is_quarter_end then
    close position "quarter_rebalance"
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("ctx.is_quarter_end()"),
        "is_quarter_end should map to ctx method.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_intraday_keyword_error_on_daily() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  skip when time < 10:00
  buy 100 shares
"#;

    let err = transpile(dsl).unwrap_err();
    assert!(
        err.message.contains("`time`"),
        "Error should mention `time`.\nGot: {err}"
    );
    assert!(
        err.message.contains("intraday"),
        "Error should mention intraday.\nGot: {err}"
    );
}

#[test]
fn test_intraday_keyword_error_is_first_bar_daily() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  skip when is_first_bar
  buy 100 shares
"#;

    let err = transpile(dsl).unwrap_err();
    assert!(
        err.message.contains("`is_first_bar`"),
        "Error should mention is_first_bar.\nGot: {err}"
    );
}

#[test]
fn test_intraday_keyword_error_minutes_since_open_daily() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  skip when minutes_since_open < 30
  buy 100 shares
"#;

    let err = transpile(dsl).unwrap_err();
    assert!(
        err.message.contains("`minutes_since_open`"),
        "Error should mention minutes_since_open.\nGot: {err}"
    );
}

#[test]
fn test_day_names_all_map_correctly() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  skip when day_of_week == friday
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("ctx.day_of_week() == 5"),
        "friday should map to 5.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_multiple_time_keywords_combined() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  skip when day_of_week == monday
  skip when month == december
  skip when is_expiry_week
  when trading_days_left < 3 and day_of_month > 25 then
    close position "month_end"
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("ctx.day_of_week() == 1"));
    assert!(rhai.contains("ctx.month() == 12"));
    assert!(rhai.contains("ctx.is_expiry_week()"));
    assert!(rhai.contains("ctx.trading_days_left() < 3"));
    assert!(rhai.contains("ctx.day_of_month() > 25"));
}

#[test]
fn test_time_literal_single_digit_hour() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval 5m

on each bar
  skip when time < 9:30
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains(r#"ctx.time() < "09:30""#),
        "Single-digit hour should be zero-padded.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_callable_properties_with_parens_also_work() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval 5m

on each bar
  skip when time() < 10:00
  skip when minutes_since_open() < 30
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("ctx.time()"),
        "time() with parens should work.\nGenerated:\n{rhai}"
    );
    assert!(
        rhai.contains("ctx.minutes_since_open()"),
        "minutes_since_open() with parens should work.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_invalid_time_literal_compile_error() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval 5m

on each bar
  skip when time < 25:00
  buy 100 shares
"#;

    let err = transpile(dsl).unwrap_err();
    assert!(
        err.message.contains("25:00"),
        "Error should mention the invalid time.\nGot: {err}"
    );
    assert!(
        err.message.contains("0-23"),
        "Error should mention valid hour range.\nGot: {err}"
    );
}

#[test]
fn test_invalid_time_minute_compile_error() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval 5m

on each bar
  skip when time > 10:75
  buy 100 shares
"#;

    let err = transpile(dsl).unwrap_err();
    assert!(
        err.message.contains("10:75"),
        "Error should mention the invalid time.\nGot: {err}"
    );
}

// ---------------------------------------------------------------------------
// Reserved name validation tests
// ---------------------------------------------------------------------------

#[test]
fn test_reserved_name_extern_day() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

extern monday = 1 "A variable named monday"

on each bar
  buy 100 shares
"#;

    let err = transpile(dsl).unwrap_err();
    assert!(
        err.message.contains("reserved day/month name"),
        "Should reject extern named 'monday'.\nGot: {err}"
    );
}

#[test]
fn test_reserved_name_state_month() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

state may = 0

on each bar
  buy 100 shares
"#;

    let err = transpile(dsl).unwrap_err();
    assert!(
        err.message.contains("reserved day/month name"),
        "Should reject state named 'may'.\nGot: {err}"
    );
}

#[test]
fn test_non_reserved_name_accepted() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

extern monday_count = 0 "Not a reserved name"
state may_trades = 0

on each bar
  buy 100 shares
"#;

    // Should not error — these are not exact matches
    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("monday_count"));
    assert!(rhai.contains("may_trades"));
}

#[test]
fn test_reserved_name_set_variable() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  set march to 3
  buy 100 shares
"#;

    let err = transpile(dsl).unwrap_err();
    assert!(
        err.message.contains("reserved day/month name"),
        "Should reject set variable named 'march'.\nGot: {err}"
    );
}

#[test]
fn test_reserved_name_for_each_variable() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  for each friday in pos.legs
    hold position
"#;

    let err = transpile(dsl).unwrap_err();
    assert!(
        err.message.contains("reserved day/month name"),
        "Should reject for-each variable named 'friday'.\nGot: {err}"
    );
}

// ---------------------------------------------------------------------------
// Inline if/else ternary tests
// ---------------------------------------------------------------------------

#[test]
fn test_inline_if_else_in_set() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval 5m

extern VIX_THRESHOLD = 20 "VIX threshold"

on each bar
  set target_delta to 0.15 if close > sma(200) else 0.30
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("if ctx.close > ctx.sma(200) { 0.15 } else { 0.30 }"),
        "Inline if/else should be transpiled.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_inline_if_else_chained_in_set() {
    let dsl = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  set size to 1.0 if rsi(14) < 30 else 0.5 if rsi(14) > 70 else 0.75
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("if ctx.rsi(14) < 30 { 1.0 } else if ctx.rsi(14) > 70 { 0.5 } else { 0.75 }"),
        "Chained inline if/else should produce else-if.\nGenerated:\n{rhai}"
    );
}

// ---------------------------------------------------------------------------
// Portfolio namespace tests
// ---------------------------------------------------------------------------

#[test]
fn test_portfolio_property_access() {
    let dsl = r#"
strategy "Portfolio Test"
  symbol SPY
  interval daily
  data ohlcv

on each bar
  skip when portfolio.exposure_pct > 0.50
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("ctx.portfolio.exposure_pct"),
        "Should rewrite portfolio.exposure_pct to ctx.portfolio.exposure_pct.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_portfolio_in_skip_when() {
    let dsl = r#"
strategy "Portfolio Skip"
  symbol SPY
  interval daily
  data ohlcv

on each bar
  skip when portfolio.drawdown < -0.10
  skip when portfolio.net_delta > 100
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("ctx.portfolio.drawdown"),
        "Should rewrite portfolio.drawdown.\nGenerated:\n{rhai}"
    );
    assert!(
        rhai.contains("ctx.portfolio.net_delta"),
        "Should rewrite portfolio.net_delta.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_portfolio_in_when_then() {
    let dsl = r#"
strategy "Portfolio Guard"
  symbol SPY
  interval daily
  data ohlcv

on each bar
  when portfolio.long_count >= 5 then
    hold position
  otherwise
    buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("ctx.portfolio.long_count >= 5"),
        "Should rewrite portfolio.long_count in when condition.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_portfolio_unknown_property_rejected() {
    let dsl = r#"
strategy "Bad Portfolio"
  symbol SPY
  interval daily
  data ohlcv

on each bar
  skip when portfolio.foo > 1
  buy 100 shares
"#;

    let err = transpile(dsl).unwrap_err();
    assert!(
        err.message.contains("portfolio") && err.message.contains("foo"),
        "Should mention unknown portfolio property.\nGot: {}",
        err.message
    );
}

#[test]
fn test_portfolio_assignment_rejected() {
    let dsl = r#"
strategy "Bad Assignment"
  symbol SPY
  interval daily
  data ohlcv

on each bar
  set portfolio.cash to 1000
  buy 100 shares
"#;

    let err = transpile(dsl).unwrap_err();
    assert!(
        err.message.contains("read-only") || err.message.contains("cannot assign"),
        "Should reject assignment to portfolio.\nGot: {}",
        err.message
    );
}

// ---------------------------------------------------------------------------
// Quantifier tests
// ---------------------------------------------------------------------------

#[test]
fn test_when_any_leg_condition() {
    let dsl = r#"
strategy "Delta Check"
  symbol SPY
  interval daily
  data ohlcv, options

on exit check
  when any leg in pos.legs has delta > 0.50 then
    close position "delta too high"
  otherwise
    hold position
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("__any_match"),
        "Should generate __any_match variable.\nGenerated:\n{rhai}"
    );
    assert!(
        rhai.contains("leg.delta > 0.50"),
        "Should qualify delta with leg.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_when_all_legs_condition() {
    let dsl = r#"
strategy "All Legs Check"
  symbol SPY
  interval daily
  data ohlcv, options

on exit check
  when all legs in pos.legs have current_price < 0.05 then
    close position "all legs worthless"
  otherwise
    hold position
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("__all_match"),
        "Should generate __all_match variable.\nGenerated:\n{rhai}"
    );
    assert!(
        rhai.contains("legs.current_price < 0.05"),
        "Should check current_price on each leg.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_when_any_with_binding() {
    let dsl = r#"
strategy "Binding Test"
  symbol SPY
  interval daily
  data ohlcv, options

on exit check
  when any leg in pos.legs has delta > 0.50 as hot_leg then
    close position "hot leg"
  otherwise
    hold position
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("let hot_leg = ();"),
        "Should declare hot_leg.\nGenerated:\n{rhai}"
    );
    assert!(
        rhai.contains("hot_leg = leg;"),
        "Should capture leg into hot_leg.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_legs_sum_aggregation() {
    let dsl = r#"
strategy "Sum Delta"
  symbol SPY
  interval daily
  data ohlcv, options

on exit check
  when pos.legs.sum(delta) > 1.0 then
    close position "net delta too high"
  otherwise
    hold position
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("__agg_"),
        "Should generate uniquely suffixed aggregation variable.\nGenerated:\n{rhai}"
    );
    assert!(
        rhai.contains(".delta"),
        "Should access .delta on each leg.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_legs_count_aggregation() {
    let dsl = r#"
strategy "Count Legs"
  symbol SPY
  interval daily
  data ohlcv, options

on exit check
  when pos.legs.count(side == "long") > 2 then
    close position "too many long legs"
  otherwise
    hold position
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("__agg_"),
        "Should generate uniquely suffixed count variable.\nGenerated:\n{rhai}"
    );
    assert!(
        rhai.contains(".side == \"long\""),
        "Should qualify side with __el_N.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_legs_min_max_avg_aggregation() {
    let dsl = r#"
strategy "Min Price"
  symbol SPY
  interval daily
  data ohlcv, options

on exit check
  when pos.legs.min(current_price) < 0.05 then
    close position "a leg is nearly worthless"
  otherwise
    hold position
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains(".current_price"),
        "Should access current_price on __el_N.\nGenerated:\n{rhai}"
    );
    assert!(
        rhai.contains("__agg_"),
        "Should generate uniquely suffixed min aggregation variable.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_multiple_aggregations_in_same_expression() {
    let dsl = r#"
strategy "Multi Agg"
  symbol SPY
  interval daily
  data ohlcv, options

on exit check
  when pos.legs.sum(delta) + pos.legs.max(strike) > 100 then
    close position "combined check"
  otherwise
    hold position
"#;

    let rhai = transpile(dsl).unwrap();
    // Both should expand with different suffixes
    assert!(
        rhai.contains("__agg_0") || rhai.contains("__agg_1"),
        "Should use unique suffixes for each aggregation.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_quantifier_outside_pos_scope_rejected() {
    let dsl = r#"
strategy "Bad Quantifier"
  symbol SPY
  interval daily
  data ohlcv, options

on each bar
  when any leg in pos.legs has delta > 0.50 then
    close position "bad"
"#;

    let err = transpile(dsl).unwrap_err();
    assert!(
        err.message.contains("pos") && err.message.contains("scope"),
        "Should mention pos scope in error.\nGot: {}",
        err.message
    );
}

#[test]
fn test_quantifier_inside_for_each_pos_allowed() {
    let dsl = r#"
strategy "Nested Quantifier"
  symbol SPY
  interval daily
  data ohlcv, options

on each bar
  for each pos in positions
    when any leg in pos.legs has delta > 0.50 then
      close position "delta too high"
    otherwise
      hold position
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("__any_match"),
        "Should generate quantifier loop inside for-each.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_invalid_leg_field_rejected() {
    let dsl = r#"
strategy "Bad Field"
  symbol SPY
  interval daily
  data ohlcv, options

on exit check
  when any leg in pos.legs has foo > 1 then
    close position "bad"
"#;

    let err = transpile(dsl).unwrap_err();
    assert!(
        err.message.contains("foo"),
        "Should mention unknown field.\nGot: {}",
        err.message
    );
}

#[test]
fn test_aggregation_on_non_numeric_field_rejected() {
    let dsl = r#"
strategy "Bad Agg"
  symbol SPY
  interval daily
  data ohlcv, options

on exit check
  when pos.legs.sum(option_type) > 1 then
    close position "bad"
  otherwise
    hold position
"#;

    let err = transpile(dsl).unwrap_err();
    assert!(
        err.message.contains("numeric") && err.message.contains("option_type"),
        "Should reject sum on non-numeric field.\nGot: {}",
        err.message
    );
}

#[test]
fn test_portfolio_and_quantifier_together() {
    let dsl = r#"
strategy "Combined"
  symbol SPY
  interval daily
  data ohlcv, options

on each bar
  skip when portfolio.exposure_pct > 0.50
  buy 100 shares

on exit check
  when any leg in pos.legs has delta > 0.50 then
    close position "delta too high"
  otherwise
    hold position
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("ctx.portfolio.exposure_pct > 0.50"));
    assert!(rhai.contains("__any_match"));
}

#[test]
fn test_avg_aggregation() {
    let dsl = r#"
strategy "Avg Delta"
  symbol SPY
  interval daily
  data ohlcv, options

on exit check
  when pos.legs.avg(delta) > 0.30 then
    close position "avg delta too high"
  otherwise
    hold position
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("__sum_") && rhai.contains("__cnt_"),
        "Should generate uniquely suffixed avg aggregation.\nGenerated:\n{rhai}"
    );
}
