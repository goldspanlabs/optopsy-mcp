# Trading DSL — Design Specification

A natural-language, indent-based DSL for the optopsy-mcp backtesting engine.
Transpiles to standard Rhai scripts that the existing engine executes unchanged.

## Architecture

```
┌──────────────────┐     ┌──────────────┐     ┌─────────────────┐
│  .trading file   │────▸│  Transpiler  │────▸│  .rhai source   │
│  (indent-based)  │     │  parser.rs   │     │  (standard Rhai)│
│                  │     │  codegen.rs  │     │                 │
└──────────────────┘     └──────────────┘     └────────┬────────┘
                                                       │
                         ┌──────────────┐              ▼
                         │ Custom Syntax │     ┌─────────────────┐
                         │  syntax.rs   │────▸│  Rhai Engine    │
                         │ (inline DSL) │     │  (run_script)   │
                         └──────────────┘     └─────────────────┘
```

**Layer 1 — Transpiler**: Parses indent-based `.trading` files into an IR,
then generates valid Rhai source. This is the primary mechanism.

**Layer 2 — Custom Syntax**: Registers `register_custom_syntax` patterns
(e.g., `buy 100 shares`) that work inside generated or hand-written Rhai.
Supplementary, not required.

## Design Principles

| Principle | Rationale |
|-----------|-----------|
| No braces or semicolons | Readability for non-technical traders |
| Indent-based blocks | Python-like structure, eliminates bracket matching |
| Keyword-driven | Every line starts with a keyword for unambiguous parsing |
| Agent-friendly | Rigid grammar that LLMs can generate without hallucination |
| Expression rewriting | `close`, `sma(200)` auto-qualify to `ctx.close`, `ctx.sma(200)` |
| 1:1 mapping | Every DSL construct maps to existing Rhai API functions |

## Grammar

### Top-Level Declarations

```
strategy "Name"
  symbol SYMBOL                          # variable or literal
  capital CAPITAL                        # variable or literal
  interval daily                         # daily|1min|5min|15min|30min|1h|2h|4h
  data ohlcv                             # ohlcv|options|ohlcv, options
  indicators sma:50, sma:200, rsi:14     # comma-separated indicator specs
  slippage mid                           # mid|spread|per_leg:N
  expiration_filter monthly              # monthly|weekly|all
  max_positions 1                        # integer
  cross_symbols QQQ, IWM                 # comma-separated symbols

param NAME = DEFAULT "description"
param NAME = DEFAULT "description" choices VAL1, VAL2

state NAME = DEFAULT
```

### Event Blocks

```
on each bar           → fn on_bar(ctx) { ... }
on exit check         → fn on_exit_check(ctx, pos) { ... }
on position opened    → fn on_position_opened(ctx, pos) { ... }
on position closed    → fn on_position_closed(ctx, pos, exit_type) { ... }
on end                → fn on_end(ctx) { ... }
```

### Statements (indented inside event blocks)

| DSL Statement | Generated Rhai |
|---------------|----------------|
| `require ind1, ind2` | `if !ctx.indicators_ready(["ind1", "ind2"]) { return []; }` |
| `skip when EXPR` | `if EXPR { return []; }` |
| `set NAME to EXPR` | `NAME = EXPR;` |
| `when EXPR then` | `if EXPR {` |
| `otherwise` | `} else {` |
| `buy EXPR shares` | `__actions.push(buy_stock(EXPR));` |
| `sell EXPR shares` | Validated sell with guard check |
| `hold position` | `return hold_position();` |
| `close position "reason"` | `return close_position("reason");` |
| `close position ID "reason"` | `return close_position_id(ID, "reason");` |
| `stop backtest "reason"` | `stop_backtest("reason");` |
| `open STRATEGY(args)` | `let __spread = ctx.STRATEGY(args); if __spread != () { ... }` |
| `plot "name" at EXPR` | `ctx.plot("name", EXPR);` |
| `plot "name" at EXPR as subchart` | `ctx.plot_with("name", EXPR, "subchart");` |
| `add EXPR to NAME` | `NAME += EXPR;` |
| `return EXPR` | `return EXPR;` |
| `raw CODE` | `CODE` (escape hatch for arbitrary Rhai) |

### Expression Rewriting

Bare identifiers are auto-qualified with `ctx.` so DSL authors never write
`ctx.` explicitly:

| DSL Expression | Rewritten Rhai |
|----------------|---------------|
| `close` | `ctx.close` |
| `sma(200)` | `ctx.sma(200)` |
| `has positions` | `ctx.has_positions()` |
| `no positions` | `!ctx.has_positions()` |
| `A and B` | `A && B` |
| `A or B` | `A \|\| B` |
| `not A` | `!A` |
| `pos.pnl_pct` | `pos.pnl_pct` (unchanged) |
| `MY_PARAM` | `MY_PARAM` (unchanged) |

Properties rewritten: `close`, `open`, `high`, `low`, `volume`, `cash`,
`equity`, `position_count`, `unrealized_pnl`, `realized_pnl`,
`total_exposure`, `bar_idx`, `date`, `datetime`.

Methods rewritten: all indicator functions, strategy constructors, position
sizing methods, cross-symbol accessors, plotting, and range queries.

### When / Otherwise Chains

Consecutive `when` blocks at the same indent level followed by an optional
`otherwise` form an if/else-if/else chain:

```
when COND1 then
  ACTION1
when COND2 then
  ACTION2
otherwise
  ACTION3
```

Generates:

```rhai
if COND1 {
    ACTION1
} else if COND2 {
    ACTION2
} else {
    ACTION3
}
```

Independent checks use separate `when` blocks without `otherwise`.

## Custom Rhai Syntax Patterns

Registered via `register_custom_syntax` for use in generated or hand-written Rhai:

| Pattern | Tokens | Result |
|---------|--------|--------|
| `buy 100 shares` | `["buy", "$expr$", "shares"]` | `buy_stock(100)` action map |
| `sell 50 shares` | `["sell", "$expr$", "shares"]` | `sell_stock(50)` action map |
| `sell validated 50 shares of ctx` | `["sell", "validated", "$expr$", "shares", "of", "$expr$"]` | Position-validated sell |
| `close position "reason"` | `["close", "position", "$expr$"]` | `close_position("reason")` |
| `hold` | `["hold"]` | `hold_position()` |

## Validation Logic

### Sell Quantity Validation

The DSL's `sell` statement generates a guard that prevents selling invalid quantities:

```rhai
// Generated from: sell 50 shares
let __sell_qty = 50;
if __sell_qty > 0 {
    __actions.push(sell_stock(__sell_qty));
}
```

For runtime validation against actual portfolio holdings, use the custom syntax:

```rhai
// In hand-written Rhai with DSL syntax enabled:
sell validated 50 shares of ctx
```

This inspects `ctx.has_positions()` at runtime and returns `()` (no action)
if no position exists to sell.

The engine layer (`engine.rs`) provides the final safety net — it validates
all actions against the current portfolio state before execution.

### Parse-Time Validation

The transpiler catches structural errors with line numbers:
- Missing `strategy` block
- Duplicate event blocks
- `when` without `then`
- `otherwise` without preceding `when`
- Empty indented blocks
- Unknown keywords

### Type Validation

The generated Rhai is validated by the existing `validate_script()` pipeline:
- Config structure validation
- Indicator declaration checking
- Callback signature verification
- Parameter type validation via `garde`

## Usage

### Rust API

```rust
use optopsy_mcp::scripting::dsl;

// Auto-detect and transpile
let source = std::fs::read_to_string("my_strategy.trading")?;
if dsl::is_trading_dsl(&source) {
    let rhai_source = dsl::transpile(&source)?;
    // Pass rhai_source to run_script_backtest()
}
```

### File Convention

- `.trading` extension for DSL files
- `.rhai` extension for standard Rhai scripts
- Both live in `scripts/strategies/`

## Complete Example

### DSL Source (`sma_crossover.trading`)

```
strategy "SMA Crossover"
  symbol SYMBOL
  capital CAPITAL
  interval daily
  data ohlcv
  indicators sma:50, sma:200, rsi:14

param THRESHOLD = 0.04 "Entry threshold"
state consecutive_losses = 0

on each bar
  require sma:50, sma:200
  skip when has positions
  skip when consecutive_losses >= 3
  when close > sma(200) * (1 + THRESHOLD) and sma(50) > sma(200) then
    buy size_by_equity(1.0) shares

on exit check
  when close < sma(200) then
    close position "below_sma"
  otherwise
    hold position

on position closed
  when pos.pnl < 0 then
    add 1 to consecutive_losses
  otherwise
    set consecutive_losses to 0
```

### Generated Rhai

```rhai
// Auto-generated from Trading DSL — do not edit by hand.

let THRESHOLD = extern("THRESHOLD", 0.04, "Entry threshold");

let consecutive_losses = 0;

fn config() {
    #{
        symbol: params.SYMBOL,
        capital: params.CAPITAL,
        interval: "daily",
        data: #{
            ohlcv: true,
            options: false,
            indicators: ["sma:50", "sma:200", "rsi:14"],
        },
    }
}

fn on_bar(ctx) {
    let __actions = [];
    if !ctx.indicators_ready(["sma:50", "sma:200"]) { return []; }
    if ctx.has_positions() { return []; }
    if consecutive_losses >= 3 { return []; }
    if ctx.close > ctx.sma(200) * (1 + THRESHOLD) && ctx.sma(50) > ctx.sma(200) {
        __actions.push(buy_stock(ctx.size_by_equity(1.0)));
    }
    __actions
}

fn on_exit_check(ctx, pos) {
    if ctx.close < ctx.sma(200) {
        return close_position("below_sma");
    } else {
        return hold_position();
    }
    hold_position()
}

fn on_position_closed(ctx, pos, exit_type) {
    if pos.pnl < 0 {
        consecutive_losses += 1;
    } else {
        consecutive_losses = 0;
    }
}
```
