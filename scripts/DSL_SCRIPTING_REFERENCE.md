# Trading DSL — Scripting Reference

Guide for AI agents generating `.trading` scripts. Use this as the authoritative
reference for what the DSL can and cannot express.

## When to Use the DSL vs Raw Rhai

| Use DSL (`.trading`) when... | Use Rhai (`.rhai`) when... |
|------------------------------|---------------------------|
| Simple entry/exit conditions | State machines (wheel strategy) |
| Indicator-based filters | Array/map construction |
| Standard position sizing | Iterating over positions or legs |
| Single-instrument strategies | Accessing `pos.legs[0].strike` |
| Agent-generated scripts | String interpolation |
| Non-technical user prompts | Complex conditional assignments |

**Rule of thumb**: if the strategy logic fits into "when X, do Y", use DSL.
If it needs loops, arrays, or nested data structures, use Rhai with `raw` blocks
or write a full `.rhai` script.

## File Structure

```
# Comments start with hash
strategy "Name"
  symbol SYMBOL        # or a literal like SPY
  capital CAPITAL      # or a literal like 100000
  interval daily
  data ohlcv           # or: ohlcv, options
  indicators sma:50, sma:200, rsi:14

param NAME = DEFAULT "description"
state NAME = DEFAULT

on each bar
  STATEMENTS...

on exit check
  STATEMENTS...

on position opened
  STATEMENTS...

on position closed
  STATEMENTS...

on end
  STATEMENTS...
```

## Strategy Block

Required. Must be the first non-comment declaration.

```
strategy "Iron Condor Income"
  symbol SPY                          # quoted as "SPY" in generated Rhai
  capital CAPITAL                     # maps to params.CAPITAL
  interval daily                      # daily|1min|5min|15min|30min|1h|2h|4h
  data ohlcv, options                 # ohlcv|options (comma-separated)
  indicators rsi:14, atr:14, sma:50   # comma-separated indicator specs
  slippage mid                        # mid|spread|per_leg:N
  expiration_filter monthly           # monthly|weekly|any
  max_positions 1                     # integer
  cross_symbols QQQ, IWM             # for price_of() access
```

All properties except `symbol` and `capital` are optional (sensible defaults apply).

## Parameters and State

```
param THRESHOLD = 0.04 "Entry threshold percentage"
param MODE = "fast" "Execution mode" choices fast, slow, balanced
state consecutive_losses = 0
state in_trade = false
```

- `param` → generates `extern()` call (visible in UI, overridable at runtime)
- `state` → generates `let` (top-level, persists across bars)
- State variables can be mutated with `set NAME to EXPR` or `add EXPR to NAME`
- Params are read-only after initialization

## Statements

### Control Flow

```
require sma:50, sma:200                    # early return if indicators not ready
skip when has positions                     # early return if condition is true
skip when rsi(14) > 70 or close < 50       # compound conditions work

when close > sma(200) then                 # if block
  buy 100 shares

when COND1 then                            # if/else-if/else chain
  ACTION1                                  # (only chains when `otherwise` present)
when COND2 then
  ACTION2
otherwise
  ACTION3
```

**Important**: consecutive `when` blocks WITHOUT `otherwise` produce **independent**
`if` statements (both can execute). With `otherwise`, they chain into `if/else if/else`.

### Actions

```
buy 100 shares                             # buy_stock(100)
buy size_by_equity(1.0) shares             # dynamic sizing
sell 50 shares                             # sell_stock(50) with qty > 0 guard
hold position                              # hold_position()
close position "take_profit"               # close_position("take_profit")
close position pos.id "manual_close"       # close_position_id(id, reason)
stop backtest "margin_call"                # stop_backtest(reason)
open iron_condor(0.20, 0.20, 45)           # strategy call with null check
open bull_put_spread(0.30, 0.15, 45)       # any ctx strategy method
```

### Variables and Plotting

```
set upper to sma(200) * 1.05               # local variable (let)
set consecutive_losses to 0                 # state reassignment (no let)
add 1 to counter                           # counter += 1
plot "Upper Band" at upper                  # ctx.plot("Upper Band", upper)
plot "RSI" at rsi(14) as subchart           # ctx.plot_with(..., "subchart")
```

### Escape Hatch

```
raw let x = #{ key: "value" };             # arbitrary Rhai (not rewritten)
raw let legs = pos.legs;                   # access complex structures
raw if legs.len() > 0 { let strike = legs[0].strike; }
```

Use `raw` for anything the DSL can't express natively.

## Expression Rules

Expressions are automatically rewritten — you never write `ctx.` in the DSL:

| DSL Expression | Generated Rhai |
|----------------|---------------|
| `close` | `ctx.close` |
| `sma(200)` | `ctx.sma(200)` |
| `rsi(14) < 30` | `ctx.rsi(14) < 30` |
| `has positions` | `ctx.has_positions()` |
| `no positions` | `!ctx.has_positions()` |
| `A and B` | `A && B` |
| `A or B` | `A \|\| B` |
| `not A` | `!A` |
| `pos.pnl_pct` | `pos.pnl_pct` (unchanged) |
| `MY_PARAM * 2` | `MY_PARAM * 2` (unchanged) |

### Rewritten Identifiers

**Properties** (no parens): `close`, `open`, `high`, `low`, `volume`, `cash`,
`equity`, `position_count`, `unrealized_pnl`, `realized_pnl`, `total_exposure`,
`bar_idx`, `date`, `datetime`

**Methods** (with parens): All indicator functions (`sma`, `ema`, `rsi`, `atr`,
`macd_line`, `bbands_upper`, etc.), all strategy constructors (`iron_condor`,
`bull_put_spread`, etc.), position sizing (`size_by_equity`, `size_by_risk`,
`size_by_volatility`, `size_by_kelly`), cross-symbol (`price_of`, `price_of_col`),
range queries (`highest_high`, `lowest_low`), crossovers (`crossed_above`,
`crossed_below`), and date/time (`day_of_week`, `month`, etc.)

### What Is NOT Rewritten

- Anything after a dot: `pos.pnl_pct`, `pos.days_held`
- User variables: `THRESHOLD`, `my_counter`, `state`
- Anything inside quotes: `"close is high"` stays as-is
- Standard operators: `+`, `-`, `*`, `/`, `>`, `<`, `>=`, `<=`, `==`, `!=`

## Available Context Methods

### Indicators (declare in `indicators` first)
```
sma(period)            ema(period)            rsi(period)
atr(period)            cci(period)            obv()
macd_line()            macd_signal()          macd_hist()
bbands_upper(period)   bbands_mid(period)     bbands_lower(period)
stochastic(period)     adx(period)            psar()
supertrend()           williams_r(period)     mfi(period)
keltner_upper(period)  keltner_lower(period)
donchian_upper(period) donchian_mid(period)   donchian_lower(period)
rank(period)           iv_rank(period)        tr()
```

### Lookback and Crossovers
```
sma_at(period, bars_ago)         ema_at(period, bars_ago)
rsi_at(period, bars_ago)         indicator_at(name, period, bars_ago)
crossed_above("sma:20", "sma:50")
crossed_below("sma:20", "sma:50")
high(n)  low(n)  open(n)  close(n)  volume(n)    # N bars ago
highest_high(period)   lowest_low(period)
highest_close(period)  lowest_close(period)
```

### Position Sizing
```
size_by_equity(fraction)                # fraction of equity (1.0 = 100%)
size_by_risk(risk_pct, stop_price)      # risk % of equity per trade
size_by_volatility(target_risk, period) # target $ risk per ATR move
size_by_kelly(fraction, lookback)       # Kelly criterion (needs 20+ trades)
```

### Strategy Constructors (options)
All return an action or `()` if resolution fails. Use with `open`:
```
long_call(delta, dte)              short_call(delta, dte)
long_put(delta, dte)               short_put(delta, dte)
covered_call(delta, dte)
bull_call_spread(long_d, short_d, dte)
bear_call_spread(short_d, long_d, dte)
bull_put_spread(short_d, long_d, dte)
bear_put_spread(long_d, short_d, dte)
iron_condor(put_d, call_d, dte)
iron_butterfly(put_d, call_d, dte)
long_straddle(call_d, put_d, dte)
short_straddle(call_d, put_d, dte)
long_strangle(put_d, call_d, dte)
short_strangle(put_d, call_d, dte)
call_calendar(delta, front_dte, back_dte)
put_calendar(delta, front_dte, back_dte)
# ... and more (see SCRIPTING_REFERENCE.md for full list)
```

### Position Properties (in `on exit check` and `on position closed`)
```
pos.pnl_pct          # P&L as fraction of entry cost
pos.days_held        # days since entry
pos.entry_cost       # cost at entry
pos.unrealized_pnl   # current unrealized P&L
pos.id               # position ID (for close_position_id)
pos.entry_date       # entry date string
pos.is_options       # true for options positions
pos.is_stock         # true for stock positions
pos.side             # "long" or "short" (stock only)
pos.source           # "script" or "assignment"
pos.dte              # days to expiration (options only)
pos.expiration       # expiration date (options only)
```

**Note**: `pos.legs` (array of leg maps) is available but accessing individual
legs requires `raw` blocks: `raw let strike = pos.legs[0].strike;`

## Known Limitations

### Cannot Express Without `raw`

1. **Loops**: `for`, `while` — use `raw` for iteration
2. **Array/map literals**: `[1, 2, 3]` or `#{ key: val }` — use `raw`
3. **Array indexing**: `pos.legs[0].strike` — use `raw`
4. **String interpolation**: `` `text ${var}` `` — use `raw` or `+` concatenation
5. **Compound assignments**: only `+=` via `add`; no `-=`, `*=`, `/=` — use `raw`
6. **Complex conditional assignments**: `let x = if cond { a } else { b }` — use `raw`
7. **Method chaining**: `value.to_string()` — use `raw`
8. **Function definitions**: user-defined functions — use `raw` or Rhai
9. **Match/pattern matching**: use when/otherwise chains or `raw`
10. **Engine-read variables**: `_group = "Cycle 1"` — use `raw`

### Expression Limitations

- `set NAME to EXPR` fails if EXPR contains ` to ` (e.g., `set x to a to b`)
- Multiline expressions are not supported (each statement is one line)
- Nested parentheses work but complex expressions may be clearer in `raw`

### State Machine Strategies

The wheel strategy and similar state machines are **better written in Rhai**.
The DSL can handle simple state variables (`state mode = "selling_puts"`) but
complex state transitions with map mutations, array access, and conditional
branching are awkward. Use `raw` blocks extensively or use Rhai directly.

## Complete Examples

### Simple Stock Strategy
```
strategy "RSI Mean Reversion"
  symbol SYMBOL
  capital CAPITAL
  interval daily
  data ohlcv
  indicators rsi:14, sma:200

param RSI_ENTRY = 30 "RSI threshold for entry"
param RSI_EXIT = 50 "RSI threshold for exit"

on each bar
  require rsi:14, sma:200
  skip when has positions
  skip when close < sma(200)
  when rsi(14) < RSI_ENTRY then
    buy size_by_equity(0.5) shares

on exit check
  when rsi(14) > RSI_EXIT then
    close position "rsi_exit"
  when pos.days_held > 20 then
    close position "max_hold"
  otherwise
    hold position
```

### Options Strategy
```
strategy "Short Put Spread"
  symbol SPY
  capital CAPITAL
  interval daily
  data ohlcv, options
  indicators rsi:14, atr:14
  slippage mid
  expiration_filter monthly
  max_positions 2

param SHORT_DELTA = 0.25 "Short put delta"
param LONG_DELTA = 0.10 "Long put delta"
param DTE = 45 "Target DTE"

on each bar
  require rsi:14
  skip when has positions
  skip when rsi(14) < 30
  open bull_put_spread(SHORT_DELTA, LONG_DELTA, DTE)

on exit check
  when pos.pnl_pct > 0.50 then
    close position "take_profit"
  when pos.pnl_pct < -1.5 then
    close position "stop_loss"
  otherwise
    hold position
```

### Strategy with State Tracking
```
strategy "Loss-Aware Entry"
  symbol SYMBOL
  capital CAPITAL
  interval daily
  data ohlcv
  indicators sma:50, sma:200

state consecutive_losses = 0
state total_trades = 0

on each bar
  require sma:50, sma:200
  skip when has positions
  skip when consecutive_losses >= 3
  when sma(50) > sma(200) and close > sma(50) then
    buy size_by_equity(0.5) shares

on exit check
  when close < sma(200) then
    close position "below_sma200"
  otherwise
    hold position

on position closed
  add 1 to total_trades
  when pos.pnl < 0 then
    add 1 to consecutive_losses
  otherwise
    set consecutive_losses to 0
```

### Mixed DSL + Raw (advanced)
```
strategy "Adaptive Strategy"
  symbol SPY
  capital CAPITAL
  interval daily
  data ohlcv
  indicators sma:20, atr:14, bbands_upper:20, bbands_lower:20

state regime = "normal"

on each bar
  require sma:20, atr:14
  skip when has positions

  # Use raw for complex conditional assignment
  raw let bb_width = (ctx.bbands_upper(20) - ctx.bbands_lower(20)) / ctx.sma(20);
  raw regime = if bb_width > 0.08 { "volatile" } else { "normal" };

  when regime == "normal" and close > sma(20) then
    buy size_by_risk(0.02, close - atr(14) * 2) shares

on exit check
  when pos.pnl_pct > 0.10 then
    close position "take_profit"
  when pos.days_held > 10 then
    close position "max_hold"
  otherwise
    hold position
```
