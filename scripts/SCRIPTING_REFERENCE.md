# Rhai Scripting Reference

Guide for writing `.rhai` backtest scripts. The AI agent should reference this when generating scripts from user prompts.

## Script Structure

Every script must define `config()` and `on_bar(ctx)`. Other callbacks are optional.

```rhai
// Top-level state variables (persisted across bars via Rhai scope)
let state = "initial";
let counter = 0;

fn config() {
    #{ symbol: params.SYMBOL, capital: params.CAPITAL, ... }
}

fn on_bar(ctx) {
    // Entry logic — return array of actions or []
    []
}

fn on_exit_check(ctx, pos) {
    // Per-position exit logic — return close or hold
    #{ action: "hold" }
}

fn on_position_closed(ctx, pos, exit_type) {
    // Fires after any position close — state transitions, tracking
}

fn on_position_opened(ctx, pos) {
    // Fires after a new position opens — logging, adjustments
}

fn on_end(ctx) {
    // Fires once after the last bar — return custom metadata map (optional)
}
```

### Callback Execution Order Per Bar

```
Phase A: Exits
  1. Auto-check: options at/past expiration → classify as expiration/assignment/called_away
  2. on_exit_check(ctx, pos) called for each open position (oldest first)
  3. Closed positions → on_position_closed(ctx, pos, exit_type)

Phase B: Entries
  4. on_bar(ctx) called with fresh context (positions updated from Phase A)
  5. Actions processed: open_spread, open_stock, open_options, close, stop
  6. Opened positions → on_position_opened(ctx, pos)

Phase C: Bookkeeping
  7. Mark-to-market all open positions (options via PriceTable, stocks via close)
  8. Update days_held, current_date for each position
  9. Record equity curve point
```

### Top-Level Variables

Variables declared with `let` at the top level persist across all bars and callbacks.
The Rhai scope is checkpointed before each callback and rewound after — local variables
inside callbacks are cleaned up, but mutations to top-level variables persist.

```rhai
let counter = 0;        // persists across bars
let state = "initial";  // can be mutated in any callback

fn on_bar(ctx) {
    counter += 1;        // mutation persists
    let temp = 42;       // cleaned up after this call returns
    []
}
```

## config() Return Shape

```rhai
fn config() {
    #{
        symbol: params.SYMBOL,       // required: ticker symbol
        capital: params.CAPITAL,     // required: starting equity
        start_date: "2020-01-01",    // optional
        end_date: "2024-12-31",      // optional
        interval: "daily",           // "daily", "1m", "5m", "15m", "1h", etc.
        auto_close_on_end: false,    // close all positions at end? (default: false)
        multiplier: 100,             // contract multiplier (default: 100)
        data: #{
            ohlcv: true,
            options: true,           // set true for options strategies
            cross_symbols: ["VIX"],  // other symbols for ctx.price_of()
            indicators: ["sma:20", "rsi:14", "atr:14", "macd_line", "bbands_upper:20"],
        },
        engine: #{
            slippage: "mid",                    // "mid", "spread", #{ type: "per_leg", per_leg: 0.05 }
            commission: #{ per_contract: 0.65, base_fee: 0.0, min_fee: 0.0 },
            expiration_filter: "any",           // "any", "weekly", "monthly"
            min_days_between_entries: 0,
            trade_selector: "nearest",          // "nearest", "highest_premium", "lowest_premium"
        },
        defaults: #{
            max_positions: 3,       // script checks this — NOT engine-enforced
            stop_loss: 0.50,        // script checks this — NOT engine-enforced
            take_profit: 0.80,      // script checks this — NOT engine-enforced
        },
    }
}
```

## ctx Object — Available Methods

### Bar Data
| Method | Returns | Description |
|--------|---------|-------------|
| `ctx.date` | String | Current date (YYYY-MM-DD) |
| `ctx.datetime` | String | Current datetime (ISO 8601) |
| `ctx.open` | f64 | Current bar open price |
| `ctx.high` | f64 | Current bar high price |
| `ctx.low` | f64 | Current bar low price |
| `ctx.close` | f64 | Current bar close price |
| `ctx.volume` | f64 | Current bar volume |
| `ctx.bar_idx` | i64 | Bar index (0-based) |
| `ctx.price(n)` | f64 or () | Close price n bars ago. Returns () if n > bar_idx |

### Historical Bar Lookback (MQL4-inspired)
Access OHLCV values N bars ago (0 = current bar). Returns `()` if out of range.

| Method | Returns | Description |
|--------|---------|-------------|
| `ctx.high(n)` | f64 or () | High price N bars ago |
| `ctx.low(n)` | f64 or () | Low price N bars ago |
| `ctx.open(n)` | f64 or () | Open price N bars ago |
| `ctx.close(n)` | f64 or () | Close price N bars ago |
| `ctx.volume(n)` | f64 or () | Volume N bars ago |

Note: `ctx.high` (no args) returns current bar's high via getter. `ctx.high(0)` also returns current bar's high. `ctx.high(5)` returns the high from 5 bars ago.

### Range Queries (MQL4-inspired iHighest/iLowest)
| Method | Returns | Description |
|--------|---------|-------------|
| `ctx.highest_high(period)` | f64 | Max high over last `period` bars (including current) |
| `ctx.lowest_low(period)` | f64 | Min low over last `period` bars |
| `ctx.highest_close(period)` | f64 | Max close over last `period` bars |
| `ctx.lowest_close(period)` | f64 | Min close over last `period` bars |

### Portfolio
| Method | Returns | Description |
|--------|---------|-------------|
| `ctx.cash` | f64 | Available cash |
| `ctx.equity` | f64 | Total portfolio value (cash + unrealized) |
| `ctx.unrealized_pnl` | f64 | Sum of unrealized P&L across all open positions |
| `ctx.realized_pnl` | f64 | Realized P&L (equity - starting capital) |
| `ctx.total_exposure` | f64 | Sum of abs(entry_cost) across all open positions |
| `ctx.positions()` | Array | All open positions |
| `ctx.position_count` | i64 | Count of script-opened positions (excludes implicit) |
| `ctx.has_positions()` | bool | True if any script-opened positions exist |

### Indicators (current bar)
All require declaration in `config().data.indicators`.

| Method | Returns | Description |
|--------|---------|-------------|
| `ctx.sma(period)` | f64 or () | Simple Moving Average |
| `ctx.ema(period)` | f64 or () | Exponential Moving Average |
| `ctx.rsi(period)` | f64 or () | Relative Strength Index (0-100) |
| `ctx.atr(period)` | f64 or () | Average True Range |
| `ctx.macd_line()` | f64 or () | MACD line (defaults: 12, 26, 9) |
| `ctx.macd_signal()` | f64 or () | MACD signal line |
| `ctx.macd_hist()` | f64 or () | MACD histogram |
| `ctx.bbands_upper(period)` | f64 or () | Bollinger upper band (std=2.0) |
| `ctx.bbands_mid(period)` | f64 or () | Bollinger middle band (SMA) |
| `ctx.bbands_lower(period)` | f64 or () | Bollinger lower band |
| `ctx.stochastic(period)` | f64 or () | Stochastic %K |
| `ctx.cci(period)` | f64 or () | Commodity Channel Index |
| `ctx.obv()` | f64 or () | On-Balance Volume (cumulative) |
| `ctx.indicator(name, period)` | f64 or () | Generic accessor |

**Custom parameter overloads:**
| Method | Description |
|--------|-------------|
| `ctx.macd_line_custom(fast, slow, signal)` | MACD with custom periods |
| `ctx.bbands_upper_custom(period, std_dev)` | BBands with custom std dev |
| `ctx.stochastic_custom(k_period, d_smooth)` | Stochastic with custom smoothing |

### Indicator Lookback (for crossover detection)
| Method | Returns | Description |
|--------|---------|-------------|
| `ctx.sma_at(period, bars_ago)` | f64 or () | SMA N bars ago |
| `ctx.ema_at(period, bars_ago)` | f64 or () | EMA N bars ago |
| `ctx.rsi_at(period, bars_ago)` | f64 or () | RSI N bars ago |
| `ctx.indicator_at(name, period, bars_ago)` | f64 or () | Any indicator N bars ago |
| `ctx.crossed_above("sma:20", "sma:50")` | bool | True if first crossed above second this bar |
| `ctx.crossed_below("sma:20", "sma:50")` | bool | True if first crossed below second this bar |

### Indicator Utility
| Method | Returns | Description |
|--------|---------|-------------|
| `ctx.indicators_ready(["sma:50", "rsi:14"])` | bool | True if all listed indicators have valid (non-NaN) values at current bar |

Replaces repeated null-check boilerplate:
```rhai
// Before: verbose null checks
let sma50 = ctx.sma(50); let rsi = ctx.rsi(14);
if sma50 == () || rsi == () { return []; }

// After: one call
if !ctx.indicators_ready(["sma:50", "rsi:14"]) { return []; }
```

### Options Strategy Helpers

Named helpers that build and wrap strategies into ready-to-use action maps. All return an action map or `()` if leg resolution fails. Use directly in the array returned from `on_bar()`.

#### Singles
| Method | Description |
|--------|-------------|
| `ctx.long_call(call_delta, dte)` | Buy one call |
| `ctx.short_call(call_delta, dte)` | Sell one call |
| `ctx.long_put(put_delta, dte)` | Buy one put |
| `ctx.short_put(put_delta, dte)` | Sell one put |
| `ctx.covered_call(call_delta, dte)` | Sell one call (assumes stock held) |

#### Vertical Spreads
| Method | Description |
|--------|-------------|
| `ctx.bull_call_spread(long_call_delta, short_call_delta, dte)` | Buy call + sell higher call |
| `ctx.bear_call_spread(short_call_delta, long_call_delta, dte)` | Sell call + buy higher call |
| `ctx.bull_put_spread(short_put_delta, long_put_delta, dte)` | Sell put + buy lower put |
| `ctx.bear_put_spread(long_put_delta, short_put_delta, dte)` | Buy put + sell lower put |

#### Straddles & Strangles
| Method | Description |
|--------|-------------|
| `ctx.long_straddle(call_delta, put_delta, dte)` | Buy call + buy put |
| `ctx.short_straddle(call_delta, put_delta, dte)` | Sell call + sell put |
| `ctx.long_strangle(put_delta, call_delta, dte)` | Buy OTM put + buy OTM call |
| `ctx.short_strangle(put_delta, call_delta, dte)` | Sell OTM put + sell OTM call |

#### Butterflies
| Method | Description |
|--------|-------------|
| `ctx.long_call_butterfly(lower_call_delta, center_call_delta, upper_call_delta, dte)` | Long wing + 2x short center + long wing (calls) |
| `ctx.short_call_butterfly(lower_call_delta, center_call_delta, upper_call_delta, dte)` | Short wing + 2x long center + short wing (calls) |
| `ctx.long_put_butterfly(lower_put_delta, center_put_delta, upper_put_delta, dte)` | Long wing + 2x short center + long wing (puts) |
| `ctx.short_put_butterfly(lower_put_delta, center_put_delta, upper_put_delta, dte)` | Short wing + 2x long center + short wing (puts) |

#### Condors (same option type)
| Method | Description |
|--------|-------------|
| `ctx.long_call_condor(outer_lower_call_delta, inner_lower_call_delta, inner_upper_call_delta, outer_upper_call_delta, dte)` | 4-leg all-call condor |
| `ctx.short_call_condor(outer_lower_call_delta, inner_lower_call_delta, inner_upper_call_delta, outer_upper_call_delta, dte)` | Inverted 4-leg call condor |
| `ctx.long_put_condor(outer_lower_put_delta, inner_lower_put_delta, inner_upper_put_delta, outer_upper_put_delta, dte)` | 4-leg all-put condor |
| `ctx.short_put_condor(outer_lower_put_delta, inner_lower_put_delta, inner_upper_put_delta, outer_upper_put_delta, dte)` | Inverted 4-leg put condor |

#### Iron Strategies (mixed put + call)
| Method | Description |
|--------|-------------|
| `ctx.iron_condor(short_put_delta, long_put_delta, short_call_delta, long_call_delta, dte)` | Sell put spread + sell call spread |
| `ctx.reverse_iron_condor(long_put_delta, short_put_delta, long_call_delta, short_call_delta, dte)` | Buy put spread + buy call spread |
| `ctx.iron_butterfly(short_put_delta, long_put_delta, short_call_delta, long_call_delta, dte)` | Sell ATM straddle + buy OTM wings |
| `ctx.reverse_iron_butterfly(long_put_delta, short_put_delta, long_call_delta, short_call_delta, dte)` | Buy ATM straddle + sell OTM wings |

#### Calendar & Diagonal (multi-expiration)
| Method | Description |
|--------|-------------|
| `ctx.call_calendar(near_call_delta, far_call_delta, near_dte, far_dte)` | Short near call + long far call |
| `ctx.put_calendar(near_put_delta, far_put_delta, near_dte, far_dte)` | Short near put + long far put |
| `ctx.call_diagonal(short_call_delta, long_call_delta, near_dte, far_dte)` | Short near call + long far call (diff deltas) |
| `ctx.put_diagonal(short_put_delta, long_put_delta, near_dte, far_dte)` | Short near put + long far put (diff deltas) |
| `ctx.double_calendar(near_put_delta, far_put_delta, near_call_delta, far_call_delta, near_dte, far_dte)` | Put calendar + call calendar |
| `ctx.double_diagonal(short_put_delta, long_put_delta, short_call_delta, long_call_delta, near_dte, far_dte)` | Put diagonal + call diagonal |

#### Usage Examples
```rhai
// Bull put spread — returns ready action map
let spread = ctx.bull_put_spread(0.30, 0.15, 45);
if spread == () { return []; }
[spread]

// Iron condor with per-leg deltas
let ic = ctx.iron_condor(0.30, 0.10, 0.30, 0.10, 45);
if ic == () { return []; }
[ic]

// Multiple positions in one bar
let put = ctx.short_put(0.25, 30);
let call = ctx.short_call(0.25, 30);
if put == () || call == () { return []; }
[put, call]
```

### Low-Level Strategy Builder

For custom leg combinations not covered by the helpers above, `ctx.build_strategy(legs)` accepts an array of leg maps. You must wrap the result in an action map manually.

| Method | Returns | Description |
|--------|---------|-------------|
| `ctx.build_strategy(legs)` | Map or () | Build any options strategy from a legs array |

```rhai
let strat = ctx.build_strategy([
    #{ side: "short", option_type: "put", delta: 0.30, dte: 45 },
    #{ side: "long", option_type: "put", delta: 0.15, dte: 45 },
]);
if strat != () {
    [#{ action: "open_spread", spread: strat }]
}
```

### Cross-Symbol
| Method | Returns | Description |
|--------|---------|-------------|
| `ctx.price_of(symbol)` | f64 or () | Close price of another symbol (forward-filled) |
| `ctx.price_of_col(symbol, col)` | f64 or () | Specific column: "open", "high", "low", "close", "volume" |

## Action Helpers (returned by on_bar / on_exit_check)

Global helper functions that return ready-to-use action maps:

| Function | Returns | Use in |
|----------|---------|--------|
| `hold_position()` | `#{ action: "hold" }` | on_exit_check |
| `close_position(reason)` | `#{ action: "close", reason }` | on_exit_check, on_bar |
| `close_position_id(id, reason)` | `#{ action: "close", position_id: id, reason }` | on_bar |
| `stop_backtest(reason)` | `#{ action: "stop", reason }` | on_bar, on_exit_check |
| `buy_stock(qty)` | `#{ action: "open_stock", side: "long", qty }` | on_bar |
| `sell_stock(qty)` | `#{ action: "open_stock", side: "short", qty }` | on_bar |

```rhai
// on_bar — entry logic
fn on_bar(ctx) {
    // Open a bull put spread
    let spread = ctx.bull_put_spread(0.30, 0.15, 45);
    if spread == () { return []; }
    [spread]

    // Open stock
    [buy_stock(100)]

    // Close position by ID
    [close_position_id(pos.id, "take_profit")]

    // Stop backtest
    [stop_backtest("capital_depleted")]

    // No action
    []
}

// on_exit_check — per-position exit logic
fn on_exit_check(ctx, pos) {
    if pos.pnl_pct > 0.50 { return close_position("take_profit"); }
    if pos.pnl_pct < -2.0 { return close_position("stop_loss"); }
    hold_position()
}
```

### Low-Level Action Maps (still supported)

The helpers above return these maps. You can also construct them directly:

```rhai
#{ action: "open_spread", spread: ctx.build_strategy([...]) }
#{ action: "open_stock", side: "long", qty: 100 }
#{ action: "close", position_id: pos.id, reason: "take_profit" }
#{ action: "stop", reason: "capital_depleted" }
#{ action: "hold" }
```

## pos Object (in on_exit_check / on_position_closed)

| Property | Type | Description |
|----------|------|-------------|
| `pos.id` | i64 | Position ID |
| `pos.entry_date` | String | Entry date (YYYY-MM-DD) |
| `pos.expiration` | String or () | Expiration date (options) or () (stock) |
| `pos.dte` | i64 or () | Days to expiration (options only) |
| `pos.entry_cost` | f64 | Entry cost (negative = credit received) |
| `pos.unrealized_pnl` | f64 | Current unrealized P&L |
| `pos.pnl_pct` | f64 | P&L as fraction of abs(entry_cost) |
| `pos.days_held` | i64 | Days since entry |
| `pos.legs` | Array or () | Leg maps (options) or () (stock) |
| `pos.side` | String or () | "long"/"short" (stock) or () (options) |
| `pos.is_options` | bool | True if options position |
| `pos.is_stock` | bool | True if stock position |
| `pos.source` | String | "script" or "assignment" |

**pos.legs element fields:**
`#{ strike, option_type, side, expiration, entry_price, current_price, delta, qty }`

## exit_type Values (in on_position_closed)

| Value | Trigger |
|-------|---------|
| `"expiration"` | Options expired OTM (all legs out-of-the-money) |
| `"assignment"` | Short put expired ITM — stock assigned at strike (engine auto-creates implicit stock) |
| `"called_away"` | Short call expired ITM — stock sold at strike (engine auto-closes implicit stock) |
| `"take_profit"` | Script returned `reason: "take_profit"` in on_exit_check |
| `"stop_loss"` | Script returned `reason: "stop_loss"` in on_exit_check |
| `"dte_exit"` | Script returned `reason: "dte_exit"` in on_exit_check |
| `"signal"` | Script returned a custom reason string (or `reason: "signal"`) |
| `"max_hold"` | Script returned `reason: "max_hold"` in on_exit_check |
| `"delta_exit"` | Script returned `reason: "delta_exit"` in on_exit_check |
| `"end_of_data"` | Backtest ended with positions still open (auto_close_on_end or final bar) |

## Parameter Injection

Parameters are injected as an immutable `params` map in the script scope. Scripts access values via `params.SYMBOL`, `params.CAPITAL`, etc. The `params` map is available in all callbacks (`config()`, `on_bar()`, `on_exit_check()`, etc.).

```rhai
fn config() {
    #{ symbol: params.SYMBOL, capital: params.CAPITAL }
}
```

**Optional params:** Use `!= ()` to check. Callers must pass `null` for unset optional params (stored as `()` in the map).
```rhai
if params.STOP_LOSS != () && pos.pnl_pct < -params.STOP_LOSS {
    return #{ action: "close", reason: "stop_loss" };
}
```

The `params` map is read-only — scripts cannot reassign its values.

## Indicator Declaration

All indicators used in callbacks must be declared in `config().data.indicators`:

```rhai
data: #{
    indicators: [
        "sma:20",           // SMA with period 20
        "rsi:14",           // RSI with period 14
        "macd_line",        // MACD defaults (12, 26, 9)
        "bbands_upper:20",  // Bollinger upper, period 20, std 2.0
        "stochastic:14",    // Stochastic %K, period 14
        "obv",              // On-Balance Volume (no period)
    ],
},
```

Undeclared indicators return () at runtime.

## config() Defaults

When optional config fields are omitted or set to `()`, the engine uses these defaults:

| Field | Default |
|-------|---------|
| `interval` | `"daily"` |
| `multiplier` | `100` |
| `timeout_secs` | `60` |
| `auto_close_on_end` | `false` |
| `data.ohlcv` | `true` |
| `data.options` | `false` |
| `engine.slippage` | `"mid"` |
| `engine.expiration_filter` | `"any"` |
| `engine.trade_selector` | `"nearest"` |

## Examples

See `scripts/strategies/` for complete examples:
- `wheel.rhai` — Stateful wheel strategy (put selling → assignment → covered calls)
- `mean_reversion_spread.rhai` — Bull put spreads on RSI dips with volatility-adaptive delta, multiple indicators, and dynamic exits
