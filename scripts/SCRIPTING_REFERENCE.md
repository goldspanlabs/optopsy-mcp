# Rhai Scripting Reference

Guide for writing `.rhai` backtest scripts. The AI agent should reference this when generating scripts from user prompts.

## Script Structure

Every script must define `config()` and `on_bar(ctx)`. Other callbacks are optional.

```rhai
// Top-level state variables (persisted across bars)
let state = "initial";
let counter = 0;

fn config() {
    #{ symbol: SYMBOL, capital: CAPITAL, ... }
}

fn on_bar(ctx) {
    // Entry logic — return array of actions
    []
}

fn on_exit_check(ctx, pos) {
    // Per-position exit logic — return close or hold
    #{ action: "hold" }
}

fn on_position_closed(ctx, pos, exit_type) {
    // State transitions (e.g., wheel)
}

fn on_position_opened(ctx, pos) {
    // Post-entry hooks
}

fn on_end(ctx) {
    // Return custom metadata map (optional)
}
```

## config() Return Shape

```rhai
fn config() {
    #{
        symbol: SYMBOL,              // required: ticker symbol
        capital: CAPITAL,            // required: starting equity
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

### Portfolio
| Method | Returns | Description |
|--------|---------|-------------|
| `ctx.cash` | f64 | Available cash |
| `ctx.equity` | f64 | Total portfolio value (cash + unrealized) |
| `ctx.positions()` | Array | All open positions |
| `ctx.position_count()` | i64 | Count of script-opened positions (excludes implicit) |
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

### Options Strategy
| Method | Returns | Description |
|--------|---------|-------------|
| `ctx.build_strategy(legs)` | Map or () | Build any options strategy from a legs array |

Each leg is a map with `side`, `option_type`, `delta`, and `dte`:
```rhai
// Single short put
let strat = ctx.build_strategy([
    #{ side: "short", option_type: "put", delta: 0.30, dte: 45 },
]);

// Iron condor (4 legs)
let strat = ctx.build_strategy([
    #{ side: "short", option_type: "put", delta: 0.30, dte: 45 },
    #{ side: "long", option_type: "put", delta: 0.10, dte: 45 },
    #{ side: "short", option_type: "call", delta: 0.30, dte: 45 },
    #{ side: "long", option_type: "call", delta: 0.10, dte: 45 },
]);

// Vertical spread
let strat = ctx.build_strategy([
    #{ side: "short", option_type: "put", delta: 0.30, dte: 45 },
    #{ side: "long", option_type: "put", delta: 0.15, dte: 45 },
]);
```

**Return shape:** Map with resolved legs including `strike`, `bid`, `ask`, `delta`, `expiration`, `dte` for each leg, or `()` if no matching contracts found.

### Cross-Symbol
| Method | Returns | Description |
|--------|---------|-------------|
| `ctx.price_of(symbol)` | f64 or () | Close price of another symbol (forward-filled) |
| `ctx.price_of_col(symbol, col)` | f64 or () | Specific column: "open", "high", "low", "close", "volume" |

## Action Maps (returned by on_bar)

```rhai
// Open options (unresolved — engine finds contracts)
[#{ action: "open_options", legs: [
    #{ side: "short", option_type: "put", delta: 0.30, dte: 45 },
]}]

// Open options (resolved — from build_strategy)
let strat = ctx.build_strategy([
    #{ side: "short", option_type: "put", delta: 0.30, dte: 45 },
]);
if strat != () {
    [#{ action: "open_options", legs: strat.legs }]
}

// Open stock
[#{ action: "open_stock", side: "long", qty: 100 }]

// Close a specific position (from on_bar — position_id required)
[#{ action: "close", position_id: pos.id, reason: "take_profit" }]

// Stop the backtest early
[#{ action: "stop", reason: "capital_depleted" }]

// No action
[]
```

## Exit Actions (returned by on_exit_check)

```rhai
// Close this position
#{ action: "close", reason: "stop_loss" }

// Keep holding
#{ action: "hold" }

// Stop the entire backtest
#{ action: "stop", reason: "max_loss_reached" }
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

`"expiration"`, `"stop_loss"`, `"take_profit"`, `"dte_exit"`, `"max_hold"`, `"signal"`, `"assignment"`, `"called_away"`, `"delta_exit"`, `"end_of_data"`

## Parameter Injection

Scripts use injected `const` values for customization:

```rhai
// These are injected by the engine before compilation:
// const SYMBOL = "SPY";
// const CAPITAL = 50000.0;
// const DELTA_TARGET = 0.30;

fn config() {
    #{ symbol: SYMBOL, capital: CAPITAL }
}
```

**Optional params:** Use `!= ()` to check. Callers must pass `null` for unset optional params (injected as `const X = ();`).
```rhai
if STOP_LOSS != () && pos.pnl_pct < -STOP_LOSS {
    return #{ action: "close", reason: "stop_loss" };
}
```

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

## Examples

See `scripts/strategies/` for complete examples:
- `wheel.rhai` — Stateful wheel strategy with state machine
