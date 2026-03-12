![Badge](https://hitscounter.dev/api/hit?url=https%3A%2F%2Fgithub.com%2Fgoldspanlabs%2Finflow&label=Visitors&icon=briefcase-fill&color=%23ca6510&message=&style=flat&tz=Canada%2FEastern)
![made-with-rust](https://img.shields.io/badge/Made%20with-Rust-1f425f.svg)
[![CI](https://github.com/goldspanlabs/optopsy-mcp/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/goldspanlabs/optopsy-mcp/actions/workflows/ci.yml)


# optopsy-mcp

An MCP server for options strategy screening and simulation, powered by a high-performance Rust rewrite of the [Optopsy](https://github.com/goldspanlabs/optopsy) engine.

> [!NOTE]
> This project is currently in a pre-release state. We are iterating quickly, which means breaking changes to the API and configuration may occur without prior notice. Use in production environments at your own risk.


## Example Prompts

Once connected via Claude Desktop or any MCP client, try asking:

**Getting started:**
- "Load SPY options data and suggest parameters for an iron condor"
- "What strategies work best for income generation?"

**Backtesting:**
- "Backtest an iron condor on SPY with $100k capital, max 5 positions, and a 50% stop loss"
- "Run a short strangle with 16-delta legs and compare it against a 30-delta version"

**Signal-based filtering:**
- "Backtest a short put that only enters when RSI is below 30"
- "Create an exit signal that fires when the 3-day price change exceeds 3%"

**Comparison and optimization:**
- "Compare iron condors vs iron butterflies with an RSI entry signal"
- "Sweep DTE and delta combinations for short puts and find the best risk-adjusted setup"

## Features

- **Multi-Source Data Integration** — Load options data from EODHD API, local Parquet cache, or S3-compatible storage with fetch-on-miss
- **Event-Driven Backtesting** — Full simulation with position management, trade log, equity curve, and risk metrics (Sharpe, Sortino, Calmar, VaR, max drawdown)
- **Formula-Based Signals** — Build entry/exit signals using a formula DSL with 35+ functions covering momentum, trend, volatility, volume, and price indicators (see [Custom Signals](#custom-signals))
- **Signal Persistence** — Save, list, load, and delete custom signals for reuse across sessions
- **32 Built-in Strategies** — Singles, verticals, straddles, strangles, butterflies, condors, iron condors/butterflies, calendars, diagonals (with multi-expiration support)
- **4 Slippage Models** — Mid, spread, liquidity-based, per-leg fixed
- **12 MCP Tools** — All accessible via Claude Desktop or any MCP-compatible client
- **Parameter Validation** — garde-powered input validation with detailed error feedback
- **HTTP & Stdio Transport** — Deploy locally via stdio or run as HTTP service on cloud platforms

## Quick Start

```bash
git clone https://github.com/goldspanlabs/optopsy-mcp.git
cd optopsy-mcp
cargo build --release
```

Add to your Claude Desktop config (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "optopsy": {
      "command": "/path/to/optopsy-mcp/target/release/optopsy-mcp"
    }
  }
}
```

Populate the data cache with [inflow](https://github.com/goldspanlabs/inflow) before your first session — see the [Data](#data) section below.

By default, data is read from `~/.optopsy/cache`. To change this, set `DATA_ROOT` in the config:

```json
{
  "mcpServers": {
    "optopsy": {
      "command": "/path/to/optopsy-mcp/target/release/optopsy-mcp",
      "env": {
        "DATA_ROOT": "/your/custom/cache/dir"
      }
    }
  }
}
```

## Data

optopsy-mcp reads options chains and OHLCV prices from a local Parquet cache at `~/.optopsy/cache/`. Use [**inflow**](https://github.com/goldspanlabs/inflow) to download and manage that data.

### inflow (recommended)

[inflow](https://github.com/goldspanlabs/inflow) is a standalone CLI for downloading and caching market data — options chains from EODHD and OHLCV prices from Yahoo Finance. It writes directly to the same `~/.optopsy/cache/` directory that optopsy-mcp reads from, with concurrent downloads, resume support, and rate limiting. See the [inflow README](https://github.com/goldspanlabs/inflow) for installation and usage.

### Cache layout

```
~/.optopsy/cache/
├── options/
│   ├── SPY.parquet
│   ├── QQQ.parquet
│   └── ...
└── prices/
    ├── SPY.parquet
    ├── QQQ.parquet
    └── ...
```

### Other data sources

optopsy-mcp also supports loading data without inflow:
- **Manual placement** — Drop any Parquet file matching the expected schema into the cache directory

### Parquet schema

Minimum required columns for options chain data:

| Column | Type | Description |
|--------|------|-------------|
| `quote_date` | Date/Datetime | Trading date |
| `expiration` | Date/Datetime | Option expiration date |
| `strike` | Float64 | Strike price |
| `option_type` | String | `"call"` or `"put"` |
| `bid` | Float64 | Bid price |
| `ask` | Float64 | Ask price |
| `delta` | Float64 | Option delta |

## Custom Signals

The `build_signal` tool lets you create formula-based entry and exit signals using a mini expression DSL. Signals are validated at parse time and evaluated against OHLCV price data during backtests. OHLCV data is auto-fetched when signals are used.

### Supported syntax

**Columns**: `close`, `open`, `high`, `low`, `volume`, `adjclose`

**Lookback**: `close[1]` (previous close), `close[5]` (5 bars ago)

**Basic rolling functions**:

| Function | Description |
|----------|-------------|
| `sma(col, period)` | Simple Moving Average |
| `ema(col, period)` | Exponential Moving Average |
| `std(col, period)` | Rolling Standard Deviation |
| `max(col, period)` | Rolling Maximum |
| `min(col, period)` | Rolling Minimum |
| `abs(expr)` | Absolute value |
| `change(col, period)` | `col - col[period]` |
| `pct_change(col, period)` | `(col - col[period]) / col[period]` |

**Momentum**:

| Function | Description |
|----------|-------------|
| `rsi(col, period)` | Relative Strength Index (Wilder smoothing) |
| `macd_line(col)` | MACD line (12/26 EMA difference) |
| `macd_signal(col)` | MACD signal line (9-period EMA of MACD) |
| `macd_hist(col)` | MACD histogram (line − signal) |
| `roc(col, period)` | Rate of Change (percentage) |
| `stochastic(close, high, low, period)` | Stochastic %K oscillator |

**Trend**:

| Function | Description |
|----------|-------------|
| `aroon_up(high, low, period)` | Aroon Up indicator |
| `aroon_down(high, low, period)` | Aroon Down indicator |
| `aroon_osc(high, low, period)` | Aroon Oscillator (up − down) |
| `supertrend(close, high, low, period, mult)` | Supertrend trend-following indicator |

**Volatility**:

| Function | Description |
|----------|-------------|
| `atr(close, high, low, period)` | Average True Range |
| `tr(close, high, low)` | True Range |
| `bbands_upper(col, period)` | Bollinger Band upper (SMA + 2σ) |
| `bbands_mid(col, period)` | Bollinger Band middle (SMA) |
| `bbands_lower(col, period)` | Bollinger Band lower (SMA − 2σ) |
| `keltner_upper(close, high, low, period, mult)` | Keltner Channel upper |
| `keltner_lower(close, high, low, period, mult)` | Keltner Channel lower |

**Volume**:

| Function | Description |
|----------|-------------|
| `obv(close, volume)` | On-Balance Volume |
| `mfi(close, high, low, volume, period)` | Money Flow Index |
| `cmf(close, high, low, volume, period)` | Chaikin Money Flow |
| `rel_volume(volume, period)` | Relative volume (current / SMA) |

**Derived / Statistical**:

| Function | Description |
|----------|-------------|
| `zscore(col, period)` | Z-score (deviation from rolling mean) |
| `rank(col, period)` | Percentile rank within rolling window |
| `range_pct(close, high, low)` | Position within bar range: `(close − low) / (high − low)` |
| `consecutive_up(col)` | Consecutive bars where value increases |
| `consecutive_down(col)` | Consecutive bars where value decreases |
| `if(cond, then, else)` | Conditional: returns `then` when `cond` is true, else `else` |

**Operators**: `+`, `-`, `*`, `/`

**Comparisons**: `>`, `<`, `>=`, `<=`, `==`, `!=`

**Logical**: `and`, `or`, `not`

### Examples

```
close > sma(close, 50) and close > sma(close, 200)
rsi(close, 14) < 30 and close > bbands_lower(close, 20)
macd_hist(close) > 0 and rel_volume(volume, 20) > 2.0
atr(close, high, low, 14) > 2.0
stochastic(close, high, low, 14) < 20 and rsi(close, 14) < 30
consecutive_down(close) >= 3 and volume > sma(volume, 20) * 1.5
```

### Signal management

Custom signals are saved for reuse across sessions (at `~/.optopsy/signals/`, or alongside the cache when `DATA_ROOT` is set):

- **Create & save**: `build_signal` with `action="create"` and `save=true`
- **List saved**: `build_signal` with `action="list"`
- **Load**: `build_signal` with `action="get"`
- **Delete**: `build_signal` with `action="delete"`
- **Validate only**: `build_signal` with `action="validate"`

Saved signals can be referenced in backtests via `{ "type": "Saved", "name": "my_signal" }` as `entry_signal` or `exit_signal`.

## Development

After cloning, configure git to use the project's shared hooks:

```bash
git config core.hooksPath .githooks
```

This enables a **pre-push hook** that runs `cargo build`, `cargo clippy -- -D warnings`, and `cargo test` before every push, matching the CI checks.

## Tech Stack

- [Polars](https://pola.rs/) — DataFrame engine for data processing
- [rmcp](https://github.com/anthropics/rmcp) — MCP server framework (v0.17)
- [Tokio](https://tokio.rs/) — Async runtime for concurrent operations
- [Axum](https://github.com/tokio-rs/axum) — HTTP server (optional, via PORT env var)
- [rust-s3](https://crates.io/crates/rust-s3) — S3-compatible object storage
- [rust_ti](https://crates.io/crates/rust_ti) — Technical analysis indicators (40+ signals)
- [garde](https://crates.io/crates/garde) — Input validation framework
- [serde + serde_json](https://serde.rs/) — JSON serialization
- [schemars](https://docs.rs/schemars/) — JSON Schema generation for MCP tools
