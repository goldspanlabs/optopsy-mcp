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
- "Load SPY options data and tell me what date range is available"
- "What strategies can I use for income generation?"
- "What parameters would you recommend for an iron condor on SPY?"

**Strategy analysis:**
- "Suggest parameters for an iron condor on SPY and then backtest them"
- "What's the win rate for a 30-delta short put on SPY with 45 DTE entries?"
- "Backtest bull put spreads at different DTE windows and find the sweet spot"

**Backtesting:**
- "Backtest an iron condor on SPY with $100k capital, max 5 positions, and a 50% stop loss"
- "Run a short strangle backtest with 16-delta legs and compare it against a 30-delta version"
- "How does adding a take profit at 50% of max profit affect iron condor performance?"

**Signal-based filtering:**
- "Backtest a short put vertical that only enters when RSI is below 30"
- "What momentum signals are available? Build me an entry filter using MACD crossover"
- "Compare iron condor results with and without a VIX-based entry signal"

**Custom signals:**
- "Create a signal that triggers when close crosses above the 50-day SMA and volume is 2x the 20-day average"
- "Build me a mean reversion signal: close below the lower Bollinger Band"
- "Save a custom exit signal that fires when the 3-day price change exceeds 3%"

**Comparison and research:**
- "Compare iron condors, iron butterflies, and short strangles side by side on SPY"
- "Which strategy has the best risk-adjusted returns: jade lizard or iron condor?"
- "Run the same iron condor backtest with mid, spread, and liquidity slippage models and compare"

## Features

- **Multi-Source Data Integration** — Load options data from EODHD API, local Parquet cache, or S3-compatible storage with fetch-on-miss
- **Event-Driven Backtesting** — Full simulation with position management, trade log, equity curve, and risk metrics (Sharpe, Sortino, Calmar, VaR, max drawdown)
- **40+ Built-in Signals** — Filter trades using technical analysis indicators across momentum, trend, volatility, overlap, price, and volume categories
- **Custom Formula Signals** — Build your own entry/exit signals using a formula DSL with price columns, lookbacks, rolling functions, and logical operators (see [Custom Signals](#custom-signals))
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

**Rolling functions**:

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

**Operators**: `+`, `-`, `*`, `/`

**Comparisons**: `>`, `<`, `>=`, `<=`, `==`, `!=`

**Logical**: `and`, `or`, `not`

### Examples

```
close > sma(close, 50) and close > sma(close, 200)
close < sma(close, 20) - 2.0 * std(close, 20)
volume > sma(volume, 20) * 2.0
pct_change(close, 1) > 0.03 or pct_change(close, 1) < -0.03
(close - low) / (high - low) < 0.2
```

### Signal management

Custom signals can be saved to `~/.optopsy/signals/` for reuse across sessions:

- **Create & save**: `build_signal` with `action="create"` and `save=true`
- **List saved**: `build_signal` with `action="list"`
- **Load**: `build_signal` with `action="get"`
- **Delete**: `build_signal` with `action="delete"`
- **Validate only**: `build_signal` with `action="validate"`

Saved signals can be referenced in backtests via `{ "type": "Saved", "name": "my_signal" }` as `entry_signal` or `exit_signal`.

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
