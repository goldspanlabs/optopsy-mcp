![Badge](https://hitscounter.dev/api/hit?url=https%3A%2F%2Fgithub.com%2Fgoldspanlabs%2Finflow&label=Visitors&icon=briefcase-fill&color=%23ca6510&message=&style=flat&tz=Canada%2FEastern)
![made-with-rust](https://img.shields.io/badge/Made%20with-Rust-1f425f.svg)
[![CI](https://github.com/goldspanlabs/optopsy-mcp/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/goldspanlabs/optopsy-mcp/actions/workflows/ci.yml)

# optopsy-mcp

A high-performance options and stock backtesting engine exposed as an [MCP](https://modelcontextprotocol.io/) server. Connect it to Claude Desktop, Claude Code, or any MCP-compatible client and backtest strategies, optimize parameters, and analyze price patterns through natural language.

> [!NOTE]
> This project is under active development. Breaking changes to the API and configuration may occur between minor versions. Pin to a specific release tag for stability.

## What You Can Do

### Write Custom Strategies

Describe a strategy in plain English and Claude generates a [Rhai](https://rhai.rs/) script that runs against historical data. Define entry logic, exit rules, position sizing, and stateful multi-phase strategies in a single script — or use the built-in wheel strategy script with parameter injection.

```
"Write a strategy that sells puts on SPY when VIX > 20 and RSI < 30, with a 50% stop loss"
"Build a custom mean-reversion strategy that buys QQQ on 3 consecutive down days and exits after a 2% gain or 5-day hold"
"Run the wheel script on SPY with 30-delta puts at 45 DTE and 25-delta calls at 30 DTE"
```

### Backtest Options and Stocks

Run event-driven simulations across 32 built-in options strategies — singles, spreads, iron condors, butterflies, calendars, diagonals, and stock-leg combos. Signal-driven stock backtesting on OHLCV data. Full position management with stop-loss, take-profit, max-hold exits, and 5 dynamic sizing methods.

```
"Backtest an iron condor on SPY with $100k capital, 30-delta wings, and a 50% stop loss"
"Backtest buying SPY when RSI drops below 30 and selling when it crosses above 70"
"Run the wheel on SPY with 30-delta puts at 45 DTE and 30-delta calls at 30 DTE"
```

### Optimize and Validate

Grid-search across delta, DTE, slippage, and signal combinations with out-of-sample validation. Walk-forward analysis with rolling train/test windows. Permutation testing for statistical significance.

```
"Sweep DTE and delta for short puts on SPY — find the best risk-adjusted setup"
"Run walk-forward on this strategy with 4 windows to check if it holds up over time"
"Is this backtest result statistically significant or just luck?"
```

### Analyze Markets

Discover seasonality, regime shifts, and price patterns. Gate entries using HMM regime detection. Cross-symbol correlation, rolling metrics, and distribution analysis.

```
"Show me SPY's average return by day of week — are any statistically significant?"
"Only enter covered calls when SPY is in a bullish HMM regime"
"Detect volatility regimes in SPY and show when they shift"
```

## MCP Tools

| Tool | Description |
|------|-------------|
| **Backtesting** | |
| `run_script` | Execute a Rhai backtest script (strategy file or inline) |
| **Statistics** | |
| `aggregate_prices` | Time-based aggregation with significance testing |
| `distribution` | Distribution analysis with normality testing |
| `correlate` | Cross-symbol or cross-metric correlation matrices |
| `rolling_metric` | Rolling window calculations (Sharpe, volatility, returns, etc.) |
| `regime_detect` | Market regime detection (volatility clustering, trend state, HMM) |
| `generate_hypotheses` | Auto-scan for statistically significant patterns with FDR correction |
| **Risk & Portfolio** | |
| `drawdown_analysis` | Full drawdown distribution with episode tracking and Ulcer Index |
| `cointegration_test` | Engle-Granger cointegration test for pairs/stat-arb strategies |
| `monte_carlo` | Block-bootstrap Monte Carlo simulation with ruin probabilities |
| `factor_attribution` | Multi-factor regression decomposing returns into factor exposures |
| `portfolio_optimize` | Optimal portfolio weights via risk parity, min variance, or max Sharpe |
| `benchmark_analysis` | Benchmark-relative metrics: alpha, beta, Information Ratio, capture ratios |

## Quick Start

```bash
git clone https://github.com/goldspanlabs/optopsy-mcp.git
cd optopsy-mcp
cargo build --release
```

### Claude Desktop (stdio)

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

### Claude Code

```bash
claude mcp add optopsy /path/to/optopsy-mcp/target/release/optopsy-mcp
```

### HTTP Mode (StreamableHTTP)

For remote or multi-client setups, run as an HTTP server exposing MCP over StreamableHTTP at `/mcp`:

```bash
PORT=8000 cargo run --release
```

### Configuration

Place your Parquet data files in `~/.optopsy/cache/` before your first session — see the [Data](#data) section below.

To change the cache directory, set `DATA_ROOT`:

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

## Key Capabilities

### 32 Options Strategies

Singles, verticals, straddles, strangles, butterflies, condors, iron condors/butterflies, calendars, diagonals, and stock-leg strategies (covered call, protective put) — with multi-expiration support for calendar and diagonal spreads.

### 4 Slippage Models

Mid, spread (bid/ask worst-case), liquidity-based (volume-scaled), and per-leg fixed.

### 5 Position Sizing Methods

Fixed quantity, fixed fractional, risk per trade, Kelly criterion, and volatility targeting.

### Rhai Scripting Engine

Write backtests as [Rhai](https://rhai.rs/) scripts with a callback-driven API. The engine provides a `BarContext` (`ctx`) object with access to OHLCV data, pre-computed indicators (SMA, EMA, RSI, ATR, MACD, Bollinger Bands, Stochastic, CCI, OBV), options chain lookup, portfolio state, and cross-symbol data. Scripts are fully sandboxed with no file or network access.

```rhai
fn config() {
    #{ symbol: params.SYMBOL, capital: params.CAPITAL,
       data: #{ ohlcv: true, options: true, indicators: ["rsi:14", "sma:50"] } }
}

fn on_bar(ctx) {
    if ctx.position_count >= 3 { return []; }
    let spread = ctx.short_put(0.30, 45);
    if spread == () { return []; }
    [spread]
}

fn on_exit_check(ctx, pos) {
    if pos.dte <= 7 { return close_position("dte_exit"); }
    hold_position()
}
```

32 named helpers are available (`bull_put_spread`, `iron_condor`, `short_strangle`, etc.) along with action builders (`hold_position()`, `close_position()`, `buy_stock()`). See `scripts/SCRIPTING_REFERENCE.md` for the full API.

A built-in wheel strategy script is included and parameterized via constant injection.

### 67 Indicators and Signal DSL

RSI, MACD, Stochastic, Bollinger Bands, Keltner Channels, Supertrend, ATR, OBV, MFI, IV Rank, HMM regime filter, and more. Available as pre-computed O(1) lookups in Rhai scripts (`ctx.rsi(14)`, `ctx.sma(50)`) and as a formula DSL for the built-in backtest tools (`rsi(close, 14) < 30 and VIX > 20`).

## Data

optopsy-mcp reads options chains and OHLCV prices from a local Parquet cache. Place your Parquet files directly into the cache directory — any file matching the expected schema will be picked up automatically.

The default cache directory is `~/.optopsy/cache/`. To use a different location, set the `DATA_ROOT` environment variable.

### Cache layout

```
~/.optopsy/cache/
├── options/          # required — options chain data
│   ├── SPY.parquet
│   └── ...
└── <category>/       # any subfolder name works for OHLCV data
    ├── SPY.parquet
    └── ...
```

`options/` is the fixed folder for options chain data. For OHLCV price data, you can organize files into any subfolder name you like (e.g. `stocks/`, `etf/`, `futures/`, `indices/`, or your own). The engine searches all non-`options` subdirectories when resolving a symbol's price data.

### Parquet schemas

#### Options data (`options/*.parquet`)

| Column | Type | Description |
|--------|------|-------------|
| `datetime` | Datetime | Quote timestamp (intraday resolution supported) |
| `expiration` | Date/Datetime | Option expiration date |
| `strike` | Float64 | Strike price |
| `option_type` | String | `"call"` or `"put"` |
| `bid` | Float64 | Bid price |
| `ask` | Float64 | Ask price |
| `delta` | Float64 | Option delta |

> **Note:** If your data has a `date` (Date) column instead of `datetime`, it will be automatically cast to a Datetime at 15:59:00 on load.

#### Price data (`<category>/*.parquet`)

| Column | Type | Description |
|--------|------|-------------|
| `datetime` | Datetime | Bar timestamp (intraday resolution supported) |
| `open` | Float64 | Open price |
| `high` | Float64 | High price |
| `low` | Float64 | Low price |
| `close` | Float64 | Close price |
| `volume` | Int64/Float64 | Volume |

<details>
<summary><h2>REST API (optional)</h2></summary>

When running in HTTP mode (`PORT=8000 cargo run`), a REST API is available alongside the MCP endpoint at `/mcp`. This is primarily used by optopsy-ui (private, unreleased) for backtest persistence and retrieval.

| Group | Method | Endpoint | Description |
|-------|--------|----------|-------------|
| **Strategies** | `GET` | `/strategies` | List strategy scripts |
| | `POST` | `/strategies` | Create a strategy |
| | `GET` | `/strategies/{id}` | Get strategy details |
| | `PUT` | `/strategies/{id}` | Update a strategy |
| | `DELETE` | `/strategies/{id}` | Delete a strategy |
| | `GET` | `/strategies/{id}/source` | Get strategy Rhai source |
| | `POST` | `/strategies/{id}/validate` | Validate a stored strategy |
| | `POST` | `/strategies/validate` | Validate inline Rhai source |
| **Runs** | `GET` | `/runs` | List backtest runs |
| | `POST` | `/runs` | Create a backtest run |
| | `POST` | `/runs/stream` | Create a backtest run (SSE stream) |
| | `GET` | `/runs/{id}` | Get run details |
| | `DELETE` | `/runs/{id}` | Delete a run |
| | `PATCH` | `/runs/{id}/analysis` | Set run analysis notes |
| **Sweeps** | `POST` | `/runs/sweep` | Create a parameter sweep |
| | `POST` | `/runs/sweep/stream` | Create a sweep (SSE stream) |
| | `POST` | `/runs/sweep/cancel` | Cancel running sweeps |
| | `GET` | `/runs/sweep/{sweepId}` | Get sweep details |
| | `DELETE` | `/runs/sweep/{sweepId}` | Delete a sweep |
| | `PATCH` | `/runs/sweep/{sweepId}/analysis` | Set sweep analysis notes |
| **Walk-Forward** | `POST` | `/walk-forward` | Run walk-forward analysis |
| **Threads** | `GET` | `/threads` | List chat threads |
| | `POST` | `/threads` | Create a thread |
| | `GET` | `/threads/{id}` | Get thread details |
| | `PATCH` | `/threads/{id}` | Update a thread |
| | `DELETE` | `/threads/{id}` | Delete a thread |
| | `GET` | `/threads/{id}/messages` | Get thread messages |
| | `POST` | `/threads/{id}/messages` | Upsert a message |
| | `DELETE` | `/threads/{id}/messages` | Delete messages |
| | `GET` | `/threads/{id}/results` | Get thread results |
| | `PUT` | `/threads/{id}/results` | Replace thread results |
| | `DELETE` | `/threads/{id}/results/{key}` | Delete a result |
| **Misc** | `GET` | `/prices/{symbol}` | Load OHLCV price data |
| | `GET` | `/profiles` | List profiles |
| | `GET` | `/health` | Health check |

Data is persisted to SQLite (`{DATA_ROOT}/optopsy.db`).

</details>

## Development

After cloning, configure git to use the project's shared hooks:

```bash
git config core.hooksPath .githooks
```

This enables a **pre-push hook** that runs `cargo fmt --check`, `cargo clippy --all-targets`, `cargo build`, and `cargo test` before every push, matching the CI checks.

```bash
cargo build --release        # Build
cargo test                   # Run all tests
cargo clippy --all-targets   # Lint
cargo fmt --check            # Check formatting
cargo run --release          # Run MCP server (stdio)
PORT=8000 cargo run --release # Run MCP + REST over HTTP
```

## Tech Stack

- [Polars](https://pola.rs/) — DataFrame engine for data processing
- [rmcp](https://github.com/anthropics/rmcp) — MCP server framework (v0.17)
- [Tokio](https://tokio.rs/) — Async runtime
- [Axum](https://github.com/tokio-rs/axum) — HTTP server and REST API (via `PORT` env var)
- [rusqlite](https://github.com/rusqlite/rusqlite) — SQLite storage for backtest persistence
- [rust_ti](https://crates.io/crates/rust_ti) — Technical analysis indicators
- [Rhai](https://rhai.rs/) — Embedded scripting language for custom strategies
- [garde](https://crates.io/crates/garde) — Input validation
- [serde](https://serde.rs/) + [schemars](https://docs.rs/schemars/) — JSON serialization and MCP schema generation
