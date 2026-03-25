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
| **Signals** | |
| `build_signal` | Create, validate, save, and manage custom signals (CRUD + catalog) |
| **Optimization** | |
| `parameter_sweep` | Grid search across delta/DTE/slippage/signal combos with OOS validation |
| `bayesian_optimize` | GP-based Bayesian optimization for large parameter spaces |
| `walk_forward` | Rolling walk-forward analysis with train/test windows |
| `permutation_test` | Statistical significance testing via date shuffling |
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

Place your Parquet data files in `~/.optopsy/cache/` before your first session — see the [Data](#data) section below.

To change the cache directory, set `DATA_ROOT` in the config:

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
    #{ symbol: SYMBOL, capital: CAPITAL,
       data: #{ ohlcv: true, options: true, indicators: ["rsi:14", "sma:50"] } }
}

fn on_bar(ctx) {
    if ctx.position_count() >= 3 { return []; }
    let strat = ctx.build_strategy([
        #{ side: "short", option_type: "put", delta: 0.30, dte: 45 },
    ]);
    if strat == () { return []; }
    [#{ action: "open_options", legs: strat.legs }]
}

fn on_exit_check(ctx, pos) {
    if pos.dte <= 7 { return #{ action: "close", reason: "dte_exit" }; }
    #{ action: "hold" }
}
```

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

## Development

After cloning, configure git to use the project's shared hooks:

```bash
git config core.hooksPath .githooks
```

This enables a **pre-push hook** that runs `cargo fmt --check`, `cargo clippy --all-targets`, `cargo build`, and `cargo test` before every push, matching the CI checks.

```bash
cargo build                  # Build
cargo test                   # Run all tests
cargo clippy --all-targets   # Lint
cargo fmt --check            # Check formatting
PORT=8000 cargo run          # Run as HTTP server (optional)
```

## Tech Stack

- [Polars](https://pola.rs/) — DataFrame engine for data processing
- [rmcp](https://github.com/anthropics/rmcp) — MCP server framework (v0.17)
- [Tokio](https://tokio.rs/) — Async runtime
- [Axum](https://github.com/tokio-rs/axum) — HTTP server (optional, via `PORT` env var)
- [rust_ti](https://crates.io/crates/rust_ti) — Technical analysis indicators
- [Rhai](https://rhai.rs/) — Embedded scripting language for custom strategies
- [garde](https://crates.io/crates/garde) — Input validation
- [serde](https://serde.rs/) + [schemars](https://docs.rs/schemars/) — JSON serialization and MCP schema generation
