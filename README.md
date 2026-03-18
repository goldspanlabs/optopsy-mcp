![Badge](https://hitscounter.dev/api/hit?url=https%3A%2F%2Fgithub.com%2Fgoldspanlabs%2Finflow&label=Visitors&icon=briefcase-fill&color=%23ca6510&message=&style=flat&tz=Canada%2FEastern)
![made-with-rust](https://img.shields.io/badge/Made%20with-Rust-1f425f.svg)
[![CI](https://github.com/goldspanlabs/optopsy-mcp/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/goldspanlabs/optopsy-mcp/actions/workflows/ci.yml)

# optopsy-mcp

A high-performance options and stock backtesting engine exposed as an [MCP](https://modelcontextprotocol.io/) server. Connect it to Claude Desktop, Claude Code, or any MCP-compatible client and backtest strategies, optimize parameters, and analyze price patterns through natural language.

> [!NOTE]
> This project is currently in a pre-release state. We are iterating quickly, which means breaking changes to the API and configuration may occur without prior notice. Use in production environments at your own risk.

## What You Can Do

### Backtest Options Strategies

Run event-driven simulations across 31 built-in options strategies — from simple singles to multi-leg iron condors, butterflies, calendars, and diagonals. Full position management with stop-loss, take-profit, max-hold exits, and 5 dynamic position sizing methods.

```
"Backtest an iron condor on SPY with $100k capital, 30-delta wings, and a 50% stop loss"
"Run a short put selling strategy with RSI < 30 as the entry filter"
```

### Backtest Stock Strategies

Signal-driven stock backtesting on OHLCV data. Define entry/exit conditions using the formula DSL and simulate long or short equity positions.

```
"Backtest buying SPY when RSI drops below 30 and selling when it crosses above 70"
"Test a mean-reversion strategy on QQQ with a 3% stop loss"
```

### Optimize Parameters

Grid-search across delta, DTE, slippage, and signal combinations with out-of-sample validation, stability scoring, and sensitivity analysis.

```
"Sweep DTE and delta for short puts on SPY — find the best risk-adjusted setup"
"Compare iron condors vs iron butterflies with different entry signals"
```

### Validate Robustness

Walk-forward analysis with rolling train/test windows and permutation testing for statistical significance.

```
"Run walk-forward on this strategy with 4 windows to check if it holds up over time"
"Is this backtest result statistically significant or just luck?"
```

### Analyze Price Patterns

Discover seasonality and time-based patterns with aggregate statistics, distribution analysis, correlation matrices, rolling metrics, and regime detection.

```
"Show me SPY's average return by day of week — are any statistically significant?"
"What's the return distribution for QQQ over the last 5 years?"
"Detect volatility regimes in SPY and show when they shift"
```

### Build Custom Signals

Create entry/exit signals with a formula DSL covering 35+ functions (momentum, trend, volatility, volume, derived stats). Save, rename, and reuse signals across sessions.

```
"Create a signal that fires when RSI < 30 and price is below the lower Bollinger Band"
"Build an exit signal for when the 3-day price change exceeds 3%"
```

## MCP Tools

| Tool | Description |
|------|-------------|
| `list_symbols` | Discover available symbols in the data cache |
| `list_strategies` | Browse all 31 built-in options strategies with leg definitions |
| `build_signal` | Create, validate, save, rename, search, and manage custom signals (CRUD + catalog) |
| `run_options_backtest` | Full event-driven options simulation with trade log and metrics |
| `run_stock_backtest` | Signal-driven stock/equity backtest on OHLCV data |
| `parameter_sweep` | Grid search across delta/DTE/slippage/signal combos with OOS validation |
| `compare_strategies` | Side-by-side comparison of multiple strategies |
| `walk_forward` | Rolling walk-forward analysis with train/test windows |
| `permutation_test` | Statistical significance testing via date shuffling |
| `get_raw_prices` | Return OHLCV price data for charting |
| `aggregate_prices` | Time-based aggregation (day-of-week, month, quarter, year, hour) with significance testing |
| `distribution` | Return distribution analysis with normality testing |
| `correlate` | Cross-symbol or cross-metric correlation matrices |
| `rolling_metric` | Rolling window calculations (Sharpe, volatility, returns, etc.) |
| `regime_detect` | Market regime detection using volatility or returns clustering |

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

## Key Capabilities

### 31 Options Strategies

Singles, verticals, straddles, strangles, butterflies, condors, iron condors/butterflies, calendars, and diagonals — with multi-expiration support for calendar and diagonal spreads.

### 4 Slippage Models

Mid, spread (bid/ask worst-case), liquidity-based (volume-scaled), and per-leg fixed.

### 5 Position Sizing Methods

Fixed quantity, fixed fractional, risk per trade, Kelly criterion, and volatility targeting.

### 40+ Built-in Signals

RSI, MACD, Stochastic, Bollinger Bands, Keltner Channels, Supertrend, Aroon, ATR, OBV, MFI, IV Rank/Percentile, and more — plus AND/OR/NOT combinators and cross-symbol signals (e.g., VIX as a filter for SPY trades).

### Formula DSL

Build custom signals with a compact expression language:

```
close > sma(close, 50) and rsi(close, 14) < 30
macd_hist(close) > 0 and rel_volume(volume, 20) > 2.0
consecutive_down(close) >= 3 and volume > sma(volume, 20) * 1.5
iv_rank(iv, 252) > 50 and bbands_lower(close, 20) > close
day_of_week() == 1 and pct_change(close, 1) < -0.005
```

Supports lookback (`close[1]`), date/time functions (`day_of_week()`, `month()`, `hour()`), conditionals (`if(cond, then, else)`), and 35+ rolling/statistical functions.

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

### Manual data

You can also place any Parquet file matching the expected schema directly into the cache directory.

### Parquet schema

Minimum required columns for options chain data:

| Column | Type | Description |
|--------|------|-------------|
| `date` | Date | Trading date (cast to `datetime` at 15:59 on load) |
| `expiration` | Date/Datetime | Option expiration date |
| `strike` | Float64 | Strike price |
| `option_type` | String | `"call"` or `"put"` |
| `bid` | Float64 | Bid price |
| `ask` | Float64 | Ask price |
| `delta` | Float64 | Option delta |

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
- [garde](https://crates.io/crates/garde) — Input validation
- [serde](https://serde.rs/) + [schemars](https://docs.rs/schemars/) — JSON serialization and MCP schema generation
