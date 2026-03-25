![Badge](https://hitscounter.dev/api/hit?url=https%3A%2F%2Fgithub.com%2Fgoldspanlabs%2Finflow&label=Visitors&icon=briefcase-fill&color=%23ca6510&message=&style=flat&tz=Canada%2FEastern)
![made-with-rust](https://img.shields.io/badge/Made%20with-Rust-1f425f.svg)
[![CI](https://github.com/goldspanlabs/optopsy-mcp/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/goldspanlabs/optopsy-mcp/actions/workflows/ci.yml)

# optopsy-mcp

A high-performance options and stock backtesting engine exposed as an [MCP](https://modelcontextprotocol.io/) server. Connect it to Claude Desktop, Claude Code, or any MCP-compatible client and backtest strategies, optimize parameters, and analyze price patterns through natural language.

> [!NOTE]
> This project is under active development. Breaking changes to the API and configuration may occur between minor versions. Pin to a specific release tag for stability.

## What You Can Do

### Backtest Options Strategies

Run event-driven simulations across 32 built-in options strategies — from simple singles to multi-leg iron condors, butterflies, calendars, diagonals, and stock-leg strategies (covered calls, protective puts). Full position management with stop-loss, take-profit, max-hold exits, and 5 dynamic position sizing methods.

```
"Backtest an iron condor on SPY with $100k capital, 30-delta wings, and a 50% stop loss"
"Run a covered call strategy on AAPL with a 30-delta short call"
"Run a short put selling strategy with RSI < 30 as the entry filter"
```

### Run the Wheel

Simulate the full wheel strategy — sell puts, get assigned, sell covered calls, get called away, repeat. Separate put/call DTE and delta configuration with cycle-level analytics.

```
"Run the wheel on SPY with 30-delta puts at 45 DTE and 30-delta calls at 30 DTE"
"Wheel strategy on SPY with VIX/VIX3M < 1.0 as entry filter"
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

### Filter by Market Regime

Gate entries using Hidden Markov Model regime detection. Fit on historical data, classify forward in real-time (no look-ahead bias), and only trade in favorable regimes.

```
"Only enter covered calls when SPY is in a bullish HMM regime"
"Skip trades during bearish regimes: hmm_regime(3, 5) != bearish"
"Combine regime with technicals: hmm_regime(2, 5) == bullish and rsi(close, 14) < 30"
```

### Analyze Price Patterns

Discover seasonality and time-based patterns with aggregate statistics, distribution analysis, correlation matrices, rolling metrics, and regime detection.

```
"Show me SPY's average return by day of week — are any statistically significant?"
"What's the return distribution for QQQ over the last 5 years?"
"Detect volatility regimes in SPY and show when they shift"
```

### Script Custom Strategies with Rhai

Write fully custom backtests using the [Rhai](https://rhai.rs/) scripting language — or let Claude generate them from natural language. Define entry logic, exit rules, position sizing, and stateful multi-phase strategies (like the wheel) in a single script. Use built-in strategy scripts or write your own from scratch.

```
"Backtest a short put strategy that only enters when VIX > 20 and RSI < 30"
"Write a custom strategy that buys SPY on 3 consecutive down days and sells after a 2% gain"
"Run the wheel strategy script on SPY with 30-delta puts and 25-delta calls"
```

### Build Custom Signals

Create entry/exit signals with a formula DSL covering 67 functions (momentum, trend, volatility, volume, regime, derived stats). Save, rename, and reuse signals across sessions.

```
"Create a signal that fires when RSI < 30 and price is below the lower Bollinger Band"
"Build an exit signal for when the 3-day price change exceeds 3%"
```

## MCP Tools

| Tool | Description |
|------|-------------|
| `list_symbols` | Discover available symbols in the data cache |
| `list_strategies` | Browse all 32 built-in options strategies with leg definitions |
| `build_signal` | Create, validate, save, rename, search, and manage custom signals (CRUD + catalog) |
| `run_options_backtest` | Full event-driven options simulation with trade log and metrics |
| `run_stock_backtest` | Signal-driven stock/equity backtest on OHLCV data |
| `run_wheel_backtest` | Wheel strategy: sell puts → assignment → covered calls → repeat |
| `parameter_sweep` | Grid search across delta/DTE/slippage/signal combos with OOS validation |
| `compare_strategies` | Side-by-side comparison of multiple strategies |
| `walk_forward` | Rolling walk-forward analysis with train/test windows |
| `permutation_test` | Statistical significance testing via date shuffling |
| `run_script` | Execute a Rhai backtest script (inline or built-in strategy) |
| `get_raw_prices` | Return OHLCV price data for charting |
| `aggregate_prices` | Time-based aggregation (day-of-week, month, quarter, year, hour) with significance testing |
| `distribution` | Return distribution analysis with normality testing |
| `correlate` | Cross-symbol or cross-metric correlation matrices |
| `rolling_metric` | Rolling window calculations (Sharpe, volatility, returns, etc.) |
| `regime_detect` | Market regime detection (volatility clustering, trend state, Gaussian HMM) |
| `generate_hypotheses` | Auto-scan for statistically significant patterns with FDR correction |
| `portfolio_backtest` | Run multiple stock strategies as a weighted portfolio |
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

### 67 Built-in Signals

RSI, MACD, Stochastic, Bollinger Bands, Keltner Channels, Supertrend, Aroon, ATR, OBV, MFI, IV Rank/Percentile, HMM regime filter, and more — plus AND/OR/NOT combinators and cross-symbol signals (e.g., VIX as a filter for SPY trades).

### Rhai Scripting Engine

Write backtests as Rhai scripts with a callback-driven API. The engine provides a `BarContext` (`ctx`) object with access to OHLCV data, 40+ pre-computed indicators, options chain lookup, portfolio state, and cross-symbol data.

```rhai
fn config() {
    #{ symbol: SYMBOL, capital: CAPITAL,
       data: #{ ohlcv: true, options: true, indicators: ["rsi:14", "sma:50"] } }
}

fn on_bar(ctx) {
    if ctx.position_count() >= 3 { return []; }
    let put = ctx.find_option("put", 0.30, 45);
    if put == () { return []; }
    [#{ action: "open_options", legs: [#{
        side: "short", option_type: "put",
        strike: put.strike, expiration: put.expiration,
        bid: put.bid, ask: put.ask,
    }]}]
}

fn on_exit_check(ctx, pos) {
    if pos.dte <= 7 { return #{ action: "close", reason: "dte_exit" }; }
    #{ action: "hold" }
}
```

Three built-in strategy scripts (`short_put`, `iron_condor`, `wheel`) are included and parameterized via constant injection. Scripts are fully sandboxed with no file or network access.

### Formula DSL

Build custom signals with a compact expression language:

```
close > sma(close, 50) and rsi(close, 14) < 30
macd_hist(close) > 0 and rel_volume(volume, 20) > 2.0
consecutive_down(close) >= 3 and volume > sma(volume, 20) * 1.5
iv_rank(iv, 252) > 50 and bbands_lower(close, 20) > close
hmm_regime(3, 5) == bullish and rsi(close, 14) < 30
day_of_week() == 1 and pct_change(close, 1) < -0.005
```

Supports lookback (`close[1]`), date/time functions (`day_of_week()`, `month()`, `hour()`), conditionals (`if(cond, then, else)`), HMM regime gating (`hmm_regime()`), cross-symbol references (`VIX > 20`), and 67 rolling/statistical functions.

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
