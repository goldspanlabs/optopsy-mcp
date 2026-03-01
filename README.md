# optopsy-mcp

Options backtesting engine exposed as an MCP server — strategy screening, simulation, and performance metrics for LLM-driven interaction.

## Features

- **Symmetric Caching Data Layer** — symbol-based data access with local cache and optional S3-compatible fetch-on-miss
- **Statistical Evaluation** — group trades by DTE/delta buckets with aggregate stats (mean, std, win rate, profit factor) for strategy research and screening
- **Backtesting** — full simulation with trade selection, position management, equity curve, and performance metrics (Sharpe, Sortino, Calmar, VaR, max drawdown)
- **32 Built-in Strategies** — singles, verticals, straddles, strangles, butterflies, condors, iron condors/butterflies, calendars, diagonals
- **4 Slippage Models** — mid, spread, liquidity-based, per-leg fixed
- **MCP Interface** — 5 tools accessible via any MCP client (Claude, etc.)

## MCP Tools

| Tool | Description |
|------|-------------|
| `load_data` | Load options data by symbol (auto-fetches from S3 if configured) |
| `list_strategies` | List all 32 available strategies with definitions |
| `evaluate_strategy` | Statistical evaluation with DTE/delta bucket grouping |
| `run_backtest` | Full simulation with trade log, equity curve, and metrics |
| `compare_strategies` | Side-by-side comparison of multiple strategies |

## Quick Start

```bash
# Build
cargo build --release

# Run as MCP server (stdio transport)
cargo run --release
```

### Claude Desktop Configuration

Add to your Claude Desktop config (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "optopsy": {
      "command": "/path/to/optopsy-mcp"
    }
  }
}
```

## Data Layer

Data is loaded by **symbol** through a caching layer that resolves Parquet files locally and optionally fetches from S3-compatible storage on cache miss.

### Local-only mode (default)

Place Parquet files in the cache directory following the `{cache_dir}/{category}/{SYMBOL}.parquet` convention:

```
~/.optopsy/cache/
  options/
    SPY.parquet
    QQQ.parquet
```

Then load with: `load_data({ symbol: "SPY" })`

### S3 mode

Set environment variables to enable automatic fetch-on-miss from an S3-compatible bucket (AWS, Railway Buckets, MinIO, R2, etc.):

| Env Var | Default | Purpose |
|---------|---------|---------|
| `DATA_ROOT` | `~/.optopsy/cache` | Local cache directory |
| `S3_BUCKET` | _(none)_ | Bucket name — if unset, S3 is disabled |
| `S3_ENDPOINT` | _(none)_ | S3-compatible endpoint URL |
| `AWS_ACCESS_KEY_ID` | _(none)_ | S3 credentials |
| `AWS_SECRET_ACCESS_KEY` | _(none)_ | S3 credentials |

When S3 is configured, files are downloaded to the local cache on first access and served from cache on subsequent calls.

### Parquet schema

Expects Parquet files with options chain data containing columns:

| Column | Type | Description |
|--------|------|-------------|
| `quote_date` | Date | Trading date |
| `expiration` | Date | Option expiration date |
| `strike` | Float | Strike price |
| `option_type` | String | `"call"` or `"put"` |
| `bid` | Float | Bid price |
| `ask` | Float | Ask price |
| `delta` | Float | Option delta |
| `symbol` | String | Underlying symbol |

The `quote_date` column is auto-normalized — `quote_date`, `data_date`, and `quote_datetime` are all accepted (Date, Datetime, or String types).

## Example Usage

Once connected via MCP:

1. Load data: `load_data({ symbol: "SPY" })`
2. Browse strategies: `list_strategies()`
3. Screen: `evaluate_strategy({ strategy: "iron_condor", leg_deltas: [...], max_entry_dte: 45, exit_dte: 14, dte_interval: 7, delta_interval: 0.05, slippage: { type: "Mid" } })`
4. Validate: `run_backtest({ strategy: "iron_condor", ..., capital: 100000, quantity: 1, max_positions: 5 })`

## Tech Stack

- [Polars](https://pola.rs/) — DataFrame engine
- [rmcp](https://github.com/anthropics/rmcp) — MCP server framework
- [rust-s3](https://crates.io/crates/rust-s3) — S3-compatible object storage
- [rust_ti](https://crates.io/crates/rust_ti) — Technical analysis indicators
- [blackscholes](https://crates.io/crates/blackscholes) — Options pricing

## License

MIT
