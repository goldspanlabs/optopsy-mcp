# optopsy-mcp

Options backtesting engine exposed as an MCP server — strategy screening, simulation, and performance metrics for LLM-driven interaction.

## Features

- **Statistical Evaluation** — group trades by DTE/delta buckets with aggregate stats (mean, std, win rate, profit factor) for strategy research and screening
- **Backtesting** — full simulation with trade selection, position management, equity curve, and performance metrics (Sharpe, Sortino, Calmar, VaR, max drawdown)
- **32 Built-in Strategies** — singles, verticals, straddles, strangles, butterflies, condors, iron condors/butterflies, calendars, diagonals
- **4 Slippage Models** — mid, spread, liquidity-based, per-leg fixed
- **MCP Interface** — 5 tools accessible via any MCP client (Claude, etc.)

## MCP Tools

| Tool | Description |
|------|-------------|
| `load_data` | Load options chain data from a Parquet file |
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

## Data Format

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

## Example Usage

Once connected via MCP:

1. Load data: `load_data({ file_path: "SPY.parquet" })`
2. Browse strategies: `list_strategies()`
3. Screen: `evaluate_strategy({ strategy: "iron_condor", leg_deltas: [...], max_entry_dte: 45, exit_dte: 14, dte_interval: 7, delta_interval: 0.05, slippage: { type: "Mid" } })`
4. Validate: `run_backtest({ strategy: "iron_condor", ..., capital: 100000, quantity: 1, max_positions: 5 })`

## Tech Stack

- [Polars](https://pola.rs/) — DataFrame engine
- [rmcp](https://github.com/anthropics/rmcp) — MCP server framework
- [rust_ti](https://crates.io/crates/rust_ti) — Technical analysis indicators
- [blackscholes](https://crates.io/crates/blackscholes) — Options pricing

## License

MIT
