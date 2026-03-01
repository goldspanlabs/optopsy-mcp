# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Development Commands

```bash
cargo build                          # build (default features)
cargo build --features postgres      # build with PostgreSQL backend
cargo test                           # run all tests
cargo test <test_name>               # run a single test by name
cargo test --test strategy_coverage  # run a specific integration test file
cargo fmt --check                    # check formatting
cargo clippy --all-targets           # lint (default features)
cargo clippy --all-targets --features postgres  # lint with postgres feature
```

CI and the pre-push hook enforce `RUSTFLAGS="-Dwarnings"` — all warnings are errors. Both default and `--features postgres` clippy must pass.

## Environment Variables

Control runtime behavior and data sources:

| Variable | Purpose | Default | Notes |
|----------|---------|---------|-------|
| `PORT` | HTTP service port; if unset, uses stdio | _(unset)_ | e.g., `PORT=8000 cargo run` |
| `EODHD_API_KEY` | Enable EODHD API for options downloads | _(unset)_ | Sets `EodhdProvider::from_env()` to Some |
| `DATA_ROOT` | Local Parquet cache directory | `~/.optopsy/cache` | Created if missing; `~/` expanded via `shellexpand` |
| `S3_BUCKET` | S3 bucket name | _(unset)_ | Requires `S3_ENDPOINT`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` |
| `S3_ENDPOINT` | S3-compatible endpoint URL | _(unset)_ | e.g., `https://s3.amazonaws.com` |
| `AWS_ACCESS_KEY_ID` | S3 credentials | _(unset)_ | Via `Credentials::from_env_specific()` |
| `AWS_SECRET_ACCESS_KEY` | S3 credentials | _(unset)_ | Via `Credentials::from_env_specific()` |
| `RUST_LOG` | Tracing filter | _(unset)_ | e.g., `RUST_LOG=optopsy_mcp=debug` |

## Architecture

**optopsy-mcp** is an options backtesting engine exposed as an MCP (Model Context Protocol) server via `rmcp 0.17`. It provides 10 tools for loading options chain data, evaluating strategies statistically, running event-driven backtests, and comparing strategies.

### Transport (`src/main.rs`)
- **stdio** (default): for local Claude Desktop integration
- **HTTP**: when `PORT` env var is set, runs axum + `StreamableHttpService` on `/mcp` with `/health` endpoint

### MCP Server (`src/server.rs`)
Holds shared state: `Arc<RwLock<Option<DataFrame>>>` for loaded options data, `Arc<CachedStore>` for the data layer, and `ToolRouter<Self>` for rmcp routing. Tool handlers delegate to `src/tools/` modules which call into `src/engine/`.

### Tool Layer (`src/tools/`)
Each tool has its own module. `ai_format.rs` enriches every response with `summary`, `key_findings`, and `suggested_next_steps`. Response types live in `response_types.rs` and derive both `Serialize` and `JsonSchema`.

### Engine (`src/engine/`)
Two main execution paths in `core.rs`:

- **evaluate_strategy()** — Statistical analysis. Filters options per leg (option type → DTE → valid quotes → closest delta), matches entry/exit rows, joins legs, applies strike ordering, computes per-leg P&L, then bins by DTE × delta buckets with aggregate stats.
- **run_backtest()** — Event-driven simulation. Builds a `HashMap<(date, exp, strike, OptionType), QuoteSnapshot>` price table for O(1) lookups, finds entry candidates, then runs a day-by-day event loop managing position opens (with `max_positions` constraint) and closes (DTE exit, stop loss, take profit, max hold, expiration). Produces trade log, equity curve, and performance metrics (Sharpe, Sortino, CAGR, VaR, etc.).

Key submodules: `filters.rs` (DTE/delta filtering), `evaluation.rs` (entry-exit matching), `event_sim.rs` (backtest event loop), `pricing.rs` (4 slippage models: Mid/Spread/Liquidity/PerLeg), `rules.rs` (strike ordering), `metrics.rs` (performance calculations), `output.rs` (DTE×delta bucketing).

### Strategies (`src/strategies/`)
32 strategies across singles, spreads, butterflies, condors, iron, and calendar categories. Built using helpers (`call_leg`, `put_leg`, `strategy`) in `helpers.rs`. `all_strategies()` returns the full list; `find_strategy(name)` does linear scan. Multi-expiration strategies (calendar/diagonal) use `ExpirationCycle::Primary`/`Secondary` tags on legs.

### Data Layer (`src/data/`)
`DataStore` trait with `CachedStore` as default — local Parquet cache at `~/.optopsy/cache/{category}/{SYMBOL}.parquet` with S3 fetch-on-miss. `ParquetStore` handles normalization of date columns (`quote_date`/`data_date`/`quote_datetime` as Date, Datetime, or String → unified `Datetime("quote_datetime")`). Path segments validated against traversal attacks.

### Signals (`src/signals/`)
TA indicator system using `rust_ti` and `blackscholes`. Modules for momentum, trend, volatility, overlap, price, volume, plus combinators. Signal registry in `registry.rs` contains ~40 indicators. Signals are **fully wired** into backtest entry/exit filtering via `entry_signal` and `exit_signal` params in `BacktestParams`. Usage requires OHLCV data loaded via `fetch_to_parquet` tool.

## Polars 0.53 Conventions

- `LazyFrame::scan_parquet(path.into(), args)` — path needs `.into()` for `PlRefPath`
- `unique_generic(Some(vec![col(...)]), strategy)` not `unique()` for column-based dedup
- `.lazy()` takes ownership — use `.clone().lazy()` when iterating
- `Scalar` has no `Display` — use `format!("{:?}", scalar.value())`
- `col()` needs `Into<PlSmallStr>` — deref `&&str` to `&str` with `*c`
- `rename()` third param is `strict: bool`

## MCP Tools: Detailed Reference

### Data Management Tools

#### `load_data`
Load options chain data by symbol with optional date filtering. Tries cache first, auto-downloads from EODHD if configured.

**Parameters:**
```json
{
  "symbol": "SPY",           // Required. Uppercase. No path separators, ".."
  "start_date": "2024-01-01", // Optional. YYYY-MM-DD format
  "end_date": "2024-12-31"   // Optional. YYYY-MM-DD format
}
```

**Response:** `LoadDataResponse`
- `summary` — Natural language overview
- `rows`, `symbols` — Data shape
- `date_range` — Min/max dates
- `columns` — Available DataFrame columns (e.g., `quote_datetime`, `expiration`, `option_type`, `strike`, `bid`, `ask`, etc.)
- `suggested_next_steps` — Recommended next actions

#### `download_options_data`
Bulk download options data from EODHD API and cache locally. Resumable (re-run to fetch only new data).

**Parameters:**
```json
{
  "symbol": "SPY"  // Required. Will download ~2 years of data if available
}
```

**Response:** `DownloadResponse`
- `summary`, `new_rows`, `total_rows` — Download summary
- `was_resumed` — True if extended existing cache
- `api_requests` — Number of API calls made
- `date_range` — Data coverage

#### `check_cache_status`
Check if Parquet cache exists for a symbol and last update time.

**Parameters:**
```json
{
  "symbol": "SPY"  // Required
}
```

**Response:** `CheckCacheResponse`
- `exists` — Boolean
- `path` — Full path to parquet file (if exists)
- `size_mb` — File size
- `last_updated` — Timestamp
- `row_count` — Number of records (if exists)

#### `fetch_to_parquet`
Download historical OHLCV data from Yahoo Finance and save as Parquet. Used to populate signal filtering requirements.

**Parameters:**
```json
{
  "symbol": "SPY",           // Required
  "start_date": "2024-01-01", // Optional
  "end_date": "2024-12-31"   // Optional
}
```

**Response:** `FetchResponse`
- `summary`, `rows` — OHLCV data summary
- `file_path` — Local Parquet path
- `date_range` — Coverage

### Strategy Tools

#### `list_strategies`
List all 32 built-in strategies with leg definitions and category.

**Parameters:** None

**Response:** `StrategiesResponse`
- Array of strategy objects with `name`, `category`, `description`, `legs` (with `side`, `option_type`, `delta` ranges)

#### `list_signals`
List all ~40 available TA signals across categories (momentum, trend, volatility, overlap, price, volume).

**Parameters:** None

**Response:** `SignalsResponse`
- Signal catalog with names, parameters, descriptions

#### `construct_signal`
Use NLP fuzzy search to find signals and generate live JSON schema for TA indicators.

**Parameters:**
```json
{
  "prompt": "RSI oversold"  // Natural language description of desired signal
}
```

**Response:** Signal specification JSON
- Candidate signals with sensible defaults
- Full JSON schema showing all variants
- Examples for And/Or combinators

### Analysis Tools

#### `evaluate_strategy`
Fast statistical analysis grouped by DTE × delta buckets. Does NOT run backtest.

**Parameters:**
```json
{
  "strategy": "Iron Condor",
  "leg_deltas": [
    {"target": 0.30, "min": 0.20, "max": 0.40},  // Call spread
    {"target": 0.70, "min": 0.60, "max": 0.80}   // Put spread
  ],
  "max_entry_dte": 45,       // Max DTE for entries
  "exit_dte": 7,             // Close positions at this DTE
  "dte_interval": 5,         // Bucket width
  "delta_interval": 0.10,    // Delta bucket width
  "slippage": {"type": "Spread"},  // Or: Mid, Liquidity, PerLeg
  "commission": {            // Optional
    "per_contract": 0.65,
    "base_fee": 0.0,
    "min_fee": 0.0
  }
}
```

**Response:** `EvaluateResponse`
- `best_bucket`, `worst_bucket`, `highest_win_rate_bucket` — Top performers
- `groups` — Full list of DTE × delta buckets with stats (mean, std, q25, median, q75, win_rate, profit_factor)
- `suggested_next_steps` — Recommendations for backtest params

#### `run_backtest`
Full event-driven day-by-day simulation with trade log and metrics.

**Parameters:**
```json
{
  "strategy": "Iron Condor",
  "leg_deltas": [
    {"target": 0.30, "min": 0.20, "max": 0.40},
    {"target": 0.70, "min": 0.60, "max": 0.80}
  ],
  "max_entry_dte": 45,
  "exit_dte": 7,
  "slippage": {"type": "Spread"},
  "commission": null,

  // Backtest-specific parameters
  "capital": 10000.0,          // Starting equity
  "quantity": 1,               // Contracts per trade
  "multiplier": 100,           // Points per contract (options standard)
  "max_positions": 5,          // Max concurrent positions
  "max_hold_days": 30,         // Optional: force close after N days
  "stop_loss": 0.50,           // Optional: loss threshold (pct of entry)
  "take_profit": 0.80,         // Optional: profit target (pct of entry)

  "selector": "Nearest",       // Trade selector: Nearest|HighestPremium|LowestPremium|First
  "adjustment_rules": [],      // Optional: position adjustments
  "entry_signal": null,        // Optional: SignalSpec for entry filtering
  "exit_signal": null          // Optional: SignalSpec for early exit
}
```

**Response:** `BacktestResponse`
- `summary`, `assessment`, `key_findings` — AI-enriched analysis
- `metrics` — Performance: Sharpe, Sortino, CAGR, VaR, max_drawdown, Calmar, win_rate, profit_factor, expectancy
- `trade_summary` — Total, winners, losers, avg P&L, best/worst trades
- `equity_curve` — ≤50 sampled points from full curve
- `trade_log` — All trades with entry/exit dates, P&L, days_held, exit_reason
- `suggested_next_steps` — Follow-up actions

#### `compare_strategies`
Side-by-side comparison of multiple strategies using shared sim params.

**Parameters:**
```json
{
  "strategies": [
    {
      "name": "Iron Condor",
      "leg_deltas": [...],
      "max_entry_dte": 45,
      "exit_dte": 7,
      "slippage": {"type": "Spread"}
    },
    {
      "name": "Vertical Spread",
      "leg_deltas": [...],
      "max_entry_dte": 30,
      "exit_dte": 5,
      "slippage": {"type": "Mid"}
    }
  ],
  "sim_params": {
    "capital": 10000.0,
    "quantity": 1,
    "multiplier": 100,
    "max_positions": 5,
    "selector": "Nearest",
    "stop_loss": 0.50,
    "take_profit": 0.80,
    "max_hold_days": 30
  }
}
```

**Response:** `CompareResponse`
- `ranking_by_sharpe`, `ranking_by_pnl` — Strategy rankings
- `best_overall` — Recommended strategy
- `results` — Full metrics for each strategy

## Type System

### Enums

**`Side`**: `Long` (1) | `Short` (-1) — Position direction

**`OptionType`**: `Call` | `Put`

**`ExpirationCycle`**: `Primary` (default) | `Secondary` (calendar/diagonal multi-expiration)

**`TradeSelector`**:
- `Nearest` — Entry on nearest expiration (default)
- `HighestPremium` — Highest entry cost
- `LowestPremium` — Lowest entry cost
- `First` — First matching entry

**`ExitType`**: `Expiration`, `StopLoss`, `TakeProfit`, `MaxHold`, `DteExit`, `Adjustment`, `Signal`

**`Slippage`**:
- `Mid` — Mid-price entry/exit
- `Spread` — Bid/ask worst case
- `Liquidity { fill_ratio: 0.0..=1.0, ref_volume: u64 }` — Volume-based slippage
- `PerLeg { per_leg: f64 }` — Fixed per-leg points

### Structs

**`TargetRange`**: `{ target: 0.0..=1.0, min: 0.0..=1.0, max: 0.0..=1.0 }` where `min ≤ max`
- Used for delta targeting per leg (e.g., `target: 0.30, min: 0.20, max: 0.40`)

**`Commission`**: `{ per_contract: f64, base_fee: f64, min_fee: f64 }`
- `calculate(num_contracts)` returns `max(base_fee + per_contract * num_contracts, min_fee)`

**`PerformanceMetrics`**: Sharpe, Sortino, CAGR, Calmar, VaR 95%, max drawdown, win rate, profit factor, expectancy, etc.

**`TradeRecord`**: Entry/exit date, strike, legs, quantity, entry_cost, exit_cost, P&L, days_held, exit_reason

## rmcp 0.17 Patterns

- `#[tool_router]` on impl block, `#[tool_handler]` on `ServerHandler` impl
- Tool functions take `Parameters<T>` where `T: Deserialize + JsonSchema`
- Tool functions can return `String` directly (framework auto-converts)
- Server struct needs `ToolRouter<Self>` field, initialized with `Self::tool_router()`
- All tool handlers check `data.write()` lock; block if data load in progress
- Validation via `garde::Validate` — calling `.validate()` returns `garde::Result` with detailed field-level errors

## Validation & Error Handling

All parameter structs derive `garde::Validate`. Common validators:
- `#[garde(range(min = N, max = M))]` — Numeric range validation
- `#[garde(length(min = N))]` — String/Vec length
- `#[garde(dive)]` — Recursive validation for nested structs
- `#[garde(custom(...))]` — Custom functions like `validate_exit_dte_lt_max`
- `#[garde(skip)]` — Skip validation for specific fields (e.g., `SignalSpec`)

Validation happens in tool handlers via `params.validate().map_err(...)?`. Invalid input returns detailed garde error messages.

## Data Layer Internals

### CachedStore (`src/data/cache.rs`)
- Holds local Parquet cache dir and optional S3 config
- `load_options(symbol, start, end)` — Try local → S3 → error
- `save_options(symbol, df)` — Write to local parquet (creates dirs as needed)
- Path traversal protection via `validate_path_segment()` — rejects `/`, `\`, `..`, empty strings

### ParquetStore (`src/data/parquet.rs`)
- Reads/writes Parquet with date column normalization
- Detects and normalizes: `quote_date` (Date), `data_date` (Date), `quote_datetime` (Datetime/String) → unified `Datetime("quote_datetime")`
- Lazy scanning for memory efficiency: `scan_parquet().select([...]).filter(...).collect()`

### EodhdProvider (`src/data/eodhd.rs`)
- `from_env()` — Returns Some if `EODHD_API_KEY` set; else None
- `download_options(symbol)` — Fetches from EODHD, normalizes, caches locally
- Resumable: checks last cached date, fetches only new rows, appends to existing parquet

## Testing

- Unit tests in `src/**/tests.rs` modules
- Integration tests in `tests/` (e.g., `strategy_coverage.rs`)
- Run with `cargo test` (no flags needed; all features tested in CI)
- Key test utilities: `tempfile::NamedTempFile` for cache isolation, `polars::testing` for DataFrame comparison

## Common Implementation Patterns

### Adding a New Tool
1. Create `src/tools/my_tool.rs` with `pub async fn execute(...) -> Result<MyResponse>`
2. Define parameter struct in `src/engine/types.rs` or `src/tools/response_types.rs`, derive `Serialize + Deserialize + JsonSchema + Validate`
3. Add `#[tool_handler]` method in `src/server.rs` that calls `tools::my_tool::execute()`
4. Return AI-formatted response via `ai_format::format_my_response(...)`

### DataFrame Filtering Chains
```rust
let lazy = df.clone().lazy()
  .filter(col("expiration").is_not_null())
  .filter(col("quote_datetime").is_not_null())
  .filter(col("bid").gt(0.0).and(col("ask").gt(0.0)))
  .collect()?;
```

### Delta Filtering (closest match)
```rust
// In filters.rs: delta_bins_for_leg()
// 1. Compute delta for all rows
// 2. Filter by min/max range
// 3. Find closest to target
```

### Signal Entry/Exit
1. Call `fetch_to_parquet` to populate OHLCV cache
2. Pass `entry_signal: Some(SignalSpec { ... })` in `BacktestParams`
3. Event loop evaluates signal on each date via `signal::evaluate(ohlcv_df, spec, date)`
4. Gates trade entry or forces position exit
