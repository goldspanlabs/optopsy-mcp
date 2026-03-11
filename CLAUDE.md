# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Development Commands

```bash
cargo build                          # build
cargo test                           # run all tests
cargo test <test_name>               # run a single test by name
cargo test --test strategy_coverage  # run a specific integration test file
cargo fmt --check                    # check formatting
cargo clippy --all-targets           # lint
```

CI and the pre-push hook enforce `RUSTFLAGS="-Dwarnings"` — all warnings are errors.

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

**optopsy-mcp** is an options and stock backtesting engine exposed as an MCP (Model Context Protocol) server via `rmcp 0.17`. It provides 11 tools for running event-driven backtests (options and equities), comparing strategies, parameter optimization, walk-forward analysis, statistical testing, and returning raw price data for charting.

### Transport (`src/main.rs`)
- **stdio** (default): for local Claude Desktop integration
- **HTTP**: when `PORT` env var is set, runs axum + `StreamableHttpService` on `/mcp` with `/health` endpoint

### MCP Server (`src/server/mod.rs`)
Holds shared state: `Arc<RwLock<HashMap<String, DataFrame>>>` for multi-symbol data storage, `Arc<CachedStore>` for the data layer, and `ToolRouter<Self>` for rmcp routing. Tool handlers delegate to `src/tools/` modules which call into `src/engine/`. Data is auto-loaded from cache when a symbol is passed to any analysis tool.

### Tool Layer (`src/tools/`)
Each tool has its own module. `ai_format/` (directory module with `backtest.rs`, `data.rs`, `advanced.rs`) enriches every response with `summary`, `key_findings`, and `suggested_next_steps`; shared constants and helper functions live in `ai_helpers.rs`. `construct_signal/` (directory module with `search.rs`, `examples.rs`) handles signal discovery and example generation. Response types live in `response_types.rs` and derive both `Serialize` and `JsonSchema`.

### Engine (`src/engine/`)
Three main execution paths:

- **evaluate_strategy()** (`core.rs`) — Statistical analysis. Filters options per leg (option type → DTE → valid quotes → closest delta), matches entry/exit rows, joins legs, applies strike ordering, computes per-leg P&L, then bins by DTE × delta buckets with aggregate stats.
- **run_backtest()** (`core.rs`) — Options event-driven simulation. Builds a `HashMap<(date, exp, strike, OptionType), QuoteSnapshot>` price table for O(1) lookups, finds entry candidates, then runs a day-by-day event loop managing position opens (with `max_positions` constraint) and closes (DTE exit, stop loss, take profit, max hold, expiration). Produces trade log, equity curve, and performance metrics (Sharpe, Sortino, CAGR, VaR, etc.).
- **run_stock_backtest()** (`stock_sim.rs`) — Stock/equity event-driven simulation on OHLCV bars. Signal-driven entries (required) with optional exit signals. Manages long/short positions with stop-loss, take-profit, max-hold exits. Uses synthetic bid-ask spread (10% of daily range) for slippage models. Reuses `PerformanceMetrics`, `TradeRecord`, and `BacktestResult`.

Key submodules: `filters.rs` (DTE/delta filtering, `filter_valid_quotes(df, min_bid_ask)`), `evaluation.rs` (entry-exit matching), `event_sim.rs` (options backtest event loop), `stock_sim.rs` (stock backtest event loop), `pricing.rs` (4 slippage models: Mid/Spread/Liquidity/PerLeg), `rules.rs` (strike ordering), `metrics.rs` (performance calculations), `output.rs` (DTE×delta bucketing with right-closed `(a, b]` intervals).

### Strategies (`src/strategies/`)
32 strategies across singles, spreads, butterflies, condors, iron, and calendar categories. Built using helpers (`call_leg`, `put_leg`, `strategy`) in `helpers.rs`. `all_strategies()` returns the full list; `find_strategy(name)` does linear scan. Multi-expiration strategies (calendar/diagonal) use `ExpirationCycle::Primary`/`Secondary` tags on legs.

### Data Layer (`src/data/`)
`DataStore` trait with `CachedStore` as default — local Parquet cache at `~/.optopsy/cache/{category}/{SYMBOL}.parquet` with S3 fetch-on-miss. `ParquetStore` handles normalization of date columns (`quote_date`/`quote_datetime` as Date, Datetime, or String → unified `Datetime("quote_datetime")`). Path segments validated against traversal attacks.

### Signals (`src/signals/`)
TA indicator system using `rust_ti` and `blackscholes`. Modules for momentum, trend, volatility, overlap, price, volume, plus combinators. Split across three focused modules: `spec.rs` (the `SignalSpec` enum with 40+ variants), `builders.rs` (`build_signal()` factory and per-category builders), and `registry.rs` (signal catalog metadata, `collect_cross_symbols`, re-exports). Signals are **fully wired** into both options and stock backtests via `entry_signal` and `exit_signal` params. For options (`BacktestParams`), signals are optional entry/exit filters. For stocks (`StockBacktestParams`), `entry_signal` is required — it drives when trades open. OHLCV data is auto-fetched when signals are used.

## Polars 0.53 Conventions

- `LazyFrame::scan_parquet(path.into(), args)` — path needs `.into()` for `PlRefPath`
- `unique_generic(Some(vec![col(...)]), strategy)` not `unique()` for column-based dedup
- `.lazy()` takes ownership — use `.clone().lazy()` when iterating
- `Scalar` has no `Display` — use `format!("{:?}", scalar.value())`
- `col()` needs `Into<PlSmallStr>` — deref `&&str` to `&str` with `*c`
- `rename()` third param is `strict: bool`

## MCP Tools: Detailed Reference

### Data Tools

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

#### `get_raw_prices`
Return raw OHLCV price data for a symbol, ready for chart generation by LLMs.
OHLCV data is auto-fetched from Yahoo Finance and cached on first access.

**Parameters:**
```json
{
  "symbol": "SPY",             // Required
  "start_date": "2024-01-01",  // Optional. YYYY-MM-DD
  "end_date": "2024-12-31",    // Optional. YYYY-MM-DD
  "limit": 500                 // Optional. Max bars to return (default: 500). null for no limit.
}
```

**Response:** `RawPricesResponse`
- `symbol`, `total_rows`, `returned_rows`, `sampled` — Metadata
- `date_range` — Min/max dates
- `prices` — Array of `{ date, open, high, low, close, adjclose, volume }` bars
- `suggested_next_steps` — Recommended next actions

#### `get_loaded_symbol`
Check what symbol is currently loaded in memory, row count, available columns.

**Parameters:** None

**Response:** `StatusResponse`
- Details about the in-memory DataFrame (symbol, rows, columns)

### Strategy & Signal Tools

#### `list_strategies`
List all 32 built-in strategies with leg definitions and category.

**Parameters:** None

**Response:** `StrategiesResponse`
- Array of strategy objects with `name`, `category`, `description`, `legs` (with `side`, `option_type`, `delta` ranges)

#### `build_signal`
Single entry point for discovering built-in signals and creating/managing custom formula-based signals. Dispatches via `action` field.

**Actions:**

| `action` | Purpose |
|----------|---------|
| `catalog` | Browse the full built-in signal catalog grouped by category (40+ signals) |
| `search` | NLP search of the built-in signal catalog |
| `validate` | Check a formula without saving |
| `create` | Build a custom signal from a price-column formula |
| `list` | List all saved custom signals |
| `get` | Load a saved signal by name |
| `delete` | Remove a saved signal by name |

**Parameters (search):**
```json
{
  "action": "search",
  "prompt": "RSI oversold"   // Required. Natural-language description (1–500 chars, non-whitespace)
}
```

**Response (search):** `BuildSignalResponse`
- `success` — `true` when at least one candidate is found
- `candidates` — Matching built-in signals with name, category, description, params, example
- `schema` — Full SignalSpec JSON schema for reference
- `column_defaults` — Default column mappings per signal type
- `combinator_examples` — Example AND/OR combinations
- `suggested_next_steps` — Recommended follow-up actions

**Parameters (validate / create):**
```json
{
  "action": "validate",          // or "create"
  "formula": "close > sma(close, 20)",
  "name": "my_signal",           // Required for create
  "description": "Price above 20-day SMA",  // Optional
  "save": true                   // Optional (create only); persists signal for reuse
}
```

**Response (validate / create):** `BuildSignalResponse`
- `success` — `true` if formula is valid
- `signal_spec` — Resulting `SignalSpec` (type `Custom`) on success
- `formula_help` — Syntax guide (columns, functions, lookback) on failure
- `saved_signals` — Always empty for create/validate; use `action='list'` to see saved signals
- `suggested_next_steps` — Recommended follow-up actions

**Available formula columns:** `close`, `open`, `high`, `low`, `volume`, `adjclose`

**Available formula functions:** `sma(col, n)`, `ema(col, n)`, `std(col, n)`, `max(col, n)`, `min(col, n)`, `abs(expr)`, `change(col, n)`, `pct_change(col, n)`

### Analysis Tools

#### `run_options_backtest`
Full event-driven day-by-day options simulation with trade log and metrics. Options data is auto-loaded from cache when `symbol` is passed.

**Parameters:**
```json
{
  "strategy": "Iron Condor",
  "leg_deltas": [
    {"target": 0.30, "min": 0.20, "max": 0.40},
    {"target": 0.70, "min": 0.60, "max": 0.80}
  ],
  "entry_dte": {"target": 45, "min": 30, "max": 60},
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
  "exit_signal": null,         // Optional: SignalSpec for early exit

  "symbol": "SPY"              // Required. Auto-loads data from cache.
}
```

**Response:** `BacktestResponse`
- `summary`, `assessment`, `key_findings` — AI-enriched analysis
- `metrics` — Performance: Sharpe, Sortino, CAGR, VaR, max_drawdown, Calmar, win_rate, profit_factor, expectancy
- `trade_summary` — Total, winners, losers, avg P&L, best/worst trades
- `equity_curve` — ≤50 sampled points from full curve
- `trade_log` — All trades with entry/exit dates, P&L, days_held, exit_reason
- `suggested_next_steps` — Follow-up actions

#### `run_stock_backtest`
Signal-driven stock/equity backtest on OHLCV data. Entry signal is required. OHLCV data is auto-fetched from Yahoo Finance.

**Parameters:**
```json
{
  "symbol": "SPY",               // Required
  "side": "Long",                // Long or Short
  "entry_signal": { ... },       // Required: SignalSpec for entry
  "exit_signal": null,           // Optional: SignalSpec for exit
  "capital": 10000.0,            // Starting equity (default: 10000)
  "quantity": 100,               // Shares per trade (default: 100)
  "max_positions": 1,            // Max concurrent positions (default: 1)
  "slippage": {"type": "Mid"},   // Slippage model (default: Mid)
  "commission": null,            // Optional: Commission config
  "stop_loss": 0.05,             // Optional: % loss from entry to trigger exit
  "take_profit": 0.10,           // Optional: % gain from entry to trigger exit
  "max_hold_days": 30,           // Optional: force close after N days
  "start_date": "2024-01-01",   // Optional: YYYY-MM-DD
  "end_date": "2024-12-31"      // Optional: YYYY-MM-DD
}
```

**Response:** `StockBacktestResponse`
- `summary`, `assessment`, `key_findings` — AI-enriched analysis
- `parameters` — Echo of input config (side, quantity, capital, slippage, signals)
- `metrics` — Same as options: Sharpe, Sortino, CAGR, VaR, max_drawdown, etc.
- `trade_summary` — Total, winners, losers, avg P&L, best/worst trades
- `trade_log` — All trades with entry/exit dates, P&L, days_held, exit_reason
- `underlying_prices` — OHLCV price overlay data
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
      "entry_dte": {"target": 45, "min": 30, "max": 60},
      "exit_dte": 7,
      "slippage": {"type": "Spread"}
    },
    {
      "name": "Vertical Spread",
      "leg_deltas": [...],
      "entry_dte": {"target": 30, "min": 20, "max": 40},
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
  },
  "symbol": "SPY"              // Required. Auto-loads data from cache.
}
```

**Response:** `CompareResponse`
- `ranking_by_sharpe`, `ranking_by_pnl` — Strategy rankings
- `best_overall` — Recommended strategy
- `results` — Full metrics for each strategy

#### `parameter_sweep`
Grid search across delta/DTE/slippage combos with out-of-sample validation and dimension sensitivity analysis. Preferred over `compare_strategies` for optimization.

**Parameters:**
```json
{
  "strategies": [
    {
      "name": "short_put",
      "leg_delta_targets": [[0.15, 0.20, 0.30]]
    }
  ],
  "sweep": {
    "entry_dte_targets": [30, 45],
    "exit_dtes": [0, 5],
    "slippage_models": [{"type": "Mid"}, {"type": "Spread"}]
  },
  "sim_params": { "capital": 10000.0, "quantity": 1, "multiplier": 100, "max_positions": 3 },
  "out_of_sample_pct": 0.3,     // Optional. 0.0 to disable OOS validation
  "direction": "bullish",        // Optional. Auto-selects strategies by market outlook
  "entry_signals": [],           // Optional. Signal variants to sweep
  "exit_signals": [],            // Optional. Signal variants to sweep
  "num_permutations": 100,       // Optional. Enable permutation-based p-values
  "permutation_seed": 42,        // Optional. Seed for reproducible permutations
  "symbol": "SPY"
}
```

**Response:** `SweepResponse`
- `ranked_results` — All combos ranked by Sharpe, with trades, PnL, p-values
- `oos_results` — Out-of-sample validation for top combos
- `dimension_sensitivity` — Per-dimension (strategy, delta, DTE, slippage) stats
- `multiple_comparisons` — Bonferroni and BH-FDR corrections (when permutations enabled)

#### `walk_forward`
Rolling walk-forward analysis with train/test windows to validate strategy robustness over time.

**Parameters:**
```json
{
  "strategy": "short_put",
  "leg_deltas": [{"target": 0.30, "min": 0.20, "max": 0.40}],
  "entry_dte": {"target": 45, "min": 30, "max": 60},
  "exit_dte": 5,
  "slippage": {"type": "Mid"},
  "sim_params": { "capital": 10000.0, "quantity": 1, "multiplier": 100, "max_positions": 3 },
  "num_windows": 4,             // Number of rolling windows
  "train_pct": 0.7,             // Train/test split ratio
  "symbol": "SPY"
}
```

**Response:** `WalkForwardResponse`
- `windows` — Per-window train/test metrics
- `summary` — Aggregate consistency metrics

#### `permutation_test`
Statistical significance test for a backtest result. Shuffles trade entry dates to build a null distribution and compute a p-value.

**Parameters:**
```json
{
  "strategy": "short_put",
  "leg_deltas": [{"target": 0.30, "min": 0.20, "max": 0.40}],
  "entry_dte": {"target": 45, "min": 30, "max": 60},
  "exit_dte": 5,
  "slippage": {"type": "Mid"},
  "sim_params": { "capital": 10000.0, "quantity": 1, "multiplier": 100, "max_positions": 3 },
  "num_permutations": 1000,
  "seed": 42,                    // Optional. For reproducibility
  "symbol": "SPY"
}
```

**Response:** `PermutationTestResponse`
- `observed_sharpe` — Original backtest Sharpe
- `p_value` — Fraction of permutations with Sharpe ≥ observed
- `null_distribution` — Sampled Sharpe values from permuted backtests

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

**`ExitType`**: `Expiration`, `StopLoss`, `TakeProfit`, `MaxHold`, `DteExit`, `Adjustment`, `Signal`, `DeltaExit`

**`Slippage`**:
- `Mid` — Mid-price entry/exit
- `Spread` — Bid/ask worst case
- `Liquidity { fill_ratio: 0.0..=1.0, ref_volume: u64 }` — Volume-based slippage
- `PerLeg { per_leg: f64 }` — Fixed per-leg points

### Structs

**`TargetRange`**: `{ target: 0.0..=1.0, min: 0.0..=1.0, max: 0.0..=1.0 }` where `min ≤ max`
- Used for delta targeting per leg (e.g., `target: 0.30, min: 0.20, max: 0.40`)

**`DteRange`**: `{ target: i32 >= 1, min: i32 >= 1, max: i32 >= 1 }` where `min ≤ max`
- Used for entry DTE range (e.g., `target: 45, min: 30, max: 60`)
- `exit_dte` must be less than `entry_dte.min`
- `TradeSelector::Nearest` picks candidates closest to `target`

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
- Detects and normalizes: `quote_date` (Date), `quote_datetime` (Datetime/String) → unified `Datetime("quote_datetime")`
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
3. Add `#[tool(...)]` method in `src/server/mod.rs` that calls `tools::my_tool::execute()`
4. Return AI-formatted response via `ai_format::format_my_response(...)`

### DataFrame Filtering Chains
```rust
let lazy = df.clone().lazy()
  .filter(col("expiration").is_not_null())
  .filter(col("quote_datetime").is_not_null())
  .filter(col("bid").gt(min_bid_ask).and(col("ask").gt(min_bid_ask)))
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
1. OHLCV data is auto-fetched when signals are used (no manual step needed)
2. Pass `entry_signal: Some(SignalSpec { ... })` in `BacktestParams`
3. Event loop evaluates signal on each date via `signal::evaluate(ohlcv_df, spec, date)`
4. Gates trade entry or forces position exit
