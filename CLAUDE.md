<!-- dgc-policy-v10 -->
# Dual-Graph Context Policy

This project uses a local dual-graph MCP server for efficient context retrieval.

## MANDATORY: Always follow this order

1. **Call `graph_continue` first** — before any file exploration, grep, or code reading.

2. **If `graph_continue` returns `needs_project=true`**: call `graph_scan` with the
   current project directory (`pwd`). Do NOT ask the user.

3. **If `graph_continue` returns `skip=true`**: project has fewer than 5 files.
   Do NOT do broad or recursive exploration. Read only specific files if their names
   are mentioned, or ask the user what to work on.

4. **Read `recommended_files`** using `graph_read`.
   - `recommended_files` may contain `file::symbol` entries (e.g. `src/auth.ts::handleLogin`).
     Pass them verbatim to `graph_read` — it reads only that symbol's lines, not the full file.

5. **Check `confidence` and obey the caps strictly:**
   - `confidence=high` -> Stop. Do NOT grep or explore further.
   - `confidence=medium` -> If recommended files are insufficient, call `fallback_rg`
     at most `max_supplementary_greps` time(s) with specific terms, then `graph_read`
     at most `max_supplementary_files` additional file(s). Then stop.
   - `confidence=low` -> Call `fallback_rg` at most `max_supplementary_greps` time(s),
     then `graph_read` at most `max_supplementary_files` file(s). Then stop.

## Token Usage

A `token-counter` MCP is available for tracking live token usage.

- To check how many tokens a large file or text will cost **before** reading it:
  `count_tokens({text: "<content>"})`
- To log actual usage after a task completes (if the user asks):
  `log_usage({input_tokens: <est>, output_tokens: <est>, description: "<task>"})`
- To show the user their running session cost:
  `get_session_stats()`

Live dashboard URL is printed at startup next to "Token usage".

## Rules

- Do NOT use `rg`, `grep`, or bash file exploration before calling `graph_continue`.
- Do NOT do broad/recursive exploration at any confidence level.
- `max_supplementary_greps` and `max_supplementary_files` are hard caps - never exceed them.
- Do NOT dump full chat history.
- Do NOT call `graph_retrieve` more than once per turn.
- After edits, call `graph_register_edit` with the changed files. Use `file::symbol` notation (e.g. `src/auth.ts::handleLogin`) when the edit targets a specific function, class, or hook.

## Context Store

Whenever you make a decision, identify a task, note a next step, fact, or blocker during a conversation, append it to `.dual-graph/context-store.json`.

**Entry format:**
```json
{"type": "decision|task|next|fact|blocker", "content": "one sentence max 15 words", "tags": ["topic"], "files": ["relevant/file.ts"], "date": "YYYY-MM-DD"}
```

**To append:** Read the file → add the new entry to the array → Write it back → call `graph_register_edit` on `.dual-graph/context-store.json`.

**Rules:**
- Only log things worth remembering across sessions (not every minor detail)
- `content` must be under 15 words
- `files` lists the files this decision/task relates to (can be empty)
- Log immediately when the item arises — not at session end

## Session End

When the user signals they are done (e.g. "bye", "done", "wrap up", "end session"), proactively update `CONTEXT.md` in the project root with:
- **Current Task**: one sentence on what was being worked on
- **Key Decisions**: bullet list, max 3 items
- **Next Steps**: bullet list, max 3 items

Keep `CONTEXT.md` under 20 lines total. Do NOT summarize the full conversation — only what's needed to resume next session.

---

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Development Commands

```bash
cargo build --release                # build (always use release)
cargo test                           # run all tests
cargo test <test_name>               # run a single test by name
cargo test --test strategy_coverage  # run a specific integration test file
cargo fmt --check                    # check formatting
cargo clippy --all-targets           # lint
```

CI uses `@stable` (latest Rust stable) and enforces `-D warnings` — all warnings are errors.
Run `rustup update` locally before developing to stay in sync with CI's toolchain and clippy lints.

## Environment Variables

Control runtime behavior and data sources:

| Variable | Purpose | Default | Notes |
|----------|---------|---------|-------|
| `PORT` | HTTP service port; if unset, uses stdio | _(unset)_ | e.g., `PORT=8000 cargo run` |
| `DATA_ROOT` | Local data directory (parquet cache + DB) | `data` (relative to CWD) | Created if missing |
| `RUST_LOG` | Tracing filter | _(unset)_ | e.g., `RUST_LOG=optopsy_mcp=debug` |

## Architecture

**optopsy-mcp** is an options and stock backtesting engine exposed as an MCP (Model Context Protocol) server via `rmcp 0.17`. It provides 13 tools for running event-driven backtests (options and equities), statistical testing, risk analysis, factor attribution, and portfolio optimization.

### Transport (`src/main.rs`)
- **stdio** (default): for local Claude Desktop integration
- **HTTP**: when `PORT` env var is set, runs axum + `StreamableHttpService` on `/mcp` with `/health` endpoint

### MCP Server (`src/server/mod.rs`)
Holds shared state: `Arc<RwLock<HashMap<String, DataFrame>>>` for multi-symbol data storage, `Arc<CachedStore>` for the data layer, and `ToolRouter<Self>` for rmcp routing. Tool handlers delegate to `src/tools/` modules which call into `src/engine/`. Data is auto-loaded from cache when a symbol is passed to any analysis tool.

### Tool Layer (`src/tools/`)
Each tool has its own module. `ai_format/` (directory module with `data.rs`, `advanced.rs`) enriches every response with `summary`, `key_findings`, and `suggested_next_steps`; shared constants and helper functions live in `ai_helpers.rs`. `construct_signal/` (directory module with `search.rs`, `examples.rs`) handles signal discovery and example generation. Response types live in `response_types/` and derive both `Serialize` and `JsonSchema`.

### Scripting Engine (`src/scripting/`)
Rhai-based scripting engine for user-defined backtesting strategies. Scripts define callback functions (`config`, `on_bar`, `on_exit_check`, etc.) and the engine drives a unified simulation loop for both options and stock backtests.

- **`engine.rs`** — Unified simulation loop with immediate exit processing, scope rewind, PriceTable MTM, and assignment detection
- **`types.rs`** — `BarContext` (exposed to scripts as `ctx`), `ScriptPosition`, `ScriptConfig`, action enums
- **`helpers.rs`** — 31 named strategy constructors (`bull_put_spread`, `iron_condor`, etc.), the `indicators_ready` utility, and 6 action builders (`hold_position`, `close_position`, `buy_stock`, etc.)
- **`indicators.rs`** — Pre-computed indicator store (SMA, EMA, RSI, ATR, MACD, BBands, Stochastic, CCI, OBV) with O(1) per-bar lookups
- **`registration.rs`** — Sandboxed Rhai engine builder (ops limit, print interception, type registration, helper registration)
- **`stdlib.rs`** — Parameter injection (`const` and scope modes), strategy script listing

Strategy scripts live in `scripts/strategies/`. See `scripts/SCRIPTING_REFERENCE.md` for the full `ctx` API.

### Engine (`src/engine/`)
Internal backtest engines used by the scripting layer and optimization tools:

- **`ohlcv.rs`** — OHLCV data loading, parsing, resampling (`load_ohlcv_df`, `bars_from_df`, `resample_ohlcv`, `detect_date_col`)
- **`filters.rs`** — DTE/delta filtering pipeline
- **`pricing.rs`** — 5 slippage models (Mid/Spread/Liquidity/PerLeg/BidAskTravel)
- **`price_table.rs`** — PriceTable hash map for O(1) options quote lookups during simulation
- **`positions.rs`** — Position management, MTM updates, last-known price carry-forward
- **`metrics.rs`** — Performance calculations (Sharpe, Sortino, CAGR, VaR, etc.)
- **`sweep.rs`** — Grid parameter sweep (Cartesian product of param ranges)
- **`bayesian.rs`** — Bayesian optimization using Gaussian Process with Expected Improvement
- **`walk_forward.rs`** — Walk-forward analysis with train/test window splits
- **`sim_types.rs`** — Shared simulation types (PriceTable, DateIndex, QuoteSnapshot)

### Strategies (`src/strategies/`)
31 strategies across singles, spreads, butterflies, condors, iron, and calendar categories. Built using helpers (`call_leg`, `put_leg`, `strategy`) in `helpers.rs`. `all_strategies()` returns the full list; `find_strategy(name)` does linear scan. Multi-expiration strategies (calendar/diagonal) use `ExpirationCycle::Primary`/`Secondary` tags on legs.

### Data Layer (`src/data/`)
`DataStore` trait with `CachedStore` as default — local Parquet cache only at `data/{category}/{SYMBOL}.parquet`, errors if data not found. `ParquetStore` handles date column normalization: options files store `date` (Date) which is cast to `datetime` (Datetime) at 15:59:00 on load; OHLCV files already have a `datetime` (Datetime) column. Path segments validated against traversal attacks.

`Database` manages SQLite via `rusqlite`. Schema is fully managed by **refinery** migrations in `migrations/`. On startup, `init_schema()` sets PRAGMAs (WAL, foreign_keys) then runs `migrations::runner().run()`. Strategies are auto-seeded from `scripts/strategies/` on first run via `seed_strategies_if_empty()`.

### Migrations (`migrations/`)
SQL migration files managed by refinery. Naming: `V{n}__{description}.sql`. Currently:
- `V1__initial_schema.sql` — All tables, indices, and columns

### Signals (`src/signals/`)
TA indicator system using `rust_ti` and `blackscholes`. Modules for momentum, trend, volatility, overlap, price, volume, plus combinators. Split across three focused modules: `spec.rs` (the `SignalSpec` enum with 40+ variants), `builders.rs` (`build_signal()` factory and per-category builders), and `registry.rs` (signal catalog metadata, `collect_cross_symbols`, re-exports). Signals are **fully wired** into both options and stock backtests via `entry_signal` and `exit_signal` params. For options (`BacktestParams`), signals are optional entry/exit filters. For stocks (`StockBacktestParams`), `entry_signal` is required — it drives when trades open. OHLCV data is loaded from the local Parquet cache when signals are used.

## Polars 0.53 Conventions

- `LazyFrame::scan_parquet(path.into(), args)` — path needs `.into()` for `PlRefPath`
- `unique_generic(Some(vec![col(...)]), strategy)` not `unique()` for column-based dedup
- `.lazy()` takes ownership — use `.clone().lazy()` when iterating
- `Scalar` has no `Display` — use `format!("{:?}", scalar.value())`
- `col()` needs `Into<PlSmallStr>` — deref `&&str` to `&str` with `*c`
- `rename()` third param is `strict: bool`

## MCP Tools: Quick Reference

### Backtest Tool
- **`run_script`** — Execute Rhai backtest scripts for options, stock, and wheel strategies.
  Pass `strategy` (filename from `scripts/strategies/`) or `script` (inline Rhai source).
  See `scripts/SCRIPTING_REFERENCE.md` for the full `ctx` API (strategy helpers, indicators, cross-symbol data, etc.).

### Statistics Tools
- **`aggregate_prices`** — Group returns by `day_of_week`, `month`, `quarter`, `year`, `hour_of_day`.
- **`distribution`** — Histogram + normality tests (Jarque-Bera, Shapiro-Wilk). Source: `price_returns` or `trade_pnl`.
- **`correlate`** — Pearson/Spearman correlation with optional rolling window and Granger lag analysis.
- **`rolling_metric`** — Rolling `volatility`, `sharpe`, `beta`, `correlation`, `drawdown`, etc. `benchmark` required for beta/correlation.
- **`regime_detect`** — Detect volatility clusters, trend states, or Gaussian HMM regimes.
- **`generate_hypotheses`** — Auto-scan for statistically significant patterns across 8 dimensions with BH-FDR correction. Results are hypotheses — validate before trusting.
  - Dimensions: `seasonality`, `price_action`, `mean_reversion`, `volume`, `volatility_regime`, `cross_asset`, `microstructure`, `autocorrelation`

### Risk & Quantitative Analysis Tools
- **`drawdown_analysis`** — Full drawdown distribution: episode tracking (depth, duration, recovery), underwater curve, Ulcer Index, Pain Ratio. Use to compare drawdown *profiles* beyond just max drawdown.
- **`cointegration_test`** — Engle-Granger cointegration test between two price series. Computes hedge ratio, ADF test on spread residuals, spread z-score, and mean-reversion half-life. Foundation for pairs/stat-arb strategies.
- **`monte_carlo`** — Block-bootstrap Monte Carlo simulation (default 10K paths). Produces confidence intervals on terminal wealth, ruin probabilities (P(loss > 10/25/50%)), max drawdown distribution, and terminal wealth histogram.
  - Uses 21-day block resampling to preserve autocorrelation structure.
- **`factor_attribution`** — Multi-factor regression decomposing returns into Market, SMB (Size), HML (Value), and Momentum exposures using ETF proxies. Tests whether alpha is genuine or factor exposure.
  - Default proxies: Market=SPY, SMB=IWM-SPY, HML=IWD-IWF, Momentum=MTUM-SPY. Customizable via `factor_proxies`.
- **`portfolio_optimize`** — Compute optimal portfolio weights for 2-20 assets using three methods:
  - `risk_parity`: Inverse-volatility weighting (equal risk contribution)
  - `min_variance`: Analytical minimum-variance portfolio (Σ^{-1} * 1 / 1' Σ^{-1} 1)
  - `max_sharpe`: Tangency portfolio maximizing Sharpe ratio
- **`benchmark_analysis`** — Benchmark-relative metrics: Jensen's alpha (with t-test), beta, Treynor ratio, Information Ratio, tracking error, up/down capture ratios, R².

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

**`ExitType`**: `Expiration`, `StopLoss`, `TakeProfit`, `MaxHold`, `DteExit`, `Adjustment`, `Signal`, `DeltaExit`, `Assignment`, `CalledAway`

**`Slippage`**:
- `Mid` — Mid-price entry/exit
- `Spread` — Bid/ask worst case
- `Liquidity { fill_ratio: 0.0..=1.0, ref_volume: u64 }` — Volume-based slippage
- `PerLeg { per_leg: f64 }` — Fixed per-leg points

**`PositionSizing`** (tagged enum, `"method"` field):
- `fixed` — Passthrough, uses fixed `quantity`
- `fixed_fractional { risk_pct: 0.001..=1.0 }` — Risk a % of equity per trade
- `risk_per_trade { risk_amount: >= 1.0 }` — Fixed dollar risk per trade
- `kelly { fraction: 0.01..=1.0, lookback: Option<usize> }` — Kelly criterion (cold-start: first 20 trades use fallback)
- `volatility_target { target_vol: 0.01..=2.0, lookback_days: 5..=252 }` — Target annualized portfolio volatility

### Structs

**`TargetRange`**: `{ target: 0.0..=1.0, min: 0.0..=1.0, max: 0.0..=1.0 }` where `min ≤ max`
- Used for delta targeting per leg (e.g., `target: 0.30, min: 0.20, max: 0.40`)

**`DteRange`**: `{ target: i32 >= 1, min: i32 >= 1, max: i32 >= 1 }` where `min ≤ max`
- Used for entry DTE range (e.g., `target: 45, min: 30, max: 60`)
- `exit_dte` must be less than `entry_dte.min`
- `TradeSelector::Nearest` picks candidates closest to `target`

**`Commission`**: `{ per_contract: f64, base_fee: f64, min_fee: f64 }`
- `calculate(num_contracts)` returns `max(base_fee + per_contract * num_contracts, min_fee)`

**`SizingConfig`**: `{ method: PositionSizing, constraints: SizingConstraints }`
- Optional on `BacktestParams`, `SimParams`, `StockBacktestParams`
- When present, overrides fixed `quantity` with per-trade dynamic sizing

**`SizingConstraints`**: `{ min_quantity: i32 (default 1), max_quantity: Option<i32> }`
- Clamps computed quantity to `[min, max]`

**`PerformanceMetrics`**: Sharpe, Sortino, CAGR, Calmar, VaR 95%, CVaR 95% (Expected Shortfall), historical VaR 95%, max drawdown, Ulcer Index, Pain Ratio, avg/max drawdown duration, win rate, profit factor, expectancy, etc.

**`TradeRecord`**: Entry/exit date, strike, legs, quantity, entry_cost, exit_cost, P&L, days_held, exit_reason, computed_quantity, entry_equity

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
- Holds local Parquet cache dir
- `load_options(symbol, start, end)` — Load from local cache or error if not found
- Path traversal protection via `validate_path_segment()` — rejects `/`, `\`, `..`, empty strings

### ParquetStore (`src/data/parquet.rs`)
- Reads/writes Parquet with date column handling
- Options files: `date` (Date) → cast to `datetime` (Datetime) at 15:59:00 on load
- OHLCV files: `datetime` (Datetime) column used directly, no normalization needed
- Lazy scanning for memory efficiency: `scan_parquet().select([...]).filter(...).collect()`

## Testing

- Unit tests in `src/**/tests.rs` modules
- Integration tests in `tests/` (e.g., `strategy_coverage.rs`)
- Run with `cargo test` (no flags needed; all features tested in CI)
- Key test utilities: `tempfile::NamedTempFile` for cache isolation, `polars::testing` for DataFrame comparison

## Common Implementation Patterns

### Adding a New Tool
1. Create `src/tools/my_tool.rs` with `pub async fn execute(...) -> Result<MyResponse>`
2. Define parameter struct in `src/engine/types.rs` or `src/tools/response_types/`, derive `Serialize + Deserialize + JsonSchema + Validate`
3. Add `#[tool(...)]` method in `src/server/mod.rs` that calls `tools::my_tool::execute()`
4. Return AI-formatted response via `ai_format::format_my_response(...)`

### DataFrame Filtering Chains
```rust
let lazy = df.clone().lazy()
  .filter(col("expiration").is_not_null())
  .filter(col("datetime").is_not_null())
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
1. OHLCV data is loaded from cache when signals are used
2. Pass `entry_signal: Some(SignalSpec { ... })` in `BacktestParams`
3. Event loop evaluates signal on each date via `signal::evaluate(ohlcv_df, spec, date)`
4. Gates trade entry or forces position exit
