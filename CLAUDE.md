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

## Architecture

**optopsy-mcp** is an options backtesting engine exposed as an MCP (Model Context Protocol) server via `rmcp 0.17`. It provides 7 tools for loading options chain data, evaluating strategies statistically, running event-driven backtests, and comparing strategies.

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
TA indicator system using `rust_ti` and `blackscholes`. Modules for momentum, trend, volatility, overlap, price, volume, plus combinators. Currently in stub/phase-in state — not yet wired into the main filter pipeline.

## Polars 0.53 Conventions

- `LazyFrame::scan_parquet(path.into(), args)` — path needs `.into()` for `PlRefPath`
- `unique_generic(Some(vec![col(...)]), strategy)` not `unique()` for column-based dedup
- `.lazy()` takes ownership — use `.clone().lazy()` when iterating
- `Scalar` has no `Display` — use `format!("{:?}", scalar.value())`
- `col()` needs `Into<PlSmallStr>` — deref `&&str` to `&str` with `*c`
- `rename()` third param is `strict: bool`

## rmcp 0.17 Patterns

- `#[tool_router]` on impl block, `#[tool_handler]` on `ServerHandler` impl
- Tool functions take `Parameters<T>` where `T: Deserialize + JsonSchema`
- Tool functions can return `String` directly (framework auto-converts)
- Server struct needs `ToolRouter<Self>` field, initialized with `Self::tool_router()`
