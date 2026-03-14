# Copilot Code Review Instructions

## Project Overview

**optopsy-mcp** is a ~30k-line Rust options/stock backtesting engine exposed as an MCP server via `rmcp 0.17`. It uses Polars 0.53 DataFrames, `garde` validation, and serves 11 tools for backtesting, strategy comparison, parameter sweeps, and statistical testing.

## CI Requirements

CI enforces `RUSTFLAGS="-Dwarnings"` — **all warnings are errors**. Every PR must pass:
- `cargo fmt --check`
- `cargo clippy --all-targets` (with pedantic lints enabled)
- `cargo test`
- `cargo build`

Flag any code that would produce warnings under these settings.

## Rust & Clippy Standards

- Clippy pedantic is enabled (`pedantic = "warn"`). Allowed exceptions: `module_name_repetitions`, `must_use_candidate`, `missing_errors_doc`, `missing_panics_doc`, and numeric cast lints (`cast_possible_truncation`, `cast_sign_loss`, `cast_possible_wrap`, `cast_precision_loss`).
- All other pedantic lints must be satisfied. Watch for: `needless_pass_by_value`, `redundant_closure`, `unnecessary_wraps`, `manual_let_else`, `uninlined_format_args`, `single_match_else`, `match_wildcard_for_single_variants`.
- Prefer `thiserror` for error types, `anyhow` for ad-hoc propagation in tool handlers.
- Use `tracing` macros (`tracing::debug!`, `tracing::warn!`) not `println!` or `eprintln!`.

## Polars 0.53 Pitfalls

Flag these common mistakes in reviews:
- `LazyFrame::scan_parquet(path, args)` — `path` must use `.into()` for `PlRefPath` conversion.
- `.lazy()` takes ownership — reviewer should check that `df.clone().lazy()` is used when the DataFrame is needed again.
- `unique_generic(Some(vec![col(...)]), strategy)` not `unique()` for column-based dedup.
- `Scalar` has no `Display` — must use `format!("{:?}", scalar.value())`.
- `col()` needs `Into<PlSmallStr>` — if iterating over `&&str`, must deref with `*c`.
- `rename()` takes a third `strict: bool` parameter.

## Architecture Rules

### Tool Layer (`src/tools/`)
- Every tool response must include AI formatting: `summary`, `key_findings`, `suggested_next_steps` via the `ai_format/` module.
- Tool parameter structs must derive `Serialize + Deserialize + JsonSchema + Validate`.
- Tool handlers must call `.validate()` on params before processing.
- Tool handlers acquire `data.write()` lock — flag any code that holds the lock longer than necessary or could deadlock.

### Engine (`src/engine/`)
- Three execution paths: `evaluate_strategy()` (statistical), `run_backtest()` (options event-driven), `run_stock_backtest()` (equity event-driven). Changes to shared components (metrics, filters, pricing) must be validated against all three.
- The options price table uses `HashMap<(date, exp, strike, OptionType), QuoteSnapshot>` for O(1) lookups — flag any code that does linear scans over price data in hot paths.
- `filter_valid_quotes(df, min_bid_ask)` filters out zero/negative bid-ask — flag any quote usage that bypasses this filter.

### Strategies (`src/strategies/`)
- 31 strategies built via `call_leg`, `put_leg`, `strategy` helpers. New strategies must follow this pattern.
- `find_strategy(name)` does linear scan — acceptable since it runs once per backtest, not in hot loops.
- Multi-expiration strategies use `ExpirationCycle::Primary`/`Secondary` tags on legs.

### Signals (`src/signals/`)
- 40+ built-in signals in `spec.rs`. Signal evaluation must be pure (no side effects, no state mutation).
- Entry signals are optional for options backtests, **required** for stock backtests.
- OHLCV data is auto-fetched when signals are used — flag any code that manually fetches price data when signals are already configured.

### Data Layer (`src/data/`)
- Path traversal protection via `validate_path_segment()` — flag any file path construction that bypasses this check.
- Parquet date normalization: `quote_date`/`quote_datetime` → unified `Datetime("quote_datetime")`. Flag any code that assumes a specific date column format without going through normalization.
- Cache pattern: local Parquet → S3 fallback → error. Flag any direct S3 access that skips the cache layer.

## Validation (`garde`)

- All user-facing parameter structs must derive `garde::Validate`.
- Nested structs need `#[garde(dive)]` to validate recursively.
- `SignalSpec` fields use `#[garde(skip)]` — signal validation happens separately.
- `exit_dte` must be less than `entry_dte.min` — enforced by `validate_exit_dte_lt_max`.
- `TargetRange` requires `min ≤ max` — flag any construction that doesn't enforce this.

## Numeric Safety

- Financial calculations must use `f64`. Flag any use of `f32` for monetary values or returns.
- Division by zero: flag any division without a zero-check guard, especially in metrics calculations (Sharpe, Sortino, profit factor).
- NaN/Inf propagation: flag calculations that could produce NaN (0.0/0.0) or Inf without handling.
- Position sizing must respect `SizingConstraints` (min/max quantity clamps).

## Concurrency & Safety

- Server state is `Arc<RwLock<HashMap<String, DataFrame>>>` — flag any code that:
  - Holds a write lock across await points (potential deadlock).
  - Clones entire DataFrames unnecessarily when a reference or lazy frame would suffice.
  - Accesses shared state without proper locking.
- MCP tool handlers are async — flag any blocking I/O (`std::fs`, `std::thread::sleep`) that should use async equivalents.

## Testing

- Unit tests live in `src/**/tests.rs` modules; integration tests in `tests/`.
- Use `tempfile::NamedTempFile` for test cache isolation — flag tests that write to the real cache directory.
- New tools and engine features must have corresponding tests.
- DataFrame test assertions should use `polars::testing` utilities.

## Performance

- Polars operations should use lazy evaluation (`.lazy()` → chain → `.collect()`) rather than eager operations in data pipelines.
- Flag unnecessary `.collect()` calls in the middle of filter chains — prefer chaining lazy operations.
- The event simulation loops (`event_sim.rs`, `stock_sim.rs`) are hot paths — flag allocations, clones, or hash lookups that could be hoisted out of the loop.

## Security

- Path traversal: all file paths derived from user input (symbol names, etc.) must go through `validate_path_segment()`.
- No secrets in code: API keys (`EODHD_API_KEY`, AWS credentials) must come from environment variables only.
- MCP tool inputs are untrusted — all must be validated before use.

## Response Format

- All tool responses use types from `response_types.rs` deriving `Serialize + JsonSchema`.
- Equity curves are sampled to ≤50 points via the AI format layer — flag any response that returns unbounded data arrays.
- AI format enrichment (`ai_format/`) must not fail the tool — errors in formatting should degrade gracefully.
