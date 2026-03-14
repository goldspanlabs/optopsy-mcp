---
applyTo: "**/*.rs"
---

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

## Logic Correctness

Scrutinize proposed changes for logical errors, especially in financial calculation paths:

- **Off-by-one errors** — fence-post mistakes in date ranges, DTE calculations, window boundaries (walk-forward train/test splits), and loop indices over trade logs or equity curves.
- **Boundary conditions** — empty DataFrames, zero-trade backtests, single-element collections, first/last day edge cases. Flag code that assumes non-empty inputs without checking.
- **Comparison and ordering bugs** — reversed `>` vs `<` in P&L thresholds (stop loss, take profit), wrong sign on `Side::Short` multipliers, sorting ascending when descending is intended (e.g., ranking by Sharpe).
- **State mutation ordering** — in the event loop (`event_sim.rs`, `stock_sim.rs`), position opens must happen after closes on the same date, equity must update before sizing calculations, and signal evaluation must precede entry checks.
- **Filter composition** — verify AND vs OR semantics when combining delta filters, DTE filters, and signal conditions. A misplaced `.or()` vs `.and()` silently changes which trades qualify.
- **Metric formula correctness** — Sharpe uses excess returns over risk-free, Sortino uses only downside deviation, CAGR requires annualization, max drawdown is peak-to-trough not trough-to-peak. Flag any deviation from standard financial definitions.
- **Rounding and truncation** — integer division where float division is needed (`i32 / i32` losing precision), premature rounding of intermediate financial results, and `as i32` truncation of quantities that should be rounded.
- **Early returns and short-circuits** — `?` or `return` that skip cleanup logic, position closing, or metric finalization. Ensure all code paths produce consistent output.

## DRY Compliance

Flag violations of Don't Repeat Yourself. This codebase has established shared utilities — new code must use them:

- **Existing shared helpers that must be reused:**
  - `ai_format/` — all tool responses must go through AI formatting, not build their own `summary`/`key_findings` inline.
  - `ai_helpers.rs` — shared constants and helper functions for response enrichment.
  - `filters.rs` — DTE filtering, delta filtering, `filter_valid_quotes()`. Do not re-implement quote filtering logic in tool or engine code.
  - `metrics.rs` / `PerformanceMetrics` — single source for Sharpe, Sortino, CAGR, etc. Do not compute these ad-hoc.
  - `pricing.rs` — slippage models. Do not inline spread calculations.
  - `sizing.rs` — position sizing methods. Do not duplicate max-loss or quantity computation.
  - `helpers.rs` in strategies — `call_leg`, `put_leg`, `strategy` builders. Do not construct `StrategyLeg` manually.
  - `response_types.rs` — shared response structs. Do not define one-off response types in tool modules.

- **Patterns to flag:**
  - Two or more tool handlers with identical data-loading, validation, or error-handling boilerplate that should be extracted.
  - Inline reimplementation of any logic that already exists in `src/engine/` (e.g., manually computing win rate instead of using `PerformanceMetrics`).
  - Duplicated Polars filter chains — if the same `.filter().filter().filter()` pattern appears in multiple places, it should be a shared function in `filters.rs`.
  - Copy-pasted match arms across signal builders — common signal construction patterns should use shared builder functions in `builders.rs`.

## Tech Debt Prevention

Flag changes that introduce or worsen technical debt:

- **Dead code & unused imports** — unreachable branches, commented-out code, `#[allow(dead_code)]` without justification.
- **Copy-paste duplication** — logic repeated across tool handlers or engine modules that should be extracted into a shared helper. Check `src/tools/` handlers for duplicated validation, data-loading, or formatting patterns.
- **Stringly-typed interfaces** — strategy names, column names, or signal identifiers passed as raw strings when an enum or newtype would prevent typos and enable compiler checks.
- **Oversized functions** — functions exceeding ~100 lines or with deep nesting (3+ levels). Suggest extraction of inner logic into well-named helpers.
- **Leaky abstractions** — tool handlers reaching into engine internals (e.g., directly manipulating DataFrames instead of going through engine APIs), or engine code aware of MCP/serialization concerns.
- **Missing or misleading error context** — bare `.unwrap()`, `.expect("failed")` without specifics, or `anyhow!("error")` without context about what failed and why. Prefer `.context("loading options data for {symbol}")` or descriptive `thiserror` variants.
- **Hardcoded magic numbers** — unnamed numeric literals in business logic (thresholds, defaults, multipliers). These should be named constants or configurable parameters.
- **Inconsistent patterns** — new code that solves the same problem differently from existing code without justification (e.g., a new tool that validates params differently, or a new engine path that computes metrics its own way instead of reusing `PerformanceMetrics`).
- **Growing parameter lists** — functions taking 5+ parameters that should be grouped into a config/params struct.
- **TODO/FIXME/HACK markers** — acceptable only with a linked issue or clear explanation. Flag any that are vague or open-ended.

## Response Format

- All tool responses use types from `response_types.rs` deriving `Serialize + JsonSchema`.
- Equity curves are sampled to ≤50 points via the AI format layer — flag any response that returns unbounded data arrays.
- AI format enrichment (`ai_format/`) must not fail the tool — errors in formatting should degrade gracefully.
