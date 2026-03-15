# Stock Pattern Analysis — Implementation Plan

## Summary

Implement 5 new read-only MCP analysis tools (`aggregate_prices`, `correlate`, `distribution`, `regime_detect`, `rolling_metric`) plus a shared `stats` module. These tools provide composable statistical primitives over OHLCV price data, following existing codebase patterns.

## Key Codebase Patterns to Follow

- **Params**: Define in `src/server/params.rs` with `#[derive(Deserialize, JsonSchema, Validate)]`
- **Response types**: Define in `src/tools/response_types.rs` with `#[derive(Serialize, Deserialize, JsonSchema)]`
- **Tool logic**: Each tool gets a module in `src/tools/` with `pub fn execute(...)` or `pub async fn execute(...)`
- **AI formatting**: Each tool gets formatting functions in `src/tools/ai_format/` (summary + suggested_next_steps)
- **Server registration**: `#[tool(name = "...", annotations(read_only_hint = true))]` in `src/server/mod.rs`
- **Validation**: `params.validate().map_err(|e| validation_err("tool_name", e))?`
- **OHLCV loading**: Use `tools::raw_prices::load_and_execute()` or directly via `CachedStore` + parquet scan
- **Return type**: `SanitizedResult<ResponseType, String>` for tools that can fail
- **New dependency**: `statrs` crate for t-distribution and chi-squared CDFs

## Implementation Steps

### Phase 1: Foundation — Shared Stats Module + Dependency

**Step 1.1: Add `statrs` dependency**
- File: `Cargo.toml`
- Add `statrs = "0.18"` (pure Rust, no unsafe, minimal deps)
- Also add `approx = "0.5"` to dev-dependencies for `assert_relative_eq!` in tests

**Step 1.2: Create `src/stats/mod.rs` module**
- File: `src/lib.rs` — add `pub mod stats;`
- Files to create:
  - `src/stats/mod.rs` — re-exports
  - `src/stats/descriptive.rs` — `mean`, `std_dev`, `median`, `percentile`, `skewness`, `kurtosis`
  - `src/stats/correlation.rs` — `pearson`, `spearman`, `covariance`
  - `src/stats/hypothesis.rs` — `t_test_one_sample`, `jarque_bera` (using `statrs` for CDFs)
  - `src/stats/histogram.rs` — `histogram()` builder → `Vec<HistogramBucket>`
  - `src/stats/rolling.rs` — `rolling_apply()` generic rolling window computation

**Implementation notes:**
- All functions operate on `&[f64]` slices for zero-copy from Polars columns
- `spearman()` uses average (fractional) ranks for ties
- `t_test_one_sample` returns `(t_stat, p_value)` using `statrs::distribution::StudentsT`
- `jarque_bera` returns `(jb_stat, p_value)` using `statrs::distribution::ChiSquared`
- Unit tests validate against scipy/numpy reference values (documented in comments)

### Phase 2: `aggregate_prices` Tool

**Step 2.1: Response types**
- File: `src/tools/response_types.rs`
- Add: `AggregatePricesResponse`, `AggregateBucket` structs

**Step 2.2: Params**
- File: `src/server/params.rs`
- Add: `AggregatePricesParams` with `symbol`, `years`, `interval`, `group_by`, `metric`, `filters`
- Validation: `group_by` of `time_of_day`/`hour_of_day` requires intraday `interval`

**Step 2.3: Tool logic**
- File: `src/tools/aggregate_prices.rs`
- Logic: Load OHLCV → apply filters → compute per-bar metric → assign buckets → compute bucket stats (using `src/stats/`) → sort naturally → generate summary/warnings

**Step 2.4: AI formatting**
- File: `src/tools/ai_format/analysis.rs` (new submodule for all 5 analysis tools)
- Function: `format_aggregate_prices()` — summary highlighting significant buckets (p<0.05), contextual next steps

**Step 2.5: Server registration**
- File: `src/server/mod.rs`
- Add `#[tool(name = "aggregate_prices", annotations(read_only_hint = true))]` handler
- Pattern: validate → load OHLCV via `load_and_execute` or direct parquet scan → call `tools::aggregate_prices::execute()` → wrap in `SanitizedResult`

**Step 2.6: Wire up modules**
- File: `src/tools/mod.rs` — add `pub mod aggregate_prices;`
- File: `src/tools/ai_format/mod.rs` — add `mod analysis;` and re-export

### Phase 3: `distribution` Tool

**Step 3.1: Response types**
- Add: `DistributionResponse`, `NormalityTest`, `TailRatio`, `HistogramBucket` (response version)

**Step 3.2: Params**
- Add: `DistributionParams` with `source` (tagged: `price_returns` with symbol/interval/years, or `trade_pnl` with values array), `n_bins`, `tests`

**Step 3.3: Tool logic**
- File: `src/tools/distribution.rs`
- Two paths: `price_returns` loads OHLCV and computes returns; `trade_pnl` uses provided array
- Compute descriptive stats, histogram, normality test, tail ratio using `src/stats/`

**Step 3.4: AI formatting + server registration**
- Same pattern as Phase 2

### Phase 4: `correlate` Tool

**Step 4.1: Response types**
- Add: `CorrelateResponse`, `RollingCorrelationPoint`, `ScatterPoint`

**Step 4.2: Params**
- Add: `CorrelateParams` with `series_a`, `series_b` (each has symbol + field), `mode`, `window`, `lag`, `years`, `interval`
- Validation: both symbols must be provided; lag < n_observations / 2

**Step 4.3: Tool logic**
- File: `src/tools/correlate.rs`
- Load both OHLCV series → align by date → compute fields → apply lag → full-period stats (pearson, spearman, r², p-value) → optional rolling correlation → scatter data (subsample to 500 max)

**Step 4.4: AI formatting + server registration**

### Phase 5: `rolling_metric` Tool

**Step 5.1: Response types**
- Add: `RollingMetricResponse`, `RollingPoint`, `RollingStats`

**Step 5.2: Params**
- Add: `RollingMetricParams` with `source` (symbol), `metric`, `window`, `benchmark`, `years`, `interval`
- Validation: `beta`/`correlation` require `benchmark`

**Step 5.3: Tool logic**
- File: `src/tools/rolling_metric.rs`
- Load OHLCV (+ benchmark if needed) → compute returns → rolling window computation using `stats::rolling_apply` → compute summary stats + trend detection (linear regression slope)
- Uses `Interval::bars_per_year()` for annualization (already exists)

**Step 5.4: AI formatting + server registration**

### Phase 6: `regime_detect` Tool

**Step 6.1: Response types**
- Add: `RegimeDetectResponse`, `RegimeInfo`, `RegimeSeriesPoint`

**Step 6.2: Params**
- Add: `RegimeDetectParams` with `symbol`, `method`, `n_regimes`, `years`, `interval`, `lookback_window`
- Validation: `n_regimes` 2-4, `interval` must be `D` or `1h`

**Step 6.3: Tool logic**
- File: `src/tools/regime_detect.rs`
- `volatility_cluster`: rolling realized vol → quantile thresholds → classify → per-regime stats → transition matrix
- `trend_state`: short/long SMA → trend strength → classify → same stats + transition matrix
- Downsampling: when intraday, collapse `regime_series` to daily resolution

**Step 6.4: AI formatting + server registration**

### Phase 7: Integration & Testing

**Step 7.1: Unit tests for stats module**
- Each function tested against scipy/numpy reference values
- Edge cases: empty slices, single element, identical values, NaN handling

**Step 7.2: Unit tests per tool**
- Create synthetic DataFrames (like `make_test_df()` pattern in `raw_prices.rs`)
- Test: correct bucket counts, validation errors, edge cases

**Step 7.3: Cargo fmt + clippy + build verification**
- `cargo fmt --check`
- `cargo clippy --all-targets` (CI enforces `-D warnings`)
- `cargo test`

## File Change Summary

### New files (12):
- `src/stats/mod.rs`
- `src/stats/descriptive.rs`
- `src/stats/correlation.rs`
- `src/stats/hypothesis.rs`
- `src/stats/histogram.rs`
- `src/stats/rolling.rs`
- `src/tools/aggregate_prices.rs`
- `src/tools/distribution.rs`
- `src/tools/correlate.rs`
- `src/tools/rolling_metric.rs`
- `src/tools/regime_detect.rs`
- `src/tools/ai_format/analysis.rs`

### Modified files (5):
- `Cargo.toml` — add `statrs`, `approx`
- `src/lib.rs` — add `pub mod stats;`
- `src/tools/mod.rs` — add 5 new tool modules
- `src/tools/ai_format/mod.rs` — add `analysis` submodule + re-exports
- `src/server/mod.rs` — add 5 tool handlers + param imports
- `src/server/params.rs` — add 5 param structs
- `src/tools/response_types.rs` — add 5 response type groups

## Design Decisions

1. **Single `stats` module** rather than inline math — reusable across all 5 tools, testable in isolation
2. **`statrs` for CDFs only** — hand-roll descriptive stats (simple, no allocation overhead), use statrs only for t-distribution and chi-squared CDFs which are non-trivial
3. **`analysis.rs` in ai_format** — single file for all 5 tools' formatting (they're similar in structure), avoids 5 tiny files
4. **OHLCV loading reuse** — all tools load via the same `CachedStore` + Yahoo auto-fetch pattern used by `get_raw_prices` and `run_stock_backtest`
5. **No intraday data source initially** — Yahoo Finance API provides daily data; intraday intervals (1m/5m/30m/1h) are supported structurally but will only work if the user has cached intraday parquet data. The `aggregate_prices` validation for `time_of_day`/`hour_of_day` will gate this properly.
6. **All tools are `read_only_hint = true`** — no state mutation, safe to cache/retry
