# Tech Debt Report

Generated 2026-03-11. Covers the full `src/` tree.

---

## 1. DRY Violations

### HIGH

#### 1.1 Exit trigger logic duplicated between options and stock backtests
- `src/engine/event_sim.rs:592-656` and `src/engine/stock_sim.rs:244-308`
- Stop loss, take profit, max hold, and signal exit checks are implemented independently in both files with near-identical structure
- **Fix:** Extract a shared `check_exit_conditions()` function parameterized by price source

#### 1.2 OBV/CMF signal pairs duplicate computation logic
- `src/signals/volume.rs:42-99` — `ObvRising` and `ObvFalling` share 16 lines of identical loop logic, differing only in `>` vs `<`
- `src/signals/volume.rs:140-191` — `CmfPositive` and `CmfNegative` same pattern
- **Fix:** Extract `compute_obv()` / `compute_cmf_values()` helpers, call from both variants

#### 1.3 Aroon/Supertrend duplication in trend signals
- `src/signals/trend.rs:14-67` — AroonUptrend and AroonDowntrend duplicate the aroon computation, differing only in `> 0.0` vs `< 0.0`
- `src/signals/trend.rs:110-177` — SupertrendBullish/Bearish same pattern
- **Fix:** Extract shared computation, parameterize the comparison

#### 1.4 Indicator dedup logic copied across tool modules
- `src/tools/backtest.rs:40-50` and `src/tools/stock_backtest.rs:45-54` — identical 10-line dedup block
- **Fix:** Extract `extend_indicators_deduped(target: &mut Vec<IndicatorData>, new: Vec<IndicatorData>)` helper

### MEDIUM

#### 1.5 Sizing summary functions nearly identical
- `src/tools/ai_format/backtest.rs:155-178` (`build_sizing_summary`) and lines 181-204 (`build_stock_sizing_summary`)
- 16 lines of identical trade log aggregation, differing only in capital source
- **Fix:** Extract shared `compute_sizing_stats(trade_log, capital)` function

#### 1.6 Signal tree traversal duplicated for IV detection
- `src/engine/core.rs:75-123` — `contains_iv_signal()` and `contains_non_iv_signal()` have identical recursive traversal differing only in which variants match
- **Fix:** Generic `traverse_signal_spec(spec, predicate)` function

#### 1.7 Path construction pattern repeated 3x in cache layer
- `src/data/cache.rs:83-107` — `cache_path()`, `local_path()`, and `ensure_local_for()` all construct `{dir}/{category}/{SYMBOL}.parquet` independently with separate `.to_uppercase()` calls
- **Fix:** Single private `build_parquet_path(&self, symbol, category)` helper

#### 1.8 Parquet path string conversion repeated 3x
- `src/data/parquet.rs:84,132,146` — `.to_string_lossy().to_string()` followed by `.as_str().into()` in three methods
- **Fix:** Private `fn scan_path(&self) -> PlRefPath` helper

#### 1.9 BacktestResponse vs StockBacktestResponse field overlap
- `src/tools/response_types.rs:57-78` and lines 137-161
- 8+ identical fields (summary, assessment, key_findings, metrics, trade_summary, trade_log, sizing_summary, underlying_prices, indicator_data, suggested_next_steps)
- **Fix:** Extract `BacktestResultCommon` struct, compose into both response types

---

## 2. Abstraction Opportunities

### MEDIUM

#### 2.1 Indicator computation boilerplate in indicators.rs
- `src/signals/indicators.rs` — 15+ `compute_*_indicator()` functions repeat: column extraction, length check, compute, pad, make_indicator
- **Fix:** Generic `compute_single_indicator<F>(df, col, min_len, compute_fn, name, display, label, thresholds)` helper

#### 2.2 Server tool handler boilerplate
- `src/server/mod.rs` — 11+ tool handlers repeat the same 5-line wrapper: `SanitizedResult(async { params.validate().map_err(...)? ... }.await)`
- **Fix:** Macro or higher-order function to eliminate per-handler boilerplate

#### 2.3 `compute_indicator_data()` is 250 lines
- `src/signals/indicators.rs:28-277` — `#[allow(clippy::too_many_lines)]` suppresses the warning
- **Fix:** Split into `compute_momentum_indicators()`, `compute_overlap_indicators()`, etc.

#### 2.4 CompareEntry to BacktestParams manual field copy
- `src/engine/core.rs:374-403` — 18+ fields manually copied with `.clone()` in a loop
- **Fix:** `impl From<(&CompareEntry, &SimParams)> for BacktestParams`

### LOW

#### 2.5 AI format functions exceed 100 lines
- `src/tools/ai_format/backtest.rs:23-152` (130 lines), lines 305-407 (103 lines), `src/tools/ai_format/advanced.rs:22-167` (145 lines)
- **Fix:** Extract `compute_key_findings()` and `compute_next_steps()` sub-functions

#### 2.6 Suggested next steps built ad-hoc everywhere
- `src/tools/ai_format/backtest.rs`, `src/tools/ai_format/advanced.rs`, `src/tools/build_signal.rs`
- Hand-coded string arrays with overlapping content
- **Fix:** Centralized `NextSteps` enum or builder

---

## 3. Dead Code & Unused Dependencies

### MEDIUM

#### 3.1 `tools::load_data` module declared but never invoked
- `src/tools/mod.rs:14` — `pub mod load_data;` exists but no tool handler calls it
- **Fix:** Remove if unused, or wire up the tool handler

### LOW

#### 3.2 `DataStore` trait methods `list_symbols()` and `date_range()` never called in production
- `src/data/mod.rs:24-28` — trait definition; implemented in cache.rs and parquet.rs but never used outside tests
- **Fix:** Remove from trait or mark as `cfg(test)` if only for testing

#### 3.3 `SignalFn::name()` only used in tests
- `src/signals/helpers.rs:9-10` — `#[allow(dead_code)]` is correct; method exists only for test assertions
- 36+ trivial name tests across momentum.rs, volume.rs, overlap.rs, trend.rs, volatility.rs, price.rs add ~300 lines
- **Fix:** Consider removing `name()` from trait and the associated test boilerplate, or consolidate into a single parameterized test

---

## 4. Inconsistencies

### MEDIUM

#### 4.1 Error wrapping patterns vary across server handlers
- `src/server/mod.rs` — some handlers use `.map_err(|e| format!("Error: {e}"))`, others use `.map_err(|e| e.to_string())`
- **Fix:** Standardize on a single error formatting pattern

### LOW

#### 4.2 Validation logic duplicated between BacktestBaseParams and ServerCompareEntry
- `src/server/params.rs:103` and line 352 — same `validate_exit_dte_lt_entry_min` applied independently
- **Fix:** Share via a common sub-struct or validation trait

#### 4.3 Default function proliferation
- `src/server/params.rs:56-83` — 8 trivial one-liner `fn default_*()` functions
- **Fix:** Consolidate into a `Defaults` const block or use `const` values where serde allows

---

## 5. Complexity & Maintainability

### MEDIUM

#### 5.1 `run_event_loop()` — 224 lines with complex state machine
- `src/engine/event_sim.rs:366-590`
- Interleaves entry candidate selection, position management, daily MTM, exit checks, and trade logging
- **Fix:** Extract `process_exits()`, `process_entries()`, `update_equity()` sub-functions

#### 5.2 `build_signal_filters()` — high cyclomatic complexity
- `src/engine/core.rs:132-237` — nested conditionals for IV/non-IV/cross-symbol/OHLCV paths
- **Fix:** Split into `resolve_ohlcv_signals()` and `resolve_iv_signals()` phases

#### 5.3 `run_stock_backtest()` — 162 lines
- `src/engine/stock_sim.rs:76-238` — interleaves entry/exit/equity logic
- **Fix:** Extract exit decision and position closing into helpers (partially done with `close_position()`)

### LOW

#### 5.4 Slippage formatting duplicated in compare functions
- `src/engine/core.rs:452-485` and lines 510-519 — same match on `Slippage` variants
- **Fix:** `impl Display for Slippage` or shared formatter

---

## 6. Type System & Safety

### MEDIUM

#### 6.1 O(n^2) indicator deduplication
- `src/tools/backtest.rs:43-48` and `src/tools/stock_backtest.rs:48-53`
- `.iter().any(|existing| existing.name == ind.name)` is O(n) per insertion
- **Fix:** Use `HashSet<String>` for seen names

### LOW

#### 6.2 Excessive `.clone()` in compare_strategies loop
- `src/engine/core.rs:372-402` — 15+ `.clone()` calls copying entry and sim_params fields
- **Fix:** Take ownership or use references where possible; `From` impl would help

#### 6.3 `ExitDecision` struct exists in stock_sim but not event_sim
- `src/engine/stock_sim.rs:66-69` vs `src/engine/event_sim.rs` (inlined logic)
- **Fix:** Share `ExitDecision` struct across both modules
