# Codebase Audit Report

**Date:** 2026-03-08
**Scope:** Full codebase audit covering engine, data layer, server, tools, strategies, and signals.

---

## Critical Issues

### 1. Condor strategies use identical deltas for inner legs (strategies/condors.rs)

Both short legs target `default_otm_delta()` (0.30) and both long legs target `default_deep_otm_delta()` (0.10). A condor requires four distinct strikes. With identical delta targets, the two short legs may select the same strike, collapsing the condor into a butterfly.

**Fix:** Use distinct delta defaults for inner vs outer legs (e.g., short call at 0.30, short put at 0.70).

### 2. Commission may be double-counted during adjustment flows (engine/event_sim.rs)

In `close_position` (line ~825), commission is applied as `* 2.0` to cover entry+exit. However, `finalize_if_all_closed` (line ~1190) also applies `commission * 2.0`. If legs are partially closed during adjustments and then finalized, the individually-closed legs get commission charged twice.

### 3. Category string case mismatch makes strategy tests no-ops (strategies/mod.rs:70,85)

Tests compare against lowercase `"spreads"` and `"butterflies"`, but actual category values are capitalized (`"Spreads"`, `"Butterflies"`). The assertions never fire, making these tests silently pass without validating anything.

### 4. Max drawdown calculation ignores initial capital (engine/metrics.rs:267)

Peak is initialized from `equity_curve[0].equity` rather than `initial_capital`. If the first equity point is already below initial capital, the drawdown from initial capital to the first point is missed.

---

## Performance Issues

### 5. `update_last_known` scans entire price table every trading day — O(M*T) (engine/event_sim.rs:1274-1285)

Iterates the entire `PriceTable` HashMap on every trading day to find entries matching `today`. A date-indexed secondary structure would reduce this to O(M) total.

### 6. Chained filter functions each materialize a full DataFrame (engine/filters.rs)

Each filter function calls `df.clone().lazy()...collect()`. When chained in `find_entry_candidates`, each intermediate result is fully materialized. Using Polars lazy API end-to-end would eliminate intermediate materializations.

### 7. Sequential `spawn_blocking` calls for date filtering (data/parquet.rs:107-124)

Start and end date filters are applied in two separate `spawn_blocking` calls. These could be combined into a single lazy filter chain in one blocking call.

---

## Logic Issues

### 8. Diagonal spreads use identical deltas for both legs (strategies/calendar.rs:36-39)

Both legs use `default_otm_delta()`. A diagonal spread requires different strikes (different deltas). Using the same delta degenerates it into a calendar spread.

### 9. `covered_call` has no stock leg (strategies/singles.rs:39-46)

Defined as just `call_leg(Side::Short, 1, default_otm_delta())` — identical to `short_call`. The description says "Sell a call against long stock" but the backtest won't model the long stock.

### 10. `short_put` and `cash_secured_put` are identical (strategies/singles.rs)

Both create the exact same leg: `put_leg(Side::Short, 1, default_otm_delta())`. Only the name differs.

### 11. DTE exit uses only primary expiration for multi-exp strategies (engine/event_sim.rs:632-634)

`check_exit_triggers` uses `position.expiration` (primary) for DTE exit. For calendar/diagonal strategies, secondary legs may have a later expiration that is ignored.

### 12. Inconsistent variance conventions — Sharpe vs Sortino (engine/metrics.rs:250,262)

Sharpe uses sample variance (N-1 denominator) while Sortino uses population variance (N denominator). This inconsistency can mislead ratio comparisons.

### 13. Inconsistent null handling between fast and slow price table paths (engine/event_sim.rs:97-108 vs 156-165)

The fast path skips null rows via `continue`, while the slow path defaults nulls to `0.0`. A null strike of 0.0 creates a valid-looking entry in the price table.

---

## Data Layer Issues

### 14. Fragile date parsing via Debug format (data/parquet.rs:154-158)

Uses `format!("{:?}", value)` then `parse_from_str(..., "%Y-%m-%d")`. After `normalize_quote_datetime` converts to Datetime, the Debug output may include time components, breaking the parse. This is a latent bug.

### 15. `local_path` bypasses path validation (data/cache.rs:93-97)

`cache_path()` calls `validate_path_segment()` on the symbol, but `local_path()` does not. If `local_path` is called with untrusted input, it's vulnerable to path traversal.

### 16. Inconsistent symbol casing: `local_path` vs `cache_path` (data/cache.rs)

`cache_path()` uppercases the symbol; `local_path()` uses it as-is. `date_range("spy")` would look for `spy.parquet` while `cache_path` looks for `SPY.parquet`.

### 17. Blocking filesystem I/O in async context (data/cache.rs:131-136, 162-178)

`ensure_local_for()` calls `std::fs::create_dir_all` and `std::fs::write` synchronously in an async fn, blocking the tokio runtime. Should use `tokio::fs` or `spawn_blocking`.

### 18. Non-atomic cache writes (data/cache.rs:135)

`std::fs::write()` writes directly to the final path. A crash mid-write leaves a corrupt file treated as a cache hit on restart. Should write to a temp file then atomically rename.

---

## Error Handling Issues

### 19. `.unwrap()` panics in `load_underlying_closes` (server.rs:205,208)

`c.date().unwrap()` and `c.f64().unwrap()` will panic if columns exist with unexpected dtypes. The outer `let...else` only handles missing columns.

### 20. `.expect()` in tool handler can crash server (tools/construct_signal.rs:20)

`serde_json::to_value(&schema).expect(...)` will panic on serialization failure. Should return an error.

### 21. `compare_strategies` silently swallows backtest failures (engine/core.rs:364-379)

Failed strategies produce zero-filled results indistinguishable from "0 trades, 0 P&L". Error messages are lost.

### 22. `calculate_var` panics on empty input (engine/metrics.rs:292-295)

`sorted.len() - 1` causes usize underflow if slice is empty. Currently guarded by callers, but unsafe as a standalone function.

### 23. TOCTOU race in `ensure_data_loaded` (server.rs:50-83)

Reads lock, drops it, then acquires write lock. Concurrent requests for the same symbol can both pass the check and trigger redundant loads.

---

## Signal Issues

### 24. IV signals require pre-processed "iv" column with no validation (signals/volatility.rs:318)

Standard OHLCV DataFrames from `fetch_to_parquet` don't contain an "iv" column. Users get a confusing "column not found" error.

### 25. No validation on zero-valued signal parameters (signals/price.rs)

`ConsecutiveUp { count: 0 }` matches any up-move. `RateOfChange { period: 0 }` always produces ROC of 0. `DrawdownBelow { window: 0 }` never fires. No validation prevents these.

### 26. `active_dates()` silently ignores CrossSymbol references (signals/mod.rs:28-48)

If called with a spec containing `CrossSymbol`, the inner signal evaluates against the primary DataFrame, silently ignoring the cross-symbol reference.

### 27. Saved signal cycles silently degrade to false (signals/registry.rs:315-322)

Cyclic `Saved` signals silently evaluate as `false` when the depth limit (8) is reached, with only a log message.

---

## Code Quality

### 28. Massive struct duplication (server.rs:282-623)

`RunBacktestParams`, `WalkForwardParams`, and `PermutationTestParams` repeat nearly identical field definitions. A shared base struct would reduce maintenance burden.

### 29. Dead code: `ref_volume` in Liquidity slippage (engine/pricing.rs:14-17)

The `ref_volume` field is stored and validated but never used in `fill_price`.

### 30. Dead code: `validate_strike_order` (engine/rules.rs:10-24)

Marked `#[allow(dead_code)]`, never called.

### 31. Dead code: unused date column allocation (tools/fetch.rs:137-154)

A null-filled date column is allocated then immediately dropped.

### 32. Duplicate `BacktestParamsSummary` construction (tools/ai_format.rs:264-294, 356-386)

~25 lines of identical struct construction duplicated in two branches.

### 33. Hardcoded version string (server.rs:1875)

`version: "0.1.0"` hardcoded instead of using `env!("CARGO_PKG_VERSION")`.

### 34. Unnecessary `#[allow(dead_code, async_fn_in_trait)]` (data/mod.rs:10)

`async_fn_in_trait` is stable since Rust 1.75. `dead_code` may hide genuinely unused code.

### 35. Fuzzy search scoring double-counts (tools/construct_signal.rs:128-141)

A token can score both +3 (exact match) AND +1 (substring) because the second check is `if` not `else if`.

---

## Recommended Priority

| Priority | Issues |
|----------|--------|
| **P0 — Fix now** | #1 (condor deltas), #2 (commission double-count), #3 (test no-ops), #4 (max drawdown) |
| **P1 — Fix soon** | #5 (price table scan), #8 (diagonal deltas), #13 (null handling), #14 (date parsing), #15 (path validation), #19-20 (panics) |
| **P2 — Improve** | #6-7 (perf), #9-12 (logic), #16-18 (data layer), #21-27 (errors/signals) |
| **P3 — Cleanup** | #28-35 (code quality) |
