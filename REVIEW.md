# Stock-Mode DRY Refactoring Tasks

This document describes code duplication and quality issues in the stock-mode feature (commit `688a718`). An agent should address these items in priority order. Each task includes the exact files/lines, what's wrong, and what to do.

After each change: run `cargo build`, `cargo clippy --all-targets`, and `cargo test`. All must pass before moving to the next task.

---

## Task 1: Extract permutation metric construction helper

**Files:** `src/engine/permutation.rs`

**Problem:** Lines ~116-142 (options `run_permutation_test`) and ~416-442 (stock `run_stock_permutation_test`) contain identical 5-metric `compute_metric_result()` call sequences repeated verbatim (27 lines each).

**Fix:** Extract a shared helper:

```rust
fn build_metric_results(
    real_metrics: &PerformanceMetrics,
    real_total_pnl: f64,
    perm_metrics: &[PermMetrics],
) -> Vec<MetricPermutationResult>
```

Call it from both `run_permutation_test()` and `run_stock_permutation_test()`. Delete the duplicated blocks.

---

## Task 2: Extract `build_stock_params_for_combo()` in sweep

**Files:** `src/engine/sweep.rs`

**Problem:** The pattern of cloning `base_params` and overriding 8 fields appears 3 times in `run_stock_sweep()` (train run ~line 694, OOS validation ~line 828, multiple comparisons ~line 1028). The options sweep already has `build_backtest_params_for_combo()` as a centralized builder — stock sweep lacks an equivalent.

**Fix:** Create:

```rust
fn build_stock_params_for_combo(
    base: &StockBacktestParams,
    combo: &StockCombo,
) -> StockBacktestParams
```

It should clone `base` and override: `entry_signal`, `exit_signal`, `side`, `interval`, `session_filter`, `stop_loss`, `take_profit`, `slippage`. Replace all 3 inline clone+override sites with calls to this function.

---

## Task 3: Use `filter_datetime_set()` in sweep instead of inline duplication

**Files:** `src/engine/sweep.rs`

**Problem:** The signal date filtering pattern (filter a `HashSet<NaiveDateTime>` to a bar date range using `first()`/`last()` with `filter_datetime_set()`) is written inline 4 times in `run_stock_sweep()`:
- Train entry dates (~line 688-697)
- Train exit dates (~line 720-730)
- OOS test entry dates (~line 806-815)
- OOS test exit dates (~line 851-861)

Each instance has the identical `if let (Some(first), Some(last))` guard returning `HashSet::new()` on empty bars.

**Fix:** Extract a helper in `sweep.rs` (or use `stock_sim::filter_datetime_set` directly):

```rust
fn filter_signals_to_bar_range(
    dates: &Option<HashSet<NaiveDateTime>>,
    bars: &[Bar],
) -> Option<HashSet<NaiveDateTime>>
```

Replace all 4 inline blocks with calls to this function.

---

## Task 4: Unify walk-forward format helpers

**Files:** `src/tools/ai_format/advanced.rs`

**Problem:** `format_walk_forward()` (lines ~224-356) and `format_walk_forward_stock()` (lines ~358-438) are ~80% identical. Both:
- Compute `window_desc` from aggregate
- Build summary string
- Map `WindowResult` to `WalkForwardWindowResult`
- Call `walk_forward_findings()`
- Build `suggested_next_steps` with identical conditional logic
- Construct `WalkForwardResponse` with identical field mapping

The only differences are: summary prefix string, `mode` field, and minor wording in next steps.

**Fix:** Delete `format_walk_forward_stock()`. Modify `format_walk_forward()` to take a `label: &str` and `mode: Option<&str>` instead of `params: &BacktestParams`. Update the two call sites:
- `tools/walk_forward.rs::execute()` — pass `params.strategy` as label, `None` as mode
- `tools/walk_forward.rs::execute_stock()` — pass the label string, `Some("stock")` as mode

---

## Task 5: Unify permutation test format helpers

**Files:** `src/tools/ai_format/advanced.rs`

**Problem:** `format_permutation_test()` (lines ~265-350) and `format_permutation_test_stock()` (lines ~443-559) share ~90% identical logic: p-value extraction, significance assessment, key findings construction, and suggested next steps. The stock version builds a `BacktestParamsSummary` with actual params from `StockBacktestParams` — it's not dummy data, but the construction is verbose.

**Fix:** Extract the shared logic into a private helper that both functions call. The helper should take: `output: PermutationOutput`, `label: &str`, `mode: Option<&str>`, and `parameters: BacktestParamsSummary`. Each public function becomes a thin wrapper that constructs the `BacktestParamsSummary` and delegates.

Update call sites in `tools/permutation_test.rs`.

---

## Task 6: Replace label-based combo matching with index in sweep

**Files:** `src/engine/sweep.rs`

**Problem:** OOS validation (~line 772) and multiple comparisons (~line 941) use `combos.iter().find(|c| c.label == r.label)` to match results back to combos. If two combos generate identical labels, only the first match is found — silent data loss.

**Fix:** Store the combo index in `SweepResult` (add a private `combo_idx: usize` field or use the position in the results vec). Replace string-based `find()` with direct index lookup: `&combos[r.combo_idx]`. This is a localized change within `run_stock_sweep()` — the `combo_idx` field doesn't need to be serialized or exposed outside the sweep module.

---

## Task 7: Deduplicate walk-forward engine functions

**Files:** `src/engine/walk_forward.rs`

**Problem:** `run_walk_forward()` (lines ~118-206) and `run_walk_forward_stock()` (lines ~248-363) share ~80% identical structure:
- Parameter validation (train_days, test_days, step)
- Date range extraction and span check
- Cursor iteration with `Days::new()`
- Train/test slicing
- Result accumulation into `WindowResult`
- Failure counting and aggregate computation

The only differences: data type (`DataFrame` vs `&[Bar]`), slice function, backtest call, and signal date filtering.

**Fix:** Extract the shared validation and cursor logic into a helper. Use a closure or callback approach:

```rust
fn walk_forward_driver<F>(
    min_date: NaiveDate,
    max_date: NaiveDate,
    train_days: i32,
    test_days: i32,
    step_days: Option<i32>,
    run_window: F,
) -> Result<WalkForwardResult>
where
    F: FnMut(NaiveDate, NaiveDate, NaiveDate, NaiveDate) -> Option<WindowResult>,
```

Both `run_walk_forward()` and `run_walk_forward_stock()` become thin wrappers that extract date bounds from their data source and provide the `run_window` closure.

---

## Notes

- Do NOT refactor the server handler branching (`server/mod.rs`). The `is_stock` forks are verbose but straightforward, and unifying them would require a large abstraction (trait or enum) that adds complexity without proportional benefit.
- Do NOT refactor `BacktestBaseParams` in `params.rs`. The monolithic struct with mode-aware validation works and changing it would break the MCP schema.
- Run `cargo fmt --check` after all changes.
- Do not add new dependencies.
