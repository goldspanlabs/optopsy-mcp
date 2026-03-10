# File Organization Refactoring Plan

## Motivation

The optopsy-mcp codebase has grown to ~27k lines in `src/` with 7 files exceeding 1,000 lines. The top 5 files alone account for 46% of the source. Breaking these down will improve navigability, reduce cognitive load per file, and make the codebase easier to review and maintain.

**Goal**: Split large files into focused modules without changing any public API or runtime behavior.

---

## Summary of Changes

| File | Current Lines | Split Into | Lines After (main file) |
|------|--------------|------------|------------------------|
| `src/engine/event_sim.rs` | 3,134 | 4 files | ~420 |
| `src/server.rs` | 2,109 | 3 files (directory module) | ~1,100 |
| `src/signals/registry.rs` | 1,903 | 3 files | ~340 |
| `src/engine/sweep.rs` | 1,756 | 2 files | ~530 |
| `src/tools/ai_format.rs` | 1,700 | 2 files | ~600 |
| `src/engine/types.rs` | 1,154 | 2 files | ~660 |
| `src/engine/core.rs` | 1,165 | deferred | — |

---

## Detailed Plan

### 1. `src/engine/event_sim.rs` (3,134 lines → 4 files) — Highest Priority

The largest file in the project. Contains price table construction, entry candidate discovery, the main event loop, position lifecycle management, and a full adjustment system — all in one file.

**New modules:**

#### `src/engine/price_table.rs` (~240 lines)
- `build_price_table()` — public entry point
- `build_price_table_fast()` — fast path via contiguous column slices
- `build_price_table_slow()` — fallback per-row dispatch
- `build_date_index()` — maps quote dates to price keys
- `days_to_naive_date()` — epoch-offset conversion
- `extract_date_from_column()` — Date/Datetime/String normalization
- Associated tests

#### `src/engine/positions.rs` (~280 lines)
- `open_position()` — create Position from EntryCandidate
- `close_position()` — close all legs, calculate realized P&L, apply commission
- `close_leg()` — close a single PositionLeg
- `mark_to_market()` — compute unrealized P&L (stays `pub`)
- `compute_position_net_delta()` — sum signed deltas across legs
- `select_candidate()` — pick best candidate by TradeSelector
- `lookup_fill_price()` — O(1) price lookup with last-known fallback
- `update_last_known()` — maintain carry-forward price cache
- Associated tests

#### `src/engine/adjustments.rs` (~320 lines)
- `check_and_apply_adjustments()` — iterate positions, evaluate rules, apply first match
- `trigger_fires()` — check DefensiveRoll/CalendarRoll/DeltaDrift triggers
- `action_position_id()` — extract target position ID from action
- `execute_adjustment()` — apply Close/Roll/Add actions
- `finalize_if_all_closed()` — finalize position when all legs closed
- Associated tests

#### `src/engine/event_sim.rs` (remains, ~420 lines)
- `find_entry_candidates()` — entry candidate discovery
- `run_event_loop()` — main daily event loop
- `check_exit_triggers()` — evaluate exit conditions
- Re-exports from `price_table`, `positions`, `adjustments`

**Visibility changes**: Private functions become `pub(crate)` when accessed cross-module.

**Key risk**: `run_event_loop` calls into all three extracted modules. Imports must be wired correctly. `check_exit_triggers` stays in `event_sim.rs` since it's tightly coupled with the loop.

---

### 2. `src/server.rs` (2,109 lines → 3 files)

Convert from a single file to a directory module (`src/server/mod.rs`).

**New modules:**

#### `src/server/sanitize.rs` (~360 lines)
- `FiniteF64Wrap`, `FiniteF64Serializer`, `FiniteF64Compound`
- `SanitizedJson`, `SanitizedResult`
- `serialize_finite`, `finite_f64` serde module
- Associated tests

#### `src/server/params.rs` (~590 lines)
- All parameter structs: `BacktestBaseParams`, `RunBacktestParams`, `WalkForwardParams`, `PermutationTestParams`, `CompareStrategiesParams`, `CheckCacheParams`, `BuildSignalParams`, `FetchToParquetParams`, `GetRawPricesParams`, `SuggestParametersParams`, `ParameterSweepParams`, `SweepSimParams`
- Validation functions and `garde` custom validators
- Default value functions (`default_capital`, `default_quantity`, etc.)
- `resolve_leg_deltas`, `resolve_sweep_strategies`, `resolve_strategy_entries`
- `validation_err` helper

#### `src/server/mod.rs` (remains, ~1,100 lines)
- `OptopsyServer` struct and helper methods (`ensure_data_loaded`, `ensure_ohlcv`, `resolve_symbol`, etc.)
- `#[tool_router]` impl block (all 13+ tool handlers)
- `#[tool_handler]` ServerHandler trait impl
- `load_underlying_closes` standalone helper

**Constraint**: The `#[tool_router]` and `#[tool_handler]` macros from rmcp must stay in one impl block on one struct. This limits how much can be extracted from the tool handler section.

**Risk**: Import paths change. All `use crate::server::X` references in other files need updating to `use crate::server::params::X` or `use crate::server::sanitize::X`. Re-exports from `mod.rs` can mitigate this.

---

### 3. `src/signals/registry.rs` (1,903 lines → 3 files)

**New modules:**

#### `src/signals/spec.rs` (~280 lines)
- `SignalSpec` enum with all variants (Momentum, Overlap, Trend, Volatility, Price, Volume, Custom, Saved, CrossSymbol, combinators)
- `impl SignalSpec` methods

#### `src/signals/builders.rs` (~510 lines)
- `build_signal()` — main entry point
- `build_signal_depth()` — recursive builder with depth limit
- Category-specific builders: `build_momentum`, `build_overlap`, `build_trend`, `build_volatility`, `build_price`, `build_volume`
- Associated tests

#### `src/signals/registry.rs` (remains, ~340 lines)
- `SignalInfo` struct
- `SIGNAL_CATALOG` static array (35 entries)
- `collect_cross_symbols()` helper
- Re-exports of `SignalSpec` and builder functions

**Risk**: `SignalSpec` is imported across ~20 files as `crate::signals::registry::SignalSpec`. Add a re-export in `signals/mod.rs` (`pub use spec::SignalSpec`) so the existing path still works, or update all imports.

---

### 4. `src/engine/sweep.rs` (1,756 lines → 2 files)

#### `src/engine/sweep_analysis.rs` (~510 lines)
- Types: `SweepStrategyEntry`, `SweepDimensions`, `SweepParams`, `SweepOutput`, `SensitivityEntry`, etc.
- Cartesian product and range expansion helpers
- `count_independent_entry_periods`, `violates_delta_ordering`
- `signal_spec_label`, `build_signal_combos`, `SignalCombo`
- `compute_sensitivity`, `stability_fingerprint`, `compute_stability`
- Associated tests

#### `src/engine/sweep.rs` (remains, ~530 lines)
- `run_sweep()` — main sweep execution
- `build_backtest_params_for_combo()` — convert sweep config to backtest params
- `run_multiple_comparisons()` — statistical comparison across sweep results
- Re-exports from `sweep_analysis`
- Associated tests

---

### 5. `src/tools/ai_format.rs` (1,700 lines → 2 files)

#### `src/tools/ai_helpers.rs` (~320 lines)
- Assessment threshold constants (`GOOD_SHARPE`, `GOOD_WIN_RATE`, etc.)
- All helper functions: P&L formatting, date formatting, assessment text generation, summary builders
- Associated tests

#### `src/tools/ai_format.rs` (remains, ~600 lines)
- 8 main formatter functions:
  - `format_backtest`, `format_compare`, `format_load_data`, `format_strategies`
  - `format_raw_prices`, `format_sweep`, `format_walk_forward`, `format_permutation_test`

---

### 6. `src/engine/types.rs` (1,154 lines → 2 files)

#### `src/engine/sim_types.rs` (~110 lines)
- Event simulation types: `PriceKey`, `PriceTable`, `DateIndex`, `QuoteSnapshot`
- Position types: `Position`, `PositionLeg`, `PositionStatus`
- Entry types: `EntryCandidate`, `CandidateLeg`
- Adjustment types: `AdjustmentAction`, `AdjustmentTrigger`, `AdjustmentRule`
- Associated tests

#### `src/engine/types.rs` (remains, ~660 lines)
- Core enums: `Direction`, `Side`, `OptionType`, `ExpirationCycle`, `Slippage`, `TradeSelector`, `ExitType`
- Value types: `TargetRange`, `DteRange`, `Commission`
- Strategy types: `LegDef`, `StrategyDef`
- Param/result structs: `BacktestParams`, `CompareParams`, `SimParams`, `BacktestResult`, `PerformanceMetrics`, `TradeRecord`, `EquityPoint`, `SweepResult`
- Re-exports from `sim_types`

---

### 7. `src/engine/core.rs` (1,165 lines) — Deferred

This file is borderline. It has clear internal sections (OHLCV helpers, `run_backtest`, `compare_strategies`) but at ~1,165 lines it's manageable. Defer unless the other refactors create a natural opportunity to split.

---

## Implementation Order

Each file should be its own commit for clean git history and easy revert:

1. **`event_sim.rs`** — Largest impact, self-contained within `engine/`
2. **`server.rs`** — Second largest, directory module conversion
3. **`registry.rs`** — High import count but re-exports mitigate
4. **`sweep.rs`** — Straightforward split
5. **`ai_format.rs`** — Simple helper extraction
6. **`types.rs`** — Small sim-types extraction

## Verification Checklist (per commit)

- [ ] `cargo build` compiles
- [ ] `cargo test` — all tests pass
- [ ] `cargo clippy --all-targets` — no warnings (`-Dwarnings` enforced)
- [ ] `cargo clippy --all-targets --features postgres` — postgres feature clean
- [ ] `cargo fmt --check` — formatting correct
- [ ] No public API changes (re-exports maintain existing import paths)

## Files That Are Fine As-Is

These files are over 200 lines but have good single-responsibility cohesion:

- `src/engine/filters.rs` (685 lines) — filtering logic, focused
- `src/engine/metrics.rs` (595 lines) — performance calculations
- `src/engine/vectorized_sim.rs` (755 lines) — vectorized simulation, self-contained
- `src/signals/momentum.rs` (508 lines), `overlap.rs` (676 lines), `volatility.rs` (1,043 lines) — indicator implementations grouped by category
- `src/strategies/` — already well-factored (~750 lines total across 8 files)
