# Tech Debt Audit: Clippy Suppressions & Long Functions

**Date:** 2026-03-16

## Overview

67 `#[allow(clippy::...)]` suppressions across 23 files. 29 suppress `too_many_lines`, 20 suppress `too_many_arguments`. The rest are minor style lints.

## Suppression Breakdown

| Suppression | Count | Concern |
|---|---|---|
| `too_many_lines` | 29 | Monolithic functions |
| `too_many_arguments` | 20 | Missing abstractions |
| `implicit_hasher` | 5 | Minor API style |
| `wildcard_imports` | 5 | Namespace pollution |
| `ref_option` | 4 | Non-idiomatic signatures |
| `unnecessary_wraps` | 2 | Forced Result types |
| `items_after_statements` | 2 | In-function struct defs |
| `trivially_copy_pass_by_ref` | 2 | Minor perf |
| `similar_names` | 2 | Variable naming |
| `let_and_return` | 1 | Style |

## Long Functions (Production Code, Sorted by Line Count)

| Lines | File | Function | What it does |
|---|---|---|---|
| **1,300** | `signals/custom.rs:662` | `build_function_call` | Giant match on 50+ function names to Polars exprs |
| **367** | `engine/sweep.rs:523` | `run_stock_sweep` | Stock parameter sweep orchestration |
| **277** | `engine/sweep.rs:38` | `run_sweep` | Options parameter sweep orchestration |
| **253** | `tools/construct_signal/examples.rs:14` | `build_example` | Build JSON example per signal name |
| **239** | `tools/rolling_metric.rs:12` | `execute` | Rolling metric computation |
| **232** | `tools/correlate.rs:14` | `execute` | Correlation analysis |
| **227** | `tools/build_signal.rs:400` | `formula_help` | Static help text construction |
| **204** | `engine/event_sim.rs:31` | `find_entry_candidates` | Filter options chain for entry |
| **188** | `engine/stock_sim.rs:556` | `resample_datetime` | OHLCV resampling |
| **187** | `engine/sweep_analysis.rs:477` | `compute_stability` | Parameter stability scores |
| **179** | `engine/stock_sim.rs:80` | `run_stock_backtest` | Stock backtest event loop |
| **169** | `tools/raw_prices.rs:19` | `execute` | Price bar extraction |
| **169** | `tools/regime_detect.rs:12` | `execute` | Market regime detection |
| **162** | `tools/ai_format/backtest.rs:372` | `format_stock_backtest` | AI response formatting |
| **160** | `engine/adjustments.rs:73` | `execute_adjustment` | Position adjustment actions |
| **159** | `signals/indicators.rs:125` | `dispatch_indicator_call` | Dispatch to indicator compute fn |
| **157** | `engine/stock_sim.rs:747` | `resample_date` | Legacy date-based resampling |
| **147** | `tools/ai_format/advanced.rs:22` | `format_sweep` | Sweep AI formatting |
| **145** | `tools/distribution.rs:13` | `execute` | Distribution histogram analysis |

## Too Many Arguments (Sorted by Arg Count)

| Args | File | Function | Suggested fix |
|---|---|---|---|
| **17** | `tools/ai_format/stats.rs:139` | `format_distribution` | `DistributionStats` struct |
| **10** | `tools/ai_format/stats.rs:256` | `format_correlate` | `CorrelationResult` struct |
| **9** | `engine/adjustments.rs:73` | `execute_adjustment` | `AdjustmentContext` struct |
| **9** | `engine/types.rs:860` | `TradeRecord::new` | Builder pattern |
| **8** | `engine/sweep.rs:325` | `build_backtest_params_for_combo` | Already has `SimParams` — partial overlap |
| **8** | `engine/pricing.rs:41` | `leg_pnl` | `LegQuote` + `FillParams` structs |
| **8** | `engine/event_sim.rs:238` | `build_trade_record` | `ClosedPosition` struct |
| **8** | `engine/positions.rs:87` | `close_position` | `CloseContext` struct |
| **8** | `engine/positions.rs:300` | `lookup_fill_price` | `LookupKey` struct |
| **8** | `engine/adjustments.rs:236` | `finalize_if_all_closed` | Shares context with `execute_adjustment` |
| **8** | `engine/adjustments.rs:300` | `check_and_apply_adjustments` | Same context pattern |
| **8** | `tools/walk_forward.rs:46` | `execute_stock` | `WalkForwardConfig` struct |
| **8** | `tools/ai_format/stats.rs:394` | `format_regime_detect` | `RegimeResult` struct |

## Priority Refactoring Targets

### 1. `build_function_call` — 1,300 lines (CRITICAL)

`signals/custom.rs:662` — A single match statement mapping 50+ function names to Polars expressions. Refactor into a dispatch table (`HashMap<&str, fn(...)>`) or split by category (math, moving averages, volatility, etc.) which already matches the signal module structure.

### 2. Simulation context passing — 8 functions, 7-9 args each (HIGH)

`positions.rs`, `adjustments.rs`, and `event_sim.rs` all pass around the same cluster: `price_table`, `last_known`, `slippage`, `params`, `trade_log`, `trade_id`, `realized_equity`. Introduce a `SimContext` struct to bundle shared simulation state.

**Affected functions:**
- `open_position` (7 args)
- `close_position` (8 args)
- `mark_to_market` (7 args)
- `lookup_fill_price` (8 args)
- `execute_adjustment` (9 args)
- `finalize_if_all_closed` (8 args)
- `check_and_apply_adjustments` (8 args)
- `build_trade_record` (8 args)

### 3. `ai_format/stats.rs` — 5 functions, 7-17 args each (MEDIUM)

Pure formatting functions that take every stat as a separate argument. Each should accept a result struct instead. `format_distribution` with 17 args is the worst.

## Not Worth Refactoring

- **Tool `execute` functions** (rolling_metric, correlate, regime_detect at 140-240 lines) — linear pipelines (load → compute → format). Splitting adds indirection without testability gain.
- **`formula_help`** (227 lines) — static string construction. Long but harmless.
- **`implicit_hasher`**, **`wildcard_imports`**, **`ref_option`** — style nits, not debt.
