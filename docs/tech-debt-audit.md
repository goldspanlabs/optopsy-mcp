# Tech Debt Audit: Clippy Suppressions & Long Functions

**Date:** 2026-03-16
**Last updated:** 2026-03-16

## Overview

Originally 67 `#[allow(clippy::...)]` suppressions across 23 files. Three priority refactoring targets were identified and completed, removing 15 suppressions and splitting a 1,300-line function into 10 modules.

## Completed Refactoring

### 1. `build_function_call` split into `custom_funcs/` modules (CRITICAL) — DONE

The 1,300-line match statement in `signals/custom.rs:662` was split into `src/signals/custom_funcs/` with 10 category modules:

| Module | Functions | Lines |
|---|---|---|
| `helpers.rs` | `FuncArg`, `extract_*`, `compute_rolling_rank`, `compute_iv_rank` | 302 |
| `rolling.rs` | sma, ema, std, max, min, bbands_mid/upper/lower | 90 |
| `math.rs` | abs, change, pct_change, roc, rel_volume, zscore, range_pct, if | 75 |
| `single_col.rs` | rsi, macd_hist/signal/line, rank, iv_rank, cci, ppo, cmo | 207 |
| `multi_col.rs` | atr, stochastic, keltner_upper/lower, obv, mfi, tr, cmf | 340 |
| `momentum_trend.rs` | williams_r, adx/plus_di/minus_di, psar, tsi, vpt | 211 |
| `volatility_adv.rs` | donchian, ichimoku, envelope, supertrend, aroon, ad, pvi, nvi, ulcer | 372 |
| `stateful.rs` | consecutive_up, consecutive_down | 53 |
| `datetime.rs` | day_of_week, month, day_of_month, hour, minute, week_of_year | 49 |
| `mod.rs` | `dispatch()` router | 65 |

`build_function_call` is now 3 lines delegating to `custom_funcs::dispatch()`. Removed `#[allow(clippy::too_many_lines)]`.

### 2. `SimContext`/`SimState` structs for event simulation (HIGH) — DONE

Added to `engine/sim_types.rs`:
- `SimContext<'a>` — immutable context: `price_table`, `params`, `strategy_def`, `ohlcv_closes`
- `SimState` — mutable accumulators: `trade_log`, `trade_id`, `realized_equity`
- `LastKnown` — type alias for the last-known price cache

Refactored 12 functions across 3 files:

| Function | Before | After | Suppressions removed |
|---|---|---|---|
| `open_position` | 7 args | 5 args | `too_many_arguments` |
| `close_position` | 8 args | 5 args | `too_many_arguments` |
| `mark_to_market` | 7 args | 4 args | `too_many_arguments`, `implicit_hasher` |
| `lookup_fill_price` | 8 args | 7 args | `too_many_arguments` |
| `trigger_fires` | 7 args | 5 args | `too_many_arguments` |
| `execute_adjustment` | 9 args | 6 args | `too_many_arguments` |
| `finalize_if_all_closed` | 8 args | 5 args | `too_many_arguments` |
| `check_and_apply_adjustments` | 8 args | 5 args | `too_many_arguments` |
| `run_event_loop` | 8 args | 5 args | `too_many_arguments`, `implicit_hasher` |
| `check_exit_triggers` | 6 args | 4 args | — |
| `compute_unrealized_pnl` | 7 args | 4 args | — |
| `compute_position_net_delta` | 4 args | 4 args | (uses `SimContext`/`LastKnown` now) |

### 3. Data structs for `ai_format/stats.rs` format functions (MEDIUM) — DONE

Added 3 data structs to bundle positional arguments:

| Struct | Fields | Replaces args in |
|---|---|---|
| `DistributionData` | 17 | `format_distribution` (was 17 args) |
| `CorrelateData` | 10 | `format_correlate` (was 10 args) |
| `RegimeDetectData` | 8 | `format_regime_detect` (was 8 args) |

Updated 3 call sites (`distribution.rs`, `correlate.rs`, `regime_detect.rs`). Also removed the unnecessary `#[allow(clippy::too_many_arguments)]` from `format_rolling_metric` (only 6 args).

## Remaining Suppressions

After refactoring, the remaining suppressions are intentional or not worth fixing:

| Suppression | Count | Status |
|---|---|---|
| `too_many_lines` | ~25 | Linear pipelines (tool execute fns, sweep orchestration) — splitting adds indirection without benefit |
| `too_many_arguments` | ~6 | `TradeRecord::new` (9 args, constructor), `build_trade_record` (8 args, all per-trade values), `build_backtest_params_for_combo`, `leg_pnl`, `execute_stock` |
| `wildcard_imports` | 5 | `use super::types::*` in engine modules — deliberate for ergonomics |
| `implicit_hasher` | ~3 | Minor API style |
| `ref_option` | 4 | Non-idiomatic but harmless |
| Other style lints | ~8 | `similar_names`, `unnecessary_wraps`, `items_after_statements`, `let_and_return` |

## Not Worth Refactoring

- **Tool `execute` functions** (rolling_metric, correlate, regime_detect at 140-240 lines) — linear pipelines (load → compute → format). Splitting adds indirection without testability gain.
- **`formula_help`** (227 lines) — static string construction. Long but harmless.
- **`TradeRecord::new`** (9 args) — constructor with all distinct fields, no natural grouping.
- **`implicit_hasher`**, **`wildcard_imports`**, **`ref_option`** — style nits, not debt.
