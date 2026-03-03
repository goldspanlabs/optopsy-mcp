# Benchmark: optopsy-mcp (Rust) vs optopsy (Python)

Comparison of the two options backtesting engines using a long call evaluate on SPY historical data.

## Setup

- **Data:** SPY options chain, 3,054,968 rows, cached as Parquet at `~/.optopsy/cache/options/SPY.parquet`
- **Strategy:** Long call (single-leg, buy call)
- **Shared parameters:**
  - `max_entry_dte=45`, `exit_dte=7`
  - `dte_interval=7`, `delta_interval=0.05`
  - `leg_delta: target=0.30, min=0.20, max=0.40`
  - `slippage=mid`
  - `commission=none`
- **Runs:** 1 warm-up + 5 timed iterations
- **Benchmark scripts:** `examples/benchmark.rs` (Rust), `benchmark_optopsy.py` (Python, in project root of optopsy)

## Results

| Metric              | optopsy (Python/Pandas) | optopsy-mcp (Rust/Polars) | Speedup |
|---------------------|-------------------------|---------------------------|---------|
| **Data Load**       | 0.640s                  | 0.054s                    | ~12x    |
| **Avg Evaluate**    | 0.158s                  | 0.038s                    | ~4.2x   |
| **Best Evaluate**   | 0.156s                  | 0.037s                    | ~4.2x   |
| **Worst Evaluate**  | 0.163s                  | 0.039s                    | ~4.2x   |
| **End-to-end**      | ~0.80s                  | ~0.09s                    | ~9x     |
| **Result groups**   | 17                      | 20                        | -       |

## Pipeline Differences

The two engines follow the same high-level flow but diverge in several details. These differences explain the 17 vs 20 group discrepancy and mean the outputs are not directly comparable without alignment.

### 1. Bid/Ask Minimum Filter — ✅ RESOLVED

| | Python | Rust |
|---|---|---|
| **Filter** | `bid > 0.05 AND ask > 0.05` | `bid > min_bid_ask AND ask > min_bid_ask` |
| **Default** | `min_bid_ask=0.05` | `min_bid_ask=0.05` (configurable) |

Both engines now default to filtering out options with bid/ask ≤ 0.05. The Rust engine accepts a `min_bid_ask` parameter to override this threshold.

### 2. P&L Units

| | Python | Rust |
|---|---|---|
| **Metric** | `pct_change = (exit - entry) / abs(entry)` | `pnl = (exit - entry) * qty * multiplier` |
| **Aggregates on** | Percentage return | Dollar P&L |
| **Multiplier applied** | No (raw option price ratio) | Yes (x100 for standard options) |

The aggregated statistics (mean, std, percentiles) are in different units. Python reports fractional returns; Rust reports dollar amounts per contract.

### 3. Exit Matching — Intentionally Different

| | Python | Rust |
|---|---|---|
| **Method** | Exact DTE match | Closest date to target |
| **Logic** | `dte == exit_dte` (with `exit_dte_tolerance=0`) | `argmin(abs(exit_date - (expiration - exit_dte)))` |
| **Fallback** | No match if exact DTE unavailable | Always picks nearest available trading day |

With `exit_dte=7`, Python requires a row where DTE is exactly 7. If no trading data exists on that exact day (weekends, holidays), the trade has no exit and is dropped. Rust finds the closest available trading date to the target, so it matches more trades.

**Status:** Intentionally kept as-is. The fuzzy matching approach is more robust for real-world data with gaps (weekends, holidays). This is the largest remaining contributor to group count differences.

### 4. Delta Selection

| | Python | Rust |
|---|---|---|
| **Group keys** | `(symbol, quote_date, expiration, option_type)` | `(quote_datetime, expiration)` |
| **Tie-breaking** | Sort by strike ascending, pick lowest | No explicit tie-breaking (arbitrary) |

For single-leg call strategies, the grouping is functionally equivalent since option type is pre-filtered. The tie-breaking difference only matters when multiple strikes have identical delta distance to the target.

### 5. DTE Bucketing — ✅ RESOLVED

| | Python | Rust |
|---|---|---|
| **Method** | `pd.cut([0, 7, 14, 21, ...])` | `((dte - 1) / interval) * interval` |
| **Interval type** | Right-closed: `(0, 7]` | Right-closed: `(0, 7]` |
| **DTE=7 assigned to** | `(0, 7]` | `(0, 7]` |
| **DTE=14 assigned to** | `(7, 14]` | `(7, 14]` |

Both engines now use right-closed `(a, b]` intervals for DTE bucketing. Boundary values (multiples of the interval) are correctly assigned to the lower bucket.

### 6. Delta Bucketing — ✅ RESOLVED

| | Python | Rust |
|---|---|---|
| **Input** | Signed `delta_entry` | `abs(delta)` |
| **Method** | `pd.cut([-1.0, -0.95, ..., 0.95, 1.0], step=0.05)` | `floor((abs_delta - ε) / interval) * interval` |
| **Bucket for delta=0.30** | `(0.25, 0.30]` | `(0.25, 0.30]` |

Both engines now use right-closed `(a, b]` intervals for delta bucketing. Boundary values are correctly assigned to the lower bucket.

### 7. Default Parameters — ✅ RESOLVED

These are the out-of-the-box defaults when no parameters are specified. The benchmark scripts explicitly set all parameters to matching values, so these did not affect the benchmark results, but they matter for casual usage.

| Parameter | Python | Rust |
|---|---|---|
| `max_entry_dte` | 90 | Required (no default) |
| `exit_dte` | 0 (hold to expiration) | Required (no default) |
| `dte_interval` | 7 | 7 |
| `delta_interval` | 0.05 | 0.05 |
| `slippage` | `mid` | `Spread` |
| `min_bid_ask` | 0.05 | 0.05 |

## Summary

The Rust engine is ~4x faster on the core evaluate and ~12x faster on data loading. The remaining group count difference comes primarily from:

1. **Exit matching** (difference #3): Rust's fuzzy matching produces more paired trades than Python's exact-DTE requirement. This is intentionally kept as-is since the fuzzy approach is more robust for real-world data with gaps.

Resolved differences:
- **Bid/ask filter** (#1): Both engines now default to `min_bid_ask=0.05`
- **DTE bucketing** (#5): Both engines now use right-closed `(a, b]` intervals
- **Delta bucketing** (#6): Both engines now use right-closed `(a, b]` intervals
- **Default parameters** (#7): `dte_interval` and `delta_interval` defaults now match Python
