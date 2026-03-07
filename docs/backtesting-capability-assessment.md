# Backtesting Capability Assessment

Gap analysis of optopsy-mcp against the Options Strategy Optimization reference workflow.

**Date:** 2026-03-07

---

## Step 1: Base Strategy Backtesting

| Capability | Status | Notes |
|---|---|---|
| Strategy types (25+) | **32 strategies** | Singles, spreads, butterflies, condors, iron, calendar |
| DTE targeting with ranges | **Full** | `DteRange { target, min, max }` |
| Strike delta targeting | **Full** | Per-leg `TargetRange` with closest-match selection |
| Spread Yield % | **Not planned** | Derivable from existing premium + OHLCV data |
| Spread Price filter | **Full** | `min_net_premium` / `max_net_premium` filter on `abs(net_premium)` at entry |
| Spread Delta (net position delta) | **Full** | `min_net_delta` / `max_net_delta` entry filter + `exit_net_delta` exit trigger |
| Entry frequency / stagger days | **Full** | `min_days_between_entries` cooldown between position opens |
| Expiration type (weeklies/monthlies) | **Full** | `expiration_filter`: `Any` / `Weekly` (Fridays) / `Monthly` (third Friday) |
| Exit DTE | **Full** | |
| Stop loss / take profit | **Full** | Percentage of entry cost |
| Exit spread delta | **Full** | `exit_net_delta` — exits when `|net_delta|` exceeds threshold (`ExitType::DeltaExit`) |
| Exit hold days | **Full** | `max_hold_days` |
| Exit strike diff % | **Not planned** | Vaguely defined; not a standard options exit trigger |
| Exit leg-specific triggers | **Not planned** | Would require restructuring position management to exit individual legs |
| Slippage (bid-ask travel %) | **Full** | 5 models: `Mid`, `Spread`, `Liquidity`, `PerLeg`, `BidAskTravel { pct }` — `bid + (ask−bid) × pct` |
| Commission modeling | **Full** | Per-contract + base fee + min fee |
| Arithmetic returns | **Yes** | |
| Notional returns | **Not planned** | Derivable from existing P&L + OHLCV data |
| Margin returns | **Not planned** | Margin requirements are broker-specific (Reg-T vs portfolio margin); any approximation would be misleading |
| Multiple concurrent positions | **Full** | Up to `max_positions` |
| Immediate re-entry after exit | **Full** | Same-day allowed |

---

## Step 2: Weighted Scoring of Permutations

| Capability | Status | Notes |
|---|---|---|
| Parameter sweep / grid search | **Full** | Cartesian product over strategies, deltas, DTEs, exit DTEs, slippage |
| OOS train/test split | **Full** | Chronological date split with configurable % |
| Dimension sensitivity analysis | **Full** | Average Sharpe/PnL per parameter value |
| Weighted composite scoring | **Not planned** | Users can evaluate individual metrics from existing output |
| Ranking by multiple metrics | **Not planned** | Sharpe and PnL rankings exist; composite scoring adds false objectivity |

---

## Step 3: Indicator Optimization (Timing Triggers)

| Capability | Status | Notes |
|---|---|---|
| Signal entry/exit filtering | **Full** | `entry_signal` / `exit_signal` with binary date-level filtering |
| Standard TA indicators | **~40 signals** | RSI, MACD, SMA, EMA, Bollinger, ATR, Stochastic, etc. |
| Signal combinators (AND/OR) | **Full** | Nested logic supported |
| Custom formula signals | **Full** | User-defined expressions |
| Proprietary volatility indicators | **Partial** | IV rank and IV percentile signals added (#60); term structure, skew, vol surface not planned — require IV surface fitting infrastructure not in current data |
| VIX-based indicators | **Full** | Cross-symbol signal support (#59) — e.g., VIX readings as entry filter for SPY strategies |
| Volatility forecasting | **Not planned** | Forecasting/prediction, not backtesting; IV rank/percentile cover realized vol signals without speculative model risk |
| Earnings-aware signals | **Not planned** | Requires external earnings calendar data source not available in current options data |
| Indicator min/max thresholds | **Full** | Directional threshold signals with range catalog (#58) — upper/lower bounds per indicator |
| Two-phase search (simulation then confirmation) | **Not planned** | Signal parameter sweep (#61) runs full backtests directly; a fast approximation phase risks discarding good candidates |
| Automated indicator sweep | **Full** | Signal parameter sweep added to `parameter_sweep` tool (#61) — grid search over signal params alongside strategy params |

---

## Step 4: Statistical Validation

| Capability | Status | Notes |
|---|---|---|
| P-value calculation | **Missing** | Not implemented |
| Permutation testing | **Missing** | Not implemented |
| Multiple comparisons correction | **Missing** | |
| Out-of-sample validation | **Full** | Train/test date split |
| Walk-forward analysis | **Missing** | |
| Parameter stability analysis | **Missing** | No neighboring-parameter robustness check |
| Performance metrics | **Full** | Sharpe, Sortino, CAGR, VaR, max drawdown, Calmar, win rate, profit factor, expectancy |

---

## Steps 5-6: Paper Trading & Live Deployment

Out of scope for a backtesting engine. Not assessed.

---

## Data & Greeks

| Capability | Status | Notes |
|---|---|---|
| Historical bid/ask data | **Full** | Via EODHD + Parquet cache |
| OHLCV underlying data | **Full** | Via Yahoo Finance fetch |
| Delta from data | **Full** | Read from source |
| Full Greeks (gamma, theta, vega, rho) | **Missing** | Only delta |
| Smooth Market Volatility (SMV) | **Missing** | No IV surface fitting |
| Theoretical edge calculation | **Missing** | No theo pricing vs market comparison |

---

## Summary

The engine solidly covers Steps 1-2 and most of Step 3: 32 strategies, DTE/delta targeting, 5 slippage models, commissions, multiple exit conditions (DTE, SL/TP, max hold, delta), entry filters (premium, delta, expiration type, stagger), grid search with OOS validation, ~40 TA signals wired into entry/exit, cross-symbol signals (VIX filtering), IV rank/percentile indicators, directional threshold ranges, and automated signal parameter sweeps.

### Major Gaps

1. **Statistical validation (Step 4)** — No p-values, permutation testing, or anti-overfitting framework. The reference doc calls this the "most critical step" and it is entirely absent.

### Bottom Line

The backtester covers Steps 1-2 solidly and Step 3 is now substantially addressed: cross-symbol signals (#59), IV rank/percentile (#60), directional thresholds (#58), and automated signal sweeps (#61) close the major indicator optimization gaps. The remaining frontier is statistical validation (Step 4) and deeper volatility surface data.
