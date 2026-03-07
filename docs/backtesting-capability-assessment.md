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
| Spread Yield % | **Missing** | No `option_price / stock_price` filter |
| Spread Price filter | **Missing** | No absolute price filter on spread cost |
| Spread Delta (net position delta) | **Missing** | No net delta entry/exit filter |
| Entry frequency / stagger days | **Missing** | Enters on every qualifying day; no cooldown period |
| Expiration type (weeklies/monthlies) | **Missing** | Only DTE ranges as proxy |
| Exit DTE | **Full** | |
| Stop loss / take profit | **Full** | Percentage of entry cost |
| Exit spread delta | **Missing** | |
| Exit hold days | **Full** | `max_hold_days` |
| Exit strike diff % | **Missing** | |
| Exit leg-specific triggers | **Missing** | |
| Slippage (bid-ask travel %) | **Partial** | 4 models exist, but not the specific `Bid + (Ask-Bid) * slippage%` formula with configurable % |
| Commission modeling | **Full** | Per-contract + base fee + min fee |
| Arithmetic returns | **Yes** | |
| Notional returns | **Missing** | No normalization by underlying price |
| Margin returns | **Missing** | No margin requirement modeling |
| Multiple concurrent positions | **Full** | Up to `max_positions` |
| Immediate re-entry after exit | **Full** | Same-day allowed |

---

## Step 2: Weighted Scoring of Permutations

| Capability | Status | Notes |
|---|---|---|
| Parameter sweep / grid search | **Full** | Cartesian product over strategies, deltas, DTEs, exit DTEs, slippage |
| OOS train/test split | **Full** | Chronological date split with configurable % |
| Dimension sensitivity analysis | **Full** | Average Sharpe/PnL per parameter value |
| Weighted composite scoring | **Missing** | Only ranks by Sharpe; no user-defined weights across metrics |
| Ranking by multiple metrics | **Partial** | Sharpe and PnL rankings exist, but no composite formula |

---

## Step 3: Indicator Optimization (Timing Triggers)

| Capability | Status | Notes |
|---|---|---|
| Signal entry/exit filtering | **Full** | `entry_signal` / `exit_signal` with binary date-level filtering |
| Standard TA indicators | **~40 signals** | RSI, MACD, SMA, EMA, Bollinger, ATR, Stochastic, etc. |
| Signal combinators (AND/OR) | **Full** | Nested logic supported |
| Custom formula signals | **Full** | User-defined expressions |
| Proprietary volatility indicators | **Missing** | No IV rank, IV percentile, term structure, skew, vol surface |
| VIX-based indicators | **Missing** | No cross-symbol signal support |
| Volatility forecasting | **Missing** | No forward-looking vol models |
| Earnings-aware signals | **Missing** | No earnings date data |
| Indicator min/max thresholds | **Partial** | Signals are binary (active/inactive), not threshold-ranged per the reference spec |
| Two-phase search (simulation then confirmation) | **Missing** | No fast simulation approximation before full backtest |
| Automated indicator sweep | **Missing** | Grid search covers strategy params, not signal params |

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

The engine solidly covers Steps 1-2 at a foundational level: 32 strategies, DTE/delta targeting, 4 slippage models, commissions, multiple exit conditions, grid search with OOS validation, and ~40 TA signals wired into entry/exit.

### Major Gaps

1. **Statistical validation (Step 4)** — No p-values, permutation testing, or anti-overfitting framework. The reference doc calls this the "most critical step" and it is entirely absent.

2. **Indicator optimization at scale (Step 3)** — Signals can be used manually, but there is no automated sweep over signal parameters, no two-phase simulation-then-confirmation search, and no way to systematically test hundreds of indicator/threshold combinations.

3. **Volatility surface data** — No IV rank/percentile, term structure, skew, or implied vs. historical vol. This eliminates the ~100 proprietary vol indicators from the reference library.

4. **Missing entry/exit parameters** — Spread yield %, spread delta, stagger days, expiration type filtering, and several exit triggers (leg-level, spread delta, strike diff %).

5. **Weighted composite scoring** — No user-defined weighting across metrics; ranking is Sharpe-only.

6. **Cross-symbol signals** — Cannot use VIX readings as entry triggers for SPY strategies.

### Bottom Line

The backtester is the foundation and works well for running and comparing strategies. The optimization loop (automated indicator search + statistical validation) that the reference document treats as the core differentiator has not been built yet.
