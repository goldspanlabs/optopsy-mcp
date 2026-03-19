# MCP Tool Roadmap — Finding Edge & Building Strategies

## Current State

The platform has 15 MCP tools, 31 options strategies, 40+ signals, and robust validation infrastructure (walk-forward, permutation tests, parameter sweeps with OOS + multiple comparisons corrections). A broad universe of historical data (options chains + OHLCV) is cached locally. The gaps below focus on what's needed to **find edge** and **manage it at portfolio scale**.

### Data Universe

**OHLCV**: 12,000+ symbols across ETFs (4,302), stocks (7,783), futures (131), and indices (126).

**Options chains to cache** (20 symbols covering all major asset classes):

| Category | Symbols | Why |
|----------|---------|-----|
| **Broad equity indices** | SPY, QQQ, IWM, DIA | Core market exposure, highest liquidity |
| **Sector ETFs** | XLF, XLE, XLK, XLV | Financials, energy, tech, healthcare — different vol profiles and cycle sensitivity |
| **Volatility** | VXX, UVXY | Vol products have unique mean-reversion and term structure dynamics |
| **Commodities** | GLD, SLV, USO | Gold, silver, oil — uncorrelated to equities, commodity-specific seasonality |
| **Bonds / Rates** | TLT, HYG | Long-duration rates and credit spreads — macro regime proxies |
| **Single names** | AAPL, TSLA, NVDA, AMZN, META | Highest options volume, earnings-driven vol, idiosyncratic patterns |

**Macro proxies** (already cached as OHLCV): TLT (rates), HYG (credit), DXY (dollar), GLD (gold), USO (oil). These feed cross-asset signals via the `CrossSymbol` signal variant.

---

## 1. Variance Risk Premium

### 1.1 Realized vs Implied Volatility Spread

Compute the variance risk premium: IV minus realized vol over rolling windows.

- **Parameters**: symbol, iv_window (e.g., 30-day ATM IV), rv_window (e.g., 20-day close-to-close), lookback years
- **Returns**: spread time series, current percentile, mean/median spread, distribution of spread values
- **Why**: The variance risk premium is structural — it exists because hedgers systematically overpay for protection. Measuring it historically tells you how fat the premium is for a given underlying, when it's rich vs thin, and whether backtest profits actually come from this premium or from lucky directional bets.
- **Implementation**: Dual-purpose — expose as a standalone analysis tool *and* as a signal (`rv_iv_spread_above` / `rv_iv_spread_below`) for use in backtest entry/exit filtering.

### Deprioritized (P3 / Nice-to-Have)

The following were considered but deprioritized. They are primarily useful for live trading or real-time decision-making, not for finding edge in historical data:

- **IV Surface (term structure + skew)** — Rich for live trading, but historical surface analysis is diagnostic, not predictive.
- **IV Rank/Percentile History** — Already exists as signals (`iv_rank_above`/`iv_rank_below`). A standalone visualization tool adds little alpha research value.
- **Greeks P&L Attribution** — Tells you *why* a trade made/lost money after the fact. Useful for understanding, but doesn't help find new edge.

---

## 2. Risk & Portfolio Analytics

### 2.1 Portfolio-Level Backtest

Run multiple strategies simultaneously with combined equity tracking.

- Parameters: array of strategy configs (each with its own symbol, signals, sizing), shared capital, allocation weights or independent capital pools
- Returns: combined equity curve, portfolio-level metrics (Sharpe, drawdown, VaR), per-strategy contribution, inter-strategy return correlation matrix
- **Why**: Real trading runs multiple strategies. Correlated drawdowns across isolated backtests can blow up a portfolio even when each strategy looks fine alone.

### 2.2 Tail Risk & Stress Testing

Extend risk metrics beyond VaR 95%.

- **CVaR / Expected Shortfall**: Average loss in the worst 5% of outcomes
- **Historical stress tests**: Replay strategy through named events (e.g., Feb 2018 volmageddon, Mar 2020 COVID crash, Oct 2023 rate shock) by filtering to those date ranges and reporting metrics
- **Max loss scenarios**: Worst N-day returns for the strategy
- **Why**: VaR tells you the threshold; CVaR tells you how bad it gets past that threshold. Options strategies have fat tails — VaR alone is misleading.

### 2.3 Exposure Analysis Over Time

Track net Greeks exposure (delta, gamma, vega, theta) through the life of a backtest.

- Returns: time series of portfolio-level Greek exposures
- **Why**: A strategy can have zero average delta but massive gamma swings. Understanding exposure evolution reveals hidden regime-dependent risk.

---

## 3. Alpha Research & Signal Discovery

### 3.1 Hypothesis Engine (`generate_hypotheses`)

Automated hypothesis generation and testing. Given a target symbol, systematically searches across all computable dimensions for statistically significant patterns, maps survivors to strategies, backtests, and validates — returning only hypotheses that pass rigorous scrutiny.

- **Parameters**: symbol, dimensions (list or "all"), min_significance (p-value threshold, default 0.05), forward_horizons (days, e.g. [5, 10, 20, 45]), min_occurrences (default 30), cross_asset_symbols (optional — defaults to scanning all cached symbols), max_results (default 10)

#### Search Dimensions

**1. Calendar / Seasonality**
- Day-of-week, month-of-year, week-of-month, day-of-month effects
- Turn-of-month (last 2 + first 2 trading days)
- Quarter-end / year-end rebalancing flows
- OpEx week (monthly + quarterly), triple/quad witching
- Pre/post-holiday effects
- *Computable from*: OHLCV date column

**2. Volatility Regime**
- IV rank / IV percentile level and direction
- Realized vs implied vol spread (variance risk premium richness)
- Vol-of-vol (stability of volatility itself)
- Term structure slope (near-term IV vs far-term IV — contango/backwardation)
- Vol regime transitions (compression → expansion, high → low)
- Vol clustering / persistence (current vol predicts future vol)
- *Computable from*: options chain IV + OHLCV for realized vol

**3. Price Action / Momentum**
- Consecutive up/down streaks and reversal rates
- Distance from N-day high/low (proximity to breakout/breakdown)
- Rate of change at multiple horizons (5d, 10d, 20d, 60d)
- Trend strength (ADX level + direction)
- Momentum divergence (price makes new high but RSI/MACD doesn't)
- Moving average alignment (stacked bullish/bearish)
- *Computable from*: OHLCV

**4. Mean-Reversion**
- Bollinger band position (z-score relative to N-day mean)
- RSI extremes at multiple lookbacks
- Distance from key moving averages (10, 20, 50, 200-day)
- Return z-score over rolling window
- Hurst exponent by timescale (confirms whether mean-reversion is even present)
- Variance ratio tests (Lo-MacKinlay) at multiple horizons
- *Computable from*: OHLCV

**5. Volume / Liquidity**
- Volume spikes relative to N-day average (>2x, >3x)
- Volume trend vs price trend divergence (rising price + falling volume = weak)
- Put/call volume ratio (from options chains)
- Options open interest changes (building or unwinding)
- Volume concentration at specific strikes (pinning risk)
- *Computable from*: OHLCV volume + options chain volume/OI

**6. Cross-Asset Lead/Lag**
- Pairwise correlation with all cached symbols
- Granger causality tests (does asset A predict asset B?)
- Optimal lag identification (cross-correlogram)
- Regime-conditional correlation (does the relationship change in high vs low vol?)
- *Computable from*: OHLCV across multiple symbols

**7. Options Market Structure**
- Put/call OI ratio level and changes
- Skew steepness (25-delta put IV vs 25-delta call IV)
- Term structure shape changes
- Gamma exposure estimation (where are dealers long/short gamma?)
- IV surface slope changes (skew flattening/steepening as signal)
- *Computable from*: options chain data (strike, IV, OI, volume by put/call)

**8. Microstructure / Gap**
- Overnight gap size and direction
- Gap fill rate by size bucket
- Opening range breakout/failure rates
- Intraday range relative to N-day average (compression/expansion)
- Time-of-day return patterns (open, midday, close)
- *Computable from*: OHLCV (daily for gaps, intraday for time-of-day)

**9. Autocorrelation Structure**
- Return autocorrelation at lags 1 through N
- Variance ratio at multiple horizons (2, 5, 10, 20 days)
- Hurst exponent (trending vs mean-reverting vs random walk)
- Regime-conditional autocorrelation (trends in one regime, mean-reverts in another)
- *Computable from*: OHLCV

**10. Composite / Convergence**
- Multi-dimensional intersection: find dates where 2+ dimensions fire simultaneously
- Regime-conditional seasonality (does January effect only work in low-vol regimes?)
- Vol-adjusted momentum (momentum signal weighted by current vol regime)
- Cross-asset + seasonality (VIX declining + turn-of-month)
- *Computable from*: combination of all above dimensions

#### What's NOT computable (requires external data)

These dimensions are used by large quant funds but require data sources beyond OHLCV + options chains:
- **Order flow / tape**: L2/L3 market data, trade-by-trade prints
- **Sentiment**: News NLP, social media, earnings call transcripts
- **Fundamentals**: Earnings, revenue, balance sheet ratios, analyst revisions
- **Macro indicators**: Fed funds rate, yield curve shape, credit spreads, PMI (though some can be proxied via cached symbols like TLT, HYG, DXY)
- **Fund flows**: ETF creation/redemption, COT reports, 13F filings

#### Pipeline

1. **Scan**: Run each enabled dimension against the target symbol's history
2. **Filter**: Discard patterns with p > min_significance after Bonferroni correction for total tests run
3. **Rank**: Order by effect size × consistency, not just statistical significance
4. **Converge**: Look for multi-dimensional intersections (patterns that fire on the same dates are stronger)
5. **Map to strategy**: Use pattern type → strategy heuristics (mean-reversion → short premium, momentum → directional spreads, vol expansion → long premium)
6. **Backtest**: Run with conservative defaults (Spread slippage, stop/take-profit, max_positions=3)
7. **Validate**: Walk-forward (4 windows, 70/30 split) + permutation test (500 permutations). Reject if p > 0.05 or >50% Sharpe decay
8. **Return**: Ranked surviving hypotheses with pattern description, strategy, metrics, validation results, confidence level, and deployable SignalSpec

- **Returns**: ranked list of validated hypotheses, each with: dimension, pattern_description, signal_spec (deployable), strategy, metrics (Sharpe, CAGR, max_drawdown, win_rate, trade_count), validation (walk_forward_consistency, permutation_p_value), confidence (HIGH/MEDIUM/LOW), example_dates
- **Guard rails**: Bonferroni + BH-FDR corrections for multiple comparisons (reuse `multiple_comparisons.rs`). Minimum 30 trades. Walk-forward required. No hypothesis survives on significance alone — must also show practical effect size and out-of-sample consistency.

### 3.2 Regime-Conditional Edge Finder

Cross-reference regime detection (already exists via `regime_detect`) with strategy/signal performance to find regime-dependent edge.

- **Parameters**: symbol, strategies (list of strategy configs to test), regime_method (volatility_cluster/trend_state/hmm), n_regimes, forward_horizon
- **Process**:
  1. Run regime detection on the symbol's history to label every date with a regime
  2. Run each strategy/signal backtest, then split trade results by the regime active at entry
  3. Compute per-regime metrics: Sharpe, win rate, avg P&L, trade count, statistical significance
- **Returns**: regime definitions (with date ranges and characteristics), per-strategy per-regime performance matrix, recommendations (e.g., "Short Put works in low-vol regime (Sharpe 1.8, p=0.02) but bleeds in high-vol regime (Sharpe -0.4)"), regime transition alerts (which regime is most recent)
- **Why**: Most strategies don't work in all environments. This tool answers "when does my edge exist and when does it disappear?" by combining two capabilities the platform already has (regime detection + backtesting) that currently require manual cross-referencing.

### 3.3 Cross-Sectional / Relative Value Ranking

Rank symbols across the cached universe by a metric at each historical date, then test whether top/bottom decile selection produces edge.

- **Parameters**: symbol list (or "all cached"), ranking metrics (IV percentile, momentum, mean-reversion score, volatility rank, etc.), rebalance frequency, long/short decile thresholds
- **Returns**: per-rebalance ranked table, long-short portfolio equity curve, spread Sharpe, turnover stats
- **Why**: Most edge comes from *relative* mispricings, not absolute levels. With a full universe of historical data, this becomes a proper cross-sectional backtest — rank at each rebalance date and test whether the ranking predicts forward returns.

### 3.4 Event Study

Analyze returns around known event types.

- Parameters: symbol, event_type (earnings, FOMC, OpEx, VIX spike, custom dates), window (days before/after)
- Returns: average return path, dispersion, hit rate, pre-event vs post-event vol ratio
- **Why**: Options edge clusters around events. Knowing that XYZ averages +2% in the 5 days pre-earnings with 70% hit rate is directly tradeable.

### 3.5 Autocorrelation & Mean-Reversion Detection

Determine whether a symbol trends or mean-reverts at a given timescale.

- **Hurst exponent**: H > 0.5 trends, H < 0.5 mean-reverts, H = 0.5 random walk
- **Variance ratio test**: Lo-MacKinlay test at multiple horizons
- **Autocorrelation function (ACF)**: Lagged return correlations with significance bands
- Parameters: symbol, max_lag, interval
- Returns: Hurst exponent, variance ratios with p-values, ACF plot data
- **Why**: Foundational for strategy selection. Running a mean-reversion strategy on a trending instrument (or vice versa) guarantees losses.

---

## 4. Execution & Timing

### 4.1 Liquidity Analysis

Analyze bid-ask spreads and volume distribution across the options chain.

- Parameters: symbol, expiry range, strike range
- Returns: spread distribution by moneyness, volume heatmap by strike/expiry, illiquidity score
- **Why**: Strategies that look great on paper with mid-price fills but trade 10 contracts/day at 20% wide spreads are not executable.

### 4.2 Intraday Entry/Exit Optimization

Dedicated analysis of optimal intraday timing.

- Parameters: symbol, strategy_type, interval (1m-1h)
- Returns: average return by time-of-day, volume profile, spread profile, optimal entry/exit windows
- **Why**: The first and last 30 minutes of trading behave differently from midday. Timing entries can add 10-30bps per trade.

---

## 5. Benchmarking & Drawdown Analysis

### 5.1 Benchmark Overlay

Overlay strategy equity curve against buy-and-hold or any reference series.

- Parameters: benchmark symbol, strategy backtest results
- Returns: dual equity curves, relative performance (alpha), rolling outperformance, up/down capture ratios
- **Why**: A 15% CAGR strategy that underperforms buy-and-hold SPY in every regime isn't adding value. Context matters.

### 5.2 Drawdown Analysis

Dedicated drawdown decomposition beyond max drawdown.

- Returns: all drawdown periods with start, trough, recovery dates, duration, depth, time-to-recovery, underwater curve
- **Why**: A -20% max drawdown that recovers in 2 weeks is very different from one that takes 18 months. Duration matters as much as depth.

---

## Priority Matrix

| Priority | Tool | Impact | Effort | Rationale |
|----------|------|--------|--------|-----------|
| **P0** | Hypothesis Engine (3.1) | High | High | Scans 10 dimensions, generates + validates strategies autonomously |
| **Done** | Regime-Conditional Edge Finder (3.2) | — | — | Handled via agent system prompt (autonomous workflow in route.ts) |
| **P1** | Cross-Sectional / Relative Value (3.3) | High | Medium | Relative-value strategy construction across universe |
| **P1** | Autocorrelation / Mean-Reversion Detection (3.5) | Medium | Low | Prevents strategy-regime mismatch, cheap to build |
| **P1** | Tail Risk & Stress Testing (2.2) | High | Medium | Historical stress replay, options have fat tails |
| **P1** | Benchmark Overlay (5.1) | Medium | Low | Trivial to build, high interpretive value |
| **P2** | RV vs IV Spread (1.1) | Medium | Medium | Can be approximated with custom signals today |
| **P2** | Portfolio-Level Backtest (2.1) | Medium | High | Less relevant for single-instrument trading |
| **P2** | Event Study (3.4) | Medium | Medium | Event-driven edge detection |
| **P2** | Drawdown Analysis (5.2) | Medium | Low | Duration matters as much as depth |
| **P2** | Exposure Analysis (2.3) | Medium | Medium | Reveals hidden regime-dependent risk |
| **P3** | IV Surface (deprioritized) | Low | Medium | Primarily useful for live trading |
| **P3** | IV Rank History (deprioritized) | Low | Low | Already exists as signals |
| **P3** | Greeks P&L Attribution (deprioritized) | Low | High | Diagnostic, not predictive |
| **P3** | Liquidity Analysis (4.1) | Low | Medium | More relevant for live execution |
| **P3** | Intraday Entry/Exit Optimization (4.2) | Low | Medium | Marginal gains |
