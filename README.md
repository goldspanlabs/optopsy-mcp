# optopsy-mcp

Options backtesting engine exposed as an MCP server — strategy screening, simulation, and performance metrics for LLM-driven interaction.

## 🎯 Recommended Tool Workflow

When connected to Claude via MCP, follow this **7-phase workflow** for optimal results:

| Phase | Tool | Purpose | Duration | Prerequisites |
|-------|------|---------|----------|---------------|
| **0** (opt) | `check_cache_status` | Verify cached data exists before downloading | <1s | — |
| **0b** (opt) | `fetch_to_parquet` | Download OHLCV price data (for signal filtering only) | 5-10s | — |
| **1** ✅ | `load_data` | **START HERE** — Load options chain | 1-30s | — |
| **2a** | `list_strategies` | Browse 32 built-in strategies | <1s | — |
| **2b** | `list_signals` | Browse 40+ TA indicators | <1s | — |
| **2c** (opt) | `construct_signal` | Build signal spec from NLP prompt | <1s | fetch_to_parquet |
| **3** (rec) | `suggest_parameters` | Get data-driven parameter ranges | 1-5s | load_data |
| **4** ✅ | `evaluate_strategy` | Fast statistical screening (DTE×delta buckets) | 2-10s | load_data |
| **5** ✅ | `run_backtest` | Full simulation with metrics | 5-30s | load_data |
| **6** | `compare_strategies` | Compare variations ranked by Sharpe | 10-60s | load_data |

**Key Points**:
- **✅ Required**: load_data, evaluate_strategy, run_backtest
- **Recommended**: suggest_parameters (avoids guessing parameters)
- **Optional**: check_cache_status, fetch_to_parquet, construct_signal, compare_strategies
- Each tool's description tells you exact prerequisites and suggested next steps
- LLMs follow this order automatically based on tool descriptions

## Features

- **Multi-Source Data Integration** — Load options data from EODHD API, local Parquet cache, or S3-compatible storage with fetch-on-miss
- **Statistical Evaluation** — Group trades by DTE/delta buckets with aggregate stats (mean, std, win rate, profit factor) for strategy research and screening
- **Event-Driven Backtesting** — Full simulation with position management, trade log, equity curve, and risk metrics (Sharpe, Sortino, Calmar, VaR, max drawdown)
- **Signal-Based Entry/Exit** — Filter trades using 40+ technical analysis indicators (momentum, trend, volatility, overlap, price, volume)
- **32 Built-in Strategies** — Singles, verticals, straddles, strangles, butterflies, condors, iron condors/butterflies, calendars, diagonals (with multi-expiration support)
- **4 Slippage Models** — Mid, spread, liquidity-based, per-leg fixed
- **Cache Management Tools** — Check cache status, fetch OHLCV data, validate schema
- **9 MCP Tools** — All accessible via Claude Desktop or any MCP-compatible client
- **Parameter Validation** — garde-powered input validation with detailed error feedback
- **HTTP & Stdio Transport** — Deploy locally via stdio or run as HTTP service on cloud platforms

## MCP Tools (10 total)

| Phase | Tool | Description |
|-------|------|-------------|
| 0 | `check_cache_status` | Check if cached parquet data exists (optional, before load_data) |
| 0b | `fetch_to_parquet` | Download OHLCV data from Yahoo Finance (only if using signals) |
| 0 | `download_options_data` | Bulk download options data from EODHD API and cache locally |
| **1** | **`load_data`** | **Load options chain by symbol (START HERE)** |
| 2a | `list_strategies` | Browse all 32 available strategies |
| 2b | `list_signals` | Browse all 40+ available TA signals |
| 2c | `construct_signal` | Build signal spec from natural language (optional) |
| 3 | `suggest_parameters` | Get data-driven parameter recommendations (recommended) |
| **4** | **`evaluate_strategy`** | **Fast statistical screening with DTE/delta buckets (required before backtest)** |
| **5** | **`run_backtest`** | **Full event-driven simulation with metrics (main output)** |
| 6 | `compare_strategies` | Compare multiple strategies side-by-side |

## Quick Start

```bash
# Build
cargo build --release

# Run as MCP server (stdio transport, default)
cargo run --release

# Or run as HTTP service
PORT=8000 cargo run --release
```

### Claude Desktop Configuration

Add to your Claude Desktop config (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "optopsy": {
      "command": "/path/to/optopsy-mcp"
    }
  }
}
```

### Optional Environment Variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `PORT` | Run as HTTP service on this port; if unset, uses stdio | _(unset)_ |
| `EODHD_API_KEY` | Enable EODHD options data downloads | _(unset)_ |
| `DATA_ROOT` | Local cache directory for Parquet files | `~/.optopsy/cache` |
| `S3_BUCKET` | S3 bucket for fallback data fetch | _(unset)_ |
| `S3_ENDPOINT` | S3-compatible endpoint URL | _(unset)_ |
| `AWS_ACCESS_KEY_ID` | S3 credentials | _(unset)_ |
| `AWS_SECRET_ACCESS_KEY` | S3 credentials | _(unset)_ |

## Data Layer

Data is loaded by **symbol** through a caching layer that supports three sources:
1. **Local Parquet cache** — fastest; place files in `~/.optopsy/cache/{category}/{SYMBOL}.parquet`
2. **EODHD API** — automatic download and cache if `EODHD_API_KEY` is set
3. **S3-compatible storage** — fallback for cache miss if configured

Data flows in priority order: Local cache → EODHD (if available) → S3 (if available)

### Data Source Priorities

**1. Local Parquet Cache (fastest)**

Place Parquet files in the cache directory following the `{cache_dir}/{category}/{SYMBOL}.parquet` convention:

```
~/.optopsy/cache/
  options/
    SPY.parquet
    QQQ.parquet
  prices/
    SPY.parquet
```

Then load with: `load_data({ symbol: "SPY" })`

**2. EODHD API Integration (automatic download)**

If `EODHD_API_KEY` is set, `load_data` will automatically:
- Check local cache first
- Download from EODHD if not cached
- Save to local cache for future use

Also supports manual bulk downloads via `download_options_data` tool.

**3. Yahoo Finance OHLCV Data**

Use `fetch_to_parquet` to download historical price data and cache it locally. Required for signal-based entry/exit filtering in backtests.

**4. S3-Compatible Fallback**

For cloud deployments, configure S3 credentials to enable automatic fetch-on-miss from an S3-compatible bucket (AWS S3, Railway Buckets, Cloudflare R2, MinIO, etc.). Files are downloaded to local cache on first access.

### Parquet schema

Expects Parquet files with options chain data containing columns:

| Column | Type | Description |
|--------|------|-------------|
| `quote_date` | Date | Trading date |
| `expiration` | Date | Option expiration date |
| `strike` | Float | Strike price |
| `option_type` | String | `"call"` or `"put"` |
| `bid` | Float | Bid price |
| `ask` | Float | Ask price |
| `delta` | Float | Option delta |
| `symbol` | String | Underlying symbol |

The `quote_date` column is auto-normalized — `quote_date`, `data_date`, and `quote_datetime` are all accepted (Date, Datetime, or String types).

## Example Usage

Once connected via MCP:

**Basic workflow (statistical screening):**
1. Load data: `load_data({ symbol: "SPY" })`
2. Browse strategies: `list_strategies()`
3. Screen: `evaluate_strategy({ strategy: "iron_condor", leg_deltas: [...], max_entry_dte: 45, exit_dte: 14, dte_interval: 7, delta_interval: 0.05, slippage: { type: "Mid" } })`
4. Validate: `run_backtest({ strategy: "iron_condor", ..., capital: 100000, quantity: 1, max_positions: 5 })`

**Advanced workflow (with signals):**
1. `fetch_to_parquet({ symbol: "SPY", category: "prices" })` — Get OHLCV data for signals
2. `list_signals()` — Browse available TA indicators
3. `run_backtest({ strategy: "iron_condor", ..., entry_signal: "rsi_oversold", exit_signal: "rsi_overbought" })`

**Data management:**
- `check_cache_status({ symbol: "SPY", category: "options" })` — Check if data is cached
- `download_options_data({ symbol: "SPY" })` — Bulk download from EODHD
- `compare_strategies({ strategies: [...], sim_params: {...} })` — Compare multiple strategies

## Architecture & Data Flow

This section explains exactly how data moves through the system during a strategy exploration session.

### System Layers

```
┌──────────────────────────────────────────────────────────────┐
│               MCP Client (Claude Desktop, etc.)              │
│          sends JSON-RPC tool calls via stdio or HTTP         │
└───────────────────────────┬──────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────┐
│                  OptopsyServer  (server.rs)                  │
│   routes tool calls · holds shared DataFrame in RwLock       │
└──────┬──────────┬────────────────┬───────────────┬───────────┘
       │          │                │               │
  load_data  list_strategies  evaluate_strategy  run_backtest /
  (tools/)    (tools/)          (tools/)         compare_strategies
       │                          │               (tools/)
       ▼                          └───────┬───────┘
┌─────────────┐                          ▼
│  data/      │               ┌──────────────────────────────┐
│  cache.rs   │               │       engine/core.rs         │
│  parquet.rs │               │  orchestrates the pipeline   │
└──────┬──────┘               └──┬───────────────────────────┘
       │                         │
  local Parquet            ┌─────┴────────────────────────────┐
  S3 fetch-on-miss         │  strategies/  find_strategy()    │
                           │  engine/filters.rs               │
                           │  engine/evaluation.rs            │
                           │  engine/event_sim.rs             │
                           │  engine/pricing.rs               │
                           │  engine/metrics.rs               │
                           └─────┬────────────────────────────┘
                                 │
                                 ▼
                        tools/ai_format.rs
                    (enriches result with summary,
                     key findings & suggested next steps)
                                 │
                                 ▼
                       JSON response → MCP client
```

### Step-by-Step: Strategy Exploration Session

#### Step 1 — Load Data (`load_data`)

```
Client → load_data({ symbol: "SPY", start_date?, end_date? })
  → CachedStore.load_options("SPY")
      → check ~/.optopsy/cache/options/SPY.parquet
      → if missing and S3 configured: download & cache locally
  → parquet.rs reads Parquet and normalises the date column
      (accepts quote_date / data_date / quote_datetime as Date,
       Datetime, or String — all normalised to quote_datetime)
  → optional date-range filter applied
  → resulting DataFrame stored in server's shared Arc<RwLock<Option<DataFrame>>>
  → returns LoadDataResponse: row count, symbols, date range,
    column list, suggested next steps
```

#### Step 2a — Browse Strategies (`list_strategies`)

```
Client → list_strategies()
  → strategies::all_strategies() → Vec<StrategyDef>
      each StrategyDef: name, category, description, legs (multi-expiration support)
      each LegDef: side (Long/Short), option_type (Call/Put), qty, delta target
  → grouped by category (singles, spreads, butterflies, condors,
    iron, calendars, diagonals)
  → returns StrategiesResponse with suggested next steps
```

#### Step 2b — Browse Signals (`list_signals`)

```
Client → list_signals()
  → signals::all_signals() → Vec<SignalDef>
      each SignalDef: name, category, description, params
      categories: momentum (RSI, MACD, Stoch), trend (SMA, EMA, ADX),
                  volatility (BBands, ATR), overlap, price, volume
  → returns SignalsResponse with all available indicators
```

#### Step 3 — Statistical Screen (`evaluate_strategy`)

This path evaluates *historical* P&L across DTE × delta buckets — fast and data-driven, no capital simulation involved.

```
Client → evaluate_strategy({ strategy, leg_deltas, max_entry_dte,
                              exit_dte, dte_interval, delta_interval,
                              slippage, commission? })

engine/core::evaluate_strategy(df, params):

  1. strategies::find_strategy(name) → StrategyDef

  2. Per leg (repeated for every leg in the strategy):
       a. filters::filter_option_type(df, "call"|"put")
            → keep only rows matching this leg's option type
       b. filters::compute_dte(df)
            → add dte = expiration − quote_datetime (integer days)
       c. filters::filter_dte_range(df, max_entry_dte, exit_dte)
            → keep rows with exit_dte ≤ dte ≤ max_entry_dte
       d. filters::filter_valid_quotes(df)
            → drop rows with zero bid or ask
       e. filters::select_closest_delta(df, target)
            → group by (quote_datetime, expiration)
            → pick the strike whose |delta| is closest to target,
              within [target.min, target.max]
       f. evaluation::match_entry_exit(entries, all_data, exit_dte)
            → for each entry row, find the exit row with the same
              (expiration, strike, option_type) whose quote_datetime
              is closest to (expiration − exit_dte)
            → returns joined DataFrame with entry & exit prices

  3. Join all leg DataFrames on (quote_datetime, expiration)
       → one row per trade opportunity that has all legs filled

  4. rules::filter_strike_order(df, num_legs, strict)
       → enforce ascending strike order across legs
         (skipped for straddles / iron butterflies)

  5. pricing::leg_pnl(...) per row, per leg
       → entry_price = mid | ask | liquidity-adjusted | fixed-per-leg
         (based on chosen Slippage model)
       → exit_price  = mid | bid | liquidity-adjusted | fixed-per-leg
       → pnl = (exit_price − entry_price) × side × qty × multiplier
       → commission subtracted (entry + exit)

  6. output::bin_and_aggregate(df, dte_interval, delta_interval)
       → create DTE buckets  e.g. [30,37), [37,44) …
       → create delta buckets e.g. [0.15,0.20), [0.20,0.25) …
       → per bucket: mean, std, min, q25, median, q75, max,
         win_rate, profit_factor, count

  → ai_format::format_evaluate()
       → identify best/worst bucket, highest win-rate bucket
       → generate natural-language summary & suggested next steps
  → returns EvaluateResponse with Vec<GroupStats>
```

#### Step 4 — Full Simulation (`run_backtest`)

This path runs a realistic, capital-constrained, event-driven backtest with optional signal-based filtering.

```
Client → run_backtest({ strategy, leg_deltas, max_entry_dte,
                        exit_dte, slippage, commission?,
                        stop_loss?, take_profit?, max_hold_days?,
                        capital, quantity, multiplier?, max_positions,
                        selector?, entry_signal?, exit_signal?,
                        ohlcv_path? })

engine/core::run_backtest(df, params):

  1. strategies::find_strategy(name) → StrategyDef

  2. event_sim::build_price_table(df)
       → iterates every row of the DataFrame once
       → builds HashMap<(date, expiration, strike, OptionType),
                         QuoteSnapshot{bid, ask, delta}>
       → also collects sorted Vec<NaiveDate> of all trading days

  3. event_sim::find_entry_candidates(df, strategy_def, params)
       → applies the same per-leg filter chain as evaluate_strategy
         (filter_option_type → compute_dte → filter_dte_range →
          filter_valid_quotes → select_closest_delta)
       → joins legs, enforces strike order, computes net_premium
       → returns Vec<EntryCandidate> (one per entry date × expiration)

  3b. signals::apply_signal_filter(candidates, entry_signal, ohlcv_path)
       → if entry_signal specified: load OHLCV data, compute TA indicators
       → filter candidates to only those where entry signal triggers on entry_date
       → optional: apply exit_signal to pre-filter positions for early exit logic

  4. event_sim::run_event_loop(price_table, candidates,
                               trading_days, params, strategy_def)
       → iterates day-by-day over trading_days:

         OPEN PHASE:
           • find candidates with entry_date == today
           • skip if positions ≥ max_positions
           • apply TradeSelector (Nearest DTE, HighestPremium,
             LowestPremium, or First)
           • create Position from EntryCandidate; charge entry cost

         CLOSE CHECK (for every open position):
           • look up today's price in PriceTable for each leg
           • compute current_value = Σ leg current prices × side × qty
           • check exit conditions in priority order:
               – DTE exit:    dte ≤ exit_dte       → ExitType::DteExit
               – Stop loss:   loss > stop_loss × |entry_cost|
                                                    → ExitType::StopLoss
               – Take profit: gain > take_profit × |entry_cost|
                                                    → ExitType::TakeProfit
               – Max hold:    days_held ≥ max_hold_days
                                                    → ExitType::MaxHold
               – Expiration:  today ≥ expiration   → ExitType::Expiration

         EQUITY UPDATE (every day):
           • realized_pnl = sum of all closed trades
           • unrealized_pnl = Σ (current_value − entry_cost) for open positions
           • equity = capital + realized_pnl + unrealized_pnl
           • appended to equity_curve as EquityPoint{datetime, equity}

       → returns (Vec<TradeRecord>, Vec<EquityPoint>)

  5. metrics::calculate_metrics(equity_curve, trade_log, capital)
       → daily returns series from equity_curve
       → Sharpe ratio  (annualised, rf=0)
       → Sortino ratio (downside deviation only)
       → max drawdown  (peak-to-trough)
       → Calmar ratio  (CAGR / max drawdown)
       → VaR 95%       (5th percentile of daily returns)
       → CAGR          (compound annual growth rate)
       → win rate, profit factor
       → avg P&L, avg winner, avg loser, avg days held
       → max consecutive losses, expectancy

  → ai_format::format_backtest()
       → trade summary (exit breakdown, best/worst trade)
       → equity curve summary (start/end equity, peak, trough)
       → sampled equity curve (≤50 points for compact transmission)
       → natural-language assessment of Sharpe quality
       → key findings & suggested next steps
  → returns BacktestResponse
```

#### Step 6 — Strategy Comparison (`compare_strategies`)

```
Client → compare_strategies({ strategies: [CompareEntry, ...],
                               sim_params })
  → for each CompareEntry:
       → assembles BacktestParams (entry params + shared sim_params)
       → calls run_backtest() (full pipeline above)
       → collects CompareResult: strategy, trades, pnl, sharpe,
         sortino, max_dd, win_rate, profit_factor, calmar,
         total_return_pct
  → ai_format::format_compare()
       → ranks strategies by Sharpe, then by total PnL
       → identifies overall best performer
       → returns CompareResponse with suggested next steps
```

### Key Data Structures

| Structure | Where defined | Role |
|-----------|---------------|------|
| `DataFrame` (Polars) | `data/` | Raw options chain — column-oriented, immutable once loaded |
| `StrategyDef` | `engine/types.rs` | Blueprint: name, category, legs, strike ordering flag |
| `LegDef` | `engine/types.rs` | Per-leg config: side, option_type, delta target, qty |
| `EntryCandidate` | `engine/types.rs` | Fully-matched option combo ready to open as a position |
| `PriceTable` | `engine/types.rs` | `HashMap<(date, exp, strike, type) → QuoteSnapshot>` for O(1) daily lookup |
| `Position` | `engine/types.rs` | Live position: legs, entry cost, status, quantity |
| `TradeRecord` | `engine/types.rs` | Closed trade: entry/exit datetime, P&L, days held, exit reason |
| `EquityPoint` | `engine/types.rs` | Daily equity snapshot (realized + unrealized) |
| `GroupStats` | `engine/types.rs` | Aggregate stats for one DTE × delta bucket |
| `PerformanceMetrics` | `engine/types.rs` | Portfolio-level risk/return metrics |

## Tech Stack

- [Polars](https://pola.rs/) — DataFrame engine for data processing
- [rmcp](https://github.com/anthropics/rmcp) — MCP server framework (v0.17)
- [Tokio](https://tokio.rs/) — Async runtime for concurrent operations
- [Axum](https://github.com/tokio-rs/axum) — HTTP server (optional, via PORT env var)
- [rust-s3](https://crates.io/crates/rust-s3) — S3-compatible object storage
- [rust_ti](https://crates.io/crates/rust_ti) — Technical analysis indicators (40+ signals)
- [blackscholes](https://crates.io/crates/blackscholes) — Options pricing models
- [garde](https://crates.io/crates/garde) — Input validation framework
- [serde + serde_json](https://serde.rs/) — JSON serialization
- [schemars](https://docs.rs/schemars/) — JSON Schema generation for MCP tools

## License

MIT
