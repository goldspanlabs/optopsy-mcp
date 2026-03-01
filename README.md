# optopsy-mcp

Options backtesting engine exposed as an MCP server — strategy screening, simulation, and performance metrics for LLM-driven interaction.

## Features

- **Symmetric Caching Data Layer** — symbol-based data access with local cache and optional S3-compatible fetch-on-miss
- **Statistical Evaluation** — group trades by DTE/delta buckets with aggregate stats (mean, std, win rate, profit factor) for strategy research and screening
- **Backtesting** — full simulation with trade selection, position management, equity curve, and performance metrics (Sharpe, Sortino, Calmar, VaR, max drawdown)
- **32 Built-in Strategies** — singles, verticals, straddles, strangles, butterflies, condors, iron condors/butterflies, calendars, diagonals
- **4 Slippage Models** — mid, spread, liquidity-based, per-leg fixed
- **MCP Interface** — 5 tools accessible via any MCP client (Claude, etc.)

## MCP Tools

| Tool | Description |
|------|-------------|
| `load_data` | Load options data by symbol (auto-fetches from S3 if configured) |
| `list_strategies` | List all 32 available strategies with definitions |
| `evaluate_strategy` | Statistical evaluation with DTE/delta bucket grouping |
| `run_backtest` | Full simulation with trade log, equity curve, and metrics |
| `compare_strategies` | Side-by-side comparison of multiple strategies |

## Quick Start

```bash
# Build
cargo build --release

# Run as MCP server (stdio transport)
cargo run --release
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

## Data Layer

Data is loaded by **symbol** through a caching layer that resolves Parquet files locally and optionally fetches from S3-compatible storage on cache miss.

### Local-only mode (default)

Place Parquet files in the cache directory following the `{cache_dir}/{category}/{SYMBOL}.parquet` convention:

```
~/.optopsy/cache/
  options/
    SPY.parquet
    QQQ.parquet
```

Then load with: `load_data({ symbol: "SPY" })`

### S3 mode

Set environment variables to enable automatic fetch-on-miss from an S3-compatible bucket (AWS, Railway Buckets, MinIO, R2, etc.):

| Env Var | Default | Purpose |
|---------|---------|---------|
| `DATA_ROOT` | `~/.optopsy/cache` | Local cache directory |
| `S3_BUCKET` | _(none)_ | Bucket name — if unset, S3 is disabled |
| `S3_ENDPOINT` | _(none)_ | S3-compatible endpoint URL |
| `AWS_ACCESS_KEY_ID` | _(none)_ | S3 credentials |
| `AWS_SECRET_ACCESS_KEY` | _(none)_ | S3 credentials |

When S3 is configured, files are downloaded to the local cache on first access and served from cache on subsequent calls.

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

1. Load data: `load_data({ symbol: "SPY" })`
2. Browse strategies: `list_strategies()`
3. Screen: `evaluate_strategy({ strategy: "iron_condor", leg_deltas: [...], max_entry_dte: 45, exit_dte: 14, dte_interval: 7, delta_interval: 0.05, slippage: { type: "Mid" } })`
4. Validate: `run_backtest({ strategy: "iron_condor", ..., capital: 100000, quantity: 1, max_positions: 5 })`

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

#### Step 2 — Browse Strategies (`list_strategies`)

```
Client → list_strategies()
  → strategies::all_strategies() → Vec<StrategyDef>
      each StrategyDef: name, category, description
      each LegDef: side (Long/Short), option_type (Call/Put), qty
  → grouped by category (singles, spreads, butterflies, condors,
    iron, calendars)
  → returns StrategiesResponse with suggested next steps
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

This path runs a realistic, capital-constrained, event-driven backtest.

```
Client → run_backtest({ strategy, leg_deltas, max_entry_dte,
                        exit_dte, slippage, commission?,
                        stop_loss?, take_profit?, max_hold_days?,
                        capital, quantity, multiplier?, max_positions,
                        selector? })

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

#### Step 5 — Strategy Comparison (`compare_strategies`)

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

- [Polars](https://pola.rs/) — DataFrame engine
- [rmcp](https://github.com/anthropics/rmcp) — MCP server framework
- [rust-s3](https://crates.io/crates/rust-s3) — S3-compatible object storage
- [rust_ti](https://crates.io/crates/rust_ti) — Technical analysis indicators
- [blackscholes](https://crates.io/crates/blackscholes) — Options pricing

## License

MIT
