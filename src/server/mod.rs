//! MCP server implementation for optopsy.
//!
//! Holds shared state (loaded `DataFrames`, data cache, tool router) and exposes
//! all MCP tool handlers via `rmcp`'s `#[tool_router]` and `#[tool_handler]` macros.

mod params;
mod sanitize;

use garde::Validate;
use polars::prelude::*;
use rmcp::{
    handler::server::router::tool::ToolRouter,
    model::{Implementation, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router, ServerHandler,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::data::cache::{validate_path_segment, CachedStore};
use crate::data::DataStore;
use crate::engine::types::{
    BacktestParams, CompareEntry, CompareParams, SimParams, EPOCH_DAYS_CE_OFFSET,
};
use crate::signals::registry::{collect_cross_symbols, SignalSpec};
use crate::tools;
use crate::tools::response_types::{
    BacktestResponse, BuildSignalResponse, CheckCacheResponse, CompareResponse,
    PermutationTestResponse, RawPricesResponse, StatusResponse, StockBacktestResponse,
    StrategiesResponse, SweepResponse, WalkForwardResponse,
};
use params::{
    resolve_leg_deltas, resolve_sweep_strategies, validate_category_read, validation_err,
    BacktestBaseParams, BuildSignalParams, CheckCacheParams, CompareStrategiesParams,
    GetRawPricesParams, ParameterSweepParams, PermutationTestParams, RunBacktestParams,
    RunStockBacktestParams, WalkForwardParams,
};
use sanitize::{SanitizedJson, SanitizedResult};

/// Loaded data: `HashMap<Symbol, DataFrame>` for multi-symbol support.
type LoadedData = HashMap<String, DataFrame>;

/// MCP server for options backtesting, holding loaded data and the tool router.
#[derive(Clone)]
pub struct OptopsyServer {
    /// Multi-symbol in-memory data storage, keyed by uppercase ticker.
    pub data: Arc<RwLock<LoadedData>>,
    /// Shared data layer for local Parquet cache and optional S3 backend.
    pub cache: Arc<CachedStore>,
    tool_router: ToolRouter<Self>,
}

impl OptopsyServer {
    /// Create a new server instance with the given data cache.
    pub fn new(cache: Arc<CachedStore>) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            cache,
            tool_router: Self::tool_router(),
        }
    }

    /// Ensure options data is loaded for a symbol, auto-loading from cache if needed.
    /// Returns `(symbol, DataFrame)`.
    async fn ensure_data_loaded(
        &self,
        symbol: Option<&str>,
    ) -> Result<(String, DataFrame), String> {
        // Fast path: try a read lock first to avoid serializing all requests when data
        // is already loaded. This covers the common case of concurrent reads.
        {
            let data = self.data.read().await;
            if !data.is_empty() {
                return match Self::resolve_symbol(&data, symbol) {
                    Ok((sym, df)) => Ok((sym.clone(), df.clone())),
                    Err(e) => Err(format!("Error: {e}")),
                };
            }
        }

        // Auto-load requires a symbol
        let sym = symbol.ok_or_else(|| {
            "No data loaded and no symbol provided. Pass a symbol (e.g. \"SPY\").".to_string()
        })?;

        // Validate the symbol to prevent path traversal attacks before passing to the data layer.
        let sym_upper = sym.to_uppercase();
        validate_path_segment(&sym_upper).map_err(|e| format!("Invalid symbol: {e}"))?;

        tracing::info!(symbol = %sym, "Auto-loading options data from cache");

        // Load data WITHOUT holding any lock so concurrent requests aren't blocked
        // during I/O. Two concurrent auto-loads for the same symbol may both fetch,
        // but the insert is idempotent.
        let df = self
            .cache
            .load_options(&sym_upper, None, None)
            .await
            .map_err(|e| format!("Failed to auto-load data for {sym}: {e}"))?;

        // Brief write lock just for insertion
        let mut data = self.data.write().await;

        // Another request may have loaded data while we were fetching — check and
        // use existing data if present for this symbol.
        if let Some(existing) = data.get(&sym_upper) {
            return Ok((sym_upper, existing.clone()));
        }

        data.insert(sym_upper.clone(), df.clone());
        Ok((sym_upper, df))
    }

    /// Ensure OHLCV price data exists for a symbol, auto-fetching from Yahoo Finance if needed.
    /// Returns the parquet file path.
    async fn ensure_ohlcv(&self, symbol: &str) -> Result<String, String> {
        // Try local cache, then S3 fallback
        if let Ok(path) = self.cache.ensure_local_for(symbol, "prices").await {
            return Ok(path.to_string_lossy().to_string());
        }

        tracing::info!(symbol = %symbol, "Auto-fetching OHLCV data from Yahoo Finance");

        tools::fetch::execute(&self.cache, symbol, "5y")
            .await
            .map_err(|e| format!("Failed to auto-fetch OHLCV data for {symbol}: {e}"))?;

        let path = self
            .cache
            .cache_path(symbol, "prices")
            .map_err(|e| format!("Error resolving OHLCV path: {e}"))?;
        Ok(path.to_string_lossy().to_string())
    }

    /// Collect all cross-symbol references from entry/exit signals and resolve their OHLCV paths.
    ///
    /// Inspects both the singular `entry_signal`/`exit_signal` and the plural
    /// `entry_signals`/`exit_signals` lists (used by parameter sweep).
    async fn resolve_cross_ohlcv_paths(
        &self,
        entry_signal: Option<&SignalSpec>,
        exit_signal: Option<&SignalSpec>,
        entry_signals: &[SignalSpec],
        exit_signals: &[SignalSpec],
    ) -> Result<HashMap<String, String>, String> {
        let mut all_symbols = std::collections::HashSet::new();
        if let Some(sig) = entry_signal {
            all_symbols.extend(collect_cross_symbols(sig));
        }
        if let Some(sig) = exit_signal {
            all_symbols.extend(collect_cross_symbols(sig));
        }
        for sig in entry_signals {
            all_symbols.extend(collect_cross_symbols(sig));
        }
        for sig in exit_signals {
            all_symbols.extend(collect_cross_symbols(sig));
        }

        let mut paths = HashMap::new();
        for sym in all_symbols {
            let path = self.ensure_ohlcv(&sym).await?;
            paths.insert(sym, path);
        }
        Ok(paths)
    }

    /// Resolve shared backtest base parameters into engine `BacktestParams`, auto-loading
    /// data and OHLCV as needed. Returns `(symbol, DataFrame, BacktestParams)`.
    async fn resolve_backtest_params(
        &self,
        base: BacktestBaseParams,
    ) -> Result<(String, DataFrame, BacktestParams), String> {
        let BacktestBaseParams {
            strategy,
            leg_deltas,
            entry_dte,
            exit_dte,
            slippage,
            commission,
            min_bid_ask,
            stop_loss,
            take_profit,
            max_hold_days,
            capital,
            quantity,
            sizing,
            multiplier,
            max_positions,
            selector,
            entry_signal,
            exit_signal,
            symbol: symbol_param,
            min_net_premium,
            max_net_premium,
            min_net_delta,
            max_net_delta,
            min_days_between_entries,
            expiration_filter,
            exit_net_delta,
        } = base;

        let (symbol, df) = self.ensure_data_loaded(symbol_param.as_deref()).await?;

        let strategy_def = crate::strategies::find_strategy(&strategy);
        let needs_ohlcv = entry_signal.is_some()
            || exit_signal.is_some()
            || matches!(
                sizing.as_ref().map(|s| &s.method),
                Some(crate::engine::types::PositionSizing::VolatilityTarget { .. })
            )
            || strategy_def.as_ref().is_some_and(|s| s.has_stock_leg);
        let ohlcv_path = if needs_ohlcv {
            Some(self.ensure_ohlcv(&symbol).await?)
        } else {
            None
        };

        let cross_ohlcv_paths = self
            .resolve_cross_ohlcv_paths(entry_signal.as_ref(), exit_signal.as_ref(), &[], &[])
            .await?;

        let leg_deltas = resolve_leg_deltas(leg_deltas, &strategy)?;

        let backtest_params = BacktestParams {
            strategy,
            leg_deltas,
            entry_dte,
            exit_dte,
            slippage,
            commission,
            min_bid_ask,
            stop_loss,
            take_profit,
            max_hold_days,
            capital,
            quantity,
            multiplier,
            max_positions,
            sizing,
            selector: selector.unwrap_or_default(),
            adjustment_rules: vec![],
            entry_signal,
            exit_signal,
            ohlcv_path,
            cross_ohlcv_paths,
            min_net_premium,
            max_net_premium,
            min_net_delta,
            max_net_delta,
            min_days_between_entries,
            expiration_filter: expiration_filter.unwrap_or_default(),
            exit_net_delta,
        };
        backtest_params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;

        Ok((symbol, df, backtest_params))
    }

    /// Resolve a symbol from the loaded data.
    /// If `symbol` is provided, look it up explicitly.
    /// If `symbol` is None:
    ///   - If no data is loaded, return error
    ///   - If exactly one symbol is loaded, use it
    ///   - If multiple symbols are loaded, return error asking for explicit symbol
    fn resolve_symbol<'a>(
        data: &'a HashMap<String, DataFrame>,
        symbol: Option<&str>,
    ) -> Result<(&'a String, &'a DataFrame), String> {
        // Check if no data is loaded first, regardless of whether symbol was provided
        if data.is_empty() {
            return Err("No data loaded. Pass a symbol parameter (e.g. \"SPY\").".to_string());
        }

        match symbol {
            Some(sym) => {
                let sym_upper = sym.to_uppercase();
                data.get_key_value(sym_upper.as_str()).ok_or_else(|| {
                    let mut loaded: Vec<&String> = data.keys().collect();
                    loaded.sort();
                    let loaded_list = loaded
                        .iter()
                        .map(|k| k.as_str())
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("Symbol '{sym_upper}' not loaded. Loaded: {loaded_list}.")
                })
            }
            None => {
                if data.len() == 1 {
                    Ok(data
                        .iter()
                        .next()
                        .expect("data.len() == 1 but iter is empty"))
                } else {
                    let mut keys: Vec<&String> = data.keys().collect();
                    keys.sort();
                    let symbols = keys
                        .iter()
                        .map(|k| k.as_str())
                        .collect::<Vec<_>>()
                        .join(", ");
                    Err(format!(
                        "Multiple symbols loaded: {symbols}. Specify the `symbol` parameter."
                    ))
                }
            }
        }
    }
}

/// Load OHLCV prices from a cached parquet file for chart overlay.
#[allow(clippy::too_many_lines)]
fn load_underlying_prices(path: &std::path::Path) -> Vec<tools::response_types::UnderlyingPrice> {
    let args = ScanArgsParquet::default();
    let path_str = path.to_string_lossy();
    let Ok(lf) = LazyFrame::scan_parquet(path_str.as_ref().into(), args) else {
        return vec![];
    };

    // Detect whether this file uses "datetime" or "date" column
    let Ok(schema) = lf.clone().collect_schema() else {
        return vec![];
    };
    let has_datetime = schema
        .get("datetime")
        .is_some_and(|dt| matches!(dt, polars::prelude::DataType::Datetime(_, _)));
    let date_col_name = if has_datetime { "datetime" } else { "date" };

    let Ok(df) = lf
        .select([
            col(date_col_name),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
        ])
        .sort([date_col_name], SortMultipleOptions::default())
        .collect()
    else {
        return vec![];
    };

    let Ok(opens) = df.column("open").and_then(|c| Ok(c.f64()?.clone())) else {
        return vec![];
    };
    let Ok(highs) = df.column("high").and_then(|c| Ok(c.f64()?.clone())) else {
        return vec![];
    };
    let Ok(lows) = df.column("low").and_then(|c| Ok(c.f64()?.clone())) else {
        return vec![];
    };
    let Ok(closes) = df.column("close").and_then(|c| Ok(c.f64()?.clone())) else {
        return vec![];
    };
    // Volume may be i64 or u64 depending on the parquet source
    let volumes = df
        .column("volume")
        .and_then(|c| Ok(c.cast(&polars::prelude::DataType::UInt64)?.u64()?.clone()))
        .ok();

    let mut prices = Vec::with_capacity(df.height());

    // Intraday path: "datetime" Datetime column
    if has_datetime {
        let Ok(dt_col_ref) = df.column("datetime") else {
            return vec![];
        };
        for i in 0..df.height() {
            let (Some(open), Some(high), Some(low), Some(close)) =
                (opens.get(i), highs.get(i), lows.get(i), closes.get(i))
            else {
                continue;
            };
            let Ok(ndt) =
                crate::engine::price_table::extract_datetime_from_column(dt_col_ref, i)
            else {
                continue;
            };
            // Use full timestamp for intraday, date-only for midnight
            let fmt = if ndt.time() == chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap() {
                ndt.format("%Y-%m-%d").to_string()
            } else {
                ndt.format("%Y-%m-%dT%H:%M:%S").to_string()
            };
            prices.push(tools::response_types::UnderlyingPrice {
                date: fmt,
                open,
                high,
                low,
                close,
                volume: volumes.as_ref().and_then(|v| v.get(i)),
            });
        }
        return prices;
    }

    // Daily path: "date" Date column
    let Ok(dates) = df.column("date").and_then(|c| Ok(c.date()?.clone())) else {
        return vec![];
    };
    for i in 0..df.height() {
        let (Some(days), Some(open), Some(high), Some(low), Some(close)) = (
            dates.phys.get(i),
            opens.get(i),
            highs.get(i),
            lows.get(i),
            closes.get(i),
        ) else {
            continue;
        };
        if let Some(date) =
            chrono::NaiveDate::from_num_days_from_ce_opt(days + EPOCH_DAYS_CE_OFFSET)
        {
            prices.push(tools::response_types::UnderlyingPrice {
                date: date.format("%Y-%m-%d").to_string(),
                open,
                high,
                low,
                close,
                volume: volumes.as_ref().and_then(|v| v.get(i)),
            });
        }
    }
    prices
}

use rmcp::handler::server::wrapper::Parameters;

#[tool_router]
impl OptopsyServer {
    /// Browse all 32 built-in options strategies grouped by category.
    ///
    /// **When to use**: To choose a strategy for analysis
    /// **Prerequisites**: None (informational, no data required)
    /// **Categories**: singles, spreads, straddles, strangles, butterflies, condors, iron, calendars, diagonals
    /// **Next tools**: `suggest_parameters()` or `run_options_backtest()` (once you pick a strategy)
    #[tool(name = "list_strategies", annotations(read_only_hint = true))]
    async fn list_strategies(&self) -> SanitizedJson<StrategiesResponse> {
        SanitizedJson(tools::strategies::execute())
    }

    /// Get status of currently loaded data.
    ///
    /// **When to use**: Check what symbol is currently loaded, row count, available columns
    /// **Prerequisites**: None (works with or without loaded data)
    /// **How it works**: Returns details about the in-memory `DataFrame` (symbol, rows, columns)
    /// **Next tool**: Proceed with `suggest_parameters()` or `run_options_backtest()`
    /// **Example usage**: After loading SPY, call this to confirm it's loaded and see column names
    #[tool(name = "get_loaded_symbol", annotations(read_only_hint = true))]
    async fn get_loaded_symbol(&self) -> SanitizedJson<StatusResponse> {
        SanitizedJson(tools::status::execute(&self.data).await)
    }

    /// Build, validate, save, list, search, and manage signals.
    ///
    /// **When to use**: When you want to discover and work with trading signals—both
    ///   searching the built-in signal catalog and defining custom entry/exit conditions
    ///   using price column formulas
    /// **Prerequisites**: None (formulas are validated at parse time, data needed only at backtest)
    ///
    /// **Actions**:
    ///   - `catalog` — Browse the full built-in signal catalog grouped by category (40+ signals)
    ///   - `search` — Search saved custom signals by name/description/formula (requires `prompt`).
    ///     Only searches user-saved signals, NOT built-ins (use the quick reference above for those)
    ///   - `create` — Build a signal from a formula, optionally save for later use
    ///   - `validate` — Check formula syntax without saving
    ///   - `list` — Show all saved custom signals
    ///   - `get` — Load a saved signal's spec
    ///   - `delete` — Remove a saved signal
    ///
    /// **Common built-in signals** (use directly as `entry_signal`/`exit_signal` JSON — no search needed):
    ///   - Momentum: `RsiBelow` (RSI < threshold), `RsiAbove` (RSI > threshold),
    ///     `MacdBullish`, `MacdBearish`, `MacdCrossover`,
    ///     `StochasticBelow`, `StochasticAbove`
    ///   - Overlap: `PriceAboveSma`, `PriceBelowSma`, `PriceAboveEma`, `PriceBelowEma`,
    ///     `SmaCrossover`, `SmaCrossunder`, `EmaCrossover`, `EmaCrossunder`
    ///   - Trend: `SupertrendBullish`, `SupertrendBearish`, `AroonUptrend`, `AroonDowntrend`
    ///   - Volatility: `AtrAbove`, `AtrBelow`, `BollingerLowerTouch`, `BollingerUpperTouch`,
    ///     `IvRankAbove`, `IvRankBelow`
    ///   - Volume: `MfiBelow`, `MfiAbove`, `ObvRising`, `ObvFalling`
    ///   - Combinators: `And { left, right }`, `Or { left, right }`, `Not { signal }`
    ///
    /// **Quick examples** (no search required for these):
    ///   - RSI < 30: `{ "type": "RsiBelow", "column": "adjclose", "threshold": 30.0 }`
    ///   - RSI > 70: `{ "type": "RsiAbove", "column": "adjclose", "threshold": 70.0 }`
    ///   - Price above SMA(50): `{ "type": "PriceAboveSma", "column": "adjclose", "period": 50 }`
    ///   - Combine two: `{ "type": "And", "left": <signal1>, "right": <signal2> }`
    ///   - Saved signal: `{ "type": "Saved", "name": "my_signal" }`
    ///
    /// **Formula syntax** (for action='create'):
    ///   - Columns: `close`, `open`, `high`, `low`, `volume`, `adjclose`
    ///   - Lookback: `close[1]` (previous bar), `close[5]` (5 bars ago)
    ///   - Functions: `sma(col, N)`, `ema(col, N)`, `std(col, N)`, `max(col, N)`,
    ///     `min(col, N)`, `abs(expr)`, `change(col, N)`, `pct_change(col, N)`
    ///   - Operators: `+`, `-`, `*`, `/`, `>`, `<`, `>=`, `<=`, `==`, `!=`
    ///   - Logical: `and`, `or`, `not`
    ///
    /// **Next tool**: `run_options_backtest()` with `entry_signal`/`exit_signal` set to the returned spec,
    ///   or use `{ "type": "Saved", "name": "signal_name" }` to reference saved signals
    #[tool(
        name = "build_signal",
        annotations(
            destructive_hint = true,
            idempotent_hint = false,
            read_only_hint = false
        )
    )]
    async fn build_signal(
        &self,
        Parameters(params): Parameters<BuildSignalParams>,
    ) -> SanitizedResult<BuildSignalResponse, String> {
        SanitizedResult(async {
            params
                .validate()
                .map_err(|e| validation_err("build_signal", e))?;

            let action = match params.action.as_str() {
                "create" => {
                    let name = params
                        .name
                        .ok_or("'name' is required for action='create'")?;
                    let formula = params
                        .formula
                        .ok_or("'formula' is required for action='create'")?;
                    tools::build_signal::Action::Create {
                        name,
                        formula,
                        description: params.description,
                        save: params.save,
                    }
                }
                "search" => {
                    let prompt = params
                        .prompt
                        .ok_or("'prompt' is required for action='search'")?;
                    tools::build_signal::Action::Search { prompt }
                }
                "list" => tools::build_signal::Action::List,
                "delete" => {
                    let name = params
                        .name
                        .ok_or("'name' is required for action='delete'")?;
                    tools::build_signal::Action::Delete { name }
                }
                "validate" => {
                    let formula = params
                        .formula
                        .ok_or("'formula' is required for action='validate'")?;
                    tools::build_signal::Action::Validate { formula }
                }
                "get" => {
                    let name = params.name.ok_or("'name' is required for action='get'")?;
                    tools::build_signal::Action::Get { name }
                }
                "catalog" => tools::build_signal::Action::Catalog,
                other => {
                    return Err(format!(
                        "Invalid action: \"{other}\". Must be \"catalog\", \"search\", \"create\", \"list\", \"delete\", \"validate\", or \"get\"."
                    ));
                }
            };

            Ok(tools::build_signal::execute(action))
        }.await)
    }

    /// Full event-driven day-by-day simulation with position management and metrics.
    ///
    /// **When to use**: Run a full capital-constrained backtest simulation
    /// **Prerequisites**: Data is auto-loaded from cache when you pass a symbol.
    ///   OHLCV data is auto-fetched when signals are used.
    /// **Next tools**: `compare_strategies()` (to test variations) or iterate on parameters
    ///
    /// **IMPORTANT**: `strategy` is REQUIRED — it defines WHAT option legs to trade.
    /// Signals only FILTER WHEN to enter/exit — they are optional add-ons.
    ///
    /// **What it simulates**:
    ///   - Day-by-day position opens (respecting `max_positions` constraint)
    ///   - Position management (stop loss, take profit, max hold days, DTE exit)
    ///   - Optional signal-based filtering (if `entry_signal`/`exit_signal` provided)
    ///   - Realistic P&L with bid/ask slippage and commissions
    /// **Output**:
    ///   - Trade log (every open/close with P&L and exit reason)
    ///   - Equity curve (daily capital evolution)
    ///   - Performance metrics (Sharpe, Sortino, Calmar, `VaR`, max drawdown, win rate, etc.)
    ///   - AI-enriched assessment and suggested next steps
    /// **Time to run**: 5-30 seconds depending on data size
    #[tool(name = "run_options_backtest", annotations(read_only_hint = true))]
    async fn run_options_backtest(
        &self,
        Parameters(params): Parameters<RunBacktestParams>,
    ) -> SanitizedResult<BacktestResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("run_options_backtest", e))?;

                tracing::info!(
                    strategy = params.base.strategy.as_str(),
                    symbol = params.base.symbol.as_deref().unwrap_or("auto"),
                    entry_dte_target = params.base.entry_dte.target,
                    entry_dte_min = params.base.entry_dte.min,
                    entry_dte_max = params.base.entry_dte.max,
                    exit_dte = params.base.exit_dte,
                    max_positions = params.base.max_positions,
                    capital = params.base.capital,
                    "Backtest request received"
                );

                let (symbol, df, backtest_params) =
                    self.resolve_backtest_params(params.base).await?;

                // Strategies with undefined max loss + sizing require stop_loss
                if backtest_params.sizing.is_some() && backtest_params.stop_loss.is_none() {
                    if let Some(strategy_def) =
                        crate::strategies::find_strategy(&backtest_params.strategy)
                    {
                        let all_short = strategy_def
                            .legs
                            .iter()
                            .all(|l| l.side == crate::engine::types::Side::Short);
                        if all_short {
                            return Err(
                                "Dynamic sizing with an all-short strategy (naked, straddle, strangle) \
                                 requires `stop_loss` to compute max loss per contract. Add a stop_loss value."
                                    .to_string(),
                            );
                        }
                    }
                }

                // Try to load underlying OHLCV close prices from cache for chart overlay
                let underlying_prices = match self.cache.ensure_local_for(&symbol, "prices").await {
                    Ok(path) => {
                        // Read on blocking thread since it's Polars I/O
                        let prices = tokio::task::spawn_blocking(
                            move || -> Vec<tools::response_types::UnderlyingPrice> {
                                load_underlying_prices(&path)
                            },
                        )
                        .await
                        .unwrap_or_default();
                        prices
                    }
                    Err(_) => vec![],
                };

                // Run backtest on a blocking thread — the engine performs synchronous
                // Polars I/O (scan_parquet) which conflicts with the tokio runtime.
                tokio::task::spawn_blocking(move || {
                    // Load OHLCV data for indicator charting (if signals need it)
                    let ohlcv_df = backtest_params.ohlcv_path.as_deref().and_then(|path| {
                        crate::engine::stock_sim::load_ohlcv_df(path, None, None).ok()
                    });
                    tools::backtest::execute(
                        &df,
                        &backtest_params,
                        underlying_prices,
                        ohlcv_df.as_ref(),
                    )
                })
                .await
                .map_err(|e| format!("Backtest task panicked: {e}"))?
                .map_err(|e| format!("Error: {e}"))
            }
            .await,
        )
    }

    /// Signal-driven stock/equity backtest on OHLCV data.
    ///
    /// **When to use**: Backtest a stock trading strategy driven by entry/exit signals
    ///   (e.g. "buy when RSI < 30, sell when RSI > 70")
    /// **Prerequisites**: None — OHLCV data is auto-fetched from Yahoo Finance and cached
    ///
    /// **Key difference from `run_options_backtest`**: This operates on stock prices (OHLCV bars),
    /// not options chains. No strategy/delta/DTE needed — signals drive everything.
    ///
    /// **What it simulates**:
    ///   - Day-by-day position opens when `entry_signal` fires
    ///   - Position management (stop loss, take profit, max hold days, exit signal)
    ///   - Realistic fills with slippage and commissions
    ///   - Long or short positions
    ///
    /// **Defaults**: quantity=100 shares (1 standard lot), capital=$10,000.
    /// Ensure capital covers (quantity × `share_price`) or trades will be skipped.
    /// For high-priced stocks like SPY (~$600), use capital ≥ 60000 with quantity=100.
    ///
    /// **Output**: Same format as options backtest — trade log, equity curve, performance metrics
    #[tool(name = "run_stock_backtest", annotations(read_only_hint = true))]
    async fn run_stock_backtest(
        &self,
        Parameters(params): Parameters<RunStockBacktestParams>,
    ) -> SanitizedResult<StockBacktestResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("run_stock_backtest", e))?;

                let symbol = params.symbol.to_uppercase();
                validate_path_segment(&symbol).map_err(|e| format!("Invalid symbol: {e}"))?;

                tracing::info!(
                    symbol = %symbol,
                    side = ?params.side.unwrap_or(crate::engine::types::Side::Long),
                    "Stock backtest request received"
                );

                // Ensure OHLCV data is available
                let ohlcv_path = self.ensure_ohlcv(&symbol).await?;

                // Resolve cross-symbol OHLCV paths for signals
                let cross_ohlcv_paths = self
                    .resolve_cross_ohlcv_paths(
                        Some(&params.entry_signal),
                        params.exit_signal.as_ref(),
                        &[],
                        &[],
                    )
                    .await?;

                // Load underlying prices for chart overlay
                let underlying_prices = match self.cache.ensure_local_for(&symbol, "prices").await {
                    Ok(path) => {
                        let prices =
                            tokio::task::spawn_blocking(move || load_underlying_prices(&path))
                                .await
                                .unwrap_or_default();
                        prices
                    }
                    Err(_) => vec![],
                };

                let start_date = params
                    .start_date
                    .as_deref()
                    .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());
                let end_date = params
                    .end_date
                    .as_deref()
                    .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());

                let stock_params = crate::engine::stock_sim::StockBacktestParams {
                    symbol,
                    side: params.side.unwrap_or(crate::engine::types::Side::Long),
                    capital: params.capital,
                    quantity: params.quantity,
                    sizing: params.sizing,
                    max_positions: params.max_positions,
                    slippage: params.slippage,
                    commission: params.commission,
                    stop_loss: params.stop_loss,
                    take_profit: params.take_profit,
                    max_hold_days: params.max_hold_days,
                    entry_signal: Some(params.entry_signal),
                    exit_signal: params.exit_signal,
                    ohlcv_path: Some(ohlcv_path),
                    cross_ohlcv_paths,
                    start_date,
                    end_date,
                    interval: params.interval.unwrap_or_default(),
                    session_filter: params.session_filter,
                };

                tokio::task::spawn_blocking(move || {
                    tools::stock_backtest::execute(&stock_params, underlying_prices)
                })
                .await
                .map_err(|e| format!("Stock backtest task panicked: {e}"))?
                .map_err(|e| format!("Error: {e}"))
            }
            .await,
        )
    }

    /// Permutation test for statistical significance of backtest results.
    ///
    /// Shuffles entry candidates across dates N times, re-runs the backtest, and compares
    /// real results against the random distribution. Produces p-values for key metrics
    /// (Sharpe, `PnL`, win rate, profit factor, CAGR).
    ///
    /// **Null hypothesis**: "the specific timing of entries doesn't matter."
    /// If p < 0.05, the strategy has a statistically significant edge.
    ///
    /// **Time to run**: scales linearly with `num_permutations` × single backtest time
    #[tool(name = "permutation_test", annotations(read_only_hint = true))]
    async fn permutation_test(
        &self,
        Parameters(params): Parameters<PermutationTestParams>,
    ) -> SanitizedResult<PermutationTestResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("permutation_test", e))?;

                tracing::info!(
                    strategy = params.base.strategy.as_str(),
                    symbol = params.base.symbol.as_deref().unwrap_or("auto"),
                    num_permutations = params.num_permutations,
                    "Permutation test request received"
                );

                let (_symbol, df, backtest_params) =
                    self.resolve_backtest_params(params.base).await?;

                let perm_params = crate::engine::permutation::PermutationParams {
                    num_permutations: params.num_permutations,
                    seed: params.seed,
                };

                tokio::task::spawn_blocking(move || {
                    let (entry_dates, exit_dates) =
                        crate::engine::core::build_signal_filters(&backtest_params, &df)?;
                    tools::permutation_test::execute(
                        &df,
                        &backtest_params,
                        &perm_params,
                        &entry_dates,
                        exit_dates.as_ref(),
                    )
                })
                .await
                .map_err(|e| format!("Permutation test task panicked: {e}"))?
                .map_err(|e| format!("Error: {e}"))
            }
            .await,
        )
    }

    /// Sweep parameter combinations across strategies, DTE, exit DTE, and slippage.
    ///
    /// **When to use**: To find optimal parameter combinations without manually building
    ///   `compare_strategies` entries. Generates cartesian product internally and ranks by Sharpe.
    /// **Prerequisites**: None — data is auto-loaded from cache when you pass a symbol.
    ///
    /// **How it works**:
    ///   1. Generates cartesian product of delta targets × DTE targets × exit DTEs × slippage models × signal variants
    ///   2. Filters invalid combos (`exit_dte` >= entry DTE min, inverted delta orderings)
    ///   3. Deduplicates identical combinations
    ///   4. Runs backtest on each combo (hard cap: 100 combinations)
    ///   5. Ranks by Sharpe ratio, computes dimension sensitivity
    ///   6. Optionally validates top 3 on out-of-sample data (default: 30% holdout)
    ///
    /// **Modes**:
    ///   - Provide `strategies` list: sweep specific strategies with custom delta grids
    ///   - Provide `direction` only: auto-select all matching strategies (bullish/bearish/neutral/volatile)
    ///   - Both: filter provided list by direction
    ///
    /// **Output**: Ranked results, dimension sensitivity analysis, OOS validation
    #[tool(name = "parameter_sweep", annotations(read_only_hint = true))]
    async fn parameter_sweep(
        &self,
        Parameters(params): Parameters<ParameterSweepParams>,
    ) -> SanitizedResult<SweepResponse, String> {
        SanitizedResult(async {
            params
                .validate()
                .map_err(|e| validation_err("parameter_sweep", e))?;

            // Validate: singular and plural signal fields are mutually exclusive
            if params.sim_params.entry_signal.is_some()
                && !params.sim_params.entry_signals.is_empty()
            {
                return Err(
                    "Cannot use both `entry_signal` (singular) and `entry_signals` (plural). \
                     Use `entry_signals` for sweeping multiple signals, or `entry_signal` for a fixed signal."
                        .to_string(),
                );
            }
            if params.sim_params.exit_signal.is_some()
                && !params.sim_params.exit_signals.is_empty()
            {
                return Err(
                    "Cannot use both `exit_signal` (singular) and `exit_signals` (plural). \
                     Use `exit_signals` for sweeping multiple signals, or `exit_signal` for a fixed signal."
                        .to_string(),
                );
            }

            let (symbol, df) = self.ensure_data_loaded(params.symbol.as_deref()).await?;

            let strategies = resolve_sweep_strategies(params.strategies, params.direction)?;

            // Auto-fetch OHLCV data if any signals are requested or any strategy has a stock leg
            let any_stock_leg = strategies.iter().any(|s| {
                crate::strategies::find_strategy(&s.name)
                    .is_some_and(|def| def.has_stock_leg)
            });
            let needs_ohlcv = params.sim_params.entry_signal.is_some()
                || params.sim_params.exit_signal.is_some()
                || !params.sim_params.entry_signals.is_empty()
                || !params.sim_params.exit_signals.is_empty()
                || any_stock_leg;
            let ohlcv_path = if needs_ohlcv {
                Some(self.ensure_ohlcv(&symbol).await?)
            } else {
                None
            };

            let cross_ohlcv_paths = self
                .resolve_cross_ohlcv_paths(
                    params.sim_params.entry_signal.as_ref(),
                    params.sim_params.exit_signal.as_ref(),
                    &params.sim_params.entry_signals,
                    &params.sim_params.exit_signals,
                )
                .await?;

            let sweep_params = crate::engine::sweep::SweepParams {
                strategies,
                sweep: crate::engine::sweep::SweepDimensions {
                    entry_dte_targets: params.sweep.entry_dte_targets,
                    exit_dtes: params.sweep.exit_dtes,
                    slippage_models: params.sweep.slippage_models,
                },
                sim_params: SimParams {
                    capital: params.sim_params.capital,
                    quantity: params.sim_params.quantity,
                    multiplier: params.sim_params.multiplier,
                    max_positions: params.sim_params.max_positions,
                    selector: params.sim_params.selector,
                    stop_loss: params.sim_params.stop_loss,
                    take_profit: params.sim_params.take_profit,
                    max_hold_days: params.sim_params.max_hold_days,
                    entry_signal: params.sim_params.entry_signal,
                    exit_signal: params.sim_params.exit_signal,
                    ohlcv_path,
                    cross_ohlcv_paths,
                    min_days_between_entries: params.sim_params.min_days_between_entries,
                    sizing: params.sim_params.sizing,
                    exit_net_delta: params.sim_params.exit_net_delta,
                },
                out_of_sample_pct: params.out_of_sample_pct / 100.0,
                direction: params.direction,
                entry_signals: params.sim_params.entry_signals,
                exit_signals: params.sim_params.exit_signals,
                num_permutations: params.num_permutations,
                permutation_seed: params.permutation_seed,
            };

            tokio::task::spawn_blocking(move || tools::sweep::execute(&df, &sweep_params))
                .await
                .map_err(|e| format!("Sweep task panicked: {e}"))?
                .map_err(|e| format!("Error: {e}"))
        }.await)
    }

    /// Rolling walk-forward validation: train on window 1, test on window 2, slide forward, repeat.
    ///
    /// **When to use**: After finding promising parameters via `run_options_backtest` or `parameter_sweep`,
    ///   validate that the strategy performs consistently across multiple time periods
    /// **Prerequisites**: None — data is auto-loaded from cache when you pass a symbol
    ///
    /// **How it works**:
    ///   1. Slides rolling train/test windows across the full date range
    ///   2. For each window: runs backtest on train slice, then on test slice
    ///   3. Collects per-window train/test metrics (Sharpe, P&L, trades, win rate)
    ///   4. Computes aggregate statistics: avg test Sharpe, % profitable windows, Sharpe decay
    ///
    /// **Key metrics**:
    ///   - `avg_train_test_sharpe_decay`: high values (>0.5) indicate overfitting
    ///   - `pct_profitable_windows`: % of test windows with positive P&L
    ///   - `std_test_sharpe`: lower = more consistent performance
    ///
    /// **Time to run**: Proportional to number of windows × backtest time per window
    #[tool(name = "walk_forward", annotations(read_only_hint = true))]
    async fn walk_forward(
        &self,
        Parameters(params): Parameters<WalkForwardParams>,
    ) -> SanitizedResult<WalkForwardResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("walk_forward", e))?;

                tracing::info!(
                    strategy = params.base.strategy.as_str(),
                    symbol = params.base.symbol.as_deref().unwrap_or("auto"),
                    train_days = params.train_days,
                    test_days = params.test_days,
                    step_days = ?params.step_days,
                    "Walk-forward request received"
                );

                let (_symbol, df, backtest_params) =
                    self.resolve_backtest_params(params.base).await?;

                let train_days = params.train_days;
                let test_days = params.test_days;
                let step_days = params.step_days;

                tokio::task::spawn_blocking(move || {
                    tools::walk_forward::execute(
                        &df,
                        &backtest_params,
                        train_days,
                        test_days,
                        step_days,
                    )
                })
                .await
                .map_err(|e| format!("Walk-forward task panicked: {e}"))?
                .map_err(|e| format!("Error: {e}"))
            }
            .await,
        )
    }

    /// Run multiple strategies in parallel and rank by performance metrics.
    ///
    /// **When to use**: After validating one strategy via `run_options_backtest()`, to test
    ///   parameter variations and find the best-performing approach
    /// **Prerequisites**: None — data is auto-loaded from cache when you pass a symbol
    /// **Why use this**: Compare different delta targets, DTE parameters, or strategies
    ///   side-by-side in a single call (faster than running multiple backtests)
    /// **Next tools**: pick best performer and iterate further, or conclude analysis
    ///
    /// **Modes**:
    ///   - Compare DTE/delta variations of same strategy
    ///   - Compare different strategies with same parameters
    ///   - Compare hybrid parameter sets
    /// **Rankings**: By Sharpe ratio (primary) and total `PnL` (secondary)
    /// **Output**: Metrics for each strategy + recommended best performer
    #[tool(name = "compare_strategies", annotations(read_only_hint = true))]
    async fn compare_strategies(
        &self,
        Parameters(params): Parameters<CompareStrategiesParams>,
    ) -> SanitizedResult<CompareResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("compare_strategies", e))?;

                let (symbol, df) = self.ensure_data_loaded(params.symbol.as_deref()).await?;

                // Auto-fetch OHLCV data if signals are requested or any strategy has a stock leg
                let any_stock_leg = params.strategies.iter().any(|s| {
                    crate::strategies::find_strategy(&s.name).is_some_and(|def| def.has_stock_leg)
                });
                let ohlcv_path = if params.entry_signal.is_some()
                    || params.exit_signal.is_some()
                    || any_stock_leg
                {
                    Some(self.ensure_ohlcv(&symbol).await?)
                } else {
                    None
                };

                let cross_ohlcv_paths = self
                    .resolve_cross_ohlcv_paths(
                        params.entry_signal.as_ref(),
                        params.exit_signal.as_ref(),
                        &[],
                        &[],
                    )
                    .await?;

                let mut sim_params = params.sim_params;
                sim_params.entry_signal = params.entry_signal;
                sim_params.exit_signal = params.exit_signal;
                sim_params.ohlcv_path = ohlcv_path;
                sim_params.cross_ohlcv_paths = cross_ohlcv_paths;

                let compare_params = CompareParams {
                    strategies: params
                        .strategies
                        .into_iter()
                        .map(|s| {
                            let leg_deltas = resolve_leg_deltas(s.leg_deltas, &s.name)?;
                            Ok(CompareEntry {
                                name: s.name,
                                leg_deltas,
                                entry_dte: s.entry_dte,
                                exit_dte: s.exit_dte,
                                slippage: s.slippage,
                                commission: s.commission,
                            })
                        })
                        .collect::<Result<Vec<_>, String>>()?,
                    sim_params,
                };
                compare_params
                    .validate()
                    .map_err(|e| validation_err("compare_strategies", e))?;

                tokio::task::spawn_blocking(move || tools::compare::execute(&df, &compare_params))
                    .await
                    .map_err(|e| format!("Compare task panicked: {e}"))?
                    .map_err(|e| format!("Error: {e}"))
            }
            .await,
        )
    }

    /// Check if cached Parquet data exists and when it was last updated.
    ///
    /// **When to use**: To avoid redundant downloads or to verify data staleness
    /// **Prerequisites**: None
    ///
    /// **Returns**:
    ///   - Cache exists (boolean)
    ///   - File path (if exists)
    ///   - File size and row count
    ///   - Last update timestamp
    #[tool(name = "check_cache_status", annotations(read_only_hint = true))]
    async fn check_cache_status(
        &self,
        Parameters(params): Parameters<CheckCacheParams>,
    ) -> SanitizedResult<CheckCacheResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("check_cache_status", e))?;
                let category = validate_category_read(&params.category)?;
                tools::cache_status::execute(&self.cache, &params.symbol, category)
                    .map_err(|e| format!("Error: {e}"))
            }
            .await,
        )
    }

    /// Return raw OHLCV price data for a symbol, ready for chart generation.
    /// Data is auto-fetched from Yahoo Finance on first access and cached locally.
    ///
    /// **When to use**: When an LLM or user needs raw price data to generate charts
    ///   (candlestick, line, area) or perform custom analysis
    /// **Prerequisites**: None — OHLCV data is auto-fetched and cached on first call
    ///
    /// **Returns**: Array of `{ date, open, high, low, close, adjclose, volume }` bars.
    /// Data is evenly sampled down to `limit` points (default 500 if omitted) to avoid
    /// overwhelming LLM context windows. Pass `limit: null` explicitly for the full dataset.
    ///
    /// **Use cases**:
    ///   - Generate candlestick or OHLC charts
    ///   - Plot price action with close/adjclose line charts
    ///   - Overlay backtest equity curves on underlying price data
    ///   - Feed into code interpreters for custom analysis
    #[tool(name = "get_raw_prices", annotations(read_only_hint = true))]
    async fn get_raw_prices(
        &self,
        Parameters(params): Parameters<GetRawPricesParams>,
    ) -> SanitizedResult<RawPricesResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("get_raw_prices", e))?;
                tools::raw_prices::load_and_execute(
                    &self.cache,
                    &params.symbol,
                    params.start_date.as_deref(),
                    params.end_date.as_deref(),
                    params.limit,
                    params.interval.unwrap_or_default(),
                )
                .await
                .map_err(|e| format!("Error: {e}"))
            }
            .await,
        )
    }
}

#[tool_handler]
impl ServerHandler for OptopsyServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: rmcp::model::ProtocolVersion::default(),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation {
                name: "optopsy-mcp".into(),
                title: Some("Optopsy Backtesting Engine".into()),
                version: env!("CARGO_PKG_VERSION").into(),
                description: Some("Event-driven backtesting engine for options (31 strategies) and stocks (signal-driven), with realistic position management and AI-compatible analysis tools".into()),
                icons: None,
                website_url: None,
            },
            instructions: Some(
                "Backtesting engine for options and stocks. Data is auto-loaded when you call any analysis tool — \
                just pass the symbol parameter.\
                \n\n## WORKFLOW\
                \n\
                \n### 1. Explore Strategies & Signals\
                \n  - list_strategies() — browse all 32 option strategies by category\
                \n  - For built-in signals (RsiBelow, MacdBullish, etc.): construct JSON directly — see build_signal tool description\
                \n  - build_signal(action=\"search\") — find saved custom signals by name\
                \n  - build_signal(action=\"catalog\") — browse all built-in signals by category\
                \n\
                \n### 2. Full Simulation\
                \n  - **Options**: run_options_backtest({ strategy, symbol, ... }) — event-driven options backtest\
                \n  - **Stocks**: run_stock_backtest({ symbol, entry_signal, ... }) — signal-driven stock backtest\
                \n    No strategy/delta/DTE needed — entry_signal is REQUIRED, exit uses stop-loss/take-profit/exit_signal\
                \n  - OHLCV data is auto-fetched from Yahoo Finance when needed\
                \n\
                \n### 3. Compare & Optimize (optional, options only)\
                \n  - parameter_sweep — PREFERRED for optimization. Generates cartesian product of delta/DTE/slippage combos automatically.\
                \n    Use `direction` to auto-select strategies by market outlook (bullish/bearish/neutral/volatile),\
                \n    or provide explicit `strategies` list with `leg_delta_targets` grids.\
                \n    Includes out-of-sample validation (default 30%) and dimension sensitivity analysis.\
                \n  - compare_strategies — use for manual side-by-side comparison of 2-3 specific configurations\
                \n    you've already chosen. NOT for grid search (use parameter_sweep instead).\
                \n\
                \n### 4. Evaluate Strategy Viability\
                \n  When a user asks you to test/evaluate if a strategy is viable, follow this reasoning loop:\
                \n\
                \n  **Step 1: Build the signals**\
                \n  - Translate the user's entry/exit conditions into SignalSpec JSON\
                \n  - For formula-based conditions: use build_signal(action=\"validate\") to verify syntax\
                \n  - For built-in indicators (RSI, MACD, etc.): construct the SignalSpec directly from the catalog\
                \n  - If a saved signal name is referenced: use build_signal(action=\"get\") to load it\
                \n  - If conditions are ambiguous, ask the user to clarify before proceeding\
                \n\
                \n  **Step 2: Run a baseline backtest**\
                \n  - For stocks: run_stock_backtest with the constructed entry/exit signals\
                \n  - For options: run_options_backtest with signals as entry/exit filters\
                \n  - Use sensible defaults (capital: 10000, quantity: 100 shares or 1 contract)\
                \n  - After results, reason about: Does it make money? Are there enough trades to be meaningful?\
                \n    Is the drawdown acceptable? Read the assessment and key_findings carefully.\
                \n  - If results are clearly negative or trade count is very low, explain why and stop —\
                \n    no need to validate a strategy that doesn't work at the basic level.\
                \n\
                \n  **Step 3: Validate significance and robustness**\
                \n  Based on the baseline results, decide which validations are worth running:\
                \n  - permutation_test — answers \"is this skill or luck?\" Run when results look good but\
                \n    you want to rule out chance. Look at p-values.\
                \n  - walk_forward — answers \"does this hold across time periods?\" Run when you want to\
                \n    check for overfitting. Look at Sharpe decay and % profitable windows.\
                \n  - parameter_sweep — answers \"is this fragile?\" Run when you suspect results depend on\
                \n    exact parameter choices. Look at dimension_sensitivity and OOS validation.\
                \n  You don't need to run all of these. Use your judgment based on what the baseline revealed.\
                \n\
                \n  **Step 4: Deliver a verdict**\
                \n  Synthesize all results into a clear assessment:\
                \n  - Summarize what passed, what raised concerns, and what failed\
                \n  - Give an overall viability judgment with reasoning\
                \n  - Suggest concrete next steps (adjust parameters, try different signals, trade with caution, etc.)\
                \n\
                \n## RULES\
                \n- For OPTIONS: strategy is ALWAYS REQUIRED — signals only filter WHEN to trade\
                \n- For STOCKS: entry_signal is ALWAYS REQUIRED — it drives when to buy/sell\
                \n- NEVER pass strategy: null for options — pick one like short_put, iron_condor, etc.\
                \n- For STOCKS: quantity means NUMBER OF SHARES (default: 100 = 1 standard lot). Do NOT pass large values like 10000.\
                \n  Ensure capital ≥ quantity × share_price, or all entries will be skipped. If `warnings` are returned, address them.\
                \n- For optimization, prefer parameter_sweep over manually enumerating compare_strategies entries\
                \n- Each tool response includes suggested_next_steps — follow them"
                    .into(),
            ),
        }
    }
}
