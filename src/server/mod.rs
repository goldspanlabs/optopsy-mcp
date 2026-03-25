//! MCP server implementation for optopsy.
//!
//! Holds shared state (loaded `DataFrames`, data cache, tool router) and exposes
//! all MCP tool handlers via `rmcp`'s `#[tool_router]` and `#[tool_handler]` macros.

mod handlers;
mod params;
mod sanitize;

pub use params::FactorProxies;

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
use crate::engine::types::BacktestParams;
use crate::signals::registry::{collect_cross_symbols, extract_formula_cross_symbols, SignalSpec};
use crate::tools;
use crate::tools::response_types::{
    AggregatePricesResponse, BayesianOptimizeResponse, BenchmarkAnalysisResponse,
    BuildSignalResponse, CointegrationResponse, CorrelateResponse, DistributionResponse,
    DrawdownAnalysisResponse, FactorAttributionResponse, HypothesisParams, HypothesisResponse,
    ListSymbolsResponse, MonteCarloResponse, PermutationTestResponse, PortfolioOptimizeResponse,
    RawPricesResponse, RegimeDetectResponse, RollingMetricResponse, StrategiesResponse,
    SweepResponse, WalkForwardResponse,
};
use params::{
    resolve_leg_deltas, tool_err, validation_err, AggregatePricesParams, BacktestBaseParams,
    BayesianOptimizeParams, BenchmarkAnalysisParams, BuildSignalParams, CointegrationParams,
    CorrelateParams, DistributionParams, DrawdownAnalysisParams, FactorAttributionParams,
    GetRawPricesParams, ListSymbolsParams, MonteCarloParams, ParameterSweepParams,
    PermutationTestParams, PortfolioOptimizeParams, RegimeDetectParams, RollingMetricParams,
    WalkForwardParams,
};
use sanitize::{SanitizedJson, SanitizedResult};

/// Loaded data: `HashMap<Symbol, DataFrame>` for multi-symbol support.
type LoadedData = HashMap<String, DataFrame>;

/// Resolved stock backtest parameters with symbol, ready for execution.
struct StockResolvedParams {
    symbol: String,
    params: crate::engine::stock_sim::StockBacktestParams,
}

/// MCP server for options backtesting, holding loaded data and the tool router.
#[derive(Clone)]
pub struct OptopsyServer {
    /// Multi-symbol in-memory data storage, keyed by uppercase ticker.
    pub data: Arc<RwLock<LoadedData>>,
    /// Shared data layer for local Parquet cache.
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

    /// Ensure OHLCV price data exists for a symbol.
    /// Returns the parquet file path.
    ///
    /// Searches `etf/`, `stocks/`, `futures/`, and `indices/` in order.
    fn ensure_ohlcv(&self, symbol: &str) -> Result<String, String> {
        match self.cache.find_ohlcv(symbol) {
            Some(path) => Ok(path.to_string_lossy().to_string()),
            None => Err(format!(
                "No OHLCV data found for {symbol}. Upload parquet to the cache directory."
            )),
        }
    }

    /// Collect all cross-symbol references from entry/exit signals and resolve their OHLCV paths.
    ///
    /// Inspects both the singular `entry_signal`/`exit_signal` and the plural
    /// `entry_signals`/`exit_signals` lists (used by parameter sweep).
    fn resolve_cross_ohlcv_paths(
        &self,
        entry_signal: Option<&SignalSpec>,
        exit_signal: Option<&SignalSpec>,
        entry_signals: &[SignalSpec],
        exit_signals: &[SignalSpec],
        extra_formulas: &[String],
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
        for formula in extra_formulas {
            all_symbols.extend(extract_formula_cross_symbols(formula));
        }

        let mut paths = HashMap::new();
        for sym in all_symbols {
            validate_path_segment(&sym)
                .map_err(|e| format!("Invalid cross-symbol \"{sym}\": {e}"))?;
            let path = self.ensure_ohlcv(&sym)?;
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
            // Stock-mode fields — ignored in this options-only path
            mode: _,
            side: _,
            interval: _,
            session_filter: _,
            start_date: _,
            end_date: _,
            max_hold_bars: _,
            min_bars_between_entries: _,
            conflict_resolution: _,
        } = base;

        // This function is only called for options mode; validation guarantees strategy is Some.
        let strategy = strategy
            .ok_or_else(|| "strategy is required for options-mode backtests".to_string())?;

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
            Some(self.ensure_ohlcv(&symbol)?)
        } else {
            None
        };

        let cross_ohlcv_paths = self.resolve_cross_ohlcv_paths(
            entry_signal.as_ref(),
            exit_signal.as_ref(),
            &[],
            &[],
            &[],
        )?;

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

    /// Resolve stock-mode backtest parameters from `BacktestBaseParams`.
    ///
    /// Ensures OHLCV data is available, builds `StockBacktestParams`, and prepares
    /// bars + signal filters. Returns everything needed to run a stock backtest.
    fn resolve_stock_backtest_params(
        &self,
        base: BacktestBaseParams,
    ) -> Result<StockResolvedParams, String> {
        // entry_signal is required for stock mode — without it no trades will ever open.
        if base.entry_signal.is_none() {
            return Err(
                "entry_signal is required for stock mode; provide a SignalSpec to drive trade entries".to_string()
            );
        }

        let symbol = base
            .symbol
            .as_deref()
            .ok_or("symbol is required for stock mode")?
            .to_uppercase();
        validate_path_segment(&symbol).map_err(|e| format!("Invalid symbol: {e}"))?;

        let ohlcv_path = self.ensure_ohlcv(&symbol)?;

        let cross_ohlcv_paths = self.resolve_cross_ohlcv_paths(
            base.entry_signal.as_ref(),
            base.exit_signal.as_ref(),
            &[],
            &[],
            &[],
        )?;

        let start_date = base
            .start_date
            .as_deref()
            .map(|s| {
                chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .map_err(|_| format!("Invalid start_date \"{s}\": expected YYYY-MM-DD"))
            })
            .transpose()?;
        let end_date = base
            .end_date
            .as_deref()
            .map(|s| {
                chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .map_err(|_| format!("Invalid end_date \"{s}\": expected YYYY-MM-DD"))
            })
            .transpose()?;

        let interval = base.interval.unwrap_or_default();
        let side = base.side.unwrap_or(crate::engine::types::Side::Long);

        let stock_params = crate::engine::stock_sim::StockBacktestParams {
            symbol: symbol.clone(),
            side,
            capital: base.capital,
            quantity: base.quantity,
            sizing: base.sizing,
            max_positions: base.max_positions,
            slippage: base.slippage,
            commission: base.commission,
            stop_loss: base.stop_loss,
            take_profit: base.take_profit,
            max_hold_days: base.max_hold_days,
            max_hold_bars: base.max_hold_bars,
            min_days_between_entries: base.min_days_between_entries,
            min_bars_between_entries: base.min_bars_between_entries,
            conflict_resolution: base.conflict_resolution.unwrap_or_default(),
            entry_signal: base.entry_signal,
            exit_signal: base.exit_signal,
            ohlcv_path: Some(ohlcv_path),
            cross_ohlcv_paths,
            start_date,
            end_date,
            interval,
            session_filter: base.session_filter,
        };

        Ok(StockResolvedParams {
            symbol,
            params: stock_params,
        })
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

use rmcp::handler::server::wrapper::Parameters;

#[tool_router]
impl OptopsyServer {
    /// Search or browse symbols available in the local Parquet cache.
    ///
    /// **Without query**: Returns a summary of cached data — category names and counts
    ///   (options, etf, stocks, futures, indices) — so you know what's available.
    /// **With query**: Case-insensitive prefix/substring search across all categories.
    ///   Returns up to 50 matching symbols with their category.
    ///
    /// **When to use**: To discover what data is available before running backtests or analysis.
    /// **Prerequisites**: None (reads the cache directory)
    /// **Next tools**: `run_options_backtest()`, `run_stock_backtest()`, or `get_raw_prices()`
    #[tool(name = "list_symbols", annotations(read_only_hint = true))]
    async fn list_symbols(
        &self,
        Parameters(params): Parameters<ListSymbolsParams>,
    ) -> SanitizedResult<ListSymbolsResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("list_symbols", e))?;
                tools::list_symbols::execute(&self.cache, params.query.as_deref()).map_err(tool_err)
            }
            .await,
        )
    }

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
    ///   - `update` — Update a saved signal: rename (`new_name`), set display name (`display_name`),
    ///     and/or change formula (`formula`). Requires `name` and `new_name` (can be same as `name`
    ///     to update in-place without renaming)
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
    ///   - Date/time (zero-arg): `day_of_week()` (1=Mon..7=Sun), `month()` (1-12),
    ///     `day_of_month()` (1-31), `hour()` (0-23), `minute()` (0-59), `week_of_year()` (1-53).
    ///     Use these to encode seasonal/day-of-week patterns found by `aggregate_prices`.
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
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("build_signal", e))?;
                handlers::signals::execute(params)
            }
            .await,
        )
    }
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
                handlers::optimization::execute_permutation_test(self, params).await
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
    ///
    /// **CRITICAL**: `entry_signals` (plural, with an 's') goes inside `sim_params`, NOT at the top level.
    ///
    /// **Example call (options)**:
    /// ```json
    /// {
    ///   "symbol": "SPY",
    ///   "direction": "bearish",
    ///   "sim_params": {
    ///     "capital": 100000, "quantity": 1, "multiplier": 100, "max_positions": 5,
    ///     "entry_signals": ["rsi(close, 14) < 30", "rsi(close, 14) < 25"]
    ///   },
    ///   "sweep": {
    ///     "delta_targets": [[0.20, 0.30], [0.30, 0.40]],
    ///     "dte_targets": [30, 45],
    ///     "exit_dte": 5
    ///   }
    /// }
    /// ```
    ///
    /// **Example call (stocks)**:
    /// ```json
    /// {
    ///   "symbol": "SPY", "mode": "stock",
    ///   "sim_params": {
    ///     "side": "Long", "capital": 100000, "quantity": 100,
    ///     "entry_signals": ["rsi(close, 14) < 30", "rsi(close, 14) < 25"],
    ///     "stop_loss": 0.05
    ///   }
    /// }
    /// ```
    #[tool(name = "parameter_sweep", annotations(read_only_hint = true))]
    async fn parameter_sweep(
        &self,
        Parameters(params): Parameters<ParameterSweepParams>,
    ) -> SanitizedResult<SweepResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("parameter_sweep", e))?;
                handlers::optimization::execute_sweep(self, params).await
            }
            .await,
        )
    }

    /// Bayesian optimization: find optimal strategy parameters using a Gaussian Process surrogate.
    ///
    /// **When to use**: When you have a large parameter space (many delta/DTE/slippage combinations)
    ///   and exhaustive grid search via `parameter_sweep` is too slow. Bayesian optimization
    ///   typically finds near-optimal configurations in 50-100 evaluations instead of exhaustive search.
    ///
    /// **How it works**:
    ///   1. Evaluates a small random initial batch (default: 10)
    ///   2. Fits a Gaussian Process surrogate model to observed (params → objective) pairs
    ///   3. Maximizes Expected Improvement acquisition function to pick the next most informative config
    ///   4. Evaluates, updates surrogate, repeats until budget exhausted
    ///
    /// **Key differences from `parameter_sweep`**:
    ///   - Continuous search over delta/DTE ranges (not discrete grid points)
    ///   - Budget-aware: specify max evaluations instead of enumerating all combinations
    ///   - Returns convergence trace showing how the objective improved over time
    ///   - Single strategy only (use `parameter_sweep` for multi-strategy comparison)
    ///
    /// **Output**: Ranked results, convergence trace, sensitivity analysis, OOS validation
    ///
    /// **Example call**:
    /// ```json
    /// {
    ///   "symbol": "SPY",
    ///   "strategy": "bull_call_spread",
    ///   "leg_delta_bounds": [{"min": 0.30, "max": 0.70}, {"min": 0.10, "max": 0.40}],
    ///   "entry_dte_min": 20,
    ///   "entry_dte_max": 60,
    ///   "exit_dtes": [0, 5, 10],
    ///   "max_evaluations": 50,
    ///   "sim_params": {"capital": 100000, "quantity": 1, "multiplier": 100}
    /// }
    /// ```
    #[tool(name = "bayesian_optimize", annotations(read_only_hint = true))]
    async fn bayesian_optimize(
        &self,
        Parameters(params): Parameters<BayesianOptimizeParams>,
    ) -> SanitizedResult<BayesianOptimizeResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("bayesian_optimize", e))?;

                tracing::info!(
                    strategy = %params.strategy,
                    symbol = params.symbol.as_deref().unwrap_or("auto"),
                    max_evaluations = params.max_evaluations,
                    objective = params.objective.as_deref().unwrap_or("sharpe"),
                    "Bayesian optimization request received"
                );

                handlers::optimization::execute_bayesian_optimize(self, params).await
            }
            .await,
        )
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
                handlers::optimization::execute_walk_forward(self, params).await
            }
            .await,
        )
    }
    /// Return raw OHLCV price data for a symbol, ready for chart generation.
    /// Data is loaded from the local Parquet cache.
    ///
    /// **When to use**: When an LLM or user needs raw price data to generate charts
    ///   (candlestick, line, area) or perform custom analysis
    /// **Prerequisites**: None — OHLCV data is loaded from cache
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
                    params.tail,
                )
                .await
                .map_err(tool_err)
            }
            .await,
        )
    }

    /// Aggregate OHLCV price statistics by time dimension (day-of-week, month, quarter, year, hour-of-day).
    /// Returns per-bucket descriptive stats with t-test p-values for significance.
    ///
    /// Use this to identify seasonal patterns, day-of-week effects, intraday hour patterns, or time-based anomalies.
    /// The `"gap"` metric measures the relative move between each bar's open and the previous bar's close
    /// for the selected interval. With daily bars this corresponds to overnight opening gaps; with intraday
    /// data (e.g., `group_by="hour_of_day"`) it reflects bar-to-bar gaps between consecutive closes and opens.
    /// `group_by="hour_of_day"` requires intraday data — pass `interval="1h"` (or `"30m"`, `"5m"`, `"1m"`).
    #[tool(name = "aggregate_prices", annotations(read_only_hint = true))]
    async fn aggregate_prices(
        &self,
        Parameters(params): Parameters<AggregatePricesParams>,
    ) -> SanitizedResult<AggregatePricesResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("aggregate_prices", e))?;
                tools::aggregate_prices::execute(
                    &self.cache,
                    &params.symbol,
                    params.years,
                    &params.group_by,
                    &params.metric,
                    params.interval,
                    params.start_date.as_deref(),
                    params.end_date.as_deref(),
                )
                .await
                .map_err(tool_err)
            }
            .await,
        )
    }

    /// Analyze the statistical distribution of price returns or trade P&L values.
    /// Returns descriptive stats, histogram, normality test, and tail analysis.
    ///
    /// Two modes: `price_returns` (auto-loads OHLCV) or `trade_pnl` (user-provided array).
    ///
    /// **Example (price returns)**:
    /// ```json
    /// {
    ///   "source": {"type": "price_returns", "symbol": "SPY", "years": 5}
    /// }
    /// ```
    ///
    /// **Example (trade P&L from a backtest)**:
    /// ```json
    /// {
    ///   "source": {"type": "trade_pnl", "values": [150.0, -80.0, 200.0, -50.0, 300.0]}
    /// }
    /// ```
    #[tool(name = "distribution", annotations(read_only_hint = true))]
    async fn distribution(
        &self,
        Parameters(params): Parameters<DistributionParams>,
    ) -> SanitizedResult<DistributionResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("distribution", e))?;
                tools::distribution::execute(&self.cache, &params.source, params.n_bins)
                    .await
                    .map_err(tool_err)
            }
            .await,
        )
    }

    /// Compute correlation between two price series (Pearson, Spearman, R²).
    /// Supports full-period and rolling correlation modes with scatter data for visualization.
    /// Optional `lag_range` enables cross-correlogram and Granger causality testing for lead/lag detection.
    #[tool(name = "correlate", annotations(read_only_hint = true))]
    async fn correlate(
        &self,
        Parameters(params): Parameters<CorrelateParams>,
    ) -> SanitizedResult<CorrelateResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("correlate", e))?;
                tools::correlate::execute(
                    &self.cache,
                    &params.series_a,
                    &params.series_b,
                    &params.mode,
                    params.window,
                    params.years,
                    params.lag_range.as_ref().map(|lr| (lr.min, lr.max)),
                    params
                        .interval
                        .unwrap_or(crate::engine::types::Interval::Daily),
                )
                .await
                .map_err(tool_err)
            }
            .await,
        )
    }

    /// Compute a rolling metric over time (volatility, Sharpe, mean return, max drawdown, beta, correlation).
    /// Returns a time series of the metric plus summary statistics and trend detection.
    ///
    /// Metrics `beta` and `correlation` require a `benchmark` symbol.
    #[tool(name = "rolling_metric", annotations(read_only_hint = true))]
    async fn rolling_metric(
        &self,
        Parameters(params): Parameters<RollingMetricParams>,
    ) -> SanitizedResult<RollingMetricResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("rolling_metric", e))?;
                tools::rolling_metric::execute(
                    &self.cache,
                    &params.symbol,
                    &params.metric,
                    params.window,
                    params.benchmark.as_deref(),
                    params.years,
                )
                .await
                .map_err(tool_err)
            }
            .await,
        )
    }

    /// Detect market regimes using volatility clustering, trend state analysis, or Hidden Markov Models.
    /// Returns per-regime statistics, a transition probability matrix, and a time series of regime labels.
    ///
    /// Methods: `volatility_cluster` (quantile-based vol regimes), `trend_state` (SMA crossover),
    /// or `hmm` (Gaussian HMM with Baum-Welch EM — learns regime parameters from data).
    #[tool(name = "regime_detect", annotations(read_only_hint = true))]
    async fn regime_detect(
        &self,
        Parameters(params): Parameters<RegimeDetectParams>,
    ) -> SanitizedResult<RegimeDetectResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("regime_detect", e))?;
                tools::regime_detect::execute(
                    &self.cache,
                    &params.symbol,
                    &params.method,
                    params.n_regimes,
                    params.years,
                    params.lookback_window,
                    params
                        .interval
                        .unwrap_or(crate::engine::types::Interval::Daily),
                )
                .await
                .map_err(tool_err)
            }
            .await,
        )
    }

    /// Scan multiple dimensions for statistically significant trading patterns.
    ///
    /// Applies BH-FDR correction to control false discoveries, computes Deflated Sharpe Ratios,
    /// deduplicates overlapping signals, and ranks hypotheses by a composite score
    /// (structural weight × DSR × regime stability).
    ///
    /// **When to use**: To discover potential trading patterns before building backtests.
    /// Results are HYPOTHESES to investigate — not confirmed strategies.
    ///
    /// **Dimensions scanned**: seasonality (day-of-week, month, turn-of-month),
    /// price action (momentum, consecutive moves), mean reversion (Bollinger, z-score),
    /// volume (spikes, low volume), volatility regime, cross-asset lead/lag,
    /// microstructure (gaps, intraday range), autocorrelation.
    ///
    /// **Output**: Ranked patterns with deployable signal specs that can be passed directly
    /// to `run_stock_backtest` or `run_options_backtest` for validation.
    ///
    /// **Time to run**: 5-15 seconds depending on number of symbols and dimensions.
    #[tool(name = "generate_hypotheses", annotations(read_only_hint = true))]
    async fn generate_hypotheses(
        &self,
        Parameters(params): Parameters<HypothesisParams>,
    ) -> SanitizedResult<HypothesisResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("generate_hypotheses", e))?;

                tracing::info!(
                    symbols = ?params.symbols,
                    dimensions = ?params.dimensions,
                    significance = params.significance,
                    "Hypothesis generation request received"
                );

                let cache = self.cache.clone();
                // Validate all symbols have OHLCV data
                for sym in &params.symbols {
                    let upper = sym.to_uppercase();
                    validate_path_segment(&upper)
                        .map_err(|e| format!("Invalid symbol \"{sym}\": {e}"))?;
                    self.ensure_ohlcv(&upper)?;
                }

                tools::hypothesis::execute(&cache, &params)
                    .await
                    .map_err(tool_err)
            }
            .await,
        )
    }
    /// Analyze the full drawdown distribution of a symbol's price history.
    ///
    /// Decomposes the equity curve into individual drawdown episodes and computes
    /// detailed statistics: episode depths, durations, recovery times, Ulcer Index,
    /// and an underwater curve for charting.
    ///
    /// **When to use**: After seeing `max_drawdown` in backtest results, use this to understand
    /// the full drawdown *distribution* — two strategies with identical `max_drawdown` can have
    /// very different drawdown profiles.
    ///
    /// **Output**: Top 20 drawdown episodes by depth, aggregate distribution stats,
    /// and an underwater curve for visualization.
    #[tool(name = "drawdown_analysis", annotations(read_only_hint = true))]
    async fn drawdown_analysis(
        &self,
        Parameters(params): Parameters<DrawdownAnalysisParams>,
    ) -> SanitizedResult<DrawdownAnalysisResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("drawdown_analysis", e))?;
                tools::drawdown_analysis::execute(&self.cache, &params.symbol, params.years)
                    .await
                    .map_err(tool_err)
            }
            .await,
        )
    }

    /// Test for cointegration between two price series using the Engle-Granger method.
    ///
    /// Fits a cointegrating regression (B = alpha + beta * A), computes the spread (residuals),
    /// and tests stationarity via an ADF test. If cointegrated, the spread is mean-reverting
    /// and suitable for pairs/statistical arbitrage strategies.
    ///
    /// **When to use**: Before building pairs trading strategies. Correlation measures
    /// co-movement of *returns*; cointegration measures whether a *spread* between two
    /// prices is mean-reverting — a much stronger condition for stat-arb.
    ///
    /// **Output**: Hedge ratio, ADF test, spread statistics (z-score, half-life), and
    /// a spread time series for charting.
    #[tool(name = "cointegration_test", annotations(read_only_hint = true))]
    async fn cointegration_test(
        &self,
        Parameters(params): Parameters<CointegrationParams>,
    ) -> SanitizedResult<CointegrationResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("cointegration_test", e))?;
                tools::cointegration::execute(
                    &self.cache,
                    &params.symbol_a,
                    &params.symbol_b,
                    params.years,
                )
                .await
                .map_err(tool_err)
            }
            .await,
        )
    }

    /// Run Monte Carlo simulations to estimate forward-looking risk and return distributions.
    ///
    /// Fits a return distribution from historical data, then generates thousands of synthetic
    /// equity paths via bootstrapped block resampling. Produces confidence intervals on
    /// terminal wealth, max drawdown distributions, and ruin probabilities.
    ///
    /// **When to use**: After backtesting, to estimate the range of possible outcomes
    /// going forward. Complements the permutation test (which tests *past* significance)
    /// with *forward-looking* risk quantification.
    ///
    /// **Output**: Percentile paths (5th/25th/50th/75th/95th), ruin probabilities,
    /// drawdown distribution, and terminal wealth histogram.
    #[tool(name = "monte_carlo", annotations(read_only_hint = true))]
    async fn monte_carlo(
        &self,
        Parameters(params): Parameters<MonteCarloParams>,
    ) -> SanitizedResult<MonteCarloResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("monte_carlo", e))?;
                tools::monte_carlo::execute(
                    &self.cache,
                    &params.symbol,
                    params.n_simulations,
                    params.horizon_days,
                    params.initial_capital,
                    params.years,
                    params.seed,
                )
                .await
                .map_err(tool_err)
            }
            .await,
        )
    }

    /// Decompose returns into systematic factor exposures and idiosyncratic alpha.
    ///
    /// Runs a multi-factor regression using ETF proxies for Market, Size (SMB),
    /// Value (HML), and Momentum factors. Answers: "Is my return explained by
    /// known risk factors, or is there genuine alpha?"
    ///
    /// **When to use**: After finding a profitable strategy, to verify the alpha
    /// isn't simply market beta or factor exposure in disguise.
    ///
    /// **Output**: Factor betas with significance tests, alpha estimate,
    /// R² (how much is explained), and return attribution breakdown.
    #[tool(name = "factor_attribution", annotations(read_only_hint = true))]
    async fn factor_attribution(
        &self,
        Parameters(params): Parameters<FactorAttributionParams>,
    ) -> SanitizedResult<FactorAttributionResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("factor_attribution", e))?;
                tools::factor_attribution::execute(
                    &self.cache,
                    &params.symbol,
                    &params.benchmark,
                    params.factor_proxies.as_ref(),
                    params.years,
                )
                .await
                .map_err(tool_err)
            }
            .await,
        )
    }

    /// Optimize portfolio weights using risk parity, minimum variance, and/or maximum Sharpe.
    ///
    /// Takes 2-20 symbols and computes optimal allocations using three methods:
    /// - **`risk_parity`**: Equal risk contribution from each asset
    /// - **`min_variance`**: Minimize total portfolio volatility
    /// - **`max_sharpe`**: Maximize risk-adjusted return (tangency portfolio)
    ///
    /// **When to use**: After identifying a set of assets/strategies, to determine
    /// optimal allocation weights rather than using equal weighting.
    ///
    /// **Output**: Optimal weights per method, expected portfolio metrics,
    /// correlation matrix, and per-asset statistics.
    #[tool(name = "portfolio_optimize", annotations(read_only_hint = true))]
    async fn portfolio_optimize(
        &self,
        Parameters(params): Parameters<PortfolioOptimizeParams>,
    ) -> SanitizedResult<PortfolioOptimizeResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("portfolio_optimize", e))?;
                tools::portfolio_optimize::execute(
                    &self.cache,
                    &params.symbols,
                    params.methods.as_deref(),
                    params.years,
                    params.risk_free_rate,
                )
                .await
                .map_err(tool_err)
            }
            .await,
        )
    }

    /// Compute benchmark-relative performance metrics: Jensen's alpha, beta, Treynor ratio,
    /// Information Ratio, tracking error, and up/down capture ratios.
    ///
    /// **When to use**: To evaluate an active strategy relative to a passive benchmark.
    /// Sharpe measures absolute risk-adjusted return; Information Ratio measures
    /// risk-adjusted *excess* return over the benchmark.
    ///
    /// **Output**: Alpha (with significance test), beta, Treynor, IR, tracking error,
    /// up/down capture ratios, and R².
    #[tool(name = "benchmark_analysis", annotations(read_only_hint = true))]
    async fn benchmark_analysis(
        &self,
        Parameters(params): Parameters<BenchmarkAnalysisParams>,
    ) -> SanitizedResult<BenchmarkAnalysisResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("benchmark_analysis", e))?;
                tools::benchmark_analysis::execute(
                    &self.cache,
                    &params.symbol,
                    &params.benchmark,
                    params.years,
                )
                .await
                .map_err(tool_err)
            }
            .await,
        )
    }

    /// Run a Rhai backtest script — unified scripting interface for backtesting.
    ///
    /// **When to use**: Execute custom backtest strategies defined as Rhai scripts.
    /// Scripts define `config()`, `on_bar(ctx)`, and optional callbacks
    /// (`on_exit_check`, `on_position_opened`, `on_position_closed`).
    /// See `scripts/SCRIPTING_REFERENCE.md` for the full API.
    ///
    /// **Primary mode**: Pass `strategy` with a script filename (without `.rhai`).
    /// Scripts are loaded from `scripts/strategies/{name}.rhai`.
    /// Write the `.rhai` file first, then reference it by name.
    ///
    /// **Fallback**: Pass `script` with inline Rhai source for quick one-off tests.
    ///
    /// **Example call**:
    /// ```json
    /// {
    ///   "strategy": "short_put",
    ///   "params": { "SYMBOL": "SPY", "CAPITAL": 50000, "DELTA_TARGET": 0.30, "DTE_TARGET": 45 }
    /// }
    /// ```
    ///
    /// **Output**: Trade log, equity curve, performance metrics (same format as other backtest tools)
    #[tool(name = "run_script", annotations(read_only_hint = true))]
    async fn run_script(
        &self,
        Parameters(params): Parameters<tools::run_script::RunScriptParams>,
    ) -> SanitizedResult<tools::run_script::RunScriptResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("run_script", e))?;
                handlers::run_script::execute(self, params)
                    .await
                    .map_err(tool_err)
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
                \n### 0. Discover Available Data\
                \n  - list_symbols() — see category counts (options, etf, stocks, futures, indices)\
                \n  - list_symbols({ query: \"SPY\" }) — search for a specific symbol across all categories\
                \n\
                \n### 1. Explore Strategies & Signals\
                \n  - list_strategies() — browse all 32 option strategies by category\
                \n  - For built-in signals (RsiBelow, MacdBullish, etc.): construct JSON directly — see build_signal tool description\
                \n  - build_signal(action=\"search\") — find saved custom signals by name\
                \n  - build_signal(action=\"catalog\") — browse all built-in signals by category\
                \n\
                \n### 2. Full Simulation\
                \n  - **run_script** — Execute Rhai backtest scripts for both options and stock strategies.\
                \n    Pass `strategy` (filename from scripts/strategies/) or `script` (inline Rhai source).\
                \n    See scripts/SCRIPTING_REFERENCE.md for the full ctx API.\
                \n  - OHLCV and options data is loaded from cache automatically\
                \n\
                \n### 3. Compare & Optimize (optional, options only)\
                \n  - parameter_sweep — PREFERRED for optimization. Generates cartesian product of delta/DTE/slippage combos automatically.\
                \n    Use `direction` to auto-select strategies by market outlook (bullish/bearish/neutral/volatile),\
                \n    or provide explicit `strategies` list with `leg_delta_targets` grids.\
                \n    Includes out-of-sample validation (default 30%) and dimension sensitivity analysis.\
                \n  - Or run run_script multiple times with different params to compare strategies manually.\
                \n\
                \n### 4. Discover Patterns (optional)\
                \n  - generate_hypotheses({ symbols: [\"SPY\"] }) — scan for statistically significant patterns\
                \n  - Results are HYPOTHESES — validate with backtest + walk_forward before trusting\
                \n\
                \n### 5. Evaluate Strategy Viability\
                \n  When a user asks you to test/evaluate if a strategy is viable, follow this reasoning loop:\
                \n\
                \n  **Step 1: Build the signals**\
                \n  - Translate the user's entry/exit conditions into formula strings (e.g. \"rsi(close, 14) < 30\")\
                \n  - For formula-based conditions: use build_signal(action=\"validate\") to verify syntax\
                \n  - For built-in indicators (RSI, MACD, etc.): write the formula directly (e.g. \"macd_hist(close) > 0\")\
                \n  - If a saved signal name is referenced: use build_signal(action=\"get\") to load it\
                \n  - If conditions are ambiguous, ask the user to clarify before proceeding\
                \n\
                \n  **Step 2: Run a baseline backtest**\
                \n  - Write a .rhai script with entry/exit logic and run via run_script\
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
                \n- For STOCKS: entry_signal is ALWAYS REQUIRED — pass a formula STRING like \"rsi(close, 14) < 30\"\
                \n- NEVER pass null for entry_signal, exit_signal, strategy, or side. Either pass a value or OMIT the field entirely.\
                \n- NEVER pass strategy: null for options — pick one like short_put, iron_condor, etc.\
                \n- For STOCKS: quantity means NUMBER OF SHARES (default: 100 = 1 standard lot). Do NOT pass large values like 10000.\
                \n  Ensure capital ≥ quantity × share_price, or all entries will be skipped. If `warnings` are returned, address them.\
                \n- For optimization, prefer parameter_sweep. Or run run_script multiple times with different params to compare manually.\
                \n- Each tool response includes suggested_next_steps — follow them"
                    .into(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    /// Load OHLCV prices from a cached parquet file for chart overlay.
    ///
    /// When `filter_datetimes` is provided, only OHLCV bars whose `datetime` matches
    /// one of the given timestamps are returned. This is used for options backtests
    /// where the options and OHLCV data share aligned timestamps (e.g. 15:59:00).
    ///
    /// When `resample_interval` is provided, the data is resampled to that interval
    /// before building the output (e.g. Daily for stock backtests to avoid returning
    /// millions of intraday bars).
    #[allow(clippy::too_many_lines)]
    fn load_underlying_prices(
        path: &std::path::Path,
        filter_datetimes: Option<&Column>,
        resample_interval: Option<crate::engine::types::Interval>,
        date_range: Option<(Option<chrono::NaiveDate>, Option<chrono::NaiveDate>)>,
    ) -> Vec<tools::response_types::UnderlyingPrice> {
        let args = ScanArgsParquet::default();
        let path_str = path.to_string_lossy();
        let Ok(lf) = LazyFrame::scan_parquet(path_str.as_ref().into(), args) else {
            return vec![];
        };

        let Ok(schema) = lf.clone().collect_schema() else {
            return vec![];
        };
        let has_datetime_col = schema
            .get("datetime")
            .is_some_and(|dt| matches!(dt, polars::prelude::DataType::Datetime(_, _)));
        let date_col_name = if has_datetime_col { "datetime" } else { "date" };

        let mut lazy = lf.select([
            col(date_col_name),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
        ]);

        if let Some(dt_filter) = filter_datetimes {
            if let Ok(unique) = dt_filter.unique() {
                if let Ok(list) = unique.take_materialized_series().implode() {
                    lazy = lazy.filter(col(date_col_name).is_in(lit(list.into_series()), false));
                }
            }
        }

        let Ok(df) = lazy
            .sort([date_col_name], SortMultipleOptions::default())
            .collect()
        else {
            return vec![];
        };

        let df = if let Some(interval) = resample_interval {
            crate::engine::ohlcv::resample_ohlcv(&df, interval).unwrap_or(df)
        } else {
            df
        };

        let df = if let Some(interval) = resample_interval {
            if interval.is_intraday() && df.column("datetime").is_ok() {
                if let Some((start, end)) = &date_range {
                    let mut filtered = df.clone();
                    if let Some(s) = start {
                        let start_dt = s.and_hms_opt(0, 0, 0).unwrap();
                        filtered = filtered
                            .clone()
                            .lazy()
                            .filter(col("datetime").gt_eq(lit(start_dt)))
                            .collect()
                            .unwrap_or(filtered);
                    }
                    if let Some(e) = end {
                        let end_next = e.succ_opt().unwrap_or(*e).and_hms_opt(0, 0, 0).unwrap();
                        filtered = filtered
                            .clone()
                            .lazy()
                            .filter(col("datetime").lt(lit(end_next)))
                            .collect()
                            .unwrap_or(filtered);
                    }
                    filtered
                } else {
                    let cutoff = (chrono::Utc::now() - chrono::Duration::days(7)).naive_utc();
                    df.clone()
                        .lazy()
                        .filter(col("datetime").gt_eq(lit(cutoff)))
                        .collect()
                        .unwrap_or(df)
                }
            } else {
                df
            }
        } else {
            df
        };

        let date_col_name = if df.column("datetime").is_ok() {
            "datetime"
        } else {
            "date"
        };
        let has_datetime = df
            .column(date_col_name)
            .ok()
            .is_some_and(|c| matches!(c.dtype(), polars::prelude::DataType::Datetime(_, _)));

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
        let volumes = df
            .column("volume")
            .and_then(|c| Ok(c.cast(&polars::prelude::DataType::UInt64)?.u64()?.clone()))
            .ok();

        let mut prices = Vec::with_capacity(df.height());

        if has_datetime {
            let Ok(dt_col_ref) = df.column(date_col_name) else {
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
                prices.push(tools::response_types::UnderlyingPrice {
                    date: ndt.and_utc().timestamp(),
                    open,
                    high,
                    low,
                    close,
                    volume: volumes.as_ref().and_then(|v| v.get(i)),
                });
            }
            return prices;
        }

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
            if let Some(date) = chrono::NaiveDate::from_num_days_from_ce_opt(
                days + crate::engine::types::EPOCH_DAYS_CE_OFFSET,
            ) {
                prices.push(tools::response_types::UnderlyingPrice {
                    date: date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp(),
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

    /// Write a synthetic intraday OHLCV `DataFrame` to a temp parquet file.
    /// Returns the path. 12 bars across 2 dates at various times.
    fn write_intraday_parquet() -> tempfile::NamedTempFile {
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 16).unwrap();
        let datetimes = vec![
            d1.and_hms_opt(9, 30, 0).unwrap(),
            d1.and_hms_opt(10, 0, 0).unwrap(),
            d1.and_hms_opt(15, 59, 0).unwrap(),
            d1.and_hms_opt(16, 0, 0).unwrap(),
            d2.and_hms_opt(9, 30, 0).unwrap(),
            d2.and_hms_opt(10, 0, 0).unwrap(),
            d2.and_hms_opt(15, 59, 0).unwrap(),
            d2.and_hms_opt(16, 0, 0).unwrap(),
        ];
        let n = datetimes.len();
        let df = df! {
            "datetime" => &datetimes,
            "open" => vec![100.0; n],
            "high" => vec![101.0; n],
            "low" => vec![99.0; n],
            "close" => vec![100.5; n],
            "volume" => vec![1000_i64; n],
        }
        .unwrap();

        let tmp = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
        polars::prelude::ParquetWriter::new(std::fs::File::create(tmp.path()).unwrap())
            .finish(&mut df.clone())
            .unwrap();
        tmp
    }

    #[test]
    fn underlying_prices_no_filter_returns_all_bars() {
        let tmp = write_intraday_parquet();
        let prices = load_underlying_prices(tmp.path(), None, None, None);
        assert_eq!(prices.len(), 8);
    }

    #[test]
    fn underlying_prices_filter_matches_only_given_timestamps() {
        let tmp = write_intraday_parquet();

        // Build a filter column with only 15:59:00 timestamps (like options data)
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 16).unwrap();
        let filter_dts = vec![
            d1.and_hms_opt(15, 59, 0).unwrap(),
            d2.and_hms_opt(15, 59, 0).unwrap(),
        ];
        let filter_col: Column = Series::new("datetime".into(), &filter_dts).into();

        let prices = load_underlying_prices(tmp.path(), Some(&filter_col), None, None);
        assert_eq!(prices.len(), 2, "should only return 15:59 bars");
        // Verify epochs correspond to 15:59:00 times
        let dt0 = chrono::DateTime::from_timestamp(prices[0].date, 0)
            .unwrap()
            .naive_utc();
        let dt1 = chrono::DateTime::from_timestamp(prices[1].date, 0)
            .unwrap()
            .naive_utc();
        assert_eq!(dt0.format("%H:%M:%S").to_string(), "15:59:00");
        assert_eq!(dt1.format("%H:%M:%S").to_string(), "15:59:00");
    }

    #[test]
    fn underlying_prices_resample_daily_reduces_to_one_per_date() {
        let tmp = write_intraday_parquet();
        let prices = load_underlying_prices(
            tmp.path(),
            None,
            Some(crate::engine::types::Interval::Daily),
            None,
        );
        // 8 intraday bars across 2 dates → 2 daily bars
        assert_eq!(prices.len(), 2, "should have one bar per date");
        // Daily bars should have date-only format (no time component)
        // Daily bars should have midnight epoch (no time component)
        let dt0 = chrono::DateTime::from_timestamp(prices[0].date, 0)
            .unwrap()
            .naive_utc();
        assert_eq!(
            dt0.format("%H:%M:%S").to_string(),
            "00:00:00",
            "daily should be midnight epoch"
        );
    }

    #[test]
    fn underlying_prices_filter_with_no_matches_returns_empty() {
        let tmp = write_intraday_parquet();

        // Filter with a timestamp that doesn't exist in the data
        let filter_dts = vec![NaiveDate::from_ymd_opt(2099, 1, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()];
        let filter_col: Column = Series::new("datetime".into(), &filter_dts).into();

        let prices = load_underlying_prices(tmp.path(), Some(&filter_col), None, None);
        assert!(prices.is_empty());
    }
}
