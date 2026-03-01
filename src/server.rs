use garde::Validate;
use polars::prelude::*;
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Json},
    model::{Implementation, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router, ServerHandler,
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::data::cache::CachedStore;
use crate::data::eodhd::EodhdProvider;
use crate::engine::types::{
    BacktestParams, Commission, CompareEntry, CompareParams, EvaluateParams, SimParams, Slippage,
    TargetRange, TradeSelector,
};
use crate::signals::registry::SignalSpec;

fn validate_exit_dte_lt_max_dte(
    max_entry_dte: &i32,
) -> impl FnOnce(&i32, &()) -> garde::Result + '_ {
    move |exit_dte: &i32, (): &()| {
        if exit_dte >= max_entry_dte {
            return Err(garde::Error::new(format!(
                "exit_dte ({exit_dte}) must be less than max_entry_dte ({max_entry_dte})"
            )));
        }
        Ok(())
    }
}
use crate::tools;
use crate::tools::response_types::{
    BacktestResponse, CheckCacheResponse, CompareResponse, ConstructSignalResponse,
    DownloadResponse, EvaluateResponse, FetchResponse, LoadDataResponse, StrategiesResponse,
    SuggestResponse,
};
use crate::tools::signals::SignalsResponse;

/// Loaded data: (symbol, `DataFrame`) tuple so we can auto-resolve OHLCV paths.
type LoadedData = Option<(String, DataFrame)>;

#[derive(Clone)]
pub struct OptopsyServer {
    pub data: Arc<RwLock<LoadedData>>,
    pub cache: Arc<CachedStore>,
    pub eodhd: Option<Arc<EodhdProvider>>,
    tool_router: ToolRouter<Self>,
}

impl OptopsyServer {
    pub fn new(cache: Arc<CachedStore>, eodhd: Option<Arc<EodhdProvider>>) -> Self {
        Self {
            data: Arc::new(RwLock::new(None)),
            cache,
            eodhd,
            tool_router: Self::tool_router(),
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct DownloadOptionsParams {
    /// US stock ticker symbol (e.g. "SPY", "AAPL", "TSLA")
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct LoadDataParams {
    /// Ticker symbol (e.g. "SPY")
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Start date filter (YYYY-MM-DD)
    #[garde(inner(pattern(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")))]
    pub start_date: Option<String>,
    /// End date filter (YYYY-MM-DD)
    #[garde(inner(pattern(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")))]
    pub end_date: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct EvaluateStrategyParams {
    /// Strategy name (e.g. '`iron_condor`')
    #[garde(length(min = 1))]
    pub strategy: String,
    /// Per-leg delta targets
    #[garde(length(min = 1), dive)]
    pub leg_deltas: Vec<TargetRange>,
    /// Maximum DTE at entry
    #[garde(range(min = 1))]
    pub max_entry_dte: i32,
    /// DTE at exit
    #[garde(range(min = 0), custom(validate_exit_dte_lt_max_dte(&self.max_entry_dte)))]
    pub exit_dte: i32,
    /// DTE bucket width (e.g. 7)
    #[garde(range(min = 1))]
    pub dte_interval: i32,
    /// Delta bucket width (e.g. 0.05)
    #[garde(range(min = 0.001, max = 1.0))]
    pub delta_interval: f64,
    /// Slippage model
    #[garde(dive)]
    pub slippage: Slippage,
    /// Commission structure (optional)
    #[garde(dive)]
    pub commission: Option<Commission>,
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct RunBacktestParams {
    /// Strategy name
    #[garde(length(min = 1))]
    pub strategy: String,
    /// Per-leg delta targets
    #[garde(length(min = 1), dive)]
    pub leg_deltas: Vec<TargetRange>,
    /// Maximum DTE at entry
    #[garde(range(min = 1))]
    pub max_entry_dte: i32,
    /// DTE at exit
    #[garde(range(min = 0), custom(validate_exit_dte_lt_max_dte(&self.max_entry_dte)))]
    pub exit_dte: i32,
    /// Slippage model
    #[garde(dive)]
    pub slippage: Slippage,
    /// Commission structure
    #[garde(dive)]
    pub commission: Option<Commission>,
    /// Stop loss threshold (multiplier of entry cost; values > 1.0 allowed)
    #[garde(inner(range(min = 0.0)))]
    pub stop_loss: Option<f64>,
    /// Take profit threshold (multiplier of entry cost; values > 1.0 allowed)
    #[garde(inner(range(min = 0.0)))]
    pub take_profit: Option<f64>,
    /// Maximum days to hold
    #[garde(inner(range(min = 1)))]
    pub max_hold_days: Option<i32>,
    /// Starting capital
    #[garde(range(min = 0.01))]
    pub capital: f64,
    /// Number of contracts per trade
    #[garde(range(min = 1))]
    pub quantity: i32,
    /// Contract multiplier (default 100)
    #[garde(inner(range(min = 1)))]
    pub multiplier: Option<i32>,
    /// Maximum concurrent positions
    #[garde(range(min = 1))]
    pub max_positions: i32,
    /// Trade selection method
    #[garde(skip)]
    pub selector: Option<TradeSelector>,
    /// Entry signal — only open trades on dates where this TA signal fires.
    /// Requires OHLCV data (call `fetch_to_parquet` first).
    #[garde(skip)]
    pub entry_signal: Option<SignalSpec>,
    /// Exit signal — close open positions on dates where this TA signal fires.
    /// Requires OHLCV data (call `fetch_to_parquet` first).
    #[garde(skip)]
    pub exit_signal: Option<SignalSpec>,
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct CompareStrategiesParams {
    /// List of strategies with their parameters
    #[garde(length(min = 2), dive)]
    pub strategies: Vec<CompareEntry>,
    /// Shared simulation parameters
    #[garde(dive)]
    pub sim_params: SimParams,
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct CheckCacheParams {
    /// Ticker symbol (e.g. "SPY")
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Cache category subdirectory (e.g. "prices", "options")
    #[garde(length(min = 1), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub category: String,
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct ConstructSignalParams {
    /// Natural language description e.g. "RSI oversold" or "MACD bullish and above 50-day SMA"
    /// Must contain at least one non-whitespace character.
    #[garde(length(min = 1, max = 500), pattern(r"[^ \t\n\r]"))]
    pub prompt: String,
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct FetchToParquetParams {
    /// Ticker symbol (e.g. "SPY")
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Cache category subdirectory (e.g. "prices")
    #[garde(length(min = 1), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub category: String,
    /// Time period to fetch (e.g. "6mo", "1y", "5y", "max"). Defaults to "6mo".
    #[garde(inner(length(min = 1)))]
    pub period: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema, Validate)]
#[serde(rename_all = "lowercase")]
pub enum RiskPreferenceParam {
    Conservative,
    Moderate,
    Aggressive,
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct SuggestParametersParams {
    /// Strategy name (e.g. '`iron_condor`')
    #[garde(length(min = 1))]
    pub strategy: String,
    /// Risk preference: conservative (tight filters), moderate (balanced), or aggressive (loose filters)
    #[garde(skip)]
    pub risk_preference: RiskPreferenceParam,
    /// Target win rate (0.0-1.0), informational only
    #[garde(inner(range(min = 0.0, max = 1.0)))]
    pub target_win_rate: Option<f64>,
    /// Target Sharpe ratio, informational only
    #[garde(skip)]
    pub target_sharpe: Option<f64>,
}

use rmcp::handler::server::wrapper::Parameters;

#[tool_router]
impl OptopsyServer {
    /// Bulk download options data from EODHD API (~2 years historical coverage).
    ///
    /// **Workflow Phase**: 0 (optional, before load_data)
    /// **When to use**: Proactively download data before analysis, or to refresh cache
    /// **Prerequisites**: EODHD_API_KEY environment variable must be set
    /// **Next tool**: load_data (will use cached data automatically)
    ///
    /// Downloads calls + puts across weekly/monthly expirations and caches locally.
    /// Resumable — re-run to extend cache with only new data.
    /// For single ad-hoc loads, just call load_data directly (auto-fetches if needed).
    #[tool(name = "download_options_data")]
    async fn download_options_data(
        &self,
        Parameters(params): Parameters<DownloadOptionsParams>,
    ) -> Result<Json<DownloadResponse>, String> {
        params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;
        tools::download::execute(self.eodhd.as_ref(), &params.symbol)
            .await
            .map(Json)
            .map_err(|e| format!("Error: {e}"))
    }

    /// Load options chain data by symbol. **START HERE for any new analysis.**
    ///
    /// **Workflow Phase**: 1/7 (entry point)
    /// **When to use**: Always run this first; all other tools require data to be loaded
    /// **Prerequisites**: None
    /// **Data sources** (in priority order):
    ///   1. Local Parquet cache (~/.optopsy/cache/options/{SYMBOL}.parquet)
    ///   2. EODHD API (if EODHD_API_KEY set) — auto-downloads & caches
    ///   3. S3-compatible storage (if S3 credentials configured)
    /// **Next tools**:
    ///   - list_strategies() or list_signals() (explore available options)
    ///   - suggest_parameters() (get data-driven parameter recommendations)
    ///   - evaluate_strategy() (fast statistical screening)
    ///
    /// Automatically handles date column normalization (quote_date/data_date/quote_datetime).
    /// Optional date filtering via start_date/end_date.
    #[tool(name = "load_data")]
    async fn load_data(
        &self,
        Parameters(params): Parameters<LoadDataParams>,
    ) -> Result<Json<LoadDataResponse>, String> {
        params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;
        let symbol = params.symbol.clone();
        tools::load_data::execute(
            &self.data,
            &self.cache,
            self.eodhd.as_ref(),
            &symbol,
            params.start_date.as_deref(),
            params.end_date.as_deref(),
        )
        .await
        .map(Json)
        .map_err(|e| format!("Error: {e}"))
    }

    /// Browse all 32 built-in options strategies grouped by category.
    ///
    /// **Workflow Phase**: 2a/7 (exploration)
    /// **When to use**: After load_data, to choose a strategy for analysis
    /// **Prerequisites**: None (informational, no data required)
    /// **Categories**: singles, spreads, straddles, strangles, butterflies, condors, iron, calendars, diagonals
    /// **Next tools**: suggest_parameters() or evaluate_strategy() (once you pick a strategy)
    #[tool(name = "list_strategies")]
    async fn list_strategies(&self) -> Json<StrategiesResponse> {
        Json(tools::strategies::execute())
    }

    /// Browse all 40+ available technical analysis (TA) signals for entry/exit filtering.
    ///
    /// **Workflow Phase**: 2b/7 (exploration)
    /// **When to use**: After list_strategies, to understand available signal options for filtering
    /// **Prerequisites**: None (informational, no data required)
    /// **Categories**: momentum (RSI, MACD, Stoch), trend (SMA, EMA, ADX),
    ///   volatility (BBands, ATR), overlap, price, volume
    /// **Next tool**: construct_signal() (if you want to use signals in backtest)
    /// **Note**: Signals are optional — only needed if you want signal-filtered entry/exit
    #[tool(name = "list_signals")]
    async fn list_signals(&self) -> Json<SignalsResponse> {
        Json(tools::signals::execute())
    }

    /// Construct a signal specification from natural language.
    ///
    /// **Workflow Phase**: 2c/7 (signal design, optional)
    /// **When to use**: If you want to filter backtests by TA signals (e.g., "RSI oversold")
    /// **Prerequisites**: fetch_to_parquet() must have been called first (to load OHLCV data)
    /// **How it works**:
    ///   - Fuzzy-searches signal catalog for matches
    ///   - Returns candidate signals with sensible defaults
    ///   - Generates live JSON schema for all signal variants
    /// **Next tool**: run_backtest() with entry_signal/exit_signal parameters set to
    ///   the JSON spec from this tool's response
    /// **Example usage**: "RSI oversold" → returns RSI signal spec with threshold=30
    /// **Note**: Signals are optional; run_backtest works without them
    #[tool(name = "construct_signal")]
    async fn construct_signal(
        &self,
        Parameters(params): Parameters<ConstructSignalParams>,
    ) -> Result<Json<ConstructSignalResponse>, String> {
        params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;
        Ok(Json(tools::construct_signal::execute(&params.prompt)))
    }

    /// Fast statistical screening without capital simulation.
    ///
    /// **Workflow Phase**: 4/7 (statistical validation)
    /// **When to use**: Before run_backtest, to validate strategy parameters and identify
    ///   promising DTE/delta ranges from historical data
    /// **Prerequisites**: load_data() must have been called first
    /// **Why use this**: Avoid wasting time on backtest simulations with poor parameter choices;
    ///   groups historical P&L by DTE × delta buckets to find winners
    /// **Next tool**: run_backtest() with parameters refined from results
    ///
    /// Returns: Best/worst buckets, win rates, profit factors, and full DTE×delta grid stats
    /// **Time to run**: Fast (seconds)
    /// **Output includes**:
    ///   - Best performer bucket (highest PnL)
    ///   - Worst bucket (warning sign)
    ///   - Highest win-rate bucket (consistency indicator)
    ///   - Full bucket grid with mean, std, quartiles, count
    #[tool(name = "evaluate_strategy")]
    async fn evaluate_strategy(
        &self,
        Parameters(params): Parameters<EvaluateStrategyParams>,
    ) -> Result<Json<EvaluateResponse>, String> {
        params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;
        let data = self.data.read().await;
        let Some((_, df)) = data.as_ref() else {
            return Err("Error: No data loaded. Call load_data first.".to_string());
        };

        let eval_params = EvaluateParams {
            strategy: params.strategy,
            leg_deltas: params.leg_deltas,
            max_entry_dte: params.max_entry_dte,
            exit_dte: params.exit_dte,
            dte_interval: params.dte_interval,
            delta_interval: params.delta_interval,
            slippage: params.slippage,
            commission: params.commission,
        };
        eval_params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;

        tools::evaluate::execute(df, &eval_params)
            .map(Json)
            .map_err(|e| format!("Error: {e}"))
    }

    /// Full event-driven day-by-day simulation with position management and metrics.
    ///
    /// **Workflow Phase**: 5/7 (full validation)
    /// **When to use**: After evaluate_strategy() to validate strategy performance in capital-constrained scenario
    /// **Prerequisites**:
    ///   - load_data() must have been called
    ///   - evaluate_strategy() recommended (not required, but avoids bad parameter choices)
    ///   - fetch_to_parquet() required ONLY if using entry_signal or exit_signal
    /// **⚠️  Warning**: Slow! Run evaluate_strategy() first to validate parameters
    /// **Next tools**: compare_strategies() (to test variations) or iterate on parameters
    ///
    /// **What it simulates**:
    ///   - Day-by-day position opens (respecting max_positions constraint)
    ///   - Position management (stop loss, take profit, max hold days, DTE exit)
    ///   - Optional signal-based filtering (if entry_signal/exit_signal provided)
    ///   - Realistic P&L with bid/ask slippage and commissions
    /// **Output**:
    ///   - Trade log (every open/close with P&L and exit reason)
    ///   - Equity curve (daily capital evolution)
    ///   - Performance metrics (Sharpe, Sortino, Calmar, VaR, max drawdown, win rate, etc.)
    ///   - AI-enriched assessment and suggested next steps
    /// **Time to run**: 5-30 seconds depending on data size
    #[tool(name = "run_backtest")]
    async fn run_backtest(
        &self,
        Parameters(params): Parameters<RunBacktestParams>,
    ) -> Result<Json<BacktestResponse>, String> {
        params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;
        let data = self.data.read().await;
        let Some((symbol, df)) = data.as_ref() else {
            return Err("Error: No data loaded. Call load_data first.".to_string());
        };

        // Auto-resolve OHLCV path if signals are requested
        let ohlcv_path = if params.entry_signal.is_some() || params.exit_signal.is_some() {
            let path = self
                .cache
                .cache_path(symbol, "prices")
                .map_err(|e| format!("Error resolving OHLCV path: {e}"))?;
            if !path.exists() {
                return Err(format!(
                    "OHLCV data not found for {symbol}. Call fetch_to_parquet({{ symbol: \"{symbol}\", category: \"prices\" }}) first."
                ));
            }
            Some(path.to_string_lossy().to_string())
        } else {
            None
        };

        let backtest_params = BacktestParams {
            strategy: params.strategy,
            leg_deltas: params.leg_deltas,
            max_entry_dte: params.max_entry_dte,
            exit_dte: params.exit_dte,
            slippage: params.slippage,
            commission: params.commission,
            stop_loss: params.stop_loss,
            take_profit: params.take_profit,
            max_hold_days: params.max_hold_days,
            capital: params.capital,
            quantity: params.quantity,
            multiplier: params.multiplier.unwrap_or(100),
            max_positions: params.max_positions,
            selector: params.selector.unwrap_or_default(),
            adjustment_rules: vec![],
            entry_signal: params.entry_signal,
            exit_signal: params.exit_signal,
            ohlcv_path,
        };
        backtest_params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;

        tools::backtest::execute(df, &backtest_params)
            .map(Json)
            .map_err(|e| format!("Error: {e}"))
    }

    /// Run multiple strategies in parallel and rank by performance metrics.
    ///
    /// **Workflow Phase**: 6/7 (comparison & optimization)
    /// **When to use**: After validating one strategy via run_backtest(), to test
    ///   parameter variations and find the best-performing approach
    /// **Prerequisites**: load_data() must have been called
    /// **Why use this**: Compare different delta targets, DTE parameters, or strategies
    ///   side-by-side in a single call (faster than running multiple backtests)
    /// **Next tools**: pick best performer and iterate further, or conclude analysis
    ///
    /// **Modes**:
    ///   - Compare DTE/delta variations of same strategy
    ///   - Compare different strategies with same parameters
    ///   - Compare hybrid parameter sets
    /// **Rankings**: By Sharpe ratio (primary) and total PnL (secondary)
    /// **Output**: Metrics for each strategy + recommended best performer
    #[tool(name = "compare_strategies")]
    async fn compare_strategies(
        &self,
        Parameters(params): Parameters<CompareStrategiesParams>,
    ) -> Result<Json<CompareResponse>, String> {
        params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;
        let data = self.data.read().await;
        let Some((_, df)) = data.as_ref() else {
            return Err("Error: No data loaded. Call load_data first.".to_string());
        };

        let compare_params = CompareParams {
            strategies: params.strategies,
            sim_params: params.sim_params,
        };
        compare_params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;

        tools::compare::execute(df, &compare_params)
            .map(Json)
            .map_err(|e| format!("Error: {e}"))
    }

    /// Check if cached Parquet data exists and when it was last updated.
    ///
    /// **Workflow Phase**: 0 (optional, before load_data/fetch_to_parquet)
    /// **When to use**: To avoid redundant downloads or to verify data staleness
    /// **Prerequisites**: None
    /// **Next tools**: load_data or fetch_to_parquet (if cache is missing/stale)
    ///
    /// **Returns**:
    ///   - Cache exists (boolean)
    ///   - File path (if exists)
    ///   - File size and row count
    ///   - Last update timestamp
    /// **Use case**: Before calling load_data/fetch_to_parquet, check if you need
    ///   to download fresh data or if cache is recent enough
    #[tool(name = "check_cache_status")]
    async fn check_cache_status(
        &self,
        Parameters(params): Parameters<CheckCacheParams>,
    ) -> Result<Json<CheckCacheResponse>, String> {
        params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;
        tools::cache_status::execute(&self.cache, &params.symbol, &params.category)
            .map(Json)
            .map_err(|e| format!("Error: {e}"))
    }

    /// Download OHLCV price data from Yahoo Finance and cache locally as Parquet.
    ///
    /// **Workflow Phase**: 0b (optional, before run_backtest with signals)
    /// **When to use**: ONLY if you want to use entry_signal or exit_signal in run_backtest
    /// **Prerequisites**: None
    /// **Next tool**: run_backtest with entry_signal/exit_signal parameters
    ///
    /// **Why separate from load_data**:
    ///   - load_data() loads options chain data (bid/ask/delta)
    ///   - fetch_to_parquet() loads price bars (OHLCV) for TA indicators
    ///   - Both can be loaded simultaneously
    /// **Categories**: Use "prices" for standard price data
    /// **Periods**: "6mo" (default), "1y", "5y", "max"
    /// **Performance**: Downloads to ~/.optopsy/cache/prices/{SYMBOL}.parquet
    ///
    /// **Important**: Not needed for basic backtest (no signals).
    ///   Only load if using construct_signal → enter signal JSON into run_backtest.
    #[tool(name = "fetch_to_parquet")]
    async fn fetch_to_parquet(
        &self,
        Parameters(params): Parameters<FetchToParquetParams>,
    ) -> Result<Json<FetchResponse>, String> {
        params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;
        let period = params.period.as_deref().unwrap_or("6mo");
        tools::fetch::execute(&self.cache, &params.symbol, &params.category, period)
            .await
            .map(Json)
            .map_err(|e| format!("Error: {e}"))
    }

    /// Analyze the loaded options chain and suggest data-driven parameters.
    ///
    /// **Workflow Phase**: 3/7 (parameter optimization)
    /// **When to use**: After load_data(), to get intelligent parameter suggestions
    ///   based on actual market data (DTE coverage, spread quality, delta distribution)
    /// **Prerequisites**: load_data() must have been called first
    /// **Next tools**: evaluate_strategy() or run_backtest() with suggested parameters
    ///
    /// **What it analyzes**:
    ///   - DTE distribution and contiguous coverage zones
    ///   - Bid/ask spread quality per DTE bucket
    ///   - Delta distribution per leg (quartile-based targeting)
    ///   - Suggested exit_dte based on data coverage
    /// **Risk preferences**: Conservative (tight filters), Moderate (balanced), Aggressive (loose)
    /// **Output**:
    ///   - leg_deltas array (optimized delta targets/ranges per leg)
    ///   - max_entry_dte (maximum viable entry DTE from data)
    ///   - exit_dte (recommended exit DTE)
    ///   - slippage model recommendation (Mid/Spread/Liquidity)
    ///   - Confidence score (combines data coverage and calendar quality)
    /// **Saves time**: No need to guess parameters; use market-driven recommendations
    #[tool(name = "suggest_parameters")]
    async fn suggest_parameters(
        &self,
        Parameters(params): Parameters<SuggestParametersParams>,
    ) -> Result<Json<SuggestResponse>, String> {
        params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;

        let risk_pref = match params.risk_preference {
            RiskPreferenceParam::Conservative => {
                crate::engine::suggest::RiskPreference::Conservative
            }
            RiskPreferenceParam::Moderate => crate::engine::suggest::RiskPreference::Moderate,
            RiskPreferenceParam::Aggressive => crate::engine::suggest::RiskPreference::Aggressive,
        };

        let suggest_params = crate::engine::suggest::SuggestParams {
            strategy: params.strategy,
            risk_preference: risk_pref,
            target_win_rate: params.target_win_rate,
            target_sharpe: params.target_sharpe,
        };

        let data = self.data.read().await;
        let Some((_, df)) = data.as_ref() else {
            return Err("Error: No data loaded. Call load_data first.".to_string());
        };

        tools::suggest::execute(df, &suggest_params)
            .map(Json)
            .map_err(|e| format!("Error: {e}"))
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
                title: None,
                version: "0.1.0".into(),
                description: None,
                icons: None,
                website_url: None,
            },
            instructions: Some(
                "Options backtesting engine. \
                \n\nRecommended exploration workflow:\
                \n0a. check_cache_status({ symbol, category }) — check if cached data \
                exists for a symbol and when it was last updated. Call this before \
                fetch_to_parquet to avoid re-downloading data that is already available.\
                \n0b. fetch_to_parquet({ symbol, category, period? }) — fetch historical \
                OHLCV data from Yahoo Finance and write it to a local Parquet file. \
                Only needed if check_cache_status shows the data is missing or stale. \
                Note: the resulting Parquet file is for OHLCV price data and is separate from \
                the options chain loaded by load_data.\
                \n1. load_data({ symbol }) — load (or auto-fetch) a symbol's options chain. \
                If the data is not cached locally and EODHD_API_KEY is set, it will \
                automatically download from EODHD. You can also use download_options_data \
                to explicitly download data first.\
                \n2. list_strategies() — browse all built-in strategies grouped by category \
                (singles, spreads, butterflies, condors, iron, calendars).\
                \n3. list_signals() — browse all available TA signals grouped by category \
                (momentum, overlap, trend, volatility, price, volume). Signals can be used as \
                entry_signal and exit_signal in run_backtest.\
                \n4. evaluate_strategy({ strategy, leg_deltas, max_entry_dte, exit_dte, \
                dte_interval, delta_interval, slippage }) — fast statistical screen that \
                groups historical trades into DTE × delta buckets and returns mean P&L, \
                win rate, profit factor, and distribution stats per bucket. \
                Use this to identify promising parameter ranges before committing to a full simulation.\
                \n5. run_backtest({ strategy, leg_deltas, ..., capital, quantity, max_positions, \
                entry_signal?, exit_signal? }) \
                — event-driven day-by-day simulation with position management (stop loss, take profit, \
                max hold, DTE exit, signal exit), equity curve, and full performance metrics \
                (Sharpe, Sortino, Calmar, VaR, CAGR, expectancy). \
                Optional: pass entry_signal to filter entries to days where a TA condition fires, \
                and/or exit_signal to trigger early exits. Signals require OHLCV data — call \
                fetch_to_parquet({ symbol, category: \"prices\" }) first.\
                \n6. compare_strategies({ strategies: [...], sim_params }) — run the same backtest \
                pipeline for multiple strategies in parallel and rank them by Sharpe and total P&L.\
                \n\nData flow summary: EODHD API → local Parquet cache → DataFrame → per-leg \
                filter/delta-select → leg join → strike-order validation → P&L calculation → \
                bucket aggregation (evaluate) or event-loop simulation (backtest) → \
                AI-enriched JSON response."
                    .into(),
            ),
        }
    }
}
