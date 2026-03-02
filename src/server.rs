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
use crate::engine::types::{
    default_delta_interval, default_dte_interval, default_multiplier, validate_exit_dte_lt_max,
};
use crate::signals::registry::SignalSpec;
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

fn default_max_entry_dte() -> i32 {
    45
}

fn default_exit_dte() -> i32 {
    9
}

fn default_max_positions() -> i32 {
    1
}

fn default_quantity() -> i32 {
    1
}

fn default_capital() -> f64 {
    10000.0
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct EvaluateStrategyParams {
    /// Strategy name
    #[garde(skip)]
    pub strategy: StrategyParam,
    /// Per-leg delta targets
    #[garde(length(min = 1), dive)]
    pub leg_deltas: Vec<TargetRange>,
    /// Maximum DTE at entry (default: 45)
    #[serde(default = "default_max_entry_dte")]
    #[garde(range(min = 1))]
    pub max_entry_dte: i32,
    /// DTE at exit (default: 9)
    #[serde(default = "default_exit_dte")]
    #[garde(range(min = 0), custom(validate_exit_dte_lt_max(&self.max_entry_dte)))]
    pub exit_dte: i32,
    /// DTE bucket width (default: 5)
    #[serde(default = "default_dte_interval")]
    #[garde(range(min = 1))]
    pub dte_interval: i32,
    /// Delta bucket width (default: 0.10)
    #[serde(default = "default_delta_interval")]
    #[garde(range(min = 0.001, max = 1.0))]
    pub delta_interval: f64,
    /// Slippage model (default: Spread)
    #[serde(default)]
    #[garde(dive)]
    pub slippage: Slippage,
    /// Commission structure (optional)
    #[serde(default)]
    #[garde(dive)]
    pub commission: Option<Commission>,
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct RunBacktestParams {
    /// Strategy name
    #[garde(skip)]
    pub strategy: StrategyParam,
    /// Per-leg delta targets
    #[garde(length(min = 1), dive)]
    pub leg_deltas: Vec<TargetRange>,
    /// Maximum DTE at entry (default: 45)
    #[serde(default = "default_max_entry_dte")]
    #[garde(range(min = 1))]
    pub max_entry_dte: i32,
    /// DTE at exit (default: 9)
    #[serde(default = "default_exit_dte")]
    #[garde(range(min = 0), custom(validate_exit_dte_lt_max(&self.max_entry_dte)))]
    pub exit_dte: i32,
    /// Slippage model (default: Spread)
    #[serde(default)]
    #[garde(dive)]
    pub slippage: Slippage,
    /// Commission structure
    #[serde(default)]
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
    /// Starting capital (default: 10000)
    #[serde(default = "default_capital")]
    #[garde(range(min = 0.01))]
    pub capital: f64,
    /// Number of contracts per trade (default: 1)
    #[serde(default = "default_quantity")]
    #[garde(range(min = 1))]
    pub quantity: i32,
    /// Contract multiplier (default: 100)
    #[serde(default = "default_multiplier")]
    #[garde(range(min = 1))]
    pub multiplier: i32,
    /// Maximum concurrent positions (default: 1)
    #[serde(default = "default_max_positions")]
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

#[derive(Debug, Clone, Deserialize, JsonSchema, Validate)]
pub struct ServerCompareEntry {
    /// Strategy name
    #[garde(skip)]
    pub name: StrategyParam,
    /// Per-leg delta targets
    #[garde(length(min = 1), dive)]
    pub leg_deltas: Vec<TargetRange>,
    /// Maximum DTE at entry (default: 45)
    #[serde(default = "default_max_entry_dte")]
    #[garde(range(min = 1))]
    pub max_entry_dte: i32,
    /// DTE at exit (default: 9)
    #[serde(default = "default_exit_dte")]
    #[garde(range(min = 0), custom(validate_exit_dte_lt_max(&self.max_entry_dte)))]
    pub exit_dte: i32,
    /// Slippage model (default: Spread)
    #[serde(default)]
    #[garde(dive)]
    pub slippage: Slippage,
    /// Commission structure
    #[serde(default)]
    #[garde(dive)]
    pub commission: Option<Commission>,
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct CompareStrategiesParams {
    /// List of strategies with their parameters
    #[garde(length(min = 2), dive)]
    pub strategies: Vec<ServerCompareEntry>,
    /// Shared simulation parameters
    #[garde(dive)]
    pub sim_params: SimParams,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema, Validate)]
#[serde(rename_all = "lowercase")]
pub enum CategoryParam {
    Prices,
    Options,
}

impl CategoryParam {
    /// Convert enum variant to lowercase string for data layer
    pub fn as_str(&self) -> &'static str {
        match self {
            CategoryParam::Prices => "prices",
            CategoryParam::Options => "options",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct CheckCacheParams {
    /// Ticker symbol (e.g. "SPY")
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Cache category
    #[garde(skip)]
    pub category: CategoryParam,
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
    /// Cache category
    #[garde(skip)]
    pub category: CategoryParam,
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

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub enum StrategyParam {
    // Singles
    LongCall,
    ShortCall,
    LongPut,
    ShortPut,
    CoveredCall,
    CashSecuredPut,
    // Spreads
    BullCallSpread,
    BearCallSpread,
    BullPutSpread,
    BearPutSpread,
    LongStraddle,
    ShortStraddle,
    LongStrangle,
    ShortStrangle,
    // Butterflies
    LongCallButterfly,
    ShortCallButterfly,
    LongPutButterfly,
    ShortPutButterfly,
    // Condors
    LongCallCondor,
    ShortCallCondor,
    LongPutCondor,
    ShortPutCondor,
    // Iron
    IronCondor,
    ReverseIronCondor,
    IronButterfly,
    ReverseIronButterfly,
    // Calendar
    CallCalendarSpread,
    PutCalendarSpread,
    CallDiagonalSpread,
    PutDiagonalSpread,
    DoubleCalendar,
    DoubleDiagonal,
}

impl StrategyParam {
    /// Convert enum variant to `snake_case` string for engine
    pub fn as_str(&self) -> &'static str {
        match self {
            StrategyParam::LongCall => "long_call",
            StrategyParam::ShortCall => "short_call",
            StrategyParam::LongPut => "long_put",
            StrategyParam::ShortPut => "short_put",
            StrategyParam::CoveredCall => "covered_call",
            StrategyParam::CashSecuredPut => "cash_secured_put",
            StrategyParam::BullCallSpread => "bull_call_spread",
            StrategyParam::BearCallSpread => "bear_call_spread",
            StrategyParam::BullPutSpread => "bull_put_spread",
            StrategyParam::BearPutSpread => "bear_put_spread",
            StrategyParam::LongStraddle => "long_straddle",
            StrategyParam::ShortStraddle => "short_straddle",
            StrategyParam::LongStrangle => "long_strangle",
            StrategyParam::ShortStrangle => "short_strangle",
            StrategyParam::LongCallButterfly => "long_call_butterfly",
            StrategyParam::ShortCallButterfly => "short_call_butterfly",
            StrategyParam::LongPutButterfly => "long_put_butterfly",
            StrategyParam::ShortPutButterfly => "short_put_butterfly",
            StrategyParam::LongCallCondor => "long_call_condor",
            StrategyParam::ShortCallCondor => "short_call_condor",
            StrategyParam::LongPutCondor => "long_put_condor",
            StrategyParam::ShortPutCondor => "short_put_condor",
            StrategyParam::IronCondor => "iron_condor",
            StrategyParam::ReverseIronCondor => "reverse_iron_condor",
            StrategyParam::IronButterfly => "iron_butterfly",
            StrategyParam::ReverseIronButterfly => "reverse_iron_butterfly",
            StrategyParam::CallCalendarSpread => "call_calendar_spread",
            StrategyParam::PutCalendarSpread => "put_calendar_spread",
            StrategyParam::CallDiagonalSpread => "call_diagonal_spread",
            StrategyParam::PutDiagonalSpread => "put_diagonal_spread",
            StrategyParam::DoubleCalendar => "double_calendar",
            StrategyParam::DoubleDiagonal => "double_diagonal",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct SuggestParametersParams {
    /// Strategy name
    #[garde(skip)]
    pub strategy: StrategyParam,
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
    /// **Workflow Phase**: 0 (optional, before `load_data`)
    /// **When to use**: Proactively download data before analysis, or to refresh cache
    /// **Prerequisites**: `EODHD_API_KEY` environment variable must be set
    /// **Next tool**: `load_data` (will use cached data automatically)
    ///
    /// Downloads calls + puts across weekly/monthly expirations and caches locally.
    /// Resumable — re-run to extend cache with only new data.
    /// For single ad-hoc loads, just call `load_data` directly (auto-fetches if needed).
    #[tool(
        name = "download_options_data",
        annotations(
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
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
    ///   2. EODHD API (if `EODHD_API_KEY` set) — auto-downloads & caches
    ///   3. S3-compatible storage (if S3 credentials configured)
    /// **Next tools**:
    ///   - `list_strategies()` or `list_signals()` (explore available options)
    ///   - `suggest_parameters()` (get data-driven parameter recommendations)
    ///   - `evaluate_strategy()` (fast statistical screening)
    ///
    /// Automatically handles date column normalization (`quote_date`/`data_date`/`quote_datetime`).
    /// Optional date filtering via `start_date`/`end_date`.
    #[tool(
        name = "load_data",
        annotations(
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
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
    /// **When to use**: After `load_data`, to choose a strategy for analysis
    /// **Prerequisites**: None (informational, no data required)
    /// **Categories**: singles, spreads, straddles, strangles, butterflies, condors, iron, calendars, diagonals
    /// **Next tools**: `suggest_parameters()` or `evaluate_strategy()` (once you pick a strategy)
    #[tool(name = "list_strategies", annotations(read_only_hint = true))]
    async fn list_strategies(&self) -> Json<StrategiesResponse> {
        Json(tools::strategies::execute())
    }

    /// Browse all 40+ available technical analysis (TA) signals for entry/exit filtering.
    ///
    /// **Workflow Phase**: 2b/7 (exploration)
    /// **When to use**: After `list_strategies`, to understand available signal options for filtering
    /// **Prerequisites**: None (informational, no data required)
    /// **Categories**: momentum (RSI, MACD, Stoch), trend (SMA, EMA, ADX),
    ///   volatility (`BBands`, `ATR`), overlap, price, volume
    /// **Next tool**: `construct_signal()` (if you want to use signals in backtest)
    /// **Note**: Signals are optional — only needed if you want signal-filtered entry/exit
    #[tool(name = "list_signals", annotations(read_only_hint = true))]
    async fn list_signals(&self) -> Json<SignalsResponse> {
        Json(tools::signals::execute())
    }

    /// Construct a signal specification from natural language.
    ///
    /// **Workflow Phase**: 2c/7 (signal design, optional)
    /// **When to use**: If you want to filter backtests by TA signals (e.g., "RSI oversold")
    /// **Prerequisites**: `fetch_to_parquet()` must have been called first (to load OHLCV data)
    /// **How it works**:
    ///   - Fuzzy-searches signal catalog for matches
    ///   - Returns candidate signals with sensible defaults
    ///   - Generates live JSON schema for all signal variants
    /// **Next tool**: `run_backtest()` with `entry_signal`/`exit_signal` parameters set to
    ///   the JSON spec from this tool's response
    /// **Example usage**: "RSI oversold" → returns RSI signal spec with threshold=30
    /// **Note**: Signals are optional; `run_backtest` works without them
    #[tool(name = "construct_signal", annotations(read_only_hint = true))]
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
    /// **When to use**: Before `run_backtest`, to validate strategy parameters and identify
    ///   promising DTE/delta ranges from historical data
    /// **Prerequisites**: `load_data()` must have been called first
    /// **Why use this**: Avoid wasting time on backtest simulations with poor parameter choices;
    ///   groups historical P&L by DTE × delta buckets to find winners
    /// **Next tool**: `run_backtest()` with parameters refined from results
    ///
    /// Returns: Best/worst buckets, win rates, profit factors, and full DTE×delta grid stats
    /// **Time to run**: Fast (seconds)
    /// **Output includes**:
    ///   - Best performer bucket (highest `PnL`)
    ///   - Worst bucket (warning sign)
    ///   - Highest win-rate bucket (consistency indicator)
    ///   - Full bucket grid with mean, std, quartiles, count
    #[tool(name = "evaluate_strategy", annotations(read_only_hint = true))]
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
            strategy: params.strategy.as_str().to_string(),
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
    /// **When to use**: After `evaluate_strategy()` to validate strategy performance in capital-constrained scenario
    /// **Prerequisites**:
    ///   - `load_data()` must have been called
    ///   - `evaluate_strategy()` recommended (not required, but avoids bad parameter choices)
    ///   - `fetch_to_parquet()` required ONLY if using `entry_signal` or `exit_signal`
    /// **⚠️  Warning**: Slow! Run `evaluate_strategy()` first to validate parameters
    /// **Next tools**: `compare_strategies()` (to test variations) or iterate on parameters
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
    #[tool(name = "run_backtest", annotations(read_only_hint = true))]
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
            strategy: params.strategy.as_str().to_string(),
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
            multiplier: params.multiplier,
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
    /// **When to use**: After validating one strategy via `run_backtest()`, to test
    ///   parameter variations and find the best-performing approach
    /// **Prerequisites**: `load_data()` must have been called
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
    ) -> Result<Json<CompareResponse>, String> {
        params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;
        let data = self.data.read().await;
        let Some((_, df)) = data.as_ref() else {
            return Err("Error: No data loaded. Call load_data first.".to_string());
        };

        let compare_params = CompareParams {
            strategies: params
                .strategies
                .into_iter()
                .map(|s| CompareEntry {
                    name: s.name.as_str().to_string(),
                    leg_deltas: s.leg_deltas,
                    max_entry_dte: s.max_entry_dte,
                    exit_dte: s.exit_dte,
                    slippage: s.slippage,
                    commission: s.commission,
                })
                .collect(),
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
    /// **Workflow Phase**: 0 (optional, before `load_data`/`fetch_to_parquet`)
    /// **When to use**: To avoid redundant downloads or to verify data staleness
    /// **Prerequisites**: None
    /// **Next tools**: `load_data` or `fetch_to_parquet` (if cache is missing/stale)
    ///
    /// **Returns**:
    ///   - Cache exists (boolean)
    ///   - File path (if exists)
    ///   - File size and row count
    ///   - Last update timestamp
    /// **Use case**: Before calling `load_data`/`fetch_to_parquet`, check if you need
    ///   to download fresh data or if cache is recent enough
    #[tool(name = "check_cache_status", annotations(read_only_hint = true))]
    async fn check_cache_status(
        &self,
        Parameters(params): Parameters<CheckCacheParams>,
    ) -> Result<Json<CheckCacheResponse>, String> {
        params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;
        tools::cache_status::execute(&self.cache, &params.symbol, params.category.as_str())
            .map(Json)
            .map_err(|e| format!("Error: {e}"))
    }

    /// Download OHLCV price data from Yahoo Finance and cache locally as Parquet.
    ///
    /// **Workflow Phase**: 0b (optional, before `run_backtest` with signals)
    /// **When to use**: ONLY if you want to use `entry_signal` or `exit_signal` in `run_backtest`
    /// **Prerequisites**: None
    /// **Next tool**: `run_backtest` with `entry_signal`/`exit_signal` parameters
    ///
    /// **Why separate from `load_data`**:
    ///   - `load_data()` loads options chain data (bid/ask/delta)
    ///   - `fetch_to_parquet()` loads price bars (OHLCV) for TA indicators
    ///   - Both can be loaded simultaneously
    /// **Categories**: Use "prices" for standard price data
    /// **Periods**: "6mo" (default), "1y", "5y", "max"
    /// **Performance**: Downloads to ~/.optopsy/cache/prices/{SYMBOL}.parquet
    ///
    /// **Important**: Not needed for basic backtest (no signals).
    ///   Only load if using `construct_signal` → enter signal JSON into `run_backtest`.
    #[tool(
        name = "fetch_to_parquet",
        annotations(
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn fetch_to_parquet(
        &self,
        Parameters(params): Parameters<FetchToParquetParams>,
    ) -> Result<Json<FetchResponse>, String> {
        params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;
        let period = params.period.as_deref().unwrap_or("6mo");
        tools::fetch::execute(
            &self.cache,
            &params.symbol,
            params.category.as_str(),
            period,
        )
        .await
        .map(Json)
        .map_err(|e| format!("Error: {e}"))
    }

    /// Analyze the loaded options chain and suggest data-driven parameters.
    ///
    /// **Workflow Phase**: 3/7 (parameter optimization)
    /// **When to use**: After `load_data()`, to get intelligent parameter suggestions
    ///   based on actual market data (DTE coverage, spread quality, delta distribution)
    /// **Prerequisites**: `load_data()` must have been called first
    /// **Next tools**: `evaluate_strategy()` or `run_backtest()` with suggested parameters
    ///
    /// **What it analyzes**:
    ///   - DTE distribution and contiguous coverage zones
    ///   - Bid/ask spread quality per DTE bucket
    ///   - Delta distribution per leg (quartile-based targeting)
    ///   - Suggested `exit_dte` based on data coverage
    /// **Risk preferences**: Conservative (tight filters), Moderate (balanced), Aggressive (loose)
    /// **Output**:
    ///   - `leg_deltas` array (optimized delta targets/ranges per leg)
    ///   - `max_entry_dte` (maximum viable entry DTE from data)
    ///   - `exit_dte` (recommended exit DTE)
    ///   - slippage model recommendation (Mid/Spread/Liquidity)
    ///   - Confidence score (combines data coverage and calendar quality)
    /// **Saves time**: No need to guess parameters; use market-driven recommendations
    #[tool(name = "suggest_parameters", annotations(read_only_hint = true))]
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
            strategy: params.strategy.as_str().to_string(),
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
                title: Some("Optopsy Options Backtesting Engine".into()),
                version: "0.1.0".into(),
                description: Some("Event-driven options backtesting engine with 32 strategies, realistic position management, and AI-compatible analysis tools".into()),
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
