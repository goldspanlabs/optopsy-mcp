use garde::Validate;
use polars::prelude::*;
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Json},
    model::{Implementation, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router, ServerHandler,
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::data::cache::CachedStore;
use crate::data::eodhd::EodhdProvider;
use crate::engine::types::{
    default_delta_interval, default_dte_interval, default_min_bid_ask, default_multiplier,
    validate_exit_dte_lt_entry_min, BacktestParams, Commission, CompareEntry, CompareParams,
    DteRange, EvaluateParams, SimParams, Slippage, TargetRange, TradeSelector,
};
use crate::signals::registry::SignalSpec;
use crate::tools;
use crate::tools::response_types::{
    BacktestResponse, BuildSignalResponse, CheckCacheResponse, CompareResponse,
    ConstructSignalResponse, DownloadResponse, EvaluateResponse, FetchResponse, LoadDataResponse,
    RawPricesResponse, StatusResponse, StrategiesResponse, SuggestResponse,
};
use crate::tools::signals::SignalsResponse;

/// Loaded data: `HashMap<Symbol, DataFrame>` for multi-symbol support.
type LoadedData = HashMap<String, DataFrame>;

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
            data: Arc::new(RwLock::new(HashMap::new())),
            cache,
            eodhd,
            tool_router: Self::tool_router(),
        }
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
            return Err("No data loaded. Call load_data first.".to_string());
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
                    Ok(data.iter().next().unwrap())
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

/// Validate that `end_date >= start_date` when both are present.
/// Signature uses `&Option<String>` because garde's `custom()` passes `&self.field`.
#[allow(clippy::ref_option)]
fn validate_end_date_after_start(
    start_date: &Option<String>,
) -> impl FnOnce(&Option<String>, &()) -> garde::Result + '_ {
    move |end_date: &Option<String>, (): &()| {
        if let (Some(start), Some(end)) = (start_date, end_date) {
            if end < start {
                return Err(garde::Error::new(format!(
                    "end_date ({end}) must be >= start_date ({start})"
                )));
            }
        }
        Ok(())
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
    #[garde(inner(pattern(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")), custom(validate_end_date_after_start(&self.start_date)))]
    pub end_date: Option<String>,
}

/// Resolve `leg_deltas`: use provided deltas or fall back to strategy defaults.
fn resolve_leg_deltas(
    leg_deltas: Option<Vec<TargetRange>>,
    strategy_name: &str,
) -> Result<Vec<TargetRange>, String> {
    if let Some(deltas) = leg_deltas {
        Ok(deltas)
    } else {
        let strategy_def = crate::strategies::find_strategy(strategy_name)
            .ok_or_else(|| format!("Error: Unknown strategy: {strategy_name}"))?;
        Ok(strategy_def.default_deltas())
    }
}

fn default_entry_dte() -> DteRange {
    DteRange {
        target: 45,
        min: 30,
        max: 60,
    }
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
    /// Per-leg delta targets (optional — uses strategy-specific defaults if omitted)
    #[serde(default)]
    #[garde(inner(length(min = 1)))]
    pub leg_deltas: Option<Vec<TargetRange>>,
    /// Entry DTE range: { target, min, max } (default: { target: 45, min: 30, max: 60 })
    #[serde(default = "default_entry_dte")]
    #[garde(dive)]
    pub entry_dte: DteRange,
    /// DTE at exit (default: 9)
    #[serde(default = "default_exit_dte")]
    #[garde(range(min = 0), custom(validate_exit_dte_lt_entry_min(&self.entry_dte)))]
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
    /// Minimum bid/ask threshold — options with bid or ask at or below this value are filtered out (default: 0.05)
    #[serde(default = "default_min_bid_ask")]
    #[garde(range(min = 0.0))]
    pub min_bid_ask: f64,
    /// Symbol to analyze (required if multiple symbols are loaded; optional if only one is loaded)
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$")))]
    pub symbol: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct RunBacktestParams {
    /// Required. Strategy to backtest — must be one of the enum variants (e.g. `long_call`,
    /// `short_put`, `iron_condor`, `short_strangle`). Cannot be null or omitted.
    /// Use `list_strategies` if unsure which to pick.
    #[garde(skip)]
    pub strategy: StrategyParam,
    /// Per-leg delta targets (optional — uses strategy-specific defaults if omitted)
    #[serde(default)]
    #[garde(inner(length(min = 1)))]
    pub leg_deltas: Option<Vec<TargetRange>>,
    /// Entry DTE range: { target, min, max } (default: { target: 45, min: 30, max: 60 })
    #[serde(default = "default_entry_dte")]
    #[garde(dive)]
    pub entry_dte: DteRange,
    /// DTE at exit (default: 9)
    #[serde(default = "default_exit_dte")]
    #[garde(range(min = 0), custom(validate_exit_dte_lt_entry_min(&self.entry_dte)))]
    pub exit_dte: i32,
    /// Slippage model (default: Spread)
    #[serde(default)]
    #[garde(dive)]
    pub slippage: Slippage,
    /// Commission structure
    #[serde(default)]
    #[garde(dive)]
    pub commission: Option<Commission>,
    /// Minimum bid/ask threshold — options with bid or ask at or below this value are filtered out (default: 0.05)
    #[serde(default = "default_min_bid_ask")]
    #[garde(range(min = 0.0))]
    pub min_bid_ask: f64,
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
    /// Symbol to backtest (required if multiple symbols are loaded; optional if only one is loaded)
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$")))]
    pub symbol: Option<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema, Validate)]
pub struct ServerCompareEntry {
    /// Strategy name
    #[garde(skip)]
    pub name: StrategyParam,
    /// Per-leg delta targets (optional — uses strategy-specific defaults if omitted)
    #[serde(default)]
    #[garde(inner(length(min = 1)))]
    pub leg_deltas: Option<Vec<TargetRange>>,
    /// Entry DTE range: { target, min, max } (default: { target: 45, min: 30, max: 60 })
    #[serde(default = "default_entry_dte")]
    #[garde(dive)]
    pub entry_dte: DteRange,
    /// DTE at exit (default: 9)
    #[serde(default = "default_exit_dte")]
    #[garde(range(min = 0), custom(validate_exit_dte_lt_entry_min(&self.entry_dte)))]
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
    /// Symbol to compare strategies on (required if multiple symbols are loaded; optional if only one is loaded)
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$")))]
    pub symbol: Option<String>,
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
    /// Optional symbol to check if OHLCV data is cached (e.g. "SPY")
    /// If provided, response will indicate whether data is ready for signal usage
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$")))]
    pub symbol: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub enum BuildSignalAction {
    /// Create a new custom signal from a formula (optionally save it)
    Create,
    /// List all saved custom signals
    List,
    /// Delete a saved signal by name
    Delete,
    /// Validate a formula without saving
    Validate,
    /// Load a saved signal and return its spec
    Get,
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct BuildSignalParams {
    /// Action to perform
    #[garde(skip)]
    pub action: BuildSignalAction,
    /// Signal name (required for create, delete, get)
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 64), pattern(r"^[A-Za-z0-9_-]+$")))]
    pub name: Option<String>,
    /// Formula expression (required for create, validate).
    /// Uses price columns (close, open, high, low, volume) with operators and functions.
    /// Examples: "close > sma(close, 20)", "volume > sma(volume, 20) * 2.0"
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 2000)))]
    pub formula: Option<String>,
    /// Optional description of what this signal detects
    #[serde(default)]
    #[garde(inner(length(max = 500)))]
    pub description: Option<String>,
    /// Whether to persist the signal to disk (default: true for create)
    #[serde(default = "default_save")]
    #[garde(skip)]
    pub save: bool,
}

fn default_save() -> bool {
    true
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct FetchToParquetParams {
    /// Ticker symbol (e.g. "SPY")
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Cache category
    #[garde(skip)]
    pub category: CategoryParam,
    /// Time period to fetch (e.g. "6mo", "1y", "5y", "max"). Defaults to "5y".
    #[garde(inner(length(min = 1)))]
    pub period: Option<String>,
}

#[allow(clippy::unnecessary_wraps)]
fn default_price_limit() -> Option<usize> {
    Some(500)
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct GetRawPricesParams {
    /// Ticker symbol (e.g. "SPY")
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Start date filter (YYYY-MM-DD)
    #[garde(inner(pattern(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")))]
    pub start_date: Option<String>,
    /// End date filter (YYYY-MM-DD)
    #[garde(inner(pattern(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")), custom(validate_end_date_after_start(&self.start_date)))]
    pub end_date: Option<String>,
    /// Maximum number of price bars to return (default: 500 if omitted).
    /// Data is evenly sampled if the total exceeds this limit.
    /// Pass `null` explicitly to disable the limit and return all bars.
    #[serde(default = "default_price_limit")]
    #[garde(skip)]
    pub limit: Option<usize>,
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
    /// Symbol to analyze (required if multiple symbols are loaded; optional if only one is loaded)
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$")))]
    pub symbol: Option<String>,
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
    /// Automatically handles date column normalization (`quote_date`/`quote_datetime`).
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

    /// Get status of currently loaded data.
    ///
    /// **Workflow Phase**: 1c/7 (data context management)
    /// **When to use**: Check what symbol is currently loaded, row count, available columns
    /// **Prerequisites**: None (works with or without loaded data)
    /// **How it works**: Returns details about the in-memory `DataFrame` (symbol, rows, columns)
    /// **Next tool**: Use `load_data()` to switch symbols, or proceed with `evaluate_strategy()` / `run_backtest()`
    /// **Example usage**: After loading SPY, call this to confirm it's loaded and see column names
    #[tool(name = "get_loaded_symbol", annotations(read_only_hint = true))]
    async fn get_loaded_symbol(&self) -> Json<StatusResponse> {
        Json(tools::status::execute(&self.data).await)
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
        Ok(Json(tools::construct_signal::execute(
            &params.prompt,
            params.symbol.as_deref(),
            self.cache.as_ref(),
        )))
    }

    /// Build, validate, save, list, and manage custom formula-based signals.
    ///
    /// **Workflow Phase**: 2d/7 (signal builder, optional)
    /// **When to use**: When built-in signals don't cover your needs and you want to
    ///   define custom entry/exit conditions using price column formulas
    /// **Prerequisites**: None (formulas are validated at parse time, data needed only at backtest)
    ///
    /// **Actions**:
    ///   - `create` — Build a signal from a formula, optionally save for later use
    ///   - `validate` — Check formula syntax without saving
    ///   - `list` — Show all saved custom signals
    ///   - `get` — Load a saved signal's spec
    ///   - `delete` — Remove a saved signal
    ///
    /// **Formula syntax**:
    ///   - Columns: `close`, `open`, `high`, `low`, `volume`, `adjclose`
    ///   - Lookback: `close[1]` (previous bar), `close[5]` (5 bars ago)
    ///   - Functions: `sma(col, N)`, `ema(col, N)`, `std(col, N)`, `max(col, N)`,
    ///     `min(col, N)`, `abs(expr)`, `change(col, N)`, `pct_change(col, N)`
    ///   - Operators: `+`, `-`, `*`, `/`, `>`, `<`, `>=`, `<=`, `==`, `!=`
    ///   - Logical: `and`, `or`, `not`
    ///
    /// **Examples**: `"close > sma(close, 20)"`, `"volume > sma(volume, 20) * 2.0"`,
    ///   `"close > close[1] * 1.02"`, `"pct_change(close, 1) > 0.03"`
    ///
    /// **Next tool**: `run_backtest()` with `entry_signal`/`exit_signal` set to the returned spec,
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
    ) -> Result<Json<BuildSignalResponse>, String> {
        params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;

        let action = match params.action {
            BuildSignalAction::Create => {
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
            BuildSignalAction::List => tools::build_signal::Action::List,
            BuildSignalAction::Delete => {
                let name = params
                    .name
                    .ok_or("'name' is required for action='delete'")?;
                tools::build_signal::Action::Delete { name }
            }
            BuildSignalAction::Validate => {
                let formula = params
                    .formula
                    .ok_or("'formula' is required for action='validate'")?;
                tools::build_signal::Action::Validate { formula }
            }
            BuildSignalAction::Get => {
                let name = params.name.ok_or("'name' is required for action='get'")?;
                tools::build_signal::Action::Get { name }
            }
        };

        Ok(Json(tools::build_signal::execute(action)))
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

        // Clone DataFrame and drop read lock before expensive analysis
        let df = {
            let data = self.data.read().await;
            let (_, df) = Self::resolve_symbol(&data, params.symbol.as_deref())
                .map_err(|e| format!("Error: {e}"))?;
            df.clone()
        };

        let strategy_name = params.strategy.as_str();
        let leg_deltas = resolve_leg_deltas(params.leg_deltas, strategy_name)?;

        let eval_params = EvaluateParams {
            strategy: strategy_name.to_string(),
            leg_deltas,
            entry_dte: params.entry_dte,
            exit_dte: params.exit_dte,
            dte_interval: params.dte_interval,
            delta_interval: params.delta_interval,
            slippage: params.slippage,
            commission: params.commission,
            min_bid_ask: params.min_bid_ask,
        };
        eval_params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;

        tools::evaluate::execute(&df, &eval_params)
            .map(Json)
            .map_err(|e| format!("Error: {e}"))
    }

    /// Full event-driven day-by-day simulation with position management and metrics.
    ///
    /// **Workflow Phase**: 5/7 (full validation)
    /// **When to use**: After `evaluate_strategy()` to validate strategy performance in capital-constrained scenario
    /// **Prerequisites**:
    ///   - `load_data()` MUST have been called (Phase 1)
    ///   - `evaluate_strategy()` MUST be called first to validate parameters (Phase 4)
    ///   - `strategy` is REQUIRED — must be a valid strategy name (e.g. `long_call`, `short_put`, `iron_condor`). Never pass null.
    ///   - ⚠️  **SIGNAL PREREQUISITE**: If `entry_signal` or `exit_signal` is provided, you MUST call
    ///     `fetch_to_parquet({ symbol: "<SYMBOL>", category: "prices" })` BEFORE calling this tool.
    ///     The backtest WILL FAIL without OHLCV data when signals are used.
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

        tracing::info!(
            strategy = params.strategy.as_str(),
            symbol = params.symbol.as_deref().unwrap_or("auto"),
            entry_dte_target = params.entry_dte.target,
            entry_dte_min = params.entry_dte.min,
            entry_dte_max = params.entry_dte.max,
            exit_dte = params.exit_dte,
            max_positions = params.max_positions,
            capital = params.capital,
            "Backtest request received"
        );

        // Clone symbol and DataFrame, resolve OHLCV path, then drop read lock before expensive backtest
        let (symbol, df) = {
            let data = self.data.read().await;
            let (sym, df) = Self::resolve_symbol(&data, params.symbol.as_deref())
                .map_err(|e| format!("Error: {e}"))?;
            (sym.clone(), df.clone())
        };

        // Auto-resolve OHLCV path if signals are requested
        let ohlcv_path = if params.entry_signal.is_some() || params.exit_signal.is_some() {
            let path = self
                .cache
                .cache_path(&symbol, "prices")
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

        let strategy_name = params.strategy.as_str();
        let leg_deltas = resolve_leg_deltas(params.leg_deltas, strategy_name)?;

        let backtest_params = BacktestParams {
            strategy: strategy_name.to_string(),
            leg_deltas,
            entry_dte: params.entry_dte,
            exit_dte: params.exit_dte,
            slippage: params.slippage,
            commission: params.commission,
            min_bid_ask: params.min_bid_ask,
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

        tools::backtest::execute(&df, &backtest_params)
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

        // Clone DataFrame and drop read lock before expensive comparison
        let df = {
            let data = self.data.read().await;
            let (_, df) = Self::resolve_symbol(&data, params.symbol.as_deref())
                .map_err(|e| format!("Error: {e}"))?;
            df.clone()
        };

        let compare_params = CompareParams {
            strategies: params
                .strategies
                .into_iter()
                .map(|s| {
                    let strategy_name = s.name.as_str();
                    let leg_deltas = resolve_leg_deltas(s.leg_deltas, strategy_name)?;
                    Ok(CompareEntry {
                        name: strategy_name.to_string(),
                        leg_deltas,
                        entry_dte: s.entry_dte,
                        exit_dte: s.exit_dte,
                        slippage: s.slippage,
                        commission: s.commission,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            sim_params: params.sim_params,
        };
        compare_params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;

        tools::compare::execute(&df, &compare_params)
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
    /// **Periods**: "5y" (default), "6mo", "1y", "max"
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
        let period = params.period.as_deref().unwrap_or("5y");
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

    /// Return raw OHLCV price data for a symbol, ready for chart generation.
    ///
    /// **Workflow Phase**: any (utility)
    /// **When to use**: When an LLM or user needs raw price data to generate charts
    ///   (candlestick, line, area) or perform custom analysis
    /// **Prerequisites**: `fetch_to_parquet()` must have been called first to cache OHLCV data
    /// **Next tools**: Use the returned `prices` array directly for visualization
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
    ) -> Result<Json<RawPricesResponse>, String> {
        params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;
        tools::raw_prices::load_and_execute(
            &self.cache,
            &params.symbol,
            params.start_date.as_deref(),
            params.end_date.as_deref(),
            params.limit,
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
    ///   - `entry_dte` (target/min/max entry DTE range from data)
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

        // Clone DataFrame and drop read lock before expensive suggestion logic
        let df = {
            let data = self.data.read().await;
            let (_, df) = Self::resolve_symbol(&data, params.symbol.as_deref())
                .map_err(|e| format!("Error: {e}"))?;
            df.clone()
        };

        tools::suggest::execute(&df, &suggest_params)
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
                "Options backtesting engine.\
                \n\n## MANDATORY WORKFLOW — Follow these phases IN ORDER for every analysis.\
                \nDo NOT skip phases. Do NOT jump ahead. Each phase builds on the previous one.\
                \n\
                \n### Phase 0: Data Preparation (optional, run if needed)\
                \n  0a. check_cache_status({ symbol, category: \"options\" }) — verify options data is cached\
                \n  0b. check_cache_status({ symbol, category: \"prices\" }) — verify OHLCV data is cached (only if using signals)\
                \n  0c. fetch_to_parquet({ symbol, category: \"prices\" }) — download OHLCV data (only if 0b shows missing AND you plan to use entry_signal/exit_signal)\
                \n  0d. download_options_data({ symbol }) — bulk download from EODHD (only if 0a shows missing)\
                \n\
                \n### Phase 1: Load Data (REQUIRED — always start here)\
                \n  1. load_data({ symbol }) — load options chain into memory. NOTHING works without this.\
                \n\
                \n### Phase 2: Explore Strategies (REQUIRED — choose what to test)\
                \n  2a. list_strategies() — browse all 32 strategies by category\
                \n  2b. list_signals() — browse TA signals (only if planning signal-filtered backtest)\
                \n  2c. construct_signal({ prompt }) — build signal JSON (only if using signals)\
                \n  2d. build_signal({ action: \"create\", ... }) — custom formula signals (only if built-in signals insufficient)\
                \n\
                \n### Phase 3: Get Parameters (RECOMMENDED — avoid guessing)\
                \n  3. suggest_parameters({ strategy, risk_preference }) — data-driven DTE/delta/slippage recommendations\
                \n\
                \n### Phase 4: Statistical Screening (RECOMMENDED — validate before slow backtest)\
                \n  4. evaluate_strategy({ strategy, ... }) — fast DTE×delta bucket analysis. Identifies best parameter zones.\
                \n\
                \n### Phase 5: Full Simulation (the main goal)\
                \n  5. run_backtest({ strategy, ... }) — event-driven backtest with equity curve, trade log, and metrics\
                \n\
                \n### Phase 6: Compare & Optimize (optional — test variations)\
                \n  6. compare_strategies({ strategies, sim_params }) — side-by-side ranking by Sharpe/PnL\
                \n\
                \n### Phase 7: Visualize (optional)\
                \n  7. get_raw_prices({ symbol }) — OHLCV price bars for chart generation\
                \n\
                \n## RULES\
                \n- ALWAYS call load_data FIRST before any analysis tool\
                \n- ALWAYS call list_strategies to select a strategy before evaluate/backtest\
                \n- ALWAYS call evaluate_strategy BEFORE run_backtest to validate parameters\
                \n- NEVER call run_backtest without completing Phases 1-4\
                \n- If using signals: MUST call fetch_to_parquet BEFORE run_backtest\
                \n- Each tool response includes suggested_next_steps — follow them\
                \n\
                \nData flow: EODHD API → local Parquet cache → DataFrame → per-leg filter/delta-select → \
                leg join → strike-order validation → P&L calculation → bucket aggregation (evaluate) \
                or event-loop simulation (backtest) → AI-enriched JSON response."
                    .into(),
            ),
        }
    }
}
