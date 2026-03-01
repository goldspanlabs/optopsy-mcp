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
use crate::engine::types::{
    BacktestParams, Commission, CompareEntry, CompareParams, EvaluateParams, SimParams, Slippage,
    TargetRange, TradeSelector,
};
use crate::tools;
use crate::tools::response_types::{
    BacktestResponse, CheckCacheResponse, CompareResponse, EvaluateResponse, FetchResponse,
    LoadDataResponse, StrategiesResponse,
};

#[derive(Clone)]
pub struct OptopsyServer {
    pub data: Arc<RwLock<Option<DataFrame>>>,
    pub cache: Arc<CachedStore>,
    tool_router: ToolRouter<Self>,
}

impl OptopsyServer {
    pub fn new(cache: Arc<CachedStore>) -> Self {
        Self {
            data: Arc::new(RwLock::new(None)),
            cache,
            tool_router: Self::tool_router(),
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct LoadDataParams {
    /// Ticker symbol (e.g. "SPY")
    #[garde(length(min = 1, max = 10))]
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
    #[garde(skip)]
    pub strategy: String,
    /// Per-leg delta targets
    #[garde(length(min = 1), dive)]
    pub leg_deltas: Vec<TargetRange>,
    /// Maximum DTE at entry
    #[garde(range(min = 1))]
    pub max_entry_dte: i32,
    /// DTE at exit
    #[garde(range(min = 0))]
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
    #[garde(skip)]
    pub strategy: String,
    /// Per-leg delta targets
    #[garde(length(min = 1), dive)]
    pub leg_deltas: Vec<TargetRange>,
    /// Maximum DTE at entry
    #[garde(range(min = 1))]
    pub max_entry_dte: i32,
    /// DTE at exit
    #[garde(range(min = 0))]
    pub exit_dte: i32,
    /// Slippage model
    #[garde(dive)]
    pub slippage: Slippage,
    /// Commission structure
    #[garde(dive)]
    pub commission: Option<Commission>,
    /// Stop loss threshold (fraction of entry cost)
    #[garde(inner(range(min = 0.0, max = 1.0)))]
    pub stop_loss: Option<f64>,
    /// Take profit threshold (fraction of entry cost)
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
    #[garde(length(min = 1, max = 10))]
    pub symbol: String,
    /// Cache category subdirectory (e.g. "prices", "options")
    #[garde(length(min = 1))]
    pub category: String,
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct FetchToParquetParams {
    /// Ticker symbol (e.g. "SPY")
    #[garde(length(min = 1, max = 10))]
    pub symbol: String,
    /// Cache category subdirectory (e.g. "prices")
    #[garde(length(min = 1))]
    pub category: String,
    /// Time period to fetch (e.g. "6mo", "1y", "5y", "max"). Defaults to "6mo".
    #[garde(skip)]
    pub period: Option<String>,
}

use rmcp::handler::server::wrapper::Parameters;

#[tool_router]
impl OptopsyServer {
    /// Load options chain data by symbol (auto-fetches from S3 cache if configured)
    #[tool(name = "load_data")]
    async fn load_data(
        &self,
        Parameters(params): Parameters<LoadDataParams>,
    ) -> Result<Json<LoadDataResponse>, String> {
        params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;
        tools::load_data::execute(
            &self.data,
            &self.cache,
            &params.symbol,
            params.start_date.as_deref(),
            params.end_date.as_deref(),
        )
        .await
        .map(Json)
        .map_err(|e| format!("Error: {e}"))
    }

    /// List all available options strategies with their definitions
    #[tool(name = "list_strategies")]
    async fn list_strategies(&self) -> Json<StrategiesResponse> {
        Json(tools::strategies::execute())
    }

    /// Evaluate a strategy statistically by grouping trades into DTE/delta buckets
    #[tool(name = "evaluate_strategy")]
    async fn evaluate_strategy(
        &self,
        Parameters(params): Parameters<EvaluateStrategyParams>,
    ) -> Result<Json<EvaluateResponse>, String> {
        params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;
        let data = self.data.read().await;
        let Some(df) = data.as_ref() else {
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

        tools::evaluate::execute(df, &eval_params)
            .map(Json)
            .map_err(|e| format!("Error: {e}"))
    }

    /// Run a full backtest simulation with trade log, equity curve, and performance metrics
    #[tool(name = "run_backtest")]
    async fn run_backtest(
        &self,
        Parameters(params): Parameters<RunBacktestParams>,
    ) -> Result<Json<BacktestResponse>, String> {
        params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;
        let data = self.data.read().await;
        let Some(df) = data.as_ref() else {
            return Err("Error: No data loaded. Call load_data first.".to_string());
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
        };

        tools::backtest::execute(df, &backtest_params)
            .map(Json)
            .map_err(|e| format!("Error: {e}"))
    }

    /// Compare multiple strategies side by side
    #[tool(name = "compare_strategies")]
    async fn compare_strategies(
        &self,
        Parameters(params): Parameters<CompareStrategiesParams>,
    ) -> Result<Json<CompareResponse>, String> {
        params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;
        let data = self.data.read().await;
        let Some(df) = data.as_ref() else {
            return Err("Error: No data loaded. Call load_data first.".to_string());
        };

        let compare_params = CompareParams {
            strategies: params.strategies,
            sim_params: params.sim_params,
        };

        tools::compare::execute(df, &compare_params)
            .map(Json)
            .map_err(|e| format!("Error: {e}"))
    }

    /// Check if cached parquet data exists for a symbol and when it was last updated
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

    /// Fetch historical OHLCV data from Yahoo Finance and save as a local Parquet file
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
                All subsequent tools operate on the in-memory DataFrame loaded here.\
                \n2. list_strategies() — browse all built-in strategies grouped by category \
                (singles, spreads, butterflies, condors, iron, calendars).\
                \n3. evaluate_strategy({ strategy, leg_deltas, max_entry_dte, exit_dte, \
                dte_interval, delta_interval, slippage }) — fast statistical screen that \
                groups historical trades into DTE × delta buckets and returns mean P&L, \
                win rate, profit factor, and distribution stats per bucket. \
                Use this to identify promising parameter ranges before committing to a full simulation.\
                \n4. run_backtest({ strategy, leg_deltas, ..., capital, quantity, max_positions }) \
                — event-driven day-by-day simulation with position management (stop loss, take profit, \
                max hold, DTE exit), equity curve, and full performance metrics \
                (Sharpe, Sortino, Calmar, VaR, CAGR, expectancy).\
                \n5. compare_strategies({ strategies: [...], sim_params }) — run the same backtest \
                pipeline for multiple strategies in parallel and rank them by Sharpe and total P&L.\
                \n\nData flow summary: raw Parquet → DataFrame → per-leg filter/delta-select → \
                leg join → strike-order validation → P&L calculation → bucket aggregation \
                (evaluate) or event-loop simulation (backtest) → AI-enriched JSON response."
                    .into(),
            ),
        }
    }
}
