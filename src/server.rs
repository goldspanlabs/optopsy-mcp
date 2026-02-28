use std::sync::Arc;
use tokio::sync::RwLock;
use rmcp::{
    ServerHandler, tool, tool_handler, tool_router,
    handler::server::router::tool::ToolRouter,
    model::{ServerCapabilities, Implementation, ServerInfo},
};
use polars::prelude::*;
use serde::Deserialize;
use schemars::JsonSchema;

use crate::engine::types::*;
use crate::tools;

#[derive(Clone)]
pub struct OptopsyServer {
    pub data: Arc<RwLock<Option<DataFrame>>>,
    tool_router: ToolRouter<Self>,
}

impl OptopsyServer {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(None)),
            tool_router: Self::tool_router(),
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct LoadDataParams {
    /// Path to the Parquet file
    pub file_path: String,
    /// Start date filter (YYYY-MM-DD)
    pub start_date: Option<String>,
    /// End date filter (YYYY-MM-DD)
    pub end_date: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct EvaluateStrategyParams {
    /// Strategy name (e.g. 'iron_condor')
    pub strategy: String,
    /// Per-leg delta targets
    pub leg_deltas: Vec<TargetRange>,
    /// Maximum DTE at entry
    pub max_entry_dte: i32,
    /// DTE at exit
    pub exit_dte: i32,
    /// DTE bucket width (e.g. 7)
    pub dte_interval: i32,
    /// Delta bucket width (e.g. 0.05)
    pub delta_interval: f64,
    /// Slippage model
    pub slippage: Slippage,
    /// Commission structure (optional)
    pub commission: Option<Commission>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RunBacktestParams {
    /// Strategy name
    pub strategy: String,
    /// Per-leg delta targets
    pub leg_deltas: Vec<TargetRange>,
    /// Maximum DTE at entry
    pub max_entry_dte: i32,
    /// DTE at exit
    pub exit_dte: i32,
    /// Slippage model
    pub slippage: Slippage,
    /// Commission structure
    pub commission: Option<Commission>,
    /// Stop loss threshold (fraction of entry cost)
    pub stop_loss: Option<f64>,
    /// Take profit threshold (fraction of entry cost)
    pub take_profit: Option<f64>,
    /// Maximum days to hold
    pub max_hold_days: Option<i32>,
    /// Starting capital
    pub capital: f64,
    /// Number of contracts per trade
    pub quantity: i32,
    /// Contract multiplier (default 100)
    pub multiplier: Option<i32>,
    /// Maximum concurrent positions
    pub max_positions: i32,
    /// Trade selection method
    pub selector: Option<TradeSelector>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CompareStrategiesParams {
    /// List of strategies with their parameters
    pub strategies: Vec<CompareEntry>,
    /// Shared simulation parameters
    pub sim_params: SimParams,
}

use rmcp::handler::server::wrapper::Parameters;

#[tool_router]
impl OptopsyServer {
    /// Load options chain data from a Parquet file
    #[tool(name = "load_data")]
    async fn load_data(&self, Parameters(params): Parameters<LoadDataParams>) -> String {
        match tools::load_data::execute(
            &self.data,
            &params.file_path,
            params.start_date.as_deref(),
            params.end_date.as_deref(),
        )
        .await
        {
            Ok(result) => result,
            Err(e) => format!("Error: {}", e),
        }
    }

    /// List all available options strategies with their definitions
    #[tool(name = "list_strategies")]
    async fn list_strategies(&self) -> String {
        match tools::strategies::execute() {
            Ok(result) => result,
            Err(e) => format!("Error: {}", e),
        }
    }

    /// Evaluate a strategy statistically by grouping trades into DTE/delta buckets
    #[tool(name = "evaluate_strategy")]
    async fn evaluate_strategy(
        &self,
        Parameters(params): Parameters<EvaluateStrategyParams>,
    ) -> String {
        let data = self.data.read().await;
        let df = match data.as_ref() {
            Some(df) => df,
            None => return "Error: No data loaded. Call load_data first.".to_string(),
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

        match tools::evaluate::execute(df, &eval_params) {
            Ok(result) => result,
            Err(e) => format!("Error: {}", e),
        }
    }

    /// Run a full backtest simulation with trade log, equity curve, and performance metrics
    #[tool(name = "run_backtest")]
    async fn run_backtest(&self, Parameters(params): Parameters<RunBacktestParams>) -> String {
        let data = self.data.read().await;
        let df = match data.as_ref() {
            Some(df) => df,
            None => return "Error: No data loaded. Call load_data first.".to_string(),
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
        };

        match tools::backtest::execute(df, &backtest_params) {
            Ok(result) => result,
            Err(e) => format!("Error: {}", e),
        }
    }

    /// Compare multiple strategies side by side
    #[tool(name = "compare_strategies")]
    async fn compare_strategies(
        &self,
        Parameters(params): Parameters<CompareStrategiesParams>,
    ) -> String {
        let data = self.data.read().await;
        let df = match data.as_ref() {
            Some(df) => df,
            None => return "Error: No data loaded. Call load_data first.".to_string(),
        };

        let compare_params = CompareParams {
            strategies: params.strategies,
            sim_params: params.sim_params,
        };

        match tools::compare::execute(df, &compare_params) {
            Ok(result) => result,
            Err(e) => format!("Error: {}", e),
        }
    }
}

#[tool_handler]
impl ServerHandler for OptopsyServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: Default::default(),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation {
                name: "optopsy-polars".into(),
                title: None,
                version: "0.1.0".into(),
                description: None,
                icons: None,
                website_url: None,
            },
            instructions: Some(
                "Options backtesting engine. Load data first with load_data, \
                then use list_strategies to see available strategies, \
                evaluate_strategy for statistical analysis, \
                or run_backtest for simulation."
                    .into(),
            ),
        }
    }
}
