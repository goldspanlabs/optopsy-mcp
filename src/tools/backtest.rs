//! MCP tool handler for `backtest` — run a single backtest or parameter sweep and persist results.

use garde::Validate;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::application::{backtests, pipeline, sweeps};
use crate::server::OptopsyServer;
use crate::tools::response_types::pipeline::PipelineResponse;
use crate::tools::response_types::sweep::SweepResponse;
use crate::tools::run_script::RunScriptResponse;

// ──────────────────────────────────────────────────────────────────────────────
// Default helpers
// ──────────────────────────────────────────────────────────────────────────────

fn default_mode() -> String {
    "grid".to_string()
}

fn default_objective() -> String {
    "sharpe".to_string()
}

fn default_max_evaluations() -> usize {
    50
}

fn default_pipeline() -> bool {
    true
}

// ──────────────────────────────────────────────────────────────────────────────
// Params
// ──────────────────────────────────────────────────────────────────────────────

/// Parameters for the `backtest` MCP tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct BacktestToolParams {
    /// Strategy name (display name or ID from the strategy store).
    #[garde(length(min = 1))]
    pub strategy: String,

    /// Sweep mode: `"grid"` (exhaustive) or `"bayesian"` (adaptive). Default `"grid"`.
    #[serde(default = "default_mode")]
    #[garde(skip)]
    pub mode: String,

    /// Objective metric to optimize. Default `"sharpe"`.
    #[serde(default = "default_objective")]
    #[garde(skip)]
    pub objective: String,

    /// Base parameters injected into the script (e.g. `SYMBOL`, `CAPITAL`).
    #[serde(default)]
    #[garde(skip)]
    pub params: HashMap<String, Value>,

    /// Parameter ranges to sweep. Omit for a single backtest.
    #[serde(default)]
    #[garde(skip)]
    pub sweep_params: Vec<sweeps::SweepParamDef>,

    /// Maximum evaluations for bayesian mode. Default 50.
    #[serde(default = "default_max_evaluations")]
    #[garde(skip)]
    pub max_evaluations: usize,

    /// Number of permutations for statistical significance testing. Default 0 (off).
    /// When > 0, runs sign-flip permutation test and applies BH-FDR correction.
    /// Values above 100,000 are rejected by input validation.
    #[serde(default)]
    #[garde(range(max = 100_000))]
    pub num_permutations: usize,

    /// Chat thread ID for associating results with a conversation.
    #[serde(default)]
    #[garde(skip)]
    pub thread_id: Option<String>,

    /// When true, sweeps automatically run the full pipeline:
    /// sweep -> significance gate -> walk-forward -> OOS gate -> monte carlo.
    /// Default true for sweeps; set `pipeline=false` to return sweep-only results.
    /// Has no effect on single backtests (only applies when `sweep_params` is non-empty).
    #[serde(default = "default_pipeline")]
    #[garde(skip)]
    pub pipeline: bool,
}

// ──────────────────────────────────────────────────────────────────────────────
// Response
// ──────────────────────────────────────────────────────────────────────────────

/// Inner struct for single backtest result (boxed to reduce enum variant size).
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct SingleBacktestResponse {
    /// Unique run ID for referencing the persisted result.
    pub run_id: String,
    /// Full backtest result.
    #[serde(flatten)]
    pub result: RunScriptResponse,
    /// Suggested next analysis steps for the agent.
    pub suggested_next_steps: Vec<String>,
}

/// Inner struct for sweep result (boxed to reduce enum variant size).
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct SweepBacktestResponse {
    /// Unique sweep ID for referencing the persisted results.
    pub sweep_id: String,
    /// Run IDs for each individual backtest within the sweep.
    pub run_ids: Vec<String>,
    /// Sweep results (ranked combos, sensitivity, timing).
    #[serde(flatten)]
    pub sweep: SweepResponse,
    /// Suggested next analysis steps for the agent.
    pub suggested_next_steps: Vec<String>,
}

/// Response from the `backtest` tool — single run, sweep, or full pipeline.
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum BacktestToolResponse {
    /// Single backtest result with full equity curve, trade log, and metrics.
    #[serde(rename = "single")]
    Single(Box<SingleBacktestResponse>),
    /// Parameter sweep result with ranked combinations (pipeline=false).
    #[serde(rename = "sweep")]
    Sweep(Box<SweepBacktestResponse>),
    /// Full pipeline: sweep + walk-forward + monte carlo with gate statuses.
    #[serde(rename = "pipeline")]
    Pipeline(Box<PipelineResponse>),
}

// ──────────────────────────────────────────────────────────────────────────────
// Execute
// ──────────────────────────────────────────────────────────────────────────────

/// Run a single backtest or parameter sweep and persist the results.
pub async fn execute(
    server: &OptopsyServer,
    params: BacktestToolParams,
) -> Result<BacktestToolResponse, anyhow::Error> {
    if params.sweep_params.is_empty() {
        execute_single(server, params).await
    } else if params.pipeline {
        let pipeline_request = build_pipeline_request(&params);
        let pipeline_response = pipeline::execute(server, &pipeline_request, "agent").await?;
        Ok(BacktestToolResponse::Pipeline(Box::new(pipeline_response)))
    } else {
        // Sweep-only (no pipeline)
        let (sweep_id, run_ids, sweep_response, strategy, symbol, _capital, _objective) =
            execute_sweep_raw(server, &params).await?;

        let upper = symbol.to_uppercase();
        let suggested_next_steps = vec![
            format!(
                "[NEXT] Re-run backtest(strategy=\"{strategy}\", symbol=\"{upper}\", pipeline=true) to run the full validation pipeline (walk-forward + monte carlo)",
            ),
            format!("[THEN] Call drawdown_analysis(symbol=\"{upper}\") to analyze drawdown episodes and risk profile"),
            format!("[THEN] Call monte_carlo(symbol=\"{upper}\") to simulate forward-looking risk"),
            format!("[TIP] Call factor_attribution(symbol=\"{upper}\") to check if alpha is genuine or factor exposure"),
        ];

        Ok(BacktestToolResponse::Sweep(Box::new(
            SweepBacktestResponse {
                sweep_id,
                run_ids,
                sweep: sweep_response,
                suggested_next_steps,
            },
        )))
    }
}

fn build_pipeline_request(params: &BacktestToolParams) -> pipeline::PipelineRequest {
    pipeline::PipelineRequest {
        strategy: params.strategy.clone(),
        mode: params.mode.clone(),
        objective: params.objective.clone(),
        params: params.params.clone(),
        sweep_params: params.sweep_params.clone(),
        max_evaluations: params.max_evaluations,
        num_permutations: params.num_permutations,
        thread_id: params.thread_id.clone(),
    }
}

/// Run a single backtest and persist the result.
async fn execute_single(
    server: &OptopsyServer,
    params: BacktestToolParams,
) -> Result<BacktestToolResponse, anyhow::Error> {
    let run_store = server.require_run_store()?;

    // Reuse the existing run_script handler for execution
    let run_params = crate::tools::run_script::RunScriptParams {
        strategy: Some(params.strategy.clone()),
        script: None,
        params: params.params.clone(),
        profile: None,
    };

    let exec_result = backtests::execute_script(server, run_params).await?;
    let strategy_key = exec_result
        .resolved_strategy_id
        .unwrap_or_else(|| params.strategy.clone());
    let response = exec_result.response;

    let symbol = backtests::resolve_symbol(&response, &params.params)
        .map_err(|(_status, msg)| anyhow::anyhow!("{msg}"))?;

    let (run_id, _created_at) = backtests::persist_backtest(
        run_store.as_ref(),
        &strategy_key,
        &params.params,
        &response,
        "agent",
        params.thread_id.as_deref(),
    )
    .map_err(|(_status, msg)| anyhow::anyhow!("{msg}"))?;

    let upper = symbol.to_uppercase();
    let suggested_next_steps = vec![
        format!("[NEXT] Call drawdown_analysis(symbol=\"{upper}\") to analyze drawdown episodes and risk profile"),
        format!("[THEN] Call monte_carlo(symbol=\"{upper}\") to simulate forward-looking risk"),
        format!("[TIP] Call factor_attribution(symbol=\"{upper}\") to check if alpha is genuine or factor exposure"),
    ];

    Ok(BacktestToolResponse::Single(Box::new(
        SingleBacktestResponse {
            run_id,
            result: response,
            suggested_next_steps,
        },
    )))
}

/// Run a parameter sweep and persist the results, returning raw components.
///
/// Returns `(sweep_id, run_ids, sweep_response, strategy_key, symbol, capital, objective)`.
#[allow(clippy::too_many_lines, clippy::type_complexity)]
async fn execute_sweep_raw(
    server: &OptopsyServer,
    params: &BacktestToolParams,
) -> Result<
    (
        String,
        Vec<String>,
        SweepResponse,
        String,
        String,
        f64,
        String,
    ),
    anyhow::Error,
> {
    let run_store = server.require_run_store()?;

    let req = sweeps::CreateSweepRequest {
        strategy: params.strategy.clone(),
        mode: params.mode.clone(),
        objective: params.objective.clone(),
        params: params.params.clone(),
        sweep_params: params.sweep_params.clone(),
        max_evaluations: params.max_evaluations,
        num_permutations: params.num_permutations,
    };

    let result = sweeps::execute_sweep(
        server,
        run_store.as_ref(),
        &req,
        "agent",
        params.thread_id.as_deref(),
        None,
        None,
    )
    .await?;

    Ok((
        result.sweep_id,
        result.run_ids,
        result.response,
        result.strategy_key,
        result.symbol,
        result.capital,
        result.objective,
    ))
}
