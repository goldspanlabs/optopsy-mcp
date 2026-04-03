//! MCP tool handler for `backtest` — run a single backtest or parameter sweep and persist results.

use std::collections::HashMap;
use std::sync::Arc;

use garde::Validate;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::engine::bayesian::{run_bayesian, BayesianConfig};
use crate::engine::permutation::apply_permutation_gate;
use crate::engine::sweep::{run_grid_sweep, GridSweepConfig};
use crate::scripting::engine::{CachingDataLoader, CancelCallback, DataLoader};
use crate::server::handlers::sweeps::{
    build_grid, persist_sweep_to_store, resolve_strategy_source_from_store, CreateSweepRequest,
    SweepParamDef,
};
use crate::server::OptopsyServer;
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
    pub sweep_params: Vec<SweepParamDef>,

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

/// Response from the `backtest` tool — either a single run or a sweep.
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum BacktestToolResponse {
    /// Single backtest result with full equity curve, trade log, and metrics.
    #[serde(rename = "single")]
    Single(Box<SingleBacktestResponse>),
    /// Parameter sweep result with ranked combinations.
    #[serde(rename = "sweep")]
    Sweep(Box<SweepBacktestResponse>),
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
    } else {
        execute_sweep(server, params).await
    }
}

/// Run a single backtest and persist the result.
async fn execute_single(
    server: &OptopsyServer,
    params: BacktestToolParams,
) -> Result<BacktestToolResponse, anyhow::Error> {
    let run_store = server
        .run_store
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Run store not configured — cannot persist results"))?;

    // Reuse the existing run_script handler for execution
    let run_params = crate::tools::run_script::RunScriptParams {
        strategy: Some(params.strategy.clone()),
        script: None,
        params: params.params.clone(),
        profile: None,
    };

    let exec_result = crate::server::handlers::run_script::execute(server, run_params).await?;
    let strategy_key = exec_result
        .resolved_strategy_id
        .unwrap_or_else(|| params.strategy.clone());
    let response = exec_result.response;

    let symbol = params
        .params
        .get("SYMBOL")
        .and_then(Value::as_str)
        .unwrap_or("UNKNOWN")
        .to_owned();
    let capital = params
        .params
        .get("CAPITAL")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);

    // Persist via the existing backtest persistence helper
    let (run_id, _created_at) = crate::server::handlers::backtests::persist_backtest(
        run_store.as_ref(),
        &strategy_key,
        &symbol,
        capital,
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

/// Run a parameter sweep and persist the results.
#[allow(clippy::too_many_lines)]
async fn execute_sweep(
    server: &OptopsyServer,
    params: BacktestToolParams,
) -> Result<BacktestToolResponse, anyhow::Error> {
    let run_store = server
        .run_store
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Run store not configured — cannot persist results"))?;

    let strategy_store = server
        .strategy_store
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Strategy store not configured"))?;

    // 1. Resolve strategy source
    let (strategy_key, script_source) =
        resolve_strategy_source_from_store(strategy_store.as_ref(), &params.strategy)
            .map_err(|(_status, msg)| anyhow::anyhow!("{msg}"))?;

    // 2. Parse script meta
    let script_meta = crate::scripting::stdlib::parse_script_meta(&strategy_key, &script_source);

    // 3. Create data loader (Arc-wrapped for concurrent sweep tasks)
    let loader: Arc<dyn DataLoader> = Arc::new(CachingDataLoader::new(
        Arc::clone(&server.cache),
        server.adjustment_store.clone(),
    ));

    // 4. Extract symbol from params
    let symbol = params
        .params
        .get("SYMBOL")
        .and_then(Value::as_str)
        .unwrap_or("UNKNOWN")
        .to_owned();

    // 5. Build CreateSweepRequest for the shared helpers
    let req = CreateSweepRequest {
        strategy: params.strategy.clone(),
        mode: params.mode.clone(),
        objective: params.objective.clone(),
        params: params.params.clone(),
        sweep_params: params.sweep_params.clone(),
        max_evaluations: params.max_evaluations,
        num_permutations: params.num_permutations,
    };

    // 6. No-op cancellation — cancellation is handled via /tasks/* endpoints
    let is_cancelled: CancelCallback = Box::new(|| false);

    // 7. Run grid or bayesian sweep
    let num_permutations = params.num_permutations;
    let sweep_response: SweepResponse = match params.mode.as_str() {
        "grid" => {
            let param_grid = build_grid(&req.sweep_params);
            let config = GridSweepConfig {
                script_source,
                base_params: req.params.clone(),
                param_grid,
                objective: req.objective.clone(),
            };
            run_grid_sweep(&config, Arc::clone(&loader), &is_cancelled, |_, _| {}).await?
        }
        "bayesian" => {
            let continuous_params: Vec<(String, f64, f64, bool, Option<f64>)> = req
                .sweep_params
                .iter()
                .map(|sp| {
                    (
                        sp.name.clone(),
                        sp.start,
                        sp.stop,
                        sp.param_type == "int",
                        sp.step,
                    )
                })
                .collect();
            let initial_samples = (params.max_evaluations / 3).max(2);
            let config = BayesianConfig {
                script_source,
                base_params: req.params.clone(),
                continuous_params,
                max_evaluations: params.max_evaluations,
                initial_samples,
                objective: req.objective.clone(),
            };
            run_bayesian(&config, loader.as_ref(), &is_cancelled, |_, _| {}).await?
        }
        other => {
            return Err(anyhow::anyhow!(
                "Invalid mode '{other}', expected 'grid' or 'bayesian'"
            ));
        }
    };

    // 7b. Apply permutation gate (if requested) — CPU-intensive, run off async executor
    let objective = req.objective.clone();
    let sweep_response = if num_permutations > 0 {
        tokio::task::spawn_blocking(move || {
            apply_permutation_gate(sweep_response, num_permutations, &objective, Some(42))
        })
        .await?
    } else {
        sweep_response
    };

    // 8. Persist via persist_sweep with source = "agent"
    let sweep_id = persist_sweep_to_store(
        run_store.as_ref(),
        &strategy_key,
        &symbol,
        &req,
        &sweep_response,
        &script_meta,
        "agent",
        params.thread_id.as_deref(),
    )
    .map_err(|(_status, msg)| anyhow::anyhow!("{msg}"))?;

    // 9. Query back run_ids from the persisted sweep
    let run_ids = run_store
        .get_sweep(&sweep_id)?
        .map(|detail| detail.runs.iter().map(|r| r.id.clone()).collect())
        .unwrap_or_default();

    // 10. Build suggested next steps
    let upper = symbol.to_uppercase();
    let suggested_next_steps = vec![
        format!(
            "[NEXT] Call walk_forward(strategy=\"{}\", symbol=\"{upper}\", params_grid=<top param ranges>) to validate parameter robustness on unseen data",
            params.strategy,
        ),
        format!("[THEN] Call drawdown_analysis(symbol=\"{upper}\") to analyze drawdown episodes and risk profile"),
        format!("[THEN] Call monte_carlo(symbol=\"{upper}\") to simulate forward-looking risk"),
        format!("[TIP] Call factor_attribution(symbol=\"{upper}\") to check if alpha is genuine or factor exposure"),
    ];

    // 11. Return response
    Ok(BacktestToolResponse::Sweep(Box::new(
        SweepBacktestResponse {
            sweep_id,
            run_ids,
            sweep: sweep_response,
            suggested_next_steps,
        },
    )))
}
