//! MCP tool handler for `parameter_sweep` — run a parameter sweep and persist results.

use std::collections::HashMap;
use std::sync::Arc;

use garde::Validate;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::engine::bayesian::{run_bayesian, BayesianConfig};
use crate::engine::sweep::{run_grid_sweep, GridSweepConfig};
use crate::scripting::engine::CachingDataLoader;
use crate::server::handlers::sweeps::{
    build_grid, persist_sweep_to_store, resolve_strategy_source_from_store, CreateSweepRequest,
    SweepParamDef,
};
use crate::server::OptopsyServer;
use crate::tools::response_types::sweep::SweepResponse;

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

/// Parameters for the `parameter_sweep` MCP tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct ParameterSweepParams {
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

    /// Chat thread ID for associating results with a conversation.
    #[serde(default)]
    #[garde(skip)]
    pub thread_id: Option<String>,
}

// ──────────────────────────────────────────────────────────────────────────────
// Response
// ──────────────────────────────────────────────────────────────────────────────

/// Response from the `parameter_sweep` tool.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ParameterSweepResponse {
    /// Unique sweep ID for referencing the persisted results.
    pub sweep_id: String,
    /// Run IDs for each individual backtest within the sweep.
    pub run_ids: Vec<String>,
    /// Sweep results (ranked combos, sensitivity, timing).
    #[serde(flatten)]
    pub sweep: SweepResponse,
}

// ──────────────────────────────────────────────────────────────────────────────
// Execute
// ──────────────────────────────────────────────────────────────────────────────

/// Run a parameter sweep (or single backtest) and persist the results.
pub async fn execute(
    server: &OptopsyServer,
    params: ParameterSweepParams,
) -> Result<ParameterSweepResponse, anyhow::Error> {
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

    // 3. Create data loader
    let loader = CachingDataLoader::new(Arc::clone(&server.cache));

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
    };

    // 6. Run grid or bayesian sweep
    let no_cancel = || false;
    let sweep_response: SweepResponse = match params.mode.as_str() {
        "grid" => {
            let param_grid = build_grid(&req.sweep_params);
            let config = GridSweepConfig {
                script_source,
                base_params: req.params.clone(),
                param_grid,
                objective: req.objective.clone(),
            };
            run_grid_sweep(&config, &loader, &no_cancel, |_, _| {}).await?
        }
        "bayesian" => {
            let continuous_params: Vec<(String, f64, f64, bool)> = req
                .sweep_params
                .iter()
                .map(|sp| (sp.name.clone(), sp.start, sp.stop, sp.param_type == "int"))
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
            run_bayesian(&config, &loader, &no_cancel, |_, _| {}).await?
        }
        other => {
            return Err(anyhow::anyhow!(
                "Invalid mode '{other}', expected 'grid' or 'bayesian'"
            ));
        }
    };

    // 7. Persist via persist_sweep with source = "agent"
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

    // 8. Query back run_ids from the persisted sweep
    let run_ids = run_store
        .get_sweep(&sweep_id)?
        .map(|detail| detail.runs.iter().map(|r| r.id.clone()).collect())
        .unwrap_or_default();

    // 9. Return response
    Ok(ParameterSweepResponse {
        sweep_id,
        run_ids,
        sweep: sweep_response,
    })
}
