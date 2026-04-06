//! Shared pipeline orchestration used by transport adapters.

use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;

use crate::application::sweeps;
use crate::server::OptopsyServer;
use crate::tools::response_types::pipeline::PipelineResponse;

pub struct PipelineRequest {
    pub strategy: String,
    pub mode: String,
    pub objective: String,
    pub params: HashMap<String, Value>,
    pub sweep_params: Vec<sweeps::SweepParamDef>,
    pub max_evaluations: usize,
    pub num_permutations: usize,
    pub thread_id: Option<String>,
}

/// Execute the full pipeline: persisted sweep followed by pipeline validation stages.
pub async fn execute(
    server: &OptopsyServer,
    request: &PipelineRequest,
    source: &str,
) -> Result<PipelineResponse> {
    let run_store = server
        .run_store
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Run store not configured — cannot persist results"))?;

    let sweep_req = sweeps::CreateSweepRequest {
        strategy: request.strategy.clone(),
        mode: request.mode.clone(),
        objective: request.objective.clone(),
        params: request.params.clone(),
        sweep_params: request.sweep_params.clone(),
        max_evaluations: request.max_evaluations,
        num_permutations: request.num_permutations,
    };

    let sweep_result = sweeps::execute_sweep(
        server,
        run_store.as_ref(),
        &sweep_req,
        source,
        request.thread_id.as_deref(),
        None,
        None,
    )
    .await?;

    crate::tools::pipeline::run_pipeline(
        server,
        &sweep_result.strategy_key,
        &sweep_result.symbol,
        sweep_result.capital,
        &sweep_result.objective,
        sweep_result.sweep_id,
        sweep_result.run_ids,
        sweep_result.response,
        request.params.clone(),
    )
    .await
}
