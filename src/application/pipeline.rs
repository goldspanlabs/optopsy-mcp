//! Shared pipeline orchestration used by transport adapters.

use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;

use crate::application::sweeps;
use crate::server::OptopsyServer;
use crate::tools::pipeline::StageCallback;
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

impl Clone for PipelineRequest {
    fn clone(&self) -> Self {
        Self {
            strategy: self.strategy.clone(),
            mode: self.mode.clone(),
            objective: self.objective.clone(),
            params: self.params.clone(),
            sweep_params: self.sweep_params.clone(),
            max_evaluations: self.max_evaluations,
            num_permutations: self.num_permutations,
            thread_id: self.thread_id.clone(),
        }
    }
}

/// Execute the full pipeline: persisted sweep followed by pipeline validation stages.
pub async fn execute(
    server: &OptopsyServer,
    request: &PipelineRequest,
    source: &str,
) -> Result<PipelineResponse> {
    execute_with_stage(server, request, source, &None).await
}

/// Execute the full pipeline with an optional stage progress callback.
pub async fn execute_with_stage(
    server: &OptopsyServer,
    request: &PipelineRequest,
    source: &str,
    on_stage: &StageCallback,
) -> Result<PipelineResponse> {
    let run_store = server.require_run_store()?;

    let sweep_req = sweeps::CreateSweepRequest {
        strategy: request.strategy.clone(),
        mode: request.mode.clone(),
        objective: request.objective.clone(),
        params: request.params.clone(),
        sweep_params: request.sweep_params.clone(),
        max_evaluations: request.max_evaluations,
        num_permutations: request.num_permutations,
    };

    if let Some(cb) = on_stage {
        cb("Sweep");
    }

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

    let response = crate::tools::pipeline::run_pipeline(
        server,
        &sweep_result.strategy_key,
        &sweep_result.symbol,
        sweep_result.capital,
        &sweep_result.objective,
        sweep_result.sweep_id.clone(),
        sweep_result.run_ids,
        sweep_result.response,
        request.params.clone(),
        on_stage,
    )
    .await?;

    // Persist pipeline stages to sweeps.analysis for tracking
    let stages_json = serde_json::to_string(&response.stages)?;
    let sweep_id = sweep_result.sweep_id;
    let store = run_store.clone();
    tokio::task::spawn_blocking(move || store.set_sweep_analysis(&sweep_id, &stages_json))
        .await??;

    Ok(response)
}
