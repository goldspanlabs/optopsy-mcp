//! Shared pipeline orchestration used by transport adapters.

use anyhow::Result;

use crate::application::sweeps;
use crate::server::OptopsyServer;
use crate::tools::backtest::BacktestToolParams;
use crate::tools::response_types::pipeline::PipelineResponse;

/// Execute the full pipeline: persisted sweep followed by pipeline validation stages.
pub async fn execute(
    server: &OptopsyServer,
    params: &BacktestToolParams,
    source: &str,
) -> Result<PipelineResponse> {
    let run_store = server
        .run_store
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Run store not configured — cannot persist results"))?;

    let sweep_req = sweeps::CreateSweepRequest {
        strategy: params.strategy.clone(),
        mode: params.mode.clone(),
        objective: params.objective.clone(),
        params: params.params.clone(),
        sweep_params: params.sweep_params.clone(),
        max_evaluations: params.max_evaluations,
        num_permutations: params.num_permutations,
    };

    let sweep_result = sweeps::execute_sweep(
        server,
        run_store.as_ref(),
        &sweep_req,
        source,
        params.thread_id.as_deref(),
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
        params.params.clone(),
    )
    .await
}
