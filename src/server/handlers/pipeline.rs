//! REST API handler for the backtest pipeline.
//!
//! Runs the full analysis pipeline
//! (`sweep` -> `significance_gate` -> `walk-forward` -> `oos_data_gate` -> `monte carlo`)
//! and returns a `PipelineResponse` with stage statuses.
//! Monte Carlo may be skipped when earlier gates do not pass.

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

use crate::application::pipeline;
use crate::server::handlers::sweeps::SweepParamDef;
use crate::server::state::AppState;
use crate::tools::response_types::pipeline::PipelineResponse;

fn default_mode() -> String {
    "grid".to_string()
}

fn default_objective() -> String {
    "sharpe".to_string()
}

fn default_max_evaluations() -> usize {
    50
}

/// Request body for `POST /runs/pipeline`.
#[derive(Debug, Deserialize)]
pub struct CreatePipelineRequest {
    pub strategy: String,
    #[serde(default = "default_mode")]
    pub mode: String,
    #[serde(default = "default_objective")]
    pub objective: String,
    pub params: HashMap<String, Value>,
    pub sweep_params: Vec<SweepParamDef>,
    #[serde(default = "default_max_evaluations")]
    pub max_evaluations: usize,
    #[serde(default)]
    pub num_permutations: usize,
    #[serde(default)]
    pub thread_id: Option<String>,
}

pub(super) fn build_pipeline_params(
    req: CreatePipelineRequest,
) -> Result<pipeline::PipelineRequest, (StatusCode, String)> {
    if req.sweep_params.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "sweep_params must be non-empty for pipeline execution".to_string(),
        ));
    }

    let params = pipeline::PipelineRequest {
        strategy: req.strategy,
        mode: req.mode,
        objective: req.objective,
        params: req.params,
        sweep_params: req.sweep_params,
        max_evaluations: req.max_evaluations,
        num_permutations: req.num_permutations,
        thread_id: req.thread_id,
    };
    if params.strategy.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "Validation error: strategy must have length at least 1".to_string(),
        ));
    }

    Ok(params)
}

/// `POST /runs/pipeline` — run the full pipeline synchronously and return the result.
pub async fn create_pipeline(
    State(state): State<AppState>,
    Json(req): Json<CreatePipelineRequest>,
) -> Result<Json<PipelineResponse>, (StatusCode, String)> {
    let params = build_pipeline_params(req)?;

    let result = pipeline::execute(&state.server, &params, "manual")
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(result))
}
