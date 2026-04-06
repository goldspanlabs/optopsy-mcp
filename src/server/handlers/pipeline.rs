//! REST API handler for the backtest pipeline.
//!
//! Runs the full analysis pipeline (sweep -> walk-forward -> monte carlo)
//! and returns a `PipelineResponse` with stage statuses.

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

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

/// `POST /runs/pipeline` — run the full pipeline synchronously and return the result.
pub async fn create_pipeline(
    State(state): State<AppState>,
    Json(req): Json<CreatePipelineRequest>,
) -> Result<Json<PipelineResponse>, (StatusCode, String)> {
    use crate::tools::backtest::BacktestToolParams;

    let params = BacktestToolParams {
        strategy: req.strategy,
        mode: req.mode,
        objective: req.objective,
        params: req.params,
        sweep_params: req.sweep_params,
        max_evaluations: req.max_evaluations,
        num_permutations: req.num_permutations,
        thread_id: req.thread_id,
        pipeline: true,
    };

    let result = crate::tools::backtest::execute(&state.server, params)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // The pipeline path always returns BacktestToolResponse::Pipeline
    match result {
        crate::tools::backtest::BacktestToolResponse::Pipeline(response) => Ok(Json(*response)),
        _ => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "Pipeline mode did not return a pipeline response".to_string(),
        )),
    }
}
