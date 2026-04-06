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
use crate::application::workflows;
use crate::server::handlers::sweeps::SweepParamDef;
use crate::server::state::AppState;
use crate::tools::response_types::pipeline::PipelineResponse;
use crate::tools::response_types::workflow::{WorkflowKind, WorkflowResponse};

fn default_mode() -> String {
    "grid".to_string()
}

/// Request body for `POST /runs/baseline-validation`.
#[derive(Debug, Deserialize)]
pub struct CreateBaselineValidationRequest {
    pub strategy: String,
    #[serde(default = "default_mode")]
    pub mode: String,
    #[serde(default = "crate::application::sweeps::default_objective")]
    pub objective: String,
    pub params: HashMap<String, Value>,
    pub sweep_params: Vec<SweepParamDef>,
    #[serde(default = "crate::application::sweeps::default_max_evaluations")]
    pub max_evaluations: usize,
    #[serde(default)]
    pub num_permutations: usize,
    #[serde(default)]
    pub thread_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateWorkflowRequest {
    #[serde(default = "default_workflow")]
    pub workflow: WorkflowKind,
    pub strategy: String,
    #[serde(default = "default_mode")]
    pub mode: String,
    #[serde(default = "crate::application::sweeps::default_objective")]
    pub objective: String,
    pub params: HashMap<String, Value>,
    pub sweep_params: Vec<SweepParamDef>,
    #[serde(default = "crate::application::sweeps::default_max_evaluations")]
    pub max_evaluations: usize,
    #[serde(default)]
    pub num_permutations: usize,
    #[serde(default)]
    pub thread_id: Option<String>,
}

fn default_workflow() -> WorkflowKind {
    WorkflowKind::BaselineValidation
}

pub(super) fn build_pipeline_params(
    req: CreateBaselineValidationRequest,
) -> Result<pipeline::PipelineRequest, (StatusCode, String)> {
    if req.strategy.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "Validation error: strategy must have length at least 1".to_string(),
        ));
    }

    if req.sweep_params.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "sweep_params must be non-empty for pipeline execution".to_string(),
        ));
    }

    if !matches!(req.mode.as_str(), "grid" | "bayesian") {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "Validation error: mode must be one of ['grid', 'bayesian'], got '{}'",
                req.mode
            ),
        ));
    }

    if req.num_permutations > 100_000 {
        return Err((
            StatusCode::BAD_REQUEST,
            "Validation error: num_permutations must be <= 100000".to_string(),
        ));
    }

    Ok(pipeline::PipelineRequest {
        strategy: req.strategy,
        mode: req.mode,
        objective: req.objective,
        params: req.params,
        sweep_params: req.sweep_params,
        max_evaluations: req.max_evaluations,
        num_permutations: req.num_permutations,
        thread_id: req.thread_id,
    })
}

pub(super) fn build_workflow_params(
    req: CreateWorkflowRequest,
) -> Result<workflows::WorkflowRequest, (StatusCode, String)> {
    let workflow = req.workflow;
    let pipeline = build_pipeline_params(CreateBaselineValidationRequest {
        strategy: req.strategy,
        mode: req.mode,
        objective: req.objective,
        params: req.params,
        sweep_params: req.sweep_params,
        max_evaluations: req.max_evaluations,
        num_permutations: req.num_permutations,
        thread_id: req.thread_id,
    })?;

    Ok(workflows::WorkflowRequest {
        kind: workflow,
        pipeline,
    })
}

/// `POST /runs/baseline-validation` — run the baseline validation workflow synchronously.
pub async fn create_baseline_validation(
    State(state): State<AppState>,
    Json(req): Json<CreateBaselineValidationRequest>,
) -> Result<Json<PipelineResponse>, (StatusCode, String)> {
    let params = build_pipeline_params(req)?;

    let result = pipeline::execute(&state.server, &params, "manual")
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(result))
}

/// `POST /runs/workflows` — run a named workflow synchronously and return the result.
pub async fn create_workflow(
    State(state): State<AppState>,
    Json(req): Json<CreateWorkflowRequest>,
) -> Result<Json<WorkflowResponse>, (StatusCode, String)> {
    let params = build_workflow_params(req)?;

    let result = workflows::execute(&state.server, &params, "manual")
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(result))
}
