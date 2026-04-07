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
use crate::application::sweeps;
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
    #[serde(default)]
    pub sweep_params: Vec<SweepParamDef>,
    /// Name of a sweep profile declared in the strategy script (e.g. "quick", "comprehensive").
    /// When set and `sweep_params` is empty, the sweep grid is loaded from the script metadata.
    #[serde(default)]
    pub sweep_profile: Option<String>,
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
    #[serde(default)]
    pub sweep_params: Vec<SweepParamDef>,
    /// Name of a sweep profile declared in the strategy script (e.g. "quick", "comprehensive").
    /// When set and `sweep_params` is empty, the sweep grid is loaded from the script metadata.
    #[serde(default)]
    pub sweep_profile: Option<String>,
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

/// Resolve sweep params from a profile declared in the strategy script.
fn resolve_sweep_params_from_script(
    state: &AppState,
    strategy: &str,
    profile_name: &str,
) -> Result<Vec<SweepParamDef>, (StatusCode, String)> {
    let store = state
        .server
        .strategy_store
        .as_ref()
        .ok_or((StatusCode::BAD_REQUEST, "No strategy store".to_string()))?;

    let (_id, source) = sweeps::resolve_strategy_source_from_store(store.as_ref(), strategy)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let mut meta = crate::scripting::stdlib::parse_script_meta(strategy, &source);
    meta.params = crate::scripting::stdlib::extract_extern_params(&source);
    let profiles = meta.sweep_profiles.ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            format!("Strategy '{}' has no sweep profiles defined", meta.name),
        )
    })?;

    let params = profiles.get(profile_name).ok_or_else(|| {
        let available: Vec<&String> = profiles.keys().collect();
        (
            StatusCode::BAD_REQUEST,
            format!(
                "Sweep profile '{}' not found for strategy '{}'. Available: {:?}",
                profile_name, meta.name, available
            ),
        )
    })?;

    // Look up param_type from extern declarations in the script metadata
    Ok(params
        .iter()
        .map(|p| {
            let param_type = meta
                .params
                .iter()
                .find(|ep| ep.name == p.name)
                .map_or_else(|| "float".to_string(), |ep| ep.param_type.clone());
            SweepParamDef {
                name: p.name.clone(),
                param_type,
                start: p.start,
                stop: p.stop,
                step: p.step,
            }
        })
        .collect())
}

pub(crate) fn build_pipeline_params(
    req: CreateBaselineValidationRequest,
    state: Option<&AppState>,
) -> Result<pipeline::PipelineRequest, (StatusCode, String)> {
    if req.strategy.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "Validation error: strategy must have length at least 1".to_string(),
        ));
    }

    // Resolve sweep_params: use explicit params, fall back to sweep_profile, or error.
    let sweep_params = if !req.sweep_params.is_empty() {
        req.sweep_params
    } else if let Some(ref profile_name) = req.sweep_profile {
        let app_state = state.ok_or((
            StatusCode::INTERNAL_SERVER_ERROR,
            "Cannot resolve sweep profile without app state".to_string(),
        ))?;
        resolve_sweep_params_from_script(app_state, &req.strategy, profile_name)?
    } else {
        return Err((
            StatusCode::BAD_REQUEST,
            "Either sweep_params or sweep_profile must be provided".to_string(),
        ));
    };

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
        sweep_params,
        max_evaluations: req.max_evaluations,
        num_permutations: req.num_permutations,
        thread_id: req.thread_id,
    })
}

pub(super) fn build_workflow_params(
    req: CreateWorkflowRequest,
    state: Option<&AppState>,
) -> Result<workflows::WorkflowRequest, (StatusCode, String)> {
    let workflow = req.workflow;
    let pipeline = build_pipeline_params(
        CreateBaselineValidationRequest {
            strategy: req.strategy,
            mode: req.mode,
            objective: req.objective,
            params: req.params,
            sweep_params: req.sweep_params,
            sweep_profile: req.sweep_profile,
            max_evaluations: req.max_evaluations,
            num_permutations: req.num_permutations,
            thread_id: req.thread_id,
        },
        state,
    )?;

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
    let params = build_pipeline_params(req, Some(&state))?;

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
    let params = build_workflow_params(req, Some(&state))?;

    let result = workflows::execute(&state.server, &params, "manual")
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(result))
}
