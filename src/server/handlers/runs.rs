//! REST API handlers for the unified `/runs` endpoints.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use std::collections::HashMap;

use crate::data::traits::{RunDetail, RunsListResponse, SweepDetail};
use crate::server::state::AppState;

#[derive(Debug, Deserialize, Default)]
pub struct ListRunsQuery {
    pub tag: Option<String>,
}

/// `GET /runs` — List all runs and sweeps, newest first.
/// Supports optional `?tag=` filter for peer lookup by strategy tags.
pub async fn list_runs(
    State(state): State<AppState>,
    Query(query): Query<ListRunsQuery>,
) -> Result<Json<RunsListResponse>, (StatusCode, String)> {
    let store = state.run_store.clone();
    let tag = query.tag;
    let response = tokio::task::spawn_blocking(move || store.list(tag.as_deref()))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(response))
}

/// `GET /runs/{id}` — Retrieve a full run detail by id.
pub async fn get_run(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<RunDetail>, (StatusCode, String)> {
    let store = state.run_store.clone();
    let detail = tokio::task::spawn_blocking(move || store.get_run(&id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Run not found".to_string()))?;
    Ok(Json(detail))
}

/// `GET /runs/sweep/{sweepId}` — Retrieve full sweep detail with child runs.
pub async fn get_sweep_detail(
    State(state): State<AppState>,
    Path(sweep_id): Path<String>,
) -> Result<Json<SweepDetail>, (StatusCode, String)> {
    let store = state.run_store.clone();
    let detail = tokio::task::spawn_blocking(move || store.get_sweep(&sweep_id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Sweep not found".to_string()))?;
    Ok(Json(detail))
}

/// `DELETE /runs/{id}` — Delete a run by id.
pub async fn delete_run(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let store = state.run_store.clone();
    let deleted = tokio::task::spawn_blocking(move || store.delete_run(&id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, "Run not found".to_string()))
    }
}

/// `DELETE /runs/sweep/{sweepId}` — Delete a sweep and its runs (CASCADE).
pub async fn delete_sweep(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let store = state.run_store.clone();
    let deleted = tokio::task::spawn_blocking(move || store.delete_sweep(&id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, "Sweep not found".to_string()))
    }
}

/// `PATCH /runs/{id}/analysis` — Save AI-generated analysis text for a run.
#[allow(clippy::implicit_hasher)]
pub async fn set_run_analysis(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<HashMap<String, String>>,
) -> Result<StatusCode, (StatusCode, String)> {
    let analysis = body
        .get("analysis")
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "Missing 'analysis' field".to_string(),
            )
        })?
        .clone();
    let store = state.run_store.clone();
    let found = tokio::task::spawn_blocking(move || store.set_run_analysis(&id, &analysis))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if found {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, "Run not found".to_string()))
    }
}

/// `PATCH /runs/sweep/{sweepId}/analysis` — Save AI-generated analysis text for a sweep.
#[allow(clippy::implicit_hasher)]
pub async fn set_sweep_analysis(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<HashMap<String, String>>,
) -> Result<StatusCode, (StatusCode, String)> {
    let analysis = body
        .get("analysis")
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "Missing 'analysis' field".to_string(),
            )
        })?
        .clone();
    let store = state.run_store.clone();
    let found = tokio::task::spawn_blocking(move || store.set_sweep_analysis(&id, &analysis))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if found {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, "Sweep not found".to_string()))
    }
}

/// `PATCH /runs/sweep/{sweepId}/wf-analysis` — Save AI-generated walk-forward analysis text for a sweep.
#[allow(clippy::implicit_hasher)]
pub async fn set_walk_forward_analysis(
    State(state): State<AppState>,
    Path(sweep_id): Path<String>,
    Json(body): Json<HashMap<String, String>>,
) -> Result<StatusCode, (StatusCode, String)> {
    let analysis = body
        .get("analysis")
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "Missing 'analysis' field".to_string(),
            )
        })?
        .clone();
    let store = state.run_store.clone();
    let found =
        tokio::task::spawn_blocking(move || store.set_walk_forward_analysis(&sweep_id, &analysis))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if found {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, "Sweep not found".to_string()))
    }
}
