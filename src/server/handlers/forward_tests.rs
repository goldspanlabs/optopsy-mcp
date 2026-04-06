//! REST API handlers for forward testing (paper trading).
//!
//! Endpoints:
//! - `POST /forward-tests`       — create a new session
//! - `GET  /forward-tests`       — list all sessions
//! - `GET  /forward-tests/{id}`  — get session status with equity curve + drift
//! - `POST /forward-tests/{id}/step` — process new bars
//! - `PATCH /forward-tests/{id}` — update session status (pause/stop/resume)
//! - `DELETE /forward-tests/{id}` — delete a session

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use std::collections::HashMap;

use crate::server::state::AppState;
use crate::tools::forward_test;
use crate::tools::response_types::forward_test::{
    ForwardTestStatusResponse, StartForwardTestResponse, StepForwardTestResponse,
};

// ──────────────────────────────────────────────────────────────────────────────
// Request types
// ──────────────────────────────────────────────────────────────────────────────

/// Request body for `POST /forward-tests`.
#[derive(Debug, Deserialize)]
pub struct CreateForwardTestRequest {
    pub strategy: String,
    pub symbol: String,
    #[serde(default = "default_capital")]
    pub capital: f64,
    #[serde(default)]
    pub params: HashMap<String, serde_json::Value>,
    pub start_date: Option<String>,
    pub baseline_sharpe: Option<f64>,
    pub baseline_win_rate: Option<f64>,
    pub baseline_max_dd: Option<f64>,
}

fn default_capital() -> f64 {
    100_000.0
}

/// Request body for `PATCH /forward-tests/{id}`.
#[derive(Debug, Deserialize)]
pub struct UpdateForwardTestRequest {
    /// New status: "active", "paused", or "stopped".
    pub status: String,
}

/// Query params for `GET /forward-tests`.
#[derive(Debug, Deserialize, Default)]
pub struct ListForwardTestsQuery {
    pub status: Option<String>,
}

// ──────────────────────────────────────────────────────────────────────────────
// Handlers
// ──────────────────────────────────────────────────────────────────────────────

/// `POST /forward-tests` — Create a new forward test session.
pub async fn create_forward_test(
    State(state): State<AppState>,
    Json(body): Json<CreateForwardTestRequest>,
) -> Result<(StatusCode, Json<StartForwardTestResponse>), (StatusCode, String)> {
    let fwd_store = state.forward_test_store.clone();
    let strategy_store = state.server.strategy_store.clone();

    let result = forward_test::start(&forward_test::StartParams {
        store: &fwd_store,
        strategy_store: strategy_store.as_deref(),
        strategy: &body.strategy,
        symbol: &body.symbol,
        capital: body.capital,
        params: &body.params,
        start_date: body.start_date.as_deref(),
        baseline_sharpe: body.baseline_sharpe,
        baseline_win_rate: body.baseline_win_rate,
        baseline_max_dd: body.baseline_max_dd,
    })
    .map_err(|e| {
        let msg = e.to_string();
        // Input validation errors → 400; internal failures → 500
        if msg.contains("must be positive")
            || msg.contains("not found")
            || msg.contains("Required parameter")
        {
            (StatusCode::BAD_REQUEST, msg)
        } else {
            (StatusCode::INTERNAL_SERVER_ERROR, msg)
        }
    })?;

    Ok((StatusCode::CREATED, Json(result)))
}

/// `GET /forward-tests` — List all forward test sessions.
pub async fn list_forward_tests(
    State(state): State<AppState>,
    Query(query): Query<ListForwardTestsQuery>,
) -> Result<Json<Vec<crate::data::forward_test_store::ForwardTestSession>>, (StatusCode, String)> {
    let store = state.forward_test_store.clone();
    let status = query.status;
    let sessions = tokio::task::spawn_blocking(move || store.list_sessions(status.as_deref()))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(sessions))
}

/// `GET /forward-tests/{id}` — Get session status with equity curve and drift analysis.
pub async fn get_forward_test(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ForwardTestStatusResponse>, (StatusCode, String)> {
    let store = state.forward_test_store.clone();
    let result =
        forward_test::status(&store, &id).map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;
    Ok(Json(result))
}

/// `POST /forward-tests/{id}/step` — Process new bars for the session.
pub async fn step_forward_test(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<StepForwardTestResponse>, (StatusCode, String)> {
    let fwd_store = state.forward_test_store.clone();
    let strategy_store = state.server.strategy_store.clone();
    let cache = state.server.cache.clone();
    let adjustment_store = state.server.adjustment_store.clone();

    let result = forward_test::step(
        &fwd_store,
        strategy_store.as_deref(),
        &cache,
        adjustment_store,
        &id,
    )
    .await
    .map_err(|e| {
        let msg = e.to_string();
        if msg.contains("not found") {
            (StatusCode::NOT_FOUND, msg)
        } else if msg.contains("only active") {
            (StatusCode::CONFLICT, msg)
        } else {
            (StatusCode::INTERNAL_SERVER_ERROR, msg)
        }
    })?;

    Ok(Json(result))
}

/// `PATCH /forward-tests/{id}` — Update session status (pause/stop/resume).
pub async fn update_forward_test(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<UpdateForwardTestRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let valid_statuses = ["active", "paused", "stopped"];
    if !valid_statuses.contains(&body.status.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "Invalid status '{}'. Must be one of: {}",
                body.status,
                valid_statuses.join(", ")
            ),
        ));
    }

    let store = state.forward_test_store.clone();
    let status = body.status.clone();
    let id_clone = id.clone();

    // Verify session exists
    let session = tokio::task::spawn_blocking(move || store.get_session(&id_clone))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Session not found".to_string()))?;

    // Validate status transitions
    match (session.status.as_str(), status.as_str()) {
        ("stopped", _) => {
            return Err((
                StatusCode::CONFLICT,
                "Cannot change status of a stopped session".to_string(),
            ))
        }
        (current, new) if current == new => return Ok(StatusCode::NO_CONTENT),
        _ => {}
    }

    let store = state.forward_test_store.clone();
    tokio::task::spawn_blocking(move || store.update_session_status(&id, &status))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /forward-tests/{id}` — Delete a session and all its data.
pub async fn delete_forward_test(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let store = state.forward_test_store.clone();

    // Verify session exists
    let id_clone = id.clone();
    let store_clone = store.clone();
    let exists = tokio::task::spawn_blocking(move || store_clone.get_session(&id_clone))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if exists.is_none() {
        return Err((StatusCode::NOT_FOUND, "Session not found".to_string()));
    }

    tokio::task::spawn_blocking(move || store.delete_session(&id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(StatusCode::NO_CONTENT)
}
