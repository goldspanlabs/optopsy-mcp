//! REST API handlers for strategy CRUD operations.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::Value;

use super::backtests::AppState;
use crate::data::cache::validate_path_segment;
use crate::data::strategy_store::StrategyRow;
use crate::data::traits::StrategyStore;
use crate::scripting::engine::ValidationResult;

/// Request body for `POST /strategies` and `PUT /strategies/{id}`.
#[derive(Debug, Deserialize)]
pub struct UpsertStrategyRequest {
    #[serde(default)]
    pub id: Option<String>,
    pub name: String,
    pub source: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub hypothesis: Option<String>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub regime: Option<Vec<String>>,
}

/// Helper to clone the strategy store `Arc` or return 503.
fn clone_store(state: &AppState) -> Result<Arc<dyn StrategyStore>, (StatusCode, String)> {
    state.server.strategy_store.clone().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Strategy store not configured".to_string(),
        )
    })
}

/// `GET /strategies` — List all strategies as `ScriptMeta`.
pub async fn list_strategies(
    State(state): State<AppState>,
) -> Result<Json<Vec<crate::scripting::stdlib::ScriptMeta>>, (StatusCode, String)> {
    let store = clone_store(&state)?;
    let scripts = tokio::task::spawn_blocking(move || store.list_scripts())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(scripts))
}

/// `GET /strategies/{id}` — Return a single strategy by id.
pub async fn get_strategy(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<StrategyRow>, (StatusCode, String)> {
    validate_path_segment(&id)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid id: {e}")))?;
    let store = clone_store(&state)?;
    let row = tokio::task::spawn_blocking(move || store.get(&id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Strategy not found".to_string()))?;
    Ok(Json(row))
}

/// `POST /strategies` — Create a new strategy.
pub async fn create_strategy(
    State(state): State<AppState>,
    Json(req): Json<UpsertStrategyRequest>,
) -> Result<(StatusCode, Json<StrategyRow>), (StatusCode, String)> {
    let id = req.id.ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            "Field 'id' is required".to_string(),
        )
    })?;
    validate_path_segment(&id)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid id: {e}")))?;

    let row = StrategyRow {
        id,
        name: req.name,
        description: req.description,
        category: req.category,
        hypothesis: req.hypothesis,
        tags: req.tags,
        regime: req.regime,
        source: req.source,
        created_at: String::new(),
        updated_at: String::new(),
    };

    let store = clone_store(&state)?;
    let row_id = row.id.clone();
    let row_clone = row;
    let store2 = store.clone();
    tokio::task::spawn_blocking(move || store.upsert(&row_clone))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Re-fetch to get server-set timestamps
    let created = tokio::task::spawn_blocking(move || store2.get(&row_id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to fetch created strategy".to_string(),
            )
        })?;
    Ok((StatusCode::CREATED, Json(created)))
}

/// `PUT /strategies/{id}` — Update an existing strategy.
pub async fn update_strategy(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpsertStrategyRequest>,
) -> Result<Json<StrategyRow>, (StatusCode, String)> {
    validate_path_segment(&id)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid id: {e}")))?;

    // Reject mismatched body id
    if let Some(ref body_id) = req.id {
        if body_id != &id {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("Body id '{body_id}' does not match path id '{id}'"),
            ));
        }
    }

    let row = StrategyRow {
        id: id.clone(),
        name: req.name,
        description: req.description,
        category: req.category,
        hypothesis: req.hypothesis,
        tags: req.tags,
        regime: req.regime,
        source: req.source,
        created_at: String::new(),
        updated_at: String::new(),
    };

    let store = clone_store(&state)?;
    let store2 = store.clone();
    let id_clone = id.clone();
    tokio::task::spawn_blocking(move || store.upsert(&row))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let updated = tokio::task::spawn_blocking(move || store2.get(&id_clone))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to fetch updated strategy".to_string(),
            )
        })?;
    Ok(Json(updated))
}

/// `DELETE /strategies/{id}` — Delete a strategy by id.
pub async fn delete_strategy(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    validate_path_segment(&id)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid id: {e}")))?;
    let store = clone_store(&state)?;
    let deleted = tokio::task::spawn_blocking(move || store.delete(&id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, "Strategy not found".to_string()))
    }
}

/// `GET /strategies/{id}/source` — Return raw Rhai source as text/plain.
pub async fn get_strategy_source(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, (StatusCode, String)> {
    validate_path_segment(&id)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid id: {e}")))?;
    let store = clone_store(&state)?;
    let source = tokio::task::spawn_blocking(move || store.get_source(&id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Strategy not found".to_string()))?;
    Ok((
        StatusCode::OK,
        [("content-type", "text/plain; charset=utf-8")],
        source,
    )
        .into_response())
}

/// Request body for `POST /strategies/validate` (inline source).
#[derive(Debug, Deserialize)]
pub struct ValidateScriptRequest {
    pub source: String,
    #[serde(default)]
    pub params: HashMap<String, Value>,
}

/// `POST /strategies/validate` — Validate inline Rhai source without saving.
pub async fn validate_script(Json(req): Json<ValidateScriptRequest>) -> Json<ValidationResult> {
    let result = tokio::task::spawn_blocking(move || {
        crate::scripting::engine::validate_script(&req.source, &req.params)
    })
    .await
    .unwrap_or_else(|e| ValidationResult {
        valid: false,
        diagnostics: vec![crate::scripting::engine::ValidationDiagnostic {
            level: crate::scripting::engine::DiagnosticLevel::Error,
            message: format!("Validation task panicked: {e}"),
        }],
        callbacks: vec![],
        config: None,
        params: vec![],
    });
    Json(result)
}

/// `POST /strategies/{id}/validate` — Validate a stored strategy's source.
#[allow(clippy::implicit_hasher)]
pub async fn validate_stored_strategy(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Option<Json<HashMap<String, Value>>>,
) -> Result<Json<ValidationResult>, (StatusCode, String)> {
    validate_path_segment(&id)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid id: {e}")))?;
    let store = clone_store(&state)?;
    let source = tokio::task::spawn_blocking(move || store.get_source(&id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Strategy not found".to_string()))?;

    let params = body.map(|j| j.0).unwrap_or_default();
    let result = tokio::task::spawn_blocking(move || {
        crate::scripting::engine::validate_script(&source, &params)
    })
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Validation panicked: {e}"),
        )
    })?;

    Ok(Json(result))
}
