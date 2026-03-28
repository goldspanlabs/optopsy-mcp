//! REST API handlers for strategy CRUD operations.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;

use super::backtests::AppState;
use crate::data::cache::validate_path_segment;
use crate::data::strategy_store::StrategyRow;

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

/// Helper to get the strategy store or return 503.
fn get_store(
    state: &AppState,
) -> Result<&dyn crate::data::traits::StrategyStore, (StatusCode, String)> {
    state.server.strategy_store.as_deref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Strategy store not configured".to_string(),
        )
    })
}

/// `GET /strategies` — List all strategies as `ScriptMeta`.
#[allow(clippy::unused_async)]
pub async fn list_strategies(
    State(state): State<AppState>,
) -> Result<Json<Vec<crate::scripting::stdlib::ScriptMeta>>, (StatusCode, String)> {
    let store = get_store(&state)?;
    let scripts = store
        .list_scripts()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(scripts))
}

/// `GET /strategies/{id}` — Return a single strategy by id.
#[allow(clippy::unused_async)]
pub async fn get_strategy(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<StrategyRow>, (StatusCode, String)> {
    let store = get_store(&state)?;
    let row = store
        .get(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("Strategy '{id}' not found")))?;
    Ok(Json(row))
}

/// `POST /strategies` — Create a new strategy.
#[allow(clippy::unused_async)]
pub async fn create_strategy(
    State(state): State<AppState>,
    Json(req): Json<UpsertStrategyRequest>,
) -> Result<(StatusCode, Json<StrategyRow>), (StatusCode, String)> {
    let store = get_store(&state)?;
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
    store
        .upsert(&row)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Re-fetch to get server-set timestamps
    let created = store
        .get(&row.id)
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
#[allow(clippy::unused_async)]
pub async fn update_strategy(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpsertStrategyRequest>,
) -> Result<Json<StrategyRow>, (StatusCode, String)> {
    let store = get_store(&state)?;
    validate_path_segment(&id)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid id: {e}")))?;

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
    store
        .upsert(&row)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let updated = store
        .get(&id)
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
#[allow(clippy::unused_async)]
pub async fn delete_strategy(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let store = get_store(&state)?;
    let deleted = store
        .delete(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, format!("Strategy '{id}' not found")))
    }
}

/// `GET /strategies/{id}/source` — Return raw Rhai source as text/plain.
#[allow(clippy::unused_async)]
pub async fn get_strategy_source(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, (StatusCode, String)> {
    let store = get_store(&state)?;
    let source = store
        .get_source(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("Strategy '{id}' not found")))?;
    Ok((
        StatusCode::OK,
        [("content-type", "text/plain; charset=utf-8")],
        source,
    )
        .into_response())
}
