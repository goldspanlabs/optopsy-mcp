//! REST API handler for walk-forward optimization.

use axum::{extract::State, http::StatusCode, Json};

use crate::engine::walk_forward::{WalkForwardParams, WalkForwardResponse};
use crate::scripting::engine::CachingDataLoader;
use std::sync::Arc;

use crate::server::state::AppState;

/// `POST /walk-forward` — Run walk-forward optimization for a strategy.
pub async fn run_walk_forward(
    State(state): State<AppState>,
    Json(params): Json<WalkForwardParams>,
) -> Result<Json<WalkForwardResponse>, (StatusCode, String)> {
    let loader = CachingDataLoader::new(Arc::clone(&state.server.cache));

    let response = crate::engine::walk_forward::execute(params, &loader)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(response))
}
