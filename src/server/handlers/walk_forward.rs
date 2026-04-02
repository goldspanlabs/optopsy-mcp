//! REST API handler for walk-forward optimization.
//!
//! Delegates to the shared `tools::walk_forward::execute()` so both MCP and REST
//! return the same `WalkForwardResponse`.

use axum::{extract::State, http::StatusCode, Json};
use garde::Validate;
use std::sync::Arc;

use crate::server::params::WalkForwardToolParams;
use crate::server::state::AppState;
use crate::tools::response_types::walk_forward::WalkForwardResponse;
use crate::tools::walk_forward as wf_tool;

/// `POST /walk-forward` — Run walk-forward optimization for a strategy.
pub async fn run_walk_forward(
    State(state): State<AppState>,
    Json(params): Json<WalkForwardToolParams>,
) -> Result<Json<WalkForwardResponse>, (StatusCode, String)> {
    params
        .validate()
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Validation error: {e}")))?;

    let cache = Arc::clone(&state.server.cache);
    let response = wf_tool::execute(
        &cache,
        state.server.adjustment_store.clone(),
        &params.strategy,
        &params.symbol,
        params.capital,
        params.params_grid,
        params.objective,
        Some(params.n_windows),
        params.mode,
        Some(params.train_pct),
        params.start_date,
        params.end_date,
        params.profile,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(response))
}
