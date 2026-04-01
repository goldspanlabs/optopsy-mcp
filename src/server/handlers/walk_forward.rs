//! REST API handler for walk-forward optimization.
//!
//! Delegates to the shared `tools::walk_forward::execute()` so both MCP and REST
//! return the same `WalkForwardResponse`.

use axum::{extract::State, http::StatusCode, Json};
use std::sync::Arc;

use crate::server::state::AppState;
use crate::tools::response_types::walk_forward::WalkForwardResponse;
use crate::tools::walk_forward as wf_tool;

/// REST parameters — mirrors the MCP tool params but uses serde defaults directly.
#[derive(Debug, serde::Deserialize)]
pub struct RestWalkForwardParams {
    pub strategy: String,
    pub symbol: String,
    #[serde(default = "default_capital")]
    pub capital: f64,
    pub params_grid: std::collections::HashMap<String, Vec<serde_json::Value>>,
    #[serde(default)]
    pub objective: Option<String>,
    #[serde(default)]
    pub n_windows: Option<usize>,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub train_pct: Option<f64>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub profile: Option<String>,
}

fn default_capital() -> f64 {
    100_000.0
}

/// `POST /walk-forward` — Run walk-forward optimization for a strategy.
pub async fn run_walk_forward(
    State(state): State<AppState>,
    Json(params): Json<RestWalkForwardParams>,
) -> Result<Json<WalkForwardResponse>, (StatusCode, String)> {
    let response = wf_tool::execute(
        &Arc::clone(&state.server.cache),
        state.server.adjustment_store.clone(),
        &params.strategy,
        &params.symbol,
        params.capital,
        params.params_grid,
        params.objective,
        params.n_windows,
        params.mode,
        params.train_pct,
        params.start_date,
        params.end_date,
        params.profile,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(response))
}
