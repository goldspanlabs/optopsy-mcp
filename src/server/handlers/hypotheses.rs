//! REST API handler for hypothesis generation.

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use garde::Validate;

use crate::data::cache::validate_path_segment;
use crate::server::state::AppState;
use crate::tools::hypothesis;
use crate::tools::response_types::hypothesis::{HypothesisParams, HypothesisResponse};

/// `POST /hypotheses` — Generate statistical hypotheses for the given symbols.
pub async fn generate_hypotheses(
    State(state): State<AppState>,
    Json(params): Json<HypothesisParams>,
) -> Result<Json<HypothesisResponse>, (StatusCode, String)> {
    params
        .validate()
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Validation error: {e}")))?;

    // Validate all symbols have OHLCV data
    for sym in &params.symbols {
        let upper = sym.to_uppercase();
        validate_path_segment(&upper).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("Invalid symbol \"{sym}\": {e}"),
            )
        })?;
        state
            .server
            .ensure_ohlcv(&upper)
            .map_err(|e| (StatusCode::NOT_FOUND, e))?;
    }

    let cache = state.server.cache.clone();
    let response = hypothesis::execute(&cache, &params)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(response))
}
