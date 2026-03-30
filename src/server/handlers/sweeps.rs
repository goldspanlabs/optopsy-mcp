//! REST API handlers for sweep CRUD and execution.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::sse::{Event, Sse},
    Json,
};
use futures::stream::{Stream, StreamExt};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use uuid;

use crate::data::traits::{SweepDetail, SweepSummary};
use crate::engine::bayesian::{run_bayesian, BayesianConfig};
use crate::engine::sweep::{run_grid_sweep, GridSweepConfig};
use crate::scripting::engine::CachingDataLoader;
use crate::server::state::AppState;

// ──────────────────────────────────────────────────────────────────────────────
// Request / query types
// ──────────────────────────────────────────────────────────────────────────────

fn default_objective() -> String {
    "sharpe".to_string()
}

fn default_max_evaluations() -> usize {
    50
}

#[derive(Debug, Deserialize)]
pub struct CreateSweepRequest {
    pub strategy: String,
    pub mode: String,
    #[serde(default = "default_objective")]
    pub objective: String,
    pub params: HashMap<String, Value>,
    pub sweep_params: Vec<SweepParamDef>,
    #[serde(default = "default_max_evaluations")]
    pub max_evaluations: usize,
}

#[derive(Debug, Deserialize, Clone, serde::Serialize)]
pub struct SweepParamDef {
    pub name: String,
    #[serde(default = "default_param_type")]
    pub param_type: String, // "int" or "float"
    pub start: f64,
    pub stop: f64,
    pub step: Option<f64>,
}

fn default_param_type() -> String {
    "float".to_string()
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub strategy: Option<String>,
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Build a Cartesian grid from sweep param definitions.
/// Each param expands from `start` to `stop` (inclusive) by `step`.
fn build_grid(sweep_params: &[SweepParamDef]) -> HashMap<String, Vec<Value>> {
    let mut grid: HashMap<String, Vec<Value>> = HashMap::new();
    for sp in sweep_params {
        let is_int = sp.param_type == "int";
        let step = sp.step.unwrap_or(if is_int { 1.0 } else { 0.01 });
        let mut values = Vec::new();
        let mut v = sp.start;
        while v <= sp.stop + f64::EPSILON {
            let rounded = (v * 10_000.0).round() / 10_000.0;
            if is_int {
                values.push(serde_json::json!(rounded as i64));
            } else {
                values.push(serde_json::json!(rounded));
            }
            v += step;
        }
        grid.insert(sp.name.clone(), values);
    }
    grid
}

/// Resolve strategy source from the strategy store (try by id, then by name).
fn resolve_strategy_source(
    state: &AppState,
    name_or_id: &str,
) -> Result<(String, String), (StatusCode, String)> {
    let store = state.server.strategy_store.as_ref().ok_or_else(|| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Strategy store not configured".to_string(),
        )
    })?;

    // Try exact ID match
    if let Ok(Some(source)) = store.get_source(name_or_id) {
        return Ok((name_or_id.to_string(), source));
    }
    // Fall back to case-insensitive name match
    if let Ok(Some((id, source))) = store.get_source_by_name(name_or_id) {
        return Ok((id, source));
    }

    Err((
        StatusCode::NOT_FOUND,
        format!("Strategy '{name_or_id}' not found"),
    ))
}

// ──────────────────────────────────────────────────────────────────────────────
// Handlers
// ──────────────────────────────────────────────────────────────────────────────

/// `POST /sweeps` — Run a parameter sweep and persist the result.
#[allow(clippy::too_many_lines)]
pub async fn create_sweep(
    State(state): State<AppState>,
    Json(req): Json<CreateSweepRequest>,
) -> Result<(StatusCode, Json<SweepDetail>), (StatusCode, String)> {
    let (strategy_key, script_source) = resolve_strategy_source(&state, &req.strategy)?;
    let loader = CachingDataLoader::new(Arc::clone(&state.server.cache));

    let symbol = req
        .params
        .get("SYMBOL")
        .and_then(Value::as_str)
        .unwrap_or("UNKNOWN")
        .to_owned();

    // Generate a run ID upfront so the cancel endpoint can target it
    let run_id = uuid::Uuid::new_v4().to_string();
    let cancellations = Arc::clone(&state.sweep_cancellations);
    let cancel_run_id = run_id.clone();
    let is_cancelled = move || {
        cancellations
            .lock()
            .is_ok_and(|set| set.contains(&cancel_run_id) || set.contains("__cancel_all__"))
    };

    // Store run_id in state so the FE can reference it for cancellation
    // (we reuse the sweep_cancellations set — the ID is only meaningful while running)
    let sweep_response = match req.mode.as_str() {
        "grid" => {
            let param_grid = build_grid(&req.sweep_params);
            let config = GridSweepConfig {
                script_source,
                base_params: req.params.clone(),
                param_grid,
                objective: req.objective.clone(),
            };
            run_grid_sweep(&config, &loader, &is_cancelled, |_, _| {})
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        }
        "bayesian" => {
            let continuous_params: Vec<(String, f64, f64, bool)> = req
                .sweep_params
                .iter()
                .map(|sp| (sp.name.clone(), sp.start, sp.stop, sp.param_type == "int"))
                .collect();
            let initial_samples = (req.max_evaluations / 3).max(2);
            let config = BayesianConfig {
                script_source,
                base_params: req.params.clone(),
                continuous_params,
                max_evaluations: req.max_evaluations,
                initial_samples,
                objective: req.objective.clone(),
            };
            run_bayesian(&config, &loader, &is_cancelled, |_, _| {})
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        }
        other => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("Invalid mode '{other}', expected 'grid' or 'bayesian'"),
            ));
        }
    };

    // Clean up cancellation flags
    if let Ok(mut set) = state.sweep_cancellations.lock() {
        set.remove(&run_id);
        set.remove("__cancel_all__");
    }

    let result_json = serde_json::to_string(&sweep_response)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let sweep_config = serde_json::json!({
        "mode": req.mode,
        "objective": req.objective,
        "sweep_params": req.sweep_params,
    });

    let combinations_total = sweep_response.combinations_total as i64;
    let execution_time_ms = sweep_response.execution_time_ms as i64;

    let (id, _created_at) = state
        .sweep_store
        .insert(
            &strategy_key,
            &symbol,
            &req.mode,
            &req.objective,
            &sweep_config,
            &result_json,
            combinations_total,
            execution_time_ms,
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let detail = state
        .sweep_store
        .get_detail(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Sweep inserted but not found".to_string(),
            )
        })?;

    Ok((StatusCode::CREATED, Json(detail)))
}

/// `GET /sweeps` — List sweep summaries, optionally filtered by strategy.
#[allow(clippy::unused_async)]
pub async fn list_sweeps(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Vec<SweepSummary>>, (StatusCode, String)> {
    let rows = state
        .sweep_store
        .list(query.strategy.as_deref())
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(rows))
}

/// `GET /sweeps/{id}` — Retrieve full sweep detail by id.
#[allow(clippy::unused_async)]
pub async fn get_sweep(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<SweepDetail>, (StatusCode, String)> {
    let detail = state
        .sweep_store
        .get_detail(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("Sweep '{id}' not found")))?;
    Ok(Json(detail))
}

/// `DELETE /sweeps/{id}` — Delete a sweep by id.
#[allow(clippy::unused_async)]
pub async fn delete_sweep(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let deleted = state
        .sweep_store
        .delete(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, format!("Sweep '{id}' not found")))
    }
}

/// `PATCH /sweeps/{id}/analysis` — Save AI-generated analysis text.
#[allow(clippy::unused_async, clippy::implicit_hasher)]
pub async fn set_sweep_analysis(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<HashMap<String, String>>,
) -> Result<StatusCode, (StatusCode, String)> {
    let analysis = body.get("analysis").ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            "Missing 'analysis' field".to_string(),
        )
    })?;
    let found = state
        .sweep_store
        .set_analysis(&id, analysis)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if found {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, format!("Sweep '{id}' not found")))
    }
}

/// `POST /sweeps/stream` — Run a sweep with SSE progress updates.
#[allow(clippy::too_many_lines)]
pub async fn create_sweep_stream(
    State(state): State<AppState>,
    Json(req): Json<CreateSweepRequest>,
) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Event>(64);

    tokio::spawn(async move {
        let (strategy_key, script_source) = match resolve_strategy_source(&state, &req.strategy) {
            Ok(v) => v,
            Err((_, msg)) => {
                let _ = tx.send(Event::default().event("error").data(msg)).await;
                return;
            }
        };
        let loader = CachingDataLoader::new(Arc::clone(&state.server.cache));
        let symbol = req
            .params
            .get("SYMBOL")
            .and_then(Value::as_str)
            .unwrap_or("UNKNOWN")
            .to_owned();

        // Progress tracking via atomics
        let current = Arc::new(AtomicUsize::new(0));
        let total = Arc::new(AtomicUsize::new(0));

        // Spawn a ticker that sends SSE progress events
        let progress_tx = tx.clone();
        let cur_clone = Arc::clone(&current);
        let tot_clone = Arc::clone(&total);
        let ticker = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
            loop {
                interval.tick().await;
                let c = cur_clone.load(Ordering::Relaxed);
                let t = tot_clone.load(Ordering::Relaxed);
                if t > 0 {
                    let data = format!("{{\"current\":{c},\"total\":{t}}}");
                    if progress_tx
                        .send(Event::default().event("progress").data(data))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                if c >= t && t > 0 {
                    break;
                }
            }
        });

        // Cancellation
        let run_id = uuid::Uuid::new_v4().to_string();
        let cancellations = Arc::clone(&state.sweep_cancellations);
        let cancel_run_id = run_id.clone();
        let is_cancelled = move || {
            cancellations
                .lock()
                .is_ok_and(|set| set.contains(&cancel_run_id) || set.contains("__cancel_all__"))
        };

        // Progress callback
        let cur_cb = Arc::clone(&current);
        let tot_cb = Arc::clone(&total);
        let on_progress = move |cur: usize, tot: usize| {
            cur_cb.store(cur, Ordering::Relaxed);
            tot_cb.store(tot, Ordering::Relaxed);
        };

        let sweep_result = match req.mode.as_str() {
            "grid" => {
                let param_grid = build_grid(&req.sweep_params);
                let config = GridSweepConfig {
                    script_source,
                    base_params: req.params.clone(),
                    param_grid,
                    objective: req.objective.clone(),
                };
                run_grid_sweep(&config, &loader, &is_cancelled, &on_progress).await
            }
            "bayesian" => {
                let continuous_params: Vec<(String, f64, f64, bool)> = req
                    .sweep_params
                    .iter()
                    .map(|sp| (sp.name.clone(), sp.start, sp.stop, sp.param_type == "int"))
                    .collect();
                let initial_samples = (req.max_evaluations / 3).max(2);
                let config = BayesianConfig {
                    script_source,
                    base_params: req.params.clone(),
                    continuous_params,
                    max_evaluations: req.max_evaluations,
                    initial_samples,
                    objective: req.objective.clone(),
                };
                run_bayesian(&config, &loader, &is_cancelled, &on_progress).await
            }
            other => {
                let _ = tx
                    .send(
                        Event::default()
                            .event("error")
                            .data(format!("Invalid mode '{other}'")),
                    )
                    .await;
                return;
            }
        };

        // Clean up cancellation flags
        if let Ok(mut set) = state.sweep_cancellations.lock() {
            set.remove(&run_id);
            set.remove("__cancel_all__");
        }

        ticker.abort();

        match sweep_result {
            Ok(sweep_response) => {
                let result_json = serde_json::to_string(&sweep_response).unwrap_or_default();
                let sweep_config = serde_json::json!({
                    "mode": req.mode,
                    "objective": req.objective,
                    "sweep_params": req.sweep_params,
                });
                let combinations_total = sweep_response.combinations_total as i64;
                let execution_time_ms = sweep_response.execution_time_ms as i64;

                match state.sweep_store.insert(
                    &strategy_key,
                    &symbol,
                    &req.mode,
                    &req.objective,
                    &sweep_config,
                    &result_json,
                    combinations_total,
                    execution_time_ms,
                ) {
                    Ok((id, _)) => {
                        if let Ok(Some(detail)) = state.sweep_store.get_detail(&id) {
                            let json = serde_json::to_string(&detail).unwrap_or_default();
                            let _ = tx.send(Event::default().event("result").data(json)).await;
                        }
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Event::default().event("error").data(e.to_string()))
                            .await;
                    }
                }
            }
            Err(e) => {
                let _ = tx
                    .send(Event::default().event("error").data(e.to_string()))
                    .await;
            }
        }

        let _ = tx.send(Event::default().event("done").data("")).await;
    });

    Sse::new(tokio_stream::wrappers::ReceiverStream::new(rx).map(Ok))
}

/// `POST /sweeps/cancel` — Cancel all running sweeps.
#[allow(clippy::unused_async)]
pub async fn cancel_sweeps(State(state): State<AppState>) -> StatusCode {
    if let Ok(mut set) = state.sweep_cancellations.lock() {
        set.insert("__cancel_all__".to_string());
    }
    StatusCode::NO_CONTENT
}
