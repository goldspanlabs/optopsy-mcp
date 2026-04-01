//! REST API handlers for task-based backtest and sweep submission.
//!
//! Tasks are queued, executed with concurrency limits, and their progress
//! streamed via reconnectable SSE.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::sse::{Event, KeepAlive, Sse},
    Json,
};
use futures::stream::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::engine::bayesian::{run_bayesian, BayesianConfig};
use crate::engine::sweep::{run_grid_sweep, GridSweepConfig};
use crate::scripting::engine::{CachingDataLoader, CancelCallback};
use crate::server::handlers::sweeps::{
    build_grid, persist_sweep_to_store, resolve_strategy_source_from_store, CreateSweepRequest,
    SweepParamDef,
};
use crate::server::state::AppState;
use crate::server::task_manager::{TaskInfo, TaskKind, TaskStatus};
use crate::tools::run_script::RunScriptParams;

// ──────────────────────────────────────────────────────────────────────────────
// Request / response types
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct SubmitBacktestRequest {
    pub strategy: String,
    pub params: HashMap<String, Value>,
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub thread_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SubmitSweepRequest {
    pub strategy: String,
    pub mode: String,
    #[serde(default = "default_objective")]
    pub objective: String,
    pub params: HashMap<String, Value>,
    pub sweep_params: Vec<SweepParamDef>,
    #[serde(default = "default_max_evaluations")]
    pub max_evaluations: usize,
    #[serde(default)]
    pub thread_id: Option<String>,
}

fn default_objective() -> String {
    "sharpe".to_string()
}

fn default_max_evaluations() -> usize {
    50
}

#[derive(Serialize)]
pub struct SubmitResponse {
    pub task_id: String,
}

/// Serializable snapshot of a task's state.
#[derive(Serialize)]
pub struct TaskSnapshot {
    pub id: String,
    pub kind: TaskKind,
    pub strategy: String,
    pub symbol: String,
    pub status: TaskStatus,
    pub progress_current: usize,
    pub progress_total: usize,
    pub queue_position: Option<usize>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub result: Option<Value>,
    pub result_id: Option<String>,
    pub error: Option<String>,
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

fn snapshot(task: &TaskInfo, queue_pos: Option<usize>) -> TaskSnapshot {
    let m = task.mutable.lock().unwrap();
    TaskSnapshot {
        id: task.id.clone(),
        kind: task.kind,
        strategy: task.strategy.clone(),
        symbol: task.symbol.clone(),
        status: task.status(),
        progress_current: task.progress_current.load(Ordering::Relaxed),
        progress_total: task.progress_total.load(Ordering::Relaxed),
        queue_position: queue_pos,
        created_at: task.created_at.to_rfc3339(),
        started_at: m.started_at.map(|t| t.to_rfc3339()),
        completed_at: m.completed_at.map(|t| t.to_rfc3339()),
        result: m.result.clone(),
        result_id: m.result_id.clone(),
        error: m.error.clone(),
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Handlers
// ──────────────────────────────────────────────────────────────────────────────

/// `POST /tasks/backtest` — Submit a single backtest task.
#[allow(clippy::unused_async, clippy::too_many_lines)]
pub async fn submit_backtest(
    State(state): State<AppState>,
    Json(req): Json<SubmitBacktestRequest>,
) -> Json<SubmitResponse> {
    let symbol = req
        .params
        .get("SYMBOL")
        .and_then(Value::as_str)
        .unwrap_or("UNKNOWN")
        .to_owned();

    let params_json =
        serde_json::to_value(&req.params).unwrap_or(Value::Object(serde_json::Map::default()));

    let task = state.task_manager.register(
        TaskKind::Single,
        &req.strategy,
        &symbol,
        req.thread_id.clone(),
        params_json,
    );
    let task_id = task.id.clone();

    // Spawn the background executor
    let tm = Arc::clone(&state.task_manager);
    let server = state.server.clone();
    let run_store = Arc::clone(&state.run_store);
    tokio::spawn(async move {
        // Wait for permit (or cancellation)
        let permit = tokio::select! {
            p = tm.acquire_permit() => p,
            () = task.cancellation_token.cancelled() => {
                tm.mark_cancelled(&task.id);
                return;
            }
        };

        // Check if cancelled while queued
        if task.cancellation_token.is_cancelled() {
            tm.mark_cancelled(&task.id);
            drop(permit);
            return;
        }

        tm.mark_running(&task.id);

        // Build progress callback from task atomics
        let task_for_progress = Arc::clone(&task);
        let progress_cb: crate::scripting::engine::ProgressCallback =
            Box::new(move |current, total| {
                task_for_progress
                    .progress_current
                    .store(current, Ordering::Relaxed);
                task_for_progress
                    .progress_total
                    .store(total, Ordering::Relaxed);
            });

        // Build cancellation check from task token
        let token = task.cancellation_token.clone();
        let is_cancelled: CancelCallback = Box::new(move || token.is_cancelled());

        let run_params = RunScriptParams {
            strategy: Some(req.strategy.clone()),
            script: None,
            params: req.params.clone(),
            profile: req.profile.clone(),
        };

        let result = super::run_script::execute_with_progress(
            &server,
            run_params,
            Some(progress_cb),
            Some(&is_cancelled),
        )
        .await;

        // Drop permit to allow next task
        drop(permit);

        if task.cancellation_token.is_cancelled() {
            tm.mark_cancelled(&task.id);
            return;
        }

        match result {
            Ok(exec_result) => {
                let strategy_key = exec_result
                    .resolved_strategy_id
                    .unwrap_or_else(|| req.strategy.clone());
                let response = exec_result.response;
                let capital = req
                    .params
                    .get("CAPITAL")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0);

                match super::backtests::persist_backtest(
                    run_store.as_ref(),
                    &strategy_key,
                    &symbol,
                    capital,
                    &req.params,
                    &response,
                    "manual",
                    req.thread_id.as_deref(),
                ) {
                    Ok((id, _)) => {
                        // Fetch the full run detail as the result
                        let result_json = run_store
                            .get_run(&id)
                            .ok()
                            .flatten()
                            .and_then(|d| serde_json::to_value(&d).ok())
                            .unwrap_or(Value::Null);
                        tm.mark_completed(&task.id, result_json, id);
                    }
                    Err((_status, msg)) => {
                        tm.mark_failed(&task.id, format!("DB insert failed: {msg}"));
                    }
                }
            }
            Err(e) => {
                tm.mark_failed(&task.id, e.to_string());
            }
        }
    });

    Json(SubmitResponse { task_id })
}

/// `POST /tasks/sweep` — Submit a sweep task.
#[allow(clippy::unused_async, clippy::too_many_lines)]
pub async fn submit_sweep(
    State(state): State<AppState>,
    Json(req): Json<SubmitSweepRequest>,
) -> Result<Json<SubmitResponse>, (StatusCode, String)> {
    let symbol = req
        .params
        .get("SYMBOL")
        .and_then(Value::as_str)
        .unwrap_or("UNKNOWN")
        .to_owned();

    let params_json =
        serde_json::to_value(&req.params).unwrap_or(Value::Object(serde_json::Map::default()));

    let task = state.task_manager.register(
        TaskKind::Sweep,
        &req.strategy,
        &symbol,
        req.thread_id.clone(),
        params_json,
    );
    let task_id = task.id.clone();

    let tm = Arc::clone(&state.task_manager);
    let server = state.server.clone();
    let run_store = Arc::clone(&state.run_store);
    tokio::spawn(async move {
        // Wait for permit (or cancellation)
        let permit = tokio::select! {
            p = tm.acquire_permit() => p,
            () = task.cancellation_token.cancelled() => {
                tm.mark_cancelled(&task.id);
                return;
            }
        };

        if task.cancellation_token.is_cancelled() {
            tm.mark_cancelled(&task.id);
            drop(permit);
            return;
        }

        tm.mark_running(&task.id);

        // Resolve strategy
        let Some(strategy_store) = server.strategy_store.as_ref() else {
            tm.mark_failed(&task.id, "Strategy store not configured".to_string());
            drop(permit);
            return;
        };

        let (strategy_key, script_source) =
            match resolve_strategy_source_from_store(strategy_store.as_ref(), &req.strategy) {
                Ok(v) => v,
                Err((_status, msg)) => {
                    tm.mark_failed(&task.id, msg);
                    drop(permit);
                    return;
                }
            };

        let script_meta =
            crate::scripting::stdlib::parse_script_meta(&strategy_key, &script_source);
        let loader =
            CachingDataLoader::new(Arc::clone(&server.cache), server.adjustment_store.clone());

        // Build cancellation check from task token
        let token = task.cancellation_token.clone();
        let is_cancelled: CancelCallback = Box::new(move || token.is_cancelled());

        // Progress callback using task atomics
        let task_for_progress = Arc::clone(&task);
        let on_progress = move |cur: usize, tot: usize| {
            task_for_progress
                .progress_current
                .store(cur, Ordering::Relaxed);
            task_for_progress
                .progress_total
                .store(tot, Ordering::Relaxed);
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
                tm.mark_failed(&task.id, format!("Invalid sweep mode '{other}'"));
                drop(permit);
                return;
            }
        };

        drop(permit);

        if task.cancellation_token.is_cancelled() {
            tm.mark_cancelled(&task.id);
            return;
        }

        match sweep_result {
            Ok(sweep_response) => {
                // Reconstruct CreateSweepRequest for persist_sweep_to_store
                let sweep_req = CreateSweepRequest {
                    strategy: req.strategy.clone(),
                    mode: req.mode.clone(),
                    objective: req.objective.clone(),
                    params: req.params.clone(),
                    sweep_params: req.sweep_params.clone(),
                    max_evaluations: req.max_evaluations,
                };

                match persist_sweep_to_store(
                    run_store.as_ref(),
                    &strategy_key,
                    &symbol,
                    &sweep_req,
                    &sweep_response,
                    &script_meta,
                    "manual",
                    req.thread_id.as_deref(),
                ) {
                    Ok(sweep_id) => {
                        let result_json = run_store
                            .get_sweep(&sweep_id)
                            .ok()
                            .flatten()
                            .and_then(|d| serde_json::to_value(&d).ok())
                            .unwrap_or(Value::Null);
                        tm.mark_completed(&task.id, result_json, sweep_id);
                    }
                    Err((_status, msg)) => {
                        tm.mark_failed(&task.id, format!("DB insert failed: {msg}"));
                    }
                }
            }
            Err(e) => {
                tm.mark_failed(&task.id, e.to_string());
            }
        }
    });

    Ok(Json(SubmitResponse { task_id }))
}

/// `GET /tasks` — List active (queued + running) tasks.
#[allow(clippy::unused_async)]
pub async fn list_tasks(State(state): State<AppState>) -> Json<Vec<TaskSnapshot>> {
    let active = state.task_manager.list_active();
    let snapshots: Vec<TaskSnapshot> = active
        .iter()
        .map(|t| {
            let pos = state.task_manager.queue_position(&t.id);
            snapshot(t, pos)
        })
        .collect();
    Json(snapshots)
}

/// `GET /tasks/{id}` — Get a single task's status.
#[allow(clippy::unused_async)]
pub async fn get_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<TaskSnapshot>, StatusCode> {
    let task = state.task_manager.get(&id).ok_or(StatusCode::NOT_FOUND)?;
    let pos = state.task_manager.queue_position(&id);
    Ok(Json(snapshot(&task, pos)))
}

/// `DELETE /tasks/{id}` — Cancel a task.
#[allow(clippy::unused_async)]
pub async fn cancel_task(State(state): State<AppState>, Path(id): Path<String>) -> StatusCode {
    if state.task_manager.cancel(&id) {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

/// `GET /tasks/{id}/stream` — SSE progress stream (reconnectable).
///
/// Sends the current state immediately on connect, then polls every 200ms
/// until the task reaches a terminal state.
#[allow(clippy::unused_async, clippy::too_many_lines)]
pub async fn stream_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>, StatusCode> {
    let task = state.task_manager.get(&id).ok_or(StatusCode::NOT_FOUND)?;
    let tm = Arc::clone(&state.task_manager);

    let (tx, rx) = tokio::sync::mpsc::channel::<Event>(32);

    tokio::spawn(async move {
        /// Helper: send an SSE event; returns false if the client disconnected.
        async fn emit(tx: &tokio::sync::mpsc::Sender<Event>, event: &str, data: String) -> bool {
            tx.send(Event::default().event(event).data(data))
                .await
                .is_ok()
        }

        let status = task.status();

        // ── Send initial state based on current status ──
        match status {
            TaskStatus::Queued => {
                let pos = tm.queue_position(&task.id).unwrap_or(0);
                emit(&tx, "queued", format!(r#"{{"position":{pos}}}"#)).await;
            }
            TaskStatus::Running => {
                let cur = task.progress_current.load(Ordering::Relaxed);
                let tot = task.progress_total.load(Ordering::Relaxed);
                emit(
                    &tx,
                    "progress",
                    format!(r#"{{"current":{cur},"total":{tot}}}"#),
                )
                .await;
            }
            TaskStatus::Completed => {
                let result_str = {
                    let m = task.mutable.lock().unwrap();
                    m.result.as_ref().map(std::string::ToString::to_string)
                };
                if let Some(s) = result_str {
                    emit(&tx, "result", s).await;
                }
                emit(&tx, "done", String::new()).await;
                return;
            }
            TaskStatus::Failed => {
                let msg = {
                    let m = task.mutable.lock().unwrap();
                    m.error.clone().unwrap_or_default()
                };
                emit(&tx, "error", msg).await;
                emit(&tx, "done", String::new()).await;
                return;
            }
            TaskStatus::Cancelled => {
                emit(&tx, "cancelled", String::new()).await;
                emit(&tx, "done", String::new()).await;
                return;
            }
        }

        // ── Track status transitions for "started" event ──
        let mut prev_status = status;

        // ── Poll loop ──
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(200));
        loop {
            interval.tick().await;

            let status = task.status();
            let current = task.progress_current.load(Ordering::Relaxed);
            let total = task.progress_total.load(Ordering::Relaxed);

            // Detect transition from Queued → Running and send "started"
            if prev_status == TaskStatus::Queued && status == TaskStatus::Running {
                emit(&tx, "started", String::new()).await;
            }
            prev_status = status;

            match status {
                TaskStatus::Completed => {
                    let result_str = {
                        let m = task.mutable.lock().unwrap();
                        m.result.as_ref().map(std::string::ToString::to_string)
                    };
                    if let Some(s) = result_str {
                        emit(&tx, "result", s).await;
                    }
                    emit(&tx, "done", String::new()).await;
                    break;
                }
                TaskStatus::Failed => {
                    let msg = {
                        let m = task.mutable.lock().unwrap();
                        m.error.clone().unwrap_or_default()
                    };
                    emit(&tx, "error", msg).await;
                    emit(&tx, "done", String::new()).await;
                    break;
                }
                TaskStatus::Cancelled => {
                    emit(&tx, "cancelled", String::new()).await;
                    emit(&tx, "done", String::new()).await;
                    break;
                }
                TaskStatus::Running => {
                    let data = format!(r#"{{"current":{current},"total":{total}}}"#);
                    if !emit(&tx, "progress", data).await {
                        break;
                    }
                }
                TaskStatus::Queued => {
                    let pos = tm.queue_position(&task.id).unwrap_or(0);
                    if !emit(&tx, "queued", format!(r#"{{"position":{pos}}}"#)).await {
                        break;
                    }
                }
            }
        }
    });

    Ok(
        Sse::new(tokio_stream::wrappers::ReceiverStream::new(rx).map(Ok))
            .keep_alive(KeepAlive::default()),
    )
}
