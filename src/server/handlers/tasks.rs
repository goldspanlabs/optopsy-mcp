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

use crate::application::backtests;
use crate::engine::bayesian::{run_bayesian, BayesianConfig};
use crate::engine::permutation::apply_permutation_gate;
use crate::engine::sweep::{run_grid_sweep, GridSweepConfig};
use crate::engine::walk_forward::{WalkForwardParams, WfMode, WfObjective};
use crate::scripting::engine::{CachingDataLoader, CancelCallback, DataLoader};
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
    /// Number of permutations for significance testing. Default 0 (off).
    #[serde(default)]
    pub num_permutations: usize,
    #[serde(default)]
    pub thread_id: Option<String>,
}

fn default_objective() -> String {
    "sharpe".to_string()
}

fn default_max_evaluations() -> usize {
    50
}

#[derive(Debug, Deserialize)]
pub struct SubmitWalkForwardRequest {
    pub strategy: String,
    pub sweep_id: String,
    pub params: HashMap<String, Value>,
    pub sweep_params: Vec<SweepParamDef>,
    #[serde(default = "default_n_windows")]
    pub n_windows: usize,
    #[serde(default = "default_train_pct")]
    pub train_pct: f64,
    #[serde(default = "default_wf_mode")]
    pub mode: String,
    #[serde(default = "default_objective")]
    pub objective: String,
    #[serde(default)]
    pub thread_id: Option<String>,
}

fn default_n_windows() -> usize {
    5
}
fn default_train_pct() -> f64 {
    0.70
}
fn default_wf_mode() -> String {
    "rolling".to_string()
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
    // Symbol from params is a pre-execution hint; actual symbol is resolved
    // from the engine result after the backtest completes.
    let symbol = req
        .params
        .get("symbol")
        .and_then(Value::as_str)
        .unwrap_or("pending")
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

        let result = backtests::execute_script_with_progress(
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

                match backtests::persist_backtest(
                    run_store.as_ref(),
                    &strategy_key,
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
    // Symbol from params is a pre-execution hint; resolved from script later.
    let symbol = req
        .params
        .get("symbol")
        .and_then(Value::as_str)
        .unwrap_or("pending")
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
        let loader: Arc<dyn DataLoader> = Arc::new(CachingDataLoader::new(
            Arc::clone(&server.cache),
            server.adjustment_store.clone(),
        ));

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

        // Resolve symbol from script before script_source is moved into sweep config
        let resolved_symbol = if req.params.contains_key("symbol") {
            symbol.clone()
        } else {
            let syms = crate::scripting::engine::resolve_symbols_from_extern_params(
                &script_source,
                &req.params,
            );
            syms.into_iter().next().unwrap_or(symbol.clone())
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
                run_grid_sweep(&config, Arc::clone(&loader), &is_cancelled, &on_progress).await
            }
            "bayesian" => {
                let continuous_params: Vec<(String, f64, f64, bool, Option<f64>)> = req
                    .sweep_params
                    .iter()
                    .map(|sp| {
                        (
                            sp.name.clone(),
                            sp.start,
                            sp.stop,
                            sp.param_type == "int",
                            sp.step,
                        )
                    })
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
                run_bayesian(&config, loader.as_ref(), &is_cancelled, &on_progress).await
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
                // Apply permutation gate — CPU-intensive, run off async executor
                let n_perms = req.num_permutations;
                let obj = req.objective.clone();
                let sweep_response = if n_perms > 0 {
                    match tokio::task::spawn_blocking(move || {
                        apply_permutation_gate(sweep_response, n_perms, &obj, Some(42))
                    })
                    .await
                    {
                        Ok(r) => r,
                        Err(e) => {
                            tm.mark_failed(&task.id, format!("Permutation gate failed: {e}"));
                            return;
                        }
                    }
                } else {
                    sweep_response
                };
                // Reconstruct CreateSweepRequest for persist_sweep_to_store
                let sweep_req = CreateSweepRequest {
                    strategy: req.strategy.clone(),
                    mode: req.mode.clone(),
                    objective: req.objective.clone(),
                    params: req.params.clone(),
                    sweep_params: req.sweep_params.clone(),
                    max_evaluations: req.max_evaluations,
                    num_permutations: req.num_permutations,
                };

                match persist_sweep_to_store(
                    run_store.as_ref(),
                    &strategy_key,
                    &resolved_symbol,
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

/// `POST /tasks/walk-forward` — Submit a walk-forward validation task.
#[allow(clippy::unused_async, clippy::too_many_lines)]
pub async fn submit_walk_forward(
    State(state): State<AppState>,
    Json(req): Json<SubmitWalkForwardRequest>,
) -> Result<Json<SubmitResponse>, (StatusCode, String)> {
    // Symbol from params; resolved from script source later if not provided
    let symbol = req
        .params
        .get("symbol")
        .and_then(Value::as_str)
        .unwrap_or("pending")
        .to_owned();

    // Will be overwritten with resolved symbol after script is loaded
    let symbol_from_params = req.params.contains_key("symbol");

    let params_json =
        serde_json::to_value(&req.params).unwrap_or(Value::Object(serde_json::Map::default()));

    let task = state.task_manager.register(
        TaskKind::WalkForward,
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

        // Resolve symbol from script if not explicitly in params
        let symbol = if symbol_from_params {
            symbol
        } else {
            crate::scripting::engine::resolve_symbols_from_extern_params(
                &script_source,
                &req.params,
            )
            .into_iter()
            .next()
            .unwrap_or(symbol)
        };

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

        // Map mode string to WfMode enum — reject invalid values
        let wf_mode = match req.mode.as_str() {
            "rolling" => WfMode::Rolling,
            "anchored" => WfMode::Anchored,
            other => {
                tm.mark_failed(&task.id, format!("Invalid walk-forward mode '{other}'"));
                drop(permit);
                return;
            }
        };

        // Map objective string to WfObjective enum — reject invalid values
        let wf_objective = match req.objective.as_str() {
            "sharpe" => WfObjective::Sharpe,
            "sortino" => WfObjective::Sortino,
            "profit_factor" => WfObjective::ProfitFactor,
            "cagr" => WfObjective::Cagr,
            other => {
                tm.mark_failed(
                    &task.id,
                    format!("Invalid walk-forward objective '{other}'"),
                );
                drop(permit);
                return;
            }
        };

        let capital = req
            .params
            .get("CAPITAL")
            .and_then(Value::as_f64)
            .unwrap_or(100_000.0);

        let wf_params = WalkForwardParams {
            strategy: strategy_key.clone(),
            symbol: symbol.clone(),
            capital,
            params_grid: build_grid(&req.sweep_params),
            objective: wf_objective,
            mode: wf_mode,
            n_windows: req.n_windows,
            train_pct: req.train_pct,
            start_date: req
                .params
                .get("START_DATE")
                .and_then(Value::as_str)
                .map(String::from),
            end_date: req
                .params
                .get("END_DATE")
                .and_then(Value::as_str)
                .map(String::from),
            profile: req
                .params
                .get("PROFILE")
                .and_then(Value::as_str)
                .map(String::from),
            script_source: Some(script_source),
            base_params: Some(req.params.clone()),
        };

        let wf_result =
            crate::engine::walk_forward::execute(wf_params, &loader, &is_cancelled, &on_progress)
                .await;

        drop(permit);

        match wf_result {
            Ok(wf_response) => {
                let status = if task.cancellation_token.is_cancelled() {
                    "cancelled"
                } else {
                    "completed"
                };
                let validation_id = uuid::Uuid::new_v4().to_string();

                // Serialize per-window results to JSON for persistence
                let window_results_json = serde_json::to_string(&wf_response.windows).ok();

                match run_store.insert_walk_forward_validation(
                    &validation_id,
                    &req.sweep_id,
                    req.n_windows as i64,
                    req.train_pct,
                    &req.mode,
                    &req.objective,
                    Some(wf_response.efficiency_ratio),
                    Some(wf_response.profitable_windows as i64),
                    Some(wf_response.total_windows as i64),
                    Some(&wf_response.param_stability),
                    status,
                    Some(wf_response.execution_time_ms as i64),
                    window_results_json.as_deref(),
                ) {
                    Ok(_) => {
                        if task.cancellation_token.is_cancelled() {
                            tm.mark_cancelled(&task.id);
                        } else {
                            let result_json = run_store
                                .get_sweep(&req.sweep_id)
                                .ok()
                                .flatten()
                                .and_then(|d| serde_json::to_value(&d).ok())
                                .unwrap_or(Value::Null);
                            tm.mark_completed(&task.id, result_json, validation_id);
                        }
                    }
                    Err(e) => {
                        tm.mark_failed(&task.id, format!("DB insert failed: {e}"));
                    }
                }
            }
            Err(e) => {
                if task.cancellation_token.is_cancelled() {
                    tm.mark_cancelled(&task.id);
                } else {
                    // Mark WF as failed in DB too
                    let failed_id = uuid::Uuid::new_v4().to_string();
                    let _ = run_store.insert_walk_forward_validation(
                        &failed_id,
                        &req.sweep_id,
                        req.n_windows as i64,
                        req.train_pct,
                        &req.mode,
                        &req.objective,
                        None,
                        None,
                        None,
                        None,
                        "failed",
                        None,
                        None,
                    );
                    tm.mark_failed(&task.id, e.to_string());
                }
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

// ──────────────────────────────────────────────────────────────────────────────
// Pipeline task
// ──────────────────────────────────────────────────────────────────────────────

/// `POST /tasks/pipeline` — submit a full pipeline (sweep + walk-forward + monte carlo) as a background task.
pub async fn submit_pipeline(
    State(state): State<AppState>,
    Json(req): Json<super::pipeline::CreatePipelineRequest>,
) -> Result<Json<SubmitResponse>, (StatusCode, String)> {
    let params = super::pipeline::build_pipeline_params(req)?;

    let symbol = params
        .params
        .get("symbol")
        .and_then(Value::as_str)
        .unwrap_or("pending")
        .to_owned();

    let params_json =
        serde_json::to_value(&params.params).unwrap_or(Value::Object(serde_json::Map::default()));

    let task = state.task_manager.register(
        TaskKind::Pipeline,
        &params.strategy,
        &symbol,
        params.thread_id.clone(),
        params_json,
    );
    let task_id = task.id.clone();

    let tm = Arc::clone(&state.task_manager);
    let server = state.server.clone();
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

        let result = crate::tools::backtest::execute(&server, params).await;

        drop(permit);

        if task.cancellation_token.is_cancelled() {
            tm.mark_cancelled(&task.id);
            return;
        }

        match result {
            Ok(crate::tools::backtest::BacktestToolResponse::Pipeline(response)) => {
                let result_json = serde_json::to_value(&*response).unwrap_or(Value::Null);
                tm.mark_completed(&task.id, result_json, response.sweep_id.clone());
            }
            Ok(_) => {
                tm.mark_failed(
                    &task.id,
                    "Pipeline mode did not return a pipeline response".to_string(),
                );
            }
            Err(e) => {
                tm.mark_failed(&task.id, e.to_string());
            }
        }
    });

    Ok(Json(SubmitResponse { task_id }))
}
