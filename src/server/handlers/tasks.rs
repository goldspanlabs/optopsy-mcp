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

use crate::application::error::{ApplicationError, ApplicationErrorKind};
use crate::application::{backtests, sweeps, tasks as app_tasks, workflows};
use crate::engine::walk_forward::{WalkForwardParams, WfMode, WfObjective};
use crate::scripting::engine::CachingDataLoader;
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
    #[serde(default = "sweeps::default_objective")]
    pub objective: String,
    pub params: HashMap<String, Value>,
    pub sweep_params: Vec<sweeps::SweepParamDef>,
    #[serde(default = "sweeps::default_max_evaluations")]
    pub max_evaluations: usize,
    /// Number of permutations for significance testing. Default 0 (off).
    #[serde(default)]
    pub num_permutations: usize,
    #[serde(default)]
    pub thread_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SubmitWalkForwardRequest {
    pub strategy: String,
    pub sweep_id: String,
    pub params: HashMap<String, Value>,
    pub sweep_params: Vec<sweeps::SweepParamDef>,
    #[serde(default = "default_n_windows")]
    pub n_windows: usize,
    #[serde(default = "default_train_pct")]
    pub train_pct: f64,
    #[serde(default = "default_wf_mode")]
    pub mode: String,
    #[serde(default = "sweeps::default_objective")]
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

fn app_error_message(error: &ApplicationError) -> String {
    match error.kind() {
        ApplicationErrorKind::Storage | ApplicationErrorKind::Internal => {
            format!("DB insert failed: {error}")
        }
        _ => error.to_string(),
    }
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

    let tm = Arc::clone(&state.task_manager);
    let server = state.server.clone();
    let run_store = Arc::clone(&state.run_store);
    tokio::spawn(async move {
        app_tasks::execute_queued_task(tm, Arc::clone(&task), async move {
            let progress_cb = app_tasks::progress_callback(&task);
            let is_cancelled = app_tasks::cancel_callback(&task);

            let run_params = RunScriptParams {
                strategy: Some(req.strategy.clone()),
                script: None,
                params: req.params.clone(),
                profile: req.profile.clone(),
            };

            let exec_result = backtests::execute_script_with_progress(
                &server,
                run_params,
                Some(progress_cb),
                Some(&is_cancelled),
            )
            .await
            .map_err(|e| e.to_string())?;

            let strategy_key = exec_result
                .resolved_strategy_id
                .unwrap_or_else(|| req.strategy.clone());
            let response = exec_result.response;

            let (id, _) = backtests::persist_backtest(
                run_store.as_ref(),
                &strategy_key,
                &req.params,
                &response,
                "manual",
                req.thread_id.as_deref(),
            )
            .map_err(|error| app_error_message(&error))?;

            let result_json = run_store
                .get_run(&id)
                .ok()
                .flatten()
                .and_then(|d| serde_json::to_value(&d).ok())
                .unwrap_or(Value::Null);

            Ok(app_tasks::TaskCompletion {
                result_json,
                result_id: id,
            })
        })
        .await;
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
        app_tasks::execute_queued_task(tm, Arc::clone(&task), async move {
            let progress = app_tasks::progress_callback(&task);
            let is_cancelled = app_tasks::cancel_callback(&task);

            let sweep_req = sweeps::CreateSweepRequest {
                strategy: req.strategy.clone(),
                mode: req.mode.clone(),
                objective: req.objective.clone(),
                params: req.params.clone(),
                sweep_params: req.sweep_params.clone(),
                max_evaluations: req.max_evaluations,
                num_permutations: req.num_permutations,
            };

            let result = sweeps::execute_sweep(
                &server,
                run_store.as_ref(),
                &sweep_req,
                "manual",
                req.thread_id.as_deref(),
                Some(progress),
                Some(&is_cancelled),
            )
            .await
            .map_err(|e| e.to_string())?;

            let result_json = run_store
                .get_sweep(&result.sweep_id)
                .ok()
                .flatten()
                .and_then(|d| serde_json::to_value(&d).ok())
                .unwrap_or(Value::Null);

            Ok(app_tasks::TaskCompletion {
                result_json,
                result_id: result.sweep_id,
            })
        })
        .await;
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
        app_tasks::execute_queued_task(tm, Arc::clone(&task), async move {
            let strategy_store = server.require_strategy_store().map_err(|e| e.to_string())?;
            let (strategy_key, script_source) =
                sweeps::resolve_strategy_source_from_store(strategy_store.as_ref(), &req.strategy)
                    .map_err(|e| e.to_string())?;

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
                CachingDataLoader::new(Arc::clone(&server.cache), server.adjustment_store_handle());
            let is_cancelled = app_tasks::cancel_callback(&task);
            let on_progress = app_tasks::progress_callback(&task);

            let wf_mode = match req.mode.as_str() {
                "rolling" => WfMode::Rolling,
                "anchored" => WfMode::Anchored,
                other => return Err(format!("Invalid walk-forward mode '{other}'")),
            };

            let wf_objective = match req.objective.as_str() {
                "sharpe" => WfObjective::Sharpe,
                "sortino" => WfObjective::Sortino,
                "profit_factor" => WfObjective::ProfitFactor,
                "cagr" => WfObjective::Cagr,
                other => return Err(format!("Invalid walk-forward objective '{other}'")),
            };

            let wf_params = WalkForwardParams {
                strategy: strategy_key,
                symbol,
                capital: req
                    .params
                    .get("CAPITAL")
                    .and_then(Value::as_f64)
                    .unwrap_or(100_000.0),
                params_grid: sweeps::build_grid(&req.sweep_params)?,
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

            let wf_response = match crate::engine::walk_forward::execute(
                wf_params,
                &loader,
                &is_cancelled,
                &on_progress,
            )
            .await
            {
                Ok(response) => response,
                Err(error) => {
                    if !task.cancellation_token.is_cancelled() {
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
                    }
                    return Err(error.to_string());
                }
            };

            let validation_id = uuid::Uuid::new_v4().to_string();
            let window_results_json = serde_json::to_string(&wf_response.windows).ok();
            let status = if task.cancellation_token.is_cancelled() {
                "cancelled"
            } else {
                "completed"
            };

            run_store
                .insert_walk_forward_validation(
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
                )
                .map_err(|e| format!("DB insert failed: {e}"))?;

            let result_json = run_store
                .get_sweep(&req.sweep_id)
                .ok()
                .flatten()
                .and_then(|d| serde_json::to_value(&d).ok())
                .unwrap_or(Value::Null);

            Ok(app_tasks::TaskCompletion {
                result_json,
                result_id: validation_id,
            })
        })
        .await;
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

/// `POST /tasks/baseline-validation` — submit the baseline validation workflow as a background task.
pub async fn submit_baseline_validation(
    State(state): State<AppState>,
    Json(req): Json<super::pipeline::CreateBaselineValidationRequest>,
) -> Result<Json<SubmitResponse>, (StatusCode, String)> {
    let workflow = workflows::WorkflowRequest {
        kind: crate::tools::response_types::workflow::WorkflowKind::BaselineValidation,
        pipeline: super::pipeline::build_pipeline_params(req, Some(&state))?,
    };

    let symbol = workflow
        .pipeline
        .params
        .get("symbol")
        .and_then(Value::as_str)
        .unwrap_or("pending")
        .to_owned();

    let params_json = serde_json::to_value(&workflow.pipeline.params)
        .unwrap_or(Value::Object(serde_json::Map::default()));

    let task = state.task_manager.register(
        TaskKind::Pipeline,
        &workflow.pipeline.strategy,
        &symbol,
        workflow.pipeline.thread_id.clone(),
        params_json,
    );
    let task_id = task.id.clone();

    let tm = Arc::clone(&state.task_manager);
    let server = state.server.clone();
    tokio::spawn(async move {
        Box::pin(app_tasks::execute_queued_task(
            tm,
            Arc::clone(&task),
            async move {
                let response = workflows::execute(&server, &workflow, "manual")
                    .await
                    .map_err(|e| e.to_string())?;
                let result_json = serde_json::to_value(&response).unwrap_or(Value::Null);
                Ok(app_tasks::TaskCompletion {
                    result_json,
                    result_id: workflow
                        .pipeline
                        .thread_id
                        .clone()
                        .unwrap_or_else(|| task.id.clone()),
                })
            },
        ))
        .await;
    });

    Ok(Json(SubmitResponse { task_id }))
}

/// `POST /tasks/workflows` — submit a named workflow as a background task.
pub async fn submit_workflow(
    State(state): State<AppState>,
    Json(req): Json<super::pipeline::CreateWorkflowRequest>,
) -> Result<Json<SubmitResponse>, (StatusCode, String)> {
    let workflow = super::pipeline::build_workflow_params(req, Some(&state))?;

    let symbol = workflow
        .pipeline
        .params
        .get("symbol")
        .and_then(Value::as_str)
        .unwrap_or("pending")
        .to_owned();

    let params_json = serde_json::to_value(&workflow.pipeline.params)
        .unwrap_or(Value::Object(serde_json::Map::default()));

    let task = state.task_manager.register(
        TaskKind::Workflow,
        &workflow.pipeline.strategy,
        &symbol,
        workflow.pipeline.thread_id.clone(),
        params_json,
    );
    let task_id = task.id.clone();

    let tm = Arc::clone(&state.task_manager);
    let server = state.server.clone();
    tokio::spawn(async move {
        Box::pin(app_tasks::execute_queued_task(
            tm,
            Arc::clone(&task),
            async move {
                let response = workflows::execute(&server, &workflow, "manual")
                    .await
                    .map_err(|e| e.to_string())?;
                let result_json = serde_json::to_value(&response).unwrap_or(Value::Null);
                Ok(app_tasks::TaskCompletion {
                    result_json,
                    result_id: task.id.clone(),
                })
            },
        ))
        .await;
    });

    Ok(Json(SubmitResponse { task_id }))
}
