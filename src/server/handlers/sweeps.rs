//! REST API handlers for sweep CRUD and execution.

use axum::{
    extract::State,
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

use crate::engine::bayesian::{run_bayesian, BayesianConfig};
use crate::engine::permutation::apply_permutation_gate;
use crate::engine::sweep::{run_grid_sweep, GridSweepConfig};
use crate::scripting::engine::{CachingDataLoader, CancelCallback, DataLoader};
use crate::server::sanitize::{sanitize, trade_row_from_record};
use crate::server::state::AppState;
use crate::tools::response_types::sweep::SweepResponse;

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
    /// Number of permutations for significance testing. Default 0 (off).
    #[serde(default)]
    pub num_permutations: usize,
}

#[derive(Debug, Deserialize, Clone, serde::Serialize, schemars::JsonSchema)]
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

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

use crate::data::traits::TradeRow;

/// Build a Cartesian grid from sweep param definitions.
/// Each param expands from `start` to `stop` (inclusive) by `step`.
pub(crate) fn build_grid(sweep_params: &[SweepParamDef]) -> HashMap<String, Vec<Value>> {
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

/// Resolve strategy source from a strategy store (try by id, then by name).
pub(crate) fn resolve_strategy_source_from_store(
    store: &dyn crate::data::traits::StrategyStore,
    name_or_id: &str,
) -> Result<(String, String), (StatusCode, String)> {
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

/// Resolve strategy source from `AppState` (convenience wrapper).
pub(crate) fn resolve_strategy_source(
    state: &AppState,
    name_or_id: &str,
) -> Result<(String, String), (StatusCode, String)> {
    let store = state.server.strategy_store.as_ref().ok_or_else(|| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Strategy store not configured".to_string(),
        )
    })?;
    resolve_strategy_source_from_store(store.as_ref(), name_or_id)
}

/// Insert sweep results into the run store.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub(crate) fn persist_sweep_to_store(
    run_store: &dyn crate::data::traits::RunStore,
    strategy_key: &str,
    symbol: &str,
    req: &CreateSweepRequest,
    sweep_response: &SweepResponse,
    script_meta: &crate::scripting::stdlib::ScriptMeta,
    source: &str,
    thread_id: Option<&str>,
) -> Result<String, (StatusCode, String)> {
    let sweep_id = uuid::Uuid::new_v4().to_string();

    let sweep_config = serde_json::json!({
        "mode": req.mode,
        "objective": req.objective,
        "sweep_params": req.sweep_params,
        "params": req.params,
        "num_permutations": req.num_permutations,
    });

    let combinations_total = sweep_response.combinations_total as i64;
    let execution_time_ms = sweep_response.execution_time_ms as i64;

    run_store
        .insert_sweep(
            &sweep_id,
            Some(strategy_key),
            symbol,
            &sweep_config,
            &req.objective,
            &req.mode,
            combinations_total,
            Some(execution_time_ms),
            source,
            thread_id,
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Insert each ranked result as a run
    let capital = req
        .params
        .get("CAPITAL")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);

    for (i, result) in sweep_response.ranked_results.iter().enumerate() {
        let run_id = uuid::Uuid::new_v4().to_string();
        let params_value = serde_json::to_value(&result.params)
            .unwrap_or(Value::Object(serde_json::Map::default()));

        // Use full backtest result if available (grid sweep), otherwise empty.
        // Inject script_meta + indicator_data so the FE detail page has full context.
        let full = sweep_response.full_results.get(i);
        let result_json = full.map_or_else(
            || "{}".to_owned(),
            |r| {
                let mut value = serde_json::to_value(&r.result)
                    .unwrap_or(Value::Object(serde_json::Map::default()));
                if let Some(obj) = value.as_object_mut() {
                    obj.remove("trade_log");
                    if let Ok(meta_val) = serde_json::to_value(script_meta) {
                        obj.insert("script_meta".to_string(), meta_val);
                    }
                    let indicators = crate::tools::run_script::format_indicator_data(
                        &r.indicator_data,
                        &r.custom_series,
                    );
                    if let Ok(ind_val) = serde_json::to_value(&indicators) {
                        obj.insert("indicator_data".to_string(), ind_val);
                    }
                }
                serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_owned())
            },
        );
        let m = full.map(|r| &r.result.metrics);

        run_store
            .insert_run(
                &run_id,
                Some(&sweep_id),
                Some(strategy_key),
                symbol,
                capital,
                &params_value,
                Some(sanitize(if capital > 0.0 {
                    result.pnl / capital * 100.0
                } else {
                    0.0
                })),
                Some(sanitize(result.win_rate)),
                Some(sanitize(result.max_drawdown)),
                Some(sanitize(result.sharpe)),
                Some(sanitize(result.sortino)),
                Some(sanitize(result.cagr)),
                Some(sanitize(result.profit_factor)),
                Some(result.trades as i64),
                m.map(|m| sanitize(m.expectancy)),
                m.map(|m| sanitize(m.var_95)),
                result.p_value,
                result.significant,
                &result_json,
                full.map(|r| r.execution_time_ms as i64),
                script_meta.hypothesis.as_deref(),
                script_meta.tags.as_ref().map(|t| t.join(",")).as_deref(),
                script_meta.regime.as_ref().map(|r| r.join(",")).as_deref(),
                source,
                thread_id,
            )
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Store trades if full result is available
        if let Some(full_result) = full {
            let trades: Vec<TradeRow> = full_result
                .result
                .trade_log
                .iter()
                .map(trade_row_from_record)
                .collect();
            run_store
                .insert_trades(&run_id, &trades)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        }
    }

    Ok(sweep_id)
}

/// Convenience wrapper that extracts `run_store` from `AppState`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn persist_sweep(
    state: &AppState,
    strategy_key: &str,
    symbol: &str,
    req: &CreateSweepRequest,
    sweep_response: &SweepResponse,
    script_meta: &crate::scripting::stdlib::ScriptMeta,
    source: &str,
    thread_id: Option<&str>,
) -> Result<String, (StatusCode, String)> {
    persist_sweep_to_store(
        state.run_store.as_ref(),
        strategy_key,
        symbol,
        req,
        sweep_response,
        script_meta,
        source,
        thread_id,
    )
}

// ──────────────────────────────────────────────────────────────────────────────
// Handlers
// ──────────────────────────────────────────────────────────────────────────────

/// `POST /runs/sweep` — Run a sweep with SSE progress updates.
#[allow(clippy::too_many_lines, clippy::unused_async)]
pub async fn create_sweep(
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
        let script_meta =
            crate::scripting::stdlib::parse_script_meta(&strategy_key, &script_source);
        let loader: Arc<dyn DataLoader> = Arc::new(CachingDataLoader::new(
            Arc::clone(&state.server.cache),
            state.server.adjustment_store.clone(),
        ));
        let symbol = req
            .params
            .get("symbol")
            .and_then(Value::as_str)
            .unwrap_or("pending")
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

        // No-op cancellation — cancellation is now handled via /tasks/* endpoints
        let is_cancelled: CancelCallback = Box::new(|| false);

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
                tracing::info!(
                    "Grid sweep: base_params={:?}, param_grid={:?}, sweep_params={:?}",
                    req.params,
                    param_grid,
                    req.sweep_params
                );
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

        ticker.abort();

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
                            let _ = tx
                                .send(Event::default().event("error").data(e.to_string()))
                                .await;
                            return;
                        }
                    }
                } else {
                    sweep_response
                };
                tracing::info!(
                    "Sweep completed: combinations_run={}, ranked_results={}",
                    sweep_response.combinations_run,
                    sweep_response.ranked_results.len()
                );
                match persist_sweep(
                    &state,
                    &strategy_key,
                    &symbol,
                    &req,
                    &sweep_response,
                    &script_meta,
                    "manual",
                    None,
                ) {
                    Ok(sweep_id) => {
                        if let Ok(Some(detail)) = state.run_store.get_sweep(&sweep_id) {
                            let json = serde_json::to_string(&detail).unwrap_or_default();
                            let _ = tx.send(Event::default().event("result").data(json)).await;
                        }
                    }
                    Err((_status, msg)) => {
                        let _ = tx.send(Event::default().event("error").data(msg)).await;
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
