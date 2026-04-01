//! REST API handlers for creating and streaming backtests.

use axum::{
    extract::State,
    http::StatusCode,
    response::sse::{Event, Sse},
    Json,
};
use futures::{stream::Stream, StreamExt};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::data::traits::TradeRow;
use crate::server::sanitize::{sanitize, trade_row_from_record};
use crate::server::state::AppState;
use crate::tools::run_script::RunScriptParams;

/// Request body for `POST /runs`.
#[derive(Debug, Deserialize)]
pub struct CreateBacktestRequest {
    pub strategy: String,
    pub params: HashMap<String, Value>,
    #[serde(default)]
    pub profile: Option<String>,
}

/// Optional request body for `POST /runs/cancel`.
#[derive(Debug, Deserialize)]
pub struct CancelRunRequest {
    pub id: Option<String>,
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Build `TradeRow` vector from a `RunScriptResponse`.
fn build_trades(response: &crate::tools::run_script::RunScriptResponse) -> Vec<TradeRow> {
    response
        .result
        .trade_log
        .iter()
        .map(trade_row_from_record)
        .collect()
}

/// Strip trades from a `RunScriptResponse` for storage (trades stored separately).
fn strip_trades_from_result_json(response: &crate::tools::run_script::RunScriptResponse) -> String {
    let mut value =
        serde_json::to_value(response).unwrap_or(Value::Object(serde_json::Map::default()));
    if let Some(obj) = value.as_object_mut() {
        if let Some(result) = obj.get_mut("result") {
            if let Some(result_obj) = result.as_object_mut() {
                result_obj.remove("trade_log");
            }
        }
    }
    serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_owned())
}

/// Insert a backtest result into the run store, returning `(id, created_at)`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn persist_backtest(
    run_store: &dyn crate::data::traits::RunStore,
    strategy_key: &str,
    symbol: &str,
    capital: f64,
    params: &HashMap<String, Value>,
    response: &crate::tools::run_script::RunScriptResponse,
    source: &str,
    thread_id: Option<&str>,
) -> Result<(String, String), (StatusCode, String)> {
    let id = uuid::Uuid::new_v4().to_string();
    let m = &response.result.metrics;
    let trades = build_trades(response);
    let result_json = strip_trades_from_result_json(response);
    let params_value = serde_json::to_value(params)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let hypothesis = response
        .script_meta
        .as_ref()
        .and_then(|m| m.hypothesis.as_deref());
    let tags_str = response
        .script_meta
        .as_ref()
        .and_then(|m| m.tags.as_ref())
        .map(|t| t.join(","));
    let regime_str = response
        .script_meta
        .as_ref()
        .and_then(|m| m.regime.as_ref())
        .map(|r| r.join(","));

    let created_at = run_store
        .insert_run(
            &id,
            None, // no sweep
            Some(strategy_key),
            symbol,
            capital,
            &params_value,
            Some(sanitize(if capital > 0.0 {
                response.result.total_pnl / capital * 100.0
            } else {
                0.0
            })),
            Some(sanitize(m.win_rate)),
            Some(sanitize(m.max_drawdown)),
            Some(sanitize(m.sharpe)),
            Some(sanitize(m.sortino)),
            Some(sanitize(m.cagr)),
            Some(sanitize(m.profit_factor)),
            Some(response.result.trade_count as i64),
            Some(sanitize(m.expectancy)),
            Some(sanitize(m.var_95)),
            &result_json,
            Some(response.execution_time_ms as i64),
            hypothesis,
            tags_str.as_deref(),
            regime_str.as_deref(),
            source,
            thread_id,
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    run_store
        .insert_trades(&id, &trades)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((id, created_at))
}

// ──────────────────────────────────────────────────────────────────────────────
// Handlers
// ──────────────────────────────────────────────────────────────────────────────

/// `POST /runs` — Run a strategy with SSE progress updates.
#[allow(clippy::unused_async, clippy::too_many_lines)]
pub async fn create_backtest(
    State(state): State<AppState>,
    Json(req): Json<CreateBacktestRequest>,
) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Event>(64);

    tokio::spawn(async move {
        // Set up progress tracking via shared atomic
        let progress_current = Arc::new(AtomicUsize::new(0));
        let progress_total = Arc::new(AtomicUsize::new(0));

        let cur = Arc::clone(&progress_current);
        let tot = Arc::clone(&progress_total);
        let progress_cb: crate::scripting::engine::ProgressCallback =
            Box::new(move |current, total| {
                cur.store(current, Ordering::Relaxed);
                tot.store(total, Ordering::Relaxed);
            });

        // Spawn a ticker that reads atomics and sends SSE progress events
        let progress_tx = tx.clone();
        let pc = Arc::clone(&progress_current);
        let pt = Arc::clone(&progress_total);
        let ticker = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(200));
            loop {
                interval.tick().await;
                let c = pc.load(Ordering::Relaxed);
                let t = pt.load(Ordering::Relaxed);
                if t > 0 {
                    let evt = Event::default()
                        .event("progress")
                        .data(format!(r#"{{"current":{c},"total":{t}}}"#));
                    if progress_tx.send(evt).await.is_err() {
                        break;
                    }
                }
            }
        });

        // Cancellation
        let run_id = uuid::Uuid::new_v4().to_string();
        let cancellations = Arc::clone(&state.cancellations);
        let cancel_run_id = run_id.clone();
        let is_cancelled: crate::scripting::engine::CancelCallback = Box::new(move || {
            cancellations
                .lock()
                .is_ok_and(|set| set.contains(&cancel_run_id) || set.contains("__cancel_all__"))
        });

        // Send run_id as first SSE event so the FE can target cancellation
        let _ = tx
            .send(
                Event::default()
                    .event("run_id")
                    .data(format!(r#"{{"id":"{run_id}"}}"#)),
            )
            .await;

        // Run the actual backtest
        let run_params = RunScriptParams {
            strategy: Some(req.strategy.clone()),
            script: None,
            params: req.params.clone(),
            profile: req.profile.clone(),
        };

        let result = super::run_script::execute_with_progress(
            &state.server,
            run_params,
            Some(progress_cb),
            Some(&is_cancelled),
        )
        .await;

        // Stop the progress ticker
        ticker.abort();

        // Clean up cancellation flag
        if let Ok(mut set) = state.cancellations.lock() {
            set.remove(&run_id);
            set.remove("__cancel_all__");
        }

        match result {
            Ok(exec_result) => {
                let strategy_key = exec_result
                    .resolved_strategy_id
                    .unwrap_or_else(|| req.strategy.clone());
                let response = exec_result.response;

                let symbol = req
                    .params
                    .get("SYMBOL")
                    .and_then(Value::as_str)
                    .unwrap_or("UNKNOWN")
                    .to_owned();

                let capital = req
                    .params
                    .get("CAPITAL")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0);

                match persist_backtest(
                    &*state.run_store,
                    &strategy_key,
                    &symbol,
                    capital,
                    &req.params,
                    &response,
                    "manual",
                    None,
                ) {
                    Ok((id, _)) => {
                        if let Ok(Some(detail)) = state.run_store.get_run(&id) {
                            let json = serde_json::to_string(&detail).unwrap_or_default();
                            let _ = tx.send(Event::default().event("result").data(json)).await;
                        }
                    }
                    Err((_status, msg)) => {
                        let _ = tx
                            .send(
                                Event::default()
                                    .event("error")
                                    .data(format!("DB insert failed: {msg}")),
                            )
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

/// `POST /runs/cancel` — Cancel a specific run or all in-flight runs.
#[allow(clippy::unused_async)]
pub async fn cancel_run(
    State(state): State<AppState>,
    body: Option<Json<CancelRunRequest>>,
) -> StatusCode {
    if let Ok(mut set) = state.cancellations.lock() {
        match body.and_then(|b| b.id.clone()) {
            Some(id) => {
                set.insert(id);
            }
            None => {
                set.insert("__cancel_all__".to_string());
            }
        }
    }
    StatusCode::NO_CONTENT
}
