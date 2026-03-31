//! REST API handlers for backtest CRUD operations.
//!
//! These handlers provide backward compatibility with the `/backtests` endpoints
//! while delegating to the unified `RunStore` internally.

use axum::{
    extract::{Path, Query, State},
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

use crate::data::traits::{RunDetail, TradeRow};
use crate::server::state::AppState;
use crate::tools::run_script::RunScriptParams;

/// Replace NaN/Infinity with 0.0 to prevent JSON serialization errors.
pub(crate) fn sanitize(v: f64) -> f64 {
    if v.is_finite() {
        v
    } else {
        0.0
    }
}

/// Request body for `POST /backtests`.
#[derive(Debug, Deserialize)]
pub struct CreateBacktestRequest {
    pub strategy: String,
    pub params: HashMap<String, Value>,
    #[serde(default)]
    pub profile: Option<String>,
}

/// Query parameters for `GET /backtests`.
#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub strategy: Option<String>,
    pub symbol: Option<String>,
    pub tag: Option<String>,
    pub regime: Option<String>,
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Build `TradeRow` vector from a `RunScriptResponse`.
///
/// Maps `TradeRecord` → `TradeRow` preserving the same field names so the
/// REST API returns the identical JSON shape as the MCP tool response.
pub(crate) fn build_trades(response: &crate::tools::run_script::RunScriptResponse) -> Vec<TradeRow> {
    response
        .result
        .trade_log
        .iter()
        .map(|t| TradeRow {
            trade_id: t.trade_id as i64,
            entry_datetime: t.entry_datetime.and_utc().timestamp(),
            exit_datetime: t.exit_datetime.and_utc().timestamp(),
            entry_cost: sanitize(t.entry_cost),
            exit_proceeds: sanitize(t.exit_proceeds),
            entry_amount: sanitize(t.entry_amount),
            entry_label: format!("{:?}", t.entry_label),
            exit_amount: sanitize(t.exit_amount),
            exit_label: format!("{:?}", t.exit_label),
            pnl: sanitize(t.pnl),
            days_held: t.days_held,
            exit_type: format!("{:?}", t.exit_type),
            legs: serde_json::to_value(&t.legs).unwrap_or(Value::Array(vec![])),
            computed_quantity: t.computed_quantity,
            entry_equity: t.entry_equity.map(sanitize),
            stock_entry_price: t.stock_entry_price.map(sanitize),
            stock_exit_price: t.stock_exit_price.map(sanitize),
            stock_pnl: t.stock_pnl.map(sanitize),
            group: t.group.clone(),
        })
        .collect()
}

/// Strip trades from a `RunScriptResponse` for storage (trades stored separately).
pub(crate) fn strip_trades_from_result_json(response: &crate::tools::run_script::RunScriptResponse) -> String {
    let mut value = serde_json::to_value(response).unwrap_or(Value::Object(serde_json::Map::default()));
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
fn persist_backtest(
    state: &AppState,
    strategy_key: &str,
    symbol: &str,
    capital: f64,
    params: &HashMap<String, Value>,
    response: &crate::tools::run_script::RunScriptResponse,
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

    let created_at = state
        .run_store
        .insert_run(
            &id,
            None, // no sweep
            Some(strategy_key),
            symbol,
            capital,
            &params_value,
            Some(sanitize(if capital > 0.0 { response.result.total_pnl / capital * 100.0 } else { 0.0 })),
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
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    state
        .run_store
        .insert_trades(&id, &trades)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((id, created_at))
}

// ──────────────────────────────────────────────────────────────────────────────
// Handlers
// ──────────────────────────────────────────────────────────────────────────────

/// `POST /backtests` — Run a strategy and persist the result.
#[allow(clippy::too_many_lines)]
pub async fn create_backtest(
    State(state): State<AppState>,
    Json(req): Json<CreateBacktestRequest>,
) -> Result<(StatusCode, Json<RunDetail>), (StatusCode, String)> {
    let run_params = RunScriptParams {
        strategy: Some(req.strategy.clone()),
        script: None,
        params: req.params.clone(),
        profile: req.profile.clone(),
    };

    let exec_result = crate::server::handlers::run_script::execute(&state.server, run_params)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
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

    let (id, _created_at) = persist_backtest(
        &state,
        &strategy_key,
        &symbol,
        capital,
        &req.params,
        &response,
    )?;

    let detail = state
        .run_store
        .get_run(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Run inserted but not found".to_string(),
            )
        })?;

    Ok((StatusCode::CREATED, Json(detail)))
}

/// `GET /backtests` — List backtest summaries (delegates to run store).
#[allow(clippy::unused_async)]
pub async fn list_backtests(
    State(state): State<AppState>,
    Query(_query): Query<ListQuery>,
) -> Result<Json<crate::data::traits::RunsListResponse>, (StatusCode, String)> {
    let response = state
        .run_store
        .list()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(response))
}

/// `GET /backtests/{id}` — Retrieve a full backtest detail by id.
#[allow(clippy::unused_async)]
pub async fn get_backtest(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<RunDetail>, (StatusCode, String)> {
    let detail = state
        .run_store
        .get_run(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("Backtest '{id}' not found")))?;
    Ok(Json(detail))
}

/// `PATCH /backtests/{id}/analysis` — Save AI-generated analysis text.
#[allow(clippy::unused_async, clippy::implicit_hasher)]
pub async fn set_backtest_analysis(
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
        .run_store
        .set_run_analysis(&id, analysis)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if found {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, format!("Backtest '{id}' not found")))
    }
}

/// `GET /backtests/{id}/trades` — Retrieve trades for a backtest.
#[allow(clippy::unused_async)]
pub async fn get_backtest_trades(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<TradeRow>>, (StatusCode, String)> {
    let detail = state
        .run_store
        .get_run(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("Backtest '{id}' not found")))?;
    Ok(Json(detail.trades))
}

/// `DELETE /backtests/{id}` — Delete a backtest by id.
#[allow(clippy::unused_async)]
pub async fn delete_backtest(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let deleted = state
        .run_store
        .delete_run(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, format!("Backtest '{id}' not found")))
    }
}

/// `POST /backtests/stream` — Run a strategy with SSE progress updates.
#[allow(clippy::unused_async, clippy::too_many_lines)]
pub async fn create_backtest_stream(
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

        // Run the actual backtest
        let run_params = RunScriptParams {
            strategy: Some(req.strategy.clone()),
            script: None,
            params: req.params.clone(),
            profile: req.profile.clone(),
        };

        let result =
            super::run_script::execute_with_progress(&state.server, run_params, Some(progress_cb))
                .await;

        // Stop the progress ticker
        ticker.abort();

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
                    &state,
                    &strategy_key,
                    &symbol,
                    capital,
                    &req.params,
                    &response,
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
