//! REST API handlers for backtest CRUD operations.

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

use crate::data::backtest_store::{BacktestDetail, BacktestSummary, MetricsRow, TradeRow};
use crate::server::state::AppState;
use crate::tools::run_script::RunScriptParams;

/// Replace NaN/Infinity with 0.0 to prevent JSON serialization errors.
fn sanitize(v: f64) -> f64 {
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
// Handlers
// ──────────────────────────────────────────────────────────────────────────────

/// `POST /backtests` — Run a strategy and persist the result.
#[allow(clippy::too_many_lines)]
pub async fn create_backtest(
    State(state): State<AppState>,
    Json(req): Json<CreateBacktestRequest>,
) -> Result<(StatusCode, Json<BacktestDetail>), (StatusCode, String)> {
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

    // Extract SYMBOL and CAPITAL from params
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

    let m = &response.result.metrics;
    let metrics = MetricsRow {
        sharpe: sanitize(m.sharpe),
        sortino: sanitize(m.sortino),
        cagr: sanitize(m.cagr),
        max_drawdown: sanitize(m.max_drawdown),
        win_rate: sanitize(m.win_rate),
        profit_factor: sanitize(m.profit_factor),
        total_pnl: sanitize(response.result.total_pnl),
        trade_count: response.result.trade_count as i64,
        expectancy: sanitize(m.expectancy),
        var_95: sanitize(m.var_95),
    };

    let trades: Vec<TradeRow> = response
        .result
        .trade_log
        .iter()
        .map(|t| {
            let pnl_pct = if t.entry_cost.abs() > 0.0 {
                t.pnl / t.entry_cost.abs()
            } else {
                0.0
            };
            TradeRow {
                trade_id: t.trade_id as i64,
                entry_datetime: t.entry_datetime.to_string(),
                exit_datetime: t.exit_datetime.to_string(),
                entry_cost: sanitize(t.entry_cost),
                exit_proceeds: sanitize(t.exit_proceeds),
                pnl: sanitize(t.pnl),
                pnl_pct: sanitize(pnl_pct),
                days_held: t.days_held,
                exit_type: format!("{:?}", t.exit_type),
                legs: serde_json::to_string(&t.legs).unwrap_or_else(|_| "[]".to_owned()),
                computed_quantity: t.computed_quantity,
                entry_equity: t.entry_equity.map(sanitize),
                group_label: t.group.clone(),
            }
        })
        .collect();

    let params_value = serde_json::to_value(&req.params)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Serialize the full response as the result_json blob
    let result_json = serde_json::to_string(&response)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Extract provenance from script_meta
    let hypothesis = response
        .script_meta
        .as_ref()
        .and_then(|m| m.hypothesis.as_deref());
    let tags = response
        .script_meta
        .as_ref()
        .and_then(|m| m.tags.as_deref());
    let regime = response
        .script_meta
        .as_ref()
        .and_then(|m| m.regime.as_deref());

    let (id, created_at) = state
        .backtest_store
        .insert(
            &strategy_key,
            &symbol,
            capital,
            &params_value,
            &metrics,
            &trades,
            &result_json,
            response.execution_time_ms as i64,
            hypothesis,
            tags,
            regime,
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((
        StatusCode::CREATED,
        Json(BacktestDetail {
            id,
            created_at,
            strategy_key,
            params: Some(serde_json::to_value(&req.params).unwrap_or_default()),
            analysis: None,
            response,
        }),
    ))
}

/// `GET /backtests` — List backtest summaries, optionally filtered.
#[allow(clippy::unused_async)]
pub async fn list_backtests(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Vec<BacktestSummary>>, (StatusCode, String)> {
    let rows = state
        .backtest_store
        .list(
            query.strategy.as_deref(),
            query.symbol.as_deref(),
            query.tag.as_deref(),
            query.regime.as_deref(),
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(rows))
}

/// `GET /backtests/{id}` — Retrieve a full backtest detail by id.
#[allow(clippy::unused_async)]
pub async fn get_backtest(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<BacktestDetail>, (StatusCode, String)> {
    let detail = state
        .backtest_store
        .get_detail(&id)
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
        .backtest_store
        .set_analysis(&id, analysis)
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
    let trades = state
        .backtest_store
        .get_trades(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(trades))
}

/// `DELETE /backtests/{id}` — Delete a backtest by id.
#[allow(clippy::unused_async)]
pub async fn delete_backtest(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let deleted = state
        .backtest_store
        .delete(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, format!("Backtest '{id}' not found")))
    }
}

/// `POST /backtests/stream` — Run a strategy with SSE progress updates.
///
/// Streams `event: progress` with `{ "current": N, "total": M }` during the
/// simulation loop, then emits `event: result` with the full `BacktestDetail`
/// JSON, and finally `event: done`.
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
                // Persist to DB (same as create_backtest)
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

                let m = &response.result.metrics;
                let metrics = MetricsRow {
                    sharpe: sanitize(m.sharpe),
                    sortino: sanitize(m.sortino),
                    cagr: sanitize(m.cagr),
                    max_drawdown: sanitize(m.max_drawdown),
                    win_rate: sanitize(m.win_rate),
                    profit_factor: sanitize(m.profit_factor),
                    total_pnl: sanitize(response.result.total_pnl),
                    trade_count: response.result.trade_count as i64,
                    expectancy: sanitize(m.expectancy),
                    var_95: sanitize(m.var_95),
                };

                let trades: Vec<TradeRow> = response
                    .result
                    .trade_log
                    .iter()
                    .map(|t| {
                        let pnl_pct = if t.entry_cost.abs() > 0.0 {
                            t.pnl / t.entry_cost.abs()
                        } else {
                            0.0
                        };
                        TradeRow {
                            trade_id: t.trade_id as i64,
                            entry_datetime: t.entry_datetime.to_string(),
                            exit_datetime: t.exit_datetime.to_string(),
                            entry_cost: sanitize(t.entry_cost),
                            exit_proceeds: sanitize(t.exit_proceeds),
                            pnl: sanitize(t.pnl),
                            pnl_pct: sanitize(pnl_pct),
                            days_held: t.days_held,
                            exit_type: format!("{:?}", t.exit_type),
                            legs: serde_json::to_string(&t.legs)
                                .unwrap_or_else(|_| "[]".to_owned()),
                            computed_quantity: t.computed_quantity,
                            entry_equity: t.entry_equity.map(sanitize),
                            group_label: t.group.clone(),
                        }
                    })
                    .collect();

                let params_value = serde_json::to_value(&req.params).unwrap_or_default();
                let result_json =
                    serde_json::to_string(&response).unwrap_or_else(|_| "{}".to_owned());

                let hypothesis = response
                    .script_meta
                    .as_ref()
                    .and_then(|m| m.hypothesis.as_deref());
                let tags = response
                    .script_meta
                    .as_ref()
                    .and_then(|m| m.tags.as_deref());
                let regime = response
                    .script_meta
                    .as_ref()
                    .and_then(|m| m.regime.as_deref());

                match state.backtest_store.insert(
                    &strategy_key,
                    &symbol,
                    capital,
                    &params_value,
                    &metrics,
                    &trades,
                    &result_json,
                    response.execution_time_ms as i64,
                    hypothesis,
                    tags,
                    regime,
                ) {
                    Ok((id, created_at)) => {
                        let detail = BacktestDetail {
                            id,
                            created_at,
                            strategy_key,
                            params: Some(serde_json::to_value(&req.params).unwrap_or_default()),
                            analysis: None,
                            response,
                        };
                        let json = serde_json::to_string(&detail).unwrap_or_default();
                        let _ = tx.send(Event::default().event("result").data(json)).await;
                    }
                    Err(e) => {
                        let _ = tx
                            .send(
                                Event::default()
                                    .event("error")
                                    .data(format!("DB insert failed: {e}")),
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
