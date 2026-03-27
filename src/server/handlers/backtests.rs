//! REST API handlers for backtest CRUD operations.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

use crate::data::backtest_store::{
    BacktestDetail, BacktestStore, BacktestSummary, MetricsRow, TradeRow,
};
use crate::server::OptopsyServer;
use crate::tools::run_script::RunScriptParams;

/// Replace NaN/Infinity with 0.0 to prevent JSON serialization errors.
fn sanitize(v: f64) -> f64 {
    if v.is_finite() {
        v
    } else {
        0.0
    }
}

/// Shared application state for backtest REST routes.
#[derive(Clone)]
pub struct AppState {
    pub server: OptopsyServer,
    pub backtest_store: BacktestStore,
}

/// Request body for `POST /backtests`.
#[derive(Debug, Deserialize)]
pub struct CreateBacktestRequest {
    pub strategy: String,
    pub params: HashMap<String, Value>,
}

/// Query parameters for `GET /backtests`.
#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub strategy: Option<String>,
    pub symbol: Option<String>,
}

// ──────────────────────────────────────────────────────────────────────────────
// Handlers
// ──────────────────────────────────────────────────────────────────────────────

/// `POST /backtests` — Run a strategy and persist the result.
pub async fn create_backtest(
    State(state): State<AppState>,
    Json(req): Json<CreateBacktestRequest>,
) -> Result<(StatusCode, Json<BacktestDetail>), (StatusCode, String)> {
    let run_params = RunScriptParams {
        strategy: Some(req.strategy.clone()),
        script: None,
        params: req.params.clone(),
    };

    let response = crate::server::handlers::run_script::execute(&state.server, run_params)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

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

    let (id, created_at) = state
        .backtest_store
        .insert(
            &req.strategy,
            &symbol,
            capital,
            &params_value,
            &metrics,
            &trades,
            &result_json,
            response.execution_time_ms as i64,
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((
        StatusCode::CREATED,
        Json(BacktestDetail {
            id,
            created_at,
            strategy_key: req.strategy,
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
        .list(query.strategy.as_deref(), query.symbol.as_deref())
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
