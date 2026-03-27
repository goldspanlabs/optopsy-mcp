//! REST API handlers for backtest CRUD operations.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

use crate::data::backtest_store::{BacktestRow, BacktestStore, BacktestSummary, MetricsRow, TradeRow};
use crate::server::OptopsyServer;
use crate::tools::run_script::RunScriptParams;

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
) -> Result<(StatusCode, Json<BacktestRow>), (StatusCode, String)> {
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
        sharpe: m.sharpe,
        sortino: m.sortino,
        cagr: m.cagr,
        max_drawdown: m.max_drawdown,
        win_rate: m.win_rate,
        profit_factor: m.profit_factor,
        total_pnl: response.result.total_pnl,
        trade_count: response.result.trade_count as i64,
        expectancy: m.expectancy,
        var_95: m.var_95,
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
                entry_cost: t.entry_cost,
                exit_proceeds: t.exit_proceeds,
                pnl: t.pnl,
                pnl_pct,
                days_held: t.days_held,
                exit_type: format!("{:?}", t.exit_type),
                legs: serde_json::to_string(&t.legs).unwrap_or_else(|_| "[]".to_owned()),
                computed_quantity: t.computed_quantity,
                entry_equity: t.entry_equity,
                group_label: t.group.clone(),
            }
        })
        .collect();

    let equity_curve_json = serde_json::to_value(&response.result.equity_curve)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let indicator_data_json = serde_json::to_value(&response.indicator_data)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let params_value = serde_json::to_value(&req.params)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let id = state
        .backtest_store
        .insert(
            &req.strategy,
            &symbol,
            capital,
            &params_value,
            &metrics,
            &trades,
            &equity_curve_json,
            &indicator_data_json,
            response.execution_time_ms as i64,
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let row = state
        .backtest_store
        .get(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Backtest not found after insert".to_owned(),
            )
        })?;

    Ok((StatusCode::CREATED, Json(row)))
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

/// `GET /backtests/{id}` — Retrieve a full backtest result by id.
#[allow(clippy::unused_async)]
pub async fn get_backtest(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<BacktestRow>, (StatusCode, String)> {
    let row = state
        .backtest_store
        .get(&id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("Backtest '{id}' not found")))?;
    Ok(Json(row))
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
