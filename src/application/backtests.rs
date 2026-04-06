//! Shared backtest workflow orchestration used by transport adapters.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;

use crate::application::error::{ApplicationError, ApplicationResult};
use crate::data::traits::{RunStore, TradeRow};
use crate::scripting::engine::{
    CachingDataLoader, CancelCallback, ProgressCallback, ScriptBacktestResult,
};
use crate::scripting::stdlib::parse_script_meta;
use crate::server::sanitize::{sanitize, trade_row_from_record};
use crate::server::OptopsyServer;
use crate::tools::run_script::{format_indicator_data, RunScriptParams, RunScriptResponse};

const DEFAULT_SCRIPT_CAPITAL: f64 = 100_000.0;

/// Result of executing a script: the response plus the resolved strategy ID (if any).
pub struct ExecuteResult {
    pub response: RunScriptResponse,
    /// The resolved strategy UUID, or `None` for inline scripts.
    pub resolved_strategy_id: Option<String>,
}

/// Execute a Rhai backtest script.
pub async fn execute_script(
    server: &OptopsyServer,
    params: RunScriptParams,
) -> Result<ExecuteResult> {
    execute_script_with_progress(server, params, None, None).await
}

/// Execute a Rhai backtest script with an optional progress callback.
pub async fn execute_script_with_progress(
    server: &OptopsyServer,
    params: RunScriptParams,
    progress: Option<ProgressCallback>,
    is_cancelled: Option<&CancelCallback>,
) -> Result<ExecuteResult> {
    let start = std::time::Instant::now();

    let (resolved_id, source) =
        crate::tools::run_script::resolve_script_source(&params, server.strategy_store.as_deref())?;

    let script_meta = resolved_id
        .as_deref()
        .or(params.strategy.as_deref())
        .map(|id| parse_script_meta(id, &source));

    let loader =
        CachingDataLoader::new(Arc::clone(&server.cache), server.adjustment_store_handle());

    let effective_params = if let Some(ref profile_name) = params.profile {
        use crate::scripting::stdlib::{load_profiles_registry, merge_profile_params};
        let registry = load_profiles_registry();
        let script_profiles = script_meta.as_ref().and_then(|m| m.profiles.as_ref());
        merge_profile_params(profile_name, &registry, script_profiles, &params.params)
    } else {
        params.params.clone()
    };

    let ScriptBacktestResult {
        result,
        metadata: _,
        indicator_data,
        custom_series,
        ..
    } = crate::scripting::engine::run_script_backtest(
        &source,
        &effective_params,
        &loader,
        progress,
        None,
        is_cancelled,
    )
    .await?;

    Ok(ExecuteResult {
        response: RunScriptResponse {
            script_meta,
            result,
            indicator_data: format_indicator_data(&indicator_data, &custom_series),
            execution_time_ms: start.elapsed().as_millis() as u64,
        },
        resolved_strategy_id: resolved_id,
    })
}

/// Resolve the symbol that should be persisted for a completed backtest.
pub fn resolve_symbol<S: BuildHasher>(
    response: &RunScriptResponse,
    params: &HashMap<String, Value, S>,
) -> ApplicationResult<String> {
    response
        .result
        .symbol
        .as_deref()
        .filter(|s| !s.is_empty())
        .or_else(|| params.get("symbol").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            ApplicationError::invalid_input("No symbol resolved — declare an `asset` in the script")
        })
}

/// Resolve the capital value used by the backtest.
#[must_use]
pub fn resolve_capital<S: BuildHasher>(params: &HashMap<String, Value, S>) -> f64 {
    params
        .get("CAPITAL")
        .and_then(Value::as_f64)
        .unwrap_or(DEFAULT_SCRIPT_CAPITAL)
}

fn build_trades(response: &RunScriptResponse) -> Vec<TradeRow> {
    response
        .result
        .trade_log
        .iter()
        .map(trade_row_from_record)
        .collect()
}

fn strip_trades_from_result_json(response: &RunScriptResponse) -> String {
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
pub fn persist_backtest<S: BuildHasher>(
    run_store: &dyn RunStore,
    strategy_key: &str,
    params: &HashMap<String, Value, S>,
    response: &RunScriptResponse,
    source: &str,
    thread_id: Option<&str>,
) -> ApplicationResult<(String, String)> {
    let id = uuid::Uuid::new_v4().to_string();
    let symbol = resolve_symbol(response, params)?;
    let capital = resolve_capital(params);
    let m = &response.result.metrics;
    let trades = build_trades(response);
    let result_json = strip_trades_from_result_json(response);
    let params_value =
        serde_json::to_value(params).map_err(|e| ApplicationError::internal(e.to_string()))?;

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
            None,
            Some(strategy_key),
            &symbol,
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
            None,
            None,
            &result_json,
            Some(response.execution_time_ms as i64),
            hypothesis,
            tags_str.as_deref(),
            regime_str.as_deref(),
            source,
            thread_id,
        )
        .map_err(|e| ApplicationError::storage(e.to_string()))?;

    run_store
        .insert_trades(&id, &trades)
        .map_err(|e| ApplicationError::storage(e.to_string()))?;

    Ok((id, created_at))
}
