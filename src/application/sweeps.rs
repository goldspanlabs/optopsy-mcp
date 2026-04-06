//! Shared sweep workflow orchestration used by transport adapters.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::data::traits::{RunStore, StrategyStore, TradeRow};
use crate::engine::bayesian::{run_bayesian, BayesianConfig};
use crate::engine::permutation::apply_permutation_gate;
use crate::engine::sweep::{run_grid_sweep, GridSweepConfig};
use crate::scripting::engine::{CachingDataLoader, CancelCallback, DataLoader, ProgressCallback};
use crate::server::sanitize::{sanitize, trade_row_from_record};
use crate::server::OptopsyServer;
use crate::tools::response_types::sweep::SweepResponse;

const DEFAULT_SCRIPT_SYMBOL: &str = "SPY";
const DEFAULT_SCRIPT_CAPITAL: f64 = 100_000.0;

fn default_objective() -> String {
    "sharpe".to_string()
}

fn default_max_evaluations() -> usize {
    50
}

fn default_param_type() -> String {
    "float".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct CreateSweepRequest {
    pub strategy: String,
    pub mode: String,
    #[serde(default = "default_objective")]
    pub objective: String,
    pub params: HashMap<String, Value>,
    pub sweep_params: Vec<SweepParamDef>,
    #[serde(default = "default_max_evaluations")]
    pub max_evaluations: usize,
    #[serde(default)]
    pub num_permutations: usize,
}

#[derive(Debug, Deserialize, Clone, Serialize, schemars::JsonSchema)]
pub struct SweepParamDef {
    pub name: String,
    #[serde(default = "default_param_type")]
    pub param_type: String,
    pub start: f64,
    pub stop: f64,
    pub step: Option<f64>,
}

pub struct ExecuteSweepResult {
    pub sweep_id: String,
    pub run_ids: Vec<String>,
    pub response: SweepResponse,
    pub strategy_key: String,
    pub symbol: String,
    pub capital: f64,
    pub objective: String,
}

struct SweepExecutionContext {
    strategy_key: String,
    script_source: String,
    script_meta: crate::scripting::stdlib::ScriptMeta,
    loader: Arc<dyn DataLoader>,
    symbol: String,
    capital: f64,
}

/// Build a Cartesian grid from sweep param definitions.
pub fn build_grid(sweep_params: &[SweepParamDef]) -> Result<HashMap<String, Vec<Value>>, String> {
    let mut grid: HashMap<String, Vec<Value>> = HashMap::new();
    for sp in sweep_params {
        let is_int = sp.param_type == "int";
        let step = sp.step.unwrap_or(if is_int { 1.0 } else { 0.01 });
        if sp.stop < sp.start {
            return Err(format!(
                "Invalid sweep param '{}' range: stop ({}) must be >= start ({})",
                sp.name, sp.stop, sp.start
            ));
        }
        if step <= 0.0 {
            return Err(format!(
                "Invalid sweep param '{}' step: step must be > 0",
                sp.name
            ));
        }
        let mut v = sp.start;
        let mut values = Vec::new();
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
    Ok(grid)
}

/// Resolve strategy source from a strategy store.
pub fn resolve_strategy_source_from_store(
    store: &dyn StrategyStore,
    name_or_id: &str,
) -> Result<(String, String), (StatusCode, String)> {
    let (id, raw) = match store.get_source(name_or_id) {
        Ok(Some(source)) => (name_or_id.to_string(), source),
        Ok(None) => match store.get_source_by_name(name_or_id) {
            Ok(Some((id, source))) => (id, source),
            Ok(None) => {
                return Err((
                    StatusCode::NOT_FOUND,
                    format!("Strategy '{name_or_id}' not found"),
                ));
            }
            Err(e) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to resolve strategy '{name_or_id}' by name: {e}"),
                ));
            }
        },
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to resolve strategy '{name_or_id}' by id: {e}"),
            ));
        }
    };

    let source = crate::tools::run_script::maybe_transpile(raw)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok((id, source))
}

/// Insert sweep results into the run store.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub fn persist_sweep_to_store(
    run_store: &dyn RunStore,
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

    run_store
        .insert_sweep(
            &sweep_id,
            Some(strategy_key),
            symbol,
            &sweep_config,
            &req.objective,
            &req.mode,
            sweep_response.combinations_total as i64,
            Some(sweep_response.execution_time_ms as i64),
            source,
            thread_id,
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let capital = req
        .params
        .get("CAPITAL")
        .and_then(Value::as_f64)
        .unwrap_or(DEFAULT_SCRIPT_CAPITAL);

    for (i, result) in sweep_response.ranked_results.iter().enumerate() {
        let run_id = uuid::Uuid::new_v4().to_string();
        let params_value = serde_json::to_value(&result.params)
            .unwrap_or(Value::Object(serde_json::Map::default()));
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

fn resolve_symbol(req: &CreateSweepRequest, script_source: &str) -> String {
    req.params
        .get("symbol")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            crate::scripting::engine::resolve_symbols_from_extern_params(script_source, &req.params)
                .into_iter()
                .next()
        })
        .unwrap_or_else(|| DEFAULT_SCRIPT_SYMBOL.to_string())
}

fn resolve_capital(req: &CreateSweepRequest) -> f64 {
    req.params
        .get("CAPITAL")
        .and_then(Value::as_f64)
        .unwrap_or(DEFAULT_SCRIPT_CAPITAL)
}

fn build_loader(server: &OptopsyServer) -> Arc<dyn DataLoader> {
    Arc::new(CachingDataLoader::new(
        Arc::clone(&server.cache),
        server.adjustment_store_handle(),
    ))
}

fn resolve_execution_context(
    server: &OptopsyServer,
    req: &CreateSweepRequest,
) -> Result<SweepExecutionContext> {
    let strategy_store = server.require_strategy_store()?;

    let (strategy_key, script_source) =
        resolve_strategy_source_from_store(strategy_store.as_ref(), &req.strategy)
            .map_err(|(_status, msg)| anyhow::anyhow!("{msg}"))?;
    let script_meta = crate::scripting::stdlib::parse_script_meta(&strategy_key, &script_source);

    Ok(SweepExecutionContext {
        strategy_key,
        script_meta,
        loader: build_loader(server),
        symbol: resolve_symbol(req, &script_source),
        capital: resolve_capital(req),
        script_source,
    })
}

async fn run_sweep_mode(
    req: &CreateSweepRequest,
    context: &SweepExecutionContext,
    progress: Option<ProgressCallback>,
    is_cancelled: Option<&CancelCallback>,
) -> Result<SweepResponse> {
    let noop_progress: ProgressCallback = Box::new(|_, _| {});
    let progress_ref = progress.as_ref().unwrap_or(&noop_progress);
    let noop_cancel: CancelCallback = Box::new(|| false);
    let cancel_ref = is_cancelled.unwrap_or(&noop_cancel);

    match req.mode.as_str() {
        "grid" => {
            let config = GridSweepConfig {
                script_source: context.script_source.clone(),
                base_params: req.params.clone(),
                param_grid: build_grid(&req.sweep_params).map_err(anyhow::Error::msg)?,
                objective: req.objective.clone(),
            };
            run_grid_sweep(
                &config,
                Arc::clone(&context.loader),
                cancel_ref,
                progress_ref,
            )
            .await
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
            let config = BayesianConfig {
                script_source: context.script_source.clone(),
                base_params: req.params.clone(),
                continuous_params,
                max_evaluations: req.max_evaluations,
                initial_samples: (req.max_evaluations / 3).max(2),
                objective: req.objective.clone(),
            };
            run_bayesian(&config, context.loader.as_ref(), cancel_ref, progress_ref).await
        }
        other => {
            anyhow::bail!("Invalid mode '{other}', expected 'grid' or 'bayesian'");
        }
    }
}

async fn apply_permutation_if_needed(
    req: &CreateSweepRequest,
    sweep_response: SweepResponse,
) -> Result<SweepResponse> {
    let objective = req.objective.clone();
    let num_permutations = req.num_permutations;
    if num_permutations > 0 {
        Ok(tokio::task::spawn_blocking(move || {
            apply_permutation_gate(sweep_response, num_permutations, &objective, Some(42))
        })
        .await?)
    } else {
        Ok(sweep_response)
    }
}

fn load_run_ids(run_store: &dyn RunStore, sweep_id: &str) -> Result<Vec<String>> {
    Ok(run_store
        .get_sweep(sweep_id)?
        .map(|detail| detail.runs.iter().map(|r| r.id.clone()).collect())
        .unwrap_or_default())
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_sweep(
    server: &OptopsyServer,
    run_store: &dyn RunStore,
    req: &CreateSweepRequest,
    source: &str,
    thread_id: Option<&str>,
    progress: Option<ProgressCallback>,
    is_cancelled: Option<&CancelCallback>,
) -> Result<ExecuteSweepResult> {
    let context = resolve_execution_context(server, req)?;
    let sweep_response = run_sweep_mode(req, &context, progress, is_cancelled).await?;
    let sweep_response = apply_permutation_if_needed(req, sweep_response).await?;
    let sweep_id = persist_sweep_to_store(
        run_store,
        &context.strategy_key,
        &context.symbol,
        req,
        &sweep_response,
        &context.script_meta,
        source,
        thread_id,
    )
    .map_err(|(_status, msg)| anyhow::anyhow!("{msg}"))?;

    let run_ids = load_run_ids(run_store, &sweep_id)?;

    Ok(ExecuteSweepResult {
        sweep_id,
        run_ids,
        response: sweep_response,
        strategy_key: context.strategy_key,
        symbol: context.symbol,
        capital: context.capital,
        objective: req.objective.clone(),
    })
}
