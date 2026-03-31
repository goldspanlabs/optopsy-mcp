//! Grid parameter sweep — Cartesian product of param ranges, compile-once execution.

use std::collections::HashMap;
use std::time::Instant;

use anyhow::Result;
use serde_json::Value;

use crate::engine::walk_forward::cartesian_product;
use crate::scripting::engine::{run_script_backtest, DataLoader};
use crate::tools::response_types::sweep::{DimensionStat, SweepResponse, SweepResult};

/// Input configuration for a grid sweep.
pub struct GridSweepConfig {
    pub script_source: String,
    pub base_params: HashMap<String, Value>,
    pub param_grid: HashMap<String, Vec<Value>>,
    pub objective: String,
}

/// Run a grid sweep over the Cartesian product of `param_grid`.
pub async fn run_grid_sweep(
    config: &GridSweepConfig,
    data_loader: &dyn DataLoader,
    is_cancelled: impl Fn() -> bool,
    on_progress: impl Fn(usize, usize),
) -> Result<SweepResponse> {
    let start = Instant::now();
    let combos = cartesian_product(&config.param_grid);
    let total = combos.len();
    let mut results: Vec<SweepResult> = Vec::new();
    let mut full_results: Vec<crate::scripting::engine::ScriptBacktestResult> = Vec::new();
    let mut failed = 0usize;

    for (idx, combo) in combos.iter().enumerate() {
        if is_cancelled() {
            break;
        }
        on_progress(idx, total);
        let mut run_params = config.base_params.clone();
        run_params.extend(combo.clone());

        match run_script_backtest(&config.script_source, &run_params, data_loader).await {
            Ok(bt) => {
                let m = &bt.result.metrics;
                results.push(SweepResult {
                    rank: 0,
                    params: combo.clone(),
                    sharpe: m.sharpe,
                    sortino: m.sortino,
                    pnl: bt.result.total_pnl,
                    trades: bt.result.trade_count,
                    win_rate: m.win_rate,
                    max_drawdown: m.max_drawdown,
                    profit_factor: m.profit_factor,
                    cagr: m.cagr,
                    calmar: m.calmar,
                });
                full_results.push(bt);
            }
            Err(_) => {
                failed += 1;
            }
        }
    }

    // Sort results and full_results together by objective
    let mut paired: Vec<_> = results.into_iter().zip(full_results).collect();
    paired.sort_by(|(a, _), (b, _)| sort_cmp_objective(a, b, &config.objective));
    let (mut results, full_results): (Vec<_>, Vec<_>) = paired.into_iter().unzip();
    for (i, r) in results.iter_mut().enumerate() {
        r.rank = i + 1;
    }

    let sensitivity = compute_sensitivity(&results, &config.param_grid, &config.objective);

    Ok(SweepResponse {
        mode: "grid".to_string(),
        objective: config.objective.clone(),
        combinations_total: total,
        combinations_run: results.len(),
        combinations_failed: failed,
        best_result: results.first().cloned(),
        ranked_results: results,
        dimension_sensitivity: sensitivity,
        convergence_trace: None,
        execution_time_ms: start.elapsed().as_millis() as u64,
        full_results,
    })
}

fn sort_cmp_objective(a: &SweepResult, b: &SweepResult, objective: &str) -> std::cmp::Ordering {
    let (va, vb) = match objective {
        "sortino" => (a.sortino, b.sortino),
        "calmar" => (a.calmar, b.calmar),
        "profit_factor" => (a.profit_factor, b.profit_factor),
        _ => (a.sharpe, b.sharpe),
    };
    vb.partial_cmp(&va).unwrap_or(std::cmp::Ordering::Equal)
}

pub fn sort_by_objective(results: &mut [SweepResult], objective: &str) {
    results.sort_by(|a, b| sort_cmp_objective(a, b, objective));
}

pub fn extract_objective(result: &SweepResult, objective: &str) -> f64 {
    match objective {
        "sortino" => result.sortino,
        "calmar" => result.calmar,
        "profit_factor" => result.profit_factor,
        _ => result.sharpe,
    }
}

#[allow(clippy::implicit_hasher)]
pub fn compute_sensitivity(
    results: &[SweepResult],
    param_grid: &HashMap<String, Vec<Value>>,
    objective: &str,
) -> HashMap<String, Vec<DimensionStat>> {
    let mut sensitivity = HashMap::new();

    for (param_name, values) in param_grid {
        let mut stats = Vec::new();
        for val in values {
            let val_str = match val {
                Value::Number(n) => n.to_string(),
                Value::String(s) => s.clone(),
                Value::Bool(b) => b.to_string(),
                other => other.to_string(),
            };

            let matching: Vec<f64> = results
                .iter()
                .filter(|r| r.params.get(param_name) == Some(val))
                .map(|r| extract_objective(r, objective))
                .filter(|v| v.is_finite())
                .collect();

            if !matching.is_empty() {
                let avg = matching.iter().sum::<f64>() / matching.len() as f64;
                stats.push(DimensionStat {
                    value: val_str,
                    avg_metric: avg,
                    count: matching.len(),
                });
            }
        }
        sensitivity.insert(param_name.clone(), stats);
    }

    sensitivity
}
