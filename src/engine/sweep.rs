//! Grid parameter sweep — Cartesian product of param ranges, compile-once execution.
//!
//! After the first combo runs (to build precomputed options data and warm the
//! data cache), remaining combos are executed concurrently via `tokio::JoinSet`
//! with a configurable concurrency limit.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use serde_json::Value;

use crate::engine::walk_forward::cartesian_product;
use crate::scripting::engine::{
    run_script_backtest, CancelCallback, DataLoader, PrecomputedOptionsData, ScriptBacktestResult,
};
use crate::tools::response_types::sweep::{DimensionStat, SweepResponse, SweepResult};

/// Input configuration for a grid sweep.
pub struct GridSweepConfig {
    pub script_source: String,
    pub base_params: HashMap<String, Value>,
    pub param_grid: HashMap<String, Vec<Value>>,
    pub objective: String,
}

/// Max concurrent backtest tasks.  Kept moderate to avoid excessive memory use
/// (each task holds its own Rhai engine + intermediate data frames).
const MAX_CONCURRENT: usize = 8;

/// Run a grid sweep over the Cartesian product of `param_grid`.
///
/// Accepts `Arc<dyn DataLoader>` so that backtest tasks can be spawned onto the
/// tokio runtime for true concurrency (the `CachingDataLoader` is `Send + Sync`).
#[allow(clippy::too_many_lines)]
pub async fn run_grid_sweep(
    config: &GridSweepConfig,
    data_loader: Arc<dyn DataLoader>,
    is_cancelled: &CancelCallback,
    on_progress: impl Fn(usize, usize),
) -> Result<SweepResponse> {
    let start = Instant::now();
    let combos = cartesian_product(&config.param_grid);
    let total = combos.len();

    if total == 0 {
        return Ok(SweepResponse {
            mode: "grid".to_string(),
            objective: config.objective.clone(),
            combinations_total: 0,
            combinations_run: 0,
            combinations_failed: 0,
            best_result: None,
            ranked_results: Vec::new(),
            dimension_sensitivity: HashMap::new(),
            convergence_trace: None,
            execution_time_ms: start.elapsed().as_millis() as u64,
            full_results: Vec::new(),
        });
    }

    let mut combos_iter = combos.into_iter();

    // ── Phase 1: Run the first combo sequentially ─────────────────────────
    // This populates the precomputed options data and warms the data cache so
    // that subsequent parallel tasks hit memory instead of disk.
    let mut results: Vec<SweepResult> = Vec::with_capacity(total);
    let mut full_results: Vec<ScriptBacktestResult> = Vec::with_capacity(total);
    let mut failed = 0usize;
    let mut precomputed: Option<PrecomputedOptionsData> = None;

    let first_combo = combos_iter.next().unwrap(); // total > 0
    on_progress(0, total); // signal total to callers

    let mut run_params = config.base_params.clone();
    run_params.extend(first_combo.iter().map(|(k, v)| (k.clone(), v.clone())));

    match run_script_backtest(
        &config.script_source,
        &run_params,
        data_loader.as_ref(),
        None,
        None,
        Some(is_cancelled),
    )
    .await
    {
        Ok(bt) => {
            precomputed.clone_from(&bt.precomputed_options);
            let m = &bt.result.metrics;
            results.push(SweepResult {
                rank: 0,
                params: first_combo,
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

    // ── Phase 2: Run remaining combos concurrently ────────────────────────
    let remaining: Vec<_> = combos_iter.enumerate().collect();
    if !remaining.is_empty() && !is_cancelled() {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT));
        // Shared cancellation flag: AtomicBool so spawned tasks observe later
        // cancellations (not a one-shot snapshot of the initial state).
        let cancel_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Wrap shared read-only state in Arcs for spawned tasks
        let script_source = Arc::new(config.script_source.clone());
        let base_params = Arc::new(config.base_params.clone());
        let precomputed_arc = precomputed.clone(); // PrecomputedOptionsData is cheap (all Arcs)

        let mut join_set = tokio::task::JoinSet::<(
            usize,
            HashMap<String, Value>,
            Result<ScriptBacktestResult>,
        )>::new();

        for (offset, combo) in remaining {
            let sem = Arc::clone(&semaphore);
            let dl = Arc::clone(&data_loader);
            let src = Arc::clone(&script_source);
            let bp = Arc::clone(&base_params);
            let pre = precomputed_arc.clone();
            let cf = Arc::clone(&cancel_flag);

            join_set.spawn(async move {
                let _permit = sem.acquire().await.expect("semaphore closed");

                if cf.load(Ordering::Relaxed) {
                    return (offset, combo, Err(anyhow::anyhow!("cancelled")));
                }

                let mut params = HashMap::clone(&bp);
                params.extend(combo.iter().map(|(k, v)| (k.clone(), v.clone())));

                // Per-task cancel callback reads the shared AtomicBool
                let cf2 = Arc::clone(&cf);
                let cancel_cb: CancelCallback = Box::new(move || cf2.load(Ordering::Relaxed));

                let result = run_script_backtest(
                    &src,
                    &params,
                    dl.as_ref(),
                    None,
                    pre.as_ref(),
                    Some(&cancel_cb),
                )
                .await;

                (offset, combo, result)
            });
        }

        // Collect results as tasks complete
        let mut completed = 1usize; // first combo already done
        while let Some(join_result) = join_set.join_next().await {
            completed += 1;
            on_progress(completed, total);

            // Propagate cancellation from the caller into spawned tasks
            if is_cancelled() {
                cancel_flag.store(true, Ordering::Relaxed);
            }

            match join_result {
                Ok((_, combo, Ok(bt))) => {
                    let m = &bt.result.metrics;
                    results.push(SweepResult {
                        rank: 0,
                        params: combo,
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
                Ok((_, _, Err(_))) => {
                    failed += 1;
                }
                Err(join_err) => {
                    tracing::warn!("Sweep task panicked: {join_err}");
                    failed += 1;
                }
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
