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
            multiple_comparisons: None,
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
                p_value: None,
                significant: None,
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
                        p_value: None,
                        significant: None,
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
        multiple_comparisons: None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_result(sharpe: f64, sortino: f64, calmar: f64, profit_factor: f64) -> SweepResult {
        SweepResult {
            rank: 0,
            params: HashMap::new(),
            sharpe,
            sortino,
            pnl: 0.0,
            trades: 0,
            win_rate: 0.0,
            max_drawdown: 0.0,
            profit_factor,
            cagr: 0.0,
            calmar,
            p_value: None,
            significant: None,
        }
    }

    fn make_result_with_params(
        params: HashMap<String, Value>,
        sharpe: f64,
        sortino: f64,
    ) -> SweepResult {
        SweepResult {
            rank: 0,
            params,
            sharpe,
            sortino,
            pnl: 0.0,
            trades: 0,
            win_rate: 0.0,
            max_drawdown: 0.0,
            profit_factor: 0.0,
            cagr: 0.0,
            calmar: 0.0,
            p_value: None,
            significant: None,
        }
    }

    // ── extract_objective ────────────────────────────────────────────

    #[test]
    fn extract_objective_sharpe_default() {
        let r = make_result(1.5, 2.0, 0.8, 3.0);
        assert!((extract_objective(&r, "sharpe") - 1.5).abs() < f64::EPSILON);
        // Unknown objective also defaults to sharpe
        assert!((extract_objective(&r, "unknown") - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn extract_objective_sortino() {
        let r = make_result(1.5, 2.0, 0.8, 3.0);
        assert!((extract_objective(&r, "sortino") - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn extract_objective_calmar() {
        let r = make_result(1.5, 2.0, 0.8, 3.0);
        assert!((extract_objective(&r, "calmar") - 0.8).abs() < f64::EPSILON);
    }

    #[test]
    fn extract_objective_profit_factor() {
        let r = make_result(1.5, 2.0, 0.8, 3.0);
        assert!((extract_objective(&r, "profit_factor") - 3.0).abs() < f64::EPSILON);
    }

    // ── sort_cmp_objective ───────────────────────────────────────────

    #[test]
    fn sort_cmp_descending_by_sharpe() {
        let a = make_result(2.0, 0.0, 0.0, 0.0);
        let b = make_result(1.0, 0.0, 0.0, 0.0);
        // Higher sharpe should come first (Less means a before b)
        assert_eq!(
            sort_cmp_objective(&a, &b, "sharpe"),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            sort_cmp_objective(&b, &a, "sharpe"),
            std::cmp::Ordering::Greater
        );
    }

    #[test]
    fn sort_cmp_equal() {
        let a = make_result(1.5, 0.0, 0.0, 0.0);
        let b = make_result(1.5, 0.0, 0.0, 0.0);
        assert_eq!(
            sort_cmp_objective(&a, &b, "sharpe"),
            std::cmp::Ordering::Equal
        );
    }

    #[test]
    fn sort_cmp_nan_handling() {
        let a = make_result(f64::NAN, 0.0, 0.0, 0.0);
        let b = make_result(1.0, 0.0, 0.0, 0.0);
        // NaN comparison falls back to Equal
        assert_eq!(
            sort_cmp_objective(&a, &b, "sharpe"),
            std::cmp::Ordering::Equal
        );
    }

    #[test]
    fn sort_by_objective_ranks_descending() {
        let mut results = vec![
            make_result(1.0, 0.0, 0.0, 0.0),
            make_result(3.0, 0.0, 0.0, 0.0),
            make_result(2.0, 0.0, 0.0, 0.0),
        ];
        sort_by_objective(&mut results, "sharpe");
        assert!((results[0].sharpe - 3.0).abs() < f64::EPSILON);
        assert!((results[1].sharpe - 2.0).abs() < f64::EPSILON);
        assert!((results[2].sharpe - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn sort_by_sortino_objective() {
        let mut results = vec![
            make_result(0.0, 1.0, 0.0, 0.0),
            make_result(0.0, 3.0, 0.0, 0.0),
            make_result(0.0, 2.0, 0.0, 0.0),
        ];
        sort_by_objective(&mut results, "sortino");
        assert!((results[0].sortino - 3.0).abs() < f64::EPSILON);
        assert!((results[1].sortino - 2.0).abs() < f64::EPSILON);
        assert!((results[2].sortino - 1.0).abs() < f64::EPSILON);
    }

    // ── compute_sensitivity ──────────────────────────────────────────

    #[test]
    fn sensitivity_single_param() {
        let mut p1 = HashMap::new();
        p1.insert("dte".to_string(), json!(7));
        let mut p2 = HashMap::new();
        p2.insert("dte".to_string(), json!(14));
        let mut p3 = HashMap::new();
        p3.insert("dte".to_string(), json!(7));

        let results = vec![
            make_result_with_params(p1, 1.0, 0.0),
            make_result_with_params(p2, 3.0, 0.0),
            make_result_with_params(p3, 2.0, 0.0),
        ];

        let mut grid = HashMap::new();
        grid.insert("dte".to_string(), vec![json!(7), json!(14)]);

        let sens = compute_sensitivity(&results, &grid, "sharpe");
        let dte_stats = sens.get("dte").unwrap();
        assert_eq!(dte_stats.len(), 2);

        // dte=7: results with sharpe 1.0 and 2.0, avg = 1.5
        let stat_7 = dte_stats.iter().find(|s| s.value == "7").unwrap();
        assert!((stat_7.avg_metric - 1.5).abs() < 1e-10);
        assert_eq!(stat_7.count, 2);

        // dte=14: results with sharpe 3.0, avg = 3.0
        let stat_14 = dte_stats.iter().find(|s| s.value == "14").unwrap();
        assert!((stat_14.avg_metric - 3.0).abs() < 1e-10);
        assert_eq!(stat_14.count, 1);
    }

    #[test]
    fn sensitivity_multi_param() {
        let mut p1 = HashMap::new();
        p1.insert("dte".to_string(), json!(7));
        p1.insert("delta".to_string(), json!(0.3));

        let mut p2 = HashMap::new();
        p2.insert("dte".to_string(), json!(7));
        p2.insert("delta".to_string(), json!(0.4));

        let results = vec![
            make_result_with_params(p1, 2.0, 0.0),
            make_result_with_params(p2, 4.0, 0.0),
        ];

        let mut grid = HashMap::new();
        grid.insert("dte".to_string(), vec![json!(7)]);
        grid.insert("delta".to_string(), vec![json!(0.3), json!(0.4)]);

        let sens = compute_sensitivity(&results, &grid, "sharpe");
        assert!(sens.contains_key("dte"));
        assert!(sens.contains_key("delta"));

        // Both results have dte=7, avg sharpe = 3.0
        let dte_stats = sens.get("dte").unwrap();
        assert_eq!(dte_stats.len(), 1);
        assert!((dte_stats[0].avg_metric - 3.0).abs() < 1e-10);
    }

    #[test]
    fn sensitivity_empty_results() {
        let grid = HashMap::new();
        let sens = compute_sensitivity(&[], &grid, "sharpe");
        assert!(sens.is_empty());
    }

    #[test]
    fn sensitivity_filters_non_finite() {
        let mut p1 = HashMap::new();
        p1.insert("x".to_string(), json!(1));
        let mut p2 = HashMap::new();
        p2.insert("x".to_string(), json!(1));

        let mut r1 = make_result_with_params(p1, f64::NAN, 0.0);
        r1.sharpe = f64::NAN;
        let r2 = make_result_with_params(p2, 2.0, 0.0);

        let mut grid = HashMap::new();
        grid.insert("x".to_string(), vec![json!(1)]);

        let sens = compute_sensitivity(&[r1, r2], &grid, "sharpe");
        let stats = sens.get("x").unwrap();
        // NaN filtered out, only finite sharpe=2.0 counted
        assert_eq!(stats[0].count, 1);
        assert!((stats[0].avg_metric - 2.0).abs() < 1e-10);
    }

    #[test]
    fn sensitivity_string_param_values() {
        let mut p1 = HashMap::new();
        p1.insert("slippage".to_string(), json!("mid"));
        let mut p2 = HashMap::new();
        p2.insert("slippage".to_string(), json!("spread"));

        let results = vec![
            make_result_with_params(p1, 1.5, 0.0),
            make_result_with_params(p2, 2.5, 0.0),
        ];

        let mut grid = HashMap::new();
        grid.insert("slippage".to_string(), vec![json!("mid"), json!("spread")]);

        let sens = compute_sensitivity(&results, &grid, "sharpe");
        let stats = sens.get("slippage").unwrap();
        let mid = stats.iter().find(|s| s.value == "mid").unwrap();
        assert!((mid.avg_metric - 1.5).abs() < 1e-10);
    }
}
