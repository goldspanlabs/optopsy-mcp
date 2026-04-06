//! Backtest pipeline orchestrator.
//!
//! When `pipeline=true` on a sweep, this module chains:
//! sweep -> significance gate -> walk-forward -> OOS data gate -> monte carlo.
//! Each stage is fail-tolerant and reports status for frontend rendering.

use std::collections::HashMap;

use anyhow::Result;
use serde_json::Value;

use crate::constants::{MIN_RETURNS_FOR_BOOTSTRAP, P_VALUE_THRESHOLD};
use crate::server::OptopsyServer;
use crate::tools::response_types::pipeline::{PipelineResponse, StageInfo, StageStatus};
use crate::tools::response_types::sweep::{SweepResponse, SweepResult};

/// Number of top parameter combos to use when building the walk-forward grid.
const TOP_COMBOS_FOR_WF: usize = 3;

/// Run the full analysis pipeline after a sweep completes.
///
/// Receives the already-finished sweep result and chains walk-forward validation
/// and Monte Carlo risk analysis, gated by statistical thresholds.
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::implicit_hasher
)]
pub async fn run_pipeline(
    server: &OptopsyServer,
    strategy: &str,
    symbol: &str,
    capital: f64,
    objective: &str,
    sweep_id: String,
    run_ids: Vec<String>,
    sweep_response: SweepResponse,
    base_params: HashMap<String, Value>,
) -> Result<PipelineResponse> {
    let pipeline_start = std::time::Instant::now();
    let mut stages: Vec<StageInfo> = Vec::new();
    let mut key_findings: Vec<String> = Vec::new();

    // Stage 1: Sweep (already completed)
    stages.push(StageInfo {
        name: "sweep".to_string(),
        status: StageStatus::Completed,
        reason: None,
        duration_ms: sweep_response.execution_time_ms,
    });

    // Collect top findings from sweep
    if let Some(best) = &sweep_response.best_result {
        let (headline_metric, secondary_metric) =
            best_sweep_combo_metrics(best, sweep_response.objective.as_str());
        key_findings.push(format!(
            "Best sweep combo: {headline_metric}, {secondary_metric}, max DD={:.1}% ({} trades)",
            best.max_drawdown * 100.0,
            best.trades,
        ));
    }

    // Gate 1: Significance — decide which combos to validate
    let top_combos = select_top_combos(&sweep_response);

    if top_combos.is_empty() {
        stages.push(StageInfo {
            name: "significance_gate".to_string(),
            status: StageStatus::Failed,
            reason: Some(format!(
                "No parameter combos passed significance gate (all p > {P_VALUE_THRESHOLD:.2} or insufficient trades)"
            )),
            duration_ms: 0,
        });

        // Skip remaining stages
        stages.push(StageInfo {
            name: "walk_forward".to_string(),
            status: StageStatus::Skipped,
            reason: Some("Skipped: significance gate failed".to_string()),
            duration_ms: 0,
        });
        stages.push(StageInfo {
            name: "oos_data_gate".to_string(),
            status: StageStatus::Skipped,
            reason: Some("Skipped: significance gate failed".to_string()),
            duration_ms: 0,
        });
        stages.push(StageInfo {
            name: "monte_carlo".to_string(),
            status: StageStatus::Skipped,
            reason: Some("Skipped: significance gate failed".to_string()),
            duration_ms: 0,
        });

        key_findings
            .push("Pipeline stopped: no statistically significant parameter combos".to_string());

        return Ok(build_response(
            stages,
            sweep_id,
            run_ids,
            sweep_response,
            None,
            None,
            key_findings,
            pipeline_start,
            symbol,
        ));
    }

    stages.push(StageInfo {
        name: "significance_gate".to_string(),
        status: StageStatus::Completed,
        reason: None,
        duration_ms: 0,
    });

    // Stage 2: Walk-forward validation
    let params_grid = build_wf_params_grid(&top_combos);
    let wf_start = std::time::Instant::now();

    // Resolve script source from strategy store so WF doesn't need filesystem access
    let raw_source = server.strategy_store.as_ref().and_then(|store| {
        store.get_source(strategy).ok().flatten().or_else(|| {
            store
                .get_source_by_name(strategy)
                .ok()
                .flatten()
                .map(|(_id, src)| src)
        })
    });
    let script_source = match raw_source {
        Some(raw) => Some(crate::tools::run_script::maybe_transpile(raw)?),
        None => None,
    };

    // Ensure base_params has the symbol for walk-forward data loading
    let mut wf_base_params = base_params;
    if !wf_base_params.contains_key("symbol") {
        wf_base_params.insert("symbol".to_string(), Value::String(symbol.to_string()));
    }
    let wf_base_params = Some(wf_base_params);

    let wf_result = crate::tools::walk_forward::execute(
        &server.cache,
        server.adjustment_store.clone(),
        strategy,
        symbol,
        capital,
        params_grid,
        Some(objective.to_string()),
        None, // n_windows (default 5)
        None, // mode (default rolling)
        None, // train_pct (default 0.70)
        None, // start_date
        None, // end_date
        None, // profile
        script_source,
        wf_base_params,
    )
    .await;

    let wf_duration = wf_start.elapsed().as_millis() as u64;

    let wf_response = match wf_result {
        Ok(wf) => {
            stages.push(StageInfo {
                name: "walk_forward".to_string(),
                status: StageStatus::Completed,
                reason: None,
                duration_ms: wf_duration,
            });

            key_findings.push(format!(
                "Walk-forward efficiency ratio: {:.2} (OOS Sharpe={:.2}, max DD={:.1}%)",
                wf.efficiency_ratio,
                wf.stitched_metrics.sharpe,
                wf.stitched_metrics.max_drawdown * 100.0,
            ));

            Some(wf)
        }
        Err(e) => {
            stages.push(StageInfo {
                name: "walk_forward".to_string(),
                status: StageStatus::Failed,
                reason: Some(format!("Walk-forward failed: {e}")),
                duration_ms: wf_duration,
            });

            // Skip remaining stages
            stages.push(StageInfo {
                name: "oos_data_gate".to_string(),
                status: StageStatus::Skipped,
                reason: Some("Skipped: walk-forward failed".to_string()),
                duration_ms: 0,
            });
            stages.push(StageInfo {
                name: "monte_carlo".to_string(),
                status: StageStatus::Skipped,
                reason: Some("Skipped: walk-forward failed".to_string()),
                duration_ms: 0,
            });

            key_findings.push(format!("Walk-forward validation failed: {e}"));

            return Ok(build_response(
                stages,
                sweep_id,
                run_ids,
                sweep_response,
                None,
                None,
                key_findings,
                pipeline_start,
                symbol,
            ));
        }
    };

    // Gate 2: OOS data sufficiency — gate on actual returns count, not equity points,
    // since Monte Carlo needs MIN_RETURNS_FOR_BOOTSTRAP returns (equity.len() - 1).
    let wf_ref = wf_response.as_ref().unwrap();
    let returns = equity_to_returns(&wf_ref.stitched_equity);
    let oos_returns_len = returns.len();

    if oos_returns_len < MIN_RETURNS_FOR_BOOTSTRAP {
        stages.push(StageInfo {
            name: "oos_data_gate".to_string(),
            status: StageStatus::Failed,
            reason: Some(format!(
                "Insufficient OOS data: {oos_returns_len} returns < {MIN_RETURNS_FOR_BOOTSTRAP} minimum for bootstrap",
            )),
            duration_ms: 0,
        });
        stages.push(StageInfo {
            name: "monte_carlo".to_string(),
            status: StageStatus::Skipped,
            reason: Some("Skipped: OOS data gate failed".to_string()),
            duration_ms: 0,
        });

        key_findings.push(format!(
            "Monte Carlo skipped: only {oos_returns_len} OOS returns (need {MIN_RETURNS_FOR_BOOTSTRAP})",
        ));

        return Ok(build_response(
            stages,
            sweep_id,
            run_ids,
            sweep_response,
            wf_response,
            None,
            key_findings,
            pipeline_start,
            symbol,
        ));
    }

    stages.push(StageInfo {
        name: "oos_data_gate".to_string(),
        status: StageStatus::Completed,
        reason: None,
        duration_ms: 0,
    });

    // Stage 3: Monte Carlo on OOS equity returns (already computed above)
    let initial_capital_oos = wf_ref
        .stitched_equity
        .first()
        .map_or(capital, |ep| ep.equity);
    let horizon = returns.len().min(252); // 1 year or available data
    let mc_label = symbol.to_uppercase();

    let mc_start = std::time::Instant::now();
    let mc_result = tokio::task::spawn_blocking(move || {
        crate::tools::monte_carlo::execute_from_returns(
            &returns,
            &mc_label,
            10_000,
            horizon,
            initial_capital_oos,
            Some(42),
        )
    })
    .await;
    let mc_duration = mc_start.elapsed().as_millis() as u64;

    let mc_response = match mc_result {
        Ok(Ok(mc)) => {
            stages.push(StageInfo {
                name: "monte_carlo".to_string(),
                status: StageStatus::Completed,
                reason: None,
                duration_ms: mc_duration,
            });

            key_findings.push(format!(
                "Monte Carlo (OOS): P(loss)={:.1}%, median max DD={:.1}%",
                mc.ruin_analysis.prob_negative_return * 100.0,
                mc.drawdown_distribution.median,
            ));

            Some(mc)
        }
        Ok(Err(e)) => {
            stages.push(StageInfo {
                name: "monte_carlo".to_string(),
                status: StageStatus::Failed,
                reason: Some(format!("Monte Carlo failed: {e}")),
                duration_ms: mc_duration,
            });
            key_findings.push(format!("Monte Carlo simulation failed: {e}"));
            None
        }
        Err(e) => {
            let reason = if e.is_panic() {
                format!("Monte Carlo task panicked: {e}")
            } else {
                format!("Monte Carlo task cancelled: {e}")
            };
            stages.push(StageInfo {
                name: "monte_carlo".to_string(),
                status: StageStatus::Failed,
                reason: Some(reason),
                duration_ms: mc_duration,
            });
            None
        }
    };

    Ok(build_response(
        stages,
        sweep_id,
        run_ids,
        sweep_response,
        wf_response,
        mc_response,
        key_findings,
        pipeline_start,
        symbol,
    ))
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Select top parameter combos for walk-forward validation.
///
/// If a permutation test was run, returns combos where `significant == true`.
/// Otherwise, returns the top N combos by objective (already ranked).
pub(crate) fn select_top_combos(sweep: &SweepResponse) -> Vec<&HashMap<String, Value>> {
    let has_permutation = sweep.multiple_comparisons.is_some()
        || sweep.ranked_results.iter().any(|r| r.p_value.is_some());

    if has_permutation {
        // Return all significant combos (up to TOP_COMBOS_FOR_WF)
        sweep
            .ranked_results
            .iter()
            .filter(|r| {
                r.significant == Some(true) && r.p_value.is_some_and(|p| p < P_VALUE_THRESHOLD)
            })
            .take(TOP_COMBOS_FOR_WF)
            .map(|r| &r.params)
            .collect()
    } else {
        // No permutation test — take top combos by objective ranking
        sweep
            .ranked_results
            .iter()
            .take(TOP_COMBOS_FOR_WF)
            .map(|r| &r.params)
            .collect()
    }
}

/// Build a walk-forward `params_grid` from the top sweep combos.
///
/// For each parameter name, collect the distinct values across the top combos.
/// This gives walk-forward a focused search space around the best-performing region.
pub(crate) fn build_wf_params_grid(
    combos: &[&HashMap<String, Value>],
) -> HashMap<String, Vec<Value>> {
    let mut grid: HashMap<String, Vec<Value>> = HashMap::new();

    for params in combos {
        for (key, value) in *params {
            let entry = grid.entry(key.clone()).or_default();
            // Deduplicate values (JSON equality)
            if !entry.iter().any(|v| v == value) {
                entry.push(value.clone());
            }
        }
    }

    grid
}

/// Convert an equity curve to period-over-period returns.
fn equity_to_returns(equity: &[crate::engine::types::EquityPoint]) -> Vec<f64> {
    equity
        .windows(2)
        .filter_map(|pair| {
            let prev = pair[0].equity;
            let curr = pair[1].equity;
            if prev.abs() > f64::EPSILON {
                Some(curr / prev - 1.0)
            } else {
                None
            }
        })
        .collect()
}

/// Assemble the final `PipelineResponse`.
#[allow(clippy::too_many_arguments)]
fn build_response(
    stages: Vec<StageInfo>,
    sweep_id: String,
    run_ids: Vec<String>,
    sweep: SweepResponse,
    walk_forward: Option<crate::tools::response_types::walk_forward::WalkForwardResponse>,
    monte_carlo: Option<crate::tools::response_types::risk::MonteCarloResponse>,
    key_findings: Vec<String>,
    pipeline_start: std::time::Instant,
    symbol: &str,
) -> PipelineResponse {
    let completed = stages
        .iter()
        .filter(|s| matches!(s.status, StageStatus::Completed))
        .count();
    let total = stages.len();

    let objective = &sweep.objective;
    let best_metric = sweep
        .best_result
        .as_ref()
        .map_or(0.0, |r| best_objective_metric(r, objective.as_str()).1);
    let summary = format!(
        "Pipeline completed: {completed}/{total} stages passed. \
         {} combos tested, best {objective}={best_metric:.2}.",
        sweep.combinations_run,
    );

    let upper = symbol.to_uppercase();
    let suggested_next_steps = build_suggested_next_steps(&stages, &upper);

    PipelineResponse {
        summary,
        stages,
        sweep_id,
        run_ids,
        sweep,
        walk_forward,
        monte_carlo,
        key_findings,
        suggested_next_steps,
        total_duration_ms: pipeline_start.elapsed().as_millis() as u64,
    }
}

fn best_objective_metric(result: &SweepResult, objective: &str) -> (&'static str, f64) {
    match objective {
        "sortino" => ("Sortino", result.sortino),
        "profit_factor" => ("Profit factor", result.profit_factor),
        "cagr" => ("CAGR", result.cagr),
        _ => ("Sharpe", result.sharpe),
    }
}

fn best_sweep_combo_metrics(result: &SweepResult, objective: &str) -> (String, String) {
    match objective {
        "sortino" => (
            format!("Sortino={:.2}", result.sortino),
            format!("CAGR={:.1}%", result.cagr * 100.0),
        ),
        "profit_factor" => (
            format!("Profit factor={:.2}", result.profit_factor),
            format!("CAGR={:.1}%", result.cagr * 100.0),
        ),
        "cagr" => (
            format!("CAGR={:.1}%", result.cagr * 100.0),
            format!("Sharpe={:.2}", result.sharpe),
        ),
        _ => (
            format!("Sharpe={:.2}", result.sharpe),
            format!("CAGR={:.1}%", result.cagr * 100.0),
        ),
    }
}

/// Build suggested next steps based on which stages completed.
fn build_suggested_next_steps(stages: &[StageInfo], symbol: &str) -> Vec<String> {
    let sig_failed = stages
        .iter()
        .any(|s| s.name == "significance_gate" && matches!(s.status, StageStatus::Failed));
    let wf_failed = stages
        .iter()
        .any(|s| s.name == "walk_forward" && matches!(s.status, StageStatus::Failed));
    let mc_completed = stages
        .iter()
        .any(|s| s.name == "monte_carlo" && matches!(s.status, StageStatus::Completed));

    if sig_failed {
        vec![
            "[NEXT] Re-run the sweep with wider parameter ranges or a different strategy".to_string(),
            "[TIP] Consider running with num_permutations=0 to skip significance testing and force walk-forward validation".to_string(),
        ]
    } else if wf_failed {
        vec![
            "[NEXT] Check that the strategy and symbol have sufficient data for walk-forward windows".to_string(),
            format!("[THEN] Call drawdown_analysis(symbol=\"{symbol}\") to evaluate risk profile"),
        ]
    } else if mc_completed {
        vec![
            format!("[NEXT] Call factor_attribution(symbol=\"{symbol}\") to check if alpha is genuine or factor exposure"),
            format!("[THEN] Call benchmark_analysis(symbol=\"{symbol}\") for relative performance metrics"),
            format!("[TIP] Call regime_detect(symbol=\"{symbol}\") to see if performance varies across market regimes"),
        ]
    } else {
        // OOS gate failed but WF succeeded
        vec![
            format!("[NEXT] Call drawdown_analysis(symbol=\"{symbol}\") to analyze drawdown episodes"),
            format!("[THEN] Call factor_attribution(symbol=\"{symbol}\") to decompose returns into factor exposures"),
            "[TIP] Monte Carlo was skipped due to insufficient OOS data — consider longer backtest period".to_string(),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equity_to_returns_basic() {
        use crate::engine::types::EquityPoint;
        use chrono::NaiveDateTime;

        let dt = NaiveDateTime::default();
        let equity = vec![
            EquityPoint {
                datetime: dt,
                equity: 100.0,
                unrealized: None,
            },
            EquityPoint {
                datetime: dt,
                equity: 110.0,
                unrealized: None,
            },
            EquityPoint {
                datetime: dt,
                equity: 105.0,
                unrealized: None,
            },
        ];
        let returns = equity_to_returns(&equity);
        assert_eq!(returns.len(), 2);
        assert!((returns[0] - 0.1).abs() < 1e-10);
        assert!((returns[1] - (-5.0 / 110.0)).abs() < 1e-10);
    }

    #[test]
    fn equity_to_returns_empty() {
        let returns = equity_to_returns(&[]);
        assert!(returns.is_empty());
    }

    #[test]
    fn build_wf_params_grid_deduplicates() {
        let mut combo1 = HashMap::new();
        combo1.insert("delta".to_string(), Value::from(0.3));
        combo1.insert("dte".to_string(), Value::from(45));

        let mut combo2 = HashMap::new();
        combo2.insert("delta".to_string(), Value::from(0.3)); // duplicate
        combo2.insert("dte".to_string(), Value::from(30));

        let top = vec![&combo1, &combo2];
        let grid = build_wf_params_grid(&top);

        assert_eq!(grid["delta"].len(), 1); // deduplicated
        assert_eq!(grid["dte"].len(), 2);
    }

    #[test]
    fn best_objective_metric_uses_requested_objective() {
        let result = SweepResult {
            rank: 1,
            params: HashMap::new(),
            sharpe: 1.2,
            sortino: 2.4,
            pnl: 1000.0,
            trades: 10,
            win_rate: 0.6,
            max_drawdown: 0.1,
            profit_factor: 1.8,
            cagr: 0.15,
            calmar: 1.5,
            p_value: None,
            significant: None,
        };

        assert_eq!(best_objective_metric(&result, "sortino"), ("Sortino", 2.4));
        assert_eq!(
            best_objective_metric(&result, "profit_factor"),
            ("Profit factor", 1.8)
        );
        assert_eq!(best_objective_metric(&result, "cagr"), ("CAGR", 0.15));
        assert_eq!(best_objective_metric(&result, "sharpe"), ("Sharpe", 1.2));
    }

    #[test]
    fn best_sweep_combo_metrics_formats_cagr_without_duplication() {
        let result = SweepResult {
            rank: 1,
            params: HashMap::new(),
            sharpe: 1.2,
            sortino: 2.4,
            pnl: 1000.0,
            trades: 10,
            win_rate: 0.6,
            max_drawdown: 0.1,
            profit_factor: 1.8,
            cagr: 0.15,
            calmar: 1.5,
            p_value: None,
            significant: None,
        };

        assert_eq!(
            best_sweep_combo_metrics(&result, "cagr"),
            ("CAGR=15.0%".to_string(), "Sharpe=1.20".to_string())
        );
    }
}
