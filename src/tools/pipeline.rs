//! Backtest pipeline orchestrator.
//!
//! When `pipeline=true` (default) on a sweep, this module chains:
//! sweep -> significance gate -> walk-forward -> OOS data gate -> monte carlo.
//! Each stage is fail-tolerant and reports status for frontend rendering.

use std::collections::HashMap;

use anyhow::Result;
use serde_json::Value;

use crate::constants::{MIN_RETURNS_FOR_BOOTSTRAP, P_VALUE_THRESHOLD};
use crate::server::OptopsyServer;
use crate::tools::response_types::pipeline::{PipelineResponse, StageInfo, StageStatus};
use crate::tools::response_types::sweep::SweepResponse;

/// Number of top parameter combos to use when building the walk-forward grid.
const TOP_COMBOS_FOR_WF: usize = 3;

/// Run the full analysis pipeline after a sweep completes.
///
/// Receives the already-finished sweep result and chains walk-forward validation
/// and Monte Carlo risk analysis, gated by statistical thresholds.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn run_pipeline(
    server: &OptopsyServer,
    strategy: &str,
    symbol: &str,
    capital: f64,
    objective: &str,
    sweep_id: String,
    run_ids: Vec<String>,
    sweep_response: SweepResponse,
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
        key_findings.push(format!(
            "Best sweep combo: Sharpe={:.2}, CAGR={:.1}%, max DD={:.1}% ({} trades)",
            best.sharpe,
            best.cagr * 100.0,
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
            reason: Some(
                "No parameter combos passed significance gate (all p > 0.05 or insufficient trades)"
                    .to_string(),
            ),
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
    let script_source = server
        .strategy_store
        .as_ref()
        .and_then(|store| {
            store.get_source(strategy).ok().flatten().or_else(|| {
                store
                    .get_source_by_name(strategy)
                    .ok()
                    .flatten()
                    .map(|(_id, src)| src)
            })
        })
        .map(|raw| crate::tools::run_script::maybe_transpile(raw).unwrap_or_else(|_| String::new()))
        .filter(|s| !s.is_empty());

    // Build base params from the best combo so WF has symbol/CAPITAL
    let base_params = top_combos.first().map(|combo| {
        let mut bp = (**combo).clone();
        if !bp.contains_key("symbol") {
            bp.insert("symbol".to_string(), Value::String(symbol.to_string()));
        }
        bp
    });

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
        base_params,
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
            ));
        }
    };

    // Gate 2: OOS data sufficiency
    let wf_ref = wf_response.as_ref().unwrap();
    let oos_equity_len = wf_ref.stitched_equity.len();

    if oos_equity_len < MIN_RETURNS_FOR_BOOTSTRAP {
        stages.push(StageInfo {
            name: "oos_data_gate".to_string(),
            status: StageStatus::Failed,
            reason: Some(format!(
                "Insufficient OOS data: {oos_equity_len} equity points < {MIN_RETURNS_FOR_BOOTSTRAP} minimum for bootstrap",
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
            "Monte Carlo skipped: only {oos_equity_len} OOS equity points (need {MIN_RETURNS_FOR_BOOTSTRAP})",
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
        ));
    }

    stages.push(StageInfo {
        name: "oos_data_gate".to_string(),
        status: StageStatus::Completed,
        reason: None,
        duration_ms: 0,
    });

    // Stage 3: Monte Carlo on OOS equity returns
    let returns = equity_to_returns(&wf_ref.stitched_equity);
    let initial_capital_oos = wf_ref
        .stitched_equity
        .first()
        .map_or(capital, |ep| ep.equity);
    let horizon = returns.len().min(252); // 1 year or available data
    let mc_label = symbol.to_string();

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
            stages.push(StageInfo {
                name: "monte_carlo".to_string(),
                status: StageStatus::Failed,
                reason: Some(format!("Monte Carlo task panicked: {e}")),
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
    ))
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Select top parameter combos for walk-forward validation.
///
/// If a permutation test was run, returns combos where `significant == true`.
/// Otherwise, returns the top N combos by objective (already ranked).
fn select_top_combos(sweep: &SweepResponse) -> Vec<&HashMap<String, Value>> {
    let has_permutation = sweep
        .ranked_results
        .first()
        .is_some_and(|r| r.significant.is_some());

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
fn build_wf_params_grid(combos: &[&HashMap<String, Value>]) -> HashMap<String, Vec<Value>> {
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
) -> PipelineResponse {
    let completed = stages
        .iter()
        .filter(|s| matches!(s.status, StageStatus::Completed))
        .count();
    let total = stages.len();

    let summary = format!(
        "Pipeline completed: {completed}/{total} stages passed. \
         {} combos tested, best Sharpe={:.2}.",
        sweep.combinations_run,
        sweep.best_result.as_ref().map_or(0.0, |r| r.sharpe),
    );

    PipelineResponse {
        summary,
        stages,
        sweep_id,
        run_ids,
        sweep,
        walk_forward,
        monte_carlo,
        key_findings,
        total_duration_ms: pipeline_start.elapsed().as_millis() as u64,
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
}
