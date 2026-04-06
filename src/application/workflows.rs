//! Workflow orchestrator for multi-step strategy workflows.

use std::collections::HashMap;
use std::time::Instant;

use anyhow::Result;
use serde_json::Value;

use crate::application::pipeline::{self, PipelineRequest};
use crate::application::sweeps;
use crate::server::OptopsyServer;
use crate::tools::response_types::walk_forward::WalkForwardResponse;
use crate::tools::response_types::workflow::{
    StrategyEvaluationResponse, WalkForwardRobustnessCheck, WorkflowKind, WorkflowResponse,
};

const DEFAULT_STRATEGY_EVAL_PERMUTATIONS: usize = 200;

pub struct WorkflowRequest {
    pub kind: WorkflowKind,
    pub pipeline: PipelineRequest,
}

#[derive(Clone, Copy)]
struct RobustnessConfig {
    label: &'static str,
    mode: &'static str,
    n_windows: usize,
    train_pct: f64,
}

pub async fn execute(
    server: &OptopsyServer,
    request: &WorkflowRequest,
    source: &str,
) -> Result<WorkflowResponse> {
    match request.kind {
        WorkflowKind::BaselineValidation => Ok(WorkflowResponse::BaselineValidation(
            pipeline::execute(server, &request.pipeline, source).await?,
        )),
        WorkflowKind::StrategyEvaluation => Ok(WorkflowResponse::StrategyEvaluation(
            execute_strategy_evaluation(server, &request.pipeline, source).await?,
        )),
    }
}

async fn execute_strategy_evaluation(
    server: &OptopsyServer,
    request: &PipelineRequest,
    source: &str,
) -> Result<StrategyEvaluationResponse> {
    let started_at = Instant::now();
    let mut eval_request = request.clone();
    if eval_request.num_permutations == 0 {
        eval_request.num_permutations = DEFAULT_STRATEGY_EVAL_PERMUTATIONS;
    }

    let pipeline = pipeline::execute(server, &eval_request, source).await?;
    let robustness_checks = if pipeline.walk_forward.is_some() {
        run_walk_forward_robustness(server, &eval_request, &pipeline).await?
    } else {
        Vec::new()
    };

    let mut key_findings = pipeline.key_findings.clone();
    if !robustness_checks.is_empty() {
        let anchored: Vec<f64> = robustness_checks
            .iter()
            .filter(|check| check.mode == "anchored")
            .map(|check| check.efficiency_ratio)
            .collect();
        let rolling: Vec<f64> = robustness_checks
            .iter()
            .filter(|check| check.mode == "rolling")
            .map(|check| check.efficiency_ratio)
            .collect();
        if !anchored.is_empty() && !rolling.is_empty() {
            let anchored_avg = anchored.iter().sum::<f64>() / anchored.len() as f64;
            let rolling_avg = rolling.iter().sum::<f64>() / rolling.len() as f64;
            key_findings.push(format!(
                "Walk-forward robustness: anchored avg efficiency={anchored_avg:.2}, rolling avg efficiency={rolling_avg:.2}"
            ));
        }
    }

    let final_verdict = synthesize_verdict(&pipeline, &robustness_checks);
    let summary = format!(
        "Strategy evaluation completed: baseline pipeline plus {} walk-forward robustness checks. {}",
        robustness_checks.len(),
        final_verdict
    );

    let mut suggested_next_steps = vec![
        "[NEXT] Compare anchored vs rolling walk-forward before trusting the default validator"
            .to_string(),
        "[THEN] Add regime filters if the strategy remains parameter-unstable".to_string(),
    ];
    suggested_next_steps.extend(pipeline.suggested_next_steps.iter().cloned());

    Ok(StrategyEvaluationResponse {
        summary,
        pipeline,
        robustness_checks,
        final_verdict,
        key_findings,
        suggested_next_steps,
        total_duration_ms: started_at.elapsed().as_millis() as u64,
    })
}

async fn run_walk_forward_robustness(
    server: &OptopsyServer,
    request: &PipelineRequest,
    pipeline: &crate::tools::response_types::pipeline::PipelineResponse,
) -> Result<Vec<WalkForwardRobustnessCheck>> {
    let top_combos = crate::tools::pipeline::select_top_combos(&pipeline.sweep);
    if top_combos.is_empty() {
        return Ok(Vec::new());
    }

    let params_grid = crate::tools::pipeline::build_wf_params_grid(&top_combos);
    let strategy_store = server.require_strategy_store()?;
    let (strategy_key, script_source) =
        sweeps::resolve_strategy_source_from_store(strategy_store.as_ref(), &request.strategy)?;

    let symbol = request
        .params
        .get("symbol")
        .and_then(Value::as_str)
        .map_or_else(|| "SPY".to_string(), ToOwned::to_owned);
    let capital = request
        .params
        .get("CAPITAL")
        .and_then(Value::as_f64)
        .unwrap_or(100_000.0);

    let mut base_params = request.params.clone();
    if !base_params.contains_key("symbol") {
        base_params.insert("symbol".to_string(), Value::String(symbol.clone()));
    }

    let configs = [
        RobustnessConfig {
            label: "rolling-5-70",
            mode: "rolling",
            n_windows: 5,
            train_pct: 0.70,
        },
        RobustnessConfig {
            label: "rolling-8-70",
            mode: "rolling",
            n_windows: 8,
            train_pct: 0.70,
        },
        RobustnessConfig {
            label: "rolling-5-80",
            mode: "rolling",
            n_windows: 5,
            train_pct: 0.80,
        },
        RobustnessConfig {
            label: "anchored-5-70",
            mode: "anchored",
            n_windows: 5,
            train_pct: 0.70,
        },
        RobustnessConfig {
            label: "anchored-8-70",
            mode: "anchored",
            n_windows: 8,
            train_pct: 0.70,
        },
        RobustnessConfig {
            label: "anchored-5-80",
            mode: "anchored",
            n_windows: 5,
            train_pct: 0.80,
        },
    ];

    let mut checks = Vec::with_capacity(configs.len());
    for config in configs {
        let response = execute_walk_forward_variant(
            server,
            &strategy_key,
            &symbol,
            capital,
            &params_grid,
            &request.objective,
            config,
            &script_source,
            &base_params,
        )
        .await?;

        checks.push(WalkForwardRobustnessCheck {
            label: config.label.to_string(),
            mode: response.mode,
            n_windows: response.n_windows,
            train_pct: config.train_pct,
            efficiency_ratio: response.efficiency_ratio,
            profitable_windows: response
                .windows
                .iter()
                .filter(|window| window.out_of_sample_metric > 0.0)
                .count(),
            total_windows: response.windows.len(),
            param_stability: summarize_param_stability(&response.windows),
        });
    }

    Ok(checks)
}

#[allow(clippy::too_many_arguments)]
async fn execute_walk_forward_variant(
    server: &OptopsyServer,
    strategy: &str,
    symbol: &str,
    capital: f64,
    params_grid: &HashMap<String, Vec<Value>>,
    objective: &str,
    config: RobustnessConfig,
    script_source: &str,
    base_params: &HashMap<String, Value>,
) -> Result<WalkForwardResponse> {
    crate::tools::walk_forward::execute(
        &server.cache,
        server.adjustment_store.clone(),
        strategy,
        symbol,
        capital,
        params_grid.clone(),
        Some(objective.to_string()),
        Some(config.n_windows),
        Some(config.mode.to_string()),
        Some(config.train_pct),
        None,
        None,
        None,
        Some(script_source.to_string()),
        Some(base_params.clone()),
    )
    .await
}

fn summarize_param_stability(
    windows: &[crate::tools::response_types::walk_forward::WalkForwardWindowResult],
) -> String {
    if windows.len() < 2 {
        return "single_window".to_string();
    }

    let unique = windows
        .iter()
        .map(|window| serde_json::to_string(&window.best_params).unwrap_or_default())
        .collect::<std::collections::HashSet<_>>()
        .len();

    if unique == 1 {
        "stable".to_string()
    } else if unique <= (windows.len() / 2).max(2) {
        "moderate".to_string()
    } else {
        "unstable".to_string()
    }
}

fn synthesize_verdict(
    pipeline: &crate::tools::response_types::pipeline::PipelineResponse,
    robustness_checks: &[WalkForwardRobustnessCheck],
) -> String {
    let anchored_avg = average_efficiency(robustness_checks, "anchored");
    let rolling_avg = average_efficiency(robustness_checks, "rolling");
    let oos_sharpe = pipeline
        .walk_forward
        .as_ref()
        .map(|wf| wf.stitched_metrics.sharpe)
        .unwrap_or_default();

    if oos_sharpe >= 0.5 && anchored_avg >= 0.8 && rolling_avg >= 0.7 {
        "Verdict: promising and reasonably robust, but still confirm with portfolio-level validation before live deployment.".to_string()
    } else if oos_sharpe >= 0.1 && anchored_avg >= 0.8 {
        "Verdict: keep and iterate. The signal appears real, but the standalone OOS edge is still too modest for immediate deployment.".to_string()
    } else {
        "Verdict: research-only. The workflow passes baseline gates, but robustness is not strong enough for deployment.".to_string()
    }
}

fn average_efficiency(checks: &[WalkForwardRobustnessCheck], mode: &str) -> f64 {
    let filtered: Vec<f64> = checks
        .iter()
        .filter(|check| check.mode == mode)
        .map(|check| check.efficiency_ratio)
        .collect();
    if filtered.is_empty() {
        0.0
    } else {
        filtered.iter().sum::<f64>() / filtered.len() as f64
    }
}
