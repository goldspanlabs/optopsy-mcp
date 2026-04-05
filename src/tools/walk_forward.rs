//! MCP tool handler for walk-forward optimization.
//!
//! Single `execute()` function used by both the MCP tool and the REST handler.
//! Delegates to `engine::walk_forward::execute()` and enriches the result with
//! AI-formatted summary, key findings, and suggested next steps.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;

use crate::data::adjustment_store::SqliteAdjustmentStore;
use crate::data::cache::CachedStore;
use crate::engine::walk_forward::{self as wf_engine, WalkForwardParams, WfMode, WfObjective};
use crate::scripting::engine::{CachingDataLoader, CancelCallback};
use crate::tools::response_types::walk_forward::{WalkForwardResponse, WalkForwardWindowResult};

/// Execute walk-forward optimization with AI-formatted response.
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::implicit_hasher
)]
pub async fn execute(
    cache: &Arc<CachedStore>,
    adjustment_store: Option<Arc<SqliteAdjustmentStore>>,
    strategy: &str,
    symbol: &str,
    capital: f64,
    params_grid: HashMap<String, Vec<Value>>,
    objective: Option<String>,
    n_windows: Option<usize>,
    mode: Option<String>,
    train_pct: Option<f64>,
    start_date: Option<String>,
    end_date: Option<String>,
    profile: Option<String>,
    script_source: String,
) -> Result<WalkForwardResponse> {
    let wf_objective = match objective.as_deref() {
        Some("sortino") => WfObjective::Sortino,
        Some("profit_factor") => WfObjective::ProfitFactor,
        Some("cagr") => WfObjective::Cagr,
        None | Some("sharpe") => WfObjective::Sharpe,
        Some(other) => anyhow::bail!(
            "Invalid objective '{other}', expected: sharpe, sortino, profit_factor, cagr"
        ),
    };
    let wf_mode = match mode.as_deref() {
        Some("anchored") => WfMode::Anchored,
        None | Some("rolling") => WfMode::Rolling,
        Some(other) => {
            anyhow::bail!("Invalid mode '{other}', expected: rolling, anchored")
        }
    };

    let engine_params = WalkForwardParams {
        strategy: strategy.to_string(),
        symbol: symbol.to_string(),
        capital,
        params_grid,
        objective: wf_objective,
        n_windows: n_windows.unwrap_or(5),
        mode: wf_mode,
        train_pct: train_pct.unwrap_or(0.70),
        start_date,
        end_date,
        profile,
        script_source,
        base_params: None,
    };

    let obj_str = engine_params.objective.clone();
    let mode_str = engine_params.mode.clone();
    let strat = engine_params.strategy.clone();
    let sym = engine_params.symbol.clone();

    let loader = CachingDataLoader::new(Arc::clone(cache), adjustment_store);
    let no_cancel: CancelCallback = Box::new(|| false);
    let result = wf_engine::execute(engine_params, &loader, &no_cancel, |_, _| {}).await?;

    // Map engine window results to tool response type
    let windows: Vec<WalkForwardWindowResult> = result
        .windows
        .into_iter()
        .map(|w| WalkForwardWindowResult {
            window_idx: w.window_idx,
            train_start: w.train_start,
            train_end: w.train_end,
            test_start: w.test_start,
            test_end: w.test_end,
            best_params: w.best_params,
            in_sample_metric: w.in_sample_metric,
            out_of_sample_metric: w.out_of_sample_metric,
        })
        .collect();

    let upper = sym.to_uppercase();
    let obj_label = match obj_str {
        WfObjective::Sharpe => "sharpe",
        WfObjective::Sortino => "sortino",
        WfObjective::ProfitFactor => "profit_factor",
        WfObjective::Cagr => "cagr",
    }
    .to_string();
    let mode_label = match mode_str {
        WfMode::Rolling => "rolling",
        WfMode::Anchored => "anchored",
    }
    .to_string();
    let er = result.efficiency_ratio;
    let n_actual = windows.len();

    // Build AI summary
    let er_assessment = if er >= 0.7 {
        "strong (strategy generalizes well)"
    } else if er >= 0.5 {
        "acceptable (moderate OOS degradation)"
    } else if er >= 0.3 {
        "weak (significant OOS degradation — possible overfit)"
    } else {
        "poor (severe OOS degradation — likely overfit)"
    };

    let summary = format!(
        "Walk-forward optimization for {strat} on {upper}: {n_actual} {mode_label} windows, \
         optimizing {obj_label}. Efficiency ratio={er:.2} ({er_assessment}). \
         Stitched OOS Sharpe={:.2}, max drawdown={:.1}%.",
        result.stitched_metrics.sharpe,
        result.stitched_metrics.max_drawdown * 100.0,
    );

    // Build key findings
    let mut key_findings = Vec::new();

    key_findings.push(format!(
        "Efficiency ratio: {er:.2} — OOS performance is {:.0}% of in-sample",
        er * 100.0,
    ));

    // Parameter stability across windows
    if windows.len() >= 2 {
        let all_same = windows
            .windows(2)
            .all(|pair| pair[0].best_params == pair[1].best_params);
        if all_same {
            key_findings
                .push("Parameters are stable across all windows (same best params)".to_string());
        } else {
            key_findings.push(
                "Parameters vary across windows — strategy may be regime-sensitive".to_string(),
            );
        }
    }

    // OOS metric trend
    if windows.len() >= 2 {
        let first_oos = windows[0].out_of_sample_metric;
        let last_oos = windows[windows.len() - 1].out_of_sample_metric;
        if last_oos > first_oos * 1.1 {
            key_findings.push("OOS performance improving over time — favorable trend".to_string());
        } else if last_oos < first_oos * 0.9 {
            key_findings
                .push("OOS performance declining over time — strategy may be decaying".to_string());
        } else {
            key_findings.push("OOS performance relatively stable across windows".to_string());
        }
    }

    key_findings.push(format!(
        "Stitched OOS metrics: Sharpe={:.2}, Sortino={:.2}, CAGR={:.1}%, max DD={:.1}%",
        result.stitched_metrics.sharpe,
        result.stitched_metrics.sortino,
        result.stitched_metrics.cagr * 100.0,
        result.stitched_metrics.max_drawdown * 100.0,
    ));

    let suggested_next_steps = vec![
        format!(
            "[NEXT] Call monte_carlo(symbol=\"{upper}\") to simulate forward-looking risk on the stitched OOS equity"
        ),
        format!(
            "[THEN] Call factor_attribution(symbol=\"{upper}\") to check if OOS alpha is genuine or factor exposure"
        ),
        if er < 0.5 {
            "[TIP] Low efficiency ratio suggests overfitting — try reducing params_grid granularity or increasing train_pct".to_string()
        } else {
            "[TIP] Good efficiency ratio — consider running with more windows or anchored mode for additional validation".to_string()
        },
    ];

    Ok(WalkForwardResponse {
        summary,
        strategy: strat,
        symbol: upper,
        mode: mode_label,
        objective: obj_label,
        n_windows: n_actual,
        efficiency_ratio: result.efficiency_ratio,
        windows,
        stitched_equity: result.stitched_equity,
        stitched_metrics: result.stitched_metrics,
        execution_time_ms: result.execution_time_ms,
        key_findings,
        suggested_next_steps,
    })
}
