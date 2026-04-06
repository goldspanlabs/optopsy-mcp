//! Integration tests for the backtest pipeline gate logic and end-to-end flow.
//!
//! Tests cover:
//! - Significance gate failing (all combos non-significant) → downstream skipped
//! - Significance gate passing (no permutation test) → top combos forwarded
//! - OOS data gate failing (too few equity points) → monte carlo skipped
//! - Full pipeline end-to-end with NVDA fixture data

mod common;

use std::collections::HashMap;

use optopsy_mcp::tools::response_types::pipeline::{PipelineResponse, StageStatus};
use optopsy_mcp::tools::response_types::sweep::{SweepResponse, SweepResult};

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Build a minimal `SweepResponse` with given ranked results.
fn make_sweep(ranked: Vec<SweepResult>) -> SweepResponse {
    let best = ranked.first().cloned();
    let n = ranked.len();
    SweepResponse {
        mode: "grid".to_string(),
        objective: "sharpe".to_string(),
        combinations_total: n,
        combinations_run: n,
        combinations_failed: 0,
        best_result: best,
        ranked_results: ranked,
        dimension_sensitivity: HashMap::new(),
        convergence_trace: None,
        execution_time_ms: 100,
        multiple_comparisons: None,
        full_results: vec![],
    }
}

/// Build a `SweepResult` with given params and significance fields.
fn make_result(
    rank: usize,
    params: HashMap<String, serde_json::Value>,
    sharpe: f64,
    significant: Option<bool>,
    p_value: Option<f64>,
) -> SweepResult {
    SweepResult {
        rank,
        params,
        sharpe,
        sortino: sharpe * 0.8,
        pnl: 5000.0,
        trades: 50,
        win_rate: 0.55,
        max_drawdown: 0.10,
        profit_factor: 1.5,
        cagr: 0.12,
        calmar: 1.2,
        p_value,
        significant,
    }
}

fn assert_stage(response: &PipelineResponse, name: &str, expected: &StageStatus) {
    let stage = response
        .stages
        .iter()
        .find(|s| s.name == name)
        .unwrap_or_else(|| panic!("Stage '{name}' not found in pipeline response"));
    assert_eq!(
        std::mem::discriminant(&stage.status),
        std::mem::discriminant(expected),
        "Stage '{}': expected {:?}, got {:?} (reason: {:?})",
        name,
        expected,
        stage.status,
        stage.reason,
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Test: significance gate fails → all downstream stages skipped
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn significance_gate_fails_skips_downstream() {
    let (state, _tmp) = common::test_app_state();

    // All combos have significant=false and high p-values
    let mut params1 = HashMap::new();
    params1.insert("DELTA".to_string(), serde_json::json!(0.3));

    let mut params2 = HashMap::new();
    params2.insert("DELTA".to_string(), serde_json::json!(0.4));

    let sweep = make_sweep(vec![
        make_result(1, params1, 0.8, Some(false), Some(0.42)),
        make_result(2, params2, 0.5, Some(false), Some(0.78)),
    ]);

    let base_params = HashMap::new(); // base params irrelevant — gate fails before WF

    let result = optopsy_mcp::tools::pipeline::run_pipeline(
        &state.server,
        "test_strategy",
        "SPY",
        100_000.0,
        "sharpe",
        "sweep-001".to_string(),
        vec!["run-1".to_string(), "run-2".to_string()],
        sweep,
        base_params,
    )
    .await;

    let response = result.expect("Pipeline should not error even when gate fails");

    // Verify stages
    assert_eq!(response.stages.len(), 5, "Should have 5 stages total");
    assert_stage(&response, "sweep", &StageStatus::Completed);
    assert_stage(&response, "significance_gate", &StageStatus::Failed);
    assert_stage(&response, "walk_forward", &StageStatus::Skipped);
    assert_stage(&response, "oos_data_gate", &StageStatus::Skipped);
    assert_stage(&response, "monte_carlo", &StageStatus::Skipped);

    // No walk-forward or monte carlo results
    assert!(response.walk_forward.is_none());
    assert!(response.monte_carlo.is_none());

    // Key findings should mention the failure
    assert!(
        response
            .key_findings
            .iter()
            .any(|f| f.contains("stopped") || f.contains("significance")),
        "Key findings should mention significance gate failure: {:?}",
        response.key_findings,
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Test: no permutation test → top combos pass significance gate
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn no_permutation_passes_significance_gate() {
    let (state, _tmp) = common::test_app_state();

    // No permutation test (significant=None, p_value=None) — gate should pass
    let mut params1 = HashMap::new();
    params1.insert("DELTA".to_string(), serde_json::json!(0.3));

    let sweep = make_sweep(vec![make_result(1, params1, 1.2, None, None)]);

    let base_params = HashMap::new(); // WF will fail anyway (nonexistent strategy)

    let result = optopsy_mcp::tools::pipeline::run_pipeline(
        &state.server,
        "nonexistent_strategy", // walk-forward will fail because strategy doesn't exist
        "SPY",
        100_000.0,
        "sharpe",
        "sweep-002".to_string(),
        vec!["run-1".to_string()],
        sweep,
        base_params,
    )
    .await;

    let response = result.expect("Pipeline should not error");

    // Significance gate should pass (no permutation test → top combos accepted)
    assert_stage(&response, "significance_gate", &StageStatus::Completed);

    // Walk-forward will fail because "nonexistent_strategy" doesn't exist in cache
    // That's fine — the point is the significance gate passed
    assert_stage(&response, "walk_forward", &StageStatus::Failed);

    // Downstream should be skipped after walk-forward failure
    assert_stage(&response, "oos_data_gate", &StageStatus::Skipped);
    assert_stage(&response, "monte_carlo", &StageStatus::Skipped);
}

// ──────────────────────────────────────────────────────────────────────────────
// Test: full pipeline with NVDA fixture (significance gate pass → WF → MC)
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn full_pipeline_with_nvda_fixture() {
    let (state, _tmp, strategy_id) = common::test_app_state_with_ohlcv();

    // SweepResult.params only contains swept combo keys (not base params)
    let swept_combo = HashMap::new(); // no swept params — WF uses base_params grid

    let sweep = make_sweep(vec![make_result(1, swept_combo, 1.5, None, None)]);

    // base_params carries the original sweep request params (symbol, CAPITAL, etc.)
    let mut base_params = HashMap::new();
    base_params.insert("symbol".to_string(), serde_json::json!("NVDA"));
    base_params.insert("CAPITAL".to_string(), serde_json::json!(100_000));

    let result = optopsy_mcp::tools::pipeline::run_pipeline(
        &state.server,
        &strategy_id,
        "NVDA",
        100_000.0,
        "sharpe",
        "sweep-003".to_string(),
        vec!["run-1".to_string()],
        sweep,
        base_params,
    )
    .await;

    let response = result.expect("Pipeline should complete");

    // Sweep and significance gate always pass
    assert_stage(&response, "sweep", &StageStatus::Completed);
    assert_stage(&response, "significance_gate", &StageStatus::Completed);

    // Walk-forward should complete (NVDA data exists in cache)
    assert_stage(&response, "walk_forward", &StageStatus::Completed);
    assert!(
        response.walk_forward.is_some(),
        "Walk-forward result should be present"
    );

    // OOS data gate — depends on how many equity points WF produces
    let oos_gate = response
        .stages
        .iter()
        .find(|s| s.name == "oos_data_gate")
        .unwrap();

    match oos_gate.status {
        StageStatus::Completed => {
            // Monte Carlo should have run
            assert_stage(&response, "monte_carlo", &StageStatus::Completed);
            assert!(
                response.monte_carlo.is_some(),
                "Monte Carlo result should be present when OOS gate passes"
            );
        }
        StageStatus::Failed => {
            // Not enough OOS data — monte carlo should be skipped
            assert_stage(&response, "monte_carlo", &StageStatus::Skipped);
            assert!(
                response.monte_carlo.is_none(),
                "Monte Carlo should be None when OOS gate fails"
            );
        }
        StageStatus::Skipped => {
            panic!("OOS gate should not be skipped when walk-forward completed");
        }
    }

    // Structural assertions
    assert_eq!(response.sweep_id, "sweep-003");
    assert!(!response.key_findings.is_empty());
    assert!(response.total_duration_ms > 0);
}

// ──────────────────────────────────────────────────────────────────────────────
// Test: pipeline response structure (sweep_id, run_ids preserved)
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_preserves_sweep_metadata() {
    let (state, _tmp) = common::test_app_state();

    let sweep = make_sweep(vec![make_result(
        1,
        HashMap::new(),
        0.5,
        Some(false),
        Some(0.9),
    )]);

    let run_ids = vec![
        "run-aaa".to_string(),
        "run-bbb".to_string(),
        "run-ccc".to_string(),
    ];

    let base_params = HashMap::new(); // base params irrelevant — gate fails before WF

    let result = optopsy_mcp::tools::pipeline::run_pipeline(
        &state.server,
        "test",
        "SPY",
        50_000.0,
        "sharpe",
        "sweep-meta-test".to_string(),
        run_ids.clone(),
        sweep,
        base_params,
    )
    .await
    .expect("Pipeline should not error");

    // Sweep metadata should be passed through
    assert_eq!(result.sweep_id, "sweep-meta-test");
    assert_eq!(result.run_ids, run_ids);
    assert_eq!(result.sweep.mode, "grid");
    assert_eq!(result.sweep.objective, "sharpe");

    // Summary should be non-empty
    assert!(!result.summary.is_empty());
}
