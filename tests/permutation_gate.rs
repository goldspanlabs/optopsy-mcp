//! Integration tests for the permutation test pipeline.
//!
//! Constructs synthetic `SweepResponse`s with known trade P&L distributions and
//! verifies that `apply_permutation_gate` correctly populates p-values,
//! significance flags, and multiple comparisons corrections.
//!
//! No real market data required — uses synthetic trade logs with controlled
//! statistical properties.

use std::collections::HashMap;

use chrono::NaiveDateTime;

use optopsy_mcp::engine::permutation::apply_permutation_gate;
use optopsy_mcp::engine::types::{
    BacktestQualityStats, BacktestResult, CashflowLabel, EquityPoint, ExitType, LegDetail,
    OptionType, PerformanceMetrics, Side, TradeRecord,
};
use optopsy_mcp::scripting::engine::ScriptBacktestResult;
use optopsy_mcp::scripting::types::CustomSeriesStore;
use optopsy_mcp::tools::response_types::sweep::{SweepResponse, SweepResult};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn dt(days: i64) -> NaiveDateTime {
    NaiveDateTime::parse_from_str("2024-01-01 16:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
        + chrono::Duration::days(days)
}

/// Build a minimal `TradeRecord` with the given P&L.
fn trade(id: usize, pnl: f64) -> TradeRecord {
    TradeRecord {
        trade_id: id,
        entry_datetime: dt(0),
        exit_datetime: dt(30),
        entry_cost: -100.0,
        exit_proceeds: 100.0 + pnl,
        entry_amount: 100.0,
        entry_label: CashflowLabel::DR,
        exit_amount: (100.0 + pnl).abs(),
        exit_label: if pnl >= 0.0 {
            CashflowLabel::CR
        } else {
            CashflowLabel::DR
        },
        pnl,
        days_held: 30,
        exit_type: ExitType::Expiration,
        legs: vec![LegDetail {
            side: Side::Short,
            option_type: OptionType::Put,
            strike: 400.0,
            expiration: "2024-02-01".into(),
            entry_price: 5.0,
            exit_price: Some(0.0),
            qty: 1,
            entry_delta: Some(-0.30),
            is_stock: false,
        }],
        computed_quantity: None,
        entry_equity: None,
        stock_entry_price: None,
        stock_exit_price: None,
        stock_pnl: None,
        group: None,
    }
}

/// Build a `ScriptBacktestResult` from a list of P&Ls.
fn backtest_from_pnls(pnls: &[f64]) -> ScriptBacktestResult {
    let trade_log: Vec<TradeRecord> = pnls.iter().enumerate().map(|(i, &p)| trade(i, p)).collect();
    ScriptBacktestResult {
        result: BacktestResult {
            symbol: Some("SPY".into()),
            trade_count: trade_log.len(),
            total_pnl: pnls.iter().sum(),
            metrics: PerformanceMetrics::default(),
            equity_curve: vec![EquityPoint {
                datetime: dt(0),
                equity: 100_000.0,
                unrealized: None,
            }],
            trade_log,
            quality: BacktestQualityStats::default(),
            warnings: vec![],
        },
        metadata: None,
        execution_time_ms: 10,
        indicator_data: HashMap::new(),
        custom_series: CustomSeriesStore {
            series: HashMap::new(),
            display_types: HashMap::new(),
            num_bars: 0,
        },
        precomputed_options: None,
    }
}

/// Build a `SweepResult` stub for rank `r`.
fn sweep_result(rank: usize) -> SweepResult {
    SweepResult {
        rank,
        params: HashMap::from([("DTE".into(), serde_json::json!(rank * 10))]),
        sharpe: 0.0,
        sortino: 0.0,
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

/// Build a `SweepResponse` with `n` combos, each getting its own P&L series.
fn build_response(combo_pnls: &[Vec<f64>]) -> SweepResponse {
    let n = combo_pnls.len();
    SweepResponse {
        mode: "grid".into(),
        objective: "sharpe".into(),
        combinations_total: n,
        combinations_run: n,
        combinations_failed: 0,
        best_result: None,
        ranked_results: (0..n).map(|i| sweep_result(i + 1)).collect(),
        dimension_sensitivity: HashMap::new(),
        convergence_trace: None,
        execution_time_ms: 100,
        multiple_comparisons: None,
        full_results: combo_pnls.iter().map(|p| backtest_from_pnls(p)).collect(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Strong-edge combo should get a low p-value and be marked significant.
#[test]
fn strong_edge_combo_is_significant() {
    // 20 trades, all solidly positive — clear directional edge
    let strong_pnls: Vec<f64> = (0..20).map(|i| 100.0 + f64::from(i) * 5.0).collect();
    let response = build_response(&[strong_pnls]);

    let result = apply_permutation_gate(response, 2_000, "sharpe", Some(42));

    let r = &result.ranked_results[0];
    assert!(r.p_value.is_some(), "p_value should be populated");
    let p = r.p_value.unwrap();
    assert!(p < 0.05, "strong edge should have p < 0.05, got {p}");
    assert_eq!(r.significant, Some(true), "should be marked significant");

    // Multiple comparisons should be present
    let mc = result.multiple_comparisons.as_ref().unwrap();
    assert_eq!(mc.len(), 2, "should have BH-FDR and Bonferroni");
    assert_eq!(mc[0].method, "benjamini_hochberg");
    assert_eq!(mc[1].method, "bonferroni");
}

/// No-edge combo should get a high p-value and NOT be marked significant.
#[test]
fn no_edge_combo_not_significant() {
    // Symmetric P&Ls: no directional edge
    let symmetric: Vec<f64> = (0..20)
        .map(|i| if i % 2 == 0 { 100.0 } else { -100.0 })
        .collect();
    let response = build_response(&[symmetric]);

    let result = apply_permutation_gate(response, 2_000, "sharpe", Some(42));

    let r = &result.ranked_results[0];
    let p = r.p_value.unwrap();
    assert!(
        p > 0.20,
        "symmetric P&Ls (no edge) should have high p-value, got {p}"
    );
    assert_eq!(
        r.significant,
        Some(false),
        "no-edge should not be significant"
    );
}

/// Mixed sweep: strong-edge and no-edge combos coexist, BH-FDR correctly
/// marks only the strong ones as significant.
#[test]
fn mixed_combos_bh_fdr_discriminates() {
    let strong: Vec<f64> = (0..20).map(|i| 100.0 + f64::from(i) * 5.0).collect();
    let weak: Vec<f64> = (0..20)
        .map(|i| if i % 2 == 0 { 50.0 } else { -50.0 })
        .collect();
    let noise: Vec<f64> = (0..20)
        .map(|i| if i % 2 == 0 { 10.0 } else { -15.0 })
        .collect();

    let response = build_response(&[strong, weak, noise]);
    let result = apply_permutation_gate(response, 2_000, "sharpe", Some(123));

    // All three should have p-values
    for (i, r) in result.ranked_results.iter().enumerate() {
        assert!(
            r.p_value.is_some(),
            "combo {i} should have a p-value populated"
        );
    }

    // The strong combo (index 0) should be significant
    assert_eq!(
        result.ranked_results[0].significant,
        Some(true),
        "strong combo should survive BH-FDR"
    );

    // The weak/noise combos should NOT be significant
    assert_eq!(
        result.ranked_results[1].significant,
        Some(false),
        "weak combo should not survive BH-FDR"
    );
    assert_eq!(
        result.ranked_results[2].significant,
        Some(false),
        "noise combo should not survive BH-FDR"
    );

    // Multiple comparisons attached with correct counts
    let mc = result.multiple_comparisons.as_ref().unwrap();
    let bh = &mc[0];
    assert_eq!(bh.num_tests, 3);
    assert!(
        bh.num_significant >= 1,
        "at least the strong combo survives"
    );

    let bonf = &mc[1];
    assert_eq!(bonf.num_tests, 3);
    // Bonferroni is stricter — should retain <= BH count
    assert!(bonf.num_significant <= bh.num_significant);
}

/// Combos with too few trades (<10) get `p_value = None` and are excluded
/// from multiple comparisons.
#[test]
fn insufficient_trades_excluded() {
    let enough: Vec<f64> = (0..15).map(|i| 80.0 + f64::from(i) * 3.0).collect();
    let too_few: Vec<f64> = vec![100.0, 200.0, -50.0]; // only 3 trades

    let response = build_response(&[enough, too_few]);
    let result = apply_permutation_gate(response, 1_000, "sharpe", Some(99));

    // First combo: sufficient trades -> has p-value
    assert!(result.ranked_results[0].p_value.is_some());

    // Second combo: too few trades -> no p-value, no significance
    assert!(
        result.ranked_results[1].p_value.is_none(),
        "< MIN_TRADES combo should have None p-value"
    );
    assert!(
        result.ranked_results[1].significant.is_none(),
        "< MIN_TRADES combo should have None significance"
    );

    // Multiple comparisons should only include 1 tested combo
    let mc = result.multiple_comparisons.as_ref().unwrap();
    assert_eq!(mc[0].num_tests, 1);
}

/// Deterministic: same seed produces identical p-values across runs.
#[test]
fn deterministic_with_seed() {
    let pnls: Vec<f64> = (0..20)
        .map(|i| if i % 3 == 0 { 150.0 } else { -40.0 })
        .collect();
    let response1 = build_response(std::slice::from_ref(&pnls));
    let response2 = build_response(std::slice::from_ref(&pnls));

    let r1 = apply_permutation_gate(response1, 2_000, "sharpe", Some(777));
    let r2 = apply_permutation_gate(response2, 2_000, "sharpe", Some(777));

    let p1 = r1.ranked_results[0].p_value.unwrap();
    let p2 = r2.ranked_results[0].p_value.unwrap();
    assert!(
        (p1 - p2).abs() < 1e-10,
        "same seed should produce identical p-values: {p1} vs {p2}"
    );
}

/// All four objectives produce valid p-values.
#[test]
fn all_objectives_produce_valid_pvalues() {
    let pnls: Vec<f64> = (0..20).map(|i| 100.0 + f64::from(i) * 10.0).collect();

    for objective in &["sharpe", "sortino", "calmar", "profit_factor"] {
        let mut response = build_response(std::slice::from_ref(&pnls));
        response.objective = objective.to_string();

        let result = apply_permutation_gate(response, 1_000, objective, Some(42));

        let p = result.ranked_results[0].p_value.unwrap();
        assert!(
            (0.0..=1.0).contains(&p),
            "{objective}: p-value {p} out of [0, 1] range"
        );
        // All-positive P&Ls should be significant for any metric
        assert!(
            p < 0.05,
            "{objective}: strong edge should have p < 0.05, got {p}"
        );
    }
}

/// `best_result` is updated to reflect permutation fields after gate application.
#[test]
fn best_result_reflects_permutation_fields() {
    let pnls: Vec<f64> = (0..20).map(|i| 100.0 + f64::from(i) * 5.0).collect();
    let response = build_response(&[pnls]);

    let result = apply_permutation_gate(response, 1_000, "sharpe", Some(42));

    let best = result
        .best_result
        .as_ref()
        .expect("best_result should exist");
    assert!(
        best.p_value.is_some(),
        "best_result should have p_value from ranked_results[0]"
    );
    assert!(
        best.significant.is_some(),
        "best_result should have significance flag"
    );
}
