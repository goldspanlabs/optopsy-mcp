//! Integration tests for the `BacktestStore` REST API storage layer.
//!
//! Exercises the full lifecycle: insert, list, get, `get_trades`, and delete.

use optopsy_mcp::data::backtest_store::{BacktestStore, MetricsRow, TradeRow};
use serde_json::Value;

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

fn sample_metrics() -> MetricsRow {
    MetricsRow {
        sharpe: 1.5,
        sortino: 2.1,
        cagr: 0.18,
        max_drawdown: -0.10,
        win_rate: 0.60,
        profit_factor: 1.9,
        total_pnl: 8_000.0,
        trade_count: 30,
        expectancy: 266.67,
        var_95: -420.0,
    }
}

fn sample_trades() -> Vec<TradeRow> {
    vec![
        TradeRow {
            trade_id: 1,
            entry_datetime: "2024-01-02T09:30:00Z".to_string(),
            exit_datetime: "2024-01-12T16:00:00Z".to_string(),
            entry_cost: 1_000.0,
            exit_proceeds: 1_300.0,
            pnl: 300.0,
            pnl_pct: 0.30,
            days_held: 10,
            exit_type: "TakeProfit".to_string(),
            legs: "[]".to_string(),
            computed_quantity: Some(2),
            entry_equity: Some(100_000.0),
            group_label: Some("Cycle 1".to_string()),
        },
        TradeRow {
            trade_id: 2,
            entry_datetime: "2024-02-05T09:30:00Z".to_string(),
            exit_datetime: "2024-02-20T16:00:00Z".to_string(),
            entry_cost: 800.0,
            exit_proceeds: 650.0,
            pnl: -150.0,
            pnl_pct: -0.1875,
            days_held: 15,
            exit_type: "StopLoss".to_string(),
            legs: "[]".to_string(),
            computed_quantity: None,
            entry_equity: Some(100_300.0),
            group_label: None,
        },
        TradeRow {
            trade_id: 3,
            entry_datetime: "2024-03-01T09:30:00Z".to_string(),
            exit_datetime: "2024-03-31T16:00:00Z".to_string(),
            entry_cost: 950.0,
            exit_proceeds: 1_100.0,
            pnl: 150.0,
            pnl_pct: 0.1579,
            days_held: 30,
            exit_type: "MaxHold".to_string(),
            legs: "[]".to_string(),
            computed_quantity: Some(1),
            entry_equity: Some(100_150.0),
            group_label: None,
        },
    ]
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 1: full lifecycle
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn full_lifecycle_insert_list_get_delete() {
    let store = BacktestStore::open_in_memory().expect("open_in_memory");

    let metrics = sample_metrics();
    let trades3 = sample_trades(); // 3 trades
    let trades1 = vec![sample_trades().remove(0)]; // 1 trade

    let ec: Value = serde_json::from_str(r#"[{"datetime":"2024-01-02","equity":100000}]"#)
        .expect("parse equity_curve");
    let ind: Value = serde_json::json!({});
    let params1: Value = serde_json::json!({"dte": 45, "delta": 0.30});
    let params2: Value = serde_json::json!({"dte": 30, "delta": 0.25});

    // ── insert two backtests ──────────────────────────────────────────────────

    let id1 = store
        .insert(
            "bb_mean_reversion",
            "SPY",
            100_000.0,
            &params1,
            &metrics,
            &trades3,
            &ec,
            &ind,
            512,
        )
        .expect("insert id1");

    let id2 = store
        .insert(
            "ibs_mean_reversion",
            "QQQ",
            50_000.0,
            &params2,
            &metrics,
            &trades1,
            &ec,
            &ind,
            256,
        )
        .expect("insert id2");

    assert_ne!(id1, id2, "UUIDs must be distinct");

    // ── list all → 2 results ──────────────────────────────────────────────────

    let all = store.list(None, None).expect("list all");
    assert_eq!(all.len(), 2, "expected 2 backtests after two inserts");

    // ── list filtered by strategy → 1 result ─────────────────────────────────

    let filtered = store
        .list(Some("bb_mean_reversion"), None)
        .expect("list by strategy");
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].strategy_key, "bb_mean_reversion");
    assert_eq!(filtered[0].symbol, "SPY");

    // ── get full result for id1 ───────────────────────────────────────────────

    let row = store.get(&id1).expect("get id1").expect("id1 should exist");

    assert_eq!(row.id, id1);
    assert_eq!(row.strategy_key, "bb_mean_reversion");
    assert_eq!(row.symbol, "SPY");
    assert_eq!(row.trades.len(), 3, "id1 should have 3 trades");
    assert!(
        (row.metrics.sharpe - 1.5).abs() < f64::EPSILON,
        "sharpe mismatch"
    );

    // ── verify equity_curve is a JSON array ───────────────────────────────────

    assert!(
        row.equity_curve.is_array(),
        "equity_curve should be a JSON array"
    );
    let arr = row.equity_curve.as_array().unwrap();
    assert!(!arr.is_empty(), "equity_curve array must not be empty");

    // ── get_trades for id1 — check exit_type and group_label ─────────────────

    let fetched_trades = store.get_trades(&id1).expect("get_trades id1");
    assert_eq!(fetched_trades.len(), 3, "expected 3 trade rows for id1");

    // Ordered by trade_id ASC
    assert_eq!(fetched_trades[0].exit_type, "TakeProfit");
    assert_eq!(
        fetched_trades[0].group_label,
        Some("Cycle 1".to_string()),
        "first trade should carry group_label 'Cycle 1'"
    );
    assert_eq!(fetched_trades[1].exit_type, "StopLoss");
    assert!(
        fetched_trades[1].group_label.is_none(),
        "second trade should have no group_label"
    );
    assert_eq!(fetched_trades[2].exit_type, "MaxHold");

    // ── delete id1 ───────────────────────────────────────────────────────────

    let deleted = store.delete(&id1).expect("delete id1");
    assert!(deleted, "delete should return true for existing id");

    // get returns None after deletion
    assert!(
        store.get(&id1).expect("get after delete").is_none(),
        "id1 should not exist after deletion"
    );

    // get_trades returns empty after deletion (CASCADE)
    let orphan_trades = store.get_trades(&id1).expect("get_trades after delete");
    assert!(
        orphan_trades.is_empty(),
        "trades should be cascade-deleted with their backtest"
    );

    // second delete returns false
    let second_delete = store.delete(&id1).expect("second delete");
    assert!(!second_delete, "second delete should return false");

    // ── id2 still intact ─────────────────────────────────────────────────────

    let row2 = store.get(&id2).expect("get id2").expect("id2 should still exist");
    assert_eq!(row2.strategy_key, "ibs_mean_reversion");
    assert_eq!(row2.symbol, "QQQ");
    assert_eq!(row2.trades.len(), 1, "id2 should still have its 1 trade");

    let remaining = store.list(None, None).expect("list after delete");
    assert_eq!(remaining.len(), 1, "only id2 should remain");
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 2: list filter combinations
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn list_filters_combine_correctly() {
    let store = BacktestStore::open_in_memory().expect("open_in_memory");

    let metrics = sample_metrics();
    let empty: Vec<TradeRow> = vec![];
    let ec: Value = serde_json::from_str(r#"[{"datetime":"2024-01-02","equity":100000}]"#)
        .expect("parse equity_curve");
    let ind: Value = serde_json::json!({});
    let p: Value = serde_json::json!({});

    // Insert 3 backtests: (strat_a, SPY), (strat_a, QQQ), (strat_b, SPY)
    store
        .insert("strat_a", "SPY", 10_000.0, &p, &metrics, &empty, &ec, &ind, 100)
        .expect("insert strat_a/SPY");
    store
        .insert("strat_a", "QQQ", 10_000.0, &p, &metrics, &empty, &ec, &ind, 100)
        .expect("insert strat_a/QQQ");
    store
        .insert("strat_b", "SPY", 10_000.0, &p, &metrics, &empty, &ec, &ind, 100)
        .expect("insert strat_b/SPY");

    // list(strat_a, SPY) = 1
    assert_eq!(
        store.list(Some("strat_a"), Some("SPY")).unwrap().len(),
        1,
        "strat_a + SPY should yield exactly 1"
    );

    // list(strat_a, None) = 2
    assert_eq!(
        store.list(Some("strat_a"), None).unwrap().len(),
        2,
        "strat_a alone should yield 2"
    );

    // list(None, SPY) = 2
    assert_eq!(
        store.list(None, Some("SPY")).unwrap().len(),
        2,
        "SPY alone should yield 2"
    );

    // list(None, None) = 3
    assert_eq!(
        store.list(None, None).unwrap().len(),
        3,
        "no filters should yield all 3"
    );
}
