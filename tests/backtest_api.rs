//! Integration tests for the [`RunStore`] REST API storage layer.
//!
//! Exercises the full lifecycle: `insert_run`, `insert_trades`, `list`, `get_run`, and `delete_run`.

use optopsy_mcp::data::database::Database;
use optopsy_mcp::data::traits::{RunStore, TradeRow};

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

fn sample_trades() -> Vec<TradeRow> {
    vec![
        TradeRow {
            trade_id: 1,
            entry_datetime: 1_704_186_600, // 2024-01-02 09:30
            exit_datetime: 1_705_075_200,  // 2024-01-12 16:00
            entry_cost: 1_000.0,
            exit_proceeds: 1_300.0,
            entry_amount: 1_000.0,
            entry_label: "DR".to_string(),
            exit_amount: 1_300.0,
            exit_label: "CR".to_string(),
            pnl: 300.0,
            days_held: 10,
            exit_type: "TakeProfit".to_string(),
            legs: serde_json::Value::Array(vec![]),
            computed_quantity: Some(2),
            entry_equity: Some(100_000.0),
            stock_entry_price: None,
            stock_exit_price: None,
            stock_pnl: None,
            group: Some("Cycle 1".to_string()),
        },
        TradeRow {
            trade_id: 2,
            entry_datetime: 1_707_122_600, // 2024-02-05 09:30
            exit_datetime: 1_708_444_800,  // 2024-02-20 16:00
            entry_cost: 800.0,
            exit_proceeds: 650.0,
            entry_amount: 800.0,
            entry_label: "DR".to_string(),
            exit_amount: 650.0,
            exit_label: "CR".to_string(),
            pnl: -150.0,
            days_held: 15,
            exit_type: "StopLoss".to_string(),
            legs: serde_json::Value::Array(vec![]),
            computed_quantity: None,
            entry_equity: Some(100_300.0),
            stock_entry_price: None,
            stock_exit_price: None,
            stock_pnl: None,
            group: None,
        },
        TradeRow {
            trade_id: 3,
            entry_datetime: 1_709_287_800, // 2024-03-01 09:30
            exit_datetime: 1_711_900_800,  // 2024-03-31 16:00
            entry_cost: 950.0,
            exit_proceeds: 1_100.0,
            entry_amount: 950.0,
            entry_label: "DR".to_string(),
            exit_amount: 1_100.0,
            exit_label: "CR".to_string(),
            pnl: 150.0,
            days_held: 30,
            exit_type: "MaxHold".to_string(),
            legs: serde_json::Value::Array(vec![]),
            computed_quantity: Some(1),
            entry_equity: Some(100_150.0),
            stock_entry_price: None,
            stock_exit_price: None,
            stock_pnl: None,
            group: None,
        },
    ]
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 1: full lifecycle
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn full_lifecycle_insert_list_get_delete() {
    let store = Database::open_in_memory().expect("open_in_memory").runs();

    let trades3 = sample_trades(); // 3 trades

    let params1 = serde_json::json!({"dte": 45, "delta": 0.30});
    let params2 = serde_json::json!({"dte": 30, "delta": 0.25});

    let id1 = uuid::Uuid::new_v4().to_string();
    let id2 = uuid::Uuid::new_v4().to_string();

    // ── insert two runs ──────────────────────────────────────────────────

    store
        .insert_run(
            &id1,
            None,
            None,
            "SPY",
            100_000.0,
            &params1,
            Some(8_000.0),
            Some(0.60),
            Some(-0.10),
            Some(1.5),
            Some(2.1),
            Some(0.18),
            Some(1.9),
            Some(30),
            Some(266.67),
            Some(-420.0),
            None,
            None,
            "{}",
            Some(512),
            None,
            None,
            None,
            "manual",
            None,
        )
        .expect("insert id1");

    store
        .insert_trades(&id1, &trades3)
        .expect("insert trades for id1");

    store
        .insert_run(
            &id2,
            None,
            None,
            "QQQ",
            50_000.0,
            &params2,
            Some(4_000.0),
            Some(0.55),
            Some(-0.08),
            Some(1.2),
            Some(1.8),
            Some(0.12),
            Some(1.6),
            Some(15),
            Some(200.0),
            Some(-300.0),
            None,
            None,
            "{}",
            Some(256),
            None,
            None,
            None,
            "manual",
            None,
        )
        .expect("insert id2");

    assert_ne!(id1, id2, "UUIDs must be distinct");

    // ── list all → 2 single runs ─────────────────────────────────────────

    let response = store.list(None).expect("list all");
    assert_eq!(response.overview.total_runs, 2, "expected 2 runs");
    assert_eq!(response.rows.len(), 2, "expected 2 rows");

    // ── get_run for a nonexistent id returns None ────────────────────────

    assert!(store.get_run("nonexistent").unwrap().is_none());

    // ── get_run for id1 — check trades ──────────────────────────────────

    let detail = store.get_run(&id1).unwrap().expect("should exist");
    assert_eq!(detail.symbol, "SPY");
    assert_eq!(detail.trades.len(), 3, "expected 3 trade rows for id1");
    assert_eq!(detail.trades[0].exit_type, "TakeProfit");
    assert_eq!(
        detail.trades[0].group,
        Some("Cycle 1".to_string()),
        "first trade should carry group 'Cycle 1'"
    );
    assert_eq!(detail.trades[1].exit_type, "StopLoss");
    assert!(
        detail.trades[1].group.is_none(),
        "second trade should have no group"
    );
    assert_eq!(detail.trades[2].exit_type, "MaxHold");

    // ── delete id1 ──────────────────────────────────────────────────────

    let deleted = store.delete_run(&id1).expect("delete id1");
    assert!(deleted, "delete should return true for existing id");

    // get_run returns None after deletion
    assert!(
        store.get_run(&id1).expect("get_run after delete").is_none(),
        "id1 should not exist after deletion"
    );

    // second delete returns false
    let second_delete = store.delete_run(&id1).expect("second delete");
    assert!(!second_delete, "second delete should return false");

    // ── id2 still intact ────────────────────────────────────────────────

    let remaining = store.list(None).expect("list after delete");
    assert_eq!(remaining.overview.total_runs, 1, "only id2 should remain");
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 2: sweep lifecycle
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn sweep_insert_and_cascade_delete() {
    let store = Database::open_in_memory().expect("open_in_memory").runs();

    let sweep_id = uuid::Uuid::new_v4().to_string();
    let config = serde_json::json!({"mode": "grid", "sweep_params": []});

    store
        .insert_sweep(
            &sweep_id,
            None,
            "SPY",
            &config,
            "sharpe",
            "grid",
            3,
            Some(1000),
            "manual",
            None,
        )
        .expect("insert sweep");

    // Insert runs for the sweep
    for i in 0..3 {
        let run_id = uuid::Uuid::new_v4().to_string();
        let params = serde_json::json!({"dte": 30 + i * 15});
        store
            .insert_run(
                &run_id,
                Some(&sweep_id),
                None,
                "SPY",
                50_000.0,
                &params,
                Some(0.10 + f64::from(i) * 0.05),
                Some(0.50),
                Some(-0.10),
                Some(1.0 + f64::from(i) * 0.5),
                None,
                None,
                None,
                Some(10),
                None,
                None,
                None,
                None,
                "{}",
                Some(100),
                None,
                None,
                None,
                "manual",
                None,
            )
            .unwrap();
    }

    let detail = store.get_sweep(&sweep_id).unwrap().expect("should exist");
    assert_eq!(detail.runs.len(), 3);

    // Deleting sweep should cascade to runs
    assert!(store.delete_sweep(&sweep_id).unwrap());
    assert!(store.get_sweep(&sweep_id).unwrap().is_none());
}
