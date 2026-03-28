//! Integration tests for the `StrategyStore` CRUD lifecycle.
//!
//! Exercises: create → get → update → list → `get_source` → delete.

use std::collections::HashMap;

use optopsy_mcp::data::database::Database;
use optopsy_mcp::data::strategy_store::StrategyRow;
use optopsy_mcp::scripting::engine::{validate_script, DiagnosticLevel};
fn sample_row(id: &str, name: &str) -> StrategyRow {
    StrategyRow {
        id: id.to_string(),
        name: name.to_string(),
        description: Some("Test strategy description".to_string()),
        category: Some("stock".to_string()),
        hypothesis: Some("Mean reversion works".to_string()),
        tags: Some(vec!["test".to_string(), "integration".to_string()]),
        regime: Some(vec!["bull".to_string()]),
        source: "fn config() { #{ name: \"test\" } }".to_string(),
        created_at: String::new(),
        updated_at: String::new(),
    }
}

#[test]
fn strategy_crud_lifecycle() {
    let db = Database::open_in_memory().expect("open_in_memory");
    let store = db.strategies();

    // 1. Create
    let row = sample_row("crud_test", "CRUD Test Strategy");
    store.upsert(&row).unwrap();

    // 2. Get
    let fetched = store
        .get("crud_test")
        .unwrap()
        .expect("should exist after create");
    assert_eq!(fetched.id, "crud_test");
    assert_eq!(fetched.name, "CRUD Test Strategy");
    assert_eq!(
        fetched.description.as_deref(),
        Some("Test strategy description")
    );
    assert_eq!(fetched.category.as_deref(), Some("stock"));
    assert_eq!(fetched.hypothesis.as_deref(), Some("Mean reversion works"));
    assert_eq!(
        fetched.tags,
        Some(vec!["test".to_string(), "integration".to_string()])
    );
    assert_eq!(fetched.regime, Some(vec!["bull".to_string()]));
    assert!(!fetched.created_at.is_empty());
    assert!(!fetched.updated_at.is_empty());

    // 3. Update
    let mut updated_row = row.clone();
    updated_row.name = "Updated Strategy".to_string();
    updated_row.source = "fn config() { #{ name: \"updated\" } }".to_string();
    updated_row.tags = Some(vec!["updated".to_string()]);
    store.upsert(&updated_row).unwrap();

    let updated = store
        .get("crud_test")
        .unwrap()
        .expect("should exist after update");
    assert_eq!(updated.name, "Updated Strategy");
    assert!(updated.source.contains("updated"));
    assert_eq!(updated.tags, Some(vec!["updated".to_string()]));

    // 4. List
    store
        .upsert(&sample_row("crud_test_2", "Another Strategy"))
        .unwrap();
    let list = store.list().unwrap();
    assert_eq!(list.len(), 2);

    // 5. Get source
    let source = store
        .get_source("crud_test")
        .unwrap()
        .expect("should have source");
    assert!(source.contains("updated"));

    // 6. Get source for non-existent
    assert!(store.get_source("nonexistent").unwrap().is_none());

    // 7. Delete
    assert!(store.delete("crud_test").unwrap());
    assert!(store.get("crud_test").unwrap().is_none());
    assert!(!store.delete("crud_test").unwrap()); // already deleted

    // 8. Count after deletion
    let remaining = store.list().unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id, "crud_test_2");
}

#[test]
fn strategy_list_scripts_includes_metadata() {
    let db = Database::open_in_memory().expect("open_in_memory");
    let store = db.strategies();

    store
        .upsert(&sample_row("script_meta_test", "Script Meta Test"))
        .unwrap();
    let scripts = store.list_scripts().unwrap();
    assert_eq!(scripts.len(), 1);
    assert_eq!(scripts[0].id, "script_meta_test");
    assert_eq!(scripts[0].name, "Script Meta Test");
}

// ──────────────────────────────────────────────────────────────────────────────
// Validation tests
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn validate_valid_stock_script() {
    let source = r#"
//! name: Test Strategy
//! category: stock

fn config() {
    #{
        symbol: "SPY",
        capital: 100000.0,
        data: #{
            indicators: ["sma:20", "rsi:14"]
        }
    }
}

fn on_bar(ctx) {
    if ctx.rsi(14) < 30.0 {
        buy_stock(100)
    } else {
        hold_position()
    }
}
"#;
    let result = validate_script(source, &HashMap::new());
    assert!(
        result.valid,
        "Expected valid, got: {:?}",
        result.diagnostics
    );
    assert!(result.callbacks.contains(&"config".to_string()));
    assert!(result.callbacks.contains(&"on_bar".to_string()));
    let cfg = result.config.unwrap();
    assert_eq!(cfg.symbol, "SPY");
    assert!((cfg.capital - 100_000.0).abs() < f64::EPSILON);
    assert_eq!(cfg.indicators, vec!["sma:20", "rsi:14"]);
}

#[test]
fn validate_syntax_error() {
    let source = "fn config() { oops this is not valid rhai }}}";
    let result = validate_script(source, &HashMap::new());
    assert!(!result.valid);
    assert!(result
        .diagnostics
        .iter()
        .any(|d| matches!(d.level, DiagnosticLevel::Error) && d.message.contains("Compile error")));
}

#[test]
fn validate_missing_callbacks() {
    let source = r#"
fn config() {
    #{
        symbol: "SPY",
        capital: 50000.0
    }
}
"#;
    let result = validate_script(source, &HashMap::new());
    // config is valid but on_bar is missing
    assert!(!result.valid);
    assert!(result
        .diagnostics
        .iter()
        .any(|d| d.message.contains("on_bar")));
}

#[test]
fn validate_bad_indicator() {
    let source = r#"
fn config() {
    #{
        symbol: "SPY",
        capital: 100000.0,
        data: #{
            indicators: ["sma:20", "bogus_indicator:14"]
        }
    }
}

fn on_bar(ctx) {
    hold_position()
}
"#;
    let result = validate_script(source, &HashMap::new());
    assert!(!result.valid);
    assert!(result
        .diagnostics
        .iter()
        .any(|d| d.message.contains("bogus_indicator")));
}

#[test]
fn validate_missing_config_field() {
    let source = r"
fn config() {
    #{
        capital: 100000.0
    }
}

fn on_bar(ctx) {
    hold_position()
}
";
    let result = validate_script(source, &HashMap::new());
    assert!(!result.valid);
    assert!(result
        .diagnostics
        .iter()
        .any(|d| matches!(d.level, DiagnosticLevel::Error) && d.message.contains("config")));
}

#[test]
fn validate_extracts_extern_params() {
    let source = r#"
let PERIOD = extern("PERIOD", 20, "SMA period");
let THRESHOLD = extern("THRESHOLD", 0.5, "Entry threshold");

fn config() {
    #{
        symbol: "SPY",
        capital: 100000.0
    }
}

fn on_bar(ctx) {
    hold_position()
}
"#;
    let result = validate_script(source, &HashMap::new());
    assert!(result.valid);
    assert_eq!(result.params.len(), 2);
    assert_eq!(result.params[0].name, "PERIOD");
    assert_eq!(result.params[1].name, "THRESHOLD");
}
