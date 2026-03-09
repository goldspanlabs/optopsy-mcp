//! Edge-case tests: empty DataFrames, NaN/Infinity in metrics,
//! zero-width DTE/delta ranges, and concurrent loading races.

mod common;

use chrono::NaiveDate;
use polars::prelude::*;

// ── Empty DataFrame tests ────────────────────────────────────────────────────

#[test]
fn filter_dte_range_on_empty_df() {
    let df = df! {
        "dte" => Vec::<i32>::new(),
        "value" => Vec::<i32>::new(),
    }
    .unwrap();
    let result = optopsy_mcp::engine::filters::filter_dte_range(&df, 60, 30).unwrap();
    assert_eq!(result.height(), 0);
}

#[test]
fn filter_valid_quotes_on_empty_df() {
    let df = df! {
        "bid" => Vec::<f64>::new(),
        "ask" => Vec::<f64>::new(),
    }
    .unwrap();
    let result = optopsy_mcp::engine::filters::filter_valid_quotes(&df, 0.05).unwrap();
    assert_eq!(result.height(), 0);
}

#[test]
fn filter_option_type_on_empty_df() {
    let df = df! {
        "option_type" => Vec::<&str>::new(),
    }
    .unwrap();
    let result = optopsy_mcp::engine::filters::filter_option_type(&df, "call").unwrap();
    assert_eq!(result.height(), 0);
}

#[test]
fn select_closest_delta_on_empty_df() {
    let dt_col: Vec<chrono::NaiveDateTime> = vec![];
    let exp_col: Vec<chrono::NaiveDateTime> = vec![];
    let df = df! {
        "quote_datetime" => &dt_col,
        "expiration" => &exp_col,
        "delta" => Vec::<f64>::new(),
        "strike" => Vec::<f64>::new(),
        "bid" => Vec::<f64>::new(),
        "ask" => Vec::<f64>::new(),
    }
    .unwrap();
    let target = optopsy_mcp::engine::types::TargetRange {
        target: 0.30,
        min: 0.20,
        max: 0.40,
    };
    let result = optopsy_mcp::engine::filters::select_closest_delta(&df, &target).unwrap();
    assert_eq!(result.height(), 0);
}

// ── Metrics edge cases: NaN/Infinity handling ────────────────────────────────

#[test]
fn metrics_with_no_trades_returns_defaults() {
    let result = optopsy_mcp::engine::metrics::calculate_metrics(&[], &[], 10_000.0).unwrap();
    assert!((result.sharpe - 0.0).abs() < f64::EPSILON);
    assert!((result.win_rate - 0.0).abs() < f64::EPSILON);
    assert!((result.profit_factor - 0.0).abs() < f64::EPSILON);
}

#[test]
fn metrics_with_zero_capital_returns_defaults() {
    let result = optopsy_mcp::engine::metrics::calculate_metrics(&[], &[], 0.0).unwrap();
    assert!((result.sharpe - 0.0).abs() < f64::EPSILON);
    assert!((result.cagr - 0.0).abs() < f64::EPSILON);
}

#[test]
fn metrics_with_negative_capital_returns_defaults() {
    let result = optopsy_mcp::engine::metrics::calculate_metrics(&[], &[], -100.0).unwrap();
    assert!((result.max_drawdown - 0.0).abs() < f64::EPSILON);
}

#[test]
fn metrics_all_fields_are_finite() {
    // Single winning trade
    let entry_dt = NaiveDate::from_ymd_opt(2024, 1, 15)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let exit_dt = NaiveDate::from_ymd_opt(2024, 2, 15)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let trade = optopsy_mcp::engine::types::TradeRecord::new(
        1,
        entry_dt,
        exit_dt,
        -100.0, // debit
        150.0,  // credit
        50.0,   // profit
        31,
        optopsy_mcp::engine::types::ExitType::DteExit,
        vec![],
    );
    let equity = vec![
        optopsy_mcp::engine::types::EquityPoint {
            datetime: entry_dt,
            equity: 10_000.0,
        },
        optopsy_mcp::engine::types::EquityPoint {
            datetime: exit_dt,
            equity: 10_050.0,
        },
    ];
    let m =
        optopsy_mcp::engine::metrics::calculate_metrics(&equity, &[trade], 10_000.0).unwrap();
    assert!(m.sharpe.is_finite(), "sharpe must be finite");
    assert!(m.sortino.is_finite(), "sortino must be finite");
    assert!(m.cagr.is_finite(), "cagr must be finite");
    assert!(m.calmar.is_finite(), "calmar must be finite");
    assert!(m.var_95.is_finite(), "var_95 must be finite");
    assert!(m.profit_factor.is_finite(), "profit_factor must be finite");
    assert!(m.expectancy.is_finite(), "expectancy must be finite");
}

// ── Zero-width DTE/delta ranges ──────────────────────────────────────────────

#[test]
fn zero_width_dte_range_returns_exact_match() {
    let df = df! {
        "dte" => &[29i32, 30, 31],
        "value" => &[1, 2, 3],
    }
    .unwrap();
    let result = optopsy_mcp::engine::filters::filter_dte_range(&df, 30, 30).unwrap();
    assert_eq!(result.height(), 1);
}

#[test]
fn zero_width_delta_range() {
    use garde::Validate;
    let tr = optopsy_mcp::engine::types::TargetRange {
        target: 0.30,
        min: 0.30,
        max: 0.30,
    };
    // Should be valid — min == max is a point, not inverted
    assert!(tr.validate().is_ok());
}

#[test]
fn zero_width_dte_range_validation() {
    use garde::Validate;
    let dte = optopsy_mcp::engine::types::DteRange {
        target: 45,
        min: 45,
        max: 45,
    };
    assert!(dte.validate().is_ok());
}

// ── Concurrent loading: verify Arc<RwLock> data map is safe ──────────────────

#[tokio::test]
async fn concurrent_read_access_does_not_panic() {
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    let data: Arc<RwLock<HashMap<String, DataFrame>>> =
        Arc::new(RwLock::new(HashMap::new()));

    // Insert a small DataFrame
    {
        let mut w = data.write().await;
        let df = df! { "x" => &[1i32, 2, 3] }.unwrap();
        w.insert("TEST".to_string(), df);
    }

    // Spawn many concurrent readers
    let mut handles = Vec::new();
    for _ in 0..50 {
        let data_clone = Arc::clone(&data);
        handles.push(tokio::spawn(async move {
            let r = data_clone.read().await;
            assert!(r.contains_key("TEST"));
            assert_eq!(r["TEST"].height(), 3);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn concurrent_write_then_read_consistent() {
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    let data: Arc<RwLock<HashMap<String, DataFrame>>> =
        Arc::new(RwLock::new(HashMap::new()));

    // Writer task
    let data_w = Arc::clone(&data);
    let writer = tokio::spawn(async move {
        for i in 0..10 {
            let mut w = data_w.write().await;
            let df = df! { "val" => &[i as i32] }.unwrap();
            w.insert(format!("SYM{i}"), df);
        }
    });

    writer.await.unwrap();

    let r = data.read().await;
    assert_eq!(r.len(), 10);
}
