//! Integration test: verify adjustment factors work end-to-end.

use chrono::NaiveDate;
use optopsy_mcp::data::adjustment_store::SplitRow;
use optopsy_mcp::engine::adjustments::AdjustmentTimeline;

fn d(s: &str) -> NaiveDate {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
}

#[test]
fn test_aapl_adjustment_factors() {
    // Real AAPL splits
    let splits = vec![
        SplitRow {
            symbol: "AAPL".into(),
            date: d("2000-06-21"),
            ratio: 2.0,
        },
        SplitRow {
            symbol: "AAPL".into(),
            date: d("2005-02-28"),
            ratio: 2.0,
        },
        SplitRow {
            symbol: "AAPL".into(),
            date: d("2014-06-09"),
            ratio: 7.0,
        },
        SplitRow {
            symbol: "AAPL".into(),
            date: d("2020-08-31"),
            ratio: 4.0,
        },
    ];

    let tl = AdjustmentTimeline::build(&splits, &[], &[]);

    // Before all splits: factor = 1/(2*2*7*4) = 1/112
    let factor = tl.factor_at(d("2000-01-01"));
    assert!((factor - 1.0 / 112.0).abs() < 1e-10);

    // Between 2005 and 2014 splits: factor = 1/(7*4) = 1/28
    let factor = tl.factor_at(d("2010-01-01"));
    assert!((factor - 1.0 / 28.0).abs() < 1e-10);

    // After all splits: factor = 1.0
    let factor = tl.factor_at(d("2024-01-01"));
    assert!((factor - 1.0).abs() < 1e-10);
}

#[test]
fn test_reverse_split() {
    let splits = vec![
        SplitRow {
            symbol: "GE".into(),
            date: d("2021-08-02"),
            ratio: 0.125,
        }, // 1:8 reverse
    ];
    let tl = AdjustmentTimeline::build(&splits, &[], &[]);

    // Pre-reverse-split: factor = 1/0.125 = 8.0
    let factor = tl.factor_at(d("2021-07-01"));
    assert!((factor - 8.0).abs() < 1e-10);
}

#[test]
fn test_split_ratio_between_dates() {
    let splits = vec![
        SplitRow {
            symbol: "AAPL".into(),
            date: d("2014-06-09"),
            ratio: 7.0,
        },
        SplitRow {
            symbol: "AAPL".into(),
            date: d("2020-08-31"),
            ratio: 4.0,
        },
    ];

    // Position opened 2019-01-01, split occurs 2020-08-31: quantity * 4
    let ratio = AdjustmentTimeline::split_ratio_between(&splits, d("2019-01-01"), d("2020-09-01"));
    assert!((ratio - 4.0).abs() < f64::EPSILON);
}

#[test]
fn test_db_seeded_splits() {
    // Verify the V2 migration seeds real data that's queryable
    let db = optopsy_mcp::data::database::Database::open_in_memory().expect("open db");
    let store = db.adjustments();

    let aapl_splits = store.splits("AAPL").expect("query splits");
    assert_eq!(aapl_splits.len(), 4, "AAPL should have 4 splits");

    let aapl_divs = store.dividends("AAPL").expect("query dividends");
    assert!(!aapl_divs.is_empty(), "AAPL should have dividends");

    // Build a timeline from real DB data (no closes for dividend adjustment in this test)
    let tl = AdjustmentTimeline::build(&aapl_splits, &[], &[]);
    let factor = tl.factor_at(d("2000-01-01"));
    assert!(
        (factor - 1.0 / 112.0).abs() < 1e-10,
        "Pre-all-splits factor should be 1/112"
    );
}
