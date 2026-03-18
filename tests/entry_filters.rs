//! Integration tests for entry and exit filters.
//!
//! Verifies that premium, delta, stagger, and expiration filters
//! affect trade selection when run through the full backtest pipeline.

use optopsy_mcp::engine::core::run_backtest;
use optopsy_mcp::engine::types::{ExitType, ExpirationFilter};

mod common;
use common::{backtest_params, delta, make_multi_strike_df};

// ---------------------------------------------------------------------------
// Baseline helper — unfiltered short put backtest
// ---------------------------------------------------------------------------

fn baseline_trade_count() -> usize {
    let df = make_multi_strike_df();
    let params = backtest_params("short_put", vec![delta(0.20)]);
    let result = run_backtest(&df, &params).expect("baseline backtest failed");
    assert!(result.trade_count > 0, "baseline must produce trades");
    result.trade_count
}

// ---------------------------------------------------------------------------
// Premium filters
// ---------------------------------------------------------------------------

#[test]
fn min_net_premium_filters_cheap_entries() {
    let df = make_multi_strike_df();
    let mut params = backtest_params("short_put", vec![delta(0.20)]);
    // Short put at delta 0.20, strike 95: entry mid ≈ 1.25.
    // Set min_net_premium high enough to exclude some or all entries.
    params.min_net_premium = Some(50.0);

    let result = run_backtest(&df, &params).expect("backtest failed");
    assert_eq!(
        result.trade_count, 0,
        "Premium $50 min should exclude all short put entries (mid ≈ $1.25)"
    );
}

#[test]
fn max_net_premium_filters_expensive_entries() {
    let df = make_multi_strike_df();
    let mut params = backtest_params("short_put", vec![delta(0.20)]);
    // Set max_net_premium very low to exclude entries
    params.max_net_premium = Some(0.01);

    let result = run_backtest(&df, &params).expect("backtest failed");
    assert_eq!(
        result.trade_count, 0,
        "Premium $0.01 max should exclude all entries"
    );
}

#[test]
fn premium_range_allows_matching_entries() {
    let baseline = baseline_trade_count();

    let df = make_multi_strike_df();
    let mut params = backtest_params("short_put", vec![delta(0.20)]);
    // Wide range that should allow all entries
    params.min_net_premium = Some(0.01);
    params.max_net_premium = Some(100.0);

    let result = run_backtest(&df, &params).expect("backtest failed");
    assert_eq!(
        result.trade_count, baseline,
        "Wide premium range should match baseline trade count"
    );
}

// ---------------------------------------------------------------------------
// Net delta filters
// ---------------------------------------------------------------------------

#[test]
fn max_net_delta_excludes_high_delta_entries() {
    let df = make_multi_strike_df();
    let mut params = backtest_params("short_put", vec![delta(0.20)]);
    // Short put net delta is negative (e.g., -0.20 × -1 side = +0.20 signed).
    // Setting max_net_delta very low should exclude entries.
    params.max_net_delta = Some(-1.0);

    let result = run_backtest(&df, &params).expect("backtest failed");
    assert_eq!(
        result.trade_count, 0,
        "max_net_delta=-1.0 should exclude all short put entries"
    );
}

#[test]
fn min_net_delta_excludes_low_delta_entries() {
    let df = make_multi_strike_df();
    let mut params = backtest_params("short_put", vec![delta(0.20)]);
    // Setting min_net_delta very high should exclude entries
    params.min_net_delta = Some(5.0);

    let result = run_backtest(&df, &params).expect("backtest failed");
    assert_eq!(
        result.trade_count, 0,
        "min_net_delta=5.0 should exclude all short put entries"
    );
}

// ---------------------------------------------------------------------------
// Stagger / cooldown
// ---------------------------------------------------------------------------

#[test]
fn min_days_between_entries_one_allows_all() {
    let baseline = baseline_trade_count();

    let df = make_multi_strike_df();
    let mut params = backtest_params("short_put", vec![delta(0.20)]);
    // 1-day cooldown should not block anything (entries are 7+ days apart in synthetic data)
    params.min_days_between_entries = Some(1);

    let result = run_backtest(&df, &params).expect("backtest failed");
    assert_eq!(
        result.trade_count, baseline,
        "1-day stagger should match baseline"
    );
}

// ---------------------------------------------------------------------------
// Expiration filter
// ---------------------------------------------------------------------------

#[test]
fn expiration_filter_monthly_affects_trades() {
    let df = make_multi_strike_df();
    let mut params = backtest_params("short_put", vec![delta(0.20)]);
    params.expiration_filter = ExpirationFilter::Monthly;

    let result = run_backtest(&df, &params);
    // Monthly filter requires third Friday — may reduce or eliminate trades
    // depending on whether expirations in synthetic data fall on third Fridays.
    // Feb 16 2024 is the third Friday of February, so near-term exp should pass.
    // Mar 15 2024 is the third Friday of March, so far-term exp should pass too.
    match result {
        Ok(r) => {
            // Both expirations in synthetic data happen to be third Fridays,
            // so trade count should match baseline.
            let baseline = baseline_trade_count();
            assert_eq!(
                r.trade_count, baseline,
                "Both synthetic expirations are third Fridays, should match baseline"
            );
        }
        Err(e) => panic!("Monthly filter should not error: {e}"),
    }
}

// ---------------------------------------------------------------------------
// Exit net delta
// ---------------------------------------------------------------------------

#[test]
fn exit_net_delta_triggers_early_exit() {
    let df = make_multi_strike_df();
    let mut params = backtest_params("short_put", vec![delta(0.20)]);
    // Very low threshold — should trigger delta exit on most positions
    params.exit_net_delta = Some(0.01);

    let result = run_backtest(&df, &params).expect("backtest failed");

    if !result.trade_log.is_empty() {
        let has_delta_exit = result
            .trade_log
            .iter()
            .any(|t| matches!(t.exit_type, ExitType::DeltaExit));
        assert!(
            has_delta_exit,
            "exit_net_delta=0.01 should produce at least one DeltaExit"
        );
    }
}

#[test]
fn exit_net_delta_high_threshold_no_effect() {
    let baseline = baseline_trade_count();

    let df = make_multi_strike_df();
    let mut params = backtest_params("short_put", vec![delta(0.20)]);
    // Very high threshold — should never trigger
    params.exit_net_delta = Some(100.0);

    let result = run_backtest(&df, &params).expect("backtest failed");
    assert_eq!(
        result.trade_count, baseline,
        "High exit_net_delta should not affect trade count"
    );

    let has_delta_exit = result
        .trade_log
        .iter()
        .any(|t| matches!(t.exit_type, ExitType::DeltaExit));
    assert!(
        !has_delta_exit,
        "exit_net_delta=100 should never trigger DeltaExit"
    );
}
