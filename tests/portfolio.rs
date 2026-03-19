//! Integration tests for the portfolio backtest pipeline.
//!
//! Tests the pure computation functions (`combine_equity_curves`,
//! `compute_correlation_matrix`, `extract_daily_returns`, `align_return_streams`,
//! `compute_contributions`) with realistic multi-strategy data sets that exercise
//! cross-date alignment, carry-forward logic, and correlation edge cases.

use chrono::{Datelike, NaiveDate};
use optopsy_mcp::engine::portfolio::{
    align_return_streams, combine_equity_curves, compute_contributions, compute_correlation_matrix,
    extract_daily_returns,
};
use optopsy_mcp::engine::types::EquityPoint;

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Build an equity curve from `(year, month, day, equity)` tuples.
fn make_equity(values: &[(i32, u32, u32, f64)]) -> Vec<EquityPoint> {
    values
        .iter()
        .map(|(y, m, d, eq)| EquityPoint {
            datetime: NaiveDate::from_ymd_opt(*y, *m, *d)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            equity: *eq,
        })
        .collect()
}

/// Generate a synthetic equity curve that grows by `daily_return` each day
/// starting from `start_equity` over `n_days` trading days starting at `start_date`.
fn synthetic_equity(
    start_date: NaiveDate,
    start_equity: f64,
    daily_return: f64,
    n_days: usize,
) -> Vec<EquityPoint> {
    let mut curve = Vec::with_capacity(n_days);
    let mut equity = start_equity;
    let mut date = start_date;
    for _ in 0..n_days {
        curve.push(EquityPoint {
            datetime: date.and_hms_opt(0, 0, 0).unwrap(),
            equity,
        });
        equity *= 1.0 + daily_return;
        // Skip weekends
        date = match date.weekday() {
            chrono::Weekday::Fri => date + chrono::Duration::days(3),
            chrono::Weekday::Sat => date + chrono::Duration::days(2),
            _ => date + chrono::Duration::days(1),
        };
    }
    curve
}

// ── Tests ───────────────────────────────────────────────────────────────────

/// Full pipeline: two strategies with different start dates, combine, correlate,
/// and compute contributions.
#[test]
fn portfolio_full_pipeline_two_strategies() {
    let start = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
    let start_offset = NaiveDate::from_ymd_opt(2024, 1, 4).unwrap();

    // Strategy A: starts Jan 2, grows 0.1% daily, 20 days, allocated $7000 (70%)
    let curve_a = synthetic_equity(start, 7000.0, 0.001, 20);
    // Strategy B: starts Jan 4 (2 days later), grows 0.2% daily, 18 days, allocated $3000 (30%)
    let curve_b = synthetic_equity(start_offset, 3000.0, 0.002, 18);

    // ── Combine ──
    let combined =
        combine_equity_curves(&[(curve_a.clone(), 0.7), (curve_b.clone(), 0.3)], 10000.0);

    // Combined curve should span union of dates
    assert!(
        combined.len() >= 20,
        "combined curve should have at least 20 points"
    );
    // First point: curve_a starts at 7000, curve_b carry-forward initial = 3000
    assert!(
        (combined[0].equity - 10000.0).abs() < 1e-6,
        "initial combined equity should be 10000, got {}",
        combined[0].equity
    );
    // Combined equity should generally increase since both strategies are growing
    let last = combined.last().unwrap().equity;
    assert!(
        last > 10000.0,
        "combined equity should grow over time, final = {last}"
    );

    // ── Extract returns and correlate ──
    let returns_a = extract_daily_returns(&curve_a);
    let returns_b = extract_daily_returns(&curve_b);
    assert!(!returns_a.is_empty());
    assert!(!returns_b.is_empty());

    let aligned = align_return_streams(&[returns_a, returns_b]);
    assert_eq!(aligned.len(), 2);
    // Both vectors should be the same length (union of dates)
    assert_eq!(aligned[0].len(), aligned[1].len());

    let labels = vec!["SPY_long".to_string(), "AAPL_long".to_string()];
    let corr = compute_correlation_matrix(&aligned, &labels);
    assert_eq!(corr.len(), 1);
    // Both have constant positive returns (nearly flat return streams) so correlation
    // should be relatively high or at least computable
    assert!(
        corr[0].correlation.is_finite(),
        "correlation should be finite"
    );

    // ── Contributions ──
    let pnl_a = curve_a.last().unwrap().equity - curve_a[0].equity;
    let pnl_b = curve_b.last().unwrap().equity - curve_b[0].equity;
    let contribs = compute_contributions(&[pnl_a, pnl_b], &labels);
    assert_eq!(contribs.len(), 2);
    // Both P&Ls are positive
    assert!(contribs[0].1 > 0.0);
    assert!(contribs[1].1 > 0.0);
    let abs_sum: f64 = contribs.iter().map(|(_, p)| p.abs()).sum();
    assert!(
        (abs_sum - 100.0).abs() < 1e-10,
        "absolute contributions should sum to 100%"
    );
}

/// Three strategies: one bullish, one bearish, one flat. Verify N*(N-1)/2 = 3
/// correlation entries and divergent contribution signs.
#[test]
fn portfolio_three_strategies_correlation_and_contributions() {
    let start = NaiveDate::from_ymd_opt(2024, 3, 1).unwrap();

    // Bull: +0.5% daily
    let bull = synthetic_equity(start, 5000.0, 0.005, 30);
    // Bear: -0.3% daily
    let bear = synthetic_equity(start, 3000.0, -0.003, 30);
    // Flat: 0% daily
    let flat = synthetic_equity(start, 2000.0, 0.0, 30);

    let returns_bull = extract_daily_returns(&bull);
    let returns_bear = extract_daily_returns(&bear);
    let returns_flat = extract_daily_returns(&flat);

    let aligned = align_return_streams(&[returns_bull, returns_bear, returns_flat]);
    assert_eq!(aligned.len(), 3);
    // All same dates so all same length
    assert_eq!(aligned[0].len(), aligned[1].len());
    assert_eq!(aligned[1].len(), aligned[2].len());

    let labels = vec!["Bull".to_string(), "Bear".to_string(), "Flat".to_string()];
    let corr = compute_correlation_matrix(&aligned, &labels);
    // 3*(3-1)/2 = 3 pairs
    assert_eq!(corr.len(), 3);

    // Flat strategy has zero variance → pearson returns 0.0
    // Find Bull-Flat and Bear-Flat entries
    for entry in &corr {
        if entry.strategy_a == "Flat" || entry.strategy_b == "Flat" {
            assert!(
                entry.correlation.abs() < 1e-10,
                "correlation with zero-variance (flat) should be 0.0, got {}",
                entry.correlation
            );
        }
    }

    // Contributions: bull is positive, bear is negative
    let pnl_bull = bull.last().unwrap().equity - bull[0].equity;
    let pnl_bear = bear.last().unwrap().equity - bear[0].equity;
    let pnl_flat = flat.last().unwrap().equity - flat[0].equity;
    assert!(pnl_bull > 0.0);
    assert!(pnl_bear < 0.0);
    assert!((pnl_flat - 0.0).abs() < 1e-10);

    let contribs = compute_contributions(&[pnl_bull, pnl_bear, pnl_flat], &labels);
    assert!(contribs[0].1 > 0.0, "bull contribution should be positive");
    assert!(contribs[1].1 < 0.0, "bear contribution should be negative");
    assert!(
        contribs[2].1.abs() < 1e-10,
        "flat contribution should be ~0%"
    );
}

/// Combine equity curves where one strategy starts much later than the other.
/// Verify carry-forward produces expected behavior.
#[test]
fn portfolio_staggered_start_carry_forward() {
    // Strategy A: 10 days starting Jan 2
    let curve_a = make_equity(&[
        (2024, 1, 2, 6000.0),
        (2024, 1, 3, 6060.0),
        (2024, 1, 4, 6120.0),
        (2024, 1, 5, 6180.0),
        (2024, 1, 8, 6240.0),
        (2024, 1, 9, 6300.0),
        (2024, 1, 10, 6360.0),
        (2024, 1, 11, 6420.0),
        (2024, 1, 12, 6480.0),
        (2024, 1, 15, 6540.0),
    ]);
    // Strategy B: starts Jan 8 (6 days later)
    let curve_b = make_equity(&[
        (2024, 1, 8, 4000.0),
        (2024, 1, 9, 4040.0),
        (2024, 1, 10, 4080.0),
        (2024, 1, 11, 4120.0),
        (2024, 1, 12, 4160.0),
        (2024, 1, 15, 4200.0),
    ]);

    let combined =
        combine_equity_curves(&[(curve_a.clone(), 0.6), (curve_b.clone(), 0.3)], 10000.0);

    // Should have union of all dates: Jan 2-5, 8-12, 15 = 10 dates
    assert_eq!(combined.len(), 10);

    // Before Jan 8, curve_b hasn't started yet → carry-forward at starting equity
    // (4000), so return_ratio = 4000/4000 = 1.0 → contributes 0.3 * 10000 * 1.0 = 3000.
    // curve_a starts at 6000 → return_ratio = 6000/6000 = 1.0 → contributes 0.6 * 10000 * 1.0 = 6000.
    // Total weights = 0.6 + 0.3 = 0.9, so initial portfolio = 9000 (10% cash).
    assert!(
        (combined[0].equity - 9000.0).abs() < 1e-6,
        "Jan 2 combined should be 9000 (60% + 30% of 10000), got {}",
        combined[0].equity
    );
}

/// Edge case: combine with zero-weight strategy.
#[test]
fn portfolio_zero_weight_strategy_ignored() {
    let c1 = make_equity(&[(2024, 1, 2, 10000.0), (2024, 1, 3, 10100.0)]);
    let c2 = make_equity(&[(2024, 1, 2, 999999.0), (2024, 1, 3, 0.01)]);

    // c2 has weight 0 → its values should not affect the portfolio
    let combined = combine_equity_curves(&[(c1, 1.0), (c2, 0.0)], 10000.0);
    assert_eq!(combined.len(), 2);
    // Zero-weight strategy has strategy_capital=0 → the if-guard skips it
    // So portfolio = c1's contribution only
    assert!(
        (combined[0].equity - 10000.0).abs() < 1e-6,
        "zero-weight strategy should not affect combined equity"
    );
}
