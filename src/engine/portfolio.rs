//! Portfolio-level computations for combining multiple stock strategy results.
//!
//! Pure computation module — no IO. Functions operate on equity curves, daily
//! returns, and P&L values produced by individual stock backtests.

use chrono::NaiveDate;
use std::collections::{BTreeMap, BTreeSet};

use super::types::EquityPoint;
use crate::stats;
use crate::tools::response_types::CorrelationEntry;

/// Extract daily percent returns with dates from an equity curve.
///
/// Returns `(date, pct_return)` pairs suitable for alignment across strategies.
pub fn extract_daily_returns(equity_curve: &[EquityPoint]) -> Vec<(NaiveDate, f64)> {
    if equity_curve.len() < 2 {
        return vec![];
    }

    let mut returns = Vec::with_capacity(equity_curve.len() - 1);
    for window in equity_curve.windows(2) {
        let prev = &window[0];
        let curr = &window[1];
        let date = curr.datetime.date();
        if prev.equity > 0.0 {
            let r = (curr.equity - prev.equity) / prev.equity;
            returns.push((date, r));
        }
    }
    returns
}

/// Align multiple return streams by date (union of all dates, 0.0 for missing days).
///
/// Returns a Vec of Vec<f64>, one per input stream, all of equal length.
pub fn align_return_streams(streams: &[Vec<(NaiveDate, f64)>]) -> Vec<Vec<f64>> {
    if streams.is_empty() {
        return vec![];
    }

    // Collect the union of all dates
    let all_dates: BTreeSet<NaiveDate> = streams
        .iter()
        .flat_map(|s| s.iter().map(|(d, _)| *d))
        .collect();

    let date_vec: Vec<NaiveDate> = all_dates.into_iter().collect();

    // For each stream, build a date→return map for O(1) lookups
    streams
        .iter()
        .map(|stream| {
            let map: BTreeMap<NaiveDate, f64> = stream.iter().copied().collect();
            date_vec
                .iter()
                .map(|d| *map.get(d).unwrap_or(&0.0))
                .collect()
        })
        .collect()
}

/// Compute N*(N-1)/2 pairwise Pearson correlations.
pub fn compute_correlation_matrix(
    daily_returns: &[Vec<f64>],
    labels: &[String],
) -> Vec<CorrelationEntry> {
    let n = daily_returns.len();
    let mut entries = Vec::with_capacity(n * (n - 1) / 2);

    for i in 0..n {
        for j in (i + 1)..n {
            let r = stats::pearson(&daily_returns[i], &daily_returns[j]).clamp(-1.0, 1.0);
            entries.push(CorrelationEntry {
                strategy_a: labels[i].clone(),
                strategy_b: labels[j].clone(),
                correlation: r,
            });
        }
    }

    entries
}

/// Combine multiple equity curves into a single portfolio equity curve using
/// capital-weighted allocation.
///
/// Each entry in `curves` is `(equity_curve, allocation_weight)` where the weight
/// is a fraction (e.g., 0.30 for 30%). For each date in the union of all curves,
/// the portfolio equity is: `sum(strategy_equity * weight)`.
///
/// Uses carry-forward for missing dates (last known equity value).
pub fn combine_equity_curves(
    curves: &[(Vec<EquityPoint>, f64)],
    total_capital: f64,
) -> Vec<EquityPoint> {
    if curves.is_empty() {
        return vec![];
    }

    // Collect all unique datetimes across all curves
    let all_datetimes: BTreeSet<chrono::NaiveDateTime> = curves
        .iter()
        .flat_map(|(curve, _)| curve.iter().map(|p| p.datetime))
        .collect();

    // For each curve, build a datetime→equity map
    let curve_maps: Vec<(BTreeMap<chrono::NaiveDateTime, f64>, f64)> = curves
        .iter()
        .map(|(curve, weight)| {
            let map: BTreeMap<chrono::NaiveDateTime, f64> =
                curve.iter().map(|p| (p.datetime, p.equity)).collect();
            (map, *weight)
        })
        .collect();

    let mut combined = Vec::with_capacity(all_datetimes.len());

    // Starting equity of each curve for return-ratio normalization.
    // This makes the function work regardless of what starting capital each
    // curve was backtested with — we normalize to returns, then reweight.
    let starting_equities: Vec<f64> = curves
        .iter()
        .map(|(curve, weight)| {
            curve
                .first()
                .map(|p| p.equity)
                .unwrap_or(total_capital * weight)
        })
        .collect();

    // Carry-forward tracker: initialized to each curve's starting equity so
    // that before a curve's first date, return_ratio = start/start = 1.0
    // (flat, no gain or loss).
    let mut last_equity: Vec<f64> = starting_equities.clone();

    for dt in &all_datetimes {
        let mut portfolio_equity = 0.0;
        for (i, (map, weight)) in curve_maps.iter().enumerate() {
            let eq = if let Some(&e) = map.get(dt) {
                last_equity[i] = e;
                e
            } else {
                last_equity[i]
            };
            // Normalize: convert equity to a return ratio relative to the
            // curve's own starting equity, then scale by portfolio weight.
            // This decouples the weighting from whatever capital the backtest used.
            let start_eq = starting_equities[i];
            if start_eq > 0.0 {
                let return_ratio = eq / start_eq;
                portfolio_equity += total_capital * weight * return_ratio;
            }
        }
        combined.push(EquityPoint {
            datetime: *dt,
            equity: portfolio_equity,
        });
    }

    combined
}

/// Compute each strategy's P&L as a percentage of total portfolio P&L.
///
/// Returns `(label, contribution_pct)` pairs. If total P&L is zero, all
/// contributions are 0.0.
pub fn compute_contributions(pnl_values: &[f64], labels: &[String]) -> Vec<(String, f64)> {
    let total_abs: f64 = pnl_values.iter().map(|p| p.abs()).sum();

    labels
        .iter()
        .zip(pnl_values.iter())
        .map(|(label, &pnl)| {
            let pct = if total_abs > 0.0 {
                (pnl / total_abs) * 100.0
            } else {
                0.0
            };
            (label.clone(), pct)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    /// Helper: build an equity curve from `(day_of_month, equity)` pairs in Jan 2024.
    fn make_equity(values: &[(i32, f64)]) -> Vec<EquityPoint> {
        values
            .iter()
            .map(|(day, eq)| EquityPoint {
                datetime: NaiveDate::from_ymd_opt(2024, 1, *day as u32)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                equity: *eq,
            })
            .collect()
    }

    /// Helper: build an equity curve from `(year, month, day, equity)` tuples.
    fn make_equity_ymd(values: &[(i32, u32, u32, f64)]) -> Vec<EquityPoint> {
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

    // ── extract_daily_returns ──────────────────────────────────────────────

    #[test]
    fn extract_daily_returns_basic() {
        let curve = make_equity(&[(1, 10000.0), (2, 10100.0), (3, 10200.0)]);
        let returns = extract_daily_returns(&curve);
        assert_eq!(returns.len(), 2);
        // Day 2: (10100 - 10000) / 10000 = 0.01
        assert!((returns[0].1 - 0.01).abs() < 1e-10);
        // Day 3: (10200 - 10100) / 10100 ≈ 0.00990099
        let expected = (10200.0 - 10100.0) / 10100.0;
        assert!((returns[1].1 - expected).abs() < 1e-10);
    }

    #[test]
    fn extract_daily_returns_empty_and_single() {
        // Empty curve returns empty
        assert!(extract_daily_returns(&[]).is_empty());
        // Single point returns empty (need 2+ for a return)
        let single = make_equity(&[(1, 10000.0)]);
        assert!(extract_daily_returns(&single).is_empty());
    }

    #[test]
    fn extract_daily_returns_negative_movement() {
        let curve = make_equity(&[(1, 10000.0), (2, 9500.0)]);
        let returns = extract_daily_returns(&curve);
        assert_eq!(returns.len(), 1);
        assert!((returns[0].1 - (-0.05)).abs() < 1e-10);
    }

    #[test]
    fn extract_daily_returns_preserves_dates() {
        let curve = make_equity(&[(3, 100.0), (5, 110.0), (8, 105.0)]);
        let returns = extract_daily_returns(&curve);
        assert_eq!(returns.len(), 2);
        // Returned dates come from the *current* point in each window
        assert_eq!(returns[0].0, NaiveDate::from_ymd_opt(2024, 1, 5).unwrap());
        assert_eq!(returns[1].0, NaiveDate::from_ymd_opt(2024, 1, 8).unwrap());
    }

    #[test]
    fn extract_daily_returns_skips_zero_equity() {
        // If previous equity is 0 the return is undefined; function should skip it
        let curve = make_equity(&[(1, 0.0), (2, 100.0), (3, 110.0)]);
        let returns = extract_daily_returns(&curve);
        // Only one valid return: day2→day3 (day1→day2 skipped because prev=0)
        assert_eq!(returns.len(), 1);
        assert!((returns[0].1 - 0.10).abs() < 1e-10);
    }

    // ── align_return_streams ───────────────────────────────────────────────

    #[test]
    fn align_return_streams_fills_zeros() {
        let s1 = vec![
            (NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), 0.01),
            (NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(), 0.02),
        ];
        let s2 = vec![(NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(), -0.01)];
        let aligned = align_return_streams(&[s1, s2]);
        assert_eq!(aligned.len(), 2);
        assert_eq!(aligned[0].len(), 2); // union has 2 dates
        assert_eq!(aligned[1].len(), 2);
        // s1 values preserved
        assert!((aligned[0][0] - 0.01).abs() < f64::EPSILON);
        assert!((aligned[0][1] - 0.02).abs() < f64::EPSILON);
        // s2 missing Jan 1 → 0.0
        assert!((aligned[1][0] - 0.0).abs() < f64::EPSILON);
        assert!((aligned[1][1] - (-0.01)).abs() < f64::EPSILON);
    }

    #[test]
    fn align_return_streams_empty_input() {
        assert!(align_return_streams(&[]).is_empty());
    }

    #[test]
    fn align_return_streams_non_overlapping() {
        let s1 = vec![
            (NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), 0.05),
            (NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(), 0.03),
        ];
        let s2 = vec![
            (NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(), -0.02),
            (NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(), 0.01),
        ];
        let aligned = align_return_streams(&[s1, s2]);
        // Union of dates: Jan 1,2,3,4 → 4 entries each
        assert_eq!(aligned[0].len(), 4);
        assert_eq!(aligned[1].len(), 4);
        // s1 has data for days 1,2 only → days 3,4 are 0.0
        assert!((aligned[0][2] - 0.0).abs() < f64::EPSILON);
        assert!((aligned[0][3] - 0.0).abs() < f64::EPSILON);
        // s2 has data for days 3,4 only → days 1,2 are 0.0
        assert!((aligned[1][0] - 0.0).abs() < f64::EPSILON);
        assert!((aligned[1][1] - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn align_return_streams_fully_overlapping() {
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let s1 = vec![(d1, 0.01), (d2, 0.02)];
        let s2 = vec![(d1, -0.01), (d2, -0.02)];
        let aligned = align_return_streams(&[s1, s2]);
        assert_eq!(aligned[0].len(), 2);
        assert_eq!(aligned[1].len(), 2);
        // No zero-fills needed since both streams cover same dates
        assert!((aligned[0][0] - 0.01).abs() < f64::EPSILON);
        assert!((aligned[1][0] - (-0.01)).abs() < f64::EPSILON);
    }

    // ── compute_correlation_matrix ─────────────────────────────────────────

    #[test]
    fn correlation_matrix_size_two_strategies() {
        let r1 = vec![0.01, 0.02, 0.03, 0.04, 0.05];
        let r2 = vec![0.02, 0.04, 0.06, 0.08, 0.10];
        let labels = vec!["A".to_string(), "B".to_string()];
        let entries = compute_correlation_matrix(&[r1, r2], &labels);
        // 2*(2-1)/2 = 1
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].strategy_a, "A");
        assert_eq!(entries[0].strategy_b, "B");
    }

    #[test]
    fn correlation_matrix_size_three_strategies() {
        let r1 = vec![0.01, -0.02, 0.03];
        let r2 = vec![0.02, -0.01, 0.01];
        let r3 = vec![-0.01, 0.02, -0.03];
        let labels = vec!["A".to_string(), "B".to_string(), "C".to_string()];
        let entries = compute_correlation_matrix(&[r1, r2, r3], &labels);
        // 3*(3-1)/2 = 3
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn correlation_matrix_perfectly_correlated() {
        let r1 = vec![0.01, 0.02, 0.03, 0.04, 0.05];
        let r2 = vec![0.02, 0.04, 0.06, 0.08, 0.10]; // r2 = 2 * r1
        let labels = vec!["A".to_string(), "B".to_string()];
        let entries = compute_correlation_matrix(&[r1, r2], &labels);
        assert_eq!(entries.len(), 1);
        assert!(
            (entries[0].correlation - 1.0).abs() < 1e-10,
            "perfectly correlated streams should yield ~1.0, got {}",
            entries[0].correlation
        );
    }

    #[test]
    fn correlation_matrix_perfectly_anti_correlated() {
        let r1 = vec![0.01, 0.02, 0.03, 0.04, 0.05];
        let r2 = vec![-0.01, -0.02, -0.03, -0.04, -0.05]; // r2 = -r1
        let labels = vec!["A".to_string(), "B".to_string()];
        let entries = compute_correlation_matrix(&[r1, r2], &labels);
        assert_eq!(entries.len(), 1);
        assert!(
            (entries[0].correlation - (-1.0)).abs() < 1e-10,
            "anti-correlated streams should yield ~-1.0, got {}",
            entries[0].correlation
        );
    }

    #[test]
    fn correlation_matrix_uncorrelated() {
        // Construct two deterministic sequences that are approximately uncorrelated
        // Using sin and cos at incommensurate frequencies
        let n = 200;
        let r1: Vec<f64> = (0..n).map(|i| (i as f64 * 0.1).sin() * 0.01).collect();
        let r2: Vec<f64> = (0..n).map(|i| (i as f64 * 0.37).cos() * 0.01).collect();
        let labels = vec!["A".to_string(), "B".to_string()];
        let entries = compute_correlation_matrix(&[r1, r2], &labels);
        assert_eq!(entries.len(), 1);
        assert!(
            entries[0].correlation.abs() < 0.2,
            "uncorrelated streams should yield ~0.0, got {}",
            entries[0].correlation
        );
    }

    #[test]
    fn correlation_matrix_labels_correct() {
        let r1 = vec![0.01, 0.02];
        let r2 = vec![0.03, 0.04];
        let r3 = vec![0.05, 0.06];
        let labels = vec!["SPY".to_string(), "AAPL".to_string(), "MSFT".to_string()];
        let entries = compute_correlation_matrix(&[r1, r2, r3], &labels);
        // Should produce pairs: (SPY,AAPL), (SPY,MSFT), (AAPL,MSFT)
        assert_eq!(entries[0].strategy_a, "SPY");
        assert_eq!(entries[0].strategy_b, "AAPL");
        assert_eq!(entries[1].strategy_a, "SPY");
        assert_eq!(entries[1].strategy_b, "MSFT");
        assert_eq!(entries[2].strategy_a, "AAPL");
        assert_eq!(entries[2].strategy_b, "MSFT");
    }

    // ── combine_equity_curves ──────────────────────────────────────────────

    #[test]
    fn combine_equity_curves_empty() {
        let combined = combine_equity_curves(&[], 10000.0);
        assert!(combined.is_empty());
    }

    #[test]
    fn combine_equity_curves_single_strategy_passthrough() {
        let c1 = make_equity(&[(1, 10000.0), (2, 10500.0), (3, 10200.0)]);
        let combined = combine_equity_curves(&[(c1.clone(), 1.0)], 10000.0);
        assert_eq!(combined.len(), 3);
        for (i, pt) in combined.iter().enumerate() {
            assert!(
                (pt.equity - c1[i].equity).abs() < 1e-10,
                "single strategy with weight=1.0 should pass through, day {}",
                i
            );
        }
    }

    #[test]
    fn combine_equity_curves_weights_actually_matter() {
        // KEY TEST: Two strategies with DIFFERENT returns, verify weights change the outcome.
        // c1 starts at 1000, goes to 1100 (+10%)
        // c2 starts at 1000, goes to 900 (-10%)
        // With 80/20 weighting on $10000: portfolio should be weighted toward c1.
        let c1 = make_equity(&[(1, 1000.0), (2, 1100.0)]); // +10%
        let c2 = make_equity(&[(1, 1000.0), (2, 900.0)]); // -10%

        // 80/20 split
        let combined_80_20 =
            combine_equity_curves(&[(c1.clone(), 0.8), (c2.clone(), 0.2)], 10000.0);
        // Day 1: 10000 (initial)
        assert!((combined_80_20[0].equity - 10000.0).abs() < 1e-6);
        // Day 2: 0.8 * 10000 * 1.10 + 0.2 * 10000 * 0.90 = 8800 + 1800 = 10600
        assert!(
            (combined_80_20[1].equity - 10600.0).abs() < 1e-6,
            "80/20 weighting day 2 should be 10600, got {}",
            combined_80_20[1].equity
        );

        // 20/80 split — opposite weighting should give different result
        let combined_20_80 =
            combine_equity_curves(&[(c1.clone(), 0.2), (c2.clone(), 0.8)], 10000.0);
        assert!((combined_20_80[0].equity - 10000.0).abs() < 1e-6);
        // Day 2: 0.2 * 10000 * 1.10 + 0.8 * 10000 * 0.90 = 2200 + 7200 = 9400
        assert!(
            (combined_20_80[1].equity - 9400.0).abs() < 1e-6,
            "20/80 weighting day 2 should be 9400, got {}",
            combined_20_80[1].equity
        );

        // 50/50 split — should be exactly 10000 (gains cancel losses)
        let combined_50_50 = combine_equity_curves(&[(c1, 0.5), (c2, 0.5)], 10000.0);
        assert!(
            (combined_50_50[1].equity - 10000.0).abs() < 1e-6,
            "50/50 weighting should net to 10000, got {}",
            combined_50_50[1].equity
        );
    }

    #[test]
    fn combine_equity_curves_arbitrary_starting_capital() {
        // Curves DON'T start at total_capital * weight — verify normalization works.
        // c1 starts at 50000 (arbitrary), goes to 55000 (+10%)
        // c2 starts at 200 (arbitrary), goes to 210 (+5%)
        // Weights: 60/40 on $10000
        let c1 = make_equity(&[(1, 50000.0), (2, 55000.0)]); // +10%
        let c2 = make_equity(&[(1, 200.0), (2, 210.0)]); // +5%
        let combined = combine_equity_curves(&[(c1, 0.6), (c2, 0.4)], 10000.0);

        // Day 1: should always start at total_capital
        assert!(
            (combined[0].equity - 10000.0).abs() < 1e-6,
            "initial portfolio equity should be 10000 regardless of curve starting values, got {}",
            combined[0].equity
        );
        // Day 2: 0.6 * 10000 * 1.10 + 0.4 * 10000 * 1.05 = 6600 + 4200 = 10800
        assert!(
            (combined[1].equity - 10800.0).abs() < 1e-6,
            "day 2 should reflect weighted returns (10800), got {}",
            combined[1].equity
        );
    }

    #[test]
    fn combine_equity_curves_three_strategies_weighted() {
        // Three strategies: +20%, -10%, +5% returns
        // Weights: 50/30/20 on $100000
        let c1 = make_equity(&[(1, 1000.0), (2, 1200.0)]); // +20%
        let c2 = make_equity(&[(1, 1000.0), (2, 900.0)]); // -10%
        let c3 = make_equity(&[(1, 1000.0), (2, 1050.0)]); // +5%
        let combined = combine_equity_curves(&[(c1, 0.5), (c2, 0.3), (c3, 0.2)], 100000.0);

        assert!((combined[0].equity - 100000.0).abs() < 1e-6);
        // Day 2: 0.5*100000*1.20 + 0.3*100000*0.90 + 0.2*100000*1.05
        //       = 60000 + 27000 + 21000 = 108000
        assert!(
            (combined[1].equity - 108000.0).abs() < 1e-6,
            "three-strategy weighted combo should be 108000, got {}",
            combined[1].equity
        );
    }

    #[test]
    fn combine_equity_curves_carry_forward_preserves_weight() {
        // c1 has days 1,2,3; c2 only has day 1,3 (missing day 2)
        // c1: 1000 → 1100 → 1200 (+10%, +9.09%)
        // c2: 500 → (carry 500) → 550 (+0%, +10%)
        // Weights: 60/40 on $10000
        let c1 = make_equity(&[(1, 1000.0), (2, 1100.0), (3, 1200.0)]);
        let c2 = make_equity(&[(1, 500.0), (3, 550.0)]);
        let combined = combine_equity_curves(&[(c1, 0.6), (c2, 0.4)], 10000.0);

        assert_eq!(combined.len(), 3);
        // Day 1: 10000
        assert!((combined[0].equity - 10000.0).abs() < 1e-6);
        // Day 2: c1 return = 1100/1000 = 1.10, c2 carry-forward = 500/500 = 1.00
        // portfolio = 0.6*10000*1.10 + 0.4*10000*1.00 = 6600 + 4000 = 10600
        assert!(
            (combined[1].equity - 10600.0).abs() < 1e-6,
            "day 2 carry-forward should hold c2 flat, got {}",
            combined[1].equity
        );
        // Day 3: c1 return = 1200/1000 = 1.20, c2 return = 550/500 = 1.10
        // portfolio = 0.6*10000*1.20 + 0.4*10000*1.10 = 7200 + 4400 = 11600
        assert!(
            (combined[2].equity - 11600.0).abs() < 1e-6,
            "day 3 should reflect both strategies' returns, got {}",
            combined[2].equity
        );
    }

    #[test]
    fn combine_equity_curves_zero_weight_ignored() {
        // Strategy with weight=0 should contribute nothing
        let c1 = make_equity(&[(1, 10000.0), (2, 11000.0)]); // +10%
        let c2 = make_equity(&[(1, 99999.0), (2, 1.0)]); // -99.99%
        let combined = combine_equity_curves(&[(c1, 1.0), (c2, 0.0)], 10000.0);

        assert!((combined[0].equity - 10000.0).abs() < 1e-6);
        // Only c1 matters: 1.0 * 10000 * 1.10 = 11000
        assert!(
            (combined[1].equity - 11000.0).abs() < 1e-6,
            "zero-weight strategy should not affect portfolio, got {}",
            combined[1].equity
        );
    }

    // ── compute_contributions ──────────────────────────────────────────────

    #[test]
    fn contributions_basic() {
        let pnls = vec![100.0, -50.0, 50.0];
        let labels = vec!["A".to_string(), "B".to_string(), "C".to_string()];
        let contribs = compute_contributions(&pnls, &labels);
        assert_eq!(contribs.len(), 3);
        // total_abs = 200, A = 100/200*100 = 50%
        assert!((contribs[0].1 - 50.0).abs() < 1e-10);
        // B = -50/200*100 = -25%
        assert!((contribs[1].1 - (-25.0)).abs() < 1e-10);
        // C = 50/200*100 = 25%
        assert!((contribs[2].1 - 25.0).abs() < 1e-10);
    }

    #[test]
    fn contributions_sum_of_absolute_values_is_100() {
        let pnls = vec![200.0, -100.0, 50.0, -150.0];
        let labels = vec![
            "A".to_string(),
            "B".to_string(),
            "C".to_string(),
            "D".to_string(),
        ];
        let contribs = compute_contributions(&pnls, &labels);
        let abs_sum: f64 = contribs.iter().map(|(_, pct)| pct.abs()).sum();
        assert!(
            (abs_sum - 100.0).abs() < 1e-10,
            "absolute contributions should sum to 100%, got {abs_sum}"
        );
    }

    #[test]
    fn contributions_zero_total_pnl() {
        let pnls = vec![0.0, 0.0];
        let labels = vec!["A".to_string(), "B".to_string()];
        let contribs = compute_contributions(&pnls, &labels);
        assert_eq!(contribs.len(), 2);
        assert!((contribs[0].1 - 0.0).abs() < f64::EPSILON);
        assert!((contribs[1].1 - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn contributions_one_negative_one_positive() {
        let pnls = vec![300.0, -100.0];
        let labels = vec!["Bull".to_string(), "Bear".to_string()];
        let contribs = compute_contributions(&pnls, &labels);
        // total_abs = 400
        // Bull = 300/400*100 = 75%
        assert!((contribs[0].1 - 75.0).abs() < 1e-10);
        // Bear = -100/400*100 = -25%
        assert!((contribs[1].1 - (-25.0)).abs() < 1e-10);
    }

    #[test]
    fn contributions_preserves_labels() {
        let pnls = vec![10.0, 20.0];
        let labels = vec!["SPY_long".to_string(), "AAPL_short".to_string()];
        let contribs = compute_contributions(&pnls, &labels);
        assert_eq!(contribs[0].0, "SPY_long");
        assert_eq!(contribs[1].0, "AAPL_short");
    }

    // ── Integration-style: extract → align → correlate pipeline ────────────

    #[test]
    fn full_pipeline_extract_align_correlate() {
        // Two equity curves with known relationship
        let curve_a = make_equity_ymd(&[
            (2024, 1, 2, 10000.0),
            (2024, 1, 3, 10100.0),  // +1%
            (2024, 1, 4, 10302.0),  // +2%
            (2024, 1, 5, 10508.04), // +2%
            (2024, 1, 8, 10613.12), // +1%
        ]);
        let curve_b = make_equity_ymd(&[
            (2024, 1, 2, 5000.0),
            (2024, 1, 3, 5050.0),  // +1%
            (2024, 1, 4, 5151.0),  // +2%
            (2024, 1, 5, 5254.02), // +2%
            (2024, 1, 8, 5306.56), // +1%
        ]);

        let returns_a = extract_daily_returns(&curve_a);
        let returns_b = extract_daily_returns(&curve_b);
        assert_eq!(returns_a.len(), 4);
        assert_eq!(returns_b.len(), 4);

        let aligned = align_return_streams(&[returns_a, returns_b]);
        assert_eq!(aligned.len(), 2);
        assert_eq!(aligned[0].len(), 4);

        let labels = vec!["A".to_string(), "B".to_string()];
        let corr = compute_correlation_matrix(&aligned, &labels);
        assert_eq!(corr.len(), 1);
        // Both curves have nearly identical return patterns → high correlation
        assert!(
            corr[0].correlation > 0.99,
            "parallel equity curves should be highly correlated, got {}",
            corr[0].correlation
        );
    }
}
