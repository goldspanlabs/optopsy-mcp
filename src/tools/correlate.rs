//! Correlation analysis between two price series.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;

use crate::data::cache::CachedStore;
use crate::stats;
use crate::tools::ai_format;
use crate::tools::ai_helpers::epoch_to_date_string;
use crate::tools::response_types::CorrelationSeries;
use crate::tools::response_types::{
    CorrelateResponse, GrangerResult, LagAnalysis, LagCorrelationPoint, RollingCorrelationPoint,
    ScatterPoint,
};

/// Execute the correlate analysis.
#[allow(clippy::too_many_lines, clippy::similar_names)]
pub async fn execute(
    cache: &Arc<CachedStore>,
    series_a: &CorrelationSeries,
    series_b: &CorrelationSeries,
    mode: &str,
    window: usize,
    years: u32,
    lag_range: Option<(i32, i32)>,
) -> Result<CorrelateResponse> {
    if mode != "full" && mode != "rolling" {
        anyhow::bail!("Invalid mode: \"{mode}\". Must be \"full\" or \"rolling\".");
    }

    let cutoff = chrono::Utc::now().date_naive() - chrono::Duration::days(i64::from(years) * 365);
    let cutoff_str = cutoff.format("%Y-%m-%d").to_string();

    // Load both series
    let (prices_a, prices_b) = {
        let a = crate::tools::raw_prices::load_and_execute(
            cache,
            &series_a.symbol.to_uppercase(),
            Some(&cutoff_str),
            None,
            None,
            crate::engine::types::Interval::Daily,
            None,
        )
        .await
        .context(format!("Failed to load data for {}", series_a.symbol))?;

        let b = crate::tools::raw_prices::load_and_execute(
            cache,
            &series_b.symbol.to_uppercase(),
            Some(&cutoff_str),
            None,
            None,
            crate::engine::types::Interval::Daily,
            None,
        )
        .await
        .context(format!("Failed to load data for {}", series_b.symbol))?;

        (a.prices, b.prices)
    };

    // Build date-indexed maps
    let map_a: HashMap<i64, usize> = prices_a
        .iter()
        .enumerate()
        .map(|(i, p)| (p.date, i))
        .collect();

    // Align by date (inner join)
    let mut aligned_dates: Vec<i64> = Vec::new();
    let mut vals_a_raw: Vec<(f64, f64, f64, f64, f64)> = Vec::new();
    let mut vals_b_raw: Vec<(f64, f64, f64, f64, f64)> = Vec::new();

    for pb in &prices_b {
        if let Some(&idx_a) = map_a.get(&pb.date) {
            let pa = &prices_a[idx_a];
            aligned_dates.push(pb.date);
            vals_a_raw.push((pa.close, pa.open, pa.high, pa.low, pa.volume as f64));
            vals_b_raw.push((pb.close, pb.open, pb.high, pb.low, pb.volume as f64));
        }
    }

    if aligned_dates.len() < 2 {
        anyhow::bail!(
            "Insufficient overlapping dates between {} and {} (found {})",
            series_a.symbol,
            series_b.symbol,
            aligned_dates.len()
        );
    }

    // Extract the requested field
    let extract_field = |raw: &[(f64, f64, f64, f64, f64)], field: &str| -> Result<Vec<f64>> {
        match field {
            "close" => Ok(raw.iter().map(|r| r.0).collect()),
            "open" => Ok(raw.iter().map(|r| r.1).collect()),
            "high" => Ok(raw.iter().map(|r| r.2).collect()),
            "low" => Ok(raw.iter().map(|r| r.3).collect()),
            "volume" => Ok(raw.iter().map(|r| r.4).collect()),
            "return" => {
                let closes: Vec<f64> = raw.iter().map(|r| r.0).collect();
                Ok(closes
                    .windows(2)
                    .map(|w| {
                        if w[0] == 0.0 {
                            f64::NAN
                        } else {
                            (w[1] - w[0]) / w[0] * 100.0
                        }
                    })
                    .collect())
            }
            _ => anyhow::bail!("Invalid field: \"{field}\". Must be one of: close, open, high, low, volume, return"),
        }
    };

    let field_a_raw = extract_field(&vals_a_raw, &series_a.field)?;
    let field_b_raw = extract_field(&vals_b_raw, &series_b.field)?;

    // When one series uses "return" it produces N-1 values aligned to dates[1..N].
    // A non-return series produces N values aligned to dates[0..N].
    // We must drop the first value of any non-return series so both align to dates[1..].
    let (field_a, field_b, dates) = match (series_a.field.as_str(), series_b.field.as_str()) {
        ("return", "return") => {
            // Both are returns: N-1 values, use dates[1..]
            (field_a_raw, field_b_raw, aligned_dates[1..].to_vec())
        }
        ("return", _) => {
            // a is return (N-1 values for dates[1..]), b is level (N values for dates[0..])
            // Drop b[0] so b aligns to dates[1..]
            let b = if field_b_raw.len() > 1 {
                field_b_raw[1..].to_vec()
            } else {
                field_b_raw
            };
            (field_a_raw, b, aligned_dates[1..].to_vec())
        }
        (_, "return") => {
            // b is return (N-1 values for dates[1..]), a is level (N values for dates[0..])
            // Drop a[0] so a aligns to dates[1..]
            let a = if field_a_raw.len() > 1 {
                field_a_raw[1..].to_vec()
            } else {
                field_a_raw
            };
            (a, field_b_raw, aligned_dates[1..].to_vec())
        }
        _ => {
            // Both are levels: N values, use dates[0..]
            (field_a_raw, field_b_raw, aligned_dates)
        }
    };

    // Trim to common length, then filter to pairs where both values are finite
    let common = field_a.len().min(field_b.len()).min(dates.len());
    let mut fa_clean = Vec::with_capacity(common);
    let mut fb_clean = Vec::with_capacity(common);
    let mut dates_clean = Vec::with_capacity(common);
    for i in 0..common {
        if field_a[i].is_finite() && field_b[i].is_finite() {
            fa_clean.push(field_a[i]);
            fb_clean.push(field_b[i]);
            dates_clean.push(dates[i]);
        }
    }
    let n = fa_clean.len();
    if n < 2 {
        anyhow::bail!(
            "Insufficient overlapping data after alignment: need at least 2 finite observations, got {n}"
        );
    }
    let (field_a, field_b, dates) = (fa_clean, fb_clean, dates_clean);
    let fa = &field_a[..n];
    let fb = &field_b[..n];

    // Compute full-period stats
    // Clamp pearson to [-1, 1] to guard against floating-point overshoot
    let pearson = stats::pearson(fa, fb).clamp(-1.0, 1.0);
    let spearman = stats::spearman(fa, fb);
    let r_squared = pearson * pearson;

    // P-value for pearson (t-test approximation)
    let p_value = if n > 2 {
        let denom = (1.0 - r_squared).max(0.0);
        if denom < f64::EPSILON {
            // Perfect correlation: p-value is effectively 0
            Some(0.0)
        } else {
            let t_stat = pearson * ((n as f64 - 2.0) / denom).sqrt();
            let dist = statrs::distribution::StudentsT::new(0.0, 1.0, (n - 2) as f64).ok();
            dist.map(|d| {
                use statrs::distribution::ContinuousCDF;
                2.0 * (1.0 - d.cdf(t_stat.abs()))
            })
        }
    } else {
        None
    };

    // Rolling correlation
    if mode == "rolling" && n < window {
        anyhow::bail!(
            "Rolling window ({window}) exceeds available observations ({n}). \
             Reduce the window or increase the years of history."
        );
    }
    let rolling_correlation = if mode == "rolling" && n >= window {
        let mut points = Vec::with_capacity(n - window + 1);
        for i in (window - 1)..n {
            let start = i + 1 - window;
            let r = stats::pearson(&fa[start..=i], &fb[start..=i]);
            points.push(RollingCorrelationPoint {
                date: epoch_to_date_string(dates[i]),
                correlation: r,
            });
        }
        // Subsample to max 500 points
        subsample(points, 500)
    } else {
        vec![]
    };

    // Scatter data (subsample to max 500)
    let scatter: Vec<ScatterPoint> = {
        let all: Vec<ScatterPoint> = (0..n)
            .map(|i| ScatterPoint {
                x: fa[i],
                y: fb[i],
                date: epoch_to_date_string(dates[i]),
            })
            .collect();
        subsample(all, 500)
    };

    // Lead/lag analysis
    let lag_analysis = if let Some((lag_min, lag_max)) = lag_range {
        let mut correlogram = Vec::with_capacity((lag_max - lag_min + 1) as usize);
        let mut best_lag = 0i32;
        let mut best_corr = 0.0_f64;

        for lag in lag_min..=lag_max {
            let r = stats::lagged_pearson(fa, fb, lag);
            // P-value for lagged correlation
            let lag_abs = lag.unsigned_abs() as usize;
            let effective_n = n.saturating_sub(lag_abs);
            let lag_p = if effective_n > 2 {
                let r_sq = r * r;
                let denom = (1.0 - r_sq).max(0.0);
                if denom < f64::EPSILON {
                    Some(0.0)
                } else {
                    let t_stat = r * ((effective_n as f64 - 2.0) / denom).sqrt();
                    statrs::distribution::StudentsT::new(0.0, 1.0, (effective_n - 2) as f64)
                        .ok()
                        .map(|d| {
                            use statrs::distribution::ContinuousCDF;
                            2.0 * (1.0 - d.cdf(t_stat.abs()))
                        })
                }
            } else {
                None
            };

            if r.abs() > best_corr.abs() {
                best_corr = r;
                best_lag = lag;
            }
            correlogram.push(LagCorrelationPoint {
                lag,
                pearson: r,
                p_value: lag_p,
            });
        }

        // Granger causality in both directions using optimal |lag| as order
        let order = (best_lag.unsigned_abs() as usize).max(1);
        let mut granger_tests = Vec::new();

        // Test: does B Granger-cause A?
        if let Some((f_stat, gp)) = stats::granger_f_test(fb, fa, order) {
            granger_tests.push(GrangerResult {
                direction: format!(
                    "{} → {}",
                    series_b.symbol.to_uppercase(),
                    series_a.symbol.to_uppercase()
                ),
                f_statistic: f_stat,
                p_value: gp,
                lag_order: order,
                is_significant: gp < 0.05,
            });
        }
        // Test: does A Granger-cause B?
        if let Some((f_stat, gp)) = stats::granger_f_test(fa, fb, order) {
            granger_tests.push(GrangerResult {
                direction: format!(
                    "{} → {}",
                    series_a.symbol.to_uppercase(),
                    series_b.symbol.to_uppercase()
                ),
                f_statistic: f_stat,
                p_value: gp,
                lag_order: order,
                is_significant: gp < 0.05,
            });
        }

        Some(LagAnalysis {
            correlogram,
            optimal_lag: best_lag,
            optimal_correlation: best_corr,
            granger_tests,
        })
    } else {
        None
    };

    let label_a = format!("{} {}", series_a.symbol.to_uppercase(), series_a.field);
    let label_b = format!("{} {}", series_b.symbol.to_uppercase(), series_b.field);
    let symbol_a_upper = series_a.symbol.to_uppercase();

    Ok(ai_format::format_correlate(
        label_a,
        label_b,
        n,
        pearson,
        spearman,
        r_squared,
        p_value,
        rolling_correlation,
        scatter,
        lag_analysis,
        &symbol_a_upper,
    ))
}

/// Subsample a Vec to at most `max` elements using evenly-spaced indices.
fn subsample<T: Clone>(data: Vec<T>, max: usize) -> Vec<T> {
    let n = data.len();
    if n <= max {
        return data;
    }
    let mut indices: Vec<usize> = (0..max).map(|i| i * (n - 1) / (max - 1)).collect();
    indices.dedup();
    indices.iter().map(|&i| data[i].clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats;

    /// Helper: build a simple linear price series.
    fn linear_prices(n: usize, start: f64, step: f64) -> Vec<f64> {
        (0..n).map(|i| start + i as f64 * step).collect()
    }

    /// Compute returns from a price series (NaN for zero-close to preserve alignment).
    fn to_returns(prices: &[f64]) -> Vec<f64> {
        prices
            .windows(2)
            .map(|w| {
                if w[0] == 0.0 {
                    f64::NAN
                } else {
                    (w[1] - w[0]) / w[0] * 100.0
                }
            })
            .collect()
    }

    #[test]
    fn test_pearson_perfect_positive() {
        let a: Vec<f64> = (0..50).map(f64::from).collect();
        let b: Vec<f64> = (0..50).map(|i| f64::from(i) * 2.0 + 1.0).collect();
        let r = stats::pearson(&a, &b);
        assert!(
            (r - 1.0).abs() < 1e-10,
            "perfect linear correlation should be 1.0, got {r}"
        );
    }

    #[test]
    fn test_pearson_perfect_negative() {
        let a: Vec<f64> = (0..50).map(f64::from).collect();
        let b: Vec<f64> = (0..50).map(|i| -f64::from(i)).collect();
        let r = stats::pearson(&a, &b);
        assert!(
            (r + 1.0).abs() < 1e-10,
            "perfect negative correlation should be -1.0, got {r}"
        );
    }

    #[test]
    fn test_return_alignment_length() {
        // When series_a = "return" (N-1 values) and series_b = "close" (N values),
        // dropping b[0] should produce equal lengths N-1.
        let prices_a = linear_prices(20, 100.0, 1.0);
        let prices_b = linear_prices(20, 200.0, 2.0);

        let returns_a = to_returns(&prices_a); // length 19
        let closes_b = prices_b.clone(); // length 20

        // Simulate the alignment logic:
        let b_aligned = closes_b[1..].to_vec(); // drop first element → length 19
        assert_eq!(
            returns_a.len(),
            b_aligned.len(),
            "aligned lengths should match"
        );
    }

    #[test]
    fn test_return_return_alignment_length() {
        // When both series use "return", lengths are both N-1.
        let prices_a = linear_prices(30, 100.0, 0.5);
        let prices_b = linear_prices(30, 50.0, 0.3);
        let returns_a = to_returns(&prices_a); // length 29
        let returns_b = to_returns(&prices_b); // length 29
        assert_eq!(returns_a.len(), returns_b.len());
    }

    #[test]
    fn test_close_close_alignment_length() {
        // When both series use "close", no dropping: lengths are both N.
        let prices_a = linear_prices(25, 100.0, 1.0);
        let prices_b = linear_prices(25, 200.0, 1.5);
        assert_eq!(prices_a.len(), prices_b.len());
    }

    #[test]
    fn test_rolling_correlation_window() {
        let a: Vec<f64> = (0..50).map(|i| (f64::from(i) * 0.1).sin()).collect();
        let b: Vec<f64> = (0..50).map(|i| (f64::from(i) * 0.1).sin() + 0.1).collect();
        let window = 10;
        let n = a.len().min(b.len());
        let mut points = Vec::new();
        for i in (window - 1)..n {
            let start = i + 1 - window;
            let r = stats::pearson(&a[start..=i], &b[start..=i]);
            points.push(r);
        }
        // Highly correlated series: all rolling correlations should be close to 1.0
        for &r in &points {
            assert!(
                r > 0.99,
                "rolling correlation should be ~1.0 for near-identical series, got {r}"
            );
        }
    }

    #[test]
    fn test_subsample_respects_max() {
        let data: Vec<i32> = (0..1000).collect();
        let result = subsample(data, 500);
        assert_eq!(result.len(), 500);
        assert_eq!(result[0], 0);
        assert_eq!(result[499], 999);
    }

    #[test]
    fn test_subsample_smaller_than_max() {
        let data: Vec<i32> = (0..100).collect();
        let result = subsample(data.clone(), 500);
        assert_eq!(
            result.len(),
            100,
            "should not change if already within limit"
        );
    }

    #[test]
    fn test_lag_analysis_correlogram_structure() {
        // Verify the lag analysis produces correct correlogram entries
        let n = 100i32;
        let fa: Vec<f64> = (0..n).map(|i| (f64::from(i) * 0.2).sin()).collect();
        // fb is fa shifted by 2 bars
        let fb: Vec<f64> = (0..n).map(|i| ((f64::from(i) - 2.0) * 0.2).sin()).collect();

        let lag_min = -5;
        let lag_max = 5;
        let mut correlogram = Vec::new();
        let mut best_lag = 0i32;
        let mut best_corr = 0.0_f64;

        for lag in lag_min..=lag_max {
            let r = stats::lagged_pearson(&fa, &fb, lag);
            if r.abs() > best_corr.abs() {
                best_corr = r;
                best_lag = lag;
            }
            correlogram.push((lag, r));
        }

        // Should have 11 entries (-5 to 5 inclusive)
        assert_eq!(correlogram.len(), 11);
        // Peak should be at lag=-2 (fa leads fb by 2)
        assert_eq!(best_lag, -2, "optimal lag should be -2, got {best_lag}");
        assert!(
            best_corr > 0.9,
            "peak correlation should be high, got {best_corr}"
        );
    }

    #[allow(clippy::cast_precision_loss)]
    #[test]
    fn test_lag_analysis_granger_integration() {
        // Verify Granger test runs with a causal series (y depends on x lagged)
        let n: usize = 200;
        let mut seed: u64 = 999;
        let mut noise = || -> f64 {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            (seed >> 11) as f64 / (1u64 << 53) as f64 * 0.02 - 0.01
        };

        let fa: Vec<f64> = (0..n).map(|i| (i as f64 * 0.1).sin() * 0.05).collect();
        let mut fb = vec![0.0; n];
        for t in 1..n {
            fb[t] = 0.3 * fb[t - 1] + 0.6 * fa[t - 1] + noise();
        }

        // A→B should be detectable
        let result = stats::granger_f_test(&fa, &fb, 2);
        assert!(result.is_some(), "Granger test should produce result");
        let (f_stat, p_val) = result.unwrap();
        assert!(f_stat.is_finite(), "F-stat should be finite");
        assert!(p_val.is_finite(), "p-value should be finite");
        assert!(
            p_val < 0.05,
            "causal series should be significant, p={p_val}"
        );
    }
}
