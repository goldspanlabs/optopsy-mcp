//! Correlation analysis between two price series.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;

use crate::data::cache::CachedStore;
use crate::stats;
use crate::tools::ai_format;
use crate::tools::response_types::CorrelationSeries;
use crate::tools::response_types::{CorrelateResponse, RollingCorrelationPoint, ScatterPoint};

/// Execute the correlate analysis.
#[allow(clippy::too_many_lines, clippy::similar_names)]
pub async fn execute(
    cache: &Arc<CachedStore>,
    series_a: &CorrelationSeries,
    series_b: &CorrelationSeries,
    mode: &str,
    window: usize,
    years: u32,
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
        )
        .await
        .context(format!("Failed to load data for {}", series_b.symbol))?;

        (a.prices, b.prices)
    };

    // Build date-indexed maps
    let map_a: HashMap<&str, usize> = prices_a
        .iter()
        .enumerate()
        .map(|(i, p)| (p.date.as_str(), i))
        .collect();

    // Align by date (inner join)
    let mut aligned_dates: Vec<String> = Vec::new();
    let mut vals_a_raw: Vec<(f64, f64, f64, f64, f64)> = Vec::new();
    let mut vals_b_raw: Vec<(f64, f64, f64, f64, f64)> = Vec::new();

    for pb in &prices_b {
        if let Some(&idx_a) = map_a.get(pb.date.as_str()) {
            let pa = &prices_a[idx_a];
            aligned_dates.push(pb.date.clone());
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
                    .map(|w| if w[0] == 0.0 { 0.0 } else { (w[1] - w[0]) / w[0] * 100.0 })
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

    // Trim to common length (guards against edge-case length mismatches)
    let n = field_a.len().min(field_b.len()).min(dates.len());
    let fa = &field_a[..n];
    let fb = &field_b[..n];

    // Compute full-period stats
    let pearson = stats::pearson(fa, fb);
    let spearman = stats::spearman(fa, fb);
    let r_squared = pearson * pearson;

    // P-value for pearson (t-test approximation)
    let p_value = if n > 2 {
        let t_stat = pearson * ((n as f64 - 2.0) / (1.0 - r_squared)).sqrt();
        let dist = statrs::distribution::StudentsT::new(0.0, 1.0, (n - 2) as f64).ok();
        dist.map(|d| {
            use statrs::distribution::ContinuousCDF;
            2.0 * (1.0 - d.cdf(t_stat.abs()))
        })
    } else {
        None
    };

    // Rolling correlation
    let rolling_correlation = if mode == "rolling" && n >= window {
        let mut points = Vec::with_capacity(n - window + 1);
        for i in (window - 1)..n {
            let start = i + 1 - window;
            let r = stats::pearson(&fa[start..=i], &fb[start..=i]);
            points.push(RollingCorrelationPoint {
                date: dates[i].clone(),
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
                date: dates[i].clone(),
            })
            .collect();
        subsample(all, 500)
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

    /// Compute returns from a price series.
    fn to_returns(prices: &[f64]) -> Vec<f64> {
        prices
            .windows(2)
            .map(|w| {
                if w[0] == 0.0 {
                    0.0
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
}
