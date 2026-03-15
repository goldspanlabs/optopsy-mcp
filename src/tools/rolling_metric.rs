//! Rolling metric computation over OHLCV price data.

use anyhow::{Context, Result};
use std::sync::Arc;

use crate::data::cache::CachedStore;
use crate::stats;
use crate::tools::ai_format;
use crate::tools::response_types::{RollingMetricResponse, RollingPoint, RollingStats};

/// Execute the `rolling_metric` analysis.
#[allow(clippy::too_many_lines)]
pub async fn execute(
    cache: &Arc<CachedStore>,
    symbol: &str,
    metric: &str,
    window: usize,
    benchmark: Option<&str>,
    years: u32,
) -> Result<RollingMetricResponse> {
    let valid_metrics = [
        "volatility",
        "sharpe",
        "mean_return",
        "max_drawdown",
        "beta",
        "correlation",
    ];
    if !valid_metrics.contains(&metric) {
        anyhow::bail!(
            "Invalid metric: \"{metric}\". Must be one of: {}",
            valid_metrics.join(", ")
        );
    }
    if (metric == "beta" || metric == "correlation") && benchmark.is_none() {
        anyhow::bail!("Metric \"{metric}\" requires a benchmark symbol");
    }

    let upper = symbol.to_uppercase();
    let cutoff = chrono::Utc::now().date_naive() - chrono::Duration::days(i64::from(years) * 365);
    let cutoff_str = cutoff.format("%Y-%m-%d").to_string();

    let resp = crate::tools::raw_prices::load_and_execute(
        cache,
        &upper,
        Some(&cutoff_str),
        None,
        None,
        crate::engine::types::Interval::Daily,
    )
    .await
    .context("Failed to load OHLCV data")?;

    if resp.prices.len() < 2 {
        anyhow::bail!("Insufficient price data for {upper}");
    }

    // Compute daily returns, skipping bars where prior close is zero
    let mut returns: Vec<f64> = Vec::with_capacity(resp.prices.len());
    let mut dates: Vec<String> = Vec::with_capacity(resp.prices.len());
    for w in resp.prices.windows(2) {
        if w[0].close != 0.0 {
            returns.push((w[1].close - w[0].close) / w[0].close);
            dates.push(w[1].date.clone());
        }
    }

    // Validate window size before computing
    if window > returns.len() {
        anyhow::bail!(
            "Rolling window ({window} days) exceeds available return data ({} observations) for {upper}. \
             Reduce the window or increase the years of history.",
            returns.len()
        );
    }

    // Load benchmark if needed
    let bench_returns = if let Some(bench_sym) = benchmark {
        let bench_upper = bench_sym.to_uppercase();
        let bench_resp = crate::tools::raw_prices::load_and_execute(
            cache,
            &bench_upper,
            Some(&cutoff_str),
            None,
            None,
            crate::engine::types::Interval::Daily,
        )
        .await
        .context(format!("Failed to load benchmark data for {bench_upper}"))?;

        // Align by date
        let bench_map: std::collections::HashMap<&str, usize> = bench_resp
            .prices
            .iter()
            .enumerate()
            .map(|(i, p)| (p.date.as_str(), i))
            .collect();

        let mut aligned = vec![f64::NAN; dates.len()];
        for (i, date) in dates.iter().enumerate() {
            if let Some(&bench_idx) = bench_map.get(date.as_str()) {
                if bench_idx > 0 {
                    let prev = bench_resp.prices[bench_idx - 1].close;
                    if prev != 0.0 {
                        aligned[i] = (bench_resp.prices[bench_idx].close - prev) / prev;
                    }
                }
            }
        }
        Some(aligned)
    } else {
        None
    };

    // Compute rolling metric
    let annualization = 252.0_f64.sqrt();
    let series_values: Vec<f64> = match metric {
        "volatility" => {
            stats::rolling_apply(&returns, window, |w| {
                stats::std_dev(w) * annualization * 100.0 // annualized %
            })
        }
        "sharpe" => stats::rolling_apply(&returns, window, |w| {
            let s = stats::std_dev(w);
            if s == 0.0 {
                0.0
            } else {
                (stats::mean(w) / s) * annualization
            }
        }),
        "mean_return" => stats::rolling_apply(&returns, window, |w| {
            stats::mean(w) * 252.0 * 100.0 // annualized %
        }),
        "max_drawdown" => stats::rolling_apply(&returns, window, |w| {
            let mut equity: f64 = 1.0;
            let mut peak: f64 = 1.0;
            let mut max_dd: f64 = 0.0;
            for &r in w {
                equity *= 1.0 + r;
                peak = peak.max(equity);
                let dd = (peak - equity) / peak;
                max_dd = max_dd.max(dd);
            }
            max_dd * 100.0
        }),
        "beta" => {
            let bench = bench_returns.as_ref().unwrap();
            rolling_paired(&returns, bench, window, |asset, bench_w| {
                let cov = stats::covariance(asset, bench_w);
                let var = stats::std_dev(bench_w).powi(2);
                if var == 0.0 {
                    0.0
                } else {
                    cov / var
                }
            })
        }
        "correlation" => {
            let bench = bench_returns.as_ref().unwrap();
            rolling_paired(&returns, bench, window, |asset, bench_w| {
                stats::pearson(asset, bench_w)
            })
        }
        _ => unreachable!(),
    };

    // Build series (skip NaN leading values)
    let mut series: Vec<RollingPoint> = Vec::new();
    for (i, &val) in series_values.iter().enumerate() {
        if val.is_finite() {
            series.push(RollingPoint {
                date: dates[i].clone(),
                value: val,
            });
        }
    }

    // Subsample to max 500 points
    if series.len() > 500 {
        let n = series.len();
        let mut indices: Vec<usize> = (0..500).map(|i| i * (n - 1) / 499).collect();
        indices.dedup();
        series = indices.into_iter().map(|i| series[i].clone()).collect();
    }

    // Compute summary statistics from non-NaN values
    let valid_values: Vec<f64> = series_values
        .iter()
        .copied()
        .filter(|v| v.is_finite())
        .collect();

    if valid_values.is_empty() {
        anyhow::bail!(
            "Rolling {metric} for {upper} produced no finite values — \
             check benchmark alignment or try increasing the years of history."
        );
    }

    let current = valid_values.last().copied().unwrap_or(0.0);
    let s_mean = stats::mean(&valid_values);
    let s_min = valid_values.iter().copied().fold(f64::INFINITY, f64::min);
    let s_max = valid_values
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    let s_std = stats::std_dev(&valid_values);

    // Simple trend detection via linear regression slope
    let trend = if valid_values.len() > 10 {
        let n = valid_values.len() as f64;
        let x_mean = (n - 1.0) / 2.0;
        let y_mean = s_mean;
        let mut num = 0.0;
        let mut den = 0.0;
        for (i, &y) in valid_values.iter().enumerate() {
            let x = i as f64;
            num += (x - x_mean) * (y - y_mean);
            den += (x - x_mean).powi(2);
        }
        let slope = if den == 0.0 { 0.0 } else { num / den };
        let normalized = slope * n / (s_std.max(1e-10));
        if normalized > 1.0 {
            "rising"
        } else if normalized < -1.0 {
            "falling"
        } else {
            "flat"
        }
    } else {
        "flat"
    };

    let rolling_stats = RollingStats {
        current,
        mean: s_mean,
        min: s_min,
        max: s_max,
        std_dev: s_std,
        trend: trend.to_string(),
    };

    Ok(ai_format::format_rolling_metric(
        &upper,
        metric,
        window,
        valid_values.len(),
        rolling_stats,
        series,
    ))
}

/// Rolling paired computation (e.g., for beta, correlation) filtering NaN benchmark values.
///
/// Uses pre-allocated scratch buffers (capacity = `window`) reused across iterations
/// to avoid per-window heap allocations.
fn rolling_paired<F>(asset: &[f64], bench: &[f64], window: usize, f: F) -> Vec<f64>
where
    F: Fn(&[f64], &[f64]) -> f64,
{
    let n = asset.len().min(bench.len());
    let mut result = vec![f64::NAN; n];
    // Pre-allocate scratch buffers once; cleared and refilled each iteration.
    let mut a_buf = Vec::with_capacity(window);
    let mut b_buf = Vec::with_capacity(window);
    for i in (window - 1)..n {
        let start = i + 1 - window;
        a_buf.clear();
        b_buf.clear();
        // Filter pairs where benchmark is non-finite
        for (&a, &b) in asset[start..=i].iter().zip(bench[start..=i].iter()) {
            if b.is_finite() {
                a_buf.push(a);
                b_buf.push(b);
            }
        }
        if a_buf.len() >= 2 {
            result[i] = f(&a_buf, &b_buf);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats;

    fn flat_returns(n: usize, r: f64) -> Vec<f64> {
        vec![r; n]
    }

    #[test]
    fn test_rolling_volatility_constant_returns() {
        // Constant returns → zero volatility
        let returns = flat_returns(100, 0.001);
        let window = 21;
        let vols = stats::rolling_apply(&returns, window, |w| stats::std_dev(w) * 252.0_f64.sqrt());
        // After warm-up period, all values should be ~0
        for &v in &vols[window - 1..] {
            assert!(v < 1e-10, "constant returns should give zero vol, got {v}");
        }
    }

    #[test]
    fn test_rolling_apply_nan_prefix() {
        // First (window-1) values should be NaN
        let returns = flat_returns(50, 0.002);
        let window = 10;
        let result = stats::rolling_apply(&returns, window, stats::mean);
        for (i, val) in result.iter().enumerate().take(window - 1) {
            assert!(val.is_nan(), "index {i} should be NaN before warmup");
        }
        assert!(
            result[window - 1].is_finite(),
            "first complete window should be finite"
        );
    }

    #[test]
    fn test_rolling_paired_nan_bench_filtered() {
        // Benchmark with NaN values: those pairs should be excluded
        let asset = vec![0.01, 0.02, -0.01, 0.03, -0.02];
        let bench = vec![0.01, f64::NAN, -0.01, 0.03, f64::NAN];
        let window = 3;
        let result = rolling_paired(&asset, &bench, window, |a, b| {
            // If NaN filtering works, we only receive valid pairs
            assert!(a.iter().all(|v| v.is_finite()));
            assert!(b.iter().all(|v| v.is_finite()));
            stats::pearson(a, b)
        });
        // Index 0 and 1: not enough window → NaN
        assert!(result[0].is_nan());
        assert!(result[1].is_nan());
        // Index 2: window [0..=2], bench[1]=NaN → only 2 valid pairs (0 and 2)
        // Should produce a finite or NaN result (but not panic)
        let _ = result[2];
    }

    #[test]
    fn test_rolling_max_drawdown_no_loss() {
        // All positive returns → max drawdown = 0
        let returns = flat_returns(50, 0.005);
        let window = 21;
        let result = stats::rolling_apply(&returns, window, |w| {
            let mut equity = 1.0_f64;
            let mut peak = 1.0_f64;
            let mut max_dd = 0.0_f64;
            for &r in w {
                equity *= 1.0 + r;
                peak = peak.max(equity);
                max_dd = max_dd.max((peak - equity) / peak);
            }
            max_dd * 100.0
        });
        for &v in result.iter().skip(window - 1) {
            assert!(
                v < 1e-10,
                "no-loss series should have zero drawdown, got {v}"
            );
        }
    }

    #[test]
    fn test_valid_values_excludes_nan() {
        let series_values = [f64::NAN, f64::NAN, 0.5, 0.6, 0.7];
        let valid: Vec<f64> = series_values
            .iter()
            .copied()
            .filter(|v| v.is_finite())
            .collect();
        assert_eq!(valid.len(), 3);
        assert!((valid[0] - 0.5).abs() < 1e-10);
    }
}
