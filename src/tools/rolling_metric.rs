//! Rolling metric computation over OHLCV price data.

use anyhow::{Context, Result};
use std::sync::Arc;

use crate::data::cache::CachedStore;
use crate::stats;
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

    // Compute daily returns
    let returns: Vec<f64> = resp
        .prices
        .windows(2)
        .map(|w| {
            if w[0].close == 0.0 {
                0.0
            } else {
                (w[1].close - w[0].close) / w[0].close
            }
        })
        .collect();
    let dates: Vec<String> = resp.prices[1..].iter().map(|p| p.date.clone()).collect();

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
            stats::rolling_apply(&returns, window, |w| stats::std_dev(w) * annualization)
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

    let summary = format!(
        "Rolling {window}-day {metric} for {upper}: current={current:.4}, mean={s_mean:.4}, \
         range=[{s_min:.4}, {s_max:.4}], trend={trend}.",
    );

    let suggested_next_steps = vec![
        format!(
            "[NEXT] Call distribution(source={{\"type\":\"price_returns\",\"symbol\":\"{upper}\"}}) for return distribution"
        ),
        format!(
            "[THEN] Call regime_detect(symbol=\"{upper}\") to identify market regimes"
        ),
    ];

    Ok(RollingMetricResponse {
        summary,
        symbol: upper,
        metric: metric.to_string(),
        window,
        n_observations: valid_values.len(),
        stats: rolling_stats,
        series,
        suggested_next_steps,
    })
}

/// Rolling paired computation (e.g., for beta, correlation) filtering NaN benchmark values.
fn rolling_paired<F>(asset: &[f64], bench: &[f64], window: usize, f: F) -> Vec<f64>
where
    F: Fn(&[f64], &[f64]) -> f64,
{
    let n = asset.len().min(bench.len());
    let mut result = vec![f64::NAN; n];
    for i in (window - 1)..n {
        let start = i + 1 - window;
        let a_win = &asset[start..=i];
        let b_win = &bench[start..=i];
        // Filter pairs where benchmark is NaN
        let (a_valid, b_valid): (Vec<f64>, Vec<f64>) = a_win
            .iter()
            .zip(b_win.iter())
            .filter(|(_, b)| b.is_finite())
            .map(|(&a, &b)| (a, b))
            .unzip();
        if a_valid.len() >= 2 {
            result[i] = f(&a_valid, &b_valid);
        }
    }
    result
}
