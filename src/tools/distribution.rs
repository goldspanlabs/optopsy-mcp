//! Distribution analysis tool: compute descriptive stats, histogram, and normality tests.

use anyhow::{Context, Result};
use std::sync::Arc;

use crate::data::cache::CachedStore;
use crate::stats;
use crate::tools::ai_format;
use crate::tools::response_types::DistributionSource;
use crate::tools::response_types::{DistributionResponse, HistogramBin, NormalityTest, TailRatio};

/// Execute the distribution analysis.
#[allow(clippy::too_many_lines)]
pub async fn execute(
    cache: &Arc<CachedStore>,
    source: &DistributionSource,
    n_bins: usize,
) -> Result<DistributionResponse> {
    let (raw_values, source_label, source_symbol): (Vec<f64>, String, Option<String>) = match source
    {
        DistributionSource::PriceReturns { symbol, years } => {
            let upper = symbol.to_uppercase();
            let cutoff =
                chrono::Utc::now().date_naive() - chrono::Duration::days(i64::from(*years) * 365);
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

            let returns: Vec<f64> = resp
                .prices
                .windows(2)
                .filter_map(|w| {
                    if w[0].close == 0.0 {
                        None
                    } else {
                        Some((w[1].close - w[0].close) / w[0].close * 100.0)
                    }
                })
                .collect();

            (returns, format!("{upper} daily returns (%)"), Some(upper))
        }
        DistributionSource::TradePnl { values, label } => {
            if values.is_empty() {
                anyhow::bail!("trade_pnl values array is empty");
            }
            (values.clone(), label.clone(), None)
        }
    };

    // Filter to finite values only — NaN/Inf propagates through all stats functions
    let values: Vec<f64> = raw_values.into_iter().filter(|v| v.is_finite()).collect();
    if values.len() < 2 {
        anyhow::bail!(
            "Insufficient finite observations in {source_label}: need at least 2, \
             got {} (check for all-NaN or all-Inf input)",
            values.len()
        );
    }

    let n = values.len();
    let m = stats::mean(&values);
    let sd = stats::std_dev(&values);
    let md = stats::median(&values);
    let sk = stats::skewness(&values);
    let kt = stats::kurtosis(&values);
    let min_val = values.iter().copied().fold(f64::INFINITY, f64::min);
    let max_val = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let p5 = stats::percentile(&values, 5.0);
    let p25 = stats::percentile(&values, 25.0);
    let p75 = stats::percentile(&values, 75.0);
    let p95 = stats::percentile(&values, 95.0);

    // Histogram
    let hist = stats::histogram(&values, n_bins);
    let histogram: Vec<HistogramBin> = hist
        .into_iter()
        .map(|b| HistogramBin {
            lower: b.lower,
            upper: b.upper,
            count: b.count,
            frequency: b.frequency,
        })
        .collect();

    // Normality test (Jarque-Bera)
    let normality = stats::jarque_bera(&values).map(|r| NormalityTest {
        test_name: "Jarque-Bera".to_string(),
        statistic: r.statistic,
        p_value: r.p_value,
        is_normal: r.p_value > 0.05,
    });

    // Tail ratio
    let tail_ratio = if sd > 0.0 {
        let left_threshold = m - 2.0 * sd;
        let right_threshold = m + 2.0 * sd;
        let left_count = values.iter().filter(|&&v| v < left_threshold).count();
        let right_count = values.iter().filter(|&&v| v > right_threshold).count();
        let left_pct = left_count as f64 / n as f64 * 100.0;
        let right_pct = right_count as f64 / n as f64 * 100.0;
        let ratio = if right_count > 0 {
            left_count as f64 / right_count as f64
        } else if left_count > 0 {
            f64::INFINITY
        } else {
            1.0
        };
        let interpretation = if ratio > 1.5 {
            "Left-skewed tails (more extreme losses than gains)".to_string()
        } else if ratio < 0.67 {
            "Right-skewed tails (more extreme gains than losses)".to_string()
        } else {
            "Roughly symmetric tails".to_string()
        };
        Some(TailRatio {
            left_tail_pct: left_pct,
            right_tail_pct: right_pct,
            ratio: if ratio.is_finite() { ratio } else { 999.99 },
            interpretation,
        })
    } else {
        None
    };

    Ok(ai_format::format_distribution(
        source_label,
        source_symbol.as_deref(),
        n,
        m,
        sd,
        md,
        sk,
        kt,
        min_val,
        max_val,
        p5,
        p25,
        p75,
        p95,
        histogram,
        normality,
        tail_ratio,
    ))
}

#[cfg(test)]
mod tests {
    use crate::stats;

    /// Build a synthetic normal-ish return series for testing.
    fn synthetic_returns(n: usize) -> Vec<f64> {
        // Simple deterministic series: sin-based with some variation
        (0..n)
            .map(|i| {
                let x = i as f64 * 0.1;
                x.sin() * 0.5 + (i % 7) as f64 * 0.1 - 0.3
            })
            .collect()
    }

    #[test]
    fn test_finite_filter_removes_nan() {
        let raw: Vec<f64> = vec![1.0, f64::NAN, 2.0, f64::INFINITY, 3.0, f64::NEG_INFINITY];
        let filtered: Vec<f64> = raw.into_iter().filter(|v| v.is_finite()).collect();
        assert_eq!(filtered, vec![1.0, 2.0, 3.0]);
        assert_eq!(filtered.len(), 3);
    }

    #[test]
    fn test_stats_on_known_data() {
        let values = vec![1.0_f64, 2.0, 3.0, 4.0, 5.0];
        let m = stats::mean(&values);
        assert!((m - 3.0).abs() < 1e-10, "mean should be 3.0, got {m}");

        let md = stats::median(&values);
        assert!((md - 3.0).abs() < 1e-10, "median should be 3.0, got {md}");

        let sd = stats::std_dev(&values);
        // Population std dev ≈ 1.4142
        assert!(sd > 1.0 && sd < 2.0, "std_dev out of range: {sd}");
    }

    #[test]
    fn test_histogram_bin_count() {
        let values = synthetic_returns(100);
        let hist = stats::histogram(&values, 10);
        assert_eq!(hist.len(), 10, "should have exactly 10 bins");
        let total: usize = hist.iter().map(|b| b.count).sum();
        assert_eq!(
            total,
            values.len(),
            "bin counts should sum to total observations"
        );
    }

    #[test]
    fn test_percentiles_ordered() {
        let values = synthetic_returns(200);
        let p5 = stats::percentile(&values, 5.0);
        let p25 = stats::percentile(&values, 25.0);
        let p75 = stats::percentile(&values, 75.0);
        let p95 = stats::percentile(&values, 95.0);
        assert!(p5 <= p25, "p5={p5} should be <= p25={p25}");
        assert!(p25 <= p75, "p25={p25} should be <= p75={p75}");
        assert!(p75 <= p95, "p75={p75} should be <= p95={p95}");
    }

    #[test]
    fn test_min_max_correct() {
        let values = [-3.0_f64, -1.0, 0.0, 2.0, 5.0];
        let min_val = values.iter().copied().fold(f64::INFINITY, f64::min);
        let max_val = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        assert!((min_val - (-3.0)).abs() < 1e-10);
        assert!((max_val - 5.0).abs() < 1e-10);
    }

    #[test]
    fn test_tail_ratio_symmetric() {
        // Standard normal-ish: ~5% each tail → ratio ≈ 1
        let values: Vec<f64> = (-50..=50).map(|i| f64::from(i) * 0.04).collect();
        let m = stats::mean(&values);
        let sd = stats::std_dev(&values);
        let left = values.iter().filter(|&&v| v < m - 2.0 * sd).count();
        let right = values.iter().filter(|&&v| v > m + 2.0 * sd).count();
        // For linear series, left == right (symmetric)
        assert_eq!(left, right, "should be symmetric for uniform linear series");
    }
}
