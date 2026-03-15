//! Distribution analysis tool: compute descriptive stats, histogram, and normality tests.

use anyhow::{Context, Result};
use std::sync::Arc;

use crate::data::cache::CachedStore;
use crate::stats;
use crate::tools::response_types::DistributionSource;
use crate::tools::response_types::{DistributionResponse, HistogramBin, NormalityTest, TailRatio};

/// Execute the distribution analysis.
#[allow(clippy::too_many_lines)]
pub async fn execute(
    cache: &Arc<CachedStore>,
    source: &DistributionSource,
    n_bins: usize,
) -> Result<DistributionResponse> {
    let (values, source_label): (Vec<f64>, String) = match source {
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

            (returns, format!("{upper} daily returns (%)"))
        }
        DistributionSource::TradePnl { values, label } => {
            if values.is_empty() {
                anyhow::bail!("trade_pnl values array is empty");
            }
            (values.clone(), label.clone())
        }
    };

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

    // Build summary
    let normal_text = normality
        .as_ref()
        .map_or("normality test not available", |n| {
            if n.is_normal {
                "consistent with normal distribution"
            } else {
                "significantly non-normal"
            }
        });

    let summary = format!(
        "Distribution of {source_label}: {n} observations, mean={m:.4}, std={sd:.4}, \
         skew={sk:.3}, kurtosis={kt:.3}. {normal_text}.",
    );

    let suggested_next_steps = vec![
        "[NEXT] Call aggregate_prices to check for seasonal patterns".to_string(),
        "[THEN] Call rolling_metric(metric=\"volatility\") to see how risk changes over time"
            .to_string(),
    ];

    Ok(DistributionResponse {
        summary,
        source: source_label,
        n_observations: n,
        mean: m,
        std_dev: sd,
        median: md,
        skewness: sk,
        kurtosis: kt,
        min: min_val,
        max: max_val,
        percentile_5: p5,
        percentile_25: p25,
        percentile_75: p75,
        percentile_95: p95,
        histogram,
        normality,
        tail_ratio,
        suggested_next_steps,
    })
}
