//! Market regime detection via volatility clustering or trend state analysis.

use anyhow::{Context, Result};
use std::sync::Arc;

use crate::data::cache::CachedStore;
use crate::stats;
use crate::tools::ai_format;
use crate::tools::response_types::{RegimeDetectResponse, RegimeInfo, RegimeSeriesPoint};

/// Execute the `regime_detect` analysis.
#[allow(clippy::too_many_lines)]
pub async fn execute(
    cache: &Arc<CachedStore>,
    symbol: &str,
    method: &str,
    n_regimes: usize,
    years: u32,
    lookback_window: usize,
) -> Result<RegimeDetectResponse> {
    let valid_methods = ["volatility_cluster", "trend_state"];
    if !valid_methods.contains(&method) {
        anyhow::bail!(
            "Invalid method: \"{method}\". Must be one of: {}",
            valid_methods.join(", ")
        );
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

    let prices = &resp.prices;

    // For trend_state, the long SMA uses lookback_window * 3 bars, so we need more data.
    let min_bars = match method {
        "trend_state" => lookback_window * 3 + 2,
        _ => lookback_window + 2,
    };
    if prices.len() < min_bars {
        anyhow::bail!(
            "Insufficient data for {upper} with method=\"{method}\": need at least {min_bars} bars, have {}",
            prices.len()
        );
    }

    // Compute daily returns; emit NaN for zero-close bars to preserve index alignment
    // with SMA arrays (critical for classify_by_trend).
    let returns: Vec<f64> = prices
        .windows(2)
        .map(|w| {
            if w[0].close == 0.0 {
                f64::NAN
            } else {
                (w[1].close - w[0].close) / w[0].close
            }
        })
        .collect();
    let dates: Vec<String> = prices[1..].iter().map(|p| p.date.clone()).collect();

    let (regime_labels, regime_names) = match method {
        "volatility_cluster" => classify_by_volatility(&returns, lookback_window, n_regimes),
        "trend_state" => classify_by_trend(prices, lookback_window, n_regimes),
        _ => unreachable!(),
    };

    // Build regime series (skip leading NaN window)
    let start_idx = regime_labels
        .iter()
        .position(|l| *l < n_regimes)
        .unwrap_or(0);

    let mut regime_series: Vec<RegimeSeriesPoint> = Vec::new();
    for i in start_idx..regime_labels.len().min(dates.len()) {
        if regime_labels[i] < n_regimes {
            regime_series.push(RegimeSeriesPoint {
                date: dates[i].clone(),
                regime: regime_names[regime_labels[i]].clone(),
            });
        }
    }

    // Subsample to max 500
    if regime_series.len() > 500 {
        let n = regime_series.len();
        let mut indices: Vec<usize> = (0..500).map(|i| i * (n - 1) / 499).collect();
        indices.dedup();
        regime_series = indices
            .into_iter()
            .map(|i| regime_series[i].clone())
            .collect();
    }

    // Per-regime statistics
    let total_classified = regime_labels.iter().filter(|&&l| l < n_regimes).count();
    if total_classified == 0 {
        anyhow::bail!(
            "No bars were classified into regimes for {upper} — \
             this usually means all returns are NaN or the data is insufficient for the chosen method."
        );
    }
    let mut regimes: Vec<RegimeInfo> = Vec::with_capacity(n_regimes);

    // Compute rolling vols once outside the loop to avoid redundant O(n) passes
    let rolling_vols = stats::rolling_apply(&returns, lookback_window, |w| {
        stats::std_dev(w) * 252.0_f64.sqrt()
    });

    for (regime_idx, name) in regime_names.iter().enumerate() {
        let regime_returns: Vec<f64> = regime_labels
            .iter()
            .enumerate()
            .filter(|(_, &l)| l == regime_idx)
            .filter_map(|(i, _)| {
                let r = returns.get(i).copied().unwrap_or(f64::NAN);
                if r.is_finite() {
                    Some(r)
                } else {
                    None
                }
            })
            .collect();

        let count = regime_returns.len();
        let m = stats::mean(&regime_returns);
        let sd = stats::std_dev(&regime_returns);

        let regime_vols: Vec<f64> = regime_labels
            .iter()
            .enumerate()
            .filter(|(_, &l)| l == regime_idx)
            .filter_map(|(i, _)| {
                let v = rolling_vols.get(i).copied().unwrap_or(f64::NAN);
                if v.is_finite() {
                    Some(v)
                } else {
                    None
                }
            })
            .collect();
        let mean_vol = stats::mean(&regime_vols);

        regimes.push(RegimeInfo {
            label: name.clone(),
            count,
            pct_of_total: if total_classified > 0 {
                count as f64 / total_classified as f64 * 100.0
            } else {
                0.0
            },
            mean_return: m * 100.0, // Convert to percentage
            std_dev: sd * 100.0,
            mean_vol: mean_vol * 100.0,
        });
    }

    // Transition matrix
    let transition_matrix = compute_transition_matrix(&regime_labels, n_regimes);

    Ok(ai_format::format_regime_detect(
        &upper,
        method,
        n_regimes,
        prices.len(),
        total_classified,
        regimes,
        transition_matrix,
        regime_series,
    ))
}

/// Classify each bar by rolling realized volatility quantiles.
fn classify_by_volatility(
    returns: &[f64],
    lookback: usize,
    n_regimes: usize,
) -> (Vec<usize>, Vec<String>) {
    let annualization = 252.0_f64.sqrt();
    let rolling_vol =
        stats::rolling_apply(returns, lookback, |w| stats::std_dev(w) * annualization);

    // Collect valid volatilities to compute quantile thresholds
    let valid_vols: Vec<f64> = rolling_vol
        .iter()
        .copied()
        .filter(|v| v.is_finite())
        .collect();

    // Compute quantile boundaries
    let mut thresholds = Vec::with_capacity(n_regimes - 1);
    for i in 1..n_regimes {
        let pct = i as f64 / n_regimes as f64 * 100.0;
        thresholds.push(stats::percentile(&valid_vols, pct));
    }

    // Assign regime labels
    let labels: Vec<usize> = rolling_vol
        .iter()
        .map(|&v| {
            if !v.is_finite() {
                return n_regimes; // sentinel for unclassified
            }
            let mut regime = 0;
            for (i, &th) in thresholds.iter().enumerate() {
                if v > th {
                    regime = i + 1;
                }
            }
            regime
        })
        .collect();

    let names: Vec<String> = match n_regimes {
        2 => vec!["Low Volatility".into(), "High Volatility".into()],
        3 => vec![
            "Low Volatility".into(),
            "Medium Volatility".into(),
            "High Volatility".into(),
        ],
        4 => vec![
            "Very Low Volatility".into(),
            "Low Volatility".into(),
            "High Volatility".into(),
            "Very High Volatility".into(),
        ],
        _ => (0..n_regimes)
            .map(|i| format!("Regime {}", i + 1))
            .collect(),
    };

    (labels, names)
}

/// Classify each bar by trend state using SMA crossover and trend strength.
fn classify_by_trend(
    prices: &[crate::tools::response_types::PriceBar],
    lookback: usize,
    n_regimes: usize,
) -> (Vec<usize>, Vec<String>) {
    let closes: Vec<f64> = prices.iter().map(|p| p.close).collect();

    // Compute short and long SMAs
    let short_window = lookback;
    let long_window = lookback * 3;
    let short_sma = stats::rolling_apply(&closes, short_window, stats::mean);
    let long_sma = stats::rolling_apply(&closes, long_window, stats::mean);

    // Returns for strength measurement; NaN for zero-close to preserve index alignment with SMAs
    let returns: Vec<f64> = closes
        .windows(2)
        .map(|w| {
            if w[0] == 0.0 {
                f64::NAN
            } else {
                (w[1] - w[0]) / w[0]
            }
        })
        .collect();

    // We need returns from index 1..n and SMAs from index 0..n
    // Labels correspond to returns (length n-1)
    let labels: Vec<usize> = (0..returns.len())
        .map(|i| {
            let sma_idx = i + 1; // offset by 1 since returns start at index 1
            let short = short_sma.get(sma_idx).copied().unwrap_or(f64::NAN);
            let long = long_sma.get(sma_idx).copied().unwrap_or(f64::NAN);

            if !short.is_finite() || !long.is_finite() {
                return n_regimes; // unclassified
            }

            // Guard against zero long SMA (prevents div-by-zero and Inf/NaN)
            if long == 0.0 {
                return n_regimes; // unclassified — treat as insufficient data
            }

            let trend_strength = (short - long) / long * 100.0;

            match n_regimes {
                2 => usize::from(trend_strength <= 0.0),
                3 => {
                    if trend_strength > 1.0 {
                        0 // Uptrend
                    } else if trend_strength < -1.0 {
                        2 // Downtrend
                    } else {
                        1 // Sideways
                    }
                }
                4 => {
                    if trend_strength > 3.0 {
                        0 // Strong uptrend
                    } else if trend_strength > 0.0 {
                        1 // Mild uptrend
                    } else if trend_strength > -3.0 {
                        2 // Mild downtrend
                    } else {
                        3 // Strong downtrend
                    }
                }
                _ => 0,
            }
        })
        .collect();

    let names: Vec<String> = match n_regimes {
        2 => vec!["Uptrend".into(), "Downtrend".into()],
        3 => vec!["Uptrend".into(), "Sideways".into(), "Downtrend".into()],
        4 => vec![
            "Strong Uptrend".into(),
            "Mild Uptrend".into(),
            "Mild Downtrend".into(),
            "Strong Downtrend".into(),
        ],
        _ => (0..n_regimes)
            .map(|i| format!("Regime {}", i + 1))
            .collect(),
    };

    (labels, names)
}

/// Compute transition probability matrix from regime label sequence.
fn compute_transition_matrix(labels: &[usize], n_regimes: usize) -> Vec<Vec<f64>> {
    let mut counts = vec![vec![0usize; n_regimes]; n_regimes];
    let mut row_totals = vec![0usize; n_regimes];

    for window in labels.windows(2) {
        let from = window[0];
        let to = window[1];
        if from < n_regimes && to < n_regimes {
            counts[from][to] += 1;
            row_totals[from] += 1;
        }
    }

    counts
        .into_iter()
        .enumerate()
        .map(|(i, row)| {
            let total = row_totals[i] as f64;
            if total == 0.0 {
                vec![0.0; n_regimes]
            } else {
                row.into_iter().map(|c| c as f64 / total).collect()
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a deterministic synthetic price series with alternating high/low volatility.
    fn synthetic_prices_alternating_vol(n: usize) -> Vec<crate::tools::response_types::PriceBar> {
        let mut prices = Vec::with_capacity(n);
        let mut close = 100.0_f64;
        for i in 0..n {
            // High vol in first half, low vol in second half
            let step = if i < n / 2 { 2.0 } else { 0.2 };
            let delta = if i % 2 == 0 { step } else { -step };
            close += delta;
            close = close.max(1.0); // avoid zero/negative
            prices.push(crate::tools::response_types::PriceBar {
                date: format!("2020-01-{:02}", (i % 28) + 1),
                open: close,
                high: close + 0.1,
                low: close - 0.1,
                close,
                adjclose: Some(close),
                volume: 1_000_000,
            });
        }
        prices
    }

    /// Build a monotonically rising price series (deterministic uptrend).
    fn rising_prices(n: usize) -> Vec<crate::tools::response_types::PriceBar> {
        (0..n)
            .map(|i| {
                let close = 100.0 + i as f64 * 0.5;
                crate::tools::response_types::PriceBar {
                    date: format!("2020-01-{:02}", (i % 28) + 1),
                    open: close,
                    high: close + 0.05,
                    low: close - 0.05,
                    close,
                    adjclose: Some(close),
                    volume: 500_000,
                }
            })
            .collect()
    }

    #[test]
    fn test_volatility_cluster_label_count() {
        let prices = synthetic_prices_alternating_vol(200);
        let returns: Vec<f64> = prices
            .windows(2)
            .map(|w| (w[1].close - w[0].close) / w[0].close)
            .collect();
        let lookback = 10;
        let n_regimes = 2;
        let (labels, names) = classify_by_volatility(&returns, lookback, n_regimes);
        assert_eq!(labels.len(), returns.len());
        assert_eq!(names.len(), n_regimes);
        // Labels must be either a valid regime index or the sentinel (n_regimes = unclassified)
        for &l in &labels {
            assert!(l <= n_regimes, "label {l} out of range");
        }
    }

    #[test]
    fn test_trend_state_rising_series() {
        let prices = rising_prices(200);
        let lookback = 10;
        let n_regimes = 2; // Uptrend or Downtrend
        let (labels, names) = classify_by_trend(&prices, lookback, n_regimes);
        assert_eq!(names[0], "Uptrend");
        assert_eq!(names[1], "Downtrend");
        // After warm-up, most bars should be classified as Uptrend (regime 0)
        let classified: Vec<usize> = labels.iter().copied().filter(|&l| l < n_regimes).collect();
        let uptrend_count = classified.iter().filter(|&&l| l == 0).count();
        assert!(
            uptrend_count as f64 / classified.len() as f64 > 0.7,
            "rising series should mostly be Uptrend, got {uptrend_count}/{} classified",
            classified.len()
        );
    }

    #[test]
    fn test_transition_matrix_row_sums() {
        // Row sums of a valid transition matrix should be ~1.0 or 0.0 (empty regime)
        let labels = vec![0, 1, 0, 1, 0, 2, 2, 1, 0, 2];
        let n_regimes = 3;
        let matrix = compute_transition_matrix(&labels, n_regimes);
        for (i, row) in matrix.iter().enumerate() {
            let s: f64 = row.iter().sum();
            assert!(
                (s - 1.0).abs() < 1e-10 || s == 0.0,
                "row {i} sum should be 1.0 or 0.0, got {s}"
            );
        }
    }

    #[test]
    fn test_trend_strength_zero_long_sma_guard() {
        // Prices of all zeros would yield long_sma = 0 → division by zero.
        // The guard should return n_regimes (unclassified) instead of Inf/NaN.
        let prices: Vec<crate::tools::response_types::PriceBar> = (0..200)
            .map(|i| crate::tools::response_types::PriceBar {
                date: format!("2020-01-{:02}", (i % 28) + 1),
                open: 0.0,
                high: 0.0,
                low: 0.0,
                close: 0.0,
                adjclose: Some(0.0),
                volume: 0,
            })
            .collect();
        let lookback = 10;
        let n_regimes = 2;
        // Should not panic even with all-zero prices
        let (labels, _names) = classify_by_trend(&prices, lookback, n_regimes);
        // All bars should be unclassified (sentinel = n_regimes)
        for &l in &labels {
            assert!(
                l <= n_regimes,
                "label {l} should be <= n_regimes={n_regimes}"
            );
        }
    }

    #[test]
    fn test_volatility_cluster_two_regimes_both_populated() {
        // With enough data and clear vol contrast, both regimes should have observations
        let prices = synthetic_prices_alternating_vol(300);
        let returns: Vec<f64> = prices
            .windows(2)
            .map(|w| (w[1].close - w[0].close) / w[0].close)
            .collect();
        let (labels, _) = classify_by_volatility(&returns, 20, 2);
        let r0 = labels.iter().filter(|&&l| l == 0).count();
        let r1 = labels.iter().filter(|&&l| l == 1).count();
        assert!(r0 > 0, "regime 0 should have observations");
        assert!(r1 > 0, "regime 1 should have observations");
    }
}
