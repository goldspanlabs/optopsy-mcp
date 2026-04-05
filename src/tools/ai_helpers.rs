//! Shared helper functions and threshold constants for AI-enriched tool responses.
//!
//! Centralises assessment logic (Sharpe tiers, p-value interpretation, data quality
//! warnings) so that all formatting modules use consistent language and thresholds.

use anyhow::Context;
use std::sync::Arc;

use crate::constants::CALENDAR_DAYS_PER_YEAR;
use crate::data::cache::CachedStore;

use super::response_types::PriceBar;

/// Convert an epoch timestamp (seconds) to a `YYYY-MM-DD` date string.
///
/// Returns the raw integer as a string if the timestamp is out of range.
pub(crate) fn epoch_to_date_string(epoch: i64) -> String {
    chrono::DateTime::from_timestamp(epoch, 0).map_or_else(
        || format!("{epoch}"),
        |dt| dt.naive_utc().format("%Y-%m-%d").to_string(),
    )
}

/// Format an epoch timestamp as date-only or full datetime depending on interval.
///
/// For intraday intervals, returns `YYYY-MM-DD HH:MM` to avoid collapsing
/// multiple bars onto the same date label. For daily and above, returns `YYYY-MM-DD`.
pub(crate) fn epoch_to_timestamp_string(
    epoch: i64,
    interval: crate::engine::types::Interval,
) -> String {
    if interval.is_intraday() {
        chrono::DateTime::from_timestamp(epoch, 0).map_or_else(
            || format!("{epoch}"),
            |dt| dt.naive_utc().format("%Y-%m-%d %H:%M").to_string(),
        )
    } else {
        epoch_to_date_string(epoch)
    }
}

// ── Shared utility helpers ──────────────────────────────────────────────────

/// Compute a date cutoff string (YYYY-MM-DD) going back `years` from today.
pub(crate) fn compute_years_cutoff(years: u32) -> String {
    let cutoff = chrono::Utc::now().date_naive()
        - chrono::Duration::days(i64::from(years) * CALENDAR_DAYS_PER_YEAR);
    cutoff.format("%Y-%m-%d").to_string()
}

/// Evenly subsample a vector down to at most `max` elements.
pub(crate) fn subsample_to_max<T: Clone>(data: Vec<T>, max: usize) -> Vec<T> {
    let n = data.len();
    if n <= max {
        return data;
    }
    let mut indices: Vec<usize> = (0..max).map(|i| i * (n - 1) / (max - 1)).collect();
    indices.dedup();
    indices.iter().map(|&i| data[i].clone()).collect()
}

/// Compute simple returns and corresponding dates from a price series.
///
/// Emits `NaN` for bars where the prior close is zero, preserving index alignment.
pub(crate) fn compute_returns(prices: &[PriceBar]) -> (Vec<f64>, Vec<i64>) {
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
    let dates: Vec<i64> = prices[1..].iter().map(|p| p.date).collect();
    (returns, dates)
}

/// Load OHLCV prices for a symbol with a year-based cutoff and minimum-bars check.
///
/// Consolidates the repeated boilerplate: uppercase symbol → date cutoff → load → min check.
pub(crate) async fn load_prices(
    cache: &Arc<CachedStore>,
    symbol: &str,
    years: u32,
    min_bars: usize,
    interval: crate::engine::types::Interval,
) -> anyhow::Result<Vec<PriceBar>> {
    let upper = symbol.to_uppercase();
    let cutoff_str = compute_years_cutoff(years);

    let resp = crate::tools::raw_prices::load_and_execute(
        cache,
        &upper,
        Some(&cutoff_str),
        None,
        None,
        interval,
        None,
    )
    .await
    .context(format!("Failed to load OHLCV data for {upper}"))?;

    if resp.prices.len() < min_bars {
        anyhow::bail!(
            "Insufficient price data for {upper}: need at least {min_bars} bars, have {}",
            resp.prices.len()
        );
    }

    Ok(resp.prices)
}

/// Load daily OHLCV prices for `symbol` starting from `cutoff_str` and compute
/// simple returns, filtering out zero-price bars and non-finite values.
pub(crate) async fn load_returns(
    cache: &Arc<CachedStore>,
    symbol: &str,
    cutoff_str: &str,
) -> anyhow::Result<Vec<f64>> {
    let resp = crate::tools::raw_prices::load_and_execute(
        cache,
        symbol,
        Some(cutoff_str),
        None,
        None,
        crate::engine::types::Interval::Daily,
        None,
    )
    .await
    .context(format!("Failed to load OHLCV data for {symbol}"))?;

    let returns: Vec<f64> = resp
        .prices
        .windows(2)
        .filter_map(|w| {
            if w[0].close == 0.0 {
                None
            } else {
                Some((w[1].close - w[0].close) / w[0].close)
            }
        })
        .filter(|r| r.is_finite())
        .collect();

    Ok(returns)
}

/// Compute p-value for a Pearson correlation coefficient.
pub(crate) fn pearson_p_value(r: f64, n: usize) -> Option<f64> {
    if n <= 2 {
        return None;
    }
    let r_sq = r * r;
    let denom = (1.0 - r_sq).max(0.0);
    if denom < f64::EPSILON {
        Some(0.0)
    } else {
        let t_stat = r * ((n as f64 - 2.0) / denom).sqrt();
        statrs::distribution::StudentsT::new(0.0, 1.0, (n - 2) as f64)
            .ok()
            .map(|d| {
                use statrs::distribution::ContinuousCDF;
                2.0 * (1.0 - d.cdf(t_stat.abs()))
            })
    }
}

/// Load returns for multiple symbols, align to common length (from end), and
/// return `(upper_symbols, aligned_returns)`.
///
/// Each return series is trimmed from the end so all have the same number of
/// observations. Fails if any symbol has fewer than `min_bars` observations.
pub(crate) async fn load_aligned_returns(
    cache: &Arc<CachedStore>,
    symbols: &[String],
    years: u32,
    min_bars: usize,
) -> anyhow::Result<(Vec<String>, Vec<Vec<f64>>)> {
    let cutoff_str = compute_years_cutoff(years);
    let mut all_returns: Vec<Vec<f64>> = Vec::with_capacity(symbols.len());
    let mut upper_symbols: Vec<String> = Vec::with_capacity(symbols.len());

    for sym in symbols {
        let upper = sym.to_uppercase();
        let returns = load_returns(cache, &upper, &cutoff_str).await?;
        if returns.len() < min_bars {
            anyhow::bail!(
                "Insufficient data for {upper}: {} observations (need {min_bars})",
                returns.len()
            );
        }
        all_returns.push(returns);
        upper_symbols.push(upper);
    }

    let min_len = all_returns.iter().map(Vec::len).min().unwrap_or(0);
    if min_len < min_bars {
        anyhow::bail!("Insufficient aligned observations: {min_len} (need at least {min_bars})");
    }

    // Trim all series to common length from the end (most recent data)
    let aligned: Vec<Vec<f64>> = all_returns
        .into_iter()
        .map(|r| {
            let start = r.len() - min_len;
            r[start..].to_vec()
        })
        .collect();

    Ok((upper_symbols, aligned))
}

/// Compute the mean of a slice of f64 values.
///
/// # Panics
///
/// Panics if `data` is empty.
pub(crate) fn mean(data: &[f64]) -> f64 {
    assert!(!data.is_empty(), "mean requires non-empty data");
    data.iter().sum::<f64>() / data.len() as f64
}

/// Compute sample variance (N-1 denominator) of a slice.
///
/// # Panics
///
/// Panics if `data` has fewer than 2 elements.
pub(crate) fn variance(data: &[f64]) -> f64 {
    assert!(data.len() >= 2, "variance requires at least 2 observations");
    let m = mean(data);
    data.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (data.len() - 1) as f64
}

/// Compute annualized covariance matrix from daily return series.
///
/// All series must have the same length and at least 2 observations.
/// `aligned` must be non-empty. Returns an n×n matrix (Vec of Vecs).
///
/// # Panics
///
/// Panics if `aligned` is empty or any series has fewer than 2 elements.
pub(crate) fn covariance_matrix(
    aligned: &[Vec<f64>],
    means: &[f64],
    annualization_factor: f64,
) -> Vec<Vec<f64>> {
    assert!(!aligned.is_empty(), "covariance_matrix requires non-empty aligned series");
    let n_assets = aligned.len();
    let n_obs = aligned[0].len();
    assert!(n_obs >= 2, "covariance_matrix requires at least 2 observations");
    let mut cov = vec![vec![0.0_f64; n_assets]; n_assets];
    for i in 0..n_assets {
        for j in i..n_assets {
            let c: f64 = aligned[i]
                .iter()
                .zip(aligned[j].iter())
                .map(|(a, b)| (a - means[i]) * (b - means[j]))
                .sum::<f64>()
                / (n_obs - 1) as f64
                * annualization_factor;
            cov[i][j] = c;
            cov[j][i] = c;
        }
    }
    cov
}

/// Parse a date string parameter with a descriptive error.
pub(crate) fn parse_date_param(
    date_str: &str,
    param_name: &str,
) -> anyhow::Result<chrono::NaiveDate> {
    date_str
        .parse::<chrono::NaiveDate>()
        .with_context(|| format!("Invalid {param_name}: {date_str}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::types::Interval;

    // ─── epoch_to_date_string ───────────────────────────────────────────

    #[test]
    fn epoch_to_date_string_known_date() {
        // 2020-01-01 00:00:00 UTC
        assert_eq!(epoch_to_date_string(1_577_836_800), "2020-01-01");
    }

    #[test]
    fn epoch_to_date_string_with_time_component() {
        // 2024-06-15 14:30:00 UTC — time should be discarded
        assert_eq!(epoch_to_date_string(1_718_461_800), "2024-06-15");
    }

    // ─── epoch_to_timestamp_string ──────────────────────────────────────

    #[test]
    fn timestamp_string_daily_returns_date_only() {
        let epoch = 1_718_461_800; // 2024-06-15 14:30:00 UTC
        let result = epoch_to_timestamp_string(epoch, Interval::Daily);
        assert_eq!(result, "2024-06-15");
    }

    #[test]
    fn timestamp_string_midnight_intraday_shows_zeros() {
        // 2024-01-02 00:00:00 UTC
        let epoch = 1_704_153_600;
        let result = epoch_to_timestamp_string(epoch, Interval::Hour1);
        assert_eq!(result, "2024-01-02 00:00");
    }

    #[test]
    fn subsample_respects_max() {
        let data: Vec<i32> = (0..1000).collect();
        let result = subsample_to_max(data, 500);
        assert_eq!(result.len(), 500);
        assert_eq!(result[0], 0);
        assert_eq!(result[499], 999);
    }

    #[test]
    fn subsample_smaller_than_max() {
        let data: Vec<i32> = (0..100).collect();
        let result = subsample_to_max(data, 500);
        assert_eq!(
            result.len(),
            100,
            "should not change if already within limit"
        );
    }

    #[test]
    fn timestamp_string_all_non_intraday_match_date_string() {
        let epoch = 1_718_461_800;
        let date_only = epoch_to_date_string(epoch);
        for interval in [Interval::Daily, Interval::Weekly, Interval::Monthly] {
            assert_eq!(
                epoch_to_timestamp_string(epoch, interval),
                date_only,
                "non-intraday interval {interval} should match epoch_to_date_string"
            );
        }
    }

    // ─── mean / variance / covariance_matrix ───────────────────────────

    #[test]
    fn mean_basic() {
        let data = [1.0, 2.0, 3.0, 4.0, 5.0];
        assert!((mean(&data) - 3.0).abs() < 1e-10);
    }

    #[test]
    #[should_panic(expected = "mean requires non-empty data")]
    fn mean_empty_panics() {
        mean(&[]);
    }

    #[test]
    fn variance_basic() {
        let data = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let v = variance(&data);
        // Sample variance = 4.571...
        assert!((v - 4.571_428_571_428_571).abs() < 1e-6);
    }

    #[test]
    #[should_panic(expected = "variance requires at least 2 observations")]
    fn variance_single_element_panics() {
        variance(&[1.0]);
    }

    #[test]
    fn covariance_matrix_2x2_symmetric() {
        let series = vec![vec![1.0, 2.0, 3.0], vec![2.0, 4.0, 6.0]];
        let means: Vec<f64> = series.iter().map(|s| mean(s)).collect();
        let cov = covariance_matrix(&series, &means, 1.0);
        assert_eq!(cov.len(), 2);
        assert_eq!(cov[0].len(), 2);
        // Symmetry
        assert!((cov[0][1] - cov[1][0]).abs() < 1e-10);
        // Perfectly correlated: cov[0][1] should equal sqrt(var_a * var_b)
        assert!(cov[0][1] > 0.0);
    }

    #[test]
    #[should_panic(expected = "covariance_matrix requires non-empty aligned series")]
    fn covariance_matrix_empty_panics() {
        let empty: Vec<Vec<f64>> = vec![];
        covariance_matrix(&empty, &[], 1.0);
    }
}
