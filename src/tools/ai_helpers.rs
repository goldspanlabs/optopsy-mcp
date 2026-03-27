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
}
