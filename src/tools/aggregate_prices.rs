//! Aggregate OHLCV price data by time dimension (day-of-week, month, quarter, year, hour-of-day).

use anyhow::{Context, Result};
use chrono::{Datelike, Timelike};
use std::sync::Arc;

use crate::constants::CALENDAR_DAYS_PER_YEAR;
use crate::data::cache::CachedStore;
use crate::server::{AggMetric, GroupBy};
use crate::stats;
use crate::tools::ai_format;
use crate::tools::response_types::{AggregateBucket, AggregatePricesResponse, DateRange};

/// Execute the `aggregate_prices` analysis.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub async fn execute(
    cache: &Arc<CachedStore>,
    symbol: &str,
    years: u32,
    group_by: GroupBy,
    metric: AggMetric,
    interval: Option<crate::engine::types::Interval>,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<AggregatePricesResponse> {
    // Resolve interval: auto-select Hour1 for hour_of_day if not specified
    let resolved_interval = interval.unwrap_or_else(|| {
        if group_by == GroupBy::HourOfDay {
            crate::engine::types::Interval::Hour1
        } else {
            crate::engine::types::Interval::Daily
        }
    });

    // Reject hour_of_day with daily-resolution intervals
    if group_by == GroupBy::HourOfDay
        && matches!(
            resolved_interval,
            crate::engine::types::Interval::Daily
                | crate::engine::types::Interval::Weekly
                | crate::engine::types::Interval::Monthly
        )
    {
        anyhow::bail!(
            "group_by=\"hour_of_day\" requires intraday data. \
             Pass interval=\"1h\", \"30m\", \"5m\", or \"1m\"."
        );
    }

    let upper = symbol.to_uppercase();

    // When no explicit date range is given, derive start_date from years so that
    // load_and_execute bypasses its 7-day intraday cutoff (which only triggers
    // when start_date is None).
    let years_start = if start_date.is_none() && end_date.is_none() && years < 50 {
        let cutoff = chrono::Utc::now().date_naive()
            - chrono::Duration::days(i64::from(years) * CALENDAR_DAYS_PER_YEAR);
        Some(cutoff.format("%Y-%m-%d").to_string())
    } else {
        None
    };
    let effective_start = start_date.or(years_start.as_deref());

    let resp = crate::tools::raw_prices::load_and_execute(
        cache,
        &upper,
        effective_start,
        end_date,
        None, // no limit
        resolved_interval,
        None, // no tail
    )
    .await
    .context("Failed to load OHLCV data")?;

    let prices = resp.prices;

    if prices.len() < 2 {
        anyhow::bail!("Insufficient price data for {upper} (need at least 2 bars)");
    }

    let total_bars = prices.len();
    let date_range = DateRange {
        start: prices.first().map(|p| p.date),
        end: prices.last().map(|p| p.date),
    };

    // Compute per-bar metric values
    let mut bar_data: Vec<(String, f64)> = Vec::with_capacity(total_bars);
    for i in 0..prices.len() {
        let epoch = prices[i].date;
        let dt = chrono::DateTime::from_timestamp(epoch, 0)
            .ok_or_else(|| anyhow::anyhow!("Invalid epoch timestamp: {epoch}"))?
            .naive_utc();
        let date = dt.date();
        let hour = if dt.time() == chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap() {
            None
        } else {
            Some(dt.time().hour())
        };

        let bucket_label = match group_by {
            GroupBy::DayOfWeek => date.format("%A").to_string(),
            GroupBy::Month => date.format("%B").to_string(),
            GroupBy::Quarter => format!("Q{}", ((date.month() - 1) / 3) + 1),
            GroupBy::Year => date.format("%Y").to_string(),
            GroupBy::HourOfDay => {
                // Midnight bars may be formatted as date-only by raw_prices,
                // so hour=None means 00:00 when using an intraday interval.
                let h = hour.unwrap_or(0);
                format!("{h:02}:00")
            }
        };

        let value = match metric {
            AggMetric::Return => {
                if i == 0 {
                    continue; // skip first bar (no previous close)
                }
                let prev_close = prices[i - 1].close;
                if prev_close == 0.0 {
                    continue;
                }
                (prices[i].close - prev_close) / prev_close * 100.0
            }
            AggMetric::Gap => {
                if i == 0 {
                    continue; // skip first bar (no previous close)
                }
                let prev_close = prices[i - 1].close;
                if prev_close == 0.0 {
                    continue;
                }
                (prices[i].open - prev_close) / prev_close * 100.0
            }
            AggMetric::Range => {
                if prices[i].low == 0.0 {
                    continue;
                }
                (prices[i].high - prices[i].low) / prices[i].low * 100.0
            }
            AggMetric::Volume => prices[i].volume as f64,
        };
        bar_data.push((bucket_label, value));
    }

    let group_by_str = group_by.as_str();
    let metric_str = metric.as_str();
    let (buckets, warnings) = build_buckets(group_by_str, metric_str, &bar_data);

    Ok(ai_format::format_aggregate_prices(
        &upper,
        group_by_str,
        metric_str,
        total_bars,
        date_range,
        buckets,
        warnings,
    ))
}

/// Sort bucket keys in natural order for the given grouping.
fn sort_bucket_keys(
    group_by: &str,
    groups: &std::collections::BTreeMap<String, Vec<f64>>,
) -> Vec<String> {
    let mut keys: Vec<String> = groups.keys().cloned().collect();
    match group_by {
        "day_of_week" => {
            let order = [
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday",
            ];
            keys.sort_by_key(|k| order.iter().position(|&d| d == k).unwrap_or(7));
        }
        "month" => {
            let order = [
                "January",
                "February",
                "March",
                "April",
                "May",
                "June",
                "July",
                "August",
                "September",
                "October",
                "November",
                "December",
            ];
            keys.sort_by_key(|k| order.iter().position(|&m| m == k).unwrap_or(12));
        }
        "quarter" | "year" | "hour_of_day" => {
            keys.sort();
        }
        _ => {}
    }
    keys
}

/// Build buckets from pre-computed bar data (label, value) pairs.
/// Extracted for testability — this is the pure aggregation logic used by `execute`.
fn build_buckets(
    group_by: &str,
    metric: &str,
    bar_data: &[(String, f64)],
) -> (Vec<AggregateBucket>, Vec<String>) {
    let mut groups: std::collections::BTreeMap<String, Vec<f64>> =
        std::collections::BTreeMap::new();
    for (label, value) in bar_data {
        groups.entry(label.clone()).or_default().push(*value);
    }

    let ordered_keys = sort_bucket_keys(group_by, &groups);
    let mut buckets = Vec::with_capacity(ordered_keys.len());
    let mut warnings = Vec::new();

    for label in &ordered_keys {
        let values = &groups[label];
        let count = values.len();
        let m = stats::mean(values);
        let md = stats::median(values);
        let sd = stats::std_dev(values);
        let min_val = values.iter().copied().fold(f64::INFINITY, f64::min);
        let max_val = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let total = values.iter().sum::<f64>();
        let positive = values.iter().filter(|&&v| v > 0.0).count();
        let positive_pct = if count > 0 {
            positive as f64 / count as f64 * 100.0
        } else {
            0.0
        };

        let p_value = if metric == "return" || metric == "gap" {
            stats::t_test_one_sample(values, 0.0).map(|r| r.p_value)
        } else {
            None
        };

        buckets.push(AggregateBucket {
            label: label.clone(),
            count,
            mean: m,
            median: md,
            std_dev: sd,
            min: min_val,
            max: max_val,
            total,
            positive_pct,
            p_value,
        });
    }

    for b in &buckets {
        if b.count < 20 {
            warnings.push(format!(
                "{}: only {} observations — interpret with caution",
                b.label, b.count
            ));
        }
    }

    (buckets, warnings)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort_bucket_keys_day_of_week() {
        let mut groups = std::collections::BTreeMap::new();
        groups.insert("Friday".to_string(), vec![1.0]);
        groups.insert("Monday".to_string(), vec![2.0]);
        groups.insert("Wednesday".to_string(), vec![3.0]);
        let keys = sort_bucket_keys("day_of_week", &groups);
        assert_eq!(keys, vec!["Monday", "Wednesday", "Friday"]);
    }

    #[test]
    fn test_sort_bucket_keys_month() {
        let mut groups = std::collections::BTreeMap::new();
        groups.insert("March".to_string(), vec![1.0]);
        groups.insert("January".to_string(), vec![2.0]);
        groups.insert("December".to_string(), vec![3.0]);
        let keys = sort_bucket_keys("month", &groups);
        assert_eq!(keys, vec!["January", "March", "December"]);
    }

    #[test]
    fn test_build_buckets_return_metric_stats() {
        let bar_data = vec![
            ("Monday".to_string(), 1.0),
            ("Monday".to_string(), 2.0),
            ("Monday".to_string(), 3.0),
            ("Tuesday".to_string(), -1.0),
            ("Tuesday".to_string(), -2.0),
        ];
        let (buckets, _) = build_buckets("day_of_week", "return", &bar_data);

        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].label, "Monday");
        assert_eq!(buckets[0].count, 3);
        assert!((buckets[0].mean - 2.0).abs() < 1e-10);
        assert!((buckets[0].median - 2.0).abs() < 1e-10);
        assert!((buckets[0].min - 1.0).abs() < 1e-10);
        assert!((buckets[0].max - 3.0).abs() < 1e-10);
        assert!((buckets[0].total - 6.0).abs() < 1e-10);
        assert!((buckets[0].positive_pct - 100.0).abs() < 1e-10);

        assert_eq!(buckets[1].label, "Tuesday");
        assert_eq!(buckets[1].count, 2);
        assert!((buckets[1].mean - (-1.5)).abs() < 1e-10);
        assert!((buckets[1].positive_pct).abs() < 1e-10); // 0% positive
    }

    #[test]
    fn test_build_buckets_volume_metric_no_p_value() {
        let bar_data = vec![
            ("Q1".to_string(), 1_000_000.0),
            ("Q1".to_string(), 2_000_000.0),
            ("Q2".to_string(), 500_000.0),
        ];
        let (buckets, _) = build_buckets("quarter", "volume", &bar_data);

        assert_eq!(buckets.len(), 2);
        // Volume metric should NOT produce p-values
        assert!(buckets[0].p_value.is_none());
        assert!(buckets[1].p_value.is_none());
        assert!((buckets[0].mean - 1_500_000.0).abs() < 1e-10);
    }

    #[test]
    fn test_build_buckets_return_metric_has_p_value() {
        // Need enough observations for t-test (>= 2)
        let bar_data: Vec<(String, f64)> = (0..30)
            .map(|i| ("January".to_string(), 0.5 + (f64::from(i)) * 0.01))
            .collect();
        let (buckets, _) = build_buckets("month", "return", &bar_data);

        assert_eq!(buckets.len(), 1);
        assert!(buckets[0].p_value.is_some());
        // All positive values, mean clearly > 0, p-value should be small
        assert!(buckets[0].p_value.unwrap() < 0.05);
    }

    #[test]
    fn test_build_buckets_small_sample_warning() {
        let bar_data = vec![("2024".to_string(), 1.0), ("2024".to_string(), 2.0)];
        let (buckets, warnings) = build_buckets("year", "return", &bar_data);

        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].count, 2);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("only 2 observations"));
    }

    #[test]
    fn test_build_buckets_no_warning_large_sample() {
        let bar_data: Vec<(String, f64)> = (0..25)
            .map(|i| ("Monday".to_string(), f64::from(i) * 0.1))
            .collect();
        let (_, warnings) = build_buckets("day_of_week", "return", &bar_data);

        assert!(warnings.is_empty());
    }

    #[test]
    fn test_build_buckets_range_metric() {
        let bar_data = vec![
            ("Monday".to_string(), 2.5),
            ("Monday".to_string(), 3.5),
            ("Tuesday".to_string(), 1.0),
        ];
        let (buckets, _) = build_buckets("day_of_week", "range", &bar_data);

        assert_eq!(buckets.len(), 2);
        assert!((buckets[0].mean - 3.0).abs() < 1e-10);
        // Range metric should NOT produce p-values
        assert!(buckets[0].p_value.is_none());
    }

    #[test]
    fn test_build_buckets_quarter_ordering() {
        let bar_data = vec![
            ("Q3".to_string(), 1.0),
            ("Q1".to_string(), 2.0),
            ("Q4".to_string(), 3.0),
            ("Q2".to_string(), 4.0),
        ];
        let (buckets, _) = build_buckets("quarter", "return", &bar_data);

        let labels: Vec<&str> = buckets.iter().map(|b| b.label.as_str()).collect();
        assert_eq!(labels, vec!["Q1", "Q2", "Q3", "Q4"]);
    }

    #[test]
    fn test_build_buckets_gap_metric_has_p_value() {
        let bar_data: Vec<(String, f64)> = (0..30)
            .map(|i| ("Monday".to_string(), 0.3 + f64::from(i) * 0.01))
            .collect();
        let (buckets, _) = build_buckets("day_of_week", "gap", &bar_data);
        assert_eq!(buckets.len(), 1);
        assert!(
            buckets[0].p_value.is_some(),
            "gap metric should produce p-values"
        );
    }

    #[test]
    fn test_sort_bucket_keys_hour_of_day() {
        let mut groups = std::collections::BTreeMap::new();
        groups.insert("14:00".to_string(), vec![1.0]);
        groups.insert("09:00".to_string(), vec![2.0]);
        groups.insert("04:00".to_string(), vec![3.0]);
        let keys = sort_bucket_keys("hour_of_day", &groups);
        assert_eq!(keys, vec!["04:00", "09:00", "14:00"]);
    }

    #[test]
    fn test_build_buckets_positive_pct_mixed() {
        let bar_data = vec![
            ("Monday".to_string(), 1.0),
            ("Monday".to_string(), -1.0),
            ("Monday".to_string(), 0.5),
            ("Monday".to_string(), -0.5),
        ];
        let (buckets, _) = build_buckets("day_of_week", "return", &bar_data);

        assert_eq!(buckets[0].count, 4);
        assert!((buckets[0].positive_pct - 50.0).abs() < 1e-10);
    }

    #[test]
    fn test_build_buckets_gap_metric_stats() {
        // Gap values: Monday has positive gaps, Tuesday has negative
        let bar_data = vec![
            ("Monday".to_string(), 0.5),
            ("Monday".to_string(), 1.0),
            ("Monday".to_string(), 0.3),
            ("Tuesday".to_string(), -0.5),
            ("Tuesday".to_string(), -1.0),
        ];
        let (buckets, _) = build_buckets("day_of_week", "gap", &bar_data);
        assert_eq!(buckets.len(), 2);

        // Monday: mean gap = 0.6
        assert_eq!(buckets[0].label, "Monday");
        assert!((buckets[0].mean - 0.6).abs() < 1e-10);
        assert!((buckets[0].positive_pct - 100.0).abs() < 1e-10);

        // Tuesday: mean gap = -0.75
        assert_eq!(buckets[1].label, "Tuesday");
        assert!((buckets[1].mean - (-0.75)).abs() < 1e-10);
        assert!((buckets[1].positive_pct).abs() < 1e-10); // 0%
    }

    #[test]
    fn test_build_buckets_hour_of_day_grouping() {
        let bar_data = vec![
            ("09:00".to_string(), 0.5),
            ("09:00".to_string(), 0.3),
            ("10:00".to_string(), -0.2),
            ("14:00".to_string(), 0.8),
        ];
        let (buckets, _) = build_buckets("hour_of_day", "return", &bar_data);
        assert_eq!(buckets.len(), 3);
        assert_eq!(buckets[0].label, "09:00");
        assert_eq!(buckets[0].count, 2);
        assert_eq!(buckets[1].label, "10:00");
        assert_eq!(buckets[2].label, "14:00");
    }

    /// Parse a date string the same way `execute` does, returning `(NaiveDate, Option<hour>)`.
    fn parse_bar_date(
        date_str: &str,
    ) -> Result<(chrono::NaiveDate, Option<u32>), Box<dyn std::error::Error>> {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S")
            .or_else(|_| chrono::NaiveDateTime::parse_from_str(date_str, "%Y-%m-%d %H:%M:%S"))
        {
            Ok((dt.date(), Some(dt.time().hour())))
        } else if let Ok(d) = date_str.parse::<chrono::NaiveDate>() {
            Ok((d, None))
        } else {
            Err(format!("Invalid date: {date_str}").into())
        }
    }

    #[test]
    fn test_intraday_datetime_parsing_and_bucketing() {
        // Simulates the full path: T-separated datetime strings → hour extraction → bucketing
        let date_strings = [
            "2024-01-02T09:30:00", // 09:00 bucket
            "2024-01-02T09:45:00", // 09:00 bucket (same hour)
            "2024-01-02T10:00:00", // 10:00 bucket
            "2024-01-02T14:30:00", // 14:00 bucket
        ];

        let mut bar_data: Vec<(String, f64)> = Vec::new();
        for (i, ds) in date_strings.iter().enumerate() {
            let (_date, hour) = parse_bar_date(ds).unwrap();
            let h = hour.expect("intraday strings should have hours");
            let label = format!("{h:02}:00");
            bar_data.push((label, (i as f64) * 0.1));
        }

        let (buckets, _) = build_buckets("hour_of_day", "return", &bar_data);
        assert_eq!(buckets.len(), 3);
        assert_eq!(buckets[0].label, "09:00");
        assert_eq!(buckets[0].count, 2);
        assert_eq!(buckets[1].label, "10:00");
        assert_eq!(buckets[2].label, "14:00");
    }

    #[test]
    fn test_midnight_bar_parsed_as_date_only_gets_hour_zero() {
        // raw_prices formats midnight (00:00:00) as date-only "YYYY-MM-DD"
        let (_date, hour) = parse_bar_date("2024-01-02").unwrap();
        assert!(hour.is_none(), "date-only should have no hour");
        // In execute(), hour.unwrap_or(0) maps this to "00:00"
        let label = format!("{:02}:00", hour.unwrap_or(0));
        assert_eq!(label, "00:00");
    }

    #[test]
    fn test_space_separated_datetime_also_parses() {
        let (_date, hour) = parse_bar_date("2024-01-02 14:30:00").unwrap();
        assert_eq!(hour, Some(14));
    }
}
