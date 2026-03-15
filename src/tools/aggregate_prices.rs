//! Aggregate OHLCV price data by time dimension (day-of-week, month, quarter, year).

use anyhow::{Context, Result};
use chrono::Datelike;
use std::sync::Arc;

use crate::data::cache::CachedStore;
use crate::stats;
use crate::tools::ai_format;
use crate::tools::response_types::{AggregateBucket, AggregatePricesResponse, DateRange};

/// Execute the `aggregate_prices` analysis.
#[allow(clippy::too_many_lines)]
pub async fn execute(
    cache: &Arc<CachedStore>,
    symbol: &str,
    years: u32,
    group_by: &str,
    metric: &str,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<AggregatePricesResponse> {
    // Validate group_by
    let valid_groups = ["day_of_week", "month", "quarter", "year"];
    if !valid_groups.contains(&group_by) {
        anyhow::bail!(
            "Invalid group_by: \"{group_by}\". Must be one of: {}",
            valid_groups.join(", ")
        );
    }
    // Validate metric
    let valid_metrics = ["return", "range", "volume"];
    if !valid_metrics.contains(&metric) {
        anyhow::bail!(
            "Invalid metric: \"{metric}\". Must be one of: {}",
            valid_metrics.join(", ")
        );
    }

    let upper = symbol.to_uppercase();
    let resp = crate::tools::raw_prices::load_and_execute(
        cache,
        &upper,
        start_date,
        end_date,
        None, // no limit
        crate::engine::types::Interval::Daily,
    )
    .await
    .context("Failed to load OHLCV data")?;

    // Filter by years if no explicit date range
    let prices = if start_date.is_none() && end_date.is_none() && years < 50 {
        let cutoff =
            chrono::Utc::now().date_naive() - chrono::Duration::days(i64::from(years) * 365);
        let cutoff_str = cutoff.format("%Y-%m-%d").to_string();
        resp.prices
            .into_iter()
            .filter(|p| p.date >= cutoff_str)
            .collect::<Vec<_>>()
    } else {
        resp.prices
    };

    if prices.len() < 2 {
        anyhow::bail!("Insufficient price data for {upper} (need at least 2 bars)");
    }

    let total_bars = prices.len();
    let date_range = DateRange {
        start: prices.first().map(|p| p.date.clone()),
        end: prices.last().map(|p| p.date.clone()),
    };

    // Compute per-bar metric values
    let mut bar_data: Vec<(String, f64)> = Vec::with_capacity(total_bars);
    for i in 0..prices.len() {
        let date_str = &prices[i].date;
        let date = date_str
            .parse::<chrono::NaiveDate>()
            .with_context(|| format!("Invalid date: {date_str}"))?;

        let bucket_label = match group_by {
            "day_of_week" => date.format("%A").to_string(),
            "month" => date.format("%B").to_string(),
            "quarter" => format!("Q{}", ((date.month() - 1) / 3) + 1),
            "year" => date.format("%Y").to_string(),
            _ => unreachable!(),
        };

        let value = match metric {
            "return" => {
                if i == 0 {
                    continue; // skip first bar (no previous close)
                }
                let prev_close = prices[i - 1].close;
                if prev_close == 0.0 {
                    continue;
                }
                (prices[i].close - prev_close) / prev_close * 100.0
            }
            "range" => {
                if prices[i].low == 0.0 {
                    continue;
                }
                (prices[i].high - prices[i].low) / prices[i].low * 100.0
            }
            "volume" => prices[i].volume as f64,
            _ => unreachable!(),
        };
        bar_data.push((bucket_label, value));
    }

    // Group by bucket label
    let mut groups: std::collections::BTreeMap<String, Vec<f64>> =
        std::collections::BTreeMap::new();
    for (label, value) in &bar_data {
        groups.entry(label.clone()).or_default().push(*value);
    }

    // Sort buckets naturally
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

        // One-sample t-test vs zero is meaningful only for return data
        let p_value = if metric == "return" {
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

    // Add warnings for small sample sizes
    for b in &buckets {
        if b.count < 20 {
            warnings.push(format!(
                "{}: only {} observations — interpret with caution",
                b.label, b.count
            ));
        }
    }

    Ok(ai_format::format_aggregate_prices(
        &upper, group_by, metric, total_bars, date_range, buckets, warnings,
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
        "quarter" | "year" => {
            keys.sort();
        }
        _ => {}
    }
    keys
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
}
