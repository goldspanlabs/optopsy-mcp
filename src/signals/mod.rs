pub mod combinators;
pub mod helpers;
pub mod momentum;
pub mod overlap;
pub mod price;
pub mod registry;
pub mod trend;
pub mod volatility;
pub mod volume;

use std::collections::HashSet;

use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::*;

use helpers::SignalFn;
use registry::{build_signal, SignalSpec};

/// Evaluate a signal spec against an OHLCV `DataFrame` and return the set of dates
/// where the signal is active (true).
///
/// Used for both entry signals (dates to allow new entries) and exit signals
/// (dates to trigger early close on open positions).
pub fn active_dates(
    spec: &SignalSpec,
    ohlcv_df: &DataFrame,
    date_col: &str,
) -> Result<HashSet<NaiveDate>> {
    let signal: Box<dyn SignalFn> = build_signal(spec);
    let bools = signal.evaluate(ohlcv_df)?;
    let bool_ca = bools.bool()?;

    let col = ohlcv_df.column(date_col)?;
    let mut result = HashSet::new();

    for i in 0..ohlcv_df.height() {
        if bool_ca.get(i) == Some(true) {
            let date = extract_naive_date(col, i)?;
            result.insert(date);
        }
    }

    Ok(result)
}

/// Extract a `NaiveDate` from a column value at a given index.
/// Handles Date, Datetime, and String column types.
fn extract_naive_date(col: &Column, idx: usize) -> Result<NaiveDate> {
    match col.dtype() {
        DataType::Date => {
            let days = col.date()?.phys.get(idx);
            match days {
                Some(d) => {
                    let date = NaiveDate::from_num_days_from_ce_opt(d + 719_163)
                        .ok_or_else(|| anyhow::anyhow!("Invalid date at index {idx}"))?;
                    Ok(date)
                }
                None => anyhow::bail!("Null date at index {idx}"),
            }
        }
        DataType::Datetime(tu, _) => {
            let val = col.datetime()?.phys.get(idx);
            match val {
                Some(v) => {
                    let ndt = match tu {
                        TimeUnit::Milliseconds => {
                            chrono::DateTime::from_timestamp_millis(v).map(|dt| dt.naive_utc())
                        }
                        TimeUnit::Microseconds => {
                            chrono::DateTime::from_timestamp_micros(v).map(|dt| dt.naive_utc())
                        }
                        TimeUnit::Nanoseconds => {
                            let secs = v / 1_000_000_000;
                            let nsecs = (v % 1_000_000_000) as u32;
                            chrono::DateTime::from_timestamp(secs, nsecs).map(|dt| dt.naive_utc())
                        }
                    };
                    match ndt {
                        Some(dt) => Ok(dt.date()),
                        None => anyhow::bail!("Invalid datetime value at index {idx}"),
                    }
                }
                None => anyhow::bail!("Null datetime at index {idx}"),
            }
        }
        DataType::String => {
            let str_val = col.str()?.get(idx);
            match str_val {
                Some(s) => {
                    // Try YYYY-MM-DD first, then YYYY-MM-DDTHH:MM:SS
                    NaiveDate::parse_from_str(s, "%Y-%m-%d")
                        .or_else(|_| {
                            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                                .map(|dt| dt.date())
                        })
                        .map_err(|e| anyhow::anyhow!("Cannot parse date '{s}': {e}"))
                }
                None => anyhow::bail!("Null string date at index {idx}"),
            }
        }
        other => anyhow::bail!("Unsupported column type for date extraction: {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_dates_from_simple_df() {
        // Build a small OHLCV-like DF with a date column and price data
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(),
        ];
        let df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[100.0, 101.0, 102.0, 103.0, 104.0],
        }
        .unwrap();

        // ConsecutiveUp with count=2: true at indices 2,3,4
        let spec = SignalSpec::ConsecutiveUp {
            column: "close".into(),
            count: 2,
        };
        let result = active_dates(&spec, &df, "date").unwrap();
        assert!(result.contains(&dates[2]));
        assert!(result.contains(&dates[3]));
        assert!(result.contains(&dates[4]));
        assert!(!result.contains(&dates[0]));
        assert!(!result.contains(&dates[1]));
    }
}
