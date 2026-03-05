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

use crate::engine::event_sim::extract_date_from_column;
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
            let date = extract_date_from_column(col, i)?;
            result.insert(date);
        }
    }

    Ok(result)
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

    #[test]
    fn active_dates_consecutive_down() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(),
        ];
        let df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[104.0, 103.0, 102.0, 101.0, 100.0],
        }
        .unwrap();

        let spec = SignalSpec::ConsecutiveDown {
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

    #[test]
    fn active_dates_no_matches() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
        ];
        let df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[100.0, 99.0, 98.0],
        }
        .unwrap();

        // Looking for 5 consecutive ups but data trends down
        let spec = SignalSpec::ConsecutiveUp {
            column: "close".into(),
            count: 5,
        };
        let result = active_dates(&spec, &df, "date").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn active_dates_with_and_combinator() {
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

        let spec = SignalSpec::And {
            left: Box::new(SignalSpec::ConsecutiveUp {
                column: "close".into(),
                count: 2,
            }),
            right: Box::new(SignalSpec::ConsecutiveUp {
                column: "close".into(),
                count: 3,
            }),
        };
        let result = active_dates(&spec, &df, "date").unwrap();
        // count=3 matches at index 3,4; count=2 matches at 2,3,4
        // AND: intersection is 3,4
        assert!(result.contains(&dates[3]));
        assert!(result.contains(&dates[4]));
        assert!(!result.contains(&dates[2]));
    }

    #[test]
    fn active_dates_with_or_combinator() {
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

        let spec = SignalSpec::Or {
            left: Box::new(SignalSpec::ConsecutiveUp {
                column: "close".into(),
                count: 4,
            }),
            right: Box::new(SignalSpec::ConsecutiveUp {
                column: "close".into(),
                count: 2,
            }),
        };
        let result = active_dates(&spec, &df, "date").unwrap();
        // count=4 matches at 4; count=2 matches at 2,3,4
        // OR: union is 2,3,4
        assert!(result.contains(&dates[2]));
        assert!(result.contains(&dates[3]));
        assert!(result.contains(&dates[4]));
    }

    #[test]
    fn active_dates_with_datetime_column() {
        use chrono::NaiveDateTime;
        let datetimes: Vec<NaiveDateTime> = vec![
            NaiveDateTime::parse_from_str("2024-01-01 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-02 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-03 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-04 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-05 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        ];
        let dt_chunked: DatetimeChunked =
            DatetimeChunked::new(PlSmallStr::from("datetime"), &datetimes);
        let df = DataFrame::new(5, vec![
            dt_chunked.into_series().into(),
            Series::new("close".into(), &[100.0, 101.0, 102.0, 103.0, 104.0]).into(),
        ])
        .unwrap();

        let spec = SignalSpec::ConsecutiveUp {
            column: "close".into(),
            count: 2,
        };
        let result = active_dates(&spec, &df, "datetime").unwrap();
        assert!(result.contains(&NaiveDate::from_ymd_opt(2024, 1, 3).unwrap()));
        assert!(result.contains(&NaiveDate::from_ymd_opt(2024, 1, 4).unwrap()));
        assert!(result.contains(&NaiveDate::from_ymd_opt(2024, 1, 5).unwrap()));
    }
}
