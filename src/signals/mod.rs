//! Technical analysis signal system for filtering backtest entry and exit dates.
//!
//! Provides 40+ built-in signals across momentum, trend, volatility, overlap,
//! price, and volume categories, plus custom formula-based signals and combinators.

pub mod builders;
pub mod combinators;
pub mod custom;
pub mod helpers;
pub mod indicators;
pub mod momentum;
pub mod overlap;
pub mod price;
pub mod registry;
pub mod spec;
pub mod storage;
pub mod trend;
pub mod volatility;
pub mod volume;

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::*;

use crate::engine::price_table::extract_date_from_column;
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
    if spec.contains_cross_symbol() {
        tracing::warn!(
            "Signal spec contains CrossSymbol references but active_dates() was called \
             without cross-symbol DataFrames. CrossSymbol signals will evaluate against \
             the primary DataFrame. Use active_dates_multi() instead."
        );
    }
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

/// Evaluate a signal spec that may contain `CrossSymbol` variants.
///
/// `primary_df` is the main symbol's OHLCV data. `cross_dfs` maps uppercase
/// secondary symbols to their OHLCV `DataFrame`s.
///
/// For plain signals, evaluates against `primary_df`. For `CrossSymbol` variants,
/// evaluates the inner signal against the referenced symbol's `DataFrame`. `And`/`Or`
/// combinators recurse so that each branch can reference a different symbol.
pub fn active_dates_multi<S: std::hash::BuildHasher>(
    spec: &SignalSpec,
    primary_df: &DataFrame,
    cross_dfs: &HashMap<String, DataFrame, S>,
    date_col: &str,
) -> Result<HashSet<NaiveDate>> {
    match spec {
        SignalSpec::CrossSymbol { symbol, signal } => {
            let upper = symbol.to_uppercase();
            let df = cross_dfs.get(&upper).ok_or_else(|| {
                anyhow::anyhow!("CrossSymbol references '{upper}' but no OHLCV data loaded for it")
            })?;
            // The inner signal may itself contain CrossSymbol or combinators
            active_dates_multi(signal, df, cross_dfs, date_col)
        }
        SignalSpec::And { left, right } => {
            let left_dates = active_dates_multi(left, primary_df, cross_dfs, date_col)?;
            let right_dates = active_dates_multi(right, primary_df, cross_dfs, date_col)?;
            Ok(left_dates.intersection(&right_dates).copied().collect())
        }
        SignalSpec::Or { left, right } => {
            let left_dates = active_dates_multi(left, primary_df, cross_dfs, date_col)?;
            let right_dates = active_dates_multi(right, primary_df, cross_dfs, date_col)?;
            Ok(left_dates.union(&right_dates).copied().collect())
        }
        // All other variants evaluate against the primary DataFrame
        _ => active_dates(spec, primary_df, date_col),
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

        // Custom consecutive_up with count=2: true at indices 2,3,4
        let spec = SignalSpec::Custom {
            name: "consecutive_up_2".into(),
            formula: "consecutive_up(close) >= 2".into(),
            description: None,
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

        let spec = SignalSpec::Custom {
            name: "consecutive_down_2".into(),
            formula: "consecutive_down(close) >= 2".into(),
            description: None,
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
        let spec = SignalSpec::Custom {
            name: "consecutive_up_5".into(),
            formula: "consecutive_up(close) >= 5".into(),
            description: None,
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
            left: Box::new(SignalSpec::Custom {
                name: "consecutive_up_2".into(),
                formula: "consecutive_up(close) >= 2".into(),
                description: None,
            }),
            right: Box::new(SignalSpec::Custom {
                name: "consecutive_up_3".into(),
                formula: "consecutive_up(close) >= 3".into(),
                description: None,
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
            left: Box::new(SignalSpec::Custom {
                name: "consecutive_up_4".into(),
                formula: "consecutive_up(close) >= 4".into(),
                description: None,
            }),
            right: Box::new(SignalSpec::Custom {
                name: "consecutive_up_2".into(),
                formula: "consecutive_up(close) >= 2".into(),
                description: None,
            }),
        };
        let result = active_dates(&spec, &df, "date").unwrap();
        // count=4 matches at 4; count=2 matches at 2,3,4
        // OR: union is 2,3,4
        assert!(result.contains(&dates[2]));
        assert!(result.contains(&dates[3]));
        assert!(result.contains(&dates[4]));
    }

    // ── Cross-symbol tests ──────────────────────────────────────────────

    #[test]
    fn active_dates_multi_cross_symbol_basic() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(),
        ];

        // Primary symbol (SPY) — trending up
        let primary_df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[100.0, 101.0, 102.0, 103.0, 104.0],
        }
        .unwrap();

        // Secondary symbol (VIX) — only dates 3,4,5 have consecutive ups
        let vix_df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[18.0, 17.0, 19.0, 21.0, 23.0],
        }
        .unwrap();

        let mut cross_dfs = HashMap::new();
        cross_dfs.insert("^VIX".to_string(), vix_df);

        // CrossSymbol: consecutive_up(count=2) on VIX
        let spec = SignalSpec::CrossSymbol {
            symbol: "^VIX".into(),
            signal: Box::new(SignalSpec::Custom {
                name: "vix_up_2".into(),
                formula: "consecutive_up(close) >= 2".into(),
                description: None,
            }),
        };

        let result = active_dates_multi(&spec, &primary_df, &cross_dfs, "date").unwrap();
        // VIX: 18→17 (down), 17→19 (up), 19→21 (up), 21→23 (up)
        // ConsecutiveUp(2) fires at indices 3,4 (two consecutive up moves)
        assert!(result.contains(&dates[3]));
        assert!(result.contains(&dates[4]));
        assert!(!result.contains(&dates[0]));
        assert!(!result.contains(&dates[1]));
        assert!(!result.contains(&dates[2]));
    }

    #[test]
    fn active_dates_multi_and_with_cross_symbol() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(),
        ];

        // Primary: all dates have consecutive up (count=2) at indices 2,3,4
        let primary_df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[100.0, 101.0, 102.0, 103.0, 104.0],
        }
        .unwrap();

        // VIX: only dates 4,5 have consecutive up (count=3)
        let vix_df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[15.0, 14.0, 16.0, 18.0, 20.0],
        }
        .unwrap();

        let mut cross_dfs = HashMap::new();
        cross_dfs.insert("^VIX".to_string(), vix_df);

        // AND: primary consecutive_up(2) AND CrossSymbol(VIX consecutive_up(3))
        let spec = SignalSpec::And {
            left: Box::new(SignalSpec::Custom {
                name: "consecutive_up_2".into(),
                formula: "consecutive_up(close) >= 2".into(),
                description: None,
            }),
            right: Box::new(SignalSpec::CrossSymbol {
                symbol: "^VIX".into(),
                signal: Box::new(SignalSpec::Custom {
                    name: "vix_up_3".into(),
                    formula: "consecutive_up(close) >= 3".into(),
                    description: None,
                }),
            }),
        };

        let result = active_dates_multi(&spec, &primary_df, &cross_dfs, "date").unwrap();
        // Primary ConsecutiveUp(2): indices 2,3,4
        // VIX: 15→14(down), 14→16(up), 16→18(up), 18→20(up)
        // VIX ConsecutiveUp(3): index 4 only
        // AND intersection: index 4
        assert!(result.contains(&dates[4]));
        assert!(!result.contains(&dates[2]));
        assert!(!result.contains(&dates[3]));
    }

    #[test]
    fn active_dates_multi_missing_cross_symbol_errors() {
        let dates = vec![NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()];
        let primary_df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates),
            "close" => &[100.0],
        }
        .unwrap();

        let cross_dfs = HashMap::new(); // empty — no VIX data

        let spec = SignalSpec::CrossSymbol {
            symbol: "^VIX".into(),
            signal: Box::new(SignalSpec::Custom {
                name: "vix_up_1".into(),
                formula: "consecutive_up(close) >= 1".into(),
                description: None,
            }),
        };

        let result = active_dates_multi(&spec, &primary_df, &cross_dfs, "date");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("^VIX"));
    }

    #[test]
    fn active_dates_multi_plain_signal_uses_primary() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
        ];
        let primary_df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[100.0, 101.0, 102.0],
        }
        .unwrap();

        let cross_dfs = HashMap::new();

        // Plain signal (no CrossSymbol) should use primary_df
        let spec = SignalSpec::Custom {
            name: "consecutive_up_2".into(),
            formula: "consecutive_up(close) >= 2".into(),
            description: None,
        };

        let result = active_dates_multi(&spec, &primary_df, &cross_dfs, "date").unwrap();
        assert!(result.contains(&dates[2]));
        assert!(!result.contains(&dates[0]));
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
        let df = DataFrame::new(
            5,
            vec![
                dt_chunked.into_series().into(),
                Series::new("close".into(), &[100.0, 101.0, 102.0, 103.0, 104.0]).into(),
            ],
        )
        .unwrap();

        let spec = SignalSpec::Custom {
            name: "consecutive_up_2".into(),
            formula: "consecutive_up(close) >= 2".into(),
            description: None,
        };
        let result = active_dates(&spec, &df, "datetime").unwrap();
        assert!(result.contains(&NaiveDate::from_ymd_opt(2024, 1, 3).unwrap()));
        assert!(result.contains(&NaiveDate::from_ymd_opt(2024, 1, 4).unwrap()));
        assert!(result.contains(&NaiveDate::from_ymd_opt(2024, 1, 5).unwrap()));
    }
}
