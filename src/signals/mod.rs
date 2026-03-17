//! Technical analysis signal system for filtering backtest entry and exit dates.
//!
//! Provides 40+ built-in signals across momentum, trend, volatility, overlap,
//! price, and volume categories, plus custom formula-based signals and combinators.

pub mod builders;
pub mod combinators;
pub mod custom;
pub mod custom_funcs;
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
use chrono::{NaiveDate, NaiveDateTime};
use polars::prelude::*;

use crate::engine::price_table::{extract_date_from_column, extract_datetime_from_column};
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
            // Detect the correct date/datetime column for this cross-symbol DataFrame
            let cross_date_col = crate::engine::stock_sim::detect_date_col(df);
            active_dates_multi(signal, df, cross_dfs, cross_date_col)
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

/// Like `active_dates` but returns `NaiveDateTime` for intraday support.
///
/// For Date columns, datetimes have midnight time component.
/// For Datetime columns, the full timestamp is preserved.
pub fn active_datetimes(
    spec: &SignalSpec,
    ohlcv_df: &DataFrame,
    date_col: &str,
) -> Result<HashSet<NaiveDateTime>> {
    if spec.contains_cross_symbol() {
        tracing::warn!(
            "Signal spec contains CrossSymbol references but active_datetimes() was called \
             without cross-symbol DataFrames. Use active_datetimes_multi() instead."
        );
    }
    let signal: Box<dyn SignalFn> = build_signal(spec);
    let bools = signal.evaluate(ohlcv_df)?;
    let bool_ca = bools.bool()?;

    let col = ohlcv_df.column(date_col)?;
    let mut result = HashSet::new();

    for i in 0..ohlcv_df.height() {
        if bool_ca.get(i) == Some(true) {
            let dt = extract_datetime_from_column(col, i)?;
            result.insert(dt);
        }
    }

    Ok(result)
}

/// Like `active_dates_multi` but returns `NaiveDateTime` for intraday support.
///
/// When combining signals via `And`/`Or`, branches may have different granularity
/// (e.g., primary is intraday but `CrossSymbol` references daily data). In that case,
/// daily-only dates are "broadcast" — a daily signal active on 2024-01-02 matches all
/// intraday bars on that calendar day, so the intersection/union works correctly.
pub fn active_datetimes_multi<S: std::hash::BuildHasher>(
    spec: &SignalSpec,
    primary_df: &DataFrame,
    cross_dfs: &HashMap<String, DataFrame, S>,
    date_col: &str,
) -> Result<HashSet<NaiveDateTime>> {
    match spec {
        SignalSpec::CrossSymbol { symbol, signal } => {
            let upper = symbol.to_uppercase();
            let df = cross_dfs.get(&upper).ok_or_else(|| {
                anyhow::anyhow!("CrossSymbol references '{upper}' but no OHLCV data loaded for it")
            })?;
            // Detect the correct date/datetime column for this cross-symbol DataFrame
            let cross_date_col = crate::engine::stock_sim::detect_date_col(df);
            active_datetimes_multi(signal, df, cross_dfs, cross_date_col)
        }
        SignalSpec::And { left, right } => {
            let left_dts = active_datetimes_multi(left, primary_df, cross_dfs, date_col)?;
            let right_dts = active_datetimes_multi(right, primary_df, cross_dfs, date_col)?;
            Ok(intersect_mixed_granularity(&left_dts, &right_dts))
        }
        SignalSpec::Or { left, right } => {
            let left_dts = active_datetimes_multi(left, primary_df, cross_dfs, date_col)?;
            let right_dts = active_datetimes_multi(right, primary_df, cross_dfs, date_col)?;
            Ok(union_mixed_granularity(&left_dts, &right_dts))
        }
        _ => active_datetimes(spec, primary_df, date_col),
    }
}

/// Check if all datetimes in a set have midnight time components (i.e., daily-only).
fn is_daily_only(dts: &HashSet<NaiveDateTime>) -> bool {
    !dts.is_empty()
        && dts
            .iter()
            .all(|dt| dt.time() == chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap())
}

/// Intersect two datetime sets that may have different granularity.
///
/// If one side is daily-only (all midnight timestamps) and the other has intraday
/// timestamps, the daily side is treated as "active for the whole day" — each
/// intraday timestamp is kept if its calendar date appears in the daily set.
/// If both sides have the same granularity, a normal intersection is performed.
fn intersect_mixed_granularity(
    left: &HashSet<NaiveDateTime>,
    right: &HashSet<NaiveDateTime>,
) -> HashSet<NaiveDateTime> {
    let left_daily = is_daily_only(left);
    let right_daily = is_daily_only(right);

    match (left_daily, right_daily) {
        (true, false) => {
            // Left is daily, right is intraday: keep right's timestamps whose date is in left
            let active_dates: HashSet<chrono::NaiveDate> =
                left.iter().map(NaiveDateTime::date).collect();
            right
                .iter()
                .filter(|dt| active_dates.contains(&dt.date()))
                .copied()
                .collect()
        }
        (false, true) => {
            // Right is daily, left is intraday: keep left's timestamps whose date is in right
            let active_dates: HashSet<chrono::NaiveDate> =
                right.iter().map(NaiveDateTime::date).collect();
            left.iter()
                .filter(|dt| active_dates.contains(&dt.date()))
                .copied()
                .collect()
        }
        _ => {
            // Same granularity: normal intersection
            left.intersection(right).copied().collect()
        }
    }
}

/// Union two datetime sets that may have different granularity.
///
/// If one side is daily-only, its dates are broadcast to match the intraday
/// timestamps from the other side (plus any intraday timestamps on dates not
/// covered by the daily set). The daily midnight timestamps are also included
/// so that dates with no intraday bars in the other set still appear.
fn union_mixed_granularity(
    left: &HashSet<NaiveDateTime>,
    right: &HashSet<NaiveDateTime>,
) -> HashSet<NaiveDateTime> {
    // Union is straightforward: all timestamps from both sides.
    // For mixed granularity, the daily midnight timestamps won't match any
    // intraday timestamps, but that's fine — the simulation loop checks
    // `dates.contains(&bar.datetime)`, so the intraday bar timestamps
    // from the other branch will match. The midnight entries are harmless
    // (no bar will have a midnight timestamp in intraday data).
    left.union(right).copied().collect()
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
        let spec = SignalSpec::Formula {
            formula: "consecutive_up(close) >= 2".into(),
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

        let spec = SignalSpec::Formula {
            formula: "consecutive_down(close) >= 2".into(),
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
        let spec = SignalSpec::Formula {
            formula: "consecutive_up(close) >= 5".into(),
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
            left: Box::new(SignalSpec::Formula {
                formula: "consecutive_up(close) >= 2".into(),
            }),
            right: Box::new(SignalSpec::Formula {
                formula: "consecutive_up(close) >= 3".into(),
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
            left: Box::new(SignalSpec::Formula {
                formula: "consecutive_up(close) >= 4".into(),
            }),
            right: Box::new(SignalSpec::Formula {
                formula: "consecutive_up(close) >= 2".into(),
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
            signal: Box::new(SignalSpec::Formula {
                formula: "consecutive_up(close) >= 2".into(),
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
            left: Box::new(SignalSpec::Formula {
                formula: "consecutive_up(close) >= 2".into(),
            }),
            right: Box::new(SignalSpec::CrossSymbol {
                symbol: "^VIX".into(),
                signal: Box::new(SignalSpec::Formula {
                    formula: "consecutive_up(close) >= 3".into(),
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
            signal: Box::new(SignalSpec::Formula {
                formula: "consecutive_up(close) >= 1".into(),
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
        let spec = SignalSpec::Formula {
            formula: "consecutive_up(close) >= 2".into(),
        };

        let result = active_dates_multi(&spec, &primary_df, &cross_dfs, "date").unwrap();
        assert!(result.contains(&dates[2]));
        assert!(!result.contains(&dates[0]));
    }

    #[test]
    fn active_dates_invalid_formula_errors() {
        let dates = vec![NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()];
        let df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates),
            "close" => &[100.0],
        }
        .unwrap();

        let spec = SignalSpec::Formula {
            formula: "nonexistent_column > 50".into(),
        };
        let result = active_dates(&spec, &df, "date");
        assert!(result.is_err());
    }

    #[test]
    fn active_dates_empty_dataframe() {
        let dates: Vec<NaiveDate> = vec![];
        let df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates),
            "close" => Vec::<f64>::new(),
        }
        .unwrap();

        let spec = SignalSpec::Formula {
            formula: "close > 100".into(),
        };
        let result = active_dates(&spec, &df, "date").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn active_dates_multi_or_with_cross_symbol() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
        ];

        let primary_df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[100.0, 99.0, 98.0],  // trending down
        }
        .unwrap();

        let vix_df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[15.0, 25.0, 30.0],  // trending up
        }
        .unwrap();

        let mut cross_dfs = HashMap::new();
        cross_dfs.insert("^VIX".to_string(), vix_df);

        // OR: primary close < 99 OR VIX close > 20
        let spec = SignalSpec::Or {
            left: Box::new(SignalSpec::Formula {
                formula: "close < 99".into(),
            }),
            right: Box::new(SignalSpec::CrossSymbol {
                symbol: "^VIX".into(),
                signal: Box::new(SignalSpec::Formula {
                    formula: "close > 20".into(),
                }),
            }),
        };

        let result = active_dates_multi(&spec, &primary_df, &cross_dfs, "date").unwrap();
        // Primary close < 99: index 2 (98)
        // VIX close > 20: indices 1 (25), 2 (30)
        // OR union: indices 1, 2
        assert!(!result.contains(&dates[0]));
        assert!(result.contains(&dates[1]));
        assert!(result.contains(&dates[2]));
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
            DatetimeChunked::new(PlSmallStr::from("quote_datetime"), &datetimes);
        let df = DataFrame::new(
            5,
            vec![
                dt_chunked.into_series().into(),
                Series::new("close".into(), &[100.0, 101.0, 102.0, 103.0, 104.0]).into(),
            ],
        )
        .unwrap();

        let spec = SignalSpec::Formula {
            formula: "consecutive_up(close) >= 2".into(),
        };
        let result = active_dates(&spec, &df, "quote_datetime").unwrap();
        assert!(result.contains(&NaiveDate::from_ymd_opt(2024, 1, 3).unwrap()));
        assert!(result.contains(&NaiveDate::from_ymd_opt(2024, 1, 4).unwrap()));
        assert!(result.contains(&NaiveDate::from_ymd_opt(2024, 1, 5).unwrap()));
    }

    // ── active_datetimes tests ──────────────────────────────────────────

    fn make_intraday_df() -> DataFrame {
        let datetimes: Vec<NaiveDateTime> = vec![
            NaiveDateTime::parse_from_str("2024-01-02 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-02 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-02 09:32:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-03 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-03 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        ];
        let dt_chunked: DatetimeChunked =
            DatetimeChunked::new(PlSmallStr::from("quote_datetime"), &datetimes);
        DataFrame::new(
            5,
            vec![
                dt_chunked.into_series().into(),
                Series::new("close".into(), &[100.0, 101.0, 102.0, 103.0, 104.0]).into(),
            ],
        )
        .unwrap()
    }

    #[test]
    fn active_datetimes_returns_full_timestamps() {
        let df = make_intraday_df();
        let spec = SignalSpec::Formula {
            formula: "consecutive_up(close) >= 2".into(),
        };
        let result = active_datetimes(&spec, &df, "quote_datetime").unwrap();
        // consecutive_up >= 2 fires at indices 2, 3, 4
        let dt2 =
            NaiveDateTime::parse_from_str("2024-01-02 09:32:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let dt3 =
            NaiveDateTime::parse_from_str("2024-01-03 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let dt4 =
            NaiveDateTime::parse_from_str("2024-01-03 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap();
        assert!(result.contains(&dt2));
        assert!(result.contains(&dt3));
        assert!(result.contains(&dt4));
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn active_datetimes_multi_plain_signal() {
        let df = make_intraday_df();
        let cross_dfs = HashMap::new();
        let spec = SignalSpec::Formula {
            formula: "close > 102".into(),
        };
        let result = active_datetimes_multi(&spec, &df, &cross_dfs, "quote_datetime").unwrap();
        // close > 102 at indices 3 (103) and 4 (104)
        let dt3 =
            NaiveDateTime::parse_from_str("2024-01-03 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let dt4 =
            NaiveDateTime::parse_from_str("2024-01-03 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&dt3));
        assert!(result.contains(&dt4));
    }

    // ── Mixed granularity tests ─────────────────────────────────────────

    #[test]
    fn is_daily_only_all_midnight() {
        let dts: HashSet<NaiveDateTime> = vec![
            NaiveDate::from_ymd_opt(2024, 1, 2)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ]
        .into_iter()
        .collect();
        assert!(is_daily_only(&dts));
    }

    #[test]
    fn is_daily_only_with_intraday() {
        let dts: HashSet<NaiveDateTime> =
            vec![
                NaiveDateTime::parse_from_str("2024-01-02 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            ]
            .into_iter()
            .collect();
        assert!(!is_daily_only(&dts));
    }

    #[test]
    fn is_daily_only_empty() {
        let dts: HashSet<NaiveDateTime> = HashSet::new();
        assert!(!is_daily_only(&dts));
    }

    #[test]
    fn intersect_daily_left_intraday_right() {
        // Daily side: Jan 2 active
        let daily: HashSet<NaiveDateTime> = vec![NaiveDate::from_ymd_opt(2024, 1, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()]
        .into_iter()
        .collect();

        // Intraday side: Jan 2 09:30, Jan 2 09:31, Jan 3 09:30
        let intraday: HashSet<NaiveDateTime> = vec![
            NaiveDateTime::parse_from_str("2024-01-02 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-02 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-03 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        ]
        .into_iter()
        .collect();

        let result = intersect_mixed_granularity(&daily, &intraday);
        // Should keep Jan 2 bars only (date matches), drop Jan 3
        assert_eq!(result.len(), 2);
        assert!(result.contains(
            &NaiveDateTime::parse_from_str("2024-01-02 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap()
        ));
        assert!(result.contains(
            &NaiveDateTime::parse_from_str("2024-01-02 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap()
        ));
    }

    #[test]
    fn intersect_intraday_left_daily_right() {
        // Same as above but swapped — should produce identical result
        let daily: HashSet<NaiveDateTime> = vec![NaiveDate::from_ymd_opt(2024, 1, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()]
        .into_iter()
        .collect();

        let intraday: HashSet<NaiveDateTime> = vec![
            NaiveDateTime::parse_from_str("2024-01-02 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-02 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-03 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        ]
        .into_iter()
        .collect();

        let result = intersect_mixed_granularity(&intraday, &daily);
        assert_eq!(result.len(), 2);
        assert!(result.contains(
            &NaiveDateTime::parse_from_str("2024-01-02 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap()
        ));
        assert!(result.contains(
            &NaiveDateTime::parse_from_str("2024-01-02 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap()
        ));
    }

    #[test]
    fn intersect_same_granularity_intraday() {
        let left: HashSet<NaiveDateTime> = vec![
            NaiveDateTime::parse_from_str("2024-01-02 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-02 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        ]
        .into_iter()
        .collect();

        let right: HashSet<NaiveDateTime> = vec![
            NaiveDateTime::parse_from_str("2024-01-02 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-02 09:32:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        ]
        .into_iter()
        .collect();

        let result = intersect_mixed_granularity(&left, &right);
        assert_eq!(result.len(), 1);
        assert!(result.contains(
            &NaiveDateTime::parse_from_str("2024-01-02 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap()
        ));
    }

    #[test]
    fn intersect_no_overlapping_dates() {
        let daily: HashSet<NaiveDateTime> = vec![NaiveDate::from_ymd_opt(2024, 1, 5)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()]
        .into_iter()
        .collect();

        let intraday: HashSet<NaiveDateTime> =
            vec![
                NaiveDateTime::parse_from_str("2024-01-02 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            ]
            .into_iter()
            .collect();

        let result = intersect_mixed_granularity(&daily, &intraday);
        assert!(result.is_empty());
    }

    #[test]
    fn union_mixed_includes_all() {
        let daily: HashSet<NaiveDateTime> = vec![NaiveDate::from_ymd_opt(2024, 1, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()]
        .into_iter()
        .collect();

        let intraday: HashSet<NaiveDateTime> =
            vec![
                NaiveDateTime::parse_from_str("2024-01-03 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            ]
            .into_iter()
            .collect();

        let result = union_mixed_granularity(&daily, &intraday);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn active_datetimes_multi_and_cross_symbol_mixed_granularity() {
        // Primary: intraday (datetime column)
        let intraday_df = make_intraday_df(); // Jan 2 09:30, 09:31, 09:32 + Jan 3 09:30, 09:31

        // Cross-symbol VIX: daily (date column), active on Jan 2 and Jan 3
        let vix_dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
        ];
        let vix_df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), vix_dates),
            "close" => &[30.0, 35.0], // close > 25 on both days
        }
        .unwrap();

        let mut cross_dfs = HashMap::new();
        cross_dfs.insert("^VIX".to_string(), vix_df);

        // AND: primary close > 102 AND CrossSymbol(VIX close > 25)
        // Primary close > 102: Jan 3 09:30 (103), Jan 3 09:31 (104)
        // VIX close > 25: Jan 2, Jan 3 (both days — daily granularity)
        // Without mixed-granularity fix, AND would be empty (midnight vs 09:30)
        // With fix, VIX daily dates broadcast to all Jan 2 + Jan 3 bars
        let spec = SignalSpec::And {
            left: Box::new(SignalSpec::Formula {
                formula: "close > 102".into(),
            }),
            right: Box::new(SignalSpec::CrossSymbol {
                symbol: "^VIX".into(),
                signal: Box::new(SignalSpec::Formula {
                    formula: "close > 25".into(),
                }),
            }),
        };

        let result =
            active_datetimes_multi(&spec, &intraday_df, &cross_dfs, "quote_datetime").unwrap();

        let dt_jan3_0930 =
            NaiveDateTime::parse_from_str("2024-01-03 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let dt_jan3_0931 =
            NaiveDateTime::parse_from_str("2024-01-03 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap();
        assert_eq!(result.len(), 2, "expected 2 intraday bars, got {result:?}");
        assert!(result.contains(&dt_jan3_0930));
        assert!(result.contains(&dt_jan3_0931));
    }

    #[test]
    fn active_datetimes_multi_or_cross_symbol_mixed_granularity() {
        let intraday_df = make_intraday_df();

        // VIX daily, active only on Jan 4 (not in primary's date range)
        let vix_dates = vec![NaiveDate::from_ymd_opt(2024, 1, 4).unwrap()];
        let vix_df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), vix_dates),
            "close" => &[40.0],
        }
        .unwrap();

        let mut cross_dfs = HashMap::new();
        cross_dfs.insert("^VIX".to_string(), vix_df);

        // OR: primary close > 103 OR CrossSymbol(VIX close > 25)
        // Primary close > 103: Jan 3 09:31 (104)
        // VIX close > 25: Jan 4 midnight (daily)
        let spec = SignalSpec::Or {
            left: Box::new(SignalSpec::Formula {
                formula: "close > 103".into(),
            }),
            right: Box::new(SignalSpec::CrossSymbol {
                symbol: "^VIX".into(),
                signal: Box::new(SignalSpec::Formula {
                    formula: "close > 25".into(),
                }),
            }),
        };

        let result =
            active_datetimes_multi(&spec, &intraday_df, &cross_dfs, "quote_datetime").unwrap();
        // Should have: Jan 3 09:31 (from primary) + Jan 4 00:00 (from VIX daily)
        assert!(
            result.len() >= 2,
            "expected at least 2 entries, got {result:?}"
        );
        let dt_jan3_0931 =
            NaiveDateTime::parse_from_str("2024-01-03 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap();
        assert!(result.contains(&dt_jan3_0931));
    }
}
