//! Wheel strategy simulation: sell puts -> assignment -> sell covered calls -> repeat.
//!
//! Provides single-leg candidate finding for puts and calls, which is the
//! building block for the wheel strategy's state machine.

use std::collections::BTreeMap;

use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::*;

use super::filters;
use super::price_table::extract_date_from_column;
use super::types::{DteRange, OptionType, TargetRange};
use crate::data::parquet::DATETIME_COL;

/// A single-leg option candidate for wheel entry.
#[derive(Debug, Clone)]
pub struct SingleLegCandidate {
    pub date: NaiveDate,
    pub expiration: NaiveDate,
    pub strike: f64,
    pub bid: f64,
    pub ask: f64,
    pub delta: f64,
    pub option_type: OptionType,
}

/// Find single-leg option candidates grouped by trading date.
///
/// Filters the options DataFrame by option type, valid quotes, DTE range,
/// and delta range. Returns the best candidate per (date, expiration) pair,
/// grouped by date.
pub fn find_single_leg_candidates(
    df: &DataFrame,
    opt_type: OptionType,
    delta: &TargetRange,
    dte: &DteRange,
    min_bid_ask: f64,
    min_strike: Option<f64>,
) -> Result<BTreeMap<NaiveDate, Vec<SingleLegCandidate>>> {
    // Step 1: Combined filter — option type + DTE + valid quotes
    let filtered =
        filters::filter_leg_candidates(df, opt_type.as_str(), dte.max, dte.min, min_bid_ask)?;

    if filtered.height() == 0 {
        return Ok(BTreeMap::new());
    }

    // Step 2: Optional min strike filter (cost basis floor for covered calls)
    let filtered = if let Some(floor) = min_strike {
        filtered
            .clone()
            .lazy()
            .filter(col("strike").gt_eq(lit(floor)))
            .collect()?
    } else {
        filtered
    };

    if filtered.height() == 0 {
        return Ok(BTreeMap::new());
    }

    // Step 3: Select closest delta per (datetime, expiration)
    let selected = filters::select_closest_delta(&filtered, delta)?;

    if selected.height() == 0 {
        return Ok(BTreeMap::new());
    }

    // Step 4: Extract rows into SingleLegCandidate structs grouped by date
    let dt_col = selected.column(DATETIME_COL)?;
    let exp_col = selected.column("expiration")?;
    let strikes = selected.column("strike")?.f64()?;
    let bids = selected.column("bid")?.f64()?;
    let asks = selected.column("ask")?.f64()?;
    let deltas = selected.column("delta")?.f64()?;

    let mut candidates: BTreeMap<NaiveDate, Vec<SingleLegCandidate>> = BTreeMap::new();

    for i in 0..selected.height() {
        let date = extract_date_from_column(dt_col, i)?;
        let expiration = extract_date_from_column(exp_col, i)?;

        let Some(strike) = strikes.get(i) else {
            continue;
        };
        let Some(bid) = bids.get(i) else {
            continue;
        };
        let Some(ask) = asks.get(i) else {
            continue;
        };
        let Some(delta_val) = deltas.get(i) else {
            continue;
        };

        candidates
            .entry(date)
            .or_default()
            .push(SingleLegCandidate {
                date,
                expiration,
                strike,
                bid,
                ask,
                delta: delta_val,
                option_type: opt_type,
            });
    }

    Ok(candidates)
}

/// Find short put candidates for the wheel strategy.
pub fn find_put_candidates(
    df: &DataFrame,
    delta: &TargetRange,
    dte: &DteRange,
    min_bid_ask: f64,
) -> Result<BTreeMap<NaiveDate, Vec<SingleLegCandidate>>> {
    find_single_leg_candidates(df, OptionType::Put, delta, dte, min_bid_ask, None)
}

/// Find covered call candidates, optionally filtering by minimum strike (cost basis floor).
pub fn find_call_candidates(
    df: &DataFrame,
    delta: &TargetRange,
    dte: &DteRange,
    min_bid_ask: f64,
    min_strike: Option<f64>,
) -> Result<BTreeMap<NaiveDate, Vec<SingleLegCandidate>>> {
    find_single_leg_candidates(df, OptionType::Call, delta, dte, min_bid_ask, min_strike)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal options DataFrame for testing.
    fn make_test_df() -> DataFrame {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 16)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ];
        let expirations = [
            NaiveDate::from_ymd_opt(2024, 2, 16).unwrap(), // 32 DTE from Jan 15
            NaiveDate::from_ymd_opt(2024, 2, 16).unwrap(), // 32 DTE from Jan 15
            NaiveDate::from_ymd_opt(2024, 2, 16).unwrap(), // 32 DTE from Jan 15
            NaiveDate::from_ymd_opt(2024, 2, 16).unwrap(), // 31 DTE from Jan 16
        ];

        let mut df = df! {
            DATETIME_COL => &dates,
            "option_type" => &["p", "p", "c", "p"],
            "strike" => &[95.0f64, 100.0, 105.0, 98.0],
            "bid" => &[2.0f64, 3.0, 1.5, 2.5],
            "ask" => &[2.50f64, 3.50, 2.0, 3.0],
            "delta" => &[-0.30f64, -0.40, 0.35, -0.32],
        }
        .unwrap();

        let exp_col =
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations).into_column();
        df.with_column(exp_col).unwrap();
        df
    }

    #[test]
    fn empty_df_returns_empty_map() {
        let df = df! {
            DATETIME_COL => Vec::<chrono::NaiveDateTime>::new(),
            "option_type" => Vec::<&str>::new(),
            "strike" => Vec::<f64>::new(),
            "bid" => Vec::<f64>::new(),
            "ask" => Vec::<f64>::new(),
            "delta" => Vec::<f64>::new(),
        }
        .unwrap();

        // Add empty expiration column with Date type
        let exp_col =
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), Vec::<NaiveDate>::new())
                .into_column();
        let mut df = df;
        df.with_column(exp_col).unwrap();

        let delta = TargetRange {
            target: 0.30,
            min: 0.20,
            max: 0.40,
        };
        let dte = DteRange {
            target: 45,
            min: 30,
            max: 60,
        };

        let result = find_put_candidates(&df, &delta, &dte, 0.0).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn filters_by_option_type_puts_only() {
        let df = make_test_df();
        let delta = TargetRange {
            target: 0.30,
            min: 0.20,
            max: 0.50,
        };
        let dte = DteRange {
            target: 32,
            min: 30,
            max: 45,
        };

        let result = find_put_candidates(&df, &delta, &dte, 0.0).unwrap();

        // All returned candidates must be puts
        for (_date, candidates) in &result {
            for c in candidates {
                assert_eq!(c.option_type, OptionType::Put);
            }
        }
        // Should have found some put candidates
        assert!(!result.is_empty());
    }

    #[test]
    fn filters_by_option_type_calls_only() {
        let df = make_test_df();
        let delta = TargetRange {
            target: 0.35,
            min: 0.20,
            max: 0.50,
        };
        let dte = DteRange {
            target: 32,
            min: 30,
            max: 45,
        };

        let result = find_call_candidates(&df, &delta, &dte, 0.0, None).unwrap();

        for (_date, candidates) in &result {
            for c in candidates {
                assert_eq!(c.option_type, OptionType::Call);
            }
        }
        // Should have found some call candidates
        assert!(!result.is_empty());
    }

    #[test]
    fn min_strike_filters_low_strikes() {
        let df = make_test_df();
        let delta = TargetRange {
            target: 0.35,
            min: 0.20,
            max: 0.50,
        };
        let dte = DteRange {
            target: 32,
            min: 30,
            max: 45,
        };

        // Set min_strike above all call strikes (105.0) — should still find the 105 call
        let result = find_call_candidates(&df, &delta, &dte, 0.0, Some(105.0)).unwrap();
        for (_date, candidates) in &result {
            for c in candidates {
                assert!(c.strike >= 105.0);
            }
        }

        // Set min_strike above all strikes — should find nothing
        let result = find_call_candidates(&df, &delta, &dte, 0.0, Some(200.0)).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn dte_out_of_range_returns_empty() {
        let df = make_test_df();
        let delta = TargetRange {
            target: 0.30,
            min: 0.20,
            max: 0.50,
        };
        // DTE range that doesn't match any rows (all are ~31-32 DTE)
        let dte = DteRange {
            target: 60,
            min: 55,
            max: 90,
        };

        let result = find_put_candidates(&df, &delta, &dte, 0.0).unwrap();
        assert!(result.is_empty());
    }
}
