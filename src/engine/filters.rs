use anyhow::Result;
use polars::prelude::*;

use super::types::TargetRange;
use crate::data::parquet::QUOTE_DATETIME_COL;

/// Compute DTE (days to expiration) from `quote_datetime` and expiration columns.
/// Casts both to Date for integer-day DTE regardless of intraday granularity.
pub fn compute_dte(df: &DataFrame) -> Result<DataFrame> {
    let ms_per_day = 86_400_000i64;
    let result = df
        .clone()
        .lazy()
        .with_column(
            ((col("expiration").cast(DataType::Date)
                - col(QUOTE_DATETIME_COL).cast(DataType::Date))
            .dt()
            .total_milliseconds(false)
                / lit(ms_per_day))
            .cast(DataType::Int32)
            .alias("dte"),
        )
        .collect()?;
    Ok(result)
}

/// Filter by DTE range [`exit_dte`, `max_entry_dte`]
pub fn filter_dte_range(df: &DataFrame, max_entry_dte: i32, exit_dte: i32) -> Result<DataFrame> {
    let result = df
        .clone()
        .lazy()
        .filter(col("dte").gt_eq(lit(exit_dte)).and(col("dte").lt_eq(lit(max_entry_dte))))
        .collect()?;
    Ok(result)
}

/// Filter by option type (call or put)
pub fn filter_option_type(df: &DataFrame, option_type: &str) -> Result<DataFrame> {
    let result = df
        .clone()
        .lazy()
        .filter(col("option_type").eq(lit(option_type)))
        .collect()?;
    Ok(result)
}

/// Select the closest option to a target delta within a range.
/// Groups by (`quote_datetime`, expiration) and picks the row closest to target delta.
pub fn select_closest_delta(df: &DataFrame, target: &TargetRange) -> Result<DataFrame> {
    let result = df
        .clone()
        .lazy()
        .filter(
            col("delta")
                .abs()
                .gt_eq(lit(target.min))
                .and(col("delta").abs().lt_eq(lit(target.max))),
        )
        .with_column(
            (col("delta").abs() - lit(target.target))
                .abs()
                .alias("delta_dist"),
        )
        .sort(
            ["delta_dist"],
            SortMultipleOptions::default().with_order_descending(false),
        )
        .unique_generic(
            Some(vec![col(QUOTE_DATETIME_COL), col("expiration")]),
            UniqueKeepStrategy::First,
        )
        .collect()?;

    // Drop the helper column
    let result = result.drop("delta_dist")?;
    Ok(result)
}

/// Filter out options with zero or negative bid
pub fn filter_valid_quotes(df: &DataFrame) -> Result<DataFrame> {
    let result = df
        .clone()
        .lazy()
        .filter(col("bid").gt(lit(0.0)).and(col("ask").gt(lit(0.0))))
        .collect()?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    /// Build a minimal options DataFrame for testing filters.
    /// Uses Datetime for quote_datetime and Date for expiration to match production data.
    fn make_options_df() -> DataFrame {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 16).unwrap().and_hms_opt(0, 0, 0).unwrap(),
        ];
        let expirations = [
            NaiveDate::from_ymd_opt(2024, 2, 16).unwrap(), // 32 DTE
            NaiveDate::from_ymd_opt(2024, 2, 16).unwrap(), // 32 DTE
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(), // 60 DTE
            NaiveDate::from_ymd_opt(2024, 2, 16).unwrap(), // 31 DTE
        ];

        // Build base df with df! macro, then add expiration as Date column
        let mut df = df! {
            QUOTE_DATETIME_COL => &dates,
            "option_type" => &["call", "call", "put", "call"],
            "strike" => &[100.0f64, 105.0, 100.0, 100.0],
            "bid" => &[2.0f64, 1.5, 3.0, 0.0],
            "ask" => &[2.50f64, 2.0, 3.50, 0.50],
            "delta" => &[0.50f64, 0.40, -0.45, 0.30],
        }
        .unwrap();
        let exp_col = DateChunked::from_naive_date(
            PlSmallStr::from("expiration"),
            expirations,
        ).into_column();
        df.with_column(exp_col).unwrap();
        df
    }

    #[test]
    fn compute_dte_correct_day_values() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 16).unwrap().and_hms_opt(0, 0, 0).unwrap(),
        ];
        let expirations = [
            NaiveDate::from_ymd_opt(2024, 1, 16).unwrap(), // 1 DTE
            NaiveDate::from_ymd_opt(2024, 2, 16).unwrap(), // 32 DTE
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(), // 59 DTE
        ];
        let mut df = df! {
            QUOTE_DATETIME_COL => &dates,
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations)
                .into_column(),
        )
        .unwrap();

        let result = compute_dte(&df).unwrap();
        let dte = result.column("dte").unwrap().i32().unwrap();
        let values: Vec<Option<i32>> = dte.into_iter().collect();
        assert_eq!(values[0], Some(1));
        assert_eq!(values[1], Some(32));
        assert_eq!(values[2], Some(59));
    }

    #[test]
    fn filter_dte_range_with_precalculated_dte() {
        // Test filter_dte_range independently with a pre-populated DTE column
        let df = df! {
            "dte" => &[10i32, 20, 30, 45, 60],
            "value" => &[1, 2, 3, 4, 5],
        }
        .unwrap();
        // Filter [20, 45]
        let result = filter_dte_range(&df, 45, 20).unwrap();
        assert_eq!(result.height(), 3); // 20, 30, 45

        // Exact boundary [30, 30]
        let result = filter_dte_range(&df, 30, 30).unwrap();
        assert_eq!(result.height(), 1);
    }

    #[test]
    fn filter_dte_range_empty_result() {
        let df = df! {
            "dte" => &[10i32, 20, 30],
            "value" => &[1, 2, 3],
        }
        .unwrap();
        let result = filter_dte_range(&df, 5, 1).unwrap();
        assert_eq!(result.height(), 0);
    }

    #[test]
    fn filter_option_type_calls() {
        let df = make_options_df();
        let result = filter_option_type(&df, "call").unwrap();
        assert_eq!(result.height(), 3);
    }

    #[test]
    fn filter_option_type_puts() {
        let df = make_options_df();
        let result = filter_option_type(&df, "put").unwrap();
        assert_eq!(result.height(), 1);
    }

    #[test]
    fn filter_valid_quotes_removes_zero_bid() {
        let df = make_options_df();
        let result = filter_valid_quotes(&df).unwrap();
        // Row with bid=0.0 should be filtered out
        assert_eq!(result.height(), 3);
    }

    #[test]
    fn filter_valid_quotes_removes_negative() {
        let df = df! {
            "bid" => &[-1.0, 2.0],
            "ask" => &[1.0, 3.0],
        }
        .unwrap();
        let result = filter_valid_quotes(&df).unwrap();
        assert_eq!(result.height(), 1);
    }

    #[test]
    fn select_closest_delta_picks_nearest() {
        let dt1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap().and_hms_opt(0, 0, 0).unwrap();
        let exp1 = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap().and_hms_opt(0, 0, 0).unwrap();
        let df = df! {
            QUOTE_DATETIME_COL => &[dt1, dt1, dt1],
            "expiration" => &[exp1, exp1, exp1],
            "delta" => &[0.30, 0.48, 0.52],
            "strike" => &[95.0, 100.0, 105.0],
            "bid" => &[1.0, 2.0, 3.0],
            "ask" => &[1.5, 2.5, 3.5],
        }
        .unwrap();

        let target = TargetRange { target: 0.50, min: 0.25, max: 0.55 };
        let result = select_closest_delta(&df, &target).unwrap();
        // Should pick delta=0.48 or 0.52 (both distance 0.02 from 0.50)
        assert_eq!(result.height(), 1);
        let selected_delta = result.column("delta").unwrap().f64().unwrap().get(0).unwrap();
        assert!((selected_delta - 0.48).abs() < 0.05 || (selected_delta - 0.52).abs() < 0.05);
    }

    #[test]
    fn select_closest_delta_filters_out_of_range() {
        let dt1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap().and_hms_opt(0, 0, 0).unwrap();
        let exp1 = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap().and_hms_opt(0, 0, 0).unwrap();
        let df = df! {
            QUOTE_DATETIME_COL => &[dt1, dt1],
            "expiration" => &[exp1, exp1],
            "delta" => &[0.10, 0.90],
            "strike" => &[90.0, 110.0],
            "bid" => &[0.5, 5.0],
            "ask" => &[1.0, 5.5],
        }
        .unwrap();

        let target = TargetRange { target: 0.50, min: 0.40, max: 0.60 };
        let result = select_closest_delta(&df, &target).unwrap();
        assert_eq!(result.height(), 0);
    }
}
