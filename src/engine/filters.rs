use anyhow::Result;
use chrono::Datelike;
use polars::prelude::*;

use super::types::{ExpirationCycle, ExpirationFilter, TargetRange};
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

/// Filter by DTE range [`min_dte`, `max_dte`]
pub fn filter_dte_range(df: &DataFrame, max_dte: i32, min_dte: i32) -> Result<DataFrame> {
    let result = df
        .clone()
        .lazy()
        .filter(
            col("dte")
                .gt_eq(lit(min_dte))
                .and(col("dte").lt_eq(lit(max_dte))),
        )
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

/// Select only the columns needed for leg joining, then rename per-leg columns
/// with the leg index suffix to avoid conflicts when joining multiple legs.
///
/// The `base_cols` are the per-leg columns to rename (e.g. `["strike", "bid", "ask", "delta"]`).
/// Each is renamed to `{col}_{leg_index}`. Only join keys (`quote_datetime`, `expiration`)
/// and the renamed columns are kept, dropping extras like `option_type` and `dte` that
/// would cause duplicate column errors when joining 3+ legs.
///
/// # Errors
///
/// Returns an error if any of the specified columns are missing from the `DataFrame`.
pub fn prepare_leg_for_join(
    df: &DataFrame,
    leg_index: usize,
    base_cols: &[&str],
) -> Result<DataFrame> {
    let mut select_cols: Vec<&str> = vec![QUOTE_DATETIME_COL, "expiration"];
    select_cols.extend_from_slice(base_cols);

    let selected = df.select(select_cols)?;

    let old_names: Vec<String> = base_cols.iter().map(|s| (*s).to_string()).collect();
    let new_names: Vec<String> = base_cols
        .iter()
        .map(|s| format!("{s}_{leg_index}"))
        .collect();

    let result = selected
        .lazy()
        .rename(old_names, new_names, true)
        .collect()?;

    Ok(result)
}

/// Like `prepare_leg_for_join`, but renames `expiration` to a cycle-specific
/// column (`expiration_primary` or `expiration_secondary`) so that legs from
/// different expiration cycles can be joined on `quote_datetime` alone and
/// then cross-filtered by `expiration_secondary > expiration_primary`.
pub fn prepare_leg_for_join_multi_exp(
    df: &DataFrame,
    leg_index: usize,
    base_cols: &[&str],
    cycle: ExpirationCycle,
) -> Result<DataFrame> {
    let exp_col_name = match cycle {
        ExpirationCycle::Primary => "expiration_primary",
        ExpirationCycle::Secondary => "expiration_secondary",
    };

    let mut select_cols: Vec<&str> = vec![QUOTE_DATETIME_COL, "expiration"];
    select_cols.extend_from_slice(base_cols);

    let selected = df.select(select_cols)?;

    let mut old_names: Vec<String> = vec!["expiration".to_string()];
    old_names.extend(base_cols.iter().map(|s| (*s).to_string()));

    let mut new_names: Vec<String> = vec![exp_col_name.to_string()];
    new_names.extend(base_cols.iter().map(|s| format!("{s}_{leg_index}")));

    let result = selected
        .lazy()
        .rename(old_names, new_names, true)
        .collect()?;

    Ok(result)
}

/// Join legs for multi-expiration strategies (calendar/diagonal).
///
/// Primary and secondary legs are joined separately within their groups on
/// `(quote_datetime, expiration_<cycle>)`, then cross-joined on `quote_datetime`
/// with a filter ensuring `expiration_secondary > expiration_primary`.
pub fn join_multi_expiration_legs(leg_dfs: &[(DataFrame, ExpirationCycle)]) -> Result<DataFrame> {
    let mut primary_dfs: Vec<&DataFrame> = Vec::new();
    let mut secondary_dfs: Vec<&DataFrame> = Vec::new();

    for (df, cycle) in leg_dfs {
        match cycle {
            ExpirationCycle::Primary => primary_dfs.push(df),
            ExpirationCycle::Secondary => secondary_dfs.push(df),
        }
    }

    if primary_dfs.is_empty() {
        anyhow::bail!("Multi-expiration strategy has no Primary legs");
    }
    if secondary_dfs.is_empty() {
        anyhow::bail!("Multi-expiration strategy has no Secondary legs");
    }

    // Join within primary group
    let mut primary = primary_dfs[0].clone();
    for df in primary_dfs.iter().skip(1) {
        let join_cols: Vec<&str> = vec![QUOTE_DATETIME_COL, "expiration_primary"];
        primary = primary
            .lazy()
            .join(
                (*df).clone().lazy(),
                join_cols.iter().map(|c| col(*c)).collect::<Vec<_>>(),
                join_cols.iter().map(|c| col(*c)).collect::<Vec<_>>(),
                JoinArgs::new(JoinType::Inner),
            )
            .collect()?;
    }

    // Join within secondary group
    let mut secondary = secondary_dfs[0].clone();
    for df in secondary_dfs.iter().skip(1) {
        let join_cols: Vec<&str> = vec![QUOTE_DATETIME_COL, "expiration_secondary"];
        secondary = secondary
            .lazy()
            .join(
                (*df).clone().lazy(),
                join_cols.iter().map(|c| col(*c)).collect::<Vec<_>>(),
                join_cols.iter().map(|c| col(*c)).collect::<Vec<_>>(),
                JoinArgs::new(JoinType::Inner),
            )
            .collect()?;
    }

    // Cross-join on quote_datetime, then filter expiration_secondary > expiration_primary
    let combined = primary
        .lazy()
        .join(
            secondary.lazy(),
            vec![col(QUOTE_DATETIME_COL)],
            vec![col(QUOTE_DATETIME_COL)],
            JoinArgs::new(JoinType::Inner),
        )
        .filter(col("expiration_secondary").gt(col("expiration_primary")))
        .collect()?;

    Ok(combined)
}

/// Join leg `DataFrame`s, dispatching to the appropriate join strategy.
///
/// If `is_multi_exp` is true, delegates to `join_multi_expiration_legs`.
/// Otherwise, performs a sequential inner join of all legs on
/// `(quote_datetime, expiration)`.
pub fn join_legs(
    leg_dfs: &[(DataFrame, ExpirationCycle)],
    is_multi_exp: bool,
) -> Result<DataFrame> {
    if is_multi_exp {
        return join_multi_expiration_legs(leg_dfs);
    }
    let mut combined = leg_dfs[0].0.clone();
    let join_cols: Vec<&str> = vec![QUOTE_DATETIME_COL, "expiration"];
    for (leg_df, _) in leg_dfs.iter().skip(1) {
        combined = combined
            .lazy()
            .join(
                leg_df.clone().lazy(),
                join_cols.iter().map(|c| col(*c)).collect::<Vec<_>>(),
                join_cols.iter().map(|c| col(*c)).collect::<Vec<_>>(),
                JoinArgs::new(JoinType::Inner),
            )
            .collect()?;
    }
    Ok(combined)
}

/// Filter out options with bid/ask below the minimum threshold
pub fn filter_valid_quotes(df: &DataFrame, min_bid_ask: f64) -> Result<DataFrame> {
    let result = df
        .clone()
        .lazy()
        .filter(
            col("bid")
                .gt(lit(min_bid_ask))
                .and(col("ask").gt(lit(min_bid_ask))),
        )
        .collect()?;
    Ok(result)
}

/// Returns `true` if `date` falls on the third Friday of its month.
pub fn is_third_friday(date: chrono::NaiveDate) -> bool {
    use chrono::Weekday;
    if date.weekday() != Weekday::Fri {
        return false;
    }
    // The third Friday has a day-of-month in [15, 21]
    let d = date.day();
    (15..=21).contains(&d)
}

/// Days-from-epoch offset: Polars stores Date as days since 1970-01-01 (Unix epoch).
/// `chrono::from_num_days_from_ce` uses day 1 CE as the origin (= day 719163 of Unix).
const EXPIRATION_EPOCH_OFFSET: i32 = 719_163;

/// Filter the options `DataFrame` to only rows whose expiration satisfies `filter`.
///
/// * `Any` — no-op, returns the `DataFrame` as-is.
/// * `Weekly` — keeps rows where expiration falls on a Friday.
/// * `Monthly` — keeps rows where expiration is the third Friday of the month.
pub fn filter_expiration_type(df: &DataFrame, filter: &ExpirationFilter) -> Result<DataFrame> {
    if matches!(filter, ExpirationFilter::Any) {
        return Ok(df.clone());
    }

    let exp_col = df.column("expiration")?;
    let exp_ca = exp_col.date()?;

    let mask: Vec<bool> = exp_ca
        .phys
        .iter()
        .map(|opt_days| {
            let Some(days) = opt_days else {
                return false;
            };
            let Some(date) =
                chrono::NaiveDate::from_num_days_from_ce_opt(days + EXPIRATION_EPOCH_OFFSET)
            else {
                return false;
            };
            match filter {
                ExpirationFilter::Any => true,
                ExpirationFilter::Weekly => date.weekday() == chrono::Weekday::Fri,
                ExpirationFilter::Monthly => is_third_friday(date),
            }
        })
        .collect();

    let mask_ca = BooleanChunked::new(PlSmallStr::from("mask"), &mask);
    Ok(df.filter(&mask_ca)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    /// Build a minimal options `DataFrame` for testing filters.
    /// Uses `Datetime` for `quote_datetime` and `Date` for expiration to match production data.
    fn make_options_df() -> DataFrame {
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
        let exp_col =
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations).into_column();
        df.with_column(exp_col).unwrap();
        df
    }

    #[test]
    fn compute_dte_correct_day_values() {
        let dates = vec![
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
            NaiveDate::from_ymd_opt(2024, 1, 16).unwrap(), // 1 DTE
            NaiveDate::from_ymd_opt(2024, 2, 16).unwrap(), // 32 DTE
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(), // 59 DTE
        ];
        let mut df = df! {
            QUOTE_DATETIME_COL => &dates,
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations).into_column(),
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
        let result = filter_valid_quotes(&df, 0.0).unwrap();
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
        let result = filter_valid_quotes(&df, 0.0).unwrap();
        assert_eq!(result.height(), 1);
    }

    #[test]
    fn filter_valid_quotes_with_min_threshold() {
        let df = df! {
            "bid" => &[0.03, 0.05, 0.10, 2.0],
            "ask" => &[0.04, 0.06, 0.15, 3.0],
        }
        .unwrap();
        // min_bid_ask=0.05 should filter out rows where bid or ask <= 0.05
        let result = filter_valid_quotes(&df, 0.05).unwrap();
        assert_eq!(result.height(), 2); // only 0.10/0.15 and 2.0/3.0 pass
    }

    #[test]
    fn select_closest_delta_picks_nearest() {
        let dt1 = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let exp1 = NaiveDate::from_ymd_opt(2024, 2, 16)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let df = df! {
            QUOTE_DATETIME_COL => &[dt1, dt1, dt1],
            "expiration" => &[exp1, exp1, exp1],
            "delta" => &[0.30, 0.48, 0.52],
            "strike" => &[95.0, 100.0, 105.0],
            "bid" => &[1.0, 2.0, 3.0],
            "ask" => &[1.5, 2.5, 3.5],
        }
        .unwrap();

        let target = TargetRange {
            target: 0.50,
            min: 0.25,
            max: 0.55,
        };
        let result = select_closest_delta(&df, &target).unwrap();
        // Should pick delta=0.48 or 0.52 (both distance 0.02 from 0.50)
        assert_eq!(result.height(), 1);
        let selected_delta = result
            .column("delta")
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();
        assert!((selected_delta - 0.48).abs() < 0.05 || (selected_delta - 0.52).abs() < 0.05);
    }

    #[test]
    fn select_closest_delta_filters_out_of_range() {
        let dt1 = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let exp1 = NaiveDate::from_ymd_opt(2024, 2, 16)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let df = df! {
            QUOTE_DATETIME_COL => &[dt1, dt1],
            "expiration" => &[exp1, exp1],
            "delta" => &[0.10, 0.90],
            "strike" => &[90.0, 110.0],
            "bid" => &[0.5, 5.0],
            "ask" => &[1.0, 5.5],
        }
        .unwrap();

        let target = TargetRange {
            target: 0.50,
            min: 0.40,
            max: 0.60,
        };
        let result = select_closest_delta(&df, &target).unwrap();
        assert_eq!(result.height(), 0);
    }

    #[test]
    fn is_third_friday_identifies_correctly() {
        // Third Friday of January 2024 is the 19th
        assert!(is_third_friday(NaiveDate::from_ymd_opt(2024, 1, 19).unwrap()));
        // First Friday of January 2024 is the 5th — not third
        assert!(!is_third_friday(NaiveDate::from_ymd_opt(2024, 1, 5).unwrap()));
        // Non-Friday date
        assert!(!is_third_friday(NaiveDate::from_ymd_opt(2024, 1, 18).unwrap()));
        // Third Friday of February 2024 is the 16th
        assert!(is_third_friday(NaiveDate::from_ymd_opt(2024, 2, 16).unwrap()));
    }

    #[test]
    fn filter_expiration_type_any_returns_all() {
        use polars::prelude::DateChunked;
        let exp1 = NaiveDate::from_ymd_opt(2024, 1, 19).unwrap(); // Friday
        let exp2 = NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(); // Monday
        let dt = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let mut df = df! {
            QUOTE_DATETIME_COL => &[dt, dt],
            "option_type" => &["call", "call"],
            "strike" => &[100.0f64, 100.0],
            "bid" => &[1.0f64, 1.0],
            "ask" => &[1.5f64, 1.5],
            "delta" => &[0.5f64, 0.5],
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), [exp1, exp2])
                .into_column(),
        )
        .unwrap();
        let result = filter_expiration_type(&df, &ExpirationFilter::Any).unwrap();
        assert_eq!(result.height(), 2);
    }

    #[test]
    fn filter_expiration_type_weekly_keeps_fridays() {
        use polars::prelude::DateChunked;
        let friday = NaiveDate::from_ymd_opt(2024, 1, 19).unwrap();
        let monday = NaiveDate::from_ymd_opt(2024, 1, 22).unwrap();
        let dt = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let mut df = df! {
            QUOTE_DATETIME_COL => &[dt, dt],
            "option_type" => &["call", "call"],
            "strike" => &[100.0f64, 100.0],
            "bid" => &[1.0f64, 1.0],
            "ask" => &[1.5f64, 1.5],
            "delta" => &[0.5f64, 0.5],
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), [friday, monday])
                .into_column(),
        )
        .unwrap();
        let result = filter_expiration_type(&df, &ExpirationFilter::Weekly).unwrap();
        assert_eq!(result.height(), 1, "Only Friday expiration should remain");
    }

    #[test]
    fn filter_expiration_type_monthly_keeps_third_fridays() {
        use polars::prelude::DateChunked;
        let third_friday = NaiveDate::from_ymd_opt(2024, 1, 19).unwrap(); // 3rd Friday Jan 2024
        let other_friday = NaiveDate::from_ymd_opt(2024, 1, 26).unwrap(); // 4th Friday Jan 2024
        let dt = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let mut df = df! {
            QUOTE_DATETIME_COL => &[dt, dt],
            "option_type" => &["call", "call"],
            "strike" => &[100.0f64, 100.0],
            "bid" => &[1.0f64, 1.0],
            "ask" => &[1.5f64, 1.5],
            "delta" => &[0.5f64, 0.5],
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(
                PlSmallStr::from("expiration"),
                [third_friday, other_friday],
            )
            .into_column(),
        )
        .unwrap();
        let result = filter_expiration_type(&df, &ExpirationFilter::Monthly).unwrap();
        assert_eq!(result.height(), 1, "Only 3rd Friday expiration should remain");
    }
}
