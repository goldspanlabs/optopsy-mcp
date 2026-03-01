use anyhow::Result;
use polars::prelude::*;

use crate::data::parquet::QUOTE_DATETIME_COL;

/// Match entry rows with corresponding exit rows based on expiration and exit DTE.
/// For each (`quote_datetime`, expiration, strike) in the entry set,
/// find the row in exit set where `quote_datetime` is closest to (expiration - `exit_dte`).
pub fn match_entry_exit(
    entries: &DataFrame,
    all_data: &DataFrame,
    exit_dte: i32,
) -> Result<DataFrame> {
    // Target exit date = expiration - exit_dte days
    let ms_per_day = 86_400_000i64;
    let entries_with_target = entries
        .clone()
        .lazy()
        .with_column(
            (col("expiration").cast(DataType::Date)
                - lit(i64::from(exit_dte) * ms_per_day)
                    .cast(DataType::Duration(TimeUnit::Milliseconds)))
            .alias("target_exit_date"),
        )
        .collect()?;

    // For exit data, we need to find rows in all_data that match
    // the same (expiration, strike, option_type) but at a later quote_datetime
    let exit_data = all_data
        .clone()
        .lazy()
        .select([
            col(QUOTE_DATETIME_COL).alias("exit_datetime"),
            col("expiration"),
            col("strike"),
            col("option_type"),
            col("bid").alias("exit_bid"),
            col("ask").alias("exit_ask"),
            col("delta").alias("exit_delta"),
        ])
        .collect()?;

    // Join entries with potential exits on (expiration, strike, option_type)
    let joined = entries_with_target
        .lazy()
        .join(
            exit_data.lazy(),
            [col("expiration"), col("strike"), col("option_type")],
            [col("expiration"), col("strike"), col("option_type")],
            JoinArgs::new(JoinType::Inner),
        )
        .filter(col("exit_datetime").gt(col(QUOTE_DATETIME_COL)))
        .collect()?;

    if joined.height() == 0 {
        return Ok(joined);
    }

    // For each entry, pick the exit closest to target_exit_date
    let result = joined
        .lazy()
        .with_column(
            (col("exit_datetime").cast(DataType::Date)
                - col("target_exit_date").cast(DataType::Date))
            .abs()
            .dt()
            .total_milliseconds(false)
            .cast(DataType::Int32)
            .alias("exit_dist"),
        )
        .sort(
            ["exit_dist"],
            SortMultipleOptions::default().with_order_descending(false),
        )
        .unique_generic(
            Some(vec![
                col(QUOTE_DATETIME_COL),
                col("expiration"),
                col("strike"),
                col("option_type"),
            ]),
            UniqueKeepStrategy::First,
        )
        .collect()?;

    // Drop helper columns
    let result = result.drop("exit_dist")?.drop("target_exit_date")?;

    Ok(result)
}
