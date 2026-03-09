use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use polars::prelude::*;
use std::path::PathBuf;

use super::DataStore;
use crate::engine::types::EPOCH_DAYS_CE_OFFSET;

/// The canonical timestamp column name used internally after normalization.
pub const QUOTE_DATETIME_COL: &str = "quote_datetime";

pub struct ParquetStore {
    path: PathBuf,
}

impl ParquetStore {
    pub fn new(path: &str) -> Self {
        Self {
            path: PathBuf::from(path),
        }
    }
}

/// Normalize the quote date/datetime column to a Datetime column named `quote_datetime`.
/// If the source column is Date, it gets cast to Datetime at midnight (00:00:00).
/// If it's already Datetime, it just gets renamed.
pub fn normalize_quote_datetime(df: DataFrame) -> Result<DataFrame> {
    // Detect the source column
    let (src_col, src_dtype) = if let Ok(c) = df.column("quote_datetime") {
        ("quote_datetime", c.dtype().clone())
    } else if let Ok(c) = df.column("quote_date") {
        ("quote_date", c.dtype().clone())
    } else {
        // No recognized date column — return as-is
        return Ok(df);
    };

    let result = match &src_dtype {
        DataType::Date => {
            // Cast Date → Datetime(Microseconds, None) at midnight
            let lf = df.lazy().with_column(
                col(src_col)
                    .cast(DataType::Datetime(TimeUnit::Microseconds, None))
                    .alias(QUOTE_DATETIME_COL),
            );
            let collected = lf.collect()?;
            if src_col == QUOTE_DATETIME_COL {
                collected
            } else {
                collected.drop(src_col)?
            }
        }
        DataType::Datetime(_, _) => {
            if src_col == QUOTE_DATETIME_COL {
                df
            } else {
                df.lazy()
                    .rename([src_col], [QUOTE_DATETIME_COL], true)
                    .collect()?
            }
        }
        DataType::String => {
            // Cast string to Date first, then to Datetime at midnight
            let lf = df.lazy().with_column(
                col(src_col)
                    .cast(DataType::Date)
                    .cast(DataType::Datetime(TimeUnit::Microseconds, None))
                    .alias(QUOTE_DATETIME_COL),
            );
            let collected = lf.collect()?;
            if src_col == QUOTE_DATETIME_COL {
                collected
            } else {
                collected.drop(src_col)?
            }
        }
        _ => df,
    };

    Ok(result)
}

impl DataStore for ParquetStore {
    async fn load_options(
        &self,
        _symbol: &str,
        start_date: Option<NaiveDate>,
        end_date: Option<NaiveDate>,
    ) -> Result<DataFrame> {
        let path_str = self.path.to_string_lossy().to_string();
        let start_dt = start_date.and_then(|d| d.and_hms_opt(0, 0, 0));
        let end_dt = end_date.and_then(|d| d.and_hms_opt(23, 59, 59));

        // Scan lazily so Polars can push date predicates down to the Parquet
        // row-group level, avoiding loading data that will be filtered out.
        let df = tokio::task::spawn_blocking(move || {
            let mut lazy =
                LazyFrame::scan_parquet(path_str.as_str().into(), ScanArgsParquet::default())?;

            // Detect and normalize the date column in the lazy pipeline
            let schema = lazy.collect_schema()?;
            let (src_col, src_dtype) = if let Some(dt) = schema.get(QUOTE_DATETIME_COL) {
                (QUOTE_DATETIME_COL, dt.clone())
            } else if let Some(dt) = schema.get("quote_date") {
                ("quote_date", dt.clone())
            } else {
                // No recognized date column — just collect as-is
                return lazy.collect().context("Failed to read Parquet file");
            };

            // Normalize to Datetime in the lazy pipeline
            lazy = match &src_dtype {
                DataType::Date => lazy.with_column(
                    col(src_col)
                        .cast(DataType::Datetime(TimeUnit::Microseconds, None))
                        .alias(QUOTE_DATETIME_COL),
                ),
                DataType::Datetime(_, _) if src_col != QUOTE_DATETIME_COL => {
                    lazy.rename([src_col], [QUOTE_DATETIME_COL], true)
                }
                DataType::String => lazy.with_column(
                    col(src_col)
                        .cast(DataType::Date)
                        .cast(DataType::Datetime(TimeUnit::Microseconds, None))
                        .alias(QUOTE_DATETIME_COL),
                ),
                _ => lazy, // already Datetime with correct name
            };

            // Apply date filters in the lazy pipeline for predicate pushdown
            if let Some(start) = start_dt {
                lazy = lazy.filter(col(QUOTE_DATETIME_COL).gt_eq(lit(start)));
            }
            if let Some(end) = end_dt {
                lazy = lazy.filter(col(QUOTE_DATETIME_COL).lt_eq(lit(end)));
            }

            // Drop original column if it was renamed/aliased and a duplicate remains
            let df = lazy.collect().context("Failed to read Parquet file")?;
            if src_col != QUOTE_DATETIME_COL && df.schema().contains(src_col) {
                df.drop(src_col)
                    .context("Failed to drop original date column")
            } else {
                Ok(df)
            }
        })
        .await
        .context("Parquet read task panicked")??;

        Ok(df)
    }

    fn list_symbols(&self) -> Result<Vec<String>> {
        let path_str = self.path.to_string_lossy().to_string();
        let df = LazyFrame::scan_parquet(path_str.as_str().into(), ScanArgsParquet::default())?
            .select([col("symbol")])
            .unique(None, UniqueKeepStrategy::First)
            .collect()?;

        let ca = df.column("symbol")?.str()?;
        Ok(ca
            .into_no_null_iter()
            .map(std::string::ToString::to_string)
            .collect())
    }

    fn date_range(&self, _symbol: &str) -> Result<(NaiveDate, NaiveDate)> {
        let path_str = self.path.to_string_lossy().to_string();
        let df = LazyFrame::scan_parquet(path_str.as_str().into(), ScanArgsParquet::default())?
            .collect()?;

        let df = normalize_quote_datetime(df)?;
        let date_col = df.column(QUOTE_DATETIME_COL)?;
        let min = date_col.min_reduce()?;
        let max = date_col.max_reduce()?;

        let start = scalar_to_date(&min)?;
        let end = scalar_to_date(&max)?;

        Ok((start, end))
    }
}

/// Extract a `NaiveDate` from a Polars `Scalar`, handling Date, Datetime, and String types.
fn scalar_to_date(scalar: &Scalar) -> Result<NaiveDate> {
    match scalar.value() {
        AnyValue::Date(days) => NaiveDate::from_num_days_from_ce_opt(*days + EPOCH_DAYS_CE_OFFSET)
            .ok_or_else(|| anyhow::anyhow!("Invalid date value: {days}")),
        AnyValue::Datetime(ts_value, tu, _) => {
            let units_per_sec = match tu {
                TimeUnit::Microseconds => 1_000_000i64,
                TimeUnit::Milliseconds => 1_000i64,
                TimeUnit::Nanoseconds => 1_000_000_000i64,
            };
            let secs = ts_value / units_per_sec;
            let nsecs = ((ts_value % units_per_sec).abs() * (1_000_000_000 / units_per_sec)) as u32;
            DateTime::<Utc>::from_timestamp(secs, nsecs)
                .map(|dt| dt.date_naive())
                .ok_or_else(|| anyhow::anyhow!("Invalid datetime value: {ts_value}"))
        }
        AnyValue::String(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .or_else(|_| NaiveDate::parse_from_str(&s[..10.min(s.len())], "%Y-%m-%d"))
            .context("Failed to parse date string"),
        other => anyhow::bail!("Unexpected scalar type for date: {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn normalize_date_column_to_datetime() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 16).unwrap(),
        ];
        let df = df! {
            "quote_date" => &dates,
            "value" => &[1, 2],
        }
        .unwrap();

        let result = normalize_quote_datetime(df).unwrap();
        assert!(result.schema().contains(QUOTE_DATETIME_COL));
        assert!(!result.schema().contains("quote_date"));
        match result.column(QUOTE_DATETIME_COL).unwrap().dtype() {
            DataType::Datetime(_, _) => {}
            other => panic!("Expected Datetime, got {other:?}"),
        }
    }

    #[test]
    fn normalize_datetime_column_passthrough() {
        let datetimes = vec![
            NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(9, 30, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 16)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        ];
        let df = df! {
            "quote_datetime" => &datetimes,
            "value" => &[1, 2],
        }
        .unwrap();

        let result = normalize_quote_datetime(df).unwrap();
        assert!(result.schema().contains(QUOTE_DATETIME_COL));
        assert_eq!(result.height(), 2);
    }

    #[test]
    fn normalize_string_column_to_datetime() {
        let df = df! {
            "quote_date" => &["2024-01-15", "2024-01-16"],
            "value" => &[1, 2],
        }
        .unwrap();

        let result = normalize_quote_datetime(df).unwrap();
        assert!(result.schema().contains(QUOTE_DATETIME_COL));
        match result.column(QUOTE_DATETIME_COL).unwrap().dtype() {
            DataType::Datetime(_, _) => {}
            other => panic!("Expected Datetime, got {other:?}"),
        }
    }

    #[test]
    fn normalize_no_recognized_column_noop() {
        let df = df! {
            "some_other_col" => &[1, 2],
            "value" => &[3, 4],
        }
        .unwrap();

        let result = normalize_quote_datetime(df.clone()).unwrap();
        assert_eq!(result.schema(), df.schema());
    }
}
