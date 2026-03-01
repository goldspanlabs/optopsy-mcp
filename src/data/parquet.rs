use anyhow::{Context, Result};
use chrono::NaiveDate;
use polars::prelude::*;
use std::path::PathBuf;

use super::DataStore;

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
    } else if let Ok(c) = df.column("data_date") {
        ("data_date", c.dtype().clone())
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
        let df = LazyFrame::scan_parquet(path_str.as_str().into(), ScanArgsParquet::default())?
            .collect()
            .context("Failed to read Parquet file")?;

        // Normalize to quote_datetime
        let mut df = normalize_quote_datetime(df)?;

        // Apply date filters if quote_datetime exists
        if df.schema().contains(QUOTE_DATETIME_COL) {
            if let Some(start) = start_date {
                let start_dt = start.and_hms_opt(0, 0, 0).unwrap();
                df = df
                    .lazy()
                    .filter(col(QUOTE_DATETIME_COL).gt_eq(lit(start_dt)))
                    .collect()?;
            }
            if let Some(end) = end_date {
                let end_dt = end.and_hms_opt(23, 59, 59).unwrap();
                df = df
                    .lazy()
                    .filter(col(QUOTE_DATETIME_COL).lt_eq(lit(end_dt)))
                    .collect()?;
            }
        }

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

        let min_str = format!("{:?}", min.value());
        let max_str = format!("{:?}", max.value());

        let start = NaiveDate::parse_from_str(min_str.trim_matches('"'), "%Y-%m-%d")?;
        let end = NaiveDate::parse_from_str(max_str.trim_matches('"'), "%Y-%m-%d")?;

        Ok((start, end))
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
    fn normalize_data_date_renamed() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 16).unwrap(),
        ];
        let df = df! {
            "data_date" => &dates,
            "value" => &[1, 2],
        }
        .unwrap();

        let result = normalize_quote_datetime(df).unwrap();
        assert!(result.schema().contains(QUOTE_DATETIME_COL));
        assert!(!result.schema().contains("data_date"));
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
