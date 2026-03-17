use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use polars::prelude::*;
use std::path::PathBuf;

use super::DataStore;
use crate::engine::types::EPOCH_DAYS_CE_OFFSET;

/// The canonical timestamp column name used across all data files.
pub const DATETIME_COL: &str = "datetime";

pub struct ParquetStore {
    path: PathBuf,
}

impl ParquetStore {
    pub fn new(path: &str) -> Self {
        Self {
            path: PathBuf::from(path),
        }
    }

    /// Convert the stored path to a `PlRefPath`-compatible string for `scan_parquet`.
    fn scan_path(&self) -> String {
        self.path.to_string_lossy().to_string()
    }
}

impl DataStore for ParquetStore {
    async fn load_options(
        &self,
        _symbol: &str,
        start_date: Option<NaiveDate>,
        end_date: Option<NaiveDate>,
    ) -> Result<DataFrame> {
        let path_str = self.scan_path();
        let start_dt = start_date.and_then(|d| d.and_hms_opt(0, 0, 0));
        let end_dt = end_date.and_then(|d| d.and_hms_opt(23, 59, 59));

        let df = tokio::task::spawn_blocking(move || {
            let mut lazy =
                LazyFrame::scan_parquet(path_str.as_str().into(), ScanArgsParquet::default())?;

            // Apply date filters for predicate pushdown
            if let Some(start) = start_dt {
                lazy = lazy.filter(col(DATETIME_COL).gt_eq(lit(start)));
            }
            if let Some(end) = end_dt {
                lazy = lazy.filter(col(DATETIME_COL).lt_eq(lit(end)));
            }

            lazy.collect().context("Failed to read Parquet file")
        })
        .await
        .context("Parquet read task panicked")??;

        Ok(df)
    }

    fn list_symbols(&self) -> Result<Vec<String>> {
        let path_str = self.scan_path();
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
        let path_str = self.scan_path();
        let df = LazyFrame::scan_parquet(path_str.as_str().into(), ScanArgsParquet::default())?
            .collect()?;

        let date_col = df.column(DATETIME_COL)?;
        let min = date_col.min_reduce()?;
        let max = date_col.max_reduce()?;

        let start = scalar_to_date(&min)?;
        let end = scalar_to_date(&max)?;

        Ok((start, end))
    }
}

/// Extract a `NaiveDate` from a Polars `Scalar`, handling Date and Datetime types.
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
        other => anyhow::bail!("Unexpected scalar type for date: {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datetime_col_constant() {
        assert_eq!(DATETIME_COL, "datetime");
    }

    #[test]
    fn scalar_to_date_from_datetime() {
        let dt = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(15, 59, 30)
            .unwrap();
        let us = dt.and_utc().timestamp_micros();
        let scalar = Scalar::new(
            DataType::Datetime(TimeUnit::Microseconds, None),
            AnyValue::Datetime(us, TimeUnit::Microseconds, None),
        );
        let result = scalar_to_date(&scalar).unwrap();
        assert_eq!(result, NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
    }
}
