use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use polars::prelude::*;
use std::path::PathBuf;

use super::DataStore;
use crate::engine::types::EPOCH_DAYS_CE_OFFSET;

/// The canonical timestamp column name used internally by the engine.
pub const DATETIME_COL: &str = "datetime";

/// Offset added when casting options `date` (Date) → `datetime` (Datetime).
/// 15:59:00 aligns with the nearest OHLCV 1-minute bar before market close.
const EOD_OFFSET_US: i64 = (15 * 3600 + 59 * 60) * 1_000_000;

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

            // Options parquets store a `date` (Date) column. Cast to Datetime at
            // 15:59:00 and rename to `datetime` so the engine has a unified column.
            let schema = lazy.collect_schema()?;
            if schema.get("date").is_some() && schema.get(DATETIME_COL).is_none() {
                lazy = lazy.with_column(
                    (col("date").cast(DataType::Datetime(TimeUnit::Microseconds, None))
                        + lit(EOD_OFFSET_US).cast(DataType::Duration(TimeUnit::Microseconds)))
                    .alias(DATETIME_COL),
                );
            }

            // Apply date filters for predicate pushdown
            if let Some(start) = start_dt {
                lazy = lazy.filter(col(DATETIME_COL).gt_eq(lit(start)));
            }
            if let Some(end) = end_dt {
                lazy = lazy.filter(col(DATETIME_COL).lt_eq(lit(end)));
            }

            let df = lazy.collect().context("Failed to read Parquet file")?;
            // Drop the original `date` column if both exist
            if df.schema().contains("date") && df.schema().contains(DATETIME_COL) {
                df.drop("date").context("Failed to drop date column")
            } else {
                Ok(df)
            }
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

        // Options files have `date`, OHLCV files have `datetime`
        let col_name = if df.schema().contains("date") {
            "date"
        } else {
            DATETIME_COL
        };
        let date_col = df.column(col_name)?;
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
    use chrono::Timelike;

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

    /// Write a `DataFrame` with a `date` (Date) column to a temp parquet file,
    /// load it via `ParquetStore::load_options`, and verify it comes back with
    /// a `datetime` (Datetime) column at 15:59:00.
    #[tokio::test]
    async fn load_options_casts_date_to_datetime_at_1559() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 16).unwrap(),
        ];
        let df = df! {
            "date" => &dates,
            "strike" => &[100.0, 105.0],
        }
        .unwrap();

        let tmp = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
        polars::prelude::ParquetWriter::new(std::fs::File::create(tmp.path()).unwrap())
            .finish(&mut df.clone())
            .unwrap();

        let store = ParquetStore::new(&tmp.path().to_string_lossy());
        let result = store.load_options("TEST", None, None).await.unwrap();

        // Should have `datetime` column, not `date`
        assert!(result.schema().contains(DATETIME_COL));
        assert!(!result.schema().contains("date"));

        // Should be Datetime type
        assert!(matches!(
            result.column(DATETIME_COL).unwrap().dtype(),
            DataType::Datetime(_, _)
        ));

        // Both values should be at 15:59:00
        let dt_col_ref = result.column(DATETIME_COL).unwrap();
        for i in 0..result.height() {
            let ndt =
                crate::engine::price_table::extract_datetime_from_column(dt_col_ref, i).unwrap();
            assert_eq!(ndt.time().hour(), 15);
            assert_eq!(ndt.time().minute(), 59);
            assert_eq!(ndt.time().second(), 0);
        }
    }

    /// When the parquet already has a `datetime` column, `load_options` should
    /// pass it through unchanged (no double-cast).
    #[tokio::test]
    async fn load_options_passthrough_existing_datetime() {
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
            "datetime" => &datetimes,
            "strike" => &[100.0, 105.0],
        }
        .unwrap();

        let tmp = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
        polars::prelude::ParquetWriter::new(std::fs::File::create(tmp.path()).unwrap())
            .finish(&mut df.clone())
            .unwrap();

        let store = ParquetStore::new(&tmp.path().to_string_lossy());
        let result = store.load_options("TEST", None, None).await.unwrap();

        assert!(result.schema().contains(DATETIME_COL));
        assert_eq!(result.height(), 2);

        // Times should be preserved, not overwritten to 15:59
        let dt_col_ref = result.column(DATETIME_COL).unwrap();
        let ndt = crate::engine::price_table::extract_datetime_from_column(dt_col_ref, 0).unwrap();
        assert_eq!(ndt.time().hour(), 9);
        assert_eq!(ndt.time().minute(), 30);
    }
}
