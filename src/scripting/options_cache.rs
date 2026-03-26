//! Date-partitioned options cache for O(1) per-bar access.
//!
//! Pre-splits the full options `DataFrame` by date at load time so each bar
//! does O(1) lookup + small-DF filter instead of scanning millions of rows.
//! Optionally pre-computes a `dte` column and pre-filters by expiration type
//! at partition time to avoid redundant work in the per-bar hot path.

use std::collections::HashMap;

use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::*;

use crate::data::parquet::DATETIME_COL;
use crate::engine::filters;
use crate::engine::types::{timestamp_to_naive_datetime, ExpirationFilter};

/// Options data pre-partitioned by quote date for O(1) per-bar access.
pub struct DatePartitionedOptions {
    pub by_date: HashMap<NaiveDate, DataFrame>,
}

impl DatePartitionedOptions {
    /// Build from a full options DataFrame by grouping on the date portion of `datetime`.
    ///
    /// After partitioning each daily slice:
    /// - Pre-computes the `dte` column so `filter_leg_candidates` can skip it.
    /// - Applies the `expiration_filter` to remove non-matching expirations early.
    pub fn from_df(df: &DataFrame, expiration_filter: &ExpirationFilter) -> Result<Self> {
        let dt_col = df.column("datetime")?;
        let dt_ca = dt_col.datetime()?;
        let tu = dt_ca.time_unit();
        let n = df.height();
        let mut date_indices: HashMap<NaiveDate, Vec<u32>> = HashMap::new();

        for i in 0..n {
            if let Some(raw) = dt_ca.phys.get(i) {
                if let Some(ndt) = timestamp_to_naive_datetime(raw, tu) {
                    date_indices.entry(ndt.date()).or_default().push(i as u32);
                }
            }
        }

        let ms_per_day = 86_400_000i64;
        let mut by_date = HashMap::with_capacity(date_indices.len());
        for (date, indices) in date_indices {
            let idx = IdxCa::new("idx".into(), &indices);
            let slice = df.take(&idx)?;

            // Pre-compute DTE column on the daily slice
            let slice = slice
                .lazy()
                .with_column(
                    ((col("expiration").cast(DataType::Date)
                        - col(DATETIME_COL).cast(DataType::Date))
                    .dt()
                    .total_milliseconds(false)
                        / lit(ms_per_day))
                    .cast(DataType::Int32)
                    .alias("dte"),
                )
                .collect()?;

            // Pre-filter by expiration type (Optimization 5)
            let slice = filters::filter_expiration_type(slice, expiration_filter)?;

            if slice.height() > 0 {
                by_date.insert(date, slice);
            }
        }

        Ok(Self { by_date })
    }

    /// Get the options slice for a given date (typically ~5K-10K rows).
    pub fn get(&self, date: NaiveDate) -> Option<&DataFrame> {
        self.by_date.get(&date)
    }
}
