//! Data layer for loading, caching, and normalizing options chain data.
//!
//! Provides a `DataStore` trait with a `CachedStore` implementation that uses
//! local Parquet files with optional S3 fetch-on-miss.

pub mod cache;
pub mod parquet;

use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::*;

/// Trait for loading options chain data from a backend store.
pub trait DataStore: Send + Sync {
    /// Load options chain data for a symbol with optional date range filtering.
    #[allow(async_fn_in_trait)]
    async fn load_options(
        &self,
        symbol: &str,
        start_date: Option<NaiveDate>,
        end_date: Option<NaiveDate>,
    ) -> Result<DataFrame>;

    /// List all symbols available in the store.
    fn list_symbols(&self) -> Result<Vec<String>>;

    /// Return the earliest and latest dates available for a symbol.
    fn date_range(&self, symbol: &str) -> Result<(NaiveDate, NaiveDate)>;
}
