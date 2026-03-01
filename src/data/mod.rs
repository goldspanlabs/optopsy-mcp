pub mod parquet;
#[cfg(feature = "postgres")]
pub mod postgres;

use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::*;

#[allow(dead_code)]
pub trait DataStore: Send + Sync {
    fn load_options(
        &self,
        symbol: &str,
        start_date: Option<NaiveDate>,
        end_date: Option<NaiveDate>,
    ) -> Result<DataFrame>;

    fn list_symbols(&self) -> Result<Vec<String>>;

    fn date_range(&self, symbol: &str) -> Result<(NaiveDate, NaiveDate)>;
}
