pub mod cache;
pub mod parquet;
#[cfg(feature = "postgres")]
pub mod postgres;

use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::*;

pub trait DataStore: Send + Sync {
    #[allow(async_fn_in_trait)]
    async fn load_options(
        &self,
        symbol: &str,
        start_date: Option<NaiveDate>,
        end_date: Option<NaiveDate>,
    ) -> Result<DataFrame>;

    fn list_symbols(&self) -> Result<Vec<String>>;

    fn date_range(&self, symbol: &str) -> Result<(NaiveDate, NaiveDate)>;
}
