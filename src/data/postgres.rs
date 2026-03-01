#[cfg(feature = "postgres")]
use anyhow::Result;
#[cfg(feature = "postgres")]
use chrono::NaiveDate;
#[cfg(feature = "postgres")]
use polars::prelude::*;
#[cfg(feature = "postgres")]
use sqlx::postgres::PgPool;

#[cfg(feature = "postgres")]
use super::DataStore;

#[cfg(feature = "postgres")]
#[allow(dead_code)]
pub struct PostgresStore {
    pool: PgPool,
}

#[cfg(feature = "postgres")]
#[allow(dead_code)]
impl PostgresStore {
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPool::connect(database_url).await?;
        Ok(Self { pool })
    }
}

#[cfg(feature = "postgres")]
impl DataStore for PostgresStore {
    fn load_options(
        &self,
        _symbol: &str,
        _start_date: Option<NaiveDate>,
        _end_date: Option<NaiveDate>,
    ) -> Result<DataFrame> {
        todo!("PostgreSQL backend not yet implemented")
    }

    fn list_symbols(&self) -> Result<Vec<String>> {
        todo!("PostgreSQL backend not yet implemented")
    }

    fn date_range(&self, _symbol: &str) -> Result<(NaiveDate, NaiveDate)> {
        todo!("PostgreSQL backend not yet implemented")
    }
}
