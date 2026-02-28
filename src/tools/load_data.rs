use std::sync::Arc;
use tokio::sync::RwLock;
use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::*;
use serde_json::json;

use crate::data::parquet::{ParquetStore, QUOTE_DATETIME_COL};
use crate::data::DataStore;

pub async fn execute(
    data: &Arc<RwLock<Option<DataFrame>>>,
    file_path: &str,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<String> {
    let store = ParquetStore::new(file_path);

    let start = start_date
        .map(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d"))
        .transpose()?;
    let end = end_date
        .map(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d"))
        .transpose()?;

    let df = store.load_options("", start, end)?;

    let rows = df.height();
    let columns: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();

    // Extract unique symbols
    let symbols = if df.schema().contains("symbol") {
        let sym_col = df.column("symbol")?;
        let unique = sym_col.unique()?.sort(Default::default())?;
        let ca = unique.str()?;
        ca.into_no_null_iter().map(|s| s.to_string()).collect::<Vec<_>>()
    } else {
        vec![]
    };

    // Get date range from normalized quote_datetime column
    let date_range = if df.schema().contains(QUOTE_DATETIME_COL) {
        let date_col = df.column(QUOTE_DATETIME_COL)?;
        let min_scalar = date_col.min_reduce()?;
        let max_scalar = date_col.max_reduce()?;
        let min_val = format!("{:?}", min_scalar.value());
        let max_val = format!("{:?}", max_scalar.value());
        json!({ "start": min_val, "end": max_val })
    } else {
        json!({ "start": null, "end": null })
    };

    let mut guard = data.write().await;
    *guard = Some(df);

    let result = json!({
        "rows": rows,
        "symbols": symbols,
        "date_range": date_range,
        "columns": columns,
    });

    Ok(serde_json::to_string_pretty(&result)?)
}
