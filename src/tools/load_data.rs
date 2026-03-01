use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::*;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::data::cache::CachedStore;
use crate::data::eodhd::EodhdProvider;
use crate::data::parquet::QUOTE_DATETIME_COL;
use crate::data::DataStore;

use super::ai_format;
use super::response_types::{DateRange, LoadDataResponse};

pub async fn execute(
    data: &Arc<RwLock<Option<DataFrame>>>,
    cache: &Arc<CachedStore>,
    eodhd: Option<&Arc<EodhdProvider>>,
    symbol: &str,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<LoadDataResponse> {
    let symbol = symbol.to_uppercase();
    let start = start_date
        .map(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d"))
        .transpose()?;
    let end = end_date
        .map(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d"))
        .transpose()?;

    // Try loading from cache (local parquet + S3 fallback).
    // On cache miss, attempt EODHD download if configured.
    let df = match cache.load_options(&symbol, start, end) {
        Ok(df) => df,
        Err(cache_err) => {
            if let Some(provider) = eodhd {
                tracing::info!(
                    %symbol,
                    "Cache miss, downloading from EODHD…"
                );
                provider.download_options(&symbol).await?;
                // Retry from cache now that the file has been written
                cache.load_options(&symbol, start, end).map_err(|e| {
                    anyhow::anyhow!(
                        "Downloaded from EODHD but failed to load: {e} (original: {cache_err})"
                    )
                })?
            } else {
                return Err(cache_err);
            }
        }
    };

    let rows = df.height();
    let columns: Vec<String> = df
        .get_column_names()
        .iter()
        .map(std::string::ToString::to_string)
        .collect();

    // Extract unique symbols
    let symbols = if df.schema().contains("symbol") {
        let sym_col = df.column("symbol")?;
        let unique = sym_col.unique()?.sort(SortOptions::default())?;
        let ca = unique.str()?;
        ca.into_no_null_iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>()
    } else if df.schema().contains("underlying_symbol") {
        let sym_col = df.column("underlying_symbol")?;
        let unique = sym_col.unique()?.sort(SortOptions::default())?;
        let ca = unique.str()?;
        ca.into_no_null_iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>()
    } else {
        vec![]
    };

    // Get date range from normalized quote_datetime column, or fallback to quote_date
    let date_range = if df.schema().contains(QUOTE_DATETIME_COL) {
        extract_date_range(&df, QUOTE_DATETIME_COL, start_date, end_date)
    } else if df.schema().contains("quote_date") {
        extract_date_range(&df, "quote_date", start_date, end_date)
    } else {
        DateRange {
            start: start_date.map(std::string::ToString::to_string),
            end: end_date.map(std::string::ToString::to_string),
        }
    };

    let mut guard = data.write().await;
    *guard = Some(df);

    Ok(ai_format::format_load_data(
        rows, symbols, date_range, columns,
    ))
}

fn format_scalar(s: &polars::prelude::Scalar) -> Option<String> {
    match s.value() {
        AnyValue::Null => None,
        AnyValue::Datetime(v, tu, _) => {
            let us = match tu {
                TimeUnit::Nanoseconds => v / 1_000,
                TimeUnit::Microseconds => *v,
                TimeUnit::Milliseconds => v * 1_000,
            };
            let secs = us / 1_000_000;
            let nanos = (us.rem_euclid(1_000_000) * 1_000) as u32;
            chrono::DateTime::from_timestamp(secs, nanos)
                .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S").to_string())
        }
        AnyValue::Date(days) => chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163)
            .map(|d| d.format("%Y-%m-%d").to_string()),
        other => Some(format!("{other}")),
    }
}

fn extract_date_range(
    df: &DataFrame,
    col_name: &str,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> DateRange {
    let Ok(date_col) = df.column(col_name) else {
        return DateRange {
            start: start_date.map(std::string::ToString::to_string),
            end: end_date.map(std::string::ToString::to_string),
        };
    };

    DateRange {
        start: date_col.min_reduce().ok().and_then(|s| format_scalar(&s)),
        end: date_col.max_reduce().ok().and_then(|s| format_scalar(&s)),
    }
}
