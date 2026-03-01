use anyhow::{Context, Result};
use chrono::NaiveDate;
use polars::prelude::*;
use std::sync::Arc;
use yahoo_finance_api as yahoo;

use crate::data::cache::CachedStore;

use super::response_types::{DateRange, FetchResponse};

pub async fn execute(
    cache: &Arc<CachedStore>,
    symbol: &str,
    category: &str,
    period: &str,
) -> Result<FetchResponse> {
    let upper = symbol.to_uppercase();
    let path = cache.cache_path(&upper, category);

    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
    }

    // Fetch OHLCV data from Yahoo Finance
    let provider = yahoo::YahooConnector::new()
        .context("Failed to create Yahoo Finance connector")?;

    let resp = provider
        .get_quote_range(&upper, "1d", period)
        .await
        .with_context(|| format!("Failed to fetch data for {upper} (period: {period})"))?;

    let quotes = resp
        .quotes()
        .with_context(|| format!("Failed to parse quotes for {upper}"))?;

    if quotes.is_empty() {
        anyhow::bail!("No data returned from Yahoo Finance for {upper} (period: {period})");
    }

    // Build a polars DataFrame from the quotes
    let dates: Vec<NaiveDate> = quotes
        .iter()
        .map(|q| {
            chrono::DateTime::from_timestamp(q.timestamp, 0)
                .map_or_else(|| chrono::Utc::now().naive_utc().date(), |dt| dt.naive_utc().date())
        })
        .collect();

    let open: Vec<f64> = quotes.iter().map(|q| q.open).collect();
    let high: Vec<f64> = quotes.iter().map(|q| q.high).collect();
    let low: Vec<f64> = quotes.iter().map(|q| q.low).collect();
    let close: Vec<f64> = quotes.iter().map(|q| q.close).collect();
    let adjclose: Vec<f64> = quotes.iter().map(|q| q.adjclose).collect();
    let volume: Vec<u64> = quotes.iter().map(|q| q.volume).collect();

    let date_col = Column::new_scalar(
        PlSmallStr::from("date"),
        Scalar::null(DataType::Date),
        dates.len(),
    );
    let mut df = df! {
        "open" => &open,
        "high" => &high,
        "low" => &low,
        "close" => &close,
        "adjclose" => &adjclose,
        "volume" => &volume,
    }?;

    // Replace placeholder date column with actual dates
    let date_series =
        DateChunked::from_naive_date(PlSmallStr::from("date"), dates.iter().copied()).into_column();
    drop(date_col);
    df = df.hstack(&[date_series])?;

    // Reorder so date is first
    df = df.select(["date", "open", "high", "low", "close", "adjclose", "volume"])?;

    let rows = df.height();
    let columns: Vec<String> = df
        .get_column_names()
        .iter()
        .map(std::string::ToString::to_string)
        .collect();

    // Extract date range
    let first_date = dates.first().map(|d| d.format("%Y-%m-%d").to_string());
    let last_date = dates.last().map(|d| d.format("%Y-%m-%d").to_string());

    // Write to parquet
    let file = std::fs::File::create(&path)
        .with_context(|| format!("Failed to create file: {}", path.display()))?;
    ParquetWriter::new(file)
        .finish(&mut df)
        .with_context(|| format!("Failed to write parquet: {}", path.display()))?;

    let file_path = path.display().to_string();
    let summary = format!(
        "Fetched {rows} bars of OHLCV data for {upper} ({period}) and saved to {file_path}."
    );

    Ok(FetchResponse {
        summary,
        rows,
        symbol: upper.clone(),
        file_path,
        date_range: DateRange {
            start: first_date,
            end: last_date,
        },
        columns,
        suggested_next_steps: vec![
            format!("Call load_data with symbol '{upper}' to load this data into memory."),
            format!("Call check_cache_status to verify the cached file for {upper}."),
        ],
    })
}
