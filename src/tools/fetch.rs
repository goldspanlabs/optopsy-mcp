use anyhow::{Context, Result};
use chrono::NaiveDate;
use polars::prelude::*;
use std::path::PathBuf;
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
    let path = cache.cache_path(&upper, category)?;

    // Fetch OHLCV data from Yahoo Finance (async network call)
    let provider =
        yahoo::YahooConnector::new().context("Failed to create Yahoo Finance connector")?;

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

    // Build a polars DataFrame from the quotes, rejecting rows with invalid timestamps
    let mut dates: Vec<NaiveDate> = Vec::with_capacity(quotes.len());
    let mut open: Vec<f64> = Vec::with_capacity(quotes.len());
    let mut high: Vec<f64> = Vec::with_capacity(quotes.len());
    let mut low: Vec<f64> = Vec::with_capacity(quotes.len());
    let mut close: Vec<f64> = Vec::with_capacity(quotes.len());
    let mut adjclose: Vec<f64> = Vec::with_capacity(quotes.len());
    let mut volume: Vec<u64> = Vec::with_capacity(quotes.len());

    for q in &quotes {
        let Some(dt) = chrono::DateTime::from_timestamp(q.timestamp, 0) else {
            tracing::warn!(timestamp = q.timestamp, "Skipping quote with invalid timestamp");
            continue;
        };
        dates.push(dt.naive_utc().date());
        open.push(q.open);
        high.push(q.high);
        low.push(q.low);
        close.push(q.close);
        adjclose.push(q.adjclose);
        volume.push(q.volume);
    }

    if dates.is_empty() {
        anyhow::bail!("All quotes for {upper} had invalid timestamps");
    }

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

    // Perform blocking filesystem work off the async executor thread
    let file_path = write_parquet(path, df).await?;

    let summary = format!(
        "Fetched {rows} bars of OHLCV data for {upper} ({period}) and saved to {file_path}."
    );

    Ok(FetchResponse {
        summary,
        rows,
        symbol: upper.clone(),
        file_path: file_path.clone(),
        date_range: DateRange {
            start: first_date,
            end: last_date,
        },
        columns,
        suggested_next_steps: vec![
            format!(
                "Use the returned file_path ('{file_path}') to load this OHLCV parquet into memory with your preferred data analysis tools."
            ),
            format!("Call check_cache_status to verify the cached file for {upper}."),
        ],
    })
}

/// Write `df` as Parquet to `path` using a blocking thread pool so the async
/// executor is not stalled by filesystem I/O.
async fn write_parquet(path: PathBuf, mut df: DataFrame) -> Result<String> {
    tokio::task::spawn_blocking(move || {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
        }
        let file = std::fs::File::create(&path)
            .with_context(|| format!("Failed to create file: {}", path.display()))?;
        ParquetWriter::new(file)
            .finish(&mut df)
            .with_context(|| format!("Failed to write parquet: {}", path.display()))?;
        Ok(path.display().to_string())
    })
    .await
    .context("Parquet write task panicked")?
}
