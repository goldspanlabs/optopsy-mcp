use anyhow::{Context, Result};
use chrono::NaiveDate;
use polars::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;
use yahoo_finance_api as yahoo;

use crate::data::cache::CachedStore;

use super::response_types::{DateRange, FetchResponse};

/// Trait for fetching quotes. Allows mocking in tests.
#[async_trait::async_trait]
pub trait QuoteProvider: Send + Sync {
    async fn fetch_quotes(&self, symbol: &str, period: &str) -> Result<Vec<yahoo::Quote>>;
}

/// Real implementation using Yahoo Finance API.
pub struct YahooQuoteProvider;

#[async_trait::async_trait]
impl QuoteProvider for YahooQuoteProvider {
    async fn fetch_quotes(&self, symbol: &str, period: &str) -> Result<Vec<yahoo::Quote>> {
        let provider =
            yahoo::YahooConnector::new().context("Failed to create Yahoo Finance connector")?;
        let resp = provider
            .get_quote_range(symbol, "1d", period)
            .await
            .with_context(|| format!("Failed to fetch data for {symbol} (period: {period})"))?;
        resp.quotes()
            .with_context(|| format!("Failed to parse quotes for {symbol}"))
    }
}

pub async fn execute(
    cache: &Arc<CachedStore>,
    symbol: &str,
    category: &str,
    period: &str,
) -> Result<FetchResponse> {
    let provider = YahooQuoteProvider;
    execute_with_provider(cache, symbol, category, period, &provider).await
}

pub async fn execute_with_provider(
    cache: &Arc<CachedStore>,
    symbol: &str,
    category: &str,
    period: &str,
    provider: &dyn QuoteProvider,
) -> Result<FetchResponse> {
    let upper = symbol.to_uppercase();
    let path = cache.cache_path(&upper, category)?;

    let quotes = provider.fetch_quotes(&upper, period).await?;

    if quotes.is_empty() {
        anyhow::bail!("No data returned for {upper} (period: {period})");
    }

    let df = build_dataframe_from_quotes(&quotes, &upper)?;
    let rows = df.height();
    let columns: Vec<String> = df
        .get_column_names()
        .iter()
        .map(std::string::ToString::to_string)
        .collect();

    // Extract date range
    let mut dates: Vec<NaiveDate> = Vec::new();
    for q in &quotes {
        if let Some(dt) = chrono::DateTime::from_timestamp(q.timestamp, 0) {
            dates.push(dt.naive_utc().date());
        }
    }

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

fn build_dataframe_from_quotes(quotes: &[yahoo::Quote], symbol: &str) -> Result<DataFrame> {
    let mut dates: Vec<NaiveDate> = Vec::with_capacity(quotes.len());
    let mut open: Vec<f64> = Vec::with_capacity(quotes.len());
    let mut high: Vec<f64> = Vec::with_capacity(quotes.len());
    let mut low: Vec<f64> = Vec::with_capacity(quotes.len());
    let mut close: Vec<f64> = Vec::with_capacity(quotes.len());
    let mut adjclose: Vec<f64> = Vec::with_capacity(quotes.len());
    let mut volume: Vec<u64> = Vec::with_capacity(quotes.len());

    for q in quotes {
        let Some(dt) = chrono::DateTime::from_timestamp(q.timestamp, 0) else {
            tracing::warn!(
                timestamp = q.timestamp,
                "Skipping quote with invalid timestamp"
            );
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
        anyhow::bail!("All quotes for {symbol} had invalid timestamps");
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

    Ok(df)
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

#[cfg(test)]
mod tests {
    use super::*;

    struct MockQuoteProviderFixture {
        quotes: Vec<yahoo::Quote>,
    }

    #[async_trait::async_trait]
    impl QuoteProvider for MockQuoteProviderFixture {
        async fn fetch_quotes(&self, _symbol: &str, _period: &str) -> Result<Vec<yahoo::Quote>> {
            Ok(self.quotes.clone())
        }
    }

    fn make_mock_quotes() -> Vec<yahoo::Quote> {
        vec![
            yahoo::Quote {
                timestamp: 1_704_067_200, // 2024-01-01
                open: 100.0,
                high: 102.0,
                low: 99.0,
                close: 101.0,
                adjclose: 101.0,
                volume: 1_000_000,
            },
            yahoo::Quote {
                timestamp: 1_704_153_600, // 2024-01-02
                open: 101.0,
                high: 103.0,
                low: 100.5,
                close: 102.5,
                adjclose: 102.5,
                volume: 1_100_000,
            },
            yahoo::Quote {
                timestamp: 1_704_240_000, // 2024-01-03
                open: 102.5,
                high: 104.0,
                low: 102.0,
                close: 103.5,
                adjclose: 103.5,
                volume: 1_050_000,
            },
        ]
    }

    #[test]
    fn build_dataframe_from_quotes_creates_correct_structure() {
        let quotes = make_mock_quotes();
        let df = build_dataframe_from_quotes(&quotes, "SPY").unwrap();

        assert_eq!(df.height(), 3);
        assert_eq!(df.width(), 7);

        // Verify column names
        let col_names = df.get_column_names();
        let columns: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            columns,
            vec!["date", "open", "high", "low", "close", "adjclose", "volume"]
        );
    }

    #[test]
    fn build_dataframe_from_quotes_rejects_invalid_timestamps() {
        let mut quotes = make_mock_quotes();
        quotes.push(yahoo::Quote {
            timestamp: i64::MAX, // Invalid timestamp
            open: 100.0,
            high: 102.0,
            low: 99.0,
            close: 101.0,
            adjclose: 101.0,
            volume: 1_000_000,
        });

        let df = build_dataframe_from_quotes(&quotes, "SPY").unwrap();
        // Should have 3 rows (the invalid one is skipped)
        assert_eq!(df.height(), 3);
    }

    #[test]
    fn build_dataframe_from_quotes_rejects_all_invalid_timestamps() {
        let quotes = vec![yahoo::Quote {
            timestamp: i64::MAX,
            open: 100.0,
            high: 102.0,
            low: 99.0,
            close: 101.0,
            adjclose: 101.0,
            volume: 1_000_000,
        }];

        let result = build_dataframe_from_quotes(&quotes, "SPY");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid timestamps"));
    }

    #[tokio::test]
    async fn execute_with_provider_creates_response() {
        let tmp = tempfile::TempDir::new().unwrap();
        let cache = Arc::new(CachedStore::new(
            tmp.path().to_path_buf(),
            "prices".to_string(),
            None,
        ));

        let mock_provider = MockQuoteProviderFixture {
            quotes: make_mock_quotes(),
        };

        let response = execute_with_provider(&cache, "SPY", "prices", "1mo", &mock_provider)
            .await
            .unwrap();

        assert_eq!(response.symbol, "SPY");
        assert_eq!(response.rows, 3);
        assert_eq!(
            response.columns,
            vec!["date", "open", "high", "low", "close", "adjclose", "volume"]
        );
        assert!(response.file_path.ends_with(".parquet"));
        assert_eq!(response.date_range.start, Some("2024-01-01".to_string()));
        assert_eq!(response.date_range.end, Some("2024-01-03".to_string()));

        // Verify file was created
        assert!(std::path::Path::new(&response.file_path).exists());
    }

    #[tokio::test]
    async fn execute_with_provider_rejects_empty_quotes() {
        let tmp = tempfile::TempDir::new().unwrap();
        let cache = Arc::new(CachedStore::new(
            tmp.path().to_path_buf(),
            "prices".to_string(),
            None,
        ));

        let mock_provider = MockQuoteProviderFixture { quotes: vec![] };

        let result = execute_with_provider(&cache, "SPY", "prices", "1mo", &mock_provider).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No data returned"));
    }
}
