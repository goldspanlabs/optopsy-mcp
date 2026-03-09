use anyhow::{Context, Result};
use polars::prelude::*;
use std::sync::Arc;

use crate::data::cache::CachedStore;

use super::ai_format;
use super::response_types::{DateRange, PriceBar, RawPricesResponse};

use crate::engine::types::EPOCH_DAYS_CE_OFFSET;

pub fn execute(
    df: &DataFrame,
    symbol: &str,
    start_date: Option<&str>,
    end_date: Option<&str>,
    limit: Option<usize>,
) -> Result<RawPricesResponse> {
    let mut lazy = df.clone().lazy();

    // Apply optional date filters
    if let Some(start) = start_date {
        let start_date = start
            .parse::<chrono::NaiveDate>()
            .with_context(|| format!("Invalid start_date: {start}"))?;
        lazy = lazy.filter(col("date").gt_eq(lit(start_date)));
    }
    if let Some(end) = end_date {
        let end_date = end
            .parse::<chrono::NaiveDate>()
            .with_context(|| format!("Invalid end_date: {end}"))?;
        lazy = lazy.filter(col("date").lt_eq(lit(end_date)));
    }

    // Sort by date ascending
    lazy = lazy.sort(["date"], SortMultipleOptions::default());

    let filtered = lazy.collect()?;
    let total_rows = filtered.height();

    // Sample if limit is specified and data exceeds it
    let (output_df, sampled) = if let Some(max) = limit {
        if total_rows > max && max > 0 {
            // Use integer math to produce evenly-spaced, deduplicated indices.
            // Always include the first and last row.
            let mut indices: Vec<u32> = if max == 1 {
                vec![(total_rows - 1) as u32]
            } else {
                (0..max)
                    .map(|i| (i * (total_rows - 1) / (max - 1)) as u32)
                    .collect()
            };
            indices.dedup();
            let idx = IdxCa::new(PlSmallStr::from("idx"), &indices);
            (filtered.take(&idx)?, true)
        } else {
            (filtered, false)
        }
    } else {
        (filtered, false)
    };

    let rows = output_df.height();

    // Extract columns into PriceBar structs
    let dates = output_df.column("date")?.date()?.clone();
    let opens = output_df.column("open")?.f64()?.clone();
    let highs = output_df.column("high")?.f64()?.clone();
    let lows = output_df.column("low")?.f64()?.clone();
    let closes = output_df.column("close")?.f64()?.clone();
    let volumes = output_df.column("volume")?.u64()?.clone();

    // Also try adjclose if available
    let adjcloses = output_df
        .column("adjclose")
        .ok()
        .and_then(|c| c.f64().ok().cloned());

    let mut bars: Vec<PriceBar> = Vec::with_capacity(rows);
    for i in 0..rows {
        let days_since_epoch = dates
            .phys
            .get(i)
            .ok_or_else(|| anyhow::anyhow!("Null date at row {i}; OHLCV data may be corrupted"))?;
        let date =
            chrono::NaiveDate::from_num_days_from_ce_opt(days_since_epoch + EPOCH_DAYS_CE_OFFSET)
                .ok_or_else(|| anyhow::anyhow!("Invalid date value at row {i}"))?
                .format("%Y-%m-%d")
                .to_string();

        bars.push(PriceBar {
            date,
            open: opens.get(i).unwrap_or(0.0),
            high: highs.get(i).unwrap_or(0.0),
            low: lows.get(i).unwrap_or(0.0),
            close: closes.get(i).unwrap_or(0.0),
            adjclose: adjcloses.as_ref().and_then(|ac| ac.get(i)),
            volume: volumes.get(i).unwrap_or(0),
        });
    }

    // Extract date range
    let first_date = bars.first().map(|b| b.date.clone());
    let last_date = bars.last().map(|b| b.date.clone());

    let date_range = DateRange {
        start: first_date,
        end: last_date,
    };

    Ok(ai_format::format_raw_prices(
        &symbol.to_uppercase(),
        total_rows,
        rows,
        sampled,
        date_range,
        bars,
    ))
}

/// Load OHLCV parquet from cache and return raw prices.
pub async fn load_and_execute(
    cache: &Arc<CachedStore>,
    symbol: &str,
    start_date: Option<&str>,
    end_date: Option<&str>,
    limit: Option<usize>,
) -> Result<RawPricesResponse> {
    let upper = symbol.to_uppercase();
    let path = cache
        .ensure_local_for(&upper, "prices")
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "No OHLCV price data cached for {upper}. \
             Call fetch_to_parquet({{ symbol: \"{upper}\", category: \"prices\" }}) first."
            )
        })?;

    // Read parquet into DataFrame
    let df = tokio::task::spawn_blocking(move || -> Result<DataFrame> {
        let args = ScanArgsParquet::default();
        let path_str = path.to_string_lossy();
        LazyFrame::scan_parquet(path_str.as_ref().into(), args)?
            .collect()
            .context("Failed to read OHLCV parquet")
    })
    .await
    .context("Parquet read task panicked")??;

    execute(&df, &upper, start_date, end_date, limit)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_df() -> DataFrame {
        let dates = DateChunked::from_naive_date(
            PlSmallStr::from("date"),
            [
                chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
                chrono::NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
                chrono::NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(),
                chrono::NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(),
                chrono::NaiveDate::from_ymd_opt(2024, 1, 8).unwrap(),
            ],
        )
        .into_column();

        df! {
            "open" => &[100.0, 101.0, 102.5, 99.0, 103.0_f64],
            "high" => &[102.0, 103.0, 104.0, 101.0, 105.0_f64],
            "low" => &[99.0, 100.5, 102.0, 98.5, 102.5_f64],
            "close" => &[101.0, 102.5, 103.5, 100.0, 104.5_f64],
            "adjclose" => &[101.0, 102.5, 103.5, 100.0, 104.5_f64],
            "volume" => &[1_000_000_u64, 1_100_000, 1_050_000, 1_200_000, 900_000],
        }
        .unwrap()
        .hstack(&[dates])
        .unwrap()
        .select(["date", "open", "high", "low", "close", "adjclose", "volume"])
        .unwrap()
    }

    #[test]
    fn returns_all_bars_without_limit() {
        let df = make_test_df();
        let resp = execute(&df, "SPY", None, None, None).unwrap();

        assert_eq!(resp.symbol, "SPY");
        assert_eq!(resp.total_rows, 5);
        assert_eq!(resp.returned_rows, 5);
        assert!(!resp.sampled);
        assert_eq!(resp.prices.len(), 5);
        assert_eq!(resp.prices[0].date, "2024-01-02");
        assert_eq!(resp.prices[0].open, 100.0);
        assert_eq!(resp.prices[4].date, "2024-01-08");
    }

    #[test]
    fn samples_when_limit_exceeded() {
        let df = make_test_df();
        let resp = execute(&df, "SPY", None, None, Some(3)).unwrap();

        assert_eq!(resp.total_rows, 5);
        assert_eq!(resp.returned_rows, 3);
        assert!(resp.sampled);
        // Should include first and last
        assert_eq!(resp.prices[0].date, "2024-01-02");
        assert_eq!(resp.prices[2].date, "2024-01-08");
    }

    #[test]
    fn date_filter_works() {
        let df = make_test_df();
        let resp = execute(&df, "SPY", Some("2024-01-03"), Some("2024-01-05"), None).unwrap();

        assert_eq!(resp.returned_rows, 3);
        assert_eq!(resp.prices[0].date, "2024-01-03");
        assert_eq!(resp.prices[2].date, "2024-01-05");
    }

    #[test]
    fn limit_larger_than_data_returns_all() {
        let df = make_test_df();
        let resp = execute(&df, "SPY", None, None, Some(100)).unwrap();

        assert_eq!(resp.returned_rows, 5);
        assert!(!resp.sampled);
    }

    #[test]
    fn adjclose_included() {
        let df = make_test_df();
        let resp = execute(&df, "SPY", None, None, None).unwrap();
        assert_eq!(resp.prices[0].adjclose, Some(101.0));
    }

    #[test]
    fn limit_one_returns_last_row() {
        let df = make_test_df();
        let resp = execute(&df, "SPY", None, None, Some(1)).unwrap();

        assert_eq!(resp.returned_rows, 1);
        assert!(resp.sampled);
        assert_eq!(resp.prices[0].date, "2024-01-08");
    }

    #[test]
    fn sampling_no_duplicate_indices() {
        let df = make_test_df();
        // limit=4 from 5 rows — should produce 4 unique indices with no duplicates
        let resp = execute(&df, "SPY", None, None, Some(4)).unwrap();

        assert_eq!(resp.returned_rows, 4);
        assert!(resp.sampled);
        // First and last should be included
        assert_eq!(resp.prices[0].date, "2024-01-02");
        assert_eq!(resp.prices[3].date, "2024-01-08");
        // All dates should be unique
        let dates: Vec<&str> = resp.prices.iter().map(|p| p.date.as_str()).collect();
        let mut deduped = dates.clone();
        deduped.dedup();
        assert_eq!(dates, deduped, "Sampling produced duplicate dates");
    }
}
