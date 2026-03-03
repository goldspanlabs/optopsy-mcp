use anyhow::{Context, Result};
use polars::prelude::*;
use std::sync::Arc;

use crate::data::cache::CachedStore;

use super::response_types::{DateRange, PriceBar, RawPricesResponse};

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
            let step = total_rows as f64 / max as f64;
            let indices: Vec<u32> = (0..max).map(|i| (i as f64 * step) as u32).collect();
            // Always include the last point
            let mut indices = indices;
            let last = (total_rows - 1) as u32;
            if indices.last() != Some(&last) {
                indices.pop();
                indices.push(last);
            }
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
        let date = dates
            .phys
            .get(i)
            .map(|d| {
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let date = epoch + chrono::Duration::days(i64::from(d));
                date.format("%Y-%m-%d").to_string()
            })
            .unwrap_or_default();

        bars.push(PriceBar {
            date,
            open: opens.get(i).unwrap_or(f64::NAN),
            high: highs.get(i).unwrap_or(f64::NAN),
            low: lows.get(i).unwrap_or(f64::NAN),
            close: closes.get(i).unwrap_or(f64::NAN),
            adjclose: adjcloses.as_ref().and_then(|ac| ac.get(i)),
            volume: volumes.get(i).unwrap_or(0),
        });
    }

    // Extract date range
    let first_date = bars.first().map(|b| b.date.clone());
    let last_date = bars.last().map(|b| b.date.clone());

    let upper = symbol.to_uppercase();
    let summary = if sampled {
        format!(
            "Returning {rows} sampled price bars for {upper} (from {total_rows} total). \
             Use these data points directly to generate charts or perform analysis."
        )
    } else {
        format!(
            "Returning {rows} price bars for {upper}. \
             Use these data points directly to generate charts or perform analysis."
        )
    };

    Ok(RawPricesResponse {
        summary,
        symbol: upper,
        total_rows,
        returned_rows: rows,
        sampled,
        date_range: DateRange {
            start: first_date,
            end: last_date,
        },
        prices: bars,
        suggested_next_steps: vec![
            "Use the prices array to generate a line chart (close prices), candlestick chart (OHLC), or area chart.".to_string(),
            "Combine with backtest equity_curve data to overlay strategy performance on price action.".to_string(),
        ],
    })
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
    let path = cache.cache_path(&upper, "prices")?;

    if !path.exists() {
        anyhow::bail!(
            "No OHLCV price data cached for {upper}. \
             Call fetch_to_parquet({{ symbol: \"{upper}\", category: \"prices\" }}) first."
        );
    }

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
}
