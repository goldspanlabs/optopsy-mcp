//! Return raw OHLCV price bars for a symbol, with optional date filtering and sampling.
//!
//! Reads cached Parquet price data, applies date range filters, and
//! down-samples to the requested limit using evenly-spaced index selection.

use anyhow::{Context, Result};
use polars::prelude::*;
use std::sync::Arc;

use crate::data::cache::CachedStore;

use super::ai_format;
use super::response_types::{DateRange, PriceBar, RawPricesResponse};

use crate::engine::types::EPOCH_DAYS_CE_OFFSET;

/// Extract price bars from an in-memory `DataFrame` with optional date range, row limit,
/// and interval resampling.
#[allow(clippy::too_many_lines)]
pub fn execute(
    df: &DataFrame,
    symbol: &str,
    start_date: Option<&str>,
    end_date: Option<&str>,
    limit: Option<usize>,
    interval: crate::engine::types::Interval,
) -> Result<RawPricesResponse> {
    let date_col_name = crate::engine::stock_sim::detect_date_col(df);
    let mut lazy = df.clone().lazy();

    // Apply optional date filters
    if let Some(start) = start_date {
        let start_date = start
            .parse::<chrono::NaiveDate>()
            .with_context(|| format!("Invalid start_date: {start}"))?;
        if date_col_name == "datetime" {
            let start_dt = start_date.and_hms_opt(0, 0, 0).unwrap();
            lazy = lazy.filter(col(date_col_name).gt_eq(lit(start_dt)));
        } else {
            lazy = lazy.filter(col(date_col_name).gt_eq(lit(start_date)));
        }
    }
    if let Some(end) = end_date {
        let end_date = end
            .parse::<chrono::NaiveDate>()
            .with_context(|| format!("Invalid end_date: {end}"))?;
        if date_col_name == "datetime" {
            let end_next = end_date
                .succ_opt()
                .unwrap_or(end_date)
                .and_hms_opt(0, 0, 0)
                .unwrap();
            lazy = lazy.filter(col(date_col_name).lt(lit(end_next)));
        } else {
            lazy = lazy.filter(col(date_col_name).lt_eq(lit(end_date)));
        }
    }

    // Sort by date/datetime ascending
    lazy = lazy.sort([date_col_name], SortMultipleOptions::default());

    let filtered = lazy.collect()?;

    // Apply interval resampling (passthrough for same-interval / Min1)
    let filtered = crate::engine::stock_sim::resample_ohlcv(&filtered, interval)?;

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
    let opens = output_df.column("open")?.f64()?.clone();
    let highs = output_df.column("high")?.f64()?.clone();
    let lows = output_df.column("low")?.f64()?.clone();
    let closes = output_df.column("close")?.f64()?.clone();
    let vol_col = output_df.column("volume")?;
    let volumes = if let polars::prelude::DataType::UInt64 = vol_col.dtype() {
        vol_col.u64()?.clone()
    } else {
        let casted = vol_col.cast(&polars::prelude::DataType::UInt64)?;
        casted.u64()?.clone()
    };

    // Also try adjclose if available
    let adjcloses = output_df
        .column("adjclose")
        .ok()
        .and_then(|c| c.f64().ok().cloned());

    let mut bars: Vec<PriceBar> = Vec::with_capacity(rows);

    // Intraday path: Datetime column → format with time
    if let Ok(dt_col_ref) = output_df.column(date_col_name) {
        if matches!(
            dt_col_ref.dtype(),
            polars::prelude::DataType::Datetime(_, _)
        ) {
            for i in 0..rows {
                let ndt = crate::engine::price_table::extract_datetime_from_column(dt_col_ref, i)?;
                let date = if ndt.time() == chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap() {
                    ndt.format("%Y-%m-%d").to_string()
                } else {
                    ndt.format("%Y-%m-%dT%H:%M:%S").to_string()
                };
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
            let date_range = DateRange {
                start: bars.first().map(|b| b.date.clone()),
                end: bars.last().map(|b| b.date.clone()),
            };
            return Ok(ai_format::format_raw_prices(
                symbol, total_rows, rows, sampled, date_range, bars,
            ));
        }
    }

    // Daily path: Date column (typically "date")
    let dates = output_df.column(date_col_name)?.date()?.clone();
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
/// Auto-fetches from Yahoo Finance on cache miss.
pub async fn load_and_execute(
    cache: &Arc<CachedStore>,
    symbol: &str,
    start_date: Option<&str>,
    end_date: Option<&str>,
    limit: Option<usize>,
    interval: crate::engine::types::Interval,
) -> Result<RawPricesResponse> {
    let upper = symbol.to_uppercase();

    // Search across OHLCV categories (etf, stocks, futures, indices)
    let path = cache
        .find_ohlcv(&upper)
        .with_context(|| format!("No OHLCV data found for {upper}. Upload parquet to the cache directory."))?;

    // Read parquet into DataFrame
    let df = tokio::task::spawn_blocking(move || -> Result<DataFrame> {
        let args = ScanArgsParquet::default();
        let path_str = path.to_string_lossy();
        let mut df = LazyFrame::scan_parquet(path_str.as_ref().into(), args)?
            .collect()
            .context("Failed to read OHLCV parquet")?;

        // Normalize: rename "quote_datetime" → "datetime" so downstream code
        // (detect_date_col, resample_ohlcv, execute) works uniformly.
        if df.schema().contains("quote_datetime") && !df.schema().contains("datetime") {
            df.rename("quote_datetime", "datetime".into())?;
        }
        Ok(df)
    })
    .await
    .context("Parquet read task panicked")??;

    execute(&df, &upper, start_date, end_date, limit, interval)
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
        let resp = execute(
            &df,
            "SPY",
            None,
            None,
            None,
            crate::engine::types::Interval::Daily,
        )
        .unwrap();

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
        let resp = execute(
            &df,
            "SPY",
            None,
            None,
            Some(3),
            crate::engine::types::Interval::Daily,
        )
        .unwrap();

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
        let resp = execute(
            &df,
            "SPY",
            Some("2024-01-03"),
            Some("2024-01-05"),
            None,
            crate::engine::types::Interval::Daily,
        )
        .unwrap();

        assert_eq!(resp.returned_rows, 3);
        assert_eq!(resp.prices[0].date, "2024-01-03");
        assert_eq!(resp.prices[2].date, "2024-01-05");
    }

    #[test]
    fn limit_larger_than_data_returns_all() {
        let df = make_test_df();
        let resp = execute(
            &df,
            "SPY",
            None,
            None,
            Some(100),
            crate::engine::types::Interval::Daily,
        )
        .unwrap();

        assert_eq!(resp.returned_rows, 5);
        assert!(!resp.sampled);
    }

    #[test]
    fn adjclose_included() {
        let df = make_test_df();
        let resp = execute(
            &df,
            "SPY",
            None,
            None,
            None,
            crate::engine::types::Interval::Daily,
        )
        .unwrap();
        assert_eq!(resp.prices[0].adjclose, Some(101.0));
    }

    #[test]
    fn limit_one_returns_last_row() {
        let df = make_test_df();
        let resp = execute(
            &df,
            "SPY",
            None,
            None,
            Some(1),
            crate::engine::types::Interval::Daily,
        )
        .unwrap();

        assert_eq!(resp.returned_rows, 1);
        assert!(resp.sampled);
        assert_eq!(resp.prices[0].date, "2024-01-08");
    }

    #[test]
    fn sampling_no_duplicate_indices() {
        let df = make_test_df();
        // limit=4 from 5 rows — should produce 4 unique indices with no duplicates
        let resp = execute(
            &df,
            "SPY",
            None,
            None,
            Some(4),
            crate::engine::types::Interval::Daily,
        )
        .unwrap();

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
