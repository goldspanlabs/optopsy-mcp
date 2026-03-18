//! Return raw OHLCV price bars for a symbol, with optional date filtering and sampling.
//!
//! Reads cached Parquet price data, applies date range filters, and
//! down-samples to the requested limit using evenly-spaced index selection.

use anyhow::{Context, Result};
use polars::prelude::*;
use std::sync::Arc;
use std::time::Instant;

use crate::data::cache::CachedStore;

use super::ai_format;
use super::ai_helpers::parse_date_param;
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
    tail: Option<bool>,
) -> Result<RawPricesResponse> {
    let date_col_name = crate::engine::stock_sim::detect_date_col(df);
    let mut lazy = df.clone().lazy();

    // Apply optional date filters
    if let Some(start) = start_date {
        let start_date = parse_date_param(start, "start_date")?;
        if date_col_name == "datetime" {
            let start_dt = start_date.and_hms_opt(0, 0, 0).unwrap();
            lazy = lazy.filter(col(date_col_name).gt_eq(lit(start_dt)));
        } else {
            lazy = lazy.filter(col(date_col_name).gt_eq(lit(start_date)));
        }
    }
    if let Some(end) = end_date {
        let end_date = parse_date_param(end, "end_date")?;
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

    // Re-detect date column — resampling may change it (e.g., datetime → date for Daily)
    let date_col_name = crate::engine::stock_sim::detect_date_col(&filtered);

    // For intraday intervals with no explicit start_date, limit to the last 7 calendar days.
    // This keeps response sizes manageable; callers can pass start_date to override.
    //
    // NOTE: `load_and_execute()` already applies an equivalent predicate-pushdown cutoff
    // during the parquet scan, so this branch is redundant when called via that path.
    // It is intentionally kept here so that direct `execute()` calls (e.g. in unit tests)
    // also respect the 7-day cap without requiring a full parquet load.
    let filtered = if interval.is_intraday() && start_date.is_none() && end_date.is_none() {
        let cutoff = (chrono::Utc::now() - chrono::Duration::days(7)).naive_utc();
        if date_col_name == "datetime" {
            filtered
                .clone()
                .lazy()
                .filter(col("datetime").gt_eq(lit(cutoff)))
                .collect()
                .unwrap_or(filtered)
        } else {
            let cutoff_date = cutoff.date();
            filtered
                .clone()
                .lazy()
                .filter(col("date").gt_eq(lit(cutoff_date)))
                .collect()
                .unwrap_or(filtered)
        }
    } else {
        filtered
    };

    let total_rows = filtered.height();

    // Tail mode: take the last N rows (for backward pagination)
    let (output_df, sampled) = if tail.unwrap_or(false) {
        if let Some(max) = limit {
            if total_rows > max {
                (filtered.tail(Some(max)), false)
            } else {
                (filtered, false)
            }
        } else {
            (filtered, false)
        }
    } else if let Some(max) = limit {
        if total_rows > max && max > 0 {
            // Take the first N rows (head). No subsampling — the FE
            // paginates backward via tail mode for older data.
            (filtered.head(Some(max)), false)
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
                let date = ndt.and_utc().timestamp();
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
                start: bars.first().map(|b| b.date),
                end: bars.last().map(|b| b.date),
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
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp();

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
    let date_range = DateRange {
        start: bars.first().map(|b| b.date),
        end: bars.last().map(|b| b.date),
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
/// Reads from the local Parquet cache.
pub async fn load_and_execute(
    cache: &Arc<CachedStore>,
    symbol: &str,
    start_date: Option<&str>,
    end_date: Option<&str>,
    limit: Option<usize>,
    interval: crate::engine::types::Interval,
    tail: Option<bool>,
) -> Result<RawPricesResponse> {
    let t0 = Instant::now();
    let upper = symbol.to_uppercase();

    eprintln!(
        "[get_raw_prices] symbol={upper} interval={interval:?} start={start_date:?} end={end_date:?} limit={limit:?}"
    );

    // Search across OHLCV categories (etf, stocks, futures, indices)
    let path = cache.find_ohlcv(&upper).with_context(|| {
        format!("No OHLCV data found for {upper}. Upload parquet to the cache directory.")
    })?;
    eprintln!("[get_raw_prices] found parquet: {}", path.display());

    // For intraday requests with no start_date, push a 7-day cutoff into
    // the parquet scan so Polars can skip irrelevant row groups (~100MB → ~2MB).
    let intraday_cutoff = if interval.is_intraday() && start_date.is_none() && end_date.is_none() {
        let cutoff = (chrono::Utc::now() - chrono::Duration::days(7)).naive_utc();
        eprintln!("[get_raw_prices] intraday cutoff: {cutoff}");
        Some(cutoff)
    } else {
        None
    };

    // Read parquet into DataFrame (with predicate pushdown for intraday)
    let t1 = Instant::now();
    let df = tokio::task::spawn_blocking(move || -> Result<DataFrame> {
        let args = ScanArgsParquet::default();
        let path_str = path.to_string_lossy();
        let mut lazy = LazyFrame::scan_parquet(path_str.as_ref().into(), args)?;
        if let Some(cutoff) = intraday_cutoff {
            // Detect which date column the file has from the schema
            let schema = lazy.clone().collect_schema()?;
            if schema.get("datetime").is_some() {
                lazy = lazy.filter(col("datetime").gt_eq(lit(cutoff)));
            } else if schema.get("date").is_some() {
                lazy = lazy.filter(col("date").gt_eq(lit(cutoff.date())));
            }
        }
        lazy.collect().context("Failed to read OHLCV parquet")
    })
    .await
    .context("Parquet read task panicked")??;
    eprintln!(
        "[get_raw_prices] parquet loaded: {} rows, {:.0}ms",
        df.height(),
        t1.elapsed().as_millis()
    );

    let t2 = Instant::now();
    let resp = execute(&df, &upper, start_date, end_date, limit, interval, tail)?;
    eprintln!(
        "[get_raw_prices] execute done: {} bars returned, sampled={}, {:.0}ms",
        resp.returned_rows,
        resp.sampled,
        t2.elapsed().as_millis()
    );

    let json_size = serde_json::to_string(&resp).map(|s| s.len()).unwrap_or(0);
    eprintln!(
        "[get_raw_prices] total: {:.0}ms, response JSON ~{}KB",
        t0.elapsed().as_millis(),
        json_size / 1024
    );

    Ok(resp)
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
            None,
        )
        .unwrap();

        assert_eq!(resp.symbol, "SPY");
        assert_eq!(resp.total_rows, 5);
        assert_eq!(resp.returned_rows, 5);
        assert!(!resp.sampled);
        assert_eq!(resp.prices.len(), 5);
        // 2024-01-02 00:00:00 UTC = 1704153600
        assert_eq!(resp.prices[0].date, 1_704_153_600);
        assert_eq!(resp.prices[0].open, 100.0);
        // 2024-01-08 00:00:00 UTC = 1704672000
        assert_eq!(resp.prices[4].date, 1_704_672_000);
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
            None,
        )
        .unwrap();

        assert_eq!(resp.total_rows, 5);
        assert_eq!(resp.returned_rows, 3);
        assert!(!resp.sampled);
        // Head truncation: first 3 rows
        assert_eq!(resp.prices[0].date, 1_704_153_600); // 2024-01-02
        assert_eq!(resp.prices[2].date, 1_704_326_400); // 2024-01-04
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
            None,
        )
        .unwrap();

        assert_eq!(resp.returned_rows, 3);
        // 2024-01-03 = 1704240000, 2024-01-05 = 1704412800
        assert_eq!(resp.prices[0].date, 1_704_240_000);
        assert_eq!(resp.prices[2].date, 1_704_412_800);
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
            None,
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
            None,
        )
        .unwrap();
        assert_eq!(resp.prices[0].adjclose, Some(101.0));
    }

    #[test]
    fn limit_one_returns_first_row() {
        let df = make_test_df();
        let resp = execute(
            &df,
            "SPY",
            None,
            None,
            Some(1),
            crate::engine::types::Interval::Daily,
            None,
        )
        .unwrap();

        assert_eq!(resp.returned_rows, 1);
        assert!(!resp.sampled);
        // Head truncation: first row
        assert_eq!(resp.prices[0].date, 1_704_153_600); // 2024-01-02
    }

    #[test]
    fn limit_returns_head_not_sampled() {
        let df = make_test_df();
        // limit=4 from 5 rows — should return the first 4 rows
        let resp = execute(
            &df,
            "SPY",
            None,
            None,
            Some(4),
            crate::engine::types::Interval::Daily,
            None,
        )
        .unwrap();

        assert_eq!(resp.returned_rows, 4);
        assert!(!resp.sampled);
        assert_eq!(resp.prices[0].date, 1_704_153_600); // 2024-01-02
        assert_eq!(resp.prices[3].date, 1_704_412_800); // 2024-01-05
    }
}
