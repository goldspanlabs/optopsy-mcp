//! General OHLCV `DataFrame` utilities: loading, parsing, resampling.
//!
//! Shared across the scripting engine, optimization tools, signals,
//! and server handlers.

use anyhow::Result;
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use std::collections::HashSet;

use super::types::{Interval, SessionFilter};

/// A single OHLCV bar for simulation (daily or intraday).
#[derive(Debug, Clone)]
pub struct Bar {
    pub datetime: NaiveDateTime,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
}

/// Detect the date/datetime column name present in the `DataFrame`.
///
/// Returns `"datetime"` only if the column exists **and** has a `Datetime` dtype,
/// otherwise falls back to `"date"`.
pub fn detect_date_col(df: &polars::prelude::DataFrame) -> &'static str {
    if let Ok(col) = df.column("datetime") {
        if matches!(col.dtype(), polars::prelude::DataType::Datetime(_, _)) {
            return "datetime";
        }
    }
    "date"
}

/// Load an OHLCV parquet file into a `DataFrame`, applying date range and
/// validity filters via Polars lazy predicates for predicate pushdown.
///
/// Supports both daily files (`"date"` Date column) and intraday files
/// (`"datetime"` Datetime column). Detection is done by schema inspection.
pub fn load_ohlcv_df(
    ohlcv_path: &str,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
) -> Result<polars::prelude::DataFrame> {
    use polars::prelude::*;

    let args = ScanArgsParquet::default();
    let mut lazy_base = LazyFrame::scan_parquet(ohlcv_path.into(), args)?;

    // Inspect schema to determine whether this file uses "datetime" or "date".
    let schema = lazy_base.collect_schema()?;
    let date_col_name = if schema
        .get("datetime")
        .is_some_and(|dt| matches!(dt, DataType::Datetime(_, _)))
    {
        "datetime"
    } else {
        "date"
    };

    let mut lazy = lazy_base.filter(col("open").gt(lit(0.0)).and(col("close").gt(lit(0.0))));

    if let Some(start) = start_date {
        if date_col_name == "datetime" {
            // Promote NaiveDate to midnight NaiveDateTime for Datetime column comparison
            let start_dt = start
                .and_hms_opt(0, 0, 0)
                .expect("midnight datetime for start_date filter");
            lazy = lazy.filter(col(date_col_name).gt_eq(lit(start_dt)));
        } else {
            lazy = lazy.filter(col(date_col_name).gt_eq(lit(start)));
        }
    }
    if let Some(end) = end_date {
        if date_col_name == "datetime" {
            // Use next day at midnight with < to include all bars on the end date
            let end_next = end
                .succ_opt()
                .unwrap_or(end)
                .and_hms_opt(0, 0, 0)
                .expect("midnight datetime for end_date filter");
            lazy = lazy.filter(col(date_col_name).lt(lit(end_next)));
        } else {
            lazy = lazy.filter(col(date_col_name).lt_eq(lit(end)));
        }
    }

    let df = lazy
        .sort([date_col_name], SortMultipleOptions::default())
        .collect()?;
    Ok(df)
}

/// Convert an already-loaded OHLCV `DataFrame` into `Bar` structs.
///
/// Supports both daily data (`"date"` Date column) and intraday data
/// (`"datetime"` Datetime column). When a `"datetime"` column is present,
/// it is used; otherwise falls back to `"date"` (promoted to midnight).
pub fn bars_from_df(df: &polars::prelude::DataFrame) -> Result<Vec<Bar>> {
    let opens = df
        .column("open")
        .map_err(|e| anyhow::anyhow!("Missing 'open' column: {e}"))?
        .f64()
        .map_err(|e| anyhow::anyhow!("'open' not f64: {e}"))?;

    let highs = df
        .column("high")
        .map_err(|e| anyhow::anyhow!("Missing 'high' column: {e}"))?
        .f64()
        .map_err(|e| anyhow::anyhow!("'high' not f64: {e}"))?;

    let lows = df
        .column("low")
        .map_err(|e| anyhow::anyhow!("Missing 'low' column: {e}"))?
        .f64()
        .map_err(|e| anyhow::anyhow!("'low' not f64: {e}"))?;

    let closes = df
        .column("close")
        .map_err(|e| anyhow::anyhow!("Missing 'close' column: {e}"))?
        .f64()
        .map_err(|e| anyhow::anyhow!("'close' not f64: {e}"))?;

    let epoch_offset = super::types::EPOCH_DAYS_CE_OFFSET;
    let mut bars = Vec::with_capacity(df.height());

    // Intraday path: "datetime" column (Datetime type)
    let date_col_name = detect_date_col(df);
    if date_col_name == "datetime" {
        let dt_col = df.column("datetime")?;
        let dt_chunked = dt_col.datetime()?;
        let tu = dt_chunked.time_unit();
        // Use zipped iterators for OHLCV columns; extract datetime from raw i64 values
        let iter = dt_chunked
            .phys
            .iter()
            .zip(opens.iter())
            .zip(highs.iter())
            .zip(lows.iter())
            .zip(closes.iter());

        for ((((ts_opt, open_opt), high_opt), low_opt), close_opt) in iter {
            let (Some(ts), Some(open), Some(high), Some(low), Some(close)) =
                (ts_opt, open_opt, high_opt, low_opt, close_opt)
            else {
                continue;
            };
            if open <= 0.0 || close <= 0.0 {
                continue;
            }
            let Some(datetime) = super::types::timestamp_to_naive_datetime(ts, tu) else {
                continue;
            };
            bars.push(Bar {
                datetime,
                open,
                high,
                low,
                close,
            });
        }
        return Ok(bars);
    }

    // Daily path: "date" column (Date type) → promote to midnight NaiveDateTime
    let dates = df
        .column("date")
        .map_err(|e| anyhow::anyhow!("Missing 'date' or 'datetime' column: {e}"))?
        .date()
        .map_err(|e| anyhow::anyhow!("'date' column is not Date type: {e}"))?;

    // Use zipped iterators instead of per-element .get(i) for better cache locality
    let iter = dates
        .phys
        .iter()
        .zip(opens.iter())
        .zip(highs.iter())
        .zip(lows.iter())
        .zip(closes.iter());

    for ((((day_opt, open_opt), high_opt), low_opt), close_opt) in iter {
        let (Some(days), Some(open), Some(high), Some(low), Some(close)) =
            (day_opt, open_opt, high_opt, low_opt, close_opt)
        else {
            continue;
        };
        if open <= 0.0 || close <= 0.0 {
            continue;
        }
        let Some(date) = NaiveDate::from_num_days_from_ce_opt(days + epoch_offset) else {
            continue;
        };
        let datetime = date
            .and_hms_opt(0, 0, 0)
            .expect("midnight datetime for OHLCV date conversion");

        bars.push(Bar {
            datetime,
            open,
            high,
            low,
            close,
        });
    }

    Ok(bars)
}

/// Parse OHLCV parquet into `Bar` structs, optionally filtering by date range.
///
/// Convenience wrapper that calls `load_ohlcv_df` + `bars_from_df`.
pub fn parse_ohlcv_bars(
    ohlcv_path: &str,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
) -> Result<Vec<Bar>> {
    let df = load_ohlcv_df(ohlcv_path, start_date, end_date)?;
    bars_from_df(&df)
}

/// Filter a `DataFrame` with a `"datetime"` column by session time window.
///
/// Returns the input unchanged if `session` is `None` or the `DataFrame` has no
/// `"datetime"` column (daily data).
pub fn filter_session(
    df: &polars::prelude::DataFrame,
    session: Option<&SessionFilter>,
) -> Result<polars::prelude::DataFrame> {
    use polars::prelude::*;

    let Some(filter) = session else {
        return Ok(df.clone());
    };

    // Only applies to DataFrames with a Datetime-typed "datetime" column
    let has_datetime = df
        .column("datetime")
        .ok()
        .is_some_and(|c| matches!(c.dtype(), DataType::Datetime(_, _)));
    if !has_datetime {
        return Ok(df.clone());
    }

    let (start_time, end_time) = filter.time_range();

    // Use hour/minute comparisons instead of raw time units to be
    // independent of Polars internal Time representation.
    // Build start/end filter using separate hour and minute comparisons.
    // time >= start: (hour > start_h) OR (hour == start_h AND minute >= start_m)
    // time < end:    (hour < end_h)   OR (hour == end_h   AND minute < end_m)
    let sh = lit(i64::from(start_time.hour()));
    let sm = lit(i64::from(start_time.minute()));
    let eh = lit(i64::from(end_time.hour()));
    let em = lit(i64::from(end_time.minute()));

    let hour = col("datetime").dt().hour().cast(DataType::Int64);
    let minute = col("datetime").dt().minute().cast(DataType::Int64);

    let ge_start = hour
        .clone()
        .gt(sh.clone())
        .or(hour.clone().eq(sh).and(minute.clone().gt_eq(sm)));
    let lt_end = hour
        .clone()
        .lt(eh.clone())
        .or(hour.eq(eh).and(minute.lt(em)));

    let filtered = df.clone().lazy().filter(ge_start.and(lt_end)).collect()?;

    Ok(filtered)
}

/// Cast a `"volume"` column to `Int64`, accepting either `Int64` or `UInt64` input.
///
/// Yahoo-fetched daily Parquets commonly store volume as `UInt64`, while
/// resampled output and some intraday sources use `Int64`. This helper
/// normalizes both to `Int64` so downstream code doesn't need to branch.
pub fn volume_as_i64(
    df: &polars::prelude::DataFrame,
) -> Result<polars::prelude::datatypes::Int64Chunked> {
    use polars::prelude::*;
    let vol = df.column("volume")?;
    match vol.dtype() {
        DataType::Int64 => Ok(vol.i64()?.clone()),
        DataType::UInt64 | DataType::Float64 => {
            let casted = vol.cast(&DataType::Int64)?;
            Ok(casted.i64()?.clone())
        }
        other => {
            anyhow::bail!("Unexpected volume dtype: {other:?}, expected Int64, UInt64, or Float64")
        }
    }
}

/// Resample OHLCV data to a different interval.
///
/// Supports both daily data (`"date"` Date column) and intraday data
/// (`"datetime"` Datetime column). Groups rows by interval boundary and
/// aggregates: open=first, high=max, low=min, close=last, adjclose=last,
/// volume=sum.
///
/// Output column type:
/// - Daily/Weekly/Monthly target -> `"date"` (Date) for backward compat
/// - Intraday target (Min5/Min30/Hour1) -> `"datetime"` (Datetime)
#[allow(clippy::too_many_lines)]
pub fn resample_ohlcv(
    df: &polars::prelude::DataFrame,
    interval: Interval,
) -> Result<polars::prelude::DataFrame> {
    use polars::prelude::{IntoLazy, SortMultipleOptions};

    // 1-minute is already the minimum granularity — just sort.
    if interval == Interval::Min1 {
        return Ok(df
            .clone()
            .lazy()
            .sort(["datetime"], SortMultipleOptions::default())
            .collect()?);
    }

    // Daily target on daily source (no datetime column) → passthrough.
    if interval == Interval::Daily && df.column("datetime").is_err() {
        return Ok(df.clone());
    }

    resample_datetime(df, interval)
}

/// Extracted OHLCV column references from a `DataFrame`.
struct OhlcvColumns<'a> {
    opens: &'a polars::prelude::Float64Chunked,
    highs: &'a polars::prelude::Float64Chunked,
    lows: &'a polars::prelude::Float64Chunked,
    closes: &'a polars::prelude::Float64Chunked,
    volumes: polars::prelude::Int64Chunked,
    has_adjclose: bool,
    adjcloses: Option<&'a polars::prelude::Float64Chunked>,
}

/// Extract OHLCV columns from a `DataFrame` for resampling aggregation.
fn extract_ohlcv_columns(df: &polars::prelude::DataFrame) -> Result<OhlcvColumns<'_>> {
    let opens = df.column("open")?.f64()?;
    let highs = df.column("high")?.f64()?;
    let lows = df.column("low")?.f64()?;
    let closes = df.column("close")?.f64()?;
    let volumes = volume_as_i64(df)?;
    let has_adjclose = df.column("adjclose").is_ok();
    let adjcloses = if has_adjclose {
        Some(df.column("adjclose")?.f64()?)
    } else {
        None
    };
    Ok(OhlcvColumns {
        opens,
        highs,
        lows,
        closes,
        volumes,
        has_adjclose,
        adjcloses,
    })
}

/// A contiguous group of rows sharing the same resampling key.
struct ResampleGroup {
    start: usize,
    end: usize, // exclusive
}

/// Resample a `DataFrame` with `"datetime"` (Datetime) column to any target interval.
///
/// Input must contain a `"datetime"` column of Datetime type. The `DataFrame` is
/// sorted by `"datetime"` internally so callers don't need to pre-sort.
#[allow(clippy::too_many_lines)]
fn resample_datetime(
    df: &polars::prelude::DataFrame,
    interval: Interval,
) -> Result<polars::prelude::DataFrame> {
    use polars::prelude::*;

    // Sort by datetime to ensure consecutive grouping is correct
    let df = df
        .clone()
        .lazy()
        .sort(["datetime"], SortMultipleOptions::default())
        .collect()?;

    let dt_col_ref = df
        .column("datetime")
        .map_err(|e| anyhow::anyhow!("Missing 'datetime' column: {e}"))?;

    let ohlcv = extract_ohlcv_columns(&df)?;

    // Extract NaiveDateTimes for grouping
    let n = df.height();
    let mut datetimes: Vec<NaiveDateTime> = Vec::with_capacity(n);
    for i in 0..n {
        datetimes.push(crate::engine::price_table::extract_datetime_from_column(
            dt_col_ref, i,
        )?);
    }

    // Build group keys by truncating to interval boundary.
    // Key is (i32, u32, u32) = enough fields for all intervals.
    // We use a generic 3-tuple to avoid an enum for each interval.
    let mut group_keys: Vec<(i32, u32, u32)> = Vec::with_capacity(n);
    for dt in &datetimes {
        let key = match interval {
            // Intraday targets: truncate time to interval boundary
            Interval::Min1 => unreachable!(), // handled by passthrough
            Interval::Min5 => {
                let trunc_min = (dt.time().minute() / 5) * 5;
                (dt.date().num_days_from_ce(), dt.time().hour(), trunc_min)
            }
            Interval::Min10 => {
                let trunc_min = (dt.time().minute() / 10) * 10;
                (dt.date().num_days_from_ce(), dt.time().hour(), trunc_min)
            }
            Interval::Min15 => {
                let trunc_min = (dt.time().minute() / 15) * 15;
                (dt.date().num_days_from_ce(), dt.time().hour(), trunc_min)
            }
            Interval::Min30 => {
                let trunc_min = (dt.time().minute() / 30) * 30;
                (dt.date().num_days_from_ce(), dt.time().hour(), trunc_min)
            }
            Interval::Hour1 => (dt.date().num_days_from_ce(), dt.time().hour(), 0),
            Interval::Hour4 => {
                let trunc_hour = (dt.time().hour() / 4) * 4;
                (dt.date().num_days_from_ce(), trunc_hour, 0)
            }
            // Daily+: group by date/week/month
            Interval::Daily => (dt.date().num_days_from_ce(), 0, 0),
            Interval::Weekly => (dt.date().iso_week().year(), dt.date().iso_week().week(), 0),
            Interval::Monthly => (dt.date().year(), dt.date().month(), 0),
        };
        group_keys.push(key);
    }

    // Group consecutive rows by key
    let mut groups: Vec<ResampleGroup> = Vec::new();
    let mut i = 0;
    while i < n {
        let key = group_keys[i];
        let start = i;
        while i < n && group_keys[i] == key {
            i += 1;
        }
        groups.push(ResampleGroup { start, end: i });
    }

    // Aggregate each group
    let mut out_datetimes: Vec<NaiveDateTime> = Vec::with_capacity(groups.len());
    let mut out_opens: Vec<f64> = Vec::with_capacity(groups.len());
    let mut out_highs: Vec<f64> = Vec::with_capacity(groups.len());
    let mut out_lows: Vec<f64> = Vec::with_capacity(groups.len());
    let mut out_closes: Vec<f64> = Vec::with_capacity(groups.len());
    let mut out_adjcloses: Vec<f64> = Vec::with_capacity(groups.len());
    let mut out_volumes: Vec<i64> = Vec::with_capacity(groups.len());

    for (gi, g) in groups.iter().enumerate() {
        let last = g.end - 1;
        // Use the first bar's timestamp as the candle open time (standard convention)
        out_datetimes.push(datetimes[g.start]);
        out_opens.push(
            ohlcv
                .opens
                .get(g.start)
                .ok_or_else(|| anyhow::anyhow!("NULL open in group {gi} at row {}", g.start))?,
        );

        let mut max_high = f64::NEG_INFINITY;
        let mut min_low = f64::INFINITY;
        let mut vol_sum: i64 = 0;
        for j in g.start..g.end {
            if let Some(h) = ohlcv.highs.get(j).filter(|v| v.is_finite()) {
                if h > max_high {
                    max_high = h;
                }
            }
            if let Some(l) = ohlcv.lows.get(j).filter(|v| v.is_finite()) {
                if l < min_low {
                    min_low = l;
                }
            }
            vol_sum += ohlcv.volumes.get(j).unwrap_or(0);
        }
        // If all highs/lows were NaN/NULL, fall back to the group's open price
        let fallback = ohlcv.opens.get(g.start).unwrap_or(0.0);
        if max_high == f64::NEG_INFINITY {
            max_high = fallback;
        }
        if min_low == f64::INFINITY {
            min_low = fallback;
        }
        out_highs.push(max_high);
        out_lows.push(min_low);
        out_closes.push(
            ohlcv
                .closes
                .get(last)
                .ok_or_else(|| anyhow::anyhow!("NULL close in group {gi} at row {last}"))?,
        );
        out_adjcloses.push(
            ohlcv
                .adjcloses
                .as_ref()
                .and_then(|ac| ac.get(last))
                .unwrap_or(ohlcv.closes.get(last).ok_or_else(|| {
                    anyhow::anyhow!("NULL close for adjclose fallback in group {gi}")
                })?),
        );
        out_volumes.push(vol_sum);
    }

    // Build output DataFrame — column type depends on target interval
    if interval.is_intraday() {
        // Output "datetime" (Datetime) column for intraday targets
        let timestamps_us: Vec<i64> = out_datetimes
            .iter()
            .map(|dt| dt.and_utc().timestamp_micros())
            .collect();
        let dt_series = Series::new("datetime".into(), &timestamps_us)
            .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
            .map_err(|e| anyhow::anyhow!("Failed to create datetime column: {e}"))?;

        let mut columns = vec![
            dt_series.into(),
            Series::new("open".into(), &out_opens).into(),
            Series::new("high".into(), &out_highs).into(),
            Series::new("low".into(), &out_lows).into(),
            Series::new("close".into(), &out_closes).into(),
        ];
        if ohlcv.has_adjclose {
            columns.push(Series::new("adjclose".into(), &out_adjcloses).into());
        }
        columns.push(Series::new("volume".into(), &out_volumes).into());

        let result =
            DataFrame::new(groups.len(), columns).map_err(|e| anyhow::anyhow!("DataFrame: {e}"))?;
        Ok(result)
    } else {
        // Output "date" (Date) column for Daily/Weekly/Monthly targets
        let dates: Vec<NaiveDate> = out_datetimes.iter().map(NaiveDateTime::date).collect();
        let date_col = DateChunked::from_naive_date(PlSmallStr::from("date"), dates).into_column();

        let mut columns = vec![
            date_col,
            Series::new("open".into(), &out_opens).into(),
            Series::new("high".into(), &out_highs).into(),
            Series::new("low".into(), &out_lows).into(),
            Series::new("close".into(), &out_closes).into(),
        ];
        if ohlcv.has_adjclose {
            columns.push(Series::new("adjclose".into(), &out_adjcloses).into());
        }
        columns.push(Series::new("volume".into(), &out_volumes).into());

        let result =
            DataFrame::new(groups.len(), columns).map_err(|e| anyhow::anyhow!("DataFrame: {e}"))?;
        Ok(result)
    }
}

/// Extract unique sorted dates from an OHLCV `DataFrame`.
pub fn extract_unique_dates(df: &polars::prelude::DataFrame) -> anyhow::Result<Vec<NaiveDate>> {
    let dt_col = detect_date_col(df);
    let series = df.column(dt_col)?;

    let epoch_offset = super::types::EPOCH_DAYS_CE_OFFSET;

    let mut dates: Vec<NaiveDate> = if series.datetime().is_ok() {
        // Datetime column: extract via the same helper used in bars_from_df
        let mut result = Vec::with_capacity(df.height());
        for i in 0..df.height() {
            if let Ok(ndt) = crate::engine::price_table::extract_datetime_from_column(series, i) {
                result.push(ndt.date());
            }
        }
        result
    } else if let Ok(ca) = series.date() {
        let mut result = Vec::with_capacity(df.height());
        for i in 0..ca.len() {
            if let Some(days) = ca.phys.get(i) {
                if let Some(date) = NaiveDate::from_num_days_from_ce_opt(days + epoch_offset) {
                    result.push(date);
                }
            }
        }
        result
    } else {
        anyhow::bail!("Column '{dt_col}' is neither Date nor Datetime");
    };

    dates.sort();
    dates.dedup();
    Ok(dates)
}

/// Derive the cache root directory from an OHLCV parquet path.
///
/// Cache paths follow the convention `{cache_root}/{category}/{SYMBOL}.parquet`, so
/// the root is two parent directories up from the file path.
pub fn ohlcv_path_to_cache_root(ohlcv_path: &str) -> Option<&std::path::Path> {
    std::path::Path::new(ohlcv_path).parent()?.parent()
}

/// Compute the effective start date for a stock data load, applying an
/// intraday lookback cap when `start_date` is `None`.
///
/// The cap is anchored to `end_date` when it is provided (common for
/// historical analyses), falling back to `Utc::now()` otherwise. This
/// guarantees that `effective_start <= end_date` regardless of the anchor.
/// When `start_date` is `Some`, it is returned unchanged.
pub fn compute_effective_start(
    interval: Interval,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
) -> Option<NaiveDate> {
    start_date.or_else(|| {
        interval.default_intraday_lookback_days().map(|days| {
            let anchor = end_date.unwrap_or_else(|| chrono::Utc::now().date_naive());
            let cap = anchor - chrono::Duration::days(days);
            tracing::info!(
                interval = %interval,
                lookback_days = days,
                anchor_date = %anchor,
                effective_start = %cap,
                "Applying default intraday lookback cap (no start_date specified)"
            );
            cap
        })
    })
}

/// Slice bars to a `[start, end)` date range (half-open interval on calendar dates).
pub fn slice_bars_by_date_range(bars: &[Bar], start: NaiveDate, end: NaiveDate) -> Vec<Bar> {
    bars.iter()
        .filter(|b| {
            let d = b.datetime.date();
            d >= start && d < end
        })
        .cloned()
        .collect()
}

/// Filter a `HashSet<NaiveDateTime>` to only datetimes within `[start, end)` calendar dates.
#[allow(clippy::implicit_hasher)]
pub fn filter_datetime_set(
    dates: &HashSet<NaiveDateTime>,
    start: NaiveDate,
    end: NaiveDate,
) -> HashSet<NaiveDateTime> {
    dates
        .iter()
        .filter(|dt| {
            let d = dt.date();
            d >= start && d < end
        })
        .copied()
        .collect()
}

/// Get the min and max calendar dates from a slice of bars.
pub fn bar_date_range(bars: &[Bar]) -> Option<(NaiveDate, NaiveDate)> {
    let min = bars.first()?.datetime.date();
    let max = bars.last()?.datetime.date();
    Some((min, max))
}
