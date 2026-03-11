//! Build the `PriceTable` hash map from a raw options `DataFrame` for O(1)
//! quote lookups during event-driven simulation.

use std::collections::HashMap;

use anyhow::{bail, Result};
use chrono::NaiveDate;
use ordered_float::OrderedFloat;
use polars::prelude::*;

#[allow(clippy::wildcard_imports)]
use super::types::*;
use crate::data::parquet::QUOTE_DATETIME_COL;

/// Build a price lookup table from the raw options `DataFrame`.
/// Returns the table and a sorted list of unique trading dates.
pub fn build_price_table(df: &DataFrame) -> Result<(PriceTable, Vec<NaiveDate>, DateIndex)> {
    let quote_col = df.column(QUOTE_DATETIME_COL)?;
    let exp_col = df.column("expiration")?;

    // Try fast path: downcast typed columns once, iterate via phys iterators
    if let (Ok(quote_ca), Ok(exp_ca)) = (quote_col.datetime(), exp_col.date()) {
        return build_price_table_fast(df, quote_ca, exp_ca);
    }

    // Fallback: per-row extract_date_from_column for non-normalized data
    build_price_table_slow(df, quote_col, exp_col)
}

/// Build a `DateIndex` from a completed `PriceTable`.
pub(crate) fn build_date_index(table: &PriceTable) -> DateIndex {
    let mut index: DateIndex = HashMap::new();
    for key in table.keys() {
        index.entry(key.0).or_default().push(*key);
    }
    index
}

/// Convert a raw epoch-offset day count (with a `719_163` CE-epoch bias) to a `NaiveDate`.
///
/// `raw` is the integer value already biased by `719_163` (i.e., `raw_stored + 719_163`).
/// Returns an error if the value overflows `i32` or does not map to a valid calendar date.
#[inline]
fn days_to_naive_date(raw: i64, label: &str, row: usize) -> Result<NaiveDate> {
    let days = i32::try_from(raw)
        .map_err(|_| anyhow::anyhow!("{label} value {raw} overflows i32 (row {row})"))?;
    NaiveDate::from_num_days_from_ce_opt(days)
        .ok_or_else(|| anyhow::anyhow!("Invalid {label} value (row {row})"))
}

/// Fast path: columns already typed as Datetime/Date after `ParquetStore` normalization.
/// Uses `cont_slice()` for zero-copy raw value access when possible.
fn build_price_table_fast(
    df: &DataFrame,
    quote_ca: &DatetimeChunked,
    exp_ca: &DateChunked,
) -> Result<(PriceTable, Vec<NaiveDate>, DateIndex)> {
    let strikes = df.column("strike")?.f64()?;
    let option_types = df.column("option_type")?.str()?;
    let bids = df.column("bid")?.f64()?;
    let asks = df.column("ask")?.f64()?;
    let deltas = df.column("delta")?.f64()?;

    let micros_per_day: i64 = match quote_ca.time_unit() {
        TimeUnit::Microseconds => 86_400_000_000,
        TimeUnit::Milliseconds => 86_400_000,
        TimeUnit::Nanoseconds => 86_400_000_000_000,
    };

    let n = df.height();
    let mut table = PriceTable::with_capacity_and_hasher(n, rustc_hash::FxBuildHasher);
    let mut dates_vec: Vec<NaiveDate> = Vec::with_capacity(n);

    // Try contiguous slice access (zero-copy, no Option wrapping, no chunk navigation).
    // This works when columns have no nulls and are stored in a single chunk.
    if let (Ok(q_vals), Ok(e_vals), Ok(s_vals), Ok(b_vals), Ok(a_vals), Ok(d_vals)) = (
        quote_ca.phys.cont_slice(),
        exp_ca.phys.cont_slice(),
        strikes.cont_slice(),
        bids.cont_slice(),
        asks.cont_slice(),
        deltas.cont_slice(),
    ) {
        for i in 0..n {
            let ot = match option_types.get(i) {
                Some("call") => OptionType::Call,
                Some("put") => OptionType::Put,
                _ => continue,
            };
            let quote_days = q_vals[i].div_euclid(micros_per_day) + i64::from(EPOCH_DAYS_CE_OFFSET);
            let quote_date = days_to_naive_date(quote_days, "quote_datetime", i)?;
            let exp_days = i64::from(e_vals[i]) + i64::from(EPOCH_DAYS_CE_OFFSET);
            let exp_date = days_to_naive_date(exp_days, "expiration", i)?;

            table.insert(
                (quote_date, exp_date, OrderedFloat(s_vals[i]), ot),
                QuoteSnapshot {
                    bid: b_vals[i],
                    ask: a_vals[i],
                    delta: d_vals[i],
                },
            );
            dates_vec.push(quote_date);
        }
    } else {
        // Chunked iterator fallback (handles nulls / multi-chunk arrays)
        for (row_idx, (((((quote_raw, exp_raw), strike), opt_str), bid), (ask, delta))) in quote_ca
            .phys
            .iter()
            .zip(exp_ca.phys.iter())
            .zip(strikes.iter())
            .zip(option_types.iter())
            .zip(bids.iter())
            .zip(asks.iter().zip(deltas.iter()))
            .enumerate()
        {
            let (
                Some(qv),
                Some(ev),
                Some(strike_val),
                Some(ot_str),
                Some(bid_val),
                Some(ask_val),
                Some(delta_val),
            ) = (quote_raw, exp_raw, strike, opt_str, bid, ask, delta)
            else {
                continue;
            };
            let opt_type = match ot_str {
                "call" => OptionType::Call,
                "put" => OptionType::Put,
                _ => continue,
            };
            let qv_days = qv.div_euclid(micros_per_day) + i64::from(EPOCH_DAYS_CE_OFFSET);
            let quote_date = days_to_naive_date(qv_days, "quote_datetime", row_idx)?;
            let ev_days = i64::from(ev) + i64::from(EPOCH_DAYS_CE_OFFSET);
            let exp_date = days_to_naive_date(ev_days, "expiration", row_idx)?;

            table.insert(
                (quote_date, exp_date, OrderedFloat(strike_val), opt_type),
                QuoteSnapshot {
                    bid: bid_val,
                    ask: ask_val,
                    delta: delta_val,
                },
            );
            dates_vec.push(quote_date);
        }
    }

    dates_vec.sort_unstable();
    dates_vec.dedup();
    let date_index = build_date_index(&table);
    Ok((table, dates_vec, date_index))
}

/// Slow fallback: per-row type dispatch via `extract_date_from_column`.
fn build_price_table_slow(
    df: &DataFrame,
    quote_col: &Column,
    exp_col: &Column,
) -> Result<(PriceTable, Vec<NaiveDate>, DateIndex)> {
    let strikes = df.column("strike")?.f64()?;
    let option_types = df.column("option_type")?.str()?;
    let bids = df.column("bid")?.f64()?;
    let asks = df.column("ask")?.f64()?;
    let deltas = df.column("delta")?.f64()?;

    let mut table = PriceTable::with_capacity_and_hasher(df.height(), rustc_hash::FxBuildHasher);
    let mut dates_set = std::collections::BTreeSet::new();

    for i in 0..df.height() {
        let quote_date = extract_date_from_column(quote_col, i)?;
        let exp_date = extract_date_from_column(exp_col, i)?;
        let Some(strike) = strikes.get(i) else {
            continue;
        };
        let Some(opt_type_str) = option_types.get(i) else {
            continue;
        };
        let opt_type = match opt_type_str {
            "call" => OptionType::Call,
            "put" => OptionType::Put,
            _ => continue,
        };
        let Some(bid) = bids.get(i) else { continue };
        let Some(ask) = asks.get(i) else { continue };
        let Some(delta) = deltas.get(i) else { continue };

        let key = (quote_date, exp_date, OrderedFloat(strike), opt_type);
        table.insert(key, QuoteSnapshot { bid, ask, delta });
        dates_set.insert(quote_date);
    }

    let trading_days: Vec<NaiveDate> = dates_set.into_iter().collect();
    let date_index = build_date_index(&table);
    Ok((table, trading_days, date_index))
}

/// Extract a `NaiveDate` from a column value at a given index.
/// Handles Date, Datetime, and String column types.
pub(crate) fn extract_date_from_column(col: &Column, idx: usize) -> Result<NaiveDate> {
    match col.dtype() {
        DataType::Date => {
            let days = col.date()?.phys.get(idx);
            match days {
                Some(d) => {
                    let date = chrono::NaiveDate::from_num_days_from_ce_opt(
                        d + EPOCH_DAYS_CE_OFFSET, // epoch offset: days from CE to 1970-01-01
                    )
                    .ok_or_else(|| anyhow::anyhow!("Invalid date at index {idx}"))?;
                    Ok(date)
                }
                None => bail!("Null date at index {idx}"),
            }
        }
        DataType::Datetime(tu, _) => {
            let val = col.datetime()?.phys.get(idx);
            match val {
                Some(v) => {
                    let ndt = match tu {
                        TimeUnit::Milliseconds => {
                            chrono::DateTime::from_timestamp_millis(v).map(|dt| dt.naive_utc())
                        }
                        TimeUnit::Microseconds => {
                            chrono::DateTime::from_timestamp_micros(v).map(|dt| dt.naive_utc())
                        }
                        TimeUnit::Nanoseconds => {
                            let secs = v.div_euclid(1_000_000_000);
                            let nsecs = v.rem_euclid(1_000_000_000) as u32;
                            chrono::DateTime::from_timestamp(secs, nsecs).map(|dt| dt.naive_utc())
                        }
                    };
                    match ndt {
                        Some(dt) => Ok(dt.date()),
                        None => bail!("Invalid datetime value at index {idx}"),
                    }
                }
                None => bail!("Null datetime at index {idx}"),
            }
        }
        DataType::String => {
            let str_val = col.str()?.get(idx);
            match str_val {
                Some(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .or_else(|_| {
                        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                            .map(|dt| dt.date())
                    })
                    .map_err(|e| anyhow::anyhow!("Cannot parse date '{s}': {e}")),
                None => bail!("Null string date at index {idx}"),
            }
        }
        other => bail!("Unsupported column type for date extraction: {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::parquet::QUOTE_DATETIME_COL;

    /// Helper: build a synthetic daily options `DataFrame` for testing `build_price_table`.
    pub(crate) fn make_daily_df() -> DataFrame {
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 22).unwrap();
        let d3 = NaiveDate::from_ymd_opt(2024, 1, 29).unwrap();
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();

        let quote_dates = vec![
            d1.and_hms_opt(0, 0, 0).unwrap(),
            d2.and_hms_opt(0, 0, 0).unwrap(),
            d3.and_hms_opt(0, 0, 0).unwrap(),
        ];
        let expirations = [exp, exp, exp];

        let mut df = df! {
            QUOTE_DATETIME_COL => &quote_dates,
            "option_type" => &["call", "call", "call"],
            "strike" => &[100.0f64, 100.0, 100.0],
            "bid" => &[5.0f64, 3.0, 2.0],
            "ask" => &[5.50f64, 3.50, 2.50],
            "delta" => &[0.50f64, 0.35, 0.25],
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations).into_column(),
        )
        .unwrap();
        df
    }

    #[test]
    fn build_price_table_basic() {
        let df = make_daily_df();
        let (table, days, _) = build_price_table(&df).unwrap();
        assert!(!table.is_empty());
        assert!(!days.is_empty());
        // Verify a specific key lookup
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let key = (d1, exp, OrderedFloat(100.0), OptionType::Call);
        assert!(table.contains_key(&key));
        let snap = table.get(&key).unwrap();
        assert!((snap.bid - 5.0).abs() < 1e-10);
    }
}
