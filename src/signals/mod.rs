//! Technical analysis signal system for filtering backtest entry and exit dates.
//!
//! Provides 40+ built-in signals across momentum, trend, volatility, overlap,
//! price, and volume categories, plus custom formula-based signals and combinators.

pub mod builders;
pub mod combinators;
pub mod custom;
pub mod custom_funcs;
pub mod helpers;
pub mod hmm_rewrite;
pub mod indicators;
pub mod momentum;
pub mod overlap;
pub mod price;
pub mod registry;
pub mod spec;
pub mod storage;
pub mod trend;
pub mod volatility;
pub mod volume;

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use chrono::{NaiveDate, NaiveDateTime};
use polars::prelude::*;

use crate::engine::hmm;
use crate::engine::price_table::{extract_date_from_column, extract_datetime_from_column};
use crate::engine::types::EPOCH_DAYS_CE_OFFSET;
use helpers::SignalFn;
use registry::{build_signal, extract_formula_cross_symbols, SignalSpec};

/// OHLCV columns to join with prefix for cross-symbol references.
const CROSS_JOIN_COLUMNS: &[&str] = &["close", "open", "high", "low", "volume", "adjclose", "iv"];

/// Pre-join cross-symbol `DataFrames` into the primary DF with prefixed column names.
///
/// For each referenced symbol, renames OHLCV columns with `SYMBOL_` prefix
/// (e.g., `close` → `VIX_close`) and left-joins on the date column.
/// If the primary DF is intraday (datetime) but the cross DF is daily (date),
/// extracts the date from the primary datetime for the join key.
#[allow(clippy::too_many_lines)]
fn pre_join_cross_dfs<S: std::hash::BuildHasher>(
    primary_df: &DataFrame,
    cross_dfs: &HashMap<String, DataFrame, S>,
    symbols: &HashSet<String>,
    date_col: &str,
) -> Result<DataFrame> {
    let mut result = primary_df.clone();

    for sym in symbols {
        let upper = sym.to_uppercase();
        let Some(cross_df) = cross_dfs.get(&upper) else {
            anyhow::bail!(
                "Formula references cross-symbol '{upper}' but no OHLCV data loaded for it. \
                 Ensure the symbol data is available in the cache."
            );
        };

        let cross_date_col = crate::engine::stock_sim::detect_date_col(cross_df);

        // Build select expressions that rename OHLCV columns with symbol prefix
        let mut select_exprs = vec![col(cross_date_col).alias("__cross_join_key")];

        for &ohlcv_col in CROSS_JOIN_COLUMNS {
            if cross_df.column(ohlcv_col).is_ok() {
                let prefixed = format!("{upper}_{ohlcv_col}");
                select_exprs.push(col(ohlcv_col).alias(&*prefixed));
            }
        }

        let cross_selected = cross_df.clone().lazy().select(select_exprs).collect()?;

        // Determine join key for primary side.
        // If primary is datetime but cross is date, extract date from primary datetime.
        let primary_is_datetime = primary_df
            .column(date_col)
            .map(|c| matches!(c.dtype(), DataType::Datetime(_, _)))
            .unwrap_or(false);
        let cross_is_date = cross_df
            .column(cross_date_col)
            .map(|c| matches!(c.dtype(), DataType::Date))
            .unwrap_or(false);

        let primary_is_date = primary_df
            .column(date_col)
            .map(|c| matches!(c.dtype(), DataType::Date))
            .unwrap_or(false);
        let cross_is_datetime = cross_df
            .column(cross_date_col)
            .map(|c| matches!(c.dtype(), DataType::Datetime(_, _)))
            .unwrap_or(false);

        if primary_is_date && cross_is_datetime {
            // Primary is daily but cross is intraday — cast cross Datetime→Date
            // to avoid one-to-many join that would multiply primary rows
            // Cast Datetime→Date and deduplicate: keep last bar per date
            let cross_col_names: Vec<_> = cross_selected
                .get_column_names()
                .iter()
                .filter(|n| n.as_str() != "__cross_join_key")
                .map(|n| col(n.as_str()).last())
                .collect();
            let cross_for_join = cross_selected
                .lazy()
                // Sort by original datetime so .last() picks the closing bar
                .sort(["__cross_join_key"], SortMultipleOptions::default())
                .with_column(
                    col("__cross_join_key")
                        .cast(DataType::Date)
                        .alias("__cross_join_key"),
                )
                .group_by_stable([col("__cross_join_key")])
                .agg(cross_col_names)
                .collect()?;

            result = result
                .lazy()
                .join(
                    cross_for_join.lazy(),
                    [col(date_col)],
                    [col("__cross_join_key")],
                    JoinArgs::new(JoinType::Left),
                )
                .collect()?;

            // Drop join key column from result
            if result.column("__cross_join_key").is_ok() {
                result = result.drop("__cross_join_key")?;
            }
        } else if primary_is_datetime && cross_is_date {
            // Add a temporary date column extracted from primary datetime for joining
            result = result
                .clone()
                .lazy()
                .with_column(
                    col(date_col)
                        .cast(DataType::Date)
                        .alias("__primary_join_date"),
                )
                .collect()?;

            // Cast cross join key to Date if it's Datetime
            let cross_key_dtype = cross_selected.column("__cross_join_key")?.dtype().clone();
            let cross_for_join = if matches!(cross_key_dtype, DataType::Datetime(_, _)) {
                cross_selected
                    .lazy()
                    .with_column(
                        col("__cross_join_key")
                            .cast(DataType::Date)
                            .alias("__cross_join_key"),
                    )
                    .collect()?
            } else {
                cross_selected
            };

            result = result
                .lazy()
                .join(
                    cross_for_join.lazy(),
                    [col("__primary_join_date")],
                    [col("__cross_join_key")],
                    JoinArgs::new(JoinType::Left),
                )
                .collect()?;

            // Drop temporary join columns
            result = result.drop("__primary_join_date")?;
            if result.column("__cross_join_key").is_ok() {
                result = result.drop("__cross_join_key")?;
            }
        } else {
            // Same granularity or both datetime — join directly
            // Cast cross join key to match primary date column type
            let primary_dtype = result.column(date_col)?.dtype().clone();
            let cross_key_dtype = cross_selected.column("__cross_join_key")?.dtype().clone();

            let cross_for_join = if primary_dtype == cross_key_dtype {
                cross_selected
            } else {
                cross_selected
                    .lazy()
                    .with_column(
                        col("__cross_join_key")
                            .cast(primary_dtype)
                            .alias("__cross_join_key"),
                    )
                    .collect()?
            };

            result = result
                .lazy()
                .join(
                    cross_for_join.lazy(),
                    [col(date_col)],
                    [col("__cross_join_key")],
                    JoinArgs::new(JoinType::Left),
                )
                .collect()?;

            // Drop join key column from result
            if result.column("__cross_join_key").is_ok() {
                result = result.drop("__cross_join_key")?;
            }
        }
    }

    Ok(result)
}

/// Evaluate a signal spec against an OHLCV `DataFrame` and return the set of dates
/// where the signal is active (true).
///
/// Used for both entry signals (dates to allow new entries) and exit signals
/// (dates to trigger early close on open positions).
pub fn active_dates(
    spec: &SignalSpec,
    ohlcv_df: &DataFrame,
    date_col: &str,
) -> Result<HashSet<NaiveDate>> {
    let signal: Box<dyn SignalFn> = build_signal(spec);
    let bools = signal.evaluate(ohlcv_df)?;
    let bool_ca = bools.bool()?;

    let col = ohlcv_df.column(date_col)?;
    let mut result = HashSet::new();

    for i in 0..ohlcv_df.height() {
        if bool_ca.get(i) == Some(true) {
            let date = extract_date_from_column(col, i)?;
            result.insert(date);
        }
    }

    Ok(result)
}

/// Evaluate a signal spec that may contain cross-symbol formula references.
///
/// `primary_df` is the main symbol's OHLCV data. `cross_dfs` maps uppercase
/// secondary symbols to their OHLCV `DataFrame`s.
///
/// For plain signals, evaluates against `primary_df`. For cross-symbol formula references,
/// evaluates the inner signal against the referenced symbol's `DataFrame`. `And`/`Or`
/// combinators recurse so that each branch can reference a different symbol.
pub fn active_dates_multi<S: std::hash::BuildHasher>(
    spec: &SignalSpec,
    primary_df: &DataFrame,
    cross_dfs: &HashMap<String, DataFrame, S>,
    date_col: &str,
) -> Result<HashSet<NaiveDate>> {
    active_dates_multi_depth(spec, primary_df, cross_dfs, date_col, 0)
}

/// Max recursion depth for `Saved` signal resolution (consistent with `build_signal_depth`).
const MAX_MULTI_DEPTH: usize = 8;

fn active_dates_multi_depth<S: std::hash::BuildHasher>(
    spec: &SignalSpec,
    primary_df: &DataFrame,
    cross_dfs: &HashMap<String, DataFrame, S>,
    date_col: &str,
    depth: usize,
) -> Result<HashSet<NaiveDate>> {
    if depth >= MAX_MULTI_DEPTH {
        anyhow::bail!(
            "Signal recursion limit ({MAX_MULTI_DEPTH}) exceeded — possible cycle in Saved signal references"
        );
    }
    match spec {
        SignalSpec::And { left, right } => {
            let left_dates =
                active_dates_multi_depth(left, primary_df, cross_dfs, date_col, depth + 1)?;
            let right_dates =
                active_dates_multi_depth(right, primary_df, cross_dfs, date_col, depth + 1)?;
            Ok(left_dates.intersection(&right_dates).copied().collect())
        }
        SignalSpec::Or { left, right } => {
            let left_dates =
                active_dates_multi_depth(left, primary_df, cross_dfs, date_col, depth + 1)?;
            let right_dates =
                active_dates_multi_depth(right, primary_df, cross_dfs, date_col, depth + 1)?;
            Ok(left_dates.union(&right_dates).copied().collect())
        }
        // Formula with potential cross-symbol references
        SignalSpec::Formula { formula } => {
            let cross_syms = extract_formula_cross_symbols(formula);
            if cross_syms.is_empty() {
                active_dates(spec, primary_df, date_col)
            } else {
                let joined = pre_join_cross_dfs(primary_df, cross_dfs, &cross_syms, date_col)?;
                active_dates(spec, &joined, date_col)
            }
        }
        // Saved: load the inner spec and recurse with incremented depth
        SignalSpec::Saved { name } => match storage::load_signal(name) {
            Ok((loaded, _, _)) => {
                active_dates_multi_depth(&loaded, primary_df, cross_dfs, date_col, depth + 1)
            }
            Err(_) => active_dates(spec, primary_df, date_col),
        },
    }
}

/// Like `active_dates` but returns `NaiveDateTime` for intraday support.
///
/// For Date columns, datetimes have midnight time component.
/// For Datetime columns, the full timestamp is preserved.
pub fn active_datetimes(
    spec: &SignalSpec,
    ohlcv_df: &DataFrame,
    date_col: &str,
) -> Result<HashSet<NaiveDateTime>> {
    let signal: Box<dyn SignalFn> = build_signal(spec);
    let bools = signal.evaluate(ohlcv_df)?;
    let bool_ca = bools.bool()?;

    let col = ohlcv_df.column(date_col)?;
    let mut result = HashSet::new();

    for i in 0..ohlcv_df.height() {
        if bool_ca.get(i) == Some(true) {
            let dt = extract_datetime_from_column(col, i)?;
            result.insert(dt);
        }
    }

    Ok(result)
}

/// Like `active_dates_multi` but returns `NaiveDateTime` for intraday support.
///
/// When combining signals via `And`/`Or`, branches may have different granularity
/// (e.g., primary is intraday but cross-symbol formula references daily data). In that case,
/// daily-only dates are "broadcast" — a daily signal active on 2024-01-02 matches all
/// intraday bars on that calendar day, so the intersection/union works correctly.
pub fn active_datetimes_multi<S: std::hash::BuildHasher>(
    spec: &SignalSpec,
    primary_df: &DataFrame,
    cross_dfs: &HashMap<String, DataFrame, S>,
    date_col: &str,
) -> Result<HashSet<NaiveDateTime>> {
    active_datetimes_multi_depth(spec, primary_df, cross_dfs, date_col, 0)
}

fn active_datetimes_multi_depth<S: std::hash::BuildHasher>(
    spec: &SignalSpec,
    primary_df: &DataFrame,
    cross_dfs: &HashMap<String, DataFrame, S>,
    date_col: &str,
    depth: usize,
) -> Result<HashSet<NaiveDateTime>> {
    if depth >= MAX_MULTI_DEPTH {
        anyhow::bail!(
            "Signal recursion limit ({MAX_MULTI_DEPTH}) exceeded — possible cycle in Saved signal references"
        );
    }
    match spec {
        SignalSpec::And { left, right } => {
            let left_dts =
                active_datetimes_multi_depth(left, primary_df, cross_dfs, date_col, depth + 1)?;
            let right_dts =
                active_datetimes_multi_depth(right, primary_df, cross_dfs, date_col, depth + 1)?;
            Ok(intersect_mixed_granularity(&left_dts, &right_dts))
        }
        SignalSpec::Or { left, right } => {
            let left_dts =
                active_datetimes_multi_depth(left, primary_df, cross_dfs, date_col, depth + 1)?;
            let right_dts =
                active_datetimes_multi_depth(right, primary_df, cross_dfs, date_col, depth + 1)?;
            Ok(union_mixed_granularity(&left_dts, &right_dts))
        }
        // Formula with potential cross-symbol references
        SignalSpec::Formula { formula } => {
            let cross_syms = extract_formula_cross_symbols(formula);
            if cross_syms.is_empty() {
                active_datetimes(spec, primary_df, date_col)
            } else {
                let joined = pre_join_cross_dfs(primary_df, cross_dfs, &cross_syms, date_col)?;
                active_datetimes(spec, &joined, date_col)
            }
        }
        // Saved: load the inner spec and recurse with incremented depth
        SignalSpec::Saved { name } => match storage::load_signal(name) {
            Ok((loaded, _, _)) => {
                active_datetimes_multi_depth(&loaded, primary_df, cross_dfs, date_col, depth + 1)
            }
            Err(_) => active_datetimes(spec, primary_df, date_col),
        },
    }
}

/// Check if all datetimes in a set have midnight time components (i.e., daily-only).
fn is_daily_only(dts: &HashSet<NaiveDateTime>) -> bool {
    !dts.is_empty()
        && dts
            .iter()
            .all(|dt| dt.time() == chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap())
}

/// Intersect two datetime sets that may have different granularity.
///
/// If one side is daily-only (all midnight timestamps) and the other has intraday
/// timestamps, the daily side is treated as "active for the whole day" — each
/// intraday timestamp is kept if its calendar date appears in the daily set.
/// If both sides have the same granularity, a normal intersection is performed.
fn intersect_mixed_granularity(
    left: &HashSet<NaiveDateTime>,
    right: &HashSet<NaiveDateTime>,
) -> HashSet<NaiveDateTime> {
    let left_daily = is_daily_only(left);
    let right_daily = is_daily_only(right);

    match (left_daily, right_daily) {
        (true, false) => {
            // Left is daily, right is intraday: keep right's timestamps whose date is in left
            let active_dates: HashSet<chrono::NaiveDate> =
                left.iter().map(NaiveDateTime::date).collect();
            right
                .iter()
                .filter(|dt| active_dates.contains(&dt.date()))
                .copied()
                .collect()
        }
        (false, true) => {
            // Right is daily, left is intraday: keep left's timestamps whose date is in right
            let active_dates: HashSet<chrono::NaiveDate> =
                right.iter().map(NaiveDateTime::date).collect();
            left.iter()
                .filter(|dt| active_dates.contains(&dt.date()))
                .copied()
                .collect()
        }
        _ => {
            // Same granularity: normal intersection
            left.intersection(right).copied().collect()
        }
    }
}

/// Union two datetime sets that may have different granularity.
///
/// If one side is daily-only, its dates are broadcast to match the intraday
/// timestamps from the other side (plus any intraday timestamps on dates not
/// covered by the daily set). The daily midnight timestamps are also included
/// so that dates with no intraday bars in the other set still appear.
fn union_mixed_granularity(
    left: &HashSet<NaiveDateTime>,
    right: &HashSet<NaiveDateTime>,
) -> HashSet<NaiveDateTime> {
    // Union is straightforward: all timestamps from both sides.
    // For mixed granularity, the daily midnight timestamps won't match any
    // intraday timestamps, but that's fine — the simulation loop checks
    // `dates.contains(&bar.datetime)`, so the intraday bar timestamps
    // from the other branch will match. The midnight entries are harmless
    // (no bar will have a midnight timestamp in intraday data).
    left.union(right).copied().collect()
}

/// Pre-process a formula string for HMM regime calls.
///
/// 1. Scans for `hmm_regime(...)` calls and rewrites the formula
/// 2. For each unique call: loads data, fits HMM, forward-filters
/// 3. Injects `__hmm_regime_*` columns into the `DataFrame`
/// 4. Returns (`rewritten_formula`, `modified_dataframe`)
///
/// If no `hmm_regime()` calls found, returns formula and `DataFrame` unchanged.
///
/// `cache_dir` is needed only when `hmm_regime` references a symbol different
/// from `primary_symbol`. Pass `None` if only the primary symbol is used.
#[allow(clippy::too_many_lines)]
pub fn preprocess_hmm_regime(
    formula: &str,
    primary_symbol: &str,
    primary_df: &DataFrame,
    cache_dir: Option<&std::path::Path>,
    date_col: &str,
    backtest_start_override: Option<chrono::NaiveDate>,
) -> Result<(String, DataFrame)> {
    let rewrite = hmm_rewrite::rewrite_formula(formula, primary_symbol)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    if rewrite.calls.is_empty() {
        return Ok((rewrite.formula, primary_df.clone()));
    }

    let mut result_df = primary_df.clone();

    // Use explicit backtest start if provided (e.g., from options data date range),
    // otherwise derive from the primary DataFrame's earliest date.
    let backtest_start = if let Some(start) = backtest_start_override {
        start
    } else {
        let first_row = primary_df
            .clone()
            .lazy()
            .sort([date_col], SortMultipleOptions::default())
            .slice(0, 1)
            .collect()?;
        if first_row.height() == 0 {
            anyhow::bail!(
                "cannot derive backtest start date: primary DataFrame for '{primary_symbol}' is empty"
            );
        }
        extract_naive_date(first_row.column(date_col)?, 0)?
    };

    for call in &rewrite.calls {
        let sym = call.symbol.as_deref().unwrap_or(primary_symbol);
        let col_name =
            hmm_rewrite::column_name(sym, call.n_regimes, call.fit_years, call.threshold);

        // Load OHLCV for the HMM symbol
        // Prefer full history from cache for HMM fitting (primary_df may be truncated
        // to the user's start_date and lack the fit-window data).
        let hmm_df = if sym.eq_ignore_ascii_case(primary_symbol) {
            match cache_dir {
                Some(dir) => load_hmm_symbol_ohlcv(dir, sym).unwrap_or_else(|e| {
                    tracing::warn!(
                        "Could not load full history for '{}' from cache ({}); \
                         falling back to primary DataFrame which may lack fit-window data",
                        sym,
                        e
                    );
                    primary_df.clone()
                }),
                None => primary_df.clone(),
            }
        } else {
            let cache = cache_dir.ok_or_else(|| {
                anyhow::anyhow!(
                    "hmm_regime references symbol '{sym}' but no cache directory available"
                )
            })?;
            load_hmm_symbol_ohlcv(cache, sym)?
        };

        // Detect date column in the HMM symbol's data
        let hmm_date_col = crate::engine::stock_sim::detect_date_col(&hmm_df);

        // Sort by date ascending so adjacent-row returns are chronologically valid
        // and the fit/apply split is determined by real temporal order, not file order.
        let hmm_df = hmm_df
            .clone()
            .lazy()
            .sort([hmm_date_col], SortMultipleOptions::default())
            .collect()?;

        // Extract dates and closes, compute returns
        let hmm_dates = hmm_df.column(hmm_date_col)?;
        let closes = hmm_df.column("close")?.f64()?;

        let mut returns = Vec::with_capacity(closes.len());
        let mut return_dates = Vec::new();
        for i in 1..closes.len() {
            if let (Some(prev), Some(curr)) = (closes.get(i - 1), closes.get(i)) {
                if prev.abs() > 1e-15 {
                    returns.push((curr - prev) / prev);
                    return_dates.push(extract_naive_date(hmm_dates, i)?);
                }
            }
        }

        // HMM operates on daily returns — deduplicate to one return per date.
        // If source data is intraday, later bars overwrite earlier ones (close-to-close).
        let mut daily_map = std::collections::BTreeMap::<chrono::NaiveDate, f64>::new();
        for (date, ret) in return_dates.iter().zip(returns.iter()) {
            daily_map.insert(*date, *ret);
        }
        let return_dates: Vec<chrono::NaiveDate> = daily_map.keys().copied().collect();
        let returns: Vec<f64> = daily_map.values().copied().collect();

        // Split into fit window and apply window
        let fit_years_days = call.fit_years as i64 * 365;
        let fit_start = backtest_start - chrono::Duration::days(fit_years_days);

        let mut fit_returns = Vec::new();
        let mut apply_returns = Vec::new();
        let mut apply_dates = Vec::new();

        for (ret, date) in returns.iter().zip(return_dates.iter()) {
            if *date < backtest_start && *date >= fit_start {
                fit_returns.push(*ret);
            } else if *date >= backtest_start {
                apply_returns.push(*ret);
                apply_dates.push(*date);
            }
        }

        if fit_returns.len() < 50 {
            anyhow::bail!(
                "hmm_regime requires at least 50 bars before backtest start date; \
                 only found {} bars for {} with fit_years={}",
                fit_returns.len(),
                sym,
                call.fit_years
            );
        }

        // Fit HMM on pre-backtest data
        let fitted = hmm::fit(&fit_returns, call.n_regimes);

        // Check for overlapping emissions
        if hmm::overlapping_emissions(&fitted) {
            tracing::warn!(
                "HMM states for {} have overlapping distributions — regime labels may be \
                 unreliable. Consider using fewer states or a longer fit window.",
                sym
            );
        }

        // Forward-filter the apply window
        let regime_labels = hmm::forward_filter(&fitted, &apply_returns, call.threshold);

        // Regime is a daily concept — all intraday bars on the same date get the same label.
        let regime_map: std::collections::HashMap<chrono::NaiveDate, usize> =
            apply_dates.into_iter().zip(regime_labels).collect();

        // Create the regime column aligned to the primary DataFrame
        let primary_dates = result_df.column(date_col)?;
        let mut regime_col = Vec::with_capacity(result_df.height());
        for i in 0..result_df.height() {
            let date = extract_naive_date(primary_dates, i)?;
            regime_col.push(regime_map.get(&date).map(|&x| x as u32));
        }

        // Inject as UInt32 column (nullable for dates outside apply window)
        let series = Series::new(col_name.into(), regime_col);
        result_df.with_column(series.into())?;
    }

    Ok((rewrite.formula, result_df))
}

/// Load OHLCV data for an HMM symbol from the cache directory.
fn load_hmm_symbol_ohlcv(cache_dir: &std::path::Path, symbol: &str) -> Result<DataFrame> {
    crate::data::cache::validate_path_segment(symbol)
        .map_err(|e| anyhow::anyhow!("invalid HMM symbol '{symbol}': {e}"))?;
    for category in &["etf", "stocks", "futures", "indices"] {
        let path = cache_dir.join(category).join(format!("{symbol}.parquet"));
        if path.exists() {
            let path_str = path.to_string_lossy();
            let args = ScanArgsParquet::default();
            return Ok(LazyFrame::scan_parquet(path_str.as_ref().into(), args)?.collect()?);
        }
    }
    anyhow::bail!(
        "no OHLCV data found for '{symbol}'; available categories: stocks, etf, indices, futures"
    )
}

/// Extract a `NaiveDate` from a date/datetime column at the given index.
fn extract_naive_date(col: &Column, idx: usize) -> Result<chrono::NaiveDate> {
    match col.dtype() {
        DataType::Date => {
            let days = col
                .date()?
                .phys
                .get(idx)
                .ok_or_else(|| anyhow::anyhow!("null date at index {idx}"))?;
            chrono::NaiveDate::from_num_days_from_ce_opt(days + EPOCH_DAYS_CE_OFFSET)
                .ok_or_else(|| anyhow::anyhow!("invalid date at index {idx}"))
        }
        DataType::Datetime(tu, _) => {
            let val = col
                .datetime()?
                .phys
                .get(idx)
                .ok_or_else(|| anyhow::anyhow!("null datetime at index {idx}"))?;
            let ndt = match tu {
                TimeUnit::Milliseconds => chrono::DateTime::from_timestamp_millis(val)
                    .ok_or_else(|| anyhow::anyhow!("invalid datetime ms at {idx}"))?
                    .naive_utc(),
                TimeUnit::Microseconds => chrono::DateTime::from_timestamp_micros(val)
                    .ok_or_else(|| anyhow::anyhow!("invalid datetime us at {idx}"))?
                    .naive_utc(),
                TimeUnit::Nanoseconds => {
                    let secs = val / 1_000_000_000;
                    let nsecs = (val % 1_000_000_000) as u32;
                    chrono::DateTime::from_timestamp(secs, nsecs)
                        .ok_or_else(|| anyhow::anyhow!("invalid datetime ns at {idx}"))?
                        .naive_utc()
                }
            };
            Ok(ndt.date())
        }
        other => Err(anyhow::anyhow!(
            "expected date/datetime column, got {other}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_dates_from_simple_df() {
        // Build a small OHLCV-like DF with a date column and price data
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(),
        ];
        let df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[100.0, 101.0, 102.0, 103.0, 104.0],
        }
        .unwrap();

        // Custom consecutive_up with count=2: true at indices 2,3,4
        let spec = SignalSpec::Formula {
            formula: "consecutive_up(close) >= 2".into(),
        };
        let result = active_dates(&spec, &df, "date").unwrap();
        assert!(result.contains(&dates[2]));
        assert!(result.contains(&dates[3]));
        assert!(result.contains(&dates[4]));
        assert!(!result.contains(&dates[0]));
        assert!(!result.contains(&dates[1]));
    }

    #[test]
    fn active_dates_consecutive_down() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(),
        ];
        let df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[104.0, 103.0, 102.0, 101.0, 100.0],
        }
        .unwrap();

        let spec = SignalSpec::Formula {
            formula: "consecutive_down(close) >= 2".into(),
        };
        let result = active_dates(&spec, &df, "date").unwrap();
        assert!(result.contains(&dates[2]));
        assert!(result.contains(&dates[3]));
        assert!(result.contains(&dates[4]));
        assert!(!result.contains(&dates[0]));
        assert!(!result.contains(&dates[1]));
    }

    #[test]
    fn active_dates_no_matches() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
        ];
        let df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[100.0, 99.0, 98.0],
        }
        .unwrap();

        // Looking for 5 consecutive ups but data trends down
        let spec = SignalSpec::Formula {
            formula: "consecutive_up(close) >= 5".into(),
        };
        let result = active_dates(&spec, &df, "date").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn active_dates_with_and_combinator() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(),
        ];
        let df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[100.0, 101.0, 102.0, 103.0, 104.0],
        }
        .unwrap();

        let spec = SignalSpec::And {
            left: Box::new(SignalSpec::Formula {
                formula: "consecutive_up(close) >= 2".into(),
            }),
            right: Box::new(SignalSpec::Formula {
                formula: "consecutive_up(close) >= 3".into(),
            }),
        };
        let result = active_dates(&spec, &df, "date").unwrap();
        // count=3 matches at index 3,4; count=2 matches at 2,3,4
        // AND: intersection is 3,4
        assert!(result.contains(&dates[3]));
        assert!(result.contains(&dates[4]));
        assert!(!result.contains(&dates[2]));
    }

    #[test]
    fn active_dates_with_or_combinator() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(),
        ];
        let df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[100.0, 101.0, 102.0, 103.0, 104.0],
        }
        .unwrap();

        let spec = SignalSpec::Or {
            left: Box::new(SignalSpec::Formula {
                formula: "consecutive_up(close) >= 4".into(),
            }),
            right: Box::new(SignalSpec::Formula {
                formula: "consecutive_up(close) >= 2".into(),
            }),
        };
        let result = active_dates(&spec, &df, "date").unwrap();
        // count=4 matches at 4; count=2 matches at 2,3,4
        // OR: union is 2,3,4
        assert!(result.contains(&dates[2]));
        assert!(result.contains(&dates[3]));
        assert!(result.contains(&dates[4]));
    }

    #[test]
    fn active_dates_multi_plain_signal_uses_primary() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
        ];
        let primary_df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[100.0, 101.0, 102.0],
        }
        .unwrap();

        let cross_dfs = HashMap::new();

        // Plain signal (no cross-symbol refs) should use primary_df
        let spec = SignalSpec::Formula {
            formula: "consecutive_up(close) >= 2".into(),
        };

        let result = active_dates_multi(&spec, &primary_df, &cross_dfs, "date").unwrap();
        assert!(result.contains(&dates[2]));
        assert!(!result.contains(&dates[0]));
    }

    #[test]
    fn active_dates_invalid_formula_errors() {
        let dates = vec![NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()];
        let df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates),
            "close" => &[100.0],
        }
        .unwrap();

        let spec = SignalSpec::Formula {
            formula: "nonexistent_column > 50".into(),
        };
        let result = active_dates(&spec, &df, "date");
        assert!(result.is_err());
    }

    #[test]
    fn active_dates_empty_dataframe() {
        let dates: Vec<NaiveDate> = vec![];
        let df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates),
            "close" => Vec::<f64>::new(),
        }
        .unwrap();

        let spec = SignalSpec::Formula {
            formula: "close > 100".into(),
        };
        let result = active_dates(&spec, &df, "date").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn active_dates_with_datetime_column() {
        use chrono::NaiveDateTime;
        let datetimes: Vec<NaiveDateTime> = vec![
            NaiveDateTime::parse_from_str("2024-01-01 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-02 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-03 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-04 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-05 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        ];
        let dt_chunked: DatetimeChunked =
            DatetimeChunked::new(PlSmallStr::from("datetime"), &datetimes);
        let df = DataFrame::new(
            5,
            vec![
                dt_chunked.into_series().into(),
                Series::new("close".into(), &[100.0, 101.0, 102.0, 103.0, 104.0]).into(),
            ],
        )
        .unwrap();

        let spec = SignalSpec::Formula {
            formula: "consecutive_up(close) >= 2".into(),
        };
        let result = active_dates(&spec, &df, "datetime").unwrap();
        assert!(result.contains(&NaiveDate::from_ymd_opt(2024, 1, 3).unwrap()));
        assert!(result.contains(&NaiveDate::from_ymd_opt(2024, 1, 4).unwrap()));
        assert!(result.contains(&NaiveDate::from_ymd_opt(2024, 1, 5).unwrap()));
    }

    // ── active_datetimes tests ──────────────────────────────────────────

    fn make_intraday_df() -> DataFrame {
        let datetimes: Vec<NaiveDateTime> = vec![
            NaiveDateTime::parse_from_str("2024-01-02 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-02 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-02 09:32:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-03 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-03 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        ];
        let dt_chunked: DatetimeChunked =
            DatetimeChunked::new(PlSmallStr::from("datetime"), &datetimes);
        DataFrame::new(
            5,
            vec![
                dt_chunked.into_series().into(),
                Series::new("close".into(), &[100.0, 101.0, 102.0, 103.0, 104.0]).into(),
            ],
        )
        .unwrap()
    }

    #[test]
    fn active_datetimes_returns_full_timestamps() {
        let df = make_intraday_df();
        let spec = SignalSpec::Formula {
            formula: "consecutive_up(close) >= 2".into(),
        };
        let result = active_datetimes(&spec, &df, "datetime").unwrap();
        // consecutive_up >= 2 fires at indices 2, 3, 4
        let dt2 =
            NaiveDateTime::parse_from_str("2024-01-02 09:32:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let dt3 =
            NaiveDateTime::parse_from_str("2024-01-03 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let dt4 =
            NaiveDateTime::parse_from_str("2024-01-03 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap();
        assert!(result.contains(&dt2));
        assert!(result.contains(&dt3));
        assert!(result.contains(&dt4));
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn active_datetimes_multi_plain_signal() {
        let df = make_intraday_df();
        let cross_dfs = HashMap::new();
        let spec = SignalSpec::Formula {
            formula: "close > 102".into(),
        };
        let result = active_datetimes_multi(&spec, &df, &cross_dfs, "datetime").unwrap();
        // close > 102 at indices 3 (103) and 4 (104)
        let dt3 =
            NaiveDateTime::parse_from_str("2024-01-03 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let dt4 =
            NaiveDateTime::parse_from_str("2024-01-03 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&dt3));
        assert!(result.contains(&dt4));
    }

    // ── Mixed granularity tests ─────────────────────────────────────────

    #[test]
    fn is_daily_only_all_midnight() {
        let dts: HashSet<NaiveDateTime> = vec![
            NaiveDate::from_ymd_opt(2024, 1, 2)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ]
        .into_iter()
        .collect();
        assert!(is_daily_only(&dts));
    }

    #[test]
    fn is_daily_only_with_intraday() {
        let dts: HashSet<NaiveDateTime> =
            vec![
                NaiveDateTime::parse_from_str("2024-01-02 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            ]
            .into_iter()
            .collect();
        assert!(!is_daily_only(&dts));
    }

    #[test]
    fn is_daily_only_empty() {
        let dts: HashSet<NaiveDateTime> = HashSet::new();
        assert!(!is_daily_only(&dts));
    }

    #[test]
    fn intersect_daily_left_intraday_right() {
        // Daily side: Jan 2 active
        let daily: HashSet<NaiveDateTime> = vec![NaiveDate::from_ymd_opt(2024, 1, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()]
        .into_iter()
        .collect();

        // Intraday side: Jan 2 09:30, Jan 2 09:31, Jan 3 09:30
        let intraday: HashSet<NaiveDateTime> = vec![
            NaiveDateTime::parse_from_str("2024-01-02 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-02 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-03 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        ]
        .into_iter()
        .collect();

        let result = intersect_mixed_granularity(&daily, &intraday);
        // Should keep Jan 2 bars only (date matches), drop Jan 3
        assert_eq!(result.len(), 2);
        assert!(result.contains(
            &NaiveDateTime::parse_from_str("2024-01-02 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap()
        ));
        assert!(result.contains(
            &NaiveDateTime::parse_from_str("2024-01-02 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap()
        ));
    }

    #[test]
    fn intersect_intraday_left_daily_right() {
        // Same as above but swapped — should produce identical result
        let daily: HashSet<NaiveDateTime> = vec![NaiveDate::from_ymd_opt(2024, 1, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()]
        .into_iter()
        .collect();

        let intraday: HashSet<NaiveDateTime> = vec![
            NaiveDateTime::parse_from_str("2024-01-02 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-02 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-03 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        ]
        .into_iter()
        .collect();

        let result = intersect_mixed_granularity(&intraday, &daily);
        assert_eq!(result.len(), 2);
        assert!(result.contains(
            &NaiveDateTime::parse_from_str("2024-01-02 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap()
        ));
        assert!(result.contains(
            &NaiveDateTime::parse_from_str("2024-01-02 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap()
        ));
    }

    #[test]
    fn intersect_same_granularity_intraday() {
        let left: HashSet<NaiveDateTime> = vec![
            NaiveDateTime::parse_from_str("2024-01-02 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-02 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        ]
        .into_iter()
        .collect();

        let right: HashSet<NaiveDateTime> = vec![
            NaiveDateTime::parse_from_str("2024-01-02 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            NaiveDateTime::parse_from_str("2024-01-02 09:32:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        ]
        .into_iter()
        .collect();

        let result = intersect_mixed_granularity(&left, &right);
        assert_eq!(result.len(), 1);
        assert!(result.contains(
            &NaiveDateTime::parse_from_str("2024-01-02 09:31:00", "%Y-%m-%d %H:%M:%S").unwrap()
        ));
    }

    #[test]
    fn intersect_no_overlapping_dates() {
        let daily: HashSet<NaiveDateTime> = vec![NaiveDate::from_ymd_opt(2024, 1, 5)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()]
        .into_iter()
        .collect();

        let intraday: HashSet<NaiveDateTime> =
            vec![
                NaiveDateTime::parse_from_str("2024-01-02 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            ]
            .into_iter()
            .collect();

        let result = intersect_mixed_granularity(&daily, &intraday);
        assert!(result.is_empty());
    }

    #[test]
    fn union_mixed_includes_all() {
        let daily: HashSet<NaiveDateTime> = vec![NaiveDate::from_ymd_opt(2024, 1, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()]
        .into_iter()
        .collect();

        let intraday: HashSet<NaiveDateTime> =
            vec![
                NaiveDateTime::parse_from_str("2024-01-03 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            ]
            .into_iter()
            .collect();

        let result = union_mixed_granularity(&daily, &intraday);
        assert_eq!(result.len(), 2);
    }

    // ── Cross-symbol formula integration tests ──────────────────────────

    #[test]
    fn active_dates_multi_formula_cross_symbol() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(),
        ];

        // Primary symbol
        let primary_df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[100.0, 101.0, 102.0, 103.0, 104.0],
        }
        .unwrap();

        // VIX data
        let vix_df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[25.0, 22.0, 18.0, 15.0, 12.0],
        }
        .unwrap();

        // VIX3M data
        let vix3m_df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[20.0, 20.0, 20.0, 20.0, 20.0],
        }
        .unwrap();

        let mut cross_dfs = HashMap::new();
        cross_dfs.insert("VIX".to_string(), vix_df);
        cross_dfs.insert("VIX3M".to_string(), vix3m_df);

        // Formula: VIX / VIX3M < 0.9
        // VIX/VIX3M: 1.25, 1.1, 0.9, 0.75, 0.6
        // < 0.9: indices 3 (0.75), 4 (0.6)
        let spec = SignalSpec::Formula {
            formula: "VIX / VIX3M < 0.9".into(),
        };

        let result = active_dates_multi(&spec, &primary_df, &cross_dfs, "date").unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&dates[3]));
        assert!(result.contains(&dates[4]));
    }

    #[test]
    fn active_dates_multi_formula_cross_symbol_dot_syntax() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
        ];

        let primary_df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[100.0, 101.0, 102.0],
        }
        .unwrap();

        let vix_df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[25.0, 35.0, 15.0],
            "high" => &[28.0, 38.0, 18.0],
        }
        .unwrap();

        let mut cross_dfs = HashMap::new();
        cross_dfs.insert("VIX".to_string(), vix_df);

        // VIX.high > 30 → indices 0 (28 no), 1 (38 yes), 2 (18 no)
        let spec = SignalSpec::Formula {
            formula: "VIX.high > 30".into(),
        };

        let result = active_dates_multi(&spec, &primary_df, &cross_dfs, "date").unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains(&dates[1]));
    }

    #[test]
    fn active_dates_multi_formula_cross_with_primary() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
        ];

        let primary_df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[100.0, 101.0, 102.0],
        }
        .unwrap();

        let vix_df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()),
            "close" => &[35.0, 25.0, 15.0],
        }
        .unwrap();

        let mut cross_dfs = HashMap::new();
        cross_dfs.insert("VIX".to_string(), vix_df);

        // VIX > 30 and close > 99 → VIX>30 at idx 0 (35), close>99 at all → idx 0
        let spec = SignalSpec::Formula {
            formula: "VIX > 30 and close > 99".into(),
        };

        let result = active_dates_multi(&spec, &primary_df, &cross_dfs, "date").unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains(&dates[0]));
    }

    #[test]
    fn active_dates_multi_formula_missing_dates_produce_null() {
        // Cross DF missing some dates → left join fills with null → signal excludes those
        let primary_dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
        ];

        let primary_df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), primary_dates.clone()),
            "close" => &[100.0, 101.0, 102.0],
        }
        .unwrap();

        // VIX only has data for Jan 2
        let vix_dates = vec![NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()];
        let vix_df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), vix_dates),
            "close" => &[35.0],
        }
        .unwrap();

        let mut cross_dfs = HashMap::new();
        cross_dfs.insert("VIX".to_string(), vix_df);

        // VIX > 30 → only Jan 2 has VIX data (35 > 30 = true), Jan 1 and Jan 3 are null
        let spec = SignalSpec::Formula {
            formula: "VIX > 30".into(),
        };

        let result = active_dates_multi(&spec, &primary_df, &cross_dfs, "date").unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains(&primary_dates[1]));
    }
}
