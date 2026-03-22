//! Handler body for the `run_wheel_backtest` tool.

use std::collections::{BTreeMap, HashSet};

use chrono::NaiveDate;
use polars::prelude::*;

use crate::data::cache::validate_path_segment;
use crate::data::parquet::DATETIME_COL;
use crate::engine::price_table::extract_date_from_column;
use crate::engine::types::EPOCH_DAYS_CE_OFFSET;
use crate::engine::wheel_sim::WheelParams;
use crate::signals;
use crate::signals::registry::{collect_cross_symbols, SignalSpec};
use crate::tools;
use crate::tools::response_types::{UnderlyingPrice, WheelBacktestResponse};

use super::super::params::{tool_err, RunWheelBacktestParams};
use super::super::{load_underlying_prices, OptopsyServer};

/// Execute the `run_wheel_backtest` tool logic.
///
/// Loads options + OHLCV data, evaluates entry signals, builds the wheel
/// engine params, and dispatches to the wheel executor.
#[allow(clippy::too_many_lines)]
pub async fn execute(
    server: &OptopsyServer,
    params: RunWheelBacktestParams,
) -> Result<WheelBacktestResponse, String> {
    let symbol = params.symbol.to_uppercase();
    validate_path_segment(&symbol).map_err(|e| format!("Invalid symbol: {e}"))?;

    tracing::info!(
        symbol = %symbol,
        put_delta_target = params.put_delta.target,
        call_delta_target = params.call_delta.target,
        capital = params.capital,
        "Wheel backtest request received"
    );

    // 1. Load options data
    let (_sym, options_df) = server.ensure_data_loaded(Some(&symbol)).await?;

    // 2. Load OHLCV data
    let ohlcv_path = server.ensure_ohlcv(&symbol)?;

    // 3. Build OHLCV close price map (BTreeMap<NaiveDate, f64>)
    let ohlcv_path_clone = ohlcv_path.clone();
    let ohlcv_closes: BTreeMap<NaiveDate, f64> =
        tokio::task::spawn_blocking(move || build_ohlcv_closes(&ohlcv_path_clone))
            .await
            .map_err(|e| format!("OHLCV load task panicked: {e}"))?
            .map_err(|e| format!("Failed to build OHLCV close map: {e}"))?;

    if ohlcv_closes.is_empty() {
        return Err(format!(
            "No OHLCV close prices found for {symbol}. Ensure price data exists in cache."
        ));
    }

    // 4. Extract unique trading days from options data
    let trading_days = extract_trading_days(&options_df)
        .map_err(|e| format!("Failed to extract trading days: {e}"))?;

    // 5. Resolve cross-symbol OHLCV paths for entry signal
    let cross_ohlcv_paths = if let Some(sig) = params.entry_signal.as_ref() {
        let cross_syms = collect_cross_symbols(sig);
        let mut paths = std::collections::HashMap::new();
        for sym in cross_syms {
            validate_path_segment(&sym)
                .map_err(|e| format!("Invalid cross-symbol \"{sym}\": {e}"))?;
            let path = server.ensure_ohlcv(&sym)?;
            paths.insert(sym, path);
        }
        paths
    } else {
        std::collections::HashMap::new()
    };

    // 6. Build entry_dates from signal (if provided)
    let entry_signal = params.entry_signal.clone();
    let ohlcv_path_for_signal = ohlcv_path.clone();
    let options_df_clone = options_df.clone();
    let entry_dates: Option<HashSet<NaiveDate>> = if let Some(ref spec) = entry_signal {
        let spec_clone = spec.clone();
        let cross_paths = cross_ohlcv_paths.clone();
        Some(
            tokio::task::spawn_blocking(move || {
                build_entry_dates(
                    &spec_clone,
                    &ohlcv_path_for_signal,
                    &cross_paths,
                    &options_df_clone,
                )
            })
            .await
            .map_err(|e| format!("Signal evaluation panicked: {e}"))?
            .map_err(|e| format!("Signal evaluation failed: {e}"))?,
        )
    } else {
        None
    };

    // 7. Parse start/end dates
    let start_date = params
        .start_date
        .as_deref()
        .map(|s| {
            NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|_| format!("Invalid start_date \"{s}\": expected YYYY-MM-DD"))
        })
        .transpose()?;
    let end_date = params
        .end_date
        .as_deref()
        .map(|s| {
            NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|_| format!("Invalid end_date \"{s}\": expected YYYY-MM-DD"))
        })
        .transpose()?;

    // 8. Filter trading days by date range
    let trading_days: Vec<NaiveDate> = trading_days
        .into_iter()
        .filter(|d| start_date.map_or(true, |s| *d >= s))
        .filter(|d| end_date.map_or(true, |e| *d <= e))
        .collect();

    // 9. Convert RunWheelBacktestParams → WheelParams
    let wheel_params = WheelParams {
        put_delta: params.put_delta,
        put_dte: params.put_dte,
        call_delta: params.call_delta,
        call_dte: params.call_dte,
        min_call_strike_at_cost: params.min_call_strike_at_cost,
        capital: params.capital,
        quantity: params.quantity,
        multiplier: params.multiplier,
        slippage: params.slippage,
        commission: params.commission,
        stop_loss: params.stop_loss,
        min_bid_ask: params.min_bid_ask,
    };

    // 10. Build underlying prices for chart overlay
    let ohlcv_path_buf = std::path::PathBuf::from(&ohlcv_path);
    let dt_filter = options_df.column(DATETIME_COL).ok().cloned();
    let underlying_prices = tokio::task::spawn_blocking(move || -> Vec<UnderlyingPrice> {
        load_underlying_prices(&ohlcv_path_buf, dt_filter.as_ref(), None, None)
    })
    .await
    .unwrap_or_default();

    // 11. Run the wheel backtest
    let options_df_run = options_df.clone();
    tokio::task::spawn_blocking(move || {
        tools::wheel_backtest::execute(
            &options_df_run,
            &ohlcv_closes,
            &wheel_params,
            entry_dates.as_ref(),
            &trading_days,
            underlying_prices,
        )
    })
    .await
    .map_err(|e| format!("Wheel backtest task panicked: {e}"))?
    .map_err(tool_err)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a `BTreeMap<NaiveDate, f64>` of close prices from an OHLCV parquet file.
fn build_ohlcv_closes(ohlcv_path: &str) -> anyhow::Result<BTreeMap<NaiveDate, f64>> {
    let args = ScanArgsParquet::default();
    let df = LazyFrame::scan_parquet(ohlcv_path.into(), args)?.collect()?;

    let mut closes_map = BTreeMap::new();

    // Intraday path: "datetime" Datetime column → extract date portion.
    let has_datetime = df
        .column("datetime")
        .ok()
        .is_some_and(|c| matches!(c.dtype(), DataType::Datetime(_, _)));

    if has_datetime {
        let sorted = df
            .lazy()
            .sort(["datetime"], SortMultipleOptions::default())
            .collect()?;
        let closes = sorted
            .column("close")
            .map_err(|e| anyhow::anyhow!("OHLCV missing 'close': {e}"))?
            .f64()
            .map_err(|e| anyhow::anyhow!("'close' not f64: {e}"))?;
        let dt_col_ref = sorted
            .column("datetime")
            .map_err(|e| anyhow::anyhow!("OHLCV missing 'datetime': {e}"))?;
        for i in 0..sorted.height() {
            let Ok(ndt) = crate::engine::price_table::extract_datetime_from_column(dt_col_ref, i)
            else {
                continue;
            };
            let date = ndt.date();
            let Some(close) = closes.get(i) else {
                continue;
            };
            if close > 0.0 {
                closes_map.insert(date, close);
            }
        }
    } else {
        // Daily path: "date" Date column
        let closes = df
            .column("close")
            .map_err(|e| anyhow::anyhow!("OHLCV missing 'close': {e}"))?
            .f64()
            .map_err(|e| anyhow::anyhow!("'close' not f64: {e}"))?;
        let dates = df
            .column("date")
            .map_err(|e| anyhow::anyhow!("OHLCV missing 'date': {e}"))?
            .date()
            .map_err(|e| anyhow::anyhow!("'date' not Date: {e}"))?;
        for i in 0..df.height() {
            let Some(days) = dates.phys.get(i) else {
                continue;
            };
            let Some(date) = NaiveDate::from_num_days_from_ce_opt(days + EPOCH_DAYS_CE_OFFSET)
            else {
                continue;
            };
            let Some(close) = closes.get(i) else {
                continue;
            };
            if close > 0.0 {
                closes_map.insert(date, close);
            }
        }
    }

    Ok(closes_map)
}

/// Extract unique sorted dates from the options DataFrame's datetime column.
fn extract_trading_days(options_df: &DataFrame) -> anyhow::Result<Vec<NaiveDate>> {
    let dt_col = options_df
        .column(DATETIME_COL)
        .map_err(|e| anyhow::anyhow!("Options data missing '{DATETIME_COL}': {e}"))?;

    let mut dates: HashSet<NaiveDate> = HashSet::new();
    for i in 0..options_df.height() {
        if let Ok(date) = extract_date_from_column(dt_col, i) {
            dates.insert(date);
        }
    }

    let mut sorted: Vec<NaiveDate> = dates.into_iter().collect();
    sorted.sort();
    Ok(sorted)
}

/// Evaluate an entry signal against OHLCV data and return the set of active dates.
///
/// Handles cross-symbol references and HMM regime preprocessing.
fn build_entry_dates(
    spec: &SignalSpec,
    ohlcv_path: &str,
    cross_ohlcv_paths: &std::collections::HashMap<String, String>,
    options_df: &DataFrame,
) -> anyhow::Result<HashSet<NaiveDate>> {
    use crate::engine::stock_sim::load_ohlcv_df;

    let mut ohlcv_df = load_ohlcv_df(ohlcv_path, None, None)?;

    // Detect date column
    let schema = ohlcv_df.schema();
    let date_col = if schema
        .get("datetime")
        .is_some_and(|dt| matches!(dt, DataType::Datetime(_, _)))
    {
        "datetime"
    } else {
        "date"
    };

    // HMM preprocessing
    let mut resolved_spec = spec.clone();
    if let SignalSpec::Formula { formula } = spec {
        if formula.contains("hmm_regime(") {
            let primary_symbol = std::path::Path::new(ohlcv_path)
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("UNKNOWN")
                .to_uppercase();

            // Options start date for HMM fitting window
            let options_start = options_df.column("datetime").ok().and_then(|c| {
                c.datetime()
                    .ok()
                    .and_then(|dt| dt.as_datetime_iter().flatten().next())
                    .map(|ndt| ndt.date())
            });

            let cache_dir = std::path::Path::new(ohlcv_path)
                .parent()
                .and_then(|p| p.parent());

            let (rewritten, updated_df) = signals::preprocess_hmm_regime(
                formula,
                &primary_symbol,
                &ohlcv_df,
                cache_dir,
                date_col,
                options_start,
            )?;
            resolved_spec = SignalSpec::Formula { formula: rewritten };
            ohlcv_df = updated_df;
        }
    }

    // Check for cross-symbol references
    let has_cross = !collect_cross_symbols(&resolved_spec).is_empty();

    if has_cross {
        let mut cross_dfs: std::collections::HashMap<String, DataFrame> =
            std::collections::HashMap::new();
        for (sym, path) in cross_ohlcv_paths {
            cross_dfs.insert(sym.to_uppercase(), load_ohlcv_df(path, None, None)?);
        }
        signals::active_dates_multi(&resolved_spec, &ohlcv_df, &cross_dfs, date_col)
    } else {
        signals::active_dates(&resolved_spec, &ohlcv_df, date_col)
    }
}
