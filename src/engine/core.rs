//! Top-level orchestration for backtests and strategy comparisons.
//!
//! Resolves strategy definitions, builds signal filters, and dispatches to
//! either the vectorized or event-driven simulation path.

use std::collections::{HashMap, HashSet};

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use polars::prelude::*;

use super::event_sim;
use super::metrics;
#[allow(clippy::wildcard_imports)]
use super::types::*;
use super::vectorized_sim;
use crate::signals;
use crate::signals::registry::{collect_cross_symbols, SignalSpec};
use crate::strategies;

type DateFilter = Option<HashSet<NaiveDate>>;

/// Load OHLCV close prices into a `BTreeMap<NaiveDate, f64>` for sizing or stock-leg strategies.
///
/// Loads when the backtest has dynamic sizing configured or when the strategy has a stock leg.
/// Returns `Ok(None)` if neither condition is met (sizing not configured and no stock leg).
/// Returns an error if OHLCV data is required but cannot be loaded or parsed.
fn load_ohlcv_closes(
    params: &BacktestParams,
    strategy_def: &StrategyDef,
) -> Result<Option<std::collections::BTreeMap<NaiveDate, f64>>> {
    if params.sizing.is_none() && !strategy_def.has_stock_leg {
        return Ok(None);
    }

    let Some(ohlcv_path) = params.ohlcv_path.as_ref() else {
        return Ok(None);
    };
    let df = load_ohlcv(ohlcv_path).context("failed to load OHLCV parquet for close prices")?;

    let mut closes_map = std::collections::BTreeMap::new();

    // Intraday path: "datetime" Datetime column → extract date portion.
    // Sort by datetime so later entries overwrite earlier ones, giving last-close-per-day.
    // Only take this branch when the column is actually a Datetime dtype.
    let has_datetime = df
        .column("datetime")
        .ok()
        .is_some_and(|c| matches!(c.dtype(), polars::prelude::DataType::Datetime(_, _)));
    if has_datetime {
        // Sort by datetime. `lazy()` takes ownership of `df`, which is fine
        // because we don't use the original DataFrame after this point.
        // `load_ohlcv` already returns data roughly ordered by datetime when
        // a date filter was applied, so this sort is typically a near no-op.
        let sorted = df
            .lazy()
            .sort(
                ["datetime"],
                polars::prelude::SortMultipleOptions::default(),
            )
            .collect()
            .context("failed to sort OHLCV data by datetime")?;
        let closes = sorted
            .column("close")
            .context("OHLCV data missing 'close' column")?
            .f64()
            .context("'close' column is not f64 dtype")?;
        let dt_col_ref = sorted
            .column("datetime")
            .context("OHLCV data missing 'datetime' column")?;
        for i in 0..sorted.height() {
            let Ok(ndt) = super::price_table::extract_datetime_from_column(dt_col_ref, i) else {
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
            .context("OHLCV data missing 'close' column")?
            .f64()
            .context("'close' column is not f64 dtype")?;
        let dates = df
            .column("date")
            .context("OHLCV data missing 'date' column")?
            .date()
            .context("'date' column is not Date dtype")?;
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

    if closes_map.is_empty() {
        Ok(None)
    } else {
        Ok(Some(closes_map))
    }
}

/// Load OHLCV parquet into a `DataFrame`.
fn load_ohlcv(ohlcv_path: &str) -> Result<DataFrame> {
    let args = ScanArgsParquet::default();
    Ok(LazyFrame::scan_parquet(ohlcv_path.into(), args)?.collect()?)
}

/// Maximum recursion depth when resolving nested/saved signal specs.
const MAX_SIGNAL_DEPTH: usize = 8;

/// Walk a `SignalSpec` tree (including nested And/Or and Saved refs) and return `true`
/// if any leaf formula satisfies `predicate`. Resolves `Saved` specs best-effort via
/// disk load, with depth guard.
fn traverse_signal_spec(
    spec: &signals::registry::SignalSpec,
    predicate: &dyn Fn(&str) -> bool,
) -> bool {
    fn inner(
        spec: &signals::registry::SignalSpec,
        pred: &dyn Fn(&str) -> bool,
        depth: usize,
    ) -> bool {
        use signals::registry::SignalSpec;
        if depth > MAX_SIGNAL_DEPTH {
            return false;
        }
        match spec {
            SignalSpec::Formula { formula } => pred(formula),
            SignalSpec::And { left, right } | SignalSpec::Or { left, right } => {
                inner(left, pred, depth + 1) || inner(right, pred, depth + 1)
            }
            SignalSpec::Saved { name } => signals::storage::load_signal(name)
                .map(|(s, _)| inner(&s, pred, depth + 1))
                .unwrap_or(false),
        }
    }
    inner(spec, predicate, 0)
}

/// Check whether a `SignalSpec` (including nested And/Or) contains any IV-based signal.
fn contains_iv_signal(spec: &signals::registry::SignalSpec) -> bool {
    traverse_signal_spec(spec, &formula_references_iv)
}

/// Check whether a `SignalSpec` tree contains any non-IV leaf signal.
fn contains_non_iv_signal(spec: &signals::registry::SignalSpec) -> bool {
    traverse_signal_spec(spec, &|f| !formula_references_iv(f))
}

/// Check if a formula string references the `iv` column.
///
/// Matches `iv` as a standalone identifier (not as part of longer words like `pivot`).
/// Looks for `iv` preceded by a non-alphanumeric char (or start of string) and followed
/// by a non-alphanumeric char (or end of string).
fn formula_references_iv(formula: &str) -> bool {
    let bytes = formula.as_bytes();
    let iv = b"iv";
    for i in 0..bytes.len().saturating_sub(1) {
        if &bytes[i..i + 2] == iv {
            let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_';
            let after_ok = i + 2 >= bytes.len()
                || !bytes[i + 2].is_ascii_alphanumeric() && bytes[i + 2] != b'_';
            if before_ok && after_ok {
                return true;
            }
        }
    }
    false
}

/// Load and prepare the OHLCV `DataFrame` for signal evaluation.
///
/// When IV-based signals are used, aggregates daily IV from the options `DataFrame`
/// and merges it into the OHLCV `DataFrame` so all signals evaluate against one unified `DataFrame`.
/// For pure IV signals (no OHLCV path), a minimal `DataFrame` is constructed from the IV aggregation.
/// When cross-symbol formula references are present, loads secondary symbol `DataFrames`
/// from `params.cross_ohlcv_paths` and uses `active_dates_multi` for evaluation.
///
/// Handles three cases: pure OHLCV signals, pure IV signals, and mixed.
/// For intraday OHLCV data, filters to only the 15:59 bar per day to
/// match options pricing time.
fn load_signal_ohlcv(
    params: &BacktestParams,
    options_df: &DataFrame,
) -> Result<(DataFrame, &'static str)> {
    let needs_iv = params.entry_signal.as_ref().is_some_and(contains_iv_signal)
        || params.exit_signal.as_ref().is_some_and(contains_iv_signal);
    let needs_ohlcv = params
        .entry_signal
        .as_ref()
        .is_some_and(contains_non_iv_signal)
        || params
            .exit_signal
            .as_ref()
            .is_some_and(contains_non_iv_signal);

    let ohlcv_df = if needs_ohlcv {
        if let Some(ohlcv_path) = params.ohlcv_path.as_deref() {
            let mut df = load_ohlcv(ohlcv_path)?;
            if needs_iv {
                let iv_df = signals::volatility::aggregate_daily_iv(options_df)?;
                df = df
                    .lazy()
                    .join(
                        iv_df.lazy(),
                        [col("date")],
                        [col("date")],
                        JoinArgs {
                            how: JoinType::Left,
                            maintain_order: MaintainOrderJoin::Left,
                            ..Default::default()
                        },
                    )
                    .collect()?;
            }
            df
        } else {
            return Err(anyhow::anyhow!(
                "ohlcv_path is required when entry_signal or exit_signal references OHLCV columns (e.g. close, volume). \
                 Pure IV formulas (e.g. iv_rank(iv, 252) > 50) do not require OHLCV data."
            ));
        }
    } else if needs_iv {
        signals::volatility::aggregate_daily_iv(options_df)?
    } else {
        return Err(anyhow::anyhow!(
            "Unable to determine required data for entry/exit signals. \
             This may indicate ohlcv_path is missing, or a Saved signal could not be resolved."
        ));
    };

    let date_col = stock_sim::detect_date_col(&ohlcv_df);

    // For intraday OHLCV, filter to only the 15:59 bar per day to match options pricing time.
    // We clone here because `DataFrame::lazy()` takes ownership, but we still need the original
    // `ohlcv_df` as a fallback if the filtered result is empty or the lazy computation fails.
    // This incurs a one-time clone cost on the intraday path, but we do not retain two full
    // DataFrames after the match completes.
    let ohlcv_df = if date_col == "datetime" {
        match ohlcv_df
            .clone()
            .lazy()
            .filter(
                col("datetime")
                    .dt()
                    .hour()
                    .eq(lit(15))
                    .and(col("datetime").dt().minute().eq(lit(59))),
            )
            .collect()
        {
            Ok(filtered) if filtered.height() > 0 => filtered,
            Ok(_empty) => {
                // No explicit 15:59 bar found; fall back to last bar per day.
                tracing::warn!(
                    "Intraday OHLCV contained no 15:59 bars; falling back to last bar per date."
                );
                intraday_last_bar_per_day(ohlcv_df)?
            }
            Err(e) => {
                // If filtering itself fails, log the error and fall back to last bar per date
                // to keep one-row-per-day semantics and avoid evaluating signals on full
                // intraday data.
                tracing::warn!(
                    "Failed to filter intraday OHLCV to 15:59 bars: {e:?}. \
                     Falling back to last bar per date."
                );
                intraday_last_bar_per_day(ohlcv_df)?
            }
        }
    } else {
        ohlcv_df
    };

    Ok((ohlcv_df, date_col))
}

/// Reduce an intraday `DataFrame` to one row per calendar date by keeping the last bar.
///
/// Sorts by `"datetime"`, adds a temporary `"_date"` column, deduplicates keeping the last
/// row per date, then drops the helper column. This preserves one-row-per-day semantics
/// when a 15:59 bar is unavailable.
fn intraday_last_bar_per_day(df: DataFrame) -> Result<DataFrame> {
    let original_cols: Vec<Expr> = df
        .get_column_names()
        .iter()
        .map(|n| col(n.as_str()))
        .collect();
    df.lazy()
        .sort(
            ["datetime"],
            polars::prelude::SortMultipleOptions::default(),
        )
        .with_column(col("datetime").dt().date().alias("_date"))
        .unique_generic(
            Some(vec![col("_date")]),
            polars::prelude::UniqueKeepStrategy::Last,
        )
        .select(original_cols)
        .collect()
        .context("failed to compute last intraday bar per date")
}

/// If the `SignalSpec` is a `Formula` variant whose formula contains `hmm_regime(`,
/// run the HMM preprocessing pass and return a rewritten spec + updated `DataFrame`.
/// Returns `None` when no HMM processing is needed (avoiding a `DataFrame` clone).
fn maybe_preprocess_hmm(
    spec: &SignalSpec,
    primary_symbol: &str,
    df: &DataFrame,
    cache_dir: Option<&std::path::Path>,
    date_col: &str,
) -> Result<Option<(SignalSpec, DataFrame)>> {
    if let SignalSpec::Formula { formula } = spec {
        if formula.contains("hmm_regime(") {
            let (rewritten, updated_df) =
                signals::preprocess_hmm_regime(formula, primary_symbol, df, cache_dir, date_col)?;
            return Ok(Some((
                SignalSpec::Formula { formula: rewritten },
                updated_df,
            )));
        }
    }
    Ok(None)
}

/// Derive a primary symbol name from an OHLCV file path (filename stem, uppercased).
fn symbol_from_ohlcv_path(path: &str) -> String {
    std::path::Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("UNKNOWN")
        .to_uppercase()
}

pub fn build_signal_filters(
    params: &BacktestParams,
    options_df: &DataFrame,
    cache_dir: Option<&std::path::Path>,
) -> Result<(DateFilter, DateFilter)> {
    if params.entry_signal.is_none() && params.exit_signal.is_none() {
        return Ok((None, None));
    }

    let (mut ohlcv_df, date_col) = load_signal_ohlcv(params, options_df)?;

    // --- HMM regime preprocessing ---
    let primary_symbol = params
        .ohlcv_path
        .as_deref()
        .map_or_else(|| "UNKNOWN".to_string(), symbol_from_ohlcv_path);

    let mut entry_signal = params.entry_signal.clone();
    let mut exit_signal = params.exit_signal.clone();

    if let Some(spec) = entry_signal.as_ref() {
        if let Some((new_spec, new_df)) =
            maybe_preprocess_hmm(spec, &primary_symbol, &ohlcv_df, cache_dir, date_col)?
        {
            entry_signal = Some(new_spec);
            ohlcv_df = new_df;
        }
    }
    if let Some(spec) = exit_signal.as_ref() {
        if let Some((new_spec, new_df)) =
            maybe_preprocess_hmm(spec, &primary_symbol, &ohlcv_df, cache_dir, date_col)?
        {
            exit_signal = Some(new_spec);
            ohlcv_df = new_df;
        }
    }

    // Check if any signal references a cross-symbol
    let has_cross = entry_signal
        .as_ref()
        .is_some_and(|s| !collect_cross_symbols(s).is_empty())
        || exit_signal
            .as_ref()
            .is_some_and(|s| !collect_cross_symbols(s).is_empty());

    if has_cross {
        // Load all cross-symbol DataFrames
        let mut cross_dfs: HashMap<String, DataFrame> = HashMap::new();
        for (sym, path) in &params.cross_ohlcv_paths {
            cross_dfs.insert(sym.to_uppercase(), load_ohlcv(path)?);
        }

        let entry_dates = entry_signal
            .as_ref()
            .map(|spec| signals::active_dates_multi(spec, &ohlcv_df, &cross_dfs, date_col))
            .transpose()?;
        let exit_dates = exit_signal
            .as_ref()
            .map(|spec| signals::active_dates_multi(spec, &ohlcv_df, &cross_dfs, date_col))
            .transpose()?;

        Ok((entry_dates, exit_dates))
    } else {
        // Fast path: no cross-symbol references
        let entry_dates = entry_signal
            .as_ref()
            .map(|spec| signals::active_dates(spec, &ohlcv_df, date_col))
            .transpose()?;
        let exit_dates = exit_signal
            .as_ref()
            .map(|spec| signals::active_dates(spec, &ohlcv_df, date_col))
            .transpose()?;

        Ok((entry_dates, exit_dates))
    }
}

/// Run a full backtest simulation.
///
/// Dispatches to the vectorized path when no adjustment rules are configured,
/// falling back to the event-driven day-by-day loop for adjustment rules.
pub fn run_backtest(df: &DataFrame, params: &BacktestParams) -> Result<BacktestResult> {
    let strategy_def = strategies::find_strategy(&params.strategy)
        .ok_or_else(|| anyhow::anyhow!("Unknown strategy: {}", params.strategy))?;

    tracing::info!(
        strategy = %params.strategy,
        legs = strategy_def.legs.len(),
        "Strategy resolved"
    );

    if params.leg_deltas.len() != strategy_def.legs.len() {
        bail!(
            "Strategy '{}' has {} legs but {} delta targets provided",
            params.strategy,
            strategy_def.legs.len(),
            params.leg_deltas.len()
        );
    }

    // Derive cache_dir from ohlcv_path (path is {cache_dir}/{category}/{SYMBOL}.parquet)
    let cache_dir = params
        .ohlcv_path
        .as_deref()
        .and_then(|p| std::path::Path::new(p).parent())
        .and_then(|p| p.parent());

    // Build signal date filters if specified (loads OHLCV at most once)
    let (entry_dates, exit_dates) = build_signal_filters(params, df, cache_dir)?;

    if entry_dates.is_some() || exit_dates.is_some() {
        tracing::info!(
            entry_signal_dates = entry_dates.as_ref().map_or(0, HashSet::len),
            exit_signal_dates = exit_dates.as_ref().map_or(0, HashSet::len),
            "Signal filters loaded"
        );
    }

    // Dynamic sizing and stock-leg strategies require sequential equity tracking — force event loop
    let use_vectorized = params.adjustment_rules.is_empty()
        && params.sizing.is_none()
        && !strategy_def.has_stock_leg;
    tracing::info!(
        path = if use_vectorized {
            "vectorized"
        } else {
            "event_loop"
        },
        "Backtest dispatch"
    );

    let (trade_log, equity_curve, quality) = if use_vectorized {
        // Vectorized path — much faster for strategies without adjustments
        vectorized_sim::run_vectorized_backtest(df, params, &entry_dates, exit_dates.as_ref())?
    } else {
        // Adjustment rules or dynamic sizing require sequential state — fall back to event loop
        run_event_loop_path(df, params, &strategy_def, &entry_dates, &exit_dates)?
    };

    let perf_metrics =
        metrics::calculate_metrics(&equity_curve, &trade_log, params.capital, 252.0)?;

    let total_pnl: f64 = trade_log.iter().map(|t| t.pnl).sum();
    tracing::info!(
        trades = trade_log.len(),
        total_pnl = format_args!("{total_pnl:.2}"),
        "Backtest complete"
    );

    Ok(BacktestResult {
        trade_count: trade_log.len(),
        total_pnl,
        metrics: perf_metrics,
        equity_curve,
        trade_log,
        quality,
        warnings: vec![],
    })
}

/// Event-loop fallback path for strategies with adjustment rules.
fn run_event_loop_path(
    df: &DataFrame,
    params: &BacktestParams,
    strategy_def: &StrategyDef,
    entry_dates: &DateFilter,
    exit_dates: &DateFilter,
) -> Result<(Vec<TradeRecord>, Vec<EquityPoint>, BacktestQualityStats)> {
    let (price_table, trading_days, date_index) = event_sim::build_price_table(df)?;
    let mut candidates = event_sim::find_entry_candidates(df, strategy_def, params)?;

    // Filter entry candidates to only dates where the entry signal is active
    if let Some(ref allowed_dates) = entry_dates {
        candidates.retain(|date, _| allowed_dates.contains(date));
    }

    // Load OHLCV closes when dynamic sizing or stock-leg strategy needs price data
    let ohlcv_closes = load_ohlcv_closes(params, strategy_def)?;

    let ctx = crate::engine::sim_types::SimContext {
        price_table: &price_table,
        params,
        strategy_def,
        ohlcv_closes: ohlcv_closes.as_ref(),
    };
    let (trade_log, equity_curve, quality) = event_sim::run_event_loop(
        &ctx,
        &candidates,
        &trading_days,
        exit_dates.as_ref(),
        &date_index,
    );

    Ok((trade_log, equity_curve, quality))
}

/// Build `BacktestParams` from a `CompareEntry` and `SimParams`.
fn build_backtest_params(entry: &CompareEntry, sim: &SimParams) -> BacktestParams {
    BacktestParams {
        strategy: entry.name.clone(),
        leg_deltas: entry.leg_deltas.clone(),
        entry_dte: entry.entry_dte.clone(),
        exit_dte: entry.exit_dte,
        slippage: entry.slippage.clone(),
        commission: entry.commission.clone(),
        min_bid_ask: default_min_bid_ask(),
        stop_loss: sim.stop_loss,
        take_profit: sim.take_profit,
        max_hold_days: sim.max_hold_days,
        capital: sim.capital,
        quantity: sim.quantity,
        sizing: sim.sizing.clone(),
        multiplier: sim.multiplier,
        max_positions: sim.max_positions,
        selector: sim.selector.clone(),
        adjustment_rules: vec![],
        entry_signal: sim.entry_signal.clone(),
        exit_signal: sim.exit_signal.clone(),
        ohlcv_path: sim.ohlcv_path.clone(),
        cross_ohlcv_paths: sim.cross_ohlcv_paths.clone(),
        min_net_premium: None,
        max_net_premium: None,
        min_net_delta: None,
        max_net_delta: None,
        min_days_between_entries: sim.min_days_between_entries,
        expiration_filter: ExpirationFilter::Any,
        exit_net_delta: sim.exit_net_delta,
    }
}

/// Compare multiple strategies.
///
/// Auto-generates descriptive labels when multiple entries share the same strategy
/// name (e.g. `long_call(Δ0.30,DTE45)` vs `long_call(Δ0.40,DTE60)`).
/// Deduplicates identical entries to avoid wasted computation.
#[allow(clippy::unnecessary_wraps)]
pub fn compare_strategies(
    df: &DataFrame,
    params: &CompareParams,
) -> Result<(Vec<CompareResult>, Vec<CompareEntry>)> {
    // Build labels and deduplicate
    let labels = build_compare_labels(&params.strategies);
    let mut results = Vec::new();
    let mut labeled_entries = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    for (entry, label) in params.strategies.iter().zip(labels.iter()) {
        // Skip duplicate entries using a full-parameter key (labels omit min/max ranges)
        let dedup_key = compare_dedup_key(entry);
        if !seen.insert(dedup_key) {
            tracing::info!("Skipping duplicate entry: {label}");
            continue;
        }

        // Store the entry as-is so `name` remains the strategy identifier
        labeled_entries.push(entry.clone());

        let backtest_params = build_backtest_params(entry, &params.sim_params);

        match run_backtest(df, &backtest_params) {
            Ok(bt) => {
                results.push(CompareResult {
                    display_name: to_display_name(label),
                    strategy: label.clone(),
                    trades: bt.trade_count,
                    pnl: bt.total_pnl,
                    sharpe: bt.metrics.sharpe,
                    sortino: bt.metrics.sortino,
                    max_dd: bt.metrics.max_drawdown,
                    win_rate: bt.metrics.win_rate,
                    profit_factor: bt.metrics.profit_factor,
                    calmar: bt.metrics.calmar,
                    total_return_pct: bt.metrics.total_return_pct,
                    trade_log: bt.trade_log,
                    error: None,
                });
            }
            Err(e) => {
                tracing::warn!("Strategy '{label}' failed: {e}");
                results.push(CompareResult {
                    display_name: to_display_name(label),
                    strategy: label.clone(),
                    trades: 0,
                    pnl: 0.0,
                    sharpe: 0.0,
                    sortino: 0.0,
                    max_dd: 0.0,
                    win_rate: 0.0,
                    profit_factor: 0.0,
                    calmar: 0.0,
                    total_return_pct: 0.0,
                    trade_log: vec![],
                    error: Some(e.to_string()),
                });
            }
        }
    }

    Ok((results, labeled_entries))
}

// ---------------------------------------------------------------------------
// Stock compare
// ---------------------------------------------------------------------------

use super::stock_sim::{self, StockBacktestParams};

/// A single stock entry for comparison.
#[derive(Debug, Clone)]
pub struct StockCompareEntry {
    pub label: String,
    pub params: StockBacktestParams,
}

/// Compare multiple stock strategies side-by-side.
///
/// Each entry carries its own `StockBacktestParams` (with entry/exit signals,
/// interval, side, etc.). Data is prepared once per unique `(ohlcv_path,
/// interval, session_filter, start_date, end_date)` group to avoid redundant
/// I/O for entries that share the same underlying data.
#[allow(clippy::too_many_lines)]
pub fn compare_stock_strategies(entries: &[StockCompareEntry]) -> Result<Vec<CompareResult>> {
    // Reject duplicate labels up front so callers get a clear error instead of silent skipping.
    let mut seen_labels: std::collections::HashSet<String> = std::collections::HashSet::new();
    for entry in entries {
        if !seen_labels.insert(entry.label.clone()) {
            anyhow::bail!(
                "Duplicate stock compare entry label '{}'; every entry must have a unique label",
                entry.label
            );
        }
    }

    // Cache prepared (bars, ohlcv_df) by a canonical data-prep key to avoid
    // re-reading and resampling the same parquet for each entry.
    let mut data_cache: HashMap<String, (Vec<stock_sim::Bar>, polars::prelude::DataFrame)> =
        HashMap::new();

    let mut results = Vec::new();

    for entry in entries {
        let ohlcv_path = entry
            .params
            .ohlcv_path
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("ohlcv_path is required for stock compare"))?;

        // Build a string key that uniquely identifies the data-prep parameters.
        let data_key = format!(
            "{}|{}|{}|{}|{}",
            ohlcv_path,
            entry.params.interval,
            entry
                .params
                .session_filter
                .as_ref()
                .map(|s| format!("{s:?}"))
                .unwrap_or_default(),
            entry
                .params
                .start_date
                .map(|d| d.to_string())
                .unwrap_or_default(),
            entry
                .params
                .end_date
                .map(|d| d.to_string())
                .unwrap_or_default(),
        );

        let (bars, ohlcv_df) = if let Some(cached) = data_cache.get(&data_key) {
            (&cached.0, &cached.1)
        } else {
            let prepared = stock_sim::prepare_stock_data(
                ohlcv_path,
                entry.params.interval,
                entry.params.session_filter.as_ref(),
                entry.params.start_date,
                entry.params.end_date,
            )?;
            data_cache.insert(data_key.clone(), prepared);
            let cached = data_cache.get(&data_key).expect("just inserted");
            (&cached.0, &cached.1)
        };

        // Derive cache_dir from ohlcv_path ({cache_dir}/{category}/{SYMBOL}.parquet)
        let (entry_dates, exit_dates) = stock_sim::build_stock_signal_filters(
            &entry.params,
            ohlcv_df,
            stock_sim::ohlcv_path_to_cache_root(ohlcv_path),
        )?;

        match stock_sim::run_stock_backtest(
            bars,
            &entry.params,
            entry_dates.as_ref(),
            exit_dates.as_ref(),
        ) {
            Ok(bt) => {
                results.push(CompareResult {
                    display_name: entry.label.clone(),
                    strategy: entry.label.clone(),
                    trades: bt.trade_count,
                    pnl: bt.total_pnl,
                    sharpe: bt.metrics.sharpe,
                    sortino: bt.metrics.sortino,
                    max_dd: bt.metrics.max_drawdown,
                    win_rate: bt.metrics.win_rate,
                    profit_factor: bt.metrics.profit_factor,
                    calmar: bt.metrics.calmar,
                    total_return_pct: bt.metrics.total_return_pct,
                    trade_log: bt.trade_log,
                    error: None,
                });
            }
            Err(e) => {
                tracing::warn!("Stock compare entry '{}' failed: {e}", entry.label);
                results.push(CompareResult {
                    display_name: entry.label.clone(),
                    strategy: entry.label.clone(),
                    trades: 0,
                    pnl: 0.0,
                    sharpe: 0.0,
                    sortino: 0.0,
                    max_dd: 0.0,
                    win_rate: 0.0,
                    profit_factor: 0.0,
                    calmar: 0.0,
                    total_return_pct: 0.0,
                    trade_log: vec![],
                    error: Some(e.to_string()),
                });
            }
        }
    }

    Ok(results)
}

/// Build descriptive labels for compare entries.
///
/// If all entries have unique strategy names, the labels are just the names.
/// Builds a canonical deduplication key that covers the full parameter set,
/// including DteRange/TargetRange min/max values that the display label omits.
fn compare_dedup_key(entry: &CompareEntry) -> String {
    let deltas: Vec<String> = entry
        .leg_deltas
        .iter()
        .map(|d| format!("{:.4}:{:.4}:{:.4}", d.target, d.min, d.max))
        .collect();
    let slippage_str = match &entry.slippage {
        Slippage::Spread => "spread".to_string(),
        Slippage::Mid => "mid".to_string(),
        Slippage::Liquidity {
            fill_ratio,
            ref_volume,
        } => {
            format!("liq:{fill_ratio:.4}:{ref_volume}")
        }
        Slippage::PerLeg { per_leg } => format!("pleg:{per_leg:.4}"),
        Slippage::BidAskTravel { pct } => format!("bat:{pct:.4}"),
    };
    let commission_str = match &entry.commission {
        None => "none".to_string(),
        Some(c) => format!("{:.4}:{:.4}:{:.4}", c.per_contract, c.base_fee, c.min_fee),
    };
    format!(
        "{}|{}|{}:{}:{}|{}|{}|{}",
        entry.name,
        deltas.join(","),
        entry.entry_dte.target,
        entry.entry_dte.min,
        entry.entry_dte.max,
        entry.exit_dte,
        slippage_str,
        commission_str,
    )
}

/// Builds a human-readable label for each compare entry.
/// e.g. `long_call(Δ0.40, DTE 45, Exit 7)` or `bull_call_spread(Δ0.50/0.10, DTE 60, Exit 9)`.
fn build_compare_labels(entries: &[CompareEntry]) -> Vec<String> {
    // Count how many times each strategy name appears
    let mut name_counts: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for entry in entries {
        *name_counts.entry(&entry.name).or_insert(0) += 1;
    }

    entries
        .iter()
        .map(|entry| {
            if name_counts.get(entry.name.as_str()).copied().unwrap_or(0) <= 1 {
                // Unique name — no suffix needed
                entry.name.clone()
            } else {
                // Duplicate name — add parameter details
                let deltas: Vec<String> = entry
                    .leg_deltas
                    .iter()
                    .map(|d| format!("{:.2}", d.target))
                    .collect();
                let delta_str = deltas.join("/");
                let slippage_suffix = match &entry.slippage {
                    Slippage::Spread => String::new(),
                    Slippage::Mid => ", mid".to_string(),
                    Slippage::Liquidity {
                        fill_ratio,
                        ref_volume,
                    } => format!(", liq(fr={fill_ratio:.2}, rv={ref_volume})"),
                    Slippage::PerLeg { per_leg } => format!(", pleg({per_leg:.2})"),
                    Slippage::BidAskTravel { pct } => format!(", bat({pct:.2})"),
                };
                format!(
                    "{}(Δ{}, DTE {}, Exit {}{})",
                    entry.name, delta_str, entry.entry_dte.target, entry.exit_dte, slippage_suffix
                )
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(name: &str, delta: f64, dte: i32) -> CompareEntry {
        CompareEntry {
            name: name.to_string(),
            leg_deltas: vec![TargetRange {
                target: delta,
                min: delta - 0.05,
                max: delta + 0.05,
            }],
            entry_dte: DteRange {
                target: dte,
                min: dte - 10,
                max: dte + 10,
            },
            exit_dte: 7,
            slippage: Slippage::Spread,
            commission: None,
        }
    }

    #[test]
    fn compare_labels_unique_names_unchanged() {
        let entries = vec![
            make_entry("iron_condor", 0.30, 45),
            make_entry("short_put", 0.25, 30),
        ];
        let labels = build_compare_labels(&entries);
        assert_eq!(labels, vec!["iron_condor", "short_put"]);
    }

    #[test]
    fn compare_labels_duplicate_names_get_params() {
        let entries = vec![
            make_entry("long_call", 0.30, 45),
            make_entry("long_call", 0.40, 45),
            make_entry("long_call", 0.40, 60),
        ];
        let labels = build_compare_labels(&entries);
        assert_eq!(labels[0], "long_call(Δ0.30, DTE 45, Exit 7)");
        assert_eq!(labels[1], "long_call(Δ0.40, DTE 45, Exit 7)");
        assert_eq!(labels[2], "long_call(Δ0.40, DTE 60, Exit 7)");
    }

    #[test]
    fn compare_labels_multi_leg_deltas() {
        let entries = vec![
            CompareEntry {
                name: "bull_call_spread".to_string(),
                leg_deltas: vec![
                    TargetRange {
                        target: 0.50,
                        min: 0.45,
                        max: 0.55,
                    },
                    TargetRange {
                        target: 0.10,
                        min: 0.05,
                        max: 0.15,
                    },
                ],
                entry_dte: DteRange {
                    target: 45,
                    min: 30,
                    max: 60,
                },
                exit_dte: 9,
                slippage: Slippage::Spread,
                commission: None,
            },
            CompareEntry {
                name: "bull_call_spread".to_string(),
                leg_deltas: vec![
                    TargetRange {
                        target: 0.50,
                        min: 0.45,
                        max: 0.55,
                    },
                    TargetRange {
                        target: 0.20,
                        min: 0.15,
                        max: 0.25,
                    },
                ],
                entry_dte: DteRange {
                    target: 45,
                    min: 30,
                    max: 60,
                },
                exit_dte: 9,
                slippage: Slippage::Spread,
                commission: None,
            },
        ];
        let labels = build_compare_labels(&entries);
        assert_eq!(labels[0], "bull_call_spread(Δ0.50/0.10, DTE 45, Exit 9)");
        assert_eq!(labels[1], "bull_call_spread(Δ0.50/0.20, DTE 45, Exit 9)");
    }

    #[test]
    fn compare_labels_slippage_suffix() {
        let mut entry_mid = make_entry("long_call", 0.30, 45);
        entry_mid.slippage = Slippage::Mid;
        let entry_spread = make_entry("long_call", 0.30, 60);
        let labels = build_compare_labels(&[entry_mid, entry_spread]);
        assert_eq!(labels[0], "long_call(Δ0.30, DTE 45, Exit 7, mid)");
        assert_eq!(labels[1], "long_call(Δ0.30, DTE 60, Exit 7)");
    }

    // Integration tests (run_backtest_*, signal_filters_*) moved to tests/options_backtest_engine.rs
    #[test]
    fn formula_references_iv_standalone() {
        assert!(formula_references_iv("iv_rank(iv, 252) > 50"));
        assert!(formula_references_iv("iv > 0.3"));
        assert!(formula_references_iv("rank(iv, 252) < 10"));
    }

    #[test]
    fn formula_references_iv_not_substring() {
        assert!(!formula_references_iv("close > sma(close, 20)"));
        assert!(!formula_references_iv("pivot > 100"));
        assert!(!formula_references_iv("relative_volume > 2"));
    }

    #[test]
    fn contains_iv_signal_detects_formula_iv() {
        let spec = signals::registry::SignalSpec::Formula {
            formula: "iv_rank(iv, 252) > 50".into(),
        };
        assert!(contains_iv_signal(&spec));
    }

    #[test]
    fn contains_iv_signal_false_for_non_iv_formula() {
        let spec = signals::registry::SignalSpec::Formula {
            formula: "rsi(close, 14) < 30".into(),
        };
        assert!(!contains_iv_signal(&spec));
    }

    #[test]
    fn contains_iv_signal_nested_and() {
        let spec = signals::registry::SignalSpec::And {
            left: Box::new(signals::registry::SignalSpec::Formula {
                formula: "rsi(close, 14) < 30".into(),
            }),
            right: Box::new(signals::registry::SignalSpec::Formula {
                formula: "iv_rank(iv, 252) > 50".into(),
            }),
        };
        assert!(contains_iv_signal(&spec));
        assert!(contains_non_iv_signal(&spec));
    }
}
