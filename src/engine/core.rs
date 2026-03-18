//! Top-level orchestration for backtests and strategy comparisons.
//!
//! Resolves strategy definitions, builds signal filters, and dispatches to
//! either the vectorized or event-driven simulation path.

use std::collections::{HashMap, HashSet};

use anyhow::{bail, Result};
use chrono::NaiveDate;
use polars::prelude::*;

use super::event_sim;
use super::metrics;
#[allow(clippy::wildcard_imports)]
use super::types::*;
use super::vectorized_sim;
use crate::signals;
use crate::signals::registry::collect_cross_symbols;
use crate::strategies;

type DateFilter = Option<HashSet<NaiveDate>>;

/// Load OHLCV close prices into a `BTreeMap<NaiveDate, f64>` for sizing or stock-leg strategies.
///
/// Loads when the backtest has dynamic sizing configured or when the strategy has a stock leg.
/// Returns `None` if neither condition is met or OHLCV data cannot be loaded.
fn load_ohlcv_closes(
    params: &BacktestParams,
    strategy_def: &StrategyDef,
) -> Option<std::collections::BTreeMap<NaiveDate, f64>> {
    if params.sizing.is_none() && !strategy_def.has_stock_leg {
        return None;
    }

    let ohlcv_path = params.ohlcv_path.as_ref()?;
    let df = load_ohlcv(ohlcv_path).ok()?;

    let mut closes_map = std::collections::BTreeMap::new();

    // Intraday path: "datetime" Datetime column → extract date portion.
    // Sort by datetime so later entries overwrite earlier ones, giving last-close-per-day.
    // Only take this branch when the column is actually a Datetime dtype.
    let has_datetime = df
        .column("datetime")
        .ok()
        .is_some_and(|c| matches!(c.dtype(), polars::prelude::DataType::Datetime(_, _)));
    if has_datetime {
        let sorted = df
            .clone()
            .lazy()
            .sort(
                ["datetime"],
                polars::prelude::SortMultipleOptions::default(),
            )
            .collect()
            .ok()?;
        let closes = sorted.column("close").ok()?.f64().ok()?;
        let dt_col_ref = sorted.column("datetime").ok()?;
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
        let closes = df.column("close").ok()?.f64().ok()?;
        let dates = df.column("date").ok()?.date().ok()?;
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
        None
    } else {
        Some(closes_map)
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
            SignalSpec::CrossSymbol { signal, .. } => inner(signal, pred, depth + 1),
            SignalSpec::And { left, right } | SignalSpec::Or { left, right } => {
                inner(left, pred, depth + 1) || inner(right, pred, depth + 1)
            }
            SignalSpec::Saved { name } => signals::storage::load_signal(name)
                .map(|s| inner(&s, pred, depth + 1))
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

/// Build entry/exit date filters from signal specs, loading OHLCV at most once.
///
/// When IV-based signals are used, aggregates daily IV from the options `DataFrame`
/// and merges it into the OHLCV `DataFrame` so all signals evaluate against one unified `DataFrame`.
/// For pure IV signals (no OHLCV path), a minimal `DataFrame` is constructed from the IV aggregation.
/// When `CrossSymbol` variants are present, loads secondary symbol `DataFrame`s
/// from `params.cross_ohlcv_paths` and uses `active_dates_multi` for evaluation.
pub fn build_signal_filters(
    params: &BacktestParams,
    options_df: &DataFrame,
) -> Result<(DateFilter, DateFilter)> {
    let has_entry = params.entry_signal.is_some();
    let has_exit = params.exit_signal.is_some();

    if !has_entry && !has_exit {
        return Ok((None, None));
    }

    let needs_iv = params.entry_signal.as_ref().is_some_and(contains_iv_signal)
        || params.exit_signal.as_ref().is_some_and(contains_iv_signal);

    // For pure IV signals, we don't require ohlcv_path.
    // Need OHLCV only if any non-IV leaf signal exists in the spec tree.
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
        // Pure IV signals only
        signals::volatility::aggregate_daily_iv(options_df)?
    } else {
        return Err(anyhow::anyhow!(
            "Unable to determine required data for entry/exit signals. \
             This may indicate ohlcv_path is missing, or a Saved signal could not be resolved."
        ));
    };

    // Detect the date column name from the OHLCV DataFrame — may be "datetime" or "date"
    let date_col = stock_sim::detect_date_col(&ohlcv_df);

    // Check if any signal references a cross-symbol
    let has_cross = params
        .entry_signal
        .as_ref()
        .is_some_and(|s| !collect_cross_symbols(s).is_empty())
        || params
            .exit_signal
            .as_ref()
            .is_some_and(|s| !collect_cross_symbols(s).is_empty());

    if has_cross {
        // Load all cross-symbol DataFrames
        let mut cross_dfs: HashMap<String, DataFrame> = HashMap::new();
        for (sym, path) in &params.cross_ohlcv_paths {
            cross_dfs.insert(sym.to_uppercase(), load_ohlcv(path)?);
        }

        let entry_dates = params
            .entry_signal
            .as_ref()
            .map(|spec| signals::active_dates_multi(spec, &ohlcv_df, &cross_dfs, date_col))
            .transpose()?;
        let exit_dates = params
            .exit_signal
            .as_ref()
            .map(|spec| signals::active_dates_multi(spec, &ohlcv_df, &cross_dfs, date_col))
            .transpose()?;

        Ok((entry_dates, exit_dates))
    } else {
        // Fast path: no cross-symbol references
        let entry_dates = params
            .entry_signal
            .as_ref()
            .map(|spec| signals::active_dates(spec, &ohlcv_df, date_col))
            .transpose()?;
        let exit_dates = params
            .exit_signal
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

    // Build signal date filters if specified (loads OHLCV at most once)
    let (entry_dates, exit_dates) = build_signal_filters(params, df)?;

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
    let ohlcv_closes = load_ohlcv_closes(params, strategy_def);

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

        let (entry_dates, exit_dates) =
            stock_sim::build_stock_signal_filters(&entry.params, ohlcv_df)?;

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
    use crate::data::parquet::DATETIME_COL;
    use chrono::NaiveDate;

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

    /// Build a daily options `DataFrame` with intermediate dates for event-driven backtest.
    /// Shows price decay from entry to exit for a long call.
    fn make_daily_options_df() -> DataFrame {
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        // 6 days of data: entry at Jan 15 (DTE=32), decay through to Feb 11 (DTE=5)
        let dates: Vec<_> = vec![
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), // DTE=32
            NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(), // DTE=25
            NaiveDate::from_ymd_opt(2024, 1, 29).unwrap(), // DTE=18
            NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),  // DTE=15
            NaiveDate::from_ymd_opt(2024, 2, 5).unwrap(),  // DTE=11
            NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(), // DTE=5
        ];

        let quote_dates: Vec<_> = dates
            .iter()
            .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
            .collect();
        let expirations: Vec<_> = dates.iter().map(|_| exp).collect();

        // Simulate time decay: option losing value over time
        let bids = vec![5.00, 4.50, 3.80, 3.20, 2.60, 2.00f64];
        let asks = vec![5.50, 5.00, 4.30, 3.70, 3.10, 2.50f64];
        let deltas = vec![0.50, 0.47, 0.42, 0.38, 0.33, 0.25f64];

        let n = dates.len();
        let mut df = df! {
            DATETIME_COL => &quote_dates,
            "option_type" => vec!["call"; n],
            "strike" => vec![100.0f64; n],
            "bid" => &bids,
            "ask" => &asks,
            "delta" => &deltas,
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations).into_column(),
        )
        .unwrap();
        df
    }

    /// Build daily data where price drops sharply to trigger stop loss.
    fn make_stop_loss_df() -> DataFrame {
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let dates: Vec<_> = vec![
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), // DTE=32, entry
            NaiveDate::from_ymd_opt(2024, 1, 16).unwrap(), // DTE=31
            NaiveDate::from_ymd_opt(2024, 1, 17).unwrap(), // DTE=30, big drop → SL
            NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(), // DTE=25
            NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(), // DTE=5
        ];

        let quote_dates: Vec<_> = dates
            .iter()
            .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
            .collect();
        let expirations: Vec<_> = dates.iter().map(|_| exp).collect();

        // Sharp drop on day 3: entry mid=5.25, day 3 mid=1.25 → loss = 400 > 50% of 525 = 262.5
        let bids = vec![5.00, 4.00, 1.00, 0.80, 0.50f64];
        let asks = vec![5.50, 4.50, 1.50, 1.30, 1.00f64];
        let deltas = vec![0.50, 0.45, 0.15, 0.12, 0.08f64];

        let n = dates.len();
        let mut df = df! {
            DATETIME_COL => &quote_dates,
            "option_type" => vec!["call"; n],
            "strike" => vec![100.0f64; n],
            "bid" => &bids,
            "ask" => &asks,
            "delta" => &deltas,
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations).into_column(),
        )
        .unwrap();
        df
    }

    fn default_backtest_params() -> BacktestParams {
        BacktestParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.50,
                min: 0.20,
                max: 0.80,
            }],
            entry_dte: DteRange {
                target: 45,
                min: 10,
                max: 60,
            },
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            sizing: None,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: None,
        }
    }

    #[test]
    fn run_backtest_e2e_long_call() {
        let df = make_daily_options_df();
        let params = default_backtest_params();

        let result = run_backtest(&df, &params);
        assert!(result.is_ok(), "run_backtest failed: {:?}", result.err());
        let bt = result.unwrap();

        assert_eq!(bt.trade_count, 1);
        // Long call: buy at mid 5.25 on Jan 15, DTE exit triggers on Feb 11 (DTE=5)
        // Sell at mid 2.25 → loss = (2.25 - 5.25) * 100 = -300
        assert!(
            (bt.total_pnl - (-300.0)).abs() < 1.0,
            "Expected ~-300 PnL, got {}",
            bt.total_pnl
        );
        assert!(!bt.equity_curve.is_empty());
        assert_eq!(bt.trade_log.len(), 1);
        // Entry Jan 15, exit Feb 11 = 27 days
        assert_eq!(bt.trade_log[0].days_held, 27);
    }

    #[test]
    fn run_backtest_daily_equity_curve_has_all_days() {
        let df = make_daily_options_df();
        let params = default_backtest_params();

        let result = run_backtest(&df, &params).unwrap();

        // Should have one equity point per trading day (6 days)
        assert_eq!(
            result.equity_curve.len(),
            6,
            "Expected 6 equity points (one per trading day), got {}",
            result.equity_curve.len()
        );

        // First day equity should include unrealized (entry at mid 5.25, current mid 5.25 → 0 unrealized)
        assert!(
            (result.equity_curve[0].equity - 10000.0).abs() < 1.0,
            "Day 1 equity should be ~10000, got {}",
            result.equity_curve[0].equity
        );
    }

    #[test]
    fn run_backtest_e2e_with_stop_loss() {
        let df = make_stop_loss_df();
        let mut params = default_backtest_params();
        params.stop_loss = Some(0.50); // 50% stop loss

        let result = run_backtest(&df, &params);
        assert!(
            result.is_ok(),
            "run_backtest with stop loss failed: {:?}",
            result.err()
        );
        let bt = result.unwrap();
        assert_eq!(bt.trade_count, 1);
        // Stop loss fires on day 3 (Jan 17) at real market prices
        assert!(
            matches!(bt.trade_log[0].exit_type, ExitType::StopLoss),
            "Expected StopLoss exit, got {:?}",
            bt.trade_log[0].exit_type
        );
        // Exit on Jan 17 = 2 days held
        assert_eq!(bt.trade_log[0].days_held, 2);
    }

    #[test]
    fn run_backtest_unknown_strategy_errors() {
        let df = make_daily_options_df();
        let mut params = default_backtest_params();
        params.strategy = "nonexistent".to_string();

        let result = run_backtest(&df, &params);
        assert!(result.is_err());
    }

    #[test]
    fn run_backtest_wrong_leg_count_errors() {
        let df = make_daily_options_df();
        let mut params = default_backtest_params();
        params.leg_deltas = vec![]; // long_call needs 1 delta, providing 0

        let result = run_backtest(&df, &params);
        assert!(result.is_err());
    }

    #[test]
    fn run_backtest_signal_without_ohlcv_path_errors() {
        let df = make_daily_options_df();
        let mut params = default_backtest_params();
        params.entry_signal = Some(signals::registry::SignalSpec::Formula {
            formula: "consecutive_up(close) >= 2".into(),
        });
        // ohlcv_path intentionally left None
        let result = run_backtest(&df, &params);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("ohlcv_path is required"),);
    }

    /// Write a minimal OHLCV parquet to a temp file for signal tests.
    /// Returns a `TempDir` that keeps the file alive until dropped.
    fn write_ohlcv_parquet(dates: &[NaiveDate], closes: &[f64]) -> (tempfile::TempDir, String) {
        let n = dates.len();
        let mut df = df! {
            "open" => vec![100.0f64; n],
            "high" => vec![105.0f64; n],
            "low" => vec![95.0f64; n],
            "close" => closes,
            "adjclose" => closes,
            "volume" => vec![1_000_000i64; n],
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("date"), dates.to_vec()).into_column(),
        )
        .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ohlcv.parquet");
        let file = std::fs::File::create(&path).unwrap();
        polars::prelude::ParquetWriter::new(file)
            .finish(&mut df)
            .unwrap();
        let path_str = path.to_string_lossy().to_string();
        (dir, path_str)
    }

    #[test]
    fn run_backtest_entry_signal_filters_candidates() {
        let df = make_daily_options_df();
        // Options dates: Jan 15, 22, 29, Feb 1, 5, 11 — all are entry candidates (DTE > exit_dte=5)
        //
        // OHLCV: closes decline throughout, so ConsecutiveUp(2) never fires.
        // All entry candidates should be blocked → 0 trades.
        let ohlcv_dates: Vec<NaiveDate> = vec![
            NaiveDate::from_ymd_opt(2024, 1, 11).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 29).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 5).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(),
        ];
        // Monotonically decreasing → ConsecutiveUp(2) never fires
        let closes = vec![107.0, 106.0, 105.0, 104.0, 103.0, 102.0, 101.0];
        let (_dir, path) = write_ohlcv_parquet(&ohlcv_dates, &closes);

        let mut params = default_backtest_params();
        params.entry_signal = Some(signals::registry::SignalSpec::Formula {
            formula: "consecutive_up(close) >= 2".into(),
        });
        params.ohlcv_path = Some(path);

        let result = run_backtest(&df, &params).unwrap();
        // All entry dates blocked since close never goes up twice in a row
        assert_eq!(result.trade_count, 0);

        // Verify baseline without signal would have produced a trade
        let mut baseline = default_backtest_params();
        baseline.entry_signal = None;
        let baseline_result = run_backtest(&df, &baseline).unwrap();
        assert!(
            baseline_result.trade_count > 0,
            "Baseline without signal should produce trades"
        );
    }

    #[test]
    fn run_backtest_exit_signal_triggers_early_close() {
        let df = make_daily_options_df();
        // Options dates: Jan 15, 22, 29, Feb 1, 5, 11
        // Without exit signal, trade closes on Feb 11 (DTE=5 exit).
        // With exit signal on Jan 29, trade should close there instead.
        let ohlcv_dates: Vec<NaiveDate> = vec![
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 29).unwrap(), // exit signal fires here
            NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 5).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(),
        ];
        // ConsecutiveUp(2): fires when 2 consecutive up closes
        // Make close go up on Jan 22 and Jan 29 → signal fires on Jan 29
        let closes = vec![100.0, 101.0, 102.0, 99.0, 98.0, 97.0];
        let (_dir, path) = write_ohlcv_parquet(&ohlcv_dates, &closes);

        let mut params = default_backtest_params();
        params.max_positions = 1; // prevent re-entry after signal exit
        params.exit_signal = Some(signals::registry::SignalSpec::Formula {
            formula: "consecutive_up(close) >= 2".into(),
        });
        params.ohlcv_path = Some(path);

        let result = run_backtest(&df, &params).unwrap();
        // First trade: entry Jan 15, signal exit Jan 29 (ConsecutiveUp fires)
        // With max_positions=1, a second trade may open after exit.
        // Verify the first trade was closed by signal.
        assert!(
            result.trade_count >= 1,
            "Expected at least 1 trade, got {}",
            result.trade_count
        );
        assert!(
            matches!(result.trade_log[0].exit_type, ExitType::Signal),
            "Expected Signal exit on first trade, got {:?}",
            result.trade_log[0].exit_type
        );
        // Entry Jan 15, exit Jan 29 = 14 days
        assert_eq!(result.trade_log[0].days_held, 14);
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn run_backtest_spread_strategy() {
        // Build data for a bull call spread: long call at lower strike, short call at higher
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let dates = [
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(),
        ];

        // Two strikes per date: 100 and 105
        let mut quote_dates = Vec::new();
        let mut expirations_vec = Vec::new();
        let mut option_types = Vec::new();
        let mut strikes = Vec::new();
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        let mut deltas = Vec::new();

        // Strike 100 data
        let bids_100 = [5.00, 4.00, 2.00f64];
        let asks_100 = [5.50, 4.50, 2.50f64];
        let deltas_100 = [0.50, 0.42, 0.25f64];

        // Strike 105 data
        let bids_105 = [3.00, 2.20, 1.00f64];
        let asks_105 = [3.50, 2.70, 1.50f64];
        let deltas_105 = [0.35, 0.28, 0.15f64];

        for (i, date) in dates.iter().enumerate() {
            // Strike 100
            quote_dates.push(date.and_hms_opt(0, 0, 0).unwrap());
            expirations_vec.push(exp);
            option_types.push("call");
            strikes.push(100.0f64);
            bids.push(bids_100[i]);
            asks.push(asks_100[i]);
            deltas.push(deltas_100[i]);

            // Strike 105
            quote_dates.push(date.and_hms_opt(0, 0, 0).unwrap());
            expirations_vec.push(exp);
            option_types.push("call");
            strikes.push(105.0f64);
            bids.push(bids_105[i]);
            asks.push(asks_105[i]);
            deltas.push(deltas_105[i]);
        }

        let mut df = df! {
            DATETIME_COL => &quote_dates,
            "option_type" => &option_types,
            "strike" => &strikes,
            "bid" => &bids,
            "ask" => &asks,
            "delta" => &deltas,
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations_vec)
                .into_column(),
        )
        .unwrap();

        let params = BacktestParams {
            strategy: "bull_call_spread".to_string(),
            leg_deltas: vec![
                TargetRange {
                    target: 0.50,
                    min: 0.20,
                    max: 0.80,
                },
                TargetRange {
                    target: 0.35,
                    min: 0.10,
                    max: 0.60,
                },
            ],
            entry_dte: DteRange {
                target: 45,
                min: 10,
                max: 60,
            },
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            sizing: None,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: None,
        };

        let result = run_backtest(&df, &params);
        assert!(
            result.is_ok(),
            "run_backtest for spread failed: {:?}",
            result.err()
        );
        let bt = result.unwrap();
        assert_eq!(bt.trade_count, 1);
        // Both legs should be present in the trade
        assert_eq!(bt.trade_log.len(), 1);
    }

    /// Build an options `DataFrame` that includes `implied_volatility` for IV signal tests.
    fn make_iv_options_df() -> DataFrame {
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let dates: Vec<_> = vec![
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 29).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 5).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(),
        ];

        let quote_dates: Vec<_> = dates
            .iter()
            .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
            .collect();
        let expirations: Vec<_> = dates.iter().map(|_| exp).collect();
        let n = dates.len();

        let mut df = df! {
            DATETIME_COL => &quote_dates,
            "option_type" => vec!["call"; n],
            "strike" => vec![100.0f64; n],
            "bid" => vec![5.00, 4.50, 3.80, 3.20, 2.60, 2.00f64],
            "ask" => vec![5.50, 5.00, 4.30, 3.70, 3.10, 2.50f64],
            "delta" => vec![0.50, 0.47, 0.42, 0.38, 0.33, 0.25f64],
            "implied_volatility" => vec![0.20, 0.25, 0.30, 0.35, 0.40, 0.45f64],
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations).into_column(),
        )
        .unwrap();
        df
    }

    #[test]
    fn run_backtest_custom_signal_without_ohlcv_errors() {
        // Any Custom signal requires ohlcv_path — all signals now use the Custom DSL.
        let df = make_iv_options_df();
        let mut params = default_backtest_params();
        params.entry_signal = Some(signals::registry::SignalSpec::Formula {
            formula: "consecutive_up(close) >= 2".into(),
        });
        params.ohlcv_path = None;

        let result = run_backtest(&df, &params);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("ohlcv_path is required"));
    }

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
