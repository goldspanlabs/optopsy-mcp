//! Stock/equity backtesting event loop.
//!
//! A simpler simulation engine that operates on OHLCV data instead of options chains.
//! Evaluates entry/exit signals on each bar, manages long/short positions with
//! stop-loss, take-profit, max-hold, and exit-signal exits, and builds an equity
//! curve for performance metric calculation.

use anyhow::Result;
use chrono::{NaiveDate, NaiveDateTime};
use std::collections::HashSet;

use super::metrics;
use super::types::{
    BacktestResult, Commission, ConflictResolution, EquityPoint, ExitType, Interval, SessionFilter,
    Side, Slippage, TradeRecord,
};
use crate::engine::pricing::fill_price;

// Re-export moved items so that existing `stock_sim::X` paths continue to compile.
pub use super::ohlcv::{
    bar_date_range, bars_from_df, compute_effective_start, detect_date_col, filter_datetime_set,
    filter_session, load_ohlcv_df, ohlcv_path_to_cache_root, parse_ohlcv_bars, resample_ohlcv,
    slice_bars_by_date_range, volume_as_i64, Bar,
};

/// Parameters for a stock backtest.
#[derive(Debug, Clone)]
pub struct StockBacktestParams {
    pub symbol: String,
    pub side: Side,
    pub capital: f64,
    pub quantity: i32,
    /// Dynamic position sizing configuration.
    pub sizing: Option<super::types::SizingConfig>,
    pub max_positions: i32,
    pub slippage: Slippage,
    pub commission: Option<Commission>,
    pub stop_loss: Option<f64>,
    pub take_profit: Option<f64>,
    pub max_hold_days: Option<i32>,
    /// Maximum bars to hold a position before force-closing (intraday alternative to `max_hold_days`).
    pub max_hold_bars: Option<i32>,
    /// Minimum calendar days between consecutive position entries (cooldown / stagger).
    pub min_days_between_entries: Option<i32>,
    /// Minimum bars between consecutive position entries (intraday alternative to `min_days_between_entries`).
    pub min_bars_between_entries: Option<i32>,
    /// How to resolve when both stop-loss and take-profit trigger on the same bar.
    pub conflict_resolution: ConflictResolution,
    pub entry_signal: Option<crate::signals::registry::SignalSpec>,
    pub exit_signal: Option<crate::signals::registry::SignalSpec>,
    pub ohlcv_path: Option<String>,
    pub cross_ohlcv_paths: std::collections::HashMap<String, String>,
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
    /// Bar interval for resampling (default: Daily).
    pub interval: Interval,
    /// Trading session filter for intraday data (e.g., Premarket only).
    pub session_filter: Option<SessionFilter>,
}

/// An open stock position tracked during simulation.
#[derive(Debug, Clone)]
struct StockPosition {
    id: usize,
    entry_datetime: NaiveDateTime,
    entry_bar_idx: usize,
    entry_price: f64,
    quantity: i32,
    side: Side,
    entry_commission: f64,
}

/// Exit decision from `check_exit`: the exit type and an optional fill price
/// override. SL/TP exits fill at the trigger price; other exits use bar close.
struct ExitDecision {
    exit_type: ExitType,
    fill_price: Option<f64>,
}

/// Run a stock backtest simulation on OHLCV data.
///
/// Returns the same `BacktestResult` used by options backtests, ensuring
/// identical output format (metrics, trade log, equity curve).
#[allow(clippy::implicit_hasher, clippy::too_many_lines)]
pub fn run_stock_backtest(
    bars: &[Bar],
    params: &StockBacktestParams,
    entry_dates: Option<&HashSet<NaiveDateTime>>,
    exit_dates: Option<&HashSet<NaiveDateTime>>,
) -> Result<BacktestResult> {
    if bars.is_empty() {
        return Ok(empty_result(params.capital));
    }

    let mut equity = params.capital;
    let mut positions: Vec<StockPosition> = Vec::new();
    let mut trade_log: Vec<TradeRecord> = Vec::new();
    let mut equity_curve: Vec<EquityPoint> = Vec::new();
    let mut next_trade_id: usize = 1;
    let mut skipped_capital: usize = 0;
    let mut first_skip_required: Option<f64> = None;
    let mut last_entry_date: Option<chrono::NaiveDate> = None;
    let mut last_entry_bar_idx: Option<usize> = None;

    let signal_fire_count = entry_dates.as_ref().map_or(0, |dates| dates.len());

    for (bar_idx, bar) in bars.iter().enumerate() {
        // ── 1. Check exits on open positions ────────────────────────────────
        let mut closed_ids = Vec::new();
        for pos in &positions {
            if let Some(decision) = check_exit(pos, bar, bar_idx, params, exit_dates) {
                closed_ids.push((pos.id, decision));
            }
        }

        // Process closes
        for (id, decision) in closed_ids {
            if let Some(idx) = positions.iter().position(|p| p.id == id) {
                let pos = positions.remove(idx);
                let equity_before_close = equity;
                let (pnl, mut record) =
                    close_position(&pos, bar, decision.exit_type, decision.fill_price, params);
                if params.sizing.is_some() {
                    record.computed_quantity = Some(pos.quantity);
                    record.entry_equity = Some(equity_before_close);
                }
                equity += pnl;
                trade_log.push(record);
            }
        }

        // ── 2. Check entry ──────────────────────────────────────────────────
        let can_enter = (positions.len() as i32) < params.max_positions;
        let signal_fires = entry_dates
            .as_ref()
            .is_some_and(|dates| dates.contains(&bar.datetime));

        // Fix 3: prevent entering on a bar where exit signal also fires (avoids 0-duration trades)
        let exit_fires = exit_dates
            .as_ref()
            .is_some_and(|dates| dates.contains(&bar.datetime));

        // Stagger check: for intraday, prefer bar-count cooldown; fall back to calendar days
        let stagger_ok = if params.interval.is_intraday() {
            match (params.min_bars_between_entries, last_entry_bar_idx) {
                (Some(min_bars), Some(last_idx)) => {
                    bar_idx.saturating_sub(last_idx) >= min_bars as usize
                }
                (Some(_), None) => true,
                _ => match (params.min_days_between_entries, last_entry_date) {
                    (Some(min_days), Some(last)) => {
                        (bar.datetime.date() - last).num_days() >= i64::from(min_days)
                    }
                    _ => true,
                },
            }
        } else {
            match (params.min_days_between_entries, last_entry_date) {
                (Some(min_days), Some(last)) => {
                    (bar.datetime.date() - last).num_days() >= i64::from(min_days)
                }
                _ => true,
            }
        };

        if can_enter && signal_fires && stagger_ok && !exit_fires {
            let entry_price =
                compute_entry_price(bar, params.side, &params.slippage, params.interval);

            // Dynamic position sizing
            let effective_qty = params.sizing.as_ref().map_or(params.quantity, |cfg| {
                let ml = super::sizing::max_loss_per_share(entry_price, params.stop_loss);
                if ml <= 0.0 {
                    return params.quantity;
                }
                let vol = super::sizing::vol_lookback(cfg).and_then(|lookback| {
                    let closes: Vec<f64> = bars[..=bar_idx].iter().map(|b| b.close).collect();
                    super::sizing::compute_realized_vol(
                        &closes,
                        lookback,
                        params.interval.bars_per_year(),
                    )
                });
                super::sizing::compute_quantity(
                    cfg,
                    equity,
                    ml,
                    &trade_log,
                    vol,
                    1, // multiplier=1 for stocks
                    params.quantity,
                )
            });

            let position_value = entry_price * f64::from(effective_qty);
            let commission_cost = params
                .commission
                .as_ref()
                .map_or(0.0, |c| c.calculate(effective_qty));

            // Capital/margin check: longs need purchase cost, shorts need full collateral
            let required_capital = position_value + commission_cost;
            if required_capital > equity {
                skipped_capital += 1;
                if first_skip_required.is_none() {
                    first_skip_required = Some(required_capital);
                }
            } else {
                let pos = StockPosition {
                    id: next_trade_id,
                    entry_datetime: bar.datetime,
                    entry_bar_idx: bar_idx,
                    entry_price,
                    quantity: effective_qty,
                    side: params.side,
                    entry_commission: commission_cost,
                };
                next_trade_id += 1;
                last_entry_date = Some(bar.datetime.date());
                last_entry_bar_idx = Some(bar_idx);
                positions.push(pos);
            }
        }

        // ── 3. Mark to market ───────────────────────────────────────────────
        let unrealized: f64 = positions
            .iter()
            .map(|pos| {
                let price_change = bar.close - pos.entry_price;
                price_change * pos.side.multiplier() * f64::from(pos.quantity)
            })
            .sum();

        equity_curve.push(EquityPoint {
            datetime: bar.datetime,
            equity: equity + unrealized,
        });
    }

    // ── 4. Force-close remaining positions at last bar ──────────────────
    if let Some(last_bar) = bars.last() {
        for pos in &positions {
            // Force-close P&L is already reflected in equity curve via mark-to-market
            let (_pnl, mut record) = close_position(pos, last_bar, ExitType::MaxHold, None, params);
            if params.sizing.is_some() {
                record.computed_quantity = Some(pos.quantity);
                record.entry_equity = Some(equity);
            }
            trade_log.push(record);
        }
    }

    let total_pnl: f64 = trade_log.iter().map(|t| t.pnl).sum();
    let trade_count = trade_log.len();

    let perf_metrics = metrics::calculate_metrics(
        &equity_curve,
        &trade_log,
        params.capital,
        params.interval.bars_per_year(),
    )
    .unwrap_or(metrics::DEFAULT_METRICS);

    // Build warnings for diagnostic feedback
    let mut warnings = Vec::new();
    if skipped_capital > 0 {
        let needed = first_skip_required.unwrap_or(0.0);
        warnings.push(format!(
            "INSUFFICIENT_CAPITAL: {skipped_capital} of {signal_fire_count} entry signals were \
             skipped because the position cost (${needed:.0} for {qty} shares) exceeds \
             available equity (${capital:.0}). Increase `capital` to at least ${needed:.0} \
             or reduce `quantity`.",
            qty = params.quantity,
            capital = params.capital,
        ));
    }

    Ok(BacktestResult {
        trade_count,
        total_pnl,
        metrics: perf_metrics,
        equity_curve,
        trade_log,
        quality: crate::engine::types::BacktestQualityStats::default(),
        warnings,
    })
}

/// Compute the trigger price for a stop-loss or take-profit exit.
///
/// For stop-loss (`is_stop = true`): Long subtracts, Short adds.
/// For take-profit (`is_stop = false`): Long adds, Short subtracts.
fn compute_trigger_price(entry_price: f64, pct: f64, side: Side, is_stop: bool) -> f64 {
    match (side, is_stop) {
        (Side::Long, true) | (Side::Short, false) => entry_price * (1.0 - pct),
        (Side::Long, false) | (Side::Short, true) => entry_price * (1.0 + pct),
    }
}

/// Check whether a bar has hit the given trigger price.
///
/// Stop-loss triggers when: Long bar.low <= trigger, Short bar.high >= trigger.
/// Take-profit triggers when: Long bar.high >= trigger, Short bar.low <= trigger.
fn check_trigger(bar: &Bar, trigger_price: f64, side: Side, is_stop: bool) -> bool {
    match (side, is_stop) {
        (Side::Long, true) | (Side::Short, false) => bar.low <= trigger_price,
        (Side::Long, false) | (Side::Short, true) => bar.high >= trigger_price,
    }
}

/// Compute the fill price for a triggered SL/TP, accounting for gap-through.
///
/// If the bar's open has already blown past the trigger level, fill at the open
/// (the trigger price was never available). Otherwise fill at the trigger price.
fn gap_fill_price(bar: &Bar, trigger_price: f64, side: Side, is_stop: bool) -> f64 {
    let gapped_through = match (side, is_stop) {
        (Side::Long, true) | (Side::Short, false) => bar.open <= trigger_price,
        (Side::Long, false) | (Side::Short, true) => bar.open >= trigger_price,
    };
    if gapped_through {
        bar.open
    } else {
        trigger_price
    }
}

/// Check if a position should be exited on this bar.
///
/// Returns an `ExitDecision` with the exit type and an optional fill price
/// override for SL/TP exits. Gap-through fills use the bar's open when the
/// open has already blown past the trigger level (the trigger price was never
/// available as a fill).
fn check_exit(
    pos: &StockPosition,
    bar: &Bar,
    bar_idx: usize,
    params: &StockBacktestParams,
    exit_dates: Option<&HashSet<NaiveDateTime>>,
) -> Option<ExitDecision> {
    // ── Evaluate SL and TP triggers independently ───────────────────────
    let sl_decision = params.stop_loss.and_then(|sl_pct| {
        let sl_price = compute_trigger_price(pos.entry_price, sl_pct, pos.side, true);
        if !check_trigger(bar, sl_price, pos.side, true) {
            return None;
        }
        let fill = gap_fill_price(bar, sl_price, pos.side, true);
        Some(ExitDecision {
            exit_type: ExitType::StopLoss,
            fill_price: Some(fill),
        })
    });

    let tp_decision = params.take_profit.and_then(|tp_pct| {
        let tp_price = compute_trigger_price(pos.entry_price, tp_pct, pos.side, false);
        if !check_trigger(bar, tp_price, pos.side, false) {
            return None;
        }
        let fill = gap_fill_price(bar, tp_price, pos.side, false);
        Some(ExitDecision {
            exit_type: ExitType::TakeProfit,
            fill_price: Some(fill),
        })
    });

    // ── Resolve SL/TP conflict when both trigger on the same bar ────────
    match (sl_decision, tp_decision) {
        (Some(sl), Some(tp)) => {
            let winner = match params.conflict_resolution {
                ConflictResolution::StopLossFirst => sl,
                ConflictResolution::TakeProfitFirst => tp,
                ConflictResolution::Nearest => {
                    let sl_dist = (sl.fill_price.unwrap_or(bar.close) - bar.open).abs();
                    let tp_dist = (tp.fill_price.unwrap_or(bar.close) - bar.open).abs();
                    if sl_dist <= tp_dist {
                        sl
                    } else {
                        tp
                    }
                }
            };
            return Some(winner);
        }
        (Some(sl), None) => return Some(sl),
        (None, Some(tp)) => return Some(tp),
        (None, None) => {}
    }

    // ── Max hold: bar-count for intraday, calendar days for daily+ ──────
    if params.interval.is_intraday() {
        if let Some(max_bars) = params.max_hold_bars {
            let bars_held = bar_idx as i64 - pos.entry_bar_idx as i64;
            if bars_held >= i64::from(max_bars) {
                return Some(ExitDecision {
                    exit_type: ExitType::MaxHold,
                    fill_price: None,
                });
            }
        }
    } else if let Some(max_days) = params.max_hold_days {
        let days_held = (bar.datetime.date() - pos.entry_datetime.date()).num_days();
        if days_held >= i64::from(max_days) {
            return Some(ExitDecision {
                exit_type: ExitType::MaxHold,
                fill_price: None,
            });
        }
    }

    // ── Exit signal ─────────────────────────────────────────────────────
    if let Some(dates) = exit_dates {
        if dates.contains(&bar.datetime) {
            return Some(ExitDecision {
                exit_type: ExitType::Signal,
                fill_price: None,
            });
        }
    }

    None
}

/// Compute entry fill price from a bar's open, applying slippage.
///
/// For stocks, we treat the open as midpoint and apply a small synthetic spread
/// based on the bar's high-low range. The spread fraction scales by interval —
/// wider bars (daily) use 10%, tighter intraday bars use smaller fractions.
/// For `Slippage::Mid`, we just use the open.
fn compute_entry_price(bar: &Bar, side: Side, slippage: &Slippage, interval: Interval) -> f64 {
    if matches!(slippage, Slippage::Mid) {
        return bar.open;
    }
    let range = bar.high - bar.low;
    let synthetic_spread = range * interval.spread_fraction();
    let bid = bar.open - synthetic_spread / 2.0;
    let ask = bar.open + synthetic_spread / 2.0;
    fill_price(bid.max(0.01), ask.max(0.01), side, slippage)
}

/// Compute exit fill price from a bar's close, applying slippage.
fn compute_exit_price(bar: &Bar, side: Side, slippage: &Slippage, interval: Interval) -> f64 {
    let exit_side = match side {
        Side::Long => Side::Short,
        Side::Short => Side::Long,
    };
    if matches!(slippage, Slippage::Mid) {
        return bar.close;
    }
    let range = bar.high - bar.low;
    let synthetic_spread = range * interval.spread_fraction();
    let bid = bar.close - synthetic_spread / 2.0;
    let ask = bar.close + synthetic_spread / 2.0;
    fill_price(bid.max(0.01), ask.max(0.01), exit_side, slippage)
}

/// Close a position and produce a `TradeRecord`.
///
/// When `trigger_price` is `Some`, the position exits at that exact price
/// (used for SL/TP fills). Otherwise, exits at bar close with slippage.
fn close_position(
    pos: &StockPosition,
    bar: &Bar,
    exit_type: ExitType,
    trigger_price: Option<f64>,
    params: &StockBacktestParams,
) -> (f64, TradeRecord) {
    let exit_price = trigger_price
        .unwrap_or_else(|| compute_exit_price(bar, pos.side, &params.slippage, params.interval));
    let direction = pos.side.multiplier();
    let qty = f64::from(pos.quantity);

    let pnl_before_commission = (exit_price - pos.entry_price) * direction * qty;
    let exit_commission = params
        .commission
        .as_ref()
        .map_or(0.0, |c| c.calculate(pos.quantity));
    let pnl = pnl_before_commission - pos.entry_commission - exit_commission;

    let days_held = (bar.datetime.date() - pos.entry_datetime.date()).num_days();

    let entry_cost = pos.entry_price * qty * direction;
    let exit_proceeds = exit_price * qty * direction;

    let record = TradeRecord::new(
        pos.id,
        pos.entry_datetime,
        bar.datetime,
        entry_cost,
        exit_proceeds,
        pnl,
        days_held,
        exit_type,
        vec![], // no legs for stock trades
    );

    (pnl, record)
}

fn empty_result(_capital: f64) -> BacktestResult {
    BacktestResult {
        trade_count: 0,
        total_pnl: 0.0,
        metrics: metrics::DEFAULT_METRICS,
        equity_curve: vec![],
        trade_log: vec![],
        quality: crate::engine::types::BacktestQualityStats::default(),
        warnings: vec![],
    }
}

/// Optional set of datetimes on which a signal is active.
type DateTimeFilter = Option<HashSet<NaiveDateTime>>;

/// Convert a `HashSet<NaiveDate>` to `HashSet<NaiveDateTime>` (midnight).
fn dates_to_datetimes(dates: HashSet<NaiveDate>) -> HashSet<NaiveDateTime> {
    dates
        .into_iter()
        .map(|d| {
            d.and_hms_opt(0, 0, 0)
                .expect("midnight datetime for signal date conversion")
        })
        .collect()
}

/// Build signal datetime filters for stock backtest from a pre-loaded OHLCV `DataFrame`.
///
/// Accepts the primary OHLCV data directly to avoid re-reading the parquet file.
/// Cross-symbol data is still loaded from `params.cross_ohlcv_paths` on demand.
///
/// For daily data (column `"date"`), signal dates are promoted to midnight datetimes.
/// For intraday data (column `"datetime"`), signal datetimes carry the full timestamp.
#[allow(clippy::too_many_lines)]
pub fn build_stock_signal_filters(
    params: &StockBacktestParams,
    ohlcv_df: &polars::prelude::DataFrame,
    cache_dir: Option<&std::path::Path>,
) -> Result<(DateTimeFilter, DateTimeFilter)> {
    use crate::signals;
    use crate::signals::registry::SignalSpec;

    if params.entry_signal.is_none() && params.exit_signal.is_none() {
        return Ok((None, None));
    }

    let date_col = detect_date_col(ohlcv_df);
    let is_intraday = date_col == "datetime";
    let params_ohlcv = params.ohlcv_path.as_deref();
    let effective_cache_dir = cache_dir.or_else(|| params_ohlcv.and_then(ohlcv_path_to_cache_root));

    // --- HMM regime preprocessing ---
    let primary_symbol = &params.symbol;
    let mut entry_signal = params.entry_signal.clone();
    let mut exit_signal = params.exit_signal.clone();
    let mut hmm_df: Option<polars::prelude::DataFrame> = None;

    if let Some(SignalSpec::Formula { ref formula }) = entry_signal {
        if formula.contains("hmm_regime(") {
            let (rewritten, updated_df) = signals::preprocess_hmm_regime(
                formula,
                primary_symbol,
                hmm_df.as_ref().unwrap_or(ohlcv_df),
                effective_cache_dir,
                date_col,
                None,
            )?;
            entry_signal = Some(SignalSpec::Formula { formula: rewritten });
            hmm_df = Some(updated_df);
        }
    }
    if let Some(SignalSpec::Formula { ref formula }) = exit_signal {
        if formula.contains("hmm_regime(") {
            let (rewritten, updated_df) = signals::preprocess_hmm_regime(
                formula,
                primary_symbol,
                hmm_df.as_ref().unwrap_or(ohlcv_df),
                effective_cache_dir,
                date_col,
                None,
            )?;
            exit_signal = Some(SignalSpec::Formula { formula: rewritten });
            hmm_df = Some(updated_df);
        }
    }
    let ohlcv_df = hmm_df.as_ref().unwrap_or(ohlcv_df);

    // Check for cross-symbol references
    let has_cross = entry_signal
        .as_ref()
        .is_some_and(|s| !signals::registry::collect_cross_symbols(s).is_empty())
        || exit_signal
            .as_ref()
            .is_some_and(|s| !signals::registry::collect_cross_symbols(s).is_empty());

    if has_cross {
        let mut cross_dfs = std::collections::HashMap::new();
        for (sym, path) in &params.cross_ohlcv_paths {
            let args = polars::prelude::ScanArgsParquet::default();
            let df =
                polars::prelude::LazyFrame::scan_parquet(path.as_str().into(), args)?.collect()?;
            cross_dfs.insert(sym.to_uppercase(), df);
        }

        if is_intraday {
            let entry_dates = entry_signal
                .as_ref()
                .map(|spec| signals::active_datetimes_multi(spec, ohlcv_df, &cross_dfs, date_col))
                .transpose()?;
            let exit_dates = exit_signal
                .as_ref()
                .map(|spec| signals::active_datetimes_multi(spec, ohlcv_df, &cross_dfs, date_col))
                .transpose()?;
            Ok((entry_dates, exit_dates))
        } else {
            let entry_dates = entry_signal
                .as_ref()
                .map(|spec| signals::active_dates_multi(spec, ohlcv_df, &cross_dfs, date_col))
                .transpose()?
                .map(dates_to_datetimes);
            let exit_dates = exit_signal
                .as_ref()
                .map(|spec| signals::active_dates_multi(spec, ohlcv_df, &cross_dfs, date_col))
                .transpose()?
                .map(dates_to_datetimes);
            Ok((entry_dates, exit_dates))
        }
    } else if is_intraday {
        let entry_dates = entry_signal
            .as_ref()
            .map(|spec| signals::active_datetimes(spec, ohlcv_df, date_col))
            .transpose()?;
        let exit_dates = exit_signal
            .as_ref()
            .map(|spec| signals::active_datetimes(spec, ohlcv_df, date_col))
            .transpose()?;
        Ok((entry_dates, exit_dates))
    } else {
        let entry_dates = entry_signal
            .as_ref()
            .map(|spec| signals::active_dates(spec, ohlcv_df, date_col))
            .transpose()?
            .map(dates_to_datetimes);
        let exit_dates = exit_signal
            .as_ref()
            .map(|spec| signals::active_dates(spec, ohlcv_df, date_col))
            .transpose()?
            .map(dates_to_datetimes);
        Ok((entry_dates, exit_dates))
    }
}

/// Load OHLCV data, apply session filter, resample, and extract bars.
/// Returns `(bars, ohlcv_df)` — the `DataFrame` is needed for signal evaluation.
///
/// When `start_date` is `None` and the interval is intraday, a default lookback
/// cap is applied to avoid loading 10+ years of minute/hourly data. The cap
/// varies by interval (see [`Interval::default_intraday_lookback_days`]).
pub fn prepare_stock_data(
    ohlcv_path: &str,
    interval: Interval,
    session_filter: Option<&SessionFilter>,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
) -> Result<(Vec<Bar>, polars::prelude::DataFrame)> {
    let effective_start = compute_effective_start(interval, start_date, end_date);
    let df = load_ohlcv_df(ohlcv_path, effective_start, end_date)?;
    let df = filter_session(&df, session_filter)?;
    let df = resample_ohlcv(&df, interval)?;
    let bars = bars_from_df(&df)?;
    Ok((bars, df))
}
