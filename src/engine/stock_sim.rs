//! Stock/equity backtesting event loop.
//!
//! A simpler simulation engine that operates on OHLCV data instead of options chains.
//! Evaluates entry/exit signals on each bar, manages long/short positions with
//! stop-loss, take-profit, max-hold, and exit-signal exits, and builds an equity
//! curve for performance metric calculation.

use anyhow::Result;
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use std::collections::HashSet;

use super::metrics;
use super::types::{
    BacktestResult, Commission, ConflictResolution, EquityPoint, ExitType, Interval, SessionFilter,
    Side, Slippage, TradeRecord,
};
use crate::engine::pricing::fill_price;

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

/// A single OHLCV bar for simulation (daily or intraday).
#[derive(Debug, Clone)]
pub struct Bar {
    pub datetime: NaiveDateTime,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
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

/// Resample OHLCV data to a different interval.
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

///
/// Supports both daily data (`"date"` Date column) and intraday data
/// (`"datetime"` Datetime column). Groups rows by interval boundary and
/// aggregates: open=first, high=max, low=min, close=last, adjclose=last,
/// volume=sum.
///
/// Output column type:
/// - Daily/Weekly/Monthly target → `"date"` (Date) for backward compat
/// - Intraday target (Min5/Min30/Hour1) → `"datetime"` (Datetime)
#[allow(clippy::too_many_lines)]
pub fn resample_ohlcv(
    df: &polars::prelude::DataFrame,
    interval: Interval,
) -> Result<polars::prelude::DataFrame> {
    use polars::prelude::{IntoLazy, SortMultipleOptions};

    let has_datetime_col = df
        .column("datetime")
        .ok()
        .and_then(|c| c.datetime().ok())
        .is_some();

    // --- Passthrough checks ---
    if interval == Interval::Min1 {
        if !has_datetime_col {
            return Err(anyhow::anyhow!(
                "Cannot resample daily data to intraday interval ({interval}). \
                 Provide intraday (datetime) data instead."
            ));
        }
        // Already at minimum granularity — no downsampling needed.
        // Sort by datetime for consistency with other intraday resamples so
        // downstream consumers can assume chronological order.
        return Ok(df
            .clone()
            .lazy()
            .sort(["datetime"], SortMultipleOptions::default())
            .collect()?);
    }
    if !has_datetime_col && interval == Interval::Daily {
        // Daily input, daily target → no-op
        return Ok(df.clone());
    }
    if !has_datetime_col && interval.is_intraday() {
        return Err(anyhow::anyhow!(
            "Cannot resample daily data to intraday interval ({interval}). \
             Provide intraday (datetime) data instead."
        ));
    }

    // --- Datetime-based resampling (intraday source) ---
    if has_datetime_col {
        return resample_datetime(df, interval);
    }

    // --- Legacy date-based resampling (daily source → weekly/monthly) ---
    resample_date(df, interval)
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

/// Legacy resample path for `DataFrame` with `"date"` (Date) column → Weekly/Monthly.
#[allow(clippy::too_many_lines)]
fn resample_date(
    df: &polars::prelude::DataFrame,
    interval: Interval,
) -> Result<polars::prelude::DataFrame> {
    use polars::prelude::*;

    let epoch_offset = super::types::EPOCH_DAYS_CE_OFFSET;

    let dates = df
        .column("date")
        .map_err(|e| anyhow::anyhow!("Missing 'date' column: {e}"))?
        .date()
        .map_err(|e| anyhow::anyhow!("'date' not Date type: {e}"))?;

    let ohlcv = extract_ohlcv_columns(df)?;

    // Build group keys for each row.
    // `iso_week().year()` correctly handles year boundaries (e.g., Dec 30 → ISO week 1 of next year).
    let n = df.height();
    let mut group_keys: Vec<(i32, u32)> = Vec::with_capacity(n);
    for i in 0..n {
        let days = dates.phys.get(i).ok_or_else(|| {
            anyhow::anyhow!("NULL date at row {i}; cannot resample with missing dates")
        })?;
        let date = NaiveDate::from_num_days_from_ce_opt(days + epoch_offset)
            .ok_or_else(|| anyhow::anyhow!("Invalid date value at row {i}"))?;
        let key = match interval {
            Interval::Weekly => (date.iso_week().year(), date.iso_week().week()),
            Interval::Monthly => (date.year(), date.month()),
            // Daily and intraday intervals cannot reach here
            _ => unreachable!(),
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

    let mut out_dates: Vec<i32> = Vec::with_capacity(groups.len());
    let mut out_opens: Vec<f64> = Vec::with_capacity(groups.len());
    let mut out_highs: Vec<f64> = Vec::with_capacity(groups.len());
    let mut out_lows: Vec<f64> = Vec::with_capacity(groups.len());
    let mut out_closes: Vec<f64> = Vec::with_capacity(groups.len());
    let mut out_adjcloses: Vec<f64> = Vec::with_capacity(groups.len());
    let mut out_volumes: Vec<i64> = Vec::with_capacity(groups.len());

    for (gi, g) in groups.iter().enumerate() {
        let last = g.end - 1;
        // Use the last trading day's date for weekly/monthly candles (period-end convention).
        // This differs from resample_datetime which uses the first bar's timestamp (candle
        // open time), because daily→weekly/monthly candles conventionally carry the date of
        // the last bar whose close price they represent.
        out_dates.push(
            dates
                .phys
                .get(last)
                .ok_or_else(|| anyhow::anyhow!("NULL date in group {gi} at row {last}"))?,
        );
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

    let reconstructed_dates: Vec<NaiveDate> = out_dates
        .iter()
        .enumerate()
        .map(|(i, &d)| {
            NaiveDate::from_num_days_from_ce_opt(d + epoch_offset).ok_or_else(|| {
                anyhow::anyhow!("Invalid date value in resampled output at index {i}")
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let date_col =
        DateChunked::from_naive_date(PlSmallStr::from("date"), reconstructed_dates).into_column();

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
            let start_dt = start.and_hms_opt(0, 0, 0).unwrap();
            lazy = lazy.filter(col(date_col_name).gt_eq(lit(start_dt)));
        } else {
            lazy = lazy.filter(col(date_col_name).gt_eq(lit(start)));
        }
    }
    if let Some(end) = end_date {
        if date_col_name == "datetime" {
            // Use next day at midnight with < to include all bars on the end date
            let end_next = end.succ_opt().unwrap_or(end).and_hms_opt(0, 0, 0).unwrap();
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
        let dt_col_ref = df.column("datetime")?;
        for i in 0..df.height() {
            let Ok(datetime) =
                crate::engine::price_table::extract_datetime_from_column(dt_col_ref, i)
            else {
                continue;
            };

            let open = opens.get(i).unwrap_or(0.0);
            let high = highs.get(i).unwrap_or(0.0);
            let low = lows.get(i).unwrap_or(0.0);
            let close = closes.get(i).unwrap_or(0.0);

            if open <= 0.0 || close <= 0.0 {
                continue;
            }

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

    for i in 0..df.height() {
        let Some(days) = dates.phys.get(i) else {
            continue;
        };
        let Some(date) = NaiveDate::from_num_days_from_ce_opt(days + epoch_offset) else {
            continue;
        };
        let datetime = date.and_hms_opt(0, 0, 0).unwrap();

        let open = opens.get(i).unwrap_or(0.0);
        let high = highs.get(i).unwrap_or(0.0);
        let low = lows.get(i).unwrap_or(0.0);
        let close = closes.get(i).unwrap_or(0.0);

        if open <= 0.0 || close <= 0.0 {
            continue;
        }

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

/// Optional set of datetimes on which a signal is active.
type DateTimeFilter = Option<HashSet<NaiveDateTime>>;

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

/// Convert a `HashSet<NaiveDate>` to `HashSet<NaiveDateTime>` (midnight).
fn dates_to_datetimes(dates: HashSet<NaiveDate>) -> HashSet<NaiveDateTime> {
    dates
        .into_iter()
        .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
        .collect()
}

/// Build signal datetime filters for stock backtest from a pre-loaded OHLCV `DataFrame`.
///
/// Accepts the primary OHLCV data directly to avoid re-reading the parquet file.
/// Cross-symbol data is still loaded from `params.cross_ohlcv_paths` on demand.
///
/// For daily data (column `"date"`), signal dates are promoted to midnight datetimes.
/// For intraday data (column `"datetime"`), signal datetimes carry the full timestamp.
pub fn build_stock_signal_filters(
    params: &StockBacktestParams,
    ohlcv_df: &polars::prelude::DataFrame,
) -> Result<(DateTimeFilter, DateTimeFilter)> {
    use crate::signals;

    let has_entry = params.entry_signal.is_some();
    let has_exit = params.exit_signal.is_some();

    if !has_entry && !has_exit {
        return Ok((None, None));
    }

    let date_col = detect_date_col(ohlcv_df);
    let is_intraday = date_col == "datetime";

    // Check for cross-symbol references
    let has_cross = params
        .entry_signal
        .as_ref()
        .is_some_and(|s| !signals::registry::collect_cross_symbols(s).is_empty())
        || params
            .exit_signal
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
            let entry_dates = params
                .entry_signal
                .as_ref()
                .map(|spec| signals::active_datetimes_multi(spec, ohlcv_df, &cross_dfs, date_col))
                .transpose()?;
            let exit_dates = params
                .exit_signal
                .as_ref()
                .map(|spec| signals::active_datetimes_multi(spec, ohlcv_df, &cross_dfs, date_col))
                .transpose()?;
            Ok((entry_dates, exit_dates))
        } else {
            let entry_dates = params
                .entry_signal
                .as_ref()
                .map(|spec| signals::active_dates_multi(spec, ohlcv_df, &cross_dfs, date_col))
                .transpose()?
                .map(dates_to_datetimes);
            let exit_dates = params
                .exit_signal
                .as_ref()
                .map(|spec| signals::active_dates_multi(spec, ohlcv_df, &cross_dfs, date_col))
                .transpose()?
                .map(dates_to_datetimes);
            Ok((entry_dates, exit_dates))
        }
    } else if is_intraday {
        let entry_dates = params
            .entry_signal
            .as_ref()
            .map(|spec| signals::active_datetimes(spec, ohlcv_df, date_col))
            .transpose()?;
        let exit_dates = params
            .exit_signal
            .as_ref()
            .map(|spec| signals::active_datetimes(spec, ohlcv_df, date_col))
            .transpose()?;
        Ok((entry_dates, exit_dates))
    } else {
        let entry_dates = params
            .entry_signal
            .as_ref()
            .map(|spec| signals::active_dates(spec, ohlcv_df, date_col))
            .transpose()?
            .map(dates_to_datetimes);
        let exit_dates = params
            .exit_signal
            .as_ref()
            .map(|spec| signals::active_dates(spec, ohlcv_df, date_col))
            .transpose()?
            .map(dates_to_datetimes);
        Ok((entry_dates, exit_dates))
    }
}

// ---------------------------------------------------------------------------
// Helpers for stock advanced tools (walk-forward, permutation, sweep, compare)
// ---------------------------------------------------------------------------

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
    if bars.is_empty() {
        return None;
    }
    let min = bars.first().unwrap().datetime.date();
    let max = bars.last().unwrap().datetime.date();
    Some((min, max))
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
