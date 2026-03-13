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
    BacktestResult, Commission, EquityPoint, ExitType, Interval, SessionFilter, Side, Slippage,
    TradeRecord,
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

    let signal_fire_count = entry_dates.as_ref().map_or(0, |dates| dates.len());

    for bar in bars {
        // ── 1. Check exits on open positions ────────────────────────────────
        let mut closed_ids = Vec::new();
        for pos in &positions {
            if let Some(decision) = check_exit(pos, bar, params, exit_dates) {
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

        if can_enter && signal_fires {
            let entry_price = compute_entry_price(bar, params.side, &params.slippage);

            // Dynamic position sizing
            let effective_qty = params.sizing.as_ref().map_or(params.quantity, |cfg| {
                let ml = super::sizing::max_loss_per_share(entry_price, params.stop_loss);
                if ml <= 0.0 {
                    return params.quantity;
                }
                let vol = super::sizing::vol_lookback(cfg).and_then(|lookback| {
                    let idx = bars
                        .iter()
                        .position(|b| b.datetime == bar.datetime)
                        .unwrap_or(0);
                    let closes: Vec<f64> = bars[..=idx].iter().map(|b| b.close).collect();
                    super::sizing::compute_realized_vol(&closes, lookback, params.interval.bars_per_year())
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
                    entry_price,
                    quantity: effective_qty,
                    side: params.side,
                    entry_commission: commission_cost,
                };
                next_trade_id += 1;
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

    let perf_metrics =
        metrics::calculate_metrics(&equity_curve, &trade_log, params.capital, params.interval.bars_per_year())
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

/// Check if a position should be exited on this bar.
///
/// Returns an `ExitDecision` with the exit type and an optional fill price
/// override for SL/TP exits (which fill at the trigger level, not bar close).
fn check_exit(
    pos: &StockPosition,
    bar: &Bar,
    params: &StockBacktestParams,
    exit_dates: Option<&HashSet<NaiveDateTime>>,
) -> Option<ExitDecision> {
    // Stop loss: check if intraday low (for longs) or high (for shorts) hit stop
    if let Some(sl_pct) = params.stop_loss {
        let sl_price = match pos.side {
            Side::Long => pos.entry_price * (1.0 - sl_pct),
            Side::Short => pos.entry_price * (1.0 + sl_pct),
        };
        let triggered = match pos.side {
            Side::Long => bar.low <= sl_price,
            Side::Short => bar.high >= sl_price,
        };
        if triggered {
            return Some(ExitDecision {
                exit_type: ExitType::StopLoss,
                fill_price: Some(sl_price),
            });
        }
    }

    // Take profit: check if intraday high (for longs) or low (for shorts) hit target
    if let Some(tp_pct) = params.take_profit {
        let tp_price = match pos.side {
            Side::Long => pos.entry_price * (1.0 + tp_pct),
            Side::Short => pos.entry_price * (1.0 - tp_pct),
        };
        let triggered = match pos.side {
            Side::Long => bar.high >= tp_price,
            Side::Short => bar.low <= tp_price,
        };
        if triggered {
            return Some(ExitDecision {
                exit_type: ExitType::TakeProfit,
                fill_price: Some(tp_price),
            });
        }
    }

    // Max hold days
    if let Some(max_days) = params.max_hold_days {
        let days_held = (bar.datetime - pos.entry_datetime).num_days();
        if days_held >= i64::from(max_days) {
            return Some(ExitDecision {
                exit_type: ExitType::MaxHold,
                fill_price: None,
            });
        }
    }

    // Exit signal
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
/// based on the bar's high-low range. For `Slippage::Mid`, we just use the open.
fn compute_entry_price(bar: &Bar, side: Side, slippage: &Slippage) -> f64 {
    if matches!(slippage, Slippage::Mid) {
        return bar.open;
    }
    // Synthetic spread: use a fraction of the bar's range as bid-ask spread
    let range = bar.high - bar.low;
    let synthetic_spread = range * 0.1; // 10% of daily range as spread
    let bid = bar.open - synthetic_spread / 2.0;
    let ask = bar.open + synthetic_spread / 2.0;
    fill_price(bid.max(0.01), ask.max(0.01), side, slippage)
}

/// Compute exit fill price from a bar's close, applying slippage.
fn compute_exit_price(bar: &Bar, side: Side, slippage: &Slippage) -> f64 {
    let exit_side = match side {
        Side::Long => Side::Short,
        Side::Short => Side::Long,
    };
    if matches!(slippage, Slippage::Mid) {
        return bar.close;
    }
    let range = bar.high - bar.low;
    let synthetic_spread = range * 0.1;
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
    let exit_price =
        trigger_price.unwrap_or_else(|| compute_exit_price(bar, pos.side, &params.slippage));
    let direction = pos.side.multiplier();
    let qty = f64::from(pos.quantity);

    let pnl_before_commission = (exit_price - pos.entry_price) * direction * qty;
    let exit_commission = params
        .commission
        .as_ref()
        .map_or(0.0, |c| c.calculate(pos.quantity));
    let pnl = pnl_before_commission - pos.entry_commission - exit_commission;

    let days_held = (bar.datetime - pos.entry_datetime).num_days();

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

/// Resample OHLCV data to a different interval.
///
/// Supports both daily data (`"date"` Date column) and intraday data
/// (`"datetime"` Datetime column). Groups rows by interval boundary and
/// aggregates: open=first, high=max, low=min, close=last, adjclose=last,
/// volume=sum.
///
/// Output column type:
/// - Daily/Weekly/Monthly target → `"date"` (Date) for backward compat
/// - Intraday target (Min5/Min30/Hour1) → `"datetime"` (Datetime)
#[allow(clippy::too_many_lines, clippy::items_after_statements)]
pub fn resample_ohlcv(
    df: &polars::prelude::DataFrame,
    interval: Interval,
) -> Result<polars::prelude::DataFrame> {
    let has_datetime_col = df
        .column("datetime")
        .ok()
        .and_then(|c| c.datetime().ok())
        .is_some();

    // --- Passthrough checks ---
    if interval == Interval::Min1 {
        // Min1 is the smallest supported interval; no resampling possible
        return Ok(df.clone());
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

/// Resample a `DataFrame` with `"datetime"` (Datetime) column to any target interval.
#[allow(clippy::too_many_lines, clippy::items_after_statements)]
fn resample_datetime(
    df: &polars::prelude::DataFrame,
    interval: Interval,
) -> Result<polars::prelude::DataFrame> {
    use polars::prelude::*;

    let dt_col = df
        .column("datetime")
        .map_err(|e| anyhow::anyhow!("Missing 'datetime' column: {e}"))?
        .datetime()
        .map_err(|e| anyhow::anyhow!("'datetime' not Datetime type: {e}"))?;

    let time_unit = dt_col.time_unit();
    let micros_per_sec: i64 = match time_unit {
        TimeUnit::Microseconds => 1_000_000,
        TimeUnit::Milliseconds => 1_000,
        TimeUnit::Nanoseconds => 1_000_000_000,
    };

    let opens = df.column("open")?.f64()?;
    let highs = df.column("high")?.f64()?;
    let lows = df.column("low")?.f64()?;
    let closes = df.column("close")?.f64()?;
    let volumes = df.column("volume")?.i64()?;
    let has_adjclose = df.column("adjclose").is_ok();
    let adjcloses = if has_adjclose {
        Some(df.column("adjclose")?.f64()?)
    } else {
        None
    };

    // Extract NaiveDateTimes for grouping
    let n = df.height();
    let mut datetimes: Vec<NaiveDateTime> = Vec::with_capacity(n);
    for i in 0..n {
        let raw = dt_col.phys.get(i).ok_or_else(|| {
            anyhow::anyhow!("NULL datetime at row {i}; cannot resample with missing datetimes")
        })?;
        let secs = raw.div_euclid(micros_per_sec);
        let subsec = raw.rem_euclid(micros_per_sec);
        let nsecs = match time_unit {
            TimeUnit::Microseconds => (subsec * 1_000) as u32,
            TimeUnit::Milliseconds => (subsec * 1_000_000) as u32,
            TimeUnit::Nanoseconds => subsec as u32,
        };
        let chrono_dt = chrono::DateTime::from_timestamp(secs, nsecs)
            .ok_or_else(|| anyhow::anyhow!("Invalid timestamp at row {i}"))?;
        datetimes.push(chrono_dt.naive_utc());
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
                (
                    dt.date().num_days_from_ce(),
                    dt.time().hour(),
                    trunc_min,
                )
            }
            Interval::Min30 => {
                let trunc_min = (dt.time().minute() / 30) * 30;
                (
                    dt.date().num_days_from_ce(),
                    dt.time().hour(),
                    trunc_min,
                )
            }
            Interval::Hour1 => (dt.date().num_days_from_ce(), dt.time().hour(), 0),
            // Daily+: group by date/week/month
            Interval::Daily => (dt.date().num_days_from_ce(), 0, 0),
            Interval::Weekly => (dt.date().iso_week().year(), dt.date().iso_week().week(), 0),
            Interval::Monthly => (dt.date().year(), dt.date().month(), 0),
        };
        group_keys.push(key);
    }

    // Group consecutive rows by key
    struct Group {
        start: usize,
        end: usize, // exclusive
    }
    let mut groups: Vec<Group> = Vec::new();
    let mut i = 0;
    while i < n {
        let key = group_keys[i];
        let start = i;
        while i < n && group_keys[i] == key {
            i += 1;
        }
        groups.push(Group { start, end: i });
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
        out_datetimes.push(datetimes[last]);
        out_opens.push(
            opens
                .get(g.start)
                .ok_or_else(|| anyhow::anyhow!("NULL open in group {gi} at row {}", g.start))?,
        );

        let mut max_high = f64::NEG_INFINITY;
        let mut min_low = f64::INFINITY;
        let mut vol_sum: i64 = 0;
        for j in g.start..g.end {
            let h = highs.get(j).unwrap_or(f64::NAN);
            let l = lows.get(j).unwrap_or(f64::NAN);
            if h > max_high {
                max_high = h;
            }
            if l < min_low {
                min_low = l;
            }
            vol_sum += volumes.get(j).unwrap_or(0);
        }
        out_highs.push(max_high);
        out_lows.push(min_low);
        out_closes.push(
            closes
                .get(last)
                .ok_or_else(|| anyhow::anyhow!("NULL close in group {gi} at row {last}"))?,
        );
        out_adjcloses.push(
            adjcloses
                .as_ref()
                .and_then(|ac| ac.get(last))
                .unwrap_or(closes.get(last).ok_or_else(|| {
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
        if has_adjclose {
            columns.push(Series::new("adjclose".into(), &out_adjcloses).into());
        }
        columns.push(Series::new("volume".into(), &out_volumes).into());

        let result = DataFrame::new(groups.len(), columns)
            .map_err(|e| anyhow::anyhow!("DataFrame: {e}"))?;
        Ok(result)
    } else {
        // Output "date" (Date) column for Daily/Weekly/Monthly targets
        let dates: Vec<NaiveDate> = out_datetimes.iter().map(NaiveDateTime::date).collect();
        let date_col =
            DateChunked::from_naive_date(PlSmallStr::from("date"), dates).into_column();

        let mut columns = vec![
            date_col,
            Series::new("open".into(), &out_opens).into(),
            Series::new("high".into(), &out_highs).into(),
            Series::new("low".into(), &out_lows).into(),
            Series::new("close".into(), &out_closes).into(),
        ];
        if has_adjclose {
            columns.push(Series::new("adjclose".into(), &out_adjcloses).into());
        }
        columns.push(Series::new("volume".into(), &out_volumes).into());

        let result = DataFrame::new(groups.len(), columns)
            .map_err(|e| anyhow::anyhow!("DataFrame: {e}"))?;
        Ok(result)
    }
}

/// Legacy resample path for `DataFrame` with `"date"` (Date) column → Weekly/Monthly.
#[allow(clippy::too_many_lines, clippy::items_after_statements)]
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

    let opens = df.column("open")?.f64()?;
    let highs = df.column("high")?.f64()?;
    let lows = df.column("low")?.f64()?;
    let closes = df.column("close")?.f64()?;
    let volumes = df.column("volume")?.i64()?;

    let has_adjclose = df.column("adjclose").is_ok();
    let adjcloses = if has_adjclose {
        Some(df.column("adjclose")?.f64()?)
    } else {
        None
    };

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
    struct Group {
        start: usize,
        end: usize, // exclusive
    }
    let mut groups: Vec<Group> = Vec::new();
    let mut i = 0;
    while i < n {
        let key = group_keys[i];
        let start = i;
        while i < n && group_keys[i] == key {
            i += 1;
        }
        groups.push(Group { start, end: i });
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
        out_dates.push(
            dates
                .phys
                .get(last)
                .ok_or_else(|| anyhow::anyhow!("NULL date in group {gi} at row {last}"))?,
        );
        out_opens.push(
            opens
                .get(g.start)
                .ok_or_else(|| anyhow::anyhow!("NULL open in group {gi} at row {}", g.start))?,
        );

        let mut max_high = f64::NEG_INFINITY;
        let mut min_low = f64::INFINITY;
        let mut vol_sum: i64 = 0;
        for j in g.start..g.end {
            let h = highs.get(j).unwrap_or(f64::NAN);
            let l = lows.get(j).unwrap_or(f64::NAN);
            if h > max_high {
                max_high = h;
            }
            if l < min_low {
                min_low = l;
            }
            vol_sum += volumes.get(j).unwrap_or(0);
        }
        out_highs.push(max_high);
        out_lows.push(min_low);
        out_closes.push(
            closes
                .get(last)
                .ok_or_else(|| anyhow::anyhow!("NULL close in group {gi} at row {last}"))?,
        );
        out_adjcloses.push(
            adjcloses
                .as_ref()
                .and_then(|ac| ac.get(last))
                .unwrap_or(closes.get(last).ok_or_else(|| {
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
    if has_adjclose {
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

    // Inspect schema to determine whether this file uses "datetime" or "date"
    let schema = lazy_base.collect_schema()?;
    let date_col_name = if schema.contains("datetime") {
        "datetime"
    } else {
        "date"
    };

    let mut lazy = lazy_base
        .filter(col("open").gt(lit(0.0)).and(col("close").gt(lit(0.0))));

    if let Some(start) = start_date {
        lazy = lazy.filter(col(date_col_name).gt_eq(lit(start)));
    }
    if let Some(end) = end_date {
        lazy = lazy.filter(col(date_col_name).lt_eq(lit(end)));
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

    // Intraday path: "datetime" column (Datetime type with microsecond timestamps)
    if let Ok(dt_col) = df.column("datetime") {
        if let Ok(dt_ca) = dt_col.datetime() {
            let micros_per_sec: i64 = match dt_ca.time_unit() {
                polars::prelude::TimeUnit::Microseconds => 1_000_000,
                polars::prelude::TimeUnit::Milliseconds => 1_000,
                polars::prelude::TimeUnit::Nanoseconds => 1_000_000_000,
            };
            for i in 0..df.height() {
                let Some(raw) = dt_ca.phys.get(i) else {
                    continue;
                };
                let secs = raw.div_euclid(micros_per_sec);
                let subsec = raw.rem_euclid(micros_per_sec);
                let nsecs = match dt_ca.time_unit() {
                    polars::prelude::TimeUnit::Microseconds => (subsec * 1_000) as u32,
                    polars::prelude::TimeUnit::Milliseconds => (subsec * 1_000_000) as u32,
                    polars::prelude::TimeUnit::Nanoseconds => subsec as u32,
                };
                let Some(chrono_dt) = chrono::DateTime::from_timestamp(secs, nsecs) else {
                    continue;
                };
                let datetime = chrono_dt.naive_utc();

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
/// Returns `"datetime"` if present, otherwise falls back to `"date"`.
pub fn detect_date_col(df: &polars::prelude::DataFrame) -> &'static str {
    if df.column("datetime").is_ok() {
        "datetime"
    } else {
        "date"
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a midnight `NaiveDateTime` from y/m/d for test bars.
    fn dt(y: i32, m: u32, d: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
    }

    fn make_bars() -> Vec<Bar> {
        vec![
            Bar {
                datetime: dt(2024, 1, 2),
                open: 100.0,
                high: 102.0,
                low: 99.0,
                close: 101.0,
            },
            Bar {
                datetime: dt(2024, 1, 3),
                open: 101.0,
                high: 103.0,
                low: 100.0,
                close: 102.0,
            },
            Bar {
                datetime: dt(2024, 1, 4),
                open: 102.0,
                high: 104.0,
                low: 101.0,
                close: 103.0,
            },
            Bar {
                datetime: dt(2024, 1, 5),
                open: 103.0,
                high: 105.0,
                low: 102.0,
                close: 104.0,
            },
            Bar {
                datetime: dt(2024, 1, 8),
                open: 104.0,
                high: 106.0,
                low: 103.0,
                close: 105.0,
            },
        ]
    }

    fn default_params() -> StockBacktestParams {
        StockBacktestParams {
            symbol: "SPY".to_string(),
            side: Side::Long,
            capital: 100_000.0,
            quantity: 100,
            sizing: None,
            max_positions: 1,
            slippage: Slippage::Mid,
            commission: None,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: std::collections::HashMap::new(),
            start_date: None,
            end_date: None,
            interval: Interval::Daily,
            session_filter: None,
        }
    }

    #[test]
    fn empty_bars_returns_zero_trades() {
        let params = default_params();
        let result = run_stock_backtest(&[], &params, None, None).unwrap();
        assert_eq!(result.trade_count, 0);
        assert!((result.total_pnl - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn no_entry_signal_no_trades() {
        let bars = make_bars();
        let params = default_params();
        let result = run_stock_backtest(&bars, &params, None, None).unwrap();
        assert_eq!(result.trade_count, 0);
    }

    #[test]
    fn entry_on_first_bar_close_at_end() {
        let bars = make_bars();
        let params = default_params();
        let mut entry_dates = HashSet::new();
        entry_dates.insert(bars[0].datetime);

        let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
        assert_eq!(result.trade_count, 1);
        // Entered at open 100, closed at close 105 → pnl = 5 * 100 = 500
        assert!((result.total_pnl - 500.0).abs() < 1e-6);
    }

    #[test]
    fn short_position_profits_on_decline() {
        let bars = vec![
            Bar {
                datetime: dt(2024, 1, 2),
                open: 105.0,
                high: 106.0,
                low: 104.0,
                close: 104.0,
            },
            Bar {
                datetime: dt(2024, 1, 3),
                open: 104.0,
                high: 105.0,
                low: 100.0,
                close: 100.0,
            },
        ];
        let mut params = default_params();
        params.side = Side::Short;
        let mut entry_dates = HashSet::new();
        entry_dates.insert(bars[0].datetime);

        let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
        assert_eq!(result.trade_count, 1);
        // Short at 105, close at 100 → pnl = (105-100) * 100 = 500
        assert!((result.total_pnl - 500.0).abs() < 1e-6);
    }

    #[test]
    fn stop_loss_fills_at_trigger_price() {
        // Entry at open 100. SL at 5% → trigger price = 95.0
        // Bar low = 94.0 (triggers SL), close = 90.0
        // P&L should use SL price 95.0, NOT close 90.0
        let bars = vec![
            Bar {
                datetime: dt(2024, 1, 2),
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
            },
            Bar {
                datetime: dt(2024, 1, 3),
                open: 100.5,
                high: 101.0,
                low: 94.0,
                close: 90.0, // Close is much lower than SL price
            },
        ];
        let mut params = default_params();
        params.stop_loss = Some(0.05);
        let mut entry_dates = HashSet::new();
        entry_dates.insert(bars[0].datetime);

        let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
        assert_eq!(result.trade_count, 1);
        assert_eq!(result.trade_log[0].exit_type, ExitType::StopLoss);
        // P&L = (95.0 - 100.0) * 100 = -500.0 (filled at SL price, not close of 90)
        assert!(
            (result.total_pnl - (-500.0)).abs() < 1e-6,
            "SL should fill at 95.0 not close 90.0, got pnl={}",
            result.total_pnl
        );
    }

    #[test]
    fn take_profit_fills_at_trigger_price() {
        // Entry at open 100. TP at 10% → trigger price = 110.0
        // Bar high = 112.0 (triggers TP), close = 111.0
        // P&L should use TP price 110.0, NOT close 111.0
        let bars = vec![
            Bar {
                datetime: dt(2024, 1, 2),
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
            },
            Bar {
                datetime: dt(2024, 1, 3),
                open: 101.0,
                high: 112.0,
                low: 100.0,
                close: 111.0,
            },
        ];
        let mut params = default_params();
        params.take_profit = Some(0.10);
        let mut entry_dates = HashSet::new();
        entry_dates.insert(bars[0].datetime);

        let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
        assert_eq!(result.trade_count, 1);
        assert_eq!(result.trade_log[0].exit_type, ExitType::TakeProfit);
        // P&L = (110.0 - 100.0) * 100 = 1000.0 (filled at TP price, not close 111)
        assert!(
            (result.total_pnl - 1000.0).abs() < 1e-6,
            "TP should fill at 110.0 not close 111.0, got pnl={}",
            result.total_pnl
        );
    }

    #[test]
    fn max_hold_days_triggers() {
        let bars = make_bars();
        let mut params = default_params();
        params.max_hold_days = Some(2);
        let mut entry_dates = HashSet::new();
        entry_dates.insert(bars[0].datetime);

        let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
        assert_eq!(result.trade_count, 1);
        assert_eq!(result.trade_log[0].exit_type, ExitType::MaxHold);
    }

    #[test]
    fn exit_signal_closes_position() {
        let bars = make_bars();
        let params = default_params();
        let mut entry_dates = HashSet::new();
        entry_dates.insert(bars[0].datetime);
        let mut exit_date_set = HashSet::new();
        exit_date_set.insert(bars[2].datetime);

        let result =
            run_stock_backtest(&bars, &params, Some(&entry_dates), Some(&exit_date_set)).unwrap();
        assert_eq!(result.trade_count, 1);
        assert_eq!(result.trade_log[0].exit_type, ExitType::Signal);
    }

    #[test]
    fn commission_in_trade_pnl() {
        let bars = make_bars();
        let mut params = default_params();
        params.commission = Some(Commission {
            per_contract: 0.01,
            base_fee: 1.0,
            min_fee: 0.0,
        });
        let mut entry_dates = HashSet::new();
        entry_dates.insert(bars[0].datetime);

        let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
        // Commission per side: 1.0 + 0.01*100 = 2.0
        // Gross PnL: (105-100)*100 = 500
        // Net: 500 - 2.0 (entry) - 2.0 (exit) = 496.0
        assert!(
            (result.total_pnl - 496.0).abs() < 1e-6,
            "Both entry and exit commission should be in trade P&L, got {}",
            result.total_pnl
        );
    }

    #[test]
    fn equity_curve_has_points_for_each_bar() {
        let bars = make_bars();
        let params = default_params();
        let mut entry_dates = HashSet::new();
        entry_dates.insert(bars[0].datetime);

        let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
        assert_eq!(result.equity_curve.len(), bars.len());
    }

    #[test]
    fn max_positions_respected() {
        let bars = make_bars();
        let mut params = default_params();
        params.max_positions = 1;
        let mut entry_dates = HashSet::new();
        entry_dates.insert(bars[0].datetime);
        entry_dates.insert(bars[1].datetime);

        let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
        assert_eq!(result.trade_count, 1);
    }

    #[test]
    fn short_entry_rejected_insufficient_margin() {
        let bars = make_bars();
        let mut params = default_params();
        params.side = Side::Short;
        params.capital = 5_000.0; // Not enough for 100 shares at ~100
        let mut entry_dates = HashSet::new();
        entry_dates.insert(bars[0].datetime);

        let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
        assert_eq!(
            result.trade_count, 0,
            "Short with insufficient margin should not open"
        );
    }

    #[test]
    fn short_entry_accepted_sufficient_margin() {
        let bars = make_bars();
        let mut params = default_params();
        params.side = Side::Short;
        params.capital = 15_000.0; // Enough for 100 shares at ~100
        let mut entry_dates = HashSet::new();
        entry_dates.insert(bars[0].datetime);

        let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
        assert_eq!(
            result.trade_count, 1,
            "Short with sufficient margin should open"
        );
    }

    // ── Resample tests ──────────────────────────────────────────────────

    fn make_daily_df() -> polars::prelude::DataFrame {
        use polars::prelude::*;
        // 10 trading days across 2 weeks: Jan 6-10 (week 2) and Jan 13-17 (week 3)
        let dates = vec![
            NaiveDate::from_ymd_opt(2025, 1, 6).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 7).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 8).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 9).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 14).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 15).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 16).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 17).unwrap(),
        ];
        let date_col =
            DateChunked::from_naive_date(PlSmallStr::from("date"), dates.clone()).into_column();

        df! {
            "open" =>    &[100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0],
            "high" =>    &[102.0, 103.0, 105.0, 106.0, 107.0, 108.0, 109.0, 110.0, 111.0, 112.0],
            "low" =>     &[ 99.0, 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0],
            "close" =>   &[101.0, 102.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0, 110.0, 111.0],
            "adjclose" => &[101.0, 102.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0, 110.0, 111.0],
            "volume" =>  &[1000_i64, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900],
        }
        .unwrap()
        .hstack(&[date_col])
        .unwrap()
        .select(["date", "open", "high", "low", "close", "adjclose", "volume"])
        .unwrap()
    }

    #[test]
    fn resample_daily_returns_same() {
        let df = make_daily_df();
        let result = resample_ohlcv(&df, Interval::Daily).unwrap();
        assert_eq!(result.height(), df.height());
    }

    #[test]
    fn resample_weekly_groups_by_week() {
        let df = make_daily_df();
        let result = resample_ohlcv(&df, Interval::Weekly).unwrap();
        // 2 ISO weeks → 2 bars
        assert_eq!(result.height(), 2);

        let opens = result.column("open").unwrap().f64().unwrap();
        let highs = result.column("high").unwrap().f64().unwrap();
        let lows = result.column("low").unwrap().f64().unwrap();
        let closes = result.column("close").unwrap().f64().unwrap();
        let volumes = result.column("volume").unwrap().i64().unwrap();

        // Week 1: open=100 (first), high=107 (max), low=99 (min), close=106 (last)
        assert!((opens.get(0).unwrap() - 100.0).abs() < 1e-6);
        assert!((highs.get(0).unwrap() - 107.0).abs() < 1e-6);
        assert!((lows.get(0).unwrap() - 99.0).abs() < 1e-6);
        assert!((closes.get(0).unwrap() - 106.0).abs() < 1e-6);
        // Volume: 1000+1100+1200+1300+1400 = 6000
        assert_eq!(volumes.get(0).unwrap(), 6000);

        // Week 2: open=105 (first), high=112 (max), low=104 (min), close=111 (last)
        assert!((opens.get(1).unwrap() - 105.0).abs() < 1e-6);
        assert!((highs.get(1).unwrap() - 112.0).abs() < 1e-6);
        assert!((lows.get(1).unwrap() - 104.0).abs() < 1e-6);
        assert!((closes.get(1).unwrap() - 111.0).abs() < 1e-6);
        // Volume: 1500+1600+1700+1800+1900 = 8500
        assert_eq!(volumes.get(1).unwrap(), 8500);
    }

    #[test]
    fn resample_monthly_groups_by_month() {
        let df = make_daily_df();
        let result = resample_ohlcv(&df, Interval::Monthly).unwrap();
        // All 10 days in Jan 2025 → 1 bar
        assert_eq!(result.height(), 1);

        let opens = result.column("open").unwrap().f64().unwrap();
        let closes = result.column("close").unwrap().f64().unwrap();

        assert!((opens.get(0).unwrap() - 100.0).abs() < 1e-6);
        assert!((closes.get(0).unwrap() - 111.0).abs() < 1e-6);
    }

    #[test]
    fn resample_preserves_date_column_type() {
        let df = make_daily_df();
        let result = resample_ohlcv(&df, Interval::Weekly).unwrap();
        // bars_from_df should work on the resampled output
        let bars = bars_from_df(&result).unwrap();
        assert_eq!(bars.len(), 2);
    }

    #[test]
    fn resample_weekly_year_boundary() {
        // Dec 30, 2024 is ISO week 1 of 2025; Jan 3, 2025 is also ISO week 1 of 2025.
        // They should group together in weekly resampling.
        use polars::prelude::*;
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 12, 30).unwrap(), // ISO week 1, 2025
            NaiveDate::from_ymd_opt(2024, 12, 31).unwrap(), // ISO week 1, 2025
            NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),   // ISO week 1, 2025
            NaiveDate::from_ymd_opt(2025, 1, 3).unwrap(),   // ISO week 1, 2025
            NaiveDate::from_ymd_opt(2025, 1, 6).unwrap(),   // ISO week 2, 2025
        ];
        let date_col = DateChunked::from_naive_date(PlSmallStr::from("date"), dates).into_column();
        let df = df! {
            "open" => &[100.0, 101.0, 102.0, 103.0, 104.0],
            "high" => &[102.0, 103.0, 104.0, 105.0, 106.0],
            "low" => &[99.0, 100.0, 101.0, 102.0, 103.0],
            "close" => &[101.0, 102.0, 103.0, 104.0, 105.0],
            "volume" => &[1000_i64, 1100, 1200, 1300, 1400],
        }
        .unwrap()
        .hstack(&[date_col])
        .unwrap()
        .select(["date", "open", "high", "low", "close", "volume"])
        .unwrap();

        let result = resample_ohlcv(&df, Interval::Weekly).unwrap();
        // Dec 30 + Dec 31 + Jan 2 + Jan 3 → ISO week 1 → 1 bar
        // Jan 6 → ISO week 2 → 1 bar
        assert_eq!(result.height(), 2);
        let volumes = result.column("volume").unwrap().i64().unwrap();
        assert_eq!(volumes.get(0).unwrap(), 1000 + 1100 + 1200 + 1300);
        assert_eq!(volumes.get(1).unwrap(), 1400);
    }

    #[test]
    fn resample_empty_dataframe() {
        use polars::prelude::*;
        let dates: Vec<NaiveDate> = vec![];
        let date_col = DateChunked::from_naive_date(PlSmallStr::from("date"), dates).into_column();
        let df = df! {
            "open" => Vec::<f64>::new(),
            "high" => Vec::<f64>::new(),
            "low" => Vec::<f64>::new(),
            "close" => Vec::<f64>::new(),
            "volume" => Vec::<i64>::new(),
        }
        .unwrap()
        .hstack(&[date_col])
        .unwrap()
        .select(["date", "open", "high", "low", "close", "volume"])
        .unwrap();

        let result = resample_ohlcv(&df, Interval::Weekly).unwrap();
        assert_eq!(result.height(), 0);
    }

    #[test]
    fn resample_without_adjclose() {
        use polars::prelude::*;
        let dates = vec![
            NaiveDate::from_ymd_opt(2025, 1, 6).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 7).unwrap(),
        ];
        let date_col = DateChunked::from_naive_date(PlSmallStr::from("date"), dates).into_column();
        let df = df! {
            "open" => &[100.0, 101.0],
            "high" => &[102.0, 103.0],
            "low" => &[99.0, 100.0],
            "close" => &[101.0, 102.0],
            "volume" => &[1000_i64, 1100],
        }
        .unwrap()
        .hstack(&[date_col])
        .unwrap()
        .select(["date", "open", "high", "low", "close", "volume"])
        .unwrap();

        let result = resample_ohlcv(&df, Interval::Weekly).unwrap();
        assert_eq!(result.height(), 1);
        // Should NOT have adjclose column
        assert!(result.column("adjclose").is_err());
    }

    #[test]
    fn resample_single_row_per_group() {
        use polars::prelude::*;
        // Two dates in different weeks, one bar each
        let dates = vec![
            NaiveDate::from_ymd_opt(2025, 1, 6).unwrap(),  // Week 2
            NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(), // Week 3
        ];
        let date_col = DateChunked::from_naive_date(PlSmallStr::from("date"), dates).into_column();
        let df = df! {
            "open" => &[100.0, 110.0],
            "high" => &[102.0, 112.0],
            "low" => &[99.0, 109.0],
            "close" => &[101.0, 111.0],
            "volume" => &[1000_i64, 2000],
        }
        .unwrap()
        .hstack(&[date_col])
        .unwrap()
        .select(["date", "open", "high", "low", "close", "volume"])
        .unwrap();

        let result = resample_ohlcv(&df, Interval::Weekly).unwrap();
        assert_eq!(result.height(), 2);
        // Each group has one bar, so OHLCV should be unchanged
        let opens = result.column("open").unwrap().f64().unwrap();
        let closes = result.column("close").unwrap().f64().unwrap();
        assert!((opens.get(0).unwrap() - 100.0).abs() < 1e-6);
        assert!((closes.get(0).unwrap() - 101.0).abs() < 1e-6);
        assert!((opens.get(1).unwrap() - 110.0).abs() < 1e-6);
        assert!((closes.get(1).unwrap() - 111.0).abs() < 1e-6);
    }

    // --- Intraday resampling tests ---

    /// Build a synthetic intraday `DataFrame` with `"datetime"` (Datetime) column.
    /// 12 one-minute bars starting at 2025-01-06 09:30:00.
    #[allow(clippy::let_and_return)]
    fn make_intraday_df() -> polars::prelude::DataFrame {
        use polars::prelude::*;

        let base = NaiveDate::from_ymd_opt(2025, 1, 6)
            .unwrap()
            .and_hms_opt(9, 30, 0)
            .unwrap();
        let timestamps_us: Vec<i64> = (0..12)
            .map(|i| {
                let dt = base + chrono::Duration::minutes(i);
                dt.and_utc().timestamp_micros()
            })
            .collect();

        let dt_series = Series::new("datetime".into(), &timestamps_us)
            .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
            .unwrap();

        let df = df! {
            "open" =>    &[100.0, 101.0, 102.0, 103.0, 104.0, 105.0,
                           106.0, 107.0, 108.0, 109.0, 110.0, 111.0],
            "high" =>    &[101.0, 102.0, 103.0, 104.0, 105.0, 106.0,
                           107.0, 108.0, 109.0, 110.0, 111.0, 112.0],
            "low" =>     &[ 99.0, 100.0, 101.0, 102.0, 103.0, 104.0,
                           105.0, 106.0, 107.0, 108.0, 109.0, 110.0],
            "close" =>   &[100.5, 101.5, 102.5, 103.5, 104.5, 105.5,
                           106.5, 107.5, 108.5, 109.5, 110.5, 111.5],
            "adjclose" => &[100.5, 101.5, 102.5, 103.5, 104.5, 105.5,
                           106.5, 107.5, 108.5, 109.5, 110.5, 111.5],
            "volume" =>  &[1000_i64, 1100, 1200, 1300, 1400, 1500,
                           1600, 1700, 1800, 1900, 2000, 2100],
        }
        .unwrap()
        .hstack(&[dt_series.into()])
        .unwrap()
        .select(["datetime", "open", "high", "low", "close", "adjclose", "volume"])
        .unwrap();

        df
    }

    #[test]
    fn resample_intraday_min1_passthrough() {
        let df = make_intraday_df();
        let result = resample_ohlcv(&df, Interval::Min1).unwrap();
        assert_eq!(result.height(), df.height());
    }

    #[test]
    fn resample_intraday_1m_to_5m() {
        let df = make_intraday_df();
        let result = resample_ohlcv(&df, Interval::Min5).unwrap();
        // 12 bars at 09:30..09:41 → 5-min groups: [09:30-09:34], [09:35-09:39], [09:40-09:41]
        // Group 1: min 30-34 (truncate to 30) → 5 bars
        // Group 2: min 35-39 (truncate to 35) → 5 bars
        // Group 3: min 40-41 (truncate to 40) → 2 bars
        assert_eq!(result.height(), 3);

        // Output should have "datetime" column (intraday target)
        assert!(result.column("datetime").is_ok());
        assert!(result.column("date").is_err());

        let opens = result.column("open").unwrap().f64().unwrap();
        let closes = result.column("close").unwrap().f64().unwrap();
        let highs = result.column("high").unwrap().f64().unwrap();
        let lows = result.column("low").unwrap().f64().unwrap();
        let volumes = result.column("volume").unwrap().i64().unwrap();

        // Group 1 (09:30-09:34): open=100, high=max(101..105)=105, low=min(99..103)=99, close=104.5
        assert!((opens.get(0).unwrap() - 100.0).abs() < 1e-6);
        assert!((highs.get(0).unwrap() - 105.0).abs() < 1e-6);
        assert!((lows.get(0).unwrap() - 99.0).abs() < 1e-6);
        assert!((closes.get(0).unwrap() - 104.5).abs() < 1e-6);
        // Volume: 1000+1100+1200+1300+1400 = 6000
        assert_eq!(volumes.get(0).unwrap(), 6000);

        // Group 3 (09:40-09:41): open=110, close=111.5, 2 bars
        assert!((opens.get(2).unwrap() - 110.0).abs() < 1e-6);
        assert!((closes.get(2).unwrap() - 111.5).abs() < 1e-6);
        assert_eq!(volumes.get(2).unwrap(), 2000 + 2100);
    }

    #[test]
    fn resample_intraday_1m_to_30m() {
        let df = make_intraday_df();
        let result = resample_ohlcv(&df, Interval::Min30).unwrap();
        // 12 bars from 09:30-09:41 all fall in the 09:30 30-min bucket
        assert_eq!(result.height(), 1);

        let opens = result.column("open").unwrap().f64().unwrap();
        let closes = result.column("close").unwrap().f64().unwrap();
        assert!((opens.get(0).unwrap() - 100.0).abs() < 1e-6);
        assert!((closes.get(0).unwrap() - 111.5).abs() < 1e-6);
    }

    #[test]
    fn resample_intraday_1m_to_hourly() {
        let df = make_intraday_df();
        let result = resample_ohlcv(&df, Interval::Hour1).unwrap();
        // All 12 bars are in the 09:xx hour → 1 group
        assert_eq!(result.height(), 1);

        let volumes = result.column("volume").unwrap().i64().unwrap();
        let expected_vol: i64 = (1000..=2100).step_by(100).sum();
        assert_eq!(volumes.get(0).unwrap(), expected_vol);
    }

    #[test]
    fn resample_intraday_to_daily() {
        let df = make_intraday_df();
        let result = resample_ohlcv(&df, Interval::Daily).unwrap();
        // All bars on 2025-01-06 → 1 daily bar
        assert_eq!(result.height(), 1);

        // Output should have "date" column (daily target), not "datetime"
        assert!(result.column("date").is_ok());
        assert!(result.column("datetime").is_err());

        let opens = result.column("open").unwrap().f64().unwrap();
        let highs = result.column("high").unwrap().f64().unwrap();
        let lows = result.column("low").unwrap().f64().unwrap();
        let closes = result.column("close").unwrap().f64().unwrap();
        assert!((opens.get(0).unwrap() - 100.0).abs() < 1e-6);
        assert!((highs.get(0).unwrap() - 112.0).abs() < 1e-6);
        assert!((lows.get(0).unwrap() - 99.0).abs() < 1e-6);
        assert!((closes.get(0).unwrap() - 111.5).abs() < 1e-6);
    }

    #[test]
    fn resample_intraday_to_weekly() {
        let df = make_intraday_df();
        let result = resample_ohlcv(&df, Interval::Weekly).unwrap();
        // All bars on same day → 1 weekly bar
        assert_eq!(result.height(), 1);
        assert!(result.column("date").is_ok());
    }

    #[test]
    fn resample_daily_to_intraday_errors() {
        let df = make_daily_df();
        let result = resample_ohlcv(&df, Interval::Min5);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Cannot resample daily data to intraday"));
    }

    #[test]
    fn resample_intraday_to_daily_feeds_bars_from_df() {
        // Verify that intraday→daily resampled output can be consumed by bars_from_df
        let df = make_intraday_df();
        let daily = resample_ohlcv(&df, Interval::Daily).unwrap();
        let bars = bars_from_df(&daily).unwrap();
        assert_eq!(bars.len(), 1);
        assert!((bars[0].open - 100.0).abs() < 1e-6);
    }

    // --- Real fixture tests (SPY 1-min Parquet) ---

    fn load_fixture_df() -> polars::prelude::DataFrame {
        use polars::prelude::*;
        LazyFrame::scan_parquet("tests/fixtures/SPY_1min_sample.parquet".into(), ScanArgsParquet::default())
            .expect("scan parquet")
            .collect()
            .expect("collect")
    }

    #[test]
    fn fixture_bars_from_df_reads_intraday() {
        let df = load_fixture_df();
        let bars = bars_from_df(&df).unwrap();
        assert!(bars.len() > 1000, "expected many bars, got {}", bars.len());
        // Bars should have sub-day precision
        assert_ne!(bars[0].datetime.time(), chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap());
    }

    #[test]
    fn fixture_resample_1m_to_5m() {
        let df = load_fixture_df();
        let result = resample_ohlcv(&df, Interval::Min5).unwrap();
        // 10269 1-min bars → ~2054 5-min bars
        assert!(result.height() > 2000 && result.height() < 2200,
            "unexpected 5m bar count: {}", result.height());
        assert!(result.column("datetime").is_ok());
    }

    #[test]
    fn fixture_resample_1m_to_hourly() {
        let df = load_fixture_df();
        let result = resample_ohlcv(&df, Interval::Hour1).unwrap();
        assert!(result.height() > 100 && result.height() < 200,
            "unexpected hourly bar count: {}", result.height());
        assert!(result.column("datetime").is_ok());
    }

    #[test]
    fn fixture_resample_1m_to_daily() {
        let df = load_fixture_df();
        let result = resample_ohlcv(&df, Interval::Daily).unwrap();
        // Multi-day dataset → several daily bars
        assert!(result.height() >= 2, "expected multiple daily bars, got {}", result.height());
        assert!(result.column("date").is_ok());

        // OHLCV invariants: high >= open, high >= close, low <= open, low <= close
        let opens = result.column("open").unwrap().f64().unwrap();
        let highs = result.column("high").unwrap().f64().unwrap();
        let lows = result.column("low").unwrap().f64().unwrap();
        let closes = result.column("close").unwrap().f64().unwrap();
        for i in 0..result.height() {
            let (o, h, l, c) = (
                opens.get(i).unwrap(), highs.get(i).unwrap(),
                lows.get(i).unwrap(), closes.get(i).unwrap(),
            );
            assert!(h >= o && h >= c, "high < open or close at row {i}");
            assert!(l <= o && l <= c, "low > open or close at row {i}");
        }
    }

    #[test]
    fn fixture_resample_to_daily_feeds_bars_from_df() {
        let df = load_fixture_df();
        let daily = resample_ohlcv(&df, Interval::Daily).unwrap();
        let bars = bars_from_df(&daily).unwrap();
        assert!(bars.len() >= 2);
        // Daily bars should be at midnight
        assert_eq!(bars[0].datetime.time(), chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap());
    }

    #[test]
    fn fixture_resample_to_weekly() {
        let df = load_fixture_df();
        let result = resample_ohlcv(&df, Interval::Weekly).unwrap();
        assert!(result.height() >= 1);
        assert!(result.column("date").is_ok());

        // Volume should sum correctly — total should match source
        let src_vol: i64 = df.column("volume").unwrap().i64().unwrap().into_iter().flatten().sum();
        let dst_vol: i64 = result.column("volume").unwrap().i64().unwrap().into_iter().flatten().sum();
        assert_eq!(src_vol, dst_vol, "volume mismatch after weekly resample");
    }

    #[test]
    fn fixture_session_filter_premarket() {
        let df = load_fixture_df();
        let mut bars = bars_from_df(&df).unwrap();
        let before = bars.len();
        let (start, end) = crate::engine::types::SessionFilter::Premarket.time_range();
        bars.retain(|b| { let t = b.datetime.time(); t >= start && t < end });
        // Premarket = 04:00-09:30 — fixture starts at 04:00 so should have premarket bars
        assert!(!bars.is_empty(), "no premarket bars found");
        assert!(bars.len() < before, "filter should reduce bar count");
        // All bars within premarket window
        for b in &bars {
            let t = b.datetime.time();
            assert!(t >= start && t < end, "bar at {t} outside premarket");
        }
    }
}
