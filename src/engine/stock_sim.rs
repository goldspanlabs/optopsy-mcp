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
    BacktestResult, Commission, EquityPoint, ExitType, Side, Slippage, TradeRecord,
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
}

/// A single day's OHLCV bar for simulation.
#[derive(Debug, Clone)]
pub struct Bar {
    date: NaiveDate,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
}

/// An open stock position tracked during simulation.
#[derive(Debug, Clone)]
struct StockPosition {
    id: usize,
    entry_date: NaiveDate,
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
    entry_dates: Option<&HashSet<NaiveDate>>,
    exit_dates: Option<&HashSet<NaiveDate>>,
) -> Result<BacktestResult> {
    if bars.is_empty() {
        return Ok(empty_result(params.capital));
    }

    let mut equity = params.capital;
    let mut positions: Vec<StockPosition> = Vec::new();
    let mut trade_log: Vec<TradeRecord> = Vec::new();
    let mut equity_curve: Vec<EquityPoint> = Vec::new();
    let mut next_trade_id: usize = 1;

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
                let (pnl, record) =
                    close_position(&pos, bar, decision.exit_type, decision.fill_price, params);
                equity += pnl;
                trade_log.push(record);
            }
        }

        // ── 2. Check entry ──────────────────────────────────────────────────
        let can_enter = (positions.len() as i32) < params.max_positions;
        let signal_fires = entry_dates
            .as_ref()
            .is_some_and(|dates| dates.contains(&bar.date));

        if can_enter && signal_fires {
            let entry_price = compute_entry_price(bar, params.side, &params.slippage);

            // Dynamic position sizing
            let effective_qty = params.sizing.as_ref().map_or(params.quantity, |cfg| {
                let ml = super::sizing::max_loss_per_share(entry_price, params.stop_loss);
                if ml <= 0.0 {
                    return params.quantity;
                }
                // Collect closes up to today for volatility computation
                let vol = {
                    let lookback = match &cfg.method {
                        super::types::PositionSizing::VolatilityTarget {
                            lookback_days, ..
                        } => *lookback_days as usize,
                        _ => 20,
                    };
                    let idx = bars.iter().position(|b| b.date == bar.date).unwrap_or(0);
                    let closes: Vec<f64> = bars[..=idx].iter().map(|b| b.close).collect();
                    super::sizing::compute_realized_vol(&closes, lookback)
                };
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
                // Skip — not enough capital/margin
            } else {
                let pos = StockPosition {
                    id: next_trade_id,
                    entry_date: bar.date,
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
            datetime: bar.date.and_hms_opt(16, 0, 0).unwrap_or_else(|| {
                NaiveDateTime::new(bar.date, chrono::NaiveTime::from_hms_opt(16, 0, 0).unwrap())
            }),
            equity: equity + unrealized,
        });
    }

    // ── 4. Force-close remaining positions at last bar ──────────────────
    if let Some(last_bar) = bars.last() {
        for pos in &positions {
            // Force-close P&L is already reflected in equity curve via mark-to-market
            let (_pnl, record) = close_position(pos, last_bar, ExitType::MaxHold, None, params);
            trade_log.push(record);
        }
    }

    let total_pnl: f64 = trade_log.iter().map(|t| t.pnl).sum();
    let trade_count = trade_log.len();

    let perf_metrics = metrics::calculate_metrics(&equity_curve, &trade_log, params.capital)
        .unwrap_or(metrics::DEFAULT_METRICS);

    Ok(BacktestResult {
        trade_count,
        total_pnl,
        metrics: perf_metrics,
        equity_curve,
        trade_log,
        quality: crate::engine::types::BacktestQualityStats::default(),
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
    exit_dates: Option<&HashSet<NaiveDate>>,
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
        let days_held = (bar.date - pos.entry_date).num_days();
        if days_held >= i64::from(max_days) {
            return Some(ExitDecision {
                exit_type: ExitType::MaxHold,
                fill_price: None,
            });
        }
    }

    // Exit signal
    if let Some(dates) = exit_dates {
        if dates.contains(&bar.date) {
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

    let days_held = (bar.date - pos.entry_date).num_days();

    let entry_cost = pos.entry_price * qty * direction;
    let exit_proceeds = exit_price * qty * direction;

    let entry_dt = pos.entry_date.and_hms_opt(9, 30, 0).unwrap_or_else(|| {
        NaiveDateTime::new(
            pos.entry_date,
            chrono::NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
        )
    });
    let exit_dt = bar.date.and_hms_opt(16, 0, 0).unwrap_or_else(|| {
        NaiveDateTime::new(bar.date, chrono::NaiveTime::from_hms_opt(16, 0, 0).unwrap())
    });

    let record = TradeRecord::new(
        pos.id,
        entry_dt,
        exit_dt,
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
    }
}

/// Load an OHLCV parquet file into a `DataFrame`, applying date range and
/// validity filters via Polars lazy predicates for predicate pushdown.
pub fn load_ohlcv_df(
    ohlcv_path: &str,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
) -> Result<polars::prelude::DataFrame> {
    use polars::prelude::*;

    let args = ScanArgsParquet::default();
    let mut lazy = LazyFrame::scan_parquet(ohlcv_path.into(), args)?
        .filter(col("open").gt(lit(0.0)).and(col("close").gt(lit(0.0))));

    if let Some(start) = start_date {
        lazy = lazy.filter(col("date").gt_eq(lit(start)));
    }
    if let Some(end) = end_date {
        lazy = lazy.filter(col("date").lt_eq(lit(end)));
    }

    let df = lazy
        .sort(["date"], SortMultipleOptions::default())
        .collect()?;
    Ok(df)
}

/// Convert an already-loaded OHLCV `DataFrame` into `Bar` structs.
pub fn bars_from_df(df: &polars::prelude::DataFrame) -> Result<Vec<Bar>> {
    let dates = df
        .column("date")
        .map_err(|e| anyhow::anyhow!("Missing 'date' column: {e}"))?
        .date()
        .map_err(|e| anyhow::anyhow!("'date' column is not Date type: {e}"))?;

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

    for i in 0..df.height() {
        let Some(days) = dates.phys.get(i) else {
            continue;
        };
        let Some(date) = NaiveDate::from_num_days_from_ce_opt(days + epoch_offset) else {
            continue;
        };

        let open = opens.get(i).unwrap_or(0.0);
        let high = highs.get(i).unwrap_or(0.0);
        let low = lows.get(i).unwrap_or(0.0);
        let close = closes.get(i).unwrap_or(0.0);

        // Validity already filtered at the lazy level, but guard against nulls
        if open <= 0.0 || close <= 0.0 {
            continue;
        }

        bars.push(Bar {
            date,
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

/// Optional set of dates on which a signal is active.
type DateFilter = Option<HashSet<NaiveDate>>;

/// Build signal date filters for stock backtest from a pre-loaded OHLCV `DataFrame`.
///
/// Accepts the primary OHLCV data directly to avoid re-reading the parquet file.
/// Cross-symbol data is still loaded from `params.cross_ohlcv_paths` on demand.
pub fn build_stock_signal_filters(
    params: &StockBacktestParams,
    ohlcv_df: &polars::prelude::DataFrame,
) -> Result<(DateFilter, DateFilter)> {
    use crate::signals;

    let has_entry = params.entry_signal.is_some();
    let has_exit = params.exit_signal.is_some();

    if !has_entry && !has_exit {
        return Ok((None, None));
    }

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

        let entry_dates = params
            .entry_signal
            .as_ref()
            .map(|spec| signals::active_dates_multi(spec, ohlcv_df, &cross_dfs, "date"))
            .transpose()?;
        let exit_dates = params
            .exit_signal
            .as_ref()
            .map(|spec| signals::active_dates_multi(spec, ohlcv_df, &cross_dfs, "date"))
            .transpose()?;

        Ok((entry_dates, exit_dates))
    } else {
        let entry_dates = params
            .entry_signal
            .as_ref()
            .map(|spec| signals::active_dates(spec, ohlcv_df, "date"))
            .transpose()?;
        let exit_dates = params
            .exit_signal
            .as_ref()
            .map(|spec| signals::active_dates(spec, ohlcv_df, "date"))
            .transpose()?;

        Ok((entry_dates, exit_dates))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_bars() -> Vec<Bar> {
        vec![
            Bar {
                date: NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
                open: 100.0,
                high: 102.0,
                low: 99.0,
                close: 101.0,
            },
            Bar {
                date: NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
                open: 101.0,
                high: 103.0,
                low: 100.0,
                close: 102.0,
            },
            Bar {
                date: NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(),
                open: 102.0,
                high: 104.0,
                low: 101.0,
                close: 103.0,
            },
            Bar {
                date: NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(),
                open: 103.0,
                high: 105.0,
                low: 102.0,
                close: 104.0,
            },
            Bar {
                date: NaiveDate::from_ymd_opt(2024, 1, 8).unwrap(),
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
        entry_dates.insert(bars[0].date);

        let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
        assert_eq!(result.trade_count, 1);
        // Entered at open 100, closed at close 105 → pnl = 5 * 100 = 500
        assert!((result.total_pnl - 500.0).abs() < 1e-6);
    }

    #[test]
    fn short_position_profits_on_decline() {
        let bars = vec![
            Bar {
                date: NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
                open: 105.0,
                high: 106.0,
                low: 104.0,
                close: 104.0,
            },
            Bar {
                date: NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
                open: 104.0,
                high: 105.0,
                low: 100.0,
                close: 100.0,
            },
        ];
        let mut params = default_params();
        params.side = Side::Short;
        let mut entry_dates = HashSet::new();
        entry_dates.insert(bars[0].date);

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
                date: NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
            },
            Bar {
                date: NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
                open: 100.5,
                high: 101.0,
                low: 94.0,
                close: 90.0, // Close is much lower than SL price
            },
        ];
        let mut params = default_params();
        params.stop_loss = Some(0.05);
        let mut entry_dates = HashSet::new();
        entry_dates.insert(bars[0].date);

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
                date: NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
            },
            Bar {
                date: NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
                open: 101.0,
                high: 112.0,
                low: 100.0,
                close: 111.0,
            },
        ];
        let mut params = default_params();
        params.take_profit = Some(0.10);
        let mut entry_dates = HashSet::new();
        entry_dates.insert(bars[0].date);

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
        entry_dates.insert(bars[0].date);

        let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
        assert_eq!(result.trade_count, 1);
        assert_eq!(result.trade_log[0].exit_type, ExitType::MaxHold);
    }

    #[test]
    fn exit_signal_closes_position() {
        let bars = make_bars();
        let params = default_params();
        let mut entry_dates = HashSet::new();
        entry_dates.insert(bars[0].date);
        let mut exit_date_set = HashSet::new();
        exit_date_set.insert(bars[2].date);

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
        entry_dates.insert(bars[0].date);

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
        entry_dates.insert(bars[0].date);

        let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
        assert_eq!(result.equity_curve.len(), bars.len());
    }

    #[test]
    fn max_positions_respected() {
        let bars = make_bars();
        let mut params = default_params();
        params.max_positions = 1;
        let mut entry_dates = HashSet::new();
        entry_dates.insert(bars[0].date);
        entry_dates.insert(bars[1].date);

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
        entry_dates.insert(bars[0].date);

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
        entry_dates.insert(bars[0].date);

        let result = run_stock_backtest(&bars, &params, Some(&entry_dates), None).unwrap();
        assert_eq!(
            result.trade_count, 1,
            "Short with sufficient margin should open"
        );
    }
}
