//! Vectorized backtest simulation — replaces the day-by-day event loop for strategies
//! without adjustment rules.
//!
//! Four-phase pipeline:
//! 1. Find entry candidates (reuse `find_entry_candidates`) + resolve exits via price table
//! 2. Apply trade selector (pick one trade per entry date)
//! 3. Vectorized early exit scanning (`SL/TP/max_hold/exit_signal`)
//! 4. Position overlap filter + build trade log & equity curve

use std::collections::{BTreeMap, HashSet};
use std::hash::BuildHasher;

use anyhow::Result;
use chrono::NaiveDate;
use ordered_float::OrderedFloat;

use super::event_sim;
use super::pricing;
#[allow(clippy::wildcard_imports)]
use super::types::*;
use crate::strategies;

/// Secondary index for O(log n) carry-forward lookups.
/// Key: (expiration, strike, `option_type`) → sorted map of date → snapshot reference data.
type CarryIndex =
    BTreeMap<(NaiveDate, OrderedFloat<f64>, OptionType), BTreeMap<NaiveDate, QuoteSnapshot>>;

/// Build the carry-forward index from the price table.
fn build_carry_index(price_table: &PriceTable) -> CarryIndex {
    let mut index = CarryIndex::new();
    for ((date, exp, strike, opt), snap) in price_table {
        index
            .entry((*exp, *strike, *opt))
            .or_default()
            .insert(*date, snap.clone());
    }
    index
}

/// Run the vectorized backtest pipeline.
///
/// Assumes no adjustment rules — caller should dispatch to event loop for those.
pub fn run_vectorized_backtest<S1: BuildHasher, S2: BuildHasher>(
    df: &polars::prelude::DataFrame,
    params: &BacktestParams,
    entry_dates: &Option<HashSet<NaiveDate, S1>>,
    exit_dates: Option<&HashSet<NaiveDate, S2>>,
) -> Result<(Vec<TradeRecord>, Vec<EquityPoint>, BacktestQualityStats)> {
    let strategy_def = strategies::find_strategy(&params.strategy)
        .ok_or_else(|| anyhow::anyhow!("Unknown strategy: {}", params.strategy))?;

    // Build price table and get trading days
    let t0 = std::time::Instant::now();
    let (price_table, trading_days) = event_sim::build_price_table(df)?;
    tracing::info!(
        elapsed_ms = t0.elapsed().as_millis(),
        entries = price_table.len(),
        "Price table built"
    );

    // Build secondary index for carry-forward lookups
    let carry_index = build_carry_index(&price_table);

    // Phase 1: Find entry candidates (same pipeline as event loop)
    let t1 = std::time::Instant::now();
    let mut candidates = event_sim::find_entry_candidates(df, &strategy_def, params)?;
    tracing::info!(
        elapsed_ms = t1.elapsed().as_millis(),
        candidates = candidates.values().map(Vec::len).sum::<usize>(),
        "Entry candidates found"
    );

    // Apply entry signal filter
    if let Some(ref allowed_dates) = entry_dates {
        candidates.retain(|date, _| allowed_dates.contains(date));
    }

    if candidates.is_empty() {
        return Ok((vec![], vec![], BacktestQualityStats::default()));
    }

    // Convert candidates to TradeRows with exit resolution via price table
    let t2 = std::time::Instant::now();
    let trades = build_trade_rows_from_candidates(
        &candidates,
        &price_table,
        &carry_index,
        &trading_days,
        &strategy_def,
        params,
    );
    tracing::info!(
        elapsed_ms = t2.elapsed().as_millis(),
        trades = trades.len(),
        "Trade rows built"
    );

    if trades.is_empty() {
        return Ok((vec![], vec![], BacktestQualityStats::default()));
    }

    // Phase 2: Apply trade selector (already one per date from find_entry_candidates,
    // but multiple expirations may exist per date)
    let trades = apply_trade_selector(trades, &params.selector, params.entry_dte.target);

    // Phase 3: Early exit scanning (SL/TP/max_hold/exit_signal)
    let has_early_exit = params.stop_loss.is_some()
        || params.take_profit.is_some()
        || params.max_hold_days.is_some()
        || exit_dates.is_some();

    let t3 = std::time::Instant::now();
    let trades = if has_early_exit {
        apply_early_exits(
            trades,
            &price_table,
            &carry_index,
            &trading_days,
            params,
            exit_dates,
        )
    } else {
        trades
    };
    tracing::info!(
        elapsed_ms = t3.elapsed().as_millis(),
        "Early exit scan done"
    );

    // Phase 4: Position overlap filter
    let trades = filter_overlapping_trades(trades, params.max_positions);

    // Build quality stats
    let total_candidates: usize = candidates.values().map(Vec::len).sum();
    let mut entry_spread_pcts = Vec::new();
    for t in &trades {
        for leg in &t.legs {
            if leg.entry_bid > 0.0 && leg.entry_ask > 0.0 {
                let mid = f64::midpoint(leg.entry_bid, leg.entry_ask);
                let spread_pct = (leg.entry_ask - leg.entry_bid) / mid * 100.0;
                entry_spread_pcts.push(spread_pct);
            }
        }
    }

    // Build trade log and equity curve
    let t4 = std::time::Instant::now();
    let (trade_log, equity_curve) =
        build_outputs(&trades, &trading_days, &price_table, &carry_index, params);
    tracing::info!(elapsed_ms = t4.elapsed().as_millis(), "Outputs built");

    let quality = BacktestQualityStats {
        trading_days_total: trading_days.len(),
        trading_days_with_data: trading_days.len(),
        total_candidates,
        positions_opened: trade_log.len(),
        entry_spread_pcts,
    };

    Ok((trade_log, equity_curve, quality))
}

/// Intermediate trade representation with all data needed for filtering and P&L.
#[derive(Debug, Clone)]
struct TradeRow {
    entry_date: NaiveDate,
    exit_date: NaiveDate,
    expiration: NaiveDate,
    secondary_expiration: Option<NaiveDate>,
    legs: Vec<TradeRowLeg>,
    entry_cost: f64,
    exit_type: ExitType,
}

#[derive(Debug, Clone)]
struct TradeRowLeg {
    side: Side,
    option_type: OptionType,
    strike: f64,
    expiration: NaiveDate,
    entry_bid: f64,
    entry_ask: f64,
    exit_bid: f64,
    exit_ask: f64,
    qty: i32,
}

/// Build trade rows from entry candidates, resolving exit prices via the price table.
///
/// For each candidate, finds the exit date by scanning trading days for the first date
/// where DTE <= `exit_dte` (matching the event loop's DTE exit logic).
fn build_trade_rows_from_candidates(
    candidates: &std::collections::BTreeMap<NaiveDate, Vec<EntryCandidate>>,
    _price_table: &PriceTable,
    carry_index: &CarryIndex,
    trading_days: &[NaiveDate],
    strategy_def: &StrategyDef,
    params: &BacktestParams,
) -> Vec<TradeRow> {
    let mut trades = Vec::new();
    let last_trading_day = trading_days.last().copied();

    for day_candidates in candidates.values() {
        for candidate in day_candidates {
            // Find exit date: first trading day where DTE <= exit_dte, or expiration
            let exit_date = find_exit_date(
                candidate.entry_date,
                candidate.expiration,
                params.exit_dte,
                trading_days,
            );

            // Skip trades whose exit falls beyond available data
            // (matches event loop behavior where such trades stay open and aren't logged)
            if let Some(last_day) = last_trading_day {
                if exit_date > last_day {
                    continue;
                }
            }

            let mut legs = Vec::new();
            let mut entry_cost = 0.0;

            for (cand_leg, leg_def) in candidate.legs.iter().zip(strategy_def.legs.iter()) {
                let contracts = leg_def.qty * params.quantity;
                let entry_price =
                    pricing::fill_price(cand_leg.bid, cand_leg.ask, leg_def.side, &params.slippage);
                entry_cost += entry_price
                    * f64::from(contracts)
                    * f64::from(params.multiplier)
                    * leg_def.side.multiplier();

                // Look up exit prices from price table, falling back to the most recent
                // quote on or before exit_date for the same contract (carry-forward behavior
                // matching the event loop's last_known lookup on data gaps).
                let (exit_bid, exit_ask) = lookup_exit_snap(
                    carry_index,
                    exit_date,
                    cand_leg.expiration,
                    OrderedFloat(cand_leg.strike),
                    cand_leg.option_type,
                )
                .map_or((0.0, 0.0), |snap| (snap.bid, snap.ask));

                legs.push(TradeRowLeg {
                    side: leg_def.side,
                    option_type: cand_leg.option_type,
                    strike: cand_leg.strike,
                    expiration: cand_leg.expiration,
                    entry_bid: cand_leg.bid,
                    entry_ask: cand_leg.ask,
                    exit_bid,
                    exit_ask,
                    qty: contracts,
                });
            }

            // Determine exit type
            let exit_type = if exit_date >= candidate.expiration {
                ExitType::Expiration
            } else {
                ExitType::DteExit
            };

            trades.push(TradeRow {
                entry_date: candidate.entry_date,
                exit_date,
                expiration: candidate.expiration,
                secondary_expiration: candidate.secondary_expiration,
                legs,
                entry_cost,
                exit_type,
            });
        }
    }

    trades
}

/// Find the exit date for a trade: first trading day where DTE <= `exit_dte`,
/// or the expiration date if no such day exists.
fn find_exit_date(
    entry_date: NaiveDate,
    expiration: NaiveDate,
    exit_dte: i32,
    trading_days: &[NaiveDate],
) -> NaiveDate {
    let start_idx = trading_days.partition_point(|d| *d <= entry_date);
    for &day in &trading_days[start_idx..] {
        let dte = (expiration - day).num_days();
        if dte <= i64::from(exit_dte) {
            return day;
        }
        if day >= expiration {
            return expiration;
        }
    }
    expiration
}

/// Pick one trade per entry date based on the `TradeSelector`.
fn apply_trade_selector(
    mut trades: Vec<TradeRow>,
    selector: &TradeSelector,
    target_dte: i32,
) -> Vec<TradeRow> {
    // Sort by entry_date, then by expiration for determinism
    trades.sort_by(|a, b| {
        a.entry_date
            .cmp(&b.entry_date)
            .then(a.expiration.cmp(&b.expiration))
    });

    let mut selected = Vec::new();
    let mut i = 0;

    while i < trades.len() {
        let date = trades[i].entry_date;

        // Collect all trades for this date
        let start = i;
        while i < trades.len() && trades[i].entry_date == date {
            i += 1;
        }
        let group = &trades[start..i];

        let pick = match selector {
            TradeSelector::Nearest => group.iter().min_by_key(|t| {
                let candidate_dte = (t.expiration - t.entry_date).num_days() as i32;
                (candidate_dte - target_dte).abs()
            }),
            TradeSelector::First => group.first(),
            TradeSelector::HighestPremium => group.iter().max_by(|a, b| {
                a.entry_cost
                    .abs()
                    .partial_cmp(&b.entry_cost.abs())
                    .unwrap_or(std::cmp::Ordering::Equal)
            }),
            TradeSelector::LowestPremium => group.iter().min_by(|a, b| {
                a.entry_cost
                    .abs()
                    .partial_cmp(&b.entry_cost.abs())
                    .unwrap_or(std::cmp::Ordering::Equal)
            }),
        };

        if let Some(trade) = pick {
            selected.push(trade.clone());
        }
    }

    selected
}

/// Early exit scanning for `Signal/MaxHold/SL/TP`.
///
/// For each trade, iterates only **trading days** (matching the event loop) and checks
/// exit conditions in priority order: Signal > `MaxHold` > SL > TP.  The earliest trigger
/// date wins; when two conditions fire on the same day, priority ordering decides.
fn apply_early_exits<S: BuildHasher>(
    mut trades: Vec<TradeRow>,
    price_table: &PriceTable,
    carry_index: &CarryIndex,
    trading_days: &[NaiveDate],
    params: &BacktestParams,
    exit_dates: Option<&HashSet<NaiveDate, S>>,
) -> Vec<TradeRow> {
    let sl_enabled = params.stop_loss.is_some();
    let tp_enabled = params.take_profit.is_some();

    for trade in &mut trades {
        let sl_threshold = params.stop_loss.map(|sl| trade.entry_cost.abs() * sl);
        let tp_threshold = params.take_profit.map(|tp| trade.entry_cost.abs() * tp);
        let max_exit = params
            .max_hold_days
            .map(|d| trade.entry_date + chrono::Duration::days(i64::from(d)));

        // Find the first trading day strictly after entry_date
        let start_idx = trading_days.partition_point(|d| *d <= trade.entry_date);

        let mut earliest_trigger: Option<(NaiveDate, ExitType)> = None;

        for &day in &trading_days[start_idx..] {
            if day > trade.exit_date {
                break;
            }

            // On the exit_date itself, only Signal can override (it has highest
            // priority in the event loop, checked before DTE/expiration exit).
            // Other triggers (MaxHold/SL/TP) have lower priority than DTE exit.
            let exit_day = day == trade.exit_date;

            // --- Signal (highest priority) ---
            if let Some(signal_dates) = exit_dates {
                if signal_dates.contains(&day) {
                    // Signal beats everything on this day and earlier non-signal triggers
                    earliest_trigger = Some((day, ExitType::Signal));
                    break; // Signal is highest priority; no earlier day can beat this
                }
            }

            // --- MaxHold ---
            // Skip on exit_day: DTE/expiration exit has higher priority
            if !exit_day {
                if let Some(max_date) = max_exit {
                    if day >= max_date {
                        // MaxHold triggers on this day — record if no earlier trigger
                        if earliest_trigger.is_none() {
                            earliest_trigger = Some((day, ExitType::MaxHold));
                        }
                        break; // MaxHold is a hard boundary
                    }
                }
            }

            // --- SL/TP (require price lookup) ---
            // Skip on exit_day: DTE/expiration exit has higher priority
            if !exit_day && (sl_enabled || tp_enabled) {
                let mut mtm = 0.0;
                let mut all_legs_have_price = true;

                for leg in &trade.legs {
                    let key = (
                        day,
                        leg.expiration,
                        OrderedFloat(leg.strike),
                        leg.option_type,
                    );
                    if let Some(snap) = price_table.get(&key) {
                        let exit_side = match leg.side {
                            Side::Long => Side::Short,
                            Side::Short => Side::Long,
                        };
                        let entry_price = pricing::fill_price(
                            leg.entry_bid,
                            leg.entry_ask,
                            leg.side,
                            &params.slippage,
                        );
                        let current_price =
                            pricing::fill_price(snap.bid, snap.ask, exit_side, &params.slippage);
                        let direction = leg.side.multiplier();
                        mtm += (current_price - entry_price)
                            * direction
                            * f64::from(leg.qty)
                            * f64::from(params.multiplier);
                    } else {
                        all_legs_have_price = false;
                    }
                }

                if all_legs_have_price {
                    if let Some(sl) = sl_threshold {
                        if mtm < -sl {
                            earliest_trigger = Some((day, ExitType::StopLoss));
                            break;
                        }
                    }
                    if let Some(tp) = tp_threshold {
                        if mtm > tp {
                            earliest_trigger = Some((day, ExitType::TakeProfit));
                            break;
                        }
                    }
                }
            }
        }

        if let Some((trigger_date, trigger_type)) = earliest_trigger {
            trade.exit_date = trigger_date;
            trade.exit_type = trigger_type;
            update_exit_prices(trade, carry_index);
        }
    }

    trades
}

/// Look up the most recent `QuoteSnapshot` for a given contract on or before `exit_date`.
///
/// Uses the carry-forward index for O(log n) lookups instead of scanning the full price table.
fn lookup_exit_snap(
    carry_index: &CarryIndex,
    exit_date: NaiveDate,
    expiration: NaiveDate,
    strike: OrderedFloat<f64>,
    option_type: OptionType,
) -> Option<&QuoteSnapshot> {
    let dates = carry_index.get(&(expiration, strike, option_type))?;
    // range(..=exit_date) gives all dates <= exit_date; last() is the most recent
    dates.range(..=exit_date).next_back().map(|(_, snap)| snap)
}

/// Update exit prices for a trade at its current `exit_date` using the price table.
/// Uses carry-forward (most recent quote on or before `exit_date`) to match the
/// event loop's behavior. Clears prices to zero if no quote is found at all,
/// avoiding lookahead bias from a previously set exit price.
fn update_exit_prices(trade: &mut TradeRow, carry_index: &CarryIndex) {
    let exit_date = trade.exit_date;
    for leg in &mut trade.legs {
        if let Some(snap) = lookup_exit_snap(
            carry_index,
            exit_date,
            leg.expiration,
            OrderedFloat(leg.strike),
            leg.option_type,
        ) {
            leg.exit_bid = snap.bid;
            leg.exit_ask = snap.ask;
        } else {
            // No price found on or before exit_date — zero out to avoid
            // retaining a stale price from a previously evaluated exit_date.
            leg.exit_bid = 0.0;
            leg.exit_ask = 0.0;
        }
    }
}

/// Filter overlapping trades: enforce `max_positions` and expiration uniqueness.
///
/// Forward scan over trades sorted by `entry_date`. The overlap check scans `selected`
/// for each trade, so it is O(n²) in the number of selected trades (bounded by `max_positions`).
fn filter_overlapping_trades(trades: Vec<TradeRow>, max_positions: i32) -> Vec<TradeRow> {
    let mut selected: Vec<TradeRow> = Vec::new();

    for trade in trades {
        // Count open positions at this trade's entry date
        let open_count = selected
            .iter()
            .filter(|t| t.exit_date > trade.entry_date)
            .count();

        if open_count >= usize::try_from(max_positions).unwrap_or(0) {
            continue;
        }

        // Check expiration uniqueness among open positions
        let has_same_exp = selected.iter().any(|t| {
            t.exit_date > trade.entry_date
                && t.expiration == trade.expiration
                && t.secondary_expiration == trade.secondary_expiration
        });

        if has_same_exp {
            continue;
        }

        selected.push(trade);
    }

    selected
}

/// Build the final trade log and equity curve from filtered trades.
fn build_outputs(
    trades: &[TradeRow],
    trading_days: &[NaiveDate],
    price_table: &PriceTable,
    carry_index: &CarryIndex,
    params: &BacktestParams,
) -> (Vec<TradeRecord>, Vec<EquityPoint>) {
    let commission = params.commission.clone().unwrap_or_default();

    // Build trade log
    let mut trade_log = Vec::with_capacity(trades.len());

    for (idx, trade) in trades.iter().enumerate() {
        let mut pnl = 0.0;
        let mut total_contracts = 0i32;

        for leg in &trade.legs {
            let leg_pl = pricing::leg_pnl(
                leg.entry_bid,
                leg.entry_ask,
                leg.exit_bid,
                leg.exit_ask,
                leg.side,
                &params.slippage,
                leg.qty,
                params.multiplier,
            );
            pnl += leg_pl;
            total_contracts += leg.qty.abs();
        }

        // Apply commission (entry + exit)
        pnl -= commission.calculate(total_contracts) * 2.0;

        let entry_dt = trade.entry_date.and_hms_opt(0, 0, 0).unwrap();
        let exit_dt = trade.exit_date.and_hms_opt(0, 0, 0).unwrap();
        let days_held = (trade.exit_date - trade.entry_date).num_days();

        trade_log.push(TradeRecord {
            trade_id: idx + 1,
            entry_datetime: entry_dt,
            exit_datetime: exit_dt,
            entry_cost: trade.entry_cost,
            exit_proceeds: trade.entry_cost + pnl,
            pnl,
            days_held,
            exit_type: trade.exit_type.clone(),
        });
    }

    // Build equity curve
    let equity_curve = build_equity_curve(
        &trade_log,
        trades,
        trading_days,
        price_table,
        carry_index,
        params,
    );

    (trade_log, equity_curve)
}

/// Build daily equity curve from trade exits and intermediate unrealized P&L.
fn build_equity_curve(
    trade_log: &[TradeRecord],
    trades: &[TradeRow],
    trading_days: &[NaiveDate],
    price_table: &PriceTable,
    _carry_index: &CarryIndex,
    params: &BacktestParams,
) -> Vec<EquityPoint> {
    let mut equity_curve = Vec::with_capacity(trading_days.len());
    let mut realized_equity = params.capital;
    let mut realized_to_date = vec![false; trade_log.len()];

    for &day in trading_days {
        // Realize trades that close on or before this day
        for (idx, record) in trade_log.iter().enumerate() {
            if !realized_to_date[idx] && record.exit_datetime.date() <= day {
                realized_equity += record.pnl;
                realized_to_date[idx] = true;
            }
        }

        // Compute unrealized P&L for open positions
        let mut unrealized = 0.0;
        for (idx, trade) in trades.iter().enumerate() {
            // Position is open if: entry_date <= day AND exit_date > day AND not yet realized
            if trade.entry_date <= day && trade.exit_date > day && !realized_to_date[idx] {
                for leg in &trade.legs {
                    let key = (
                        day,
                        leg.expiration,
                        OrderedFloat(leg.strike),
                        leg.option_type,
                    );
                    if let Some(snap) = price_table.get(&key) {
                        let exit_side = match leg.side {
                            Side::Long => Side::Short,
                            Side::Short => Side::Long,
                        };
                        let entry_price = pricing::fill_price(
                            leg.entry_bid,
                            leg.entry_ask,
                            leg.side,
                            &params.slippage,
                        );
                        let current_price =
                            pricing::fill_price(snap.bid, snap.ask, exit_side, &params.slippage);
                        let direction = leg.side.multiplier();
                        unrealized += (current_price - entry_price)
                            * direction
                            * f64::from(leg.qty)
                            * f64::from(params.multiplier);
                    }
                }
            }
        }

        equity_curve.push(EquityPoint {
            datetime: day.and_hms_opt(0, 0, 0).unwrap(),
            equity: realized_equity + unrealized,
        });
    }

    equity_curve
}
