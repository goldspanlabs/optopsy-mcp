//! Position lifecycle management: opening, closing, mark-to-market,
//! candidate selection, delta computation, and last-known price caching.

use std::collections::HashMap;

use chrono::NaiveDate;
use ordered_float::OrderedFloat;

use super::pricing;
#[allow(clippy::wildcard_imports)]
use super::types::*;

/// Create a new position from an entry candidate.
///
/// `effective_quantity` overrides `params.quantity` when dynamic sizing is active.
/// Returns `None` if the strategy has a stock leg but no OHLCV price is available for the entry date.
pub(crate) fn open_position(
    candidate: &EntryCandidate,
    date: NaiveDate,
    ctx: &SimContext,
    id: usize,
    effective_quantity: Option<i32>,
) -> Option<Position> {
    let params = ctx.params;
    let strategy_def = ctx.strategy_def;
    let ohlcv_closes = ctx.ohlcv_closes;

    let qty = effective_quantity.unwrap_or(params.quantity);
    let mut legs = Vec::new();
    let mut entry_cost = 0.0;

    for (i, (cand_leg, leg_def)) in candidate
        .legs
        .iter()
        .zip(strategy_def.legs.iter())
        .enumerate()
    {
        let entry_price =
            pricing::fill_price(cand_leg.bid, cand_leg.ask, leg_def.side, &params.slippage);

        let contracts = leg_def.qty * qty;
        entry_cost += entry_price
            * f64::from(contracts)
            * f64::from(params.multiplier)
            * leg_def.side.multiplier();

        legs.push(PositionLeg {
            leg_index: i,
            side: leg_def.side,
            option_type: leg_def.option_type,
            strike: cand_leg.strike,
            expiration: cand_leg.expiration,
            entry_price,
            qty: contracts,
            closed: false,
            close_price: None,
            close_date: None,
        });
    }

    // Stock leg: look up OHLCV close price on the entry date
    let stock_entry_price = if strategy_def.has_stock_leg {
        let stock_price = ohlcv_closes.and_then(|c| c.get(&date).copied())?;
        // Long stock cost: stock_price × qty × multiplier (100 shares per contract)
        let shares = f64::from(qty) * f64::from(params.multiplier);
        entry_cost += stock_price * shares;
        Some(stock_price)
    } else {
        None
    };

    Some(Position {
        id,
        entry_date: date,
        expiration: candidate.expiration,
        secondary_expiration: candidate.secondary_expiration,
        legs,
        entry_cost,
        quantity: qty,
        multiplier: params.multiplier,
        status: PositionStatus::Open,
        stock_entry_price,
    })
}

/// Close a position, setting leg close prices from current market.
/// Returns realized P&L for the position.
pub(crate) fn close_position(
    position: &mut Position,
    date: NaiveDate,
    ctx: &SimContext,
    last_known: &LastKnown,
    exit_type: ExitType,
) -> f64 {
    let price_table = ctx.price_table;
    let slippage = &ctx.params.slippage;
    let commission = ctx.params.commission.clone().unwrap_or_default();
    let ohlcv_closes = ctx.ohlcv_closes;

    let mut pnl = 0.0;
    let mut total_contracts = 0i32;

    for leg in &mut position.legs {
        // Count all contracts for commission (including previously closed adjustment legs)
        total_contracts += leg.qty.abs();

        if leg.closed {
            continue;
        }

        let key = (
            date,
            leg.expiration,
            OrderedFloat(leg.strike),
            leg.option_type,
        );

        let snapshot = price_table.get(&key).or_else(|| {
            last_known.get(&(leg.expiration, OrderedFloat(leg.strike), leg.option_type))
        });

        let exit_side = match leg.side {
            Side::Long => Side::Short,
            Side::Short => Side::Long,
        };

        let close_price = if let Some(snap) = snapshot {
            pricing::fill_price(snap.bid, snap.ask, exit_side, slippage)
        } else {
            // No price available — assume worthless at expiration
            0.0
        };

        let direction = leg.side.multiplier();
        pnl += (close_price - leg.entry_price)
            * direction
            * f64::from(leg.qty)
            * f64::from(position.multiplier);

        leg.closed = true;
        leg.close_price = Some(close_price);
        leg.close_date = Some(date);
    }

    // Stock leg P&L: carry-forward using range(..=date).next_back()
    if let Some(entry_price) = position.stock_entry_price {
        let exit_price = ohlcv_closes
            .and_then(|c| c.range(..=date).next_back().map(|(_, &v)| v))
            .unwrap_or(entry_price); // fallback: flat P&L
        let shares = f64::from(position.quantity) * f64::from(position.multiplier);
        pnl += (exit_price - entry_price) * shares;
    }

    // Apply commission (entry + exit) on all legs including adjustment-closed ones
    pnl -= commission.calculate(total_contracts) * 2.0;

    position.status = PositionStatus::Closed(exit_type);
    pnl
}

/// Calculate unrealized P&L for a position at current market prices.
pub fn mark_to_market(
    position: &Position,
    date: NaiveDate,
    ctx: &SimContext,
    last_known: &LastKnown,
) -> f64 {
    let price_table = ctx.price_table;
    let slippage = &ctx.params.slippage;
    let multiplier = ctx.params.multiplier;
    let ohlcv_closes = ctx.ohlcv_closes;

    let mut mtm = 0.0;

    for leg in &position.legs {
        if leg.closed {
            // Use actual close price for closed legs
            if let Some(close_price) = leg.close_price {
                let exit_side = match leg.side {
                    Side::Long => Side::Short,
                    Side::Short => Side::Long,
                };
                let direction = leg.side.multiplier();
                mtm += (close_price - leg.entry_price)
                    * direction
                    * f64::from(leg.qty)
                    * f64::from(multiplier);
                let _ = exit_side; // side used for fill price was already applied
            }
            continue;
        }

        let key = (
            date,
            leg.expiration,
            OrderedFloat(leg.strike),
            leg.option_type,
        );

        let snapshot = price_table.get(&key).or_else(|| {
            last_known.get(&(leg.expiration, OrderedFloat(leg.strike), leg.option_type))
        });

        if let Some(snap) = snapshot {
            // To close: long sells (use Short fill), short buys (use Long fill)
            let exit_side = match leg.side {
                Side::Long => Side::Short,
                Side::Short => Side::Long,
            };
            let current_price = pricing::fill_price(snap.bid, snap.ask, exit_side, slippage);
            let direction = leg.side.multiplier();
            mtm += (current_price - leg.entry_price)
                * direction
                * f64::from(leg.qty)
                * f64::from(multiplier);
        }
        // If no price found, MTM contribution is 0 (conservative)
    }

    // Stock leg unrealized P&L
    if let Some(entry_price) = position.stock_entry_price {
        let current_price = ohlcv_closes
            .and_then(|c| c.range(..=date).next_back().map(|(_, &v)| v))
            .unwrap_or(entry_price);
        let shares = f64::from(position.quantity) * f64::from(multiplier);
        mtm += (current_price - entry_price) * shares;
    }

    mtm
}

/// Compute the current signed net position delta from live option quotes.
/// Returns sum of (delta × `side_multiplier` × qty) for all open legs,
/// plus +1.0 per contract for stock-leg strategies (e.g. covered call).
/// Falls back to the last-known quote when no live price exists.
pub(crate) fn compute_position_net_delta(
    position: &Position,
    today: NaiveDate,
    ctx: &SimContext,
    last_known: &LastKnown,
) -> f64 {
    let mut net_delta = 0.0;

    // Stock leg contributes +1.0 delta per contract (long 100 shares = 1.0 delta)
    if position.stock_entry_price.is_some() {
        net_delta += f64::from(position.quantity);
    }

    for leg in &position.legs {
        if leg.closed {
            continue;
        }
        let key = (
            today,
            leg.expiration,
            OrderedFloat(leg.strike),
            leg.option_type,
        );
        let snap = ctx.price_table.get(&key).or_else(|| {
            last_known.get(&(leg.expiration, OrderedFloat(leg.strike), leg.option_type))
        });
        if let Some(s) = snap {
            net_delta += s.delta * leg.side.multiplier() * f64::from(leg.qty);
        }
    }
    net_delta
}

/// Select the best candidate based on `TradeSelector`.
pub(crate) fn select_candidate<'a>(
    candidates: &[&'a EntryCandidate],
    selector: &TradeSelector,
    target_dte: i32,
) -> Option<&'a EntryCandidate> {
    if candidates.is_empty() {
        return None;
    }

    match selector {
        TradeSelector::Nearest => candidates
            .iter()
            .min_by_key(|c| {
                let dte = (c.expiration - c.entry_date).num_days() as i32;
                (dte - target_dte).abs()
            })
            .copied(),
        TradeSelector::First => candidates.first().copied(),
        TradeSelector::HighestPremium => candidates
            .iter()
            .max_by(|a, b| {
                a.net_premium
                    .abs()
                    .partial_cmp(&b.net_premium.abs())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .copied(),
        TradeSelector::LowestPremium => candidates
            .iter()
            .min_by(|a, b| {
                a.net_premium
                    .abs()
                    .partial_cmp(&b.net_premium.abs())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .copied(),
    }
}

/// Look up the fill price for a leg from the price table or last-known cache.
pub(crate) fn lookup_fill_price(
    leg_exp: NaiveDate,
    leg_strike: f64,
    leg_opt_type: OptionType,
    fill_side: Side,
    today: NaiveDate,
    ctx: &SimContext,
    last_known: &LastKnown,
) -> f64 {
    let key = (today, leg_exp, OrderedFloat(leg_strike), leg_opt_type);
    let snap = ctx
        .price_table
        .get(&key)
        .or_else(|| last_known.get(&(leg_exp, OrderedFloat(leg_strike), leg_opt_type)));
    snap.map_or(0.0, |s| {
        pricing::fill_price(s.bid, s.ask, fill_side, &ctx.params.slippage)
    })
}

/// Close a single leg of a position by setting its close price/date.
pub(crate) fn close_leg(leg: &mut PositionLeg, today: NaiveDate, close_price: f64) {
    leg.closed = true;
    leg.close_price = Some(close_price);
    leg.close_date = Some(today);
}

/// Update the last-known price cache for carry-forward on gaps.
pub(crate) fn update_last_known(
    price_table: &PriceTable,
    date_index: &DateIndex,
    today: NaiveDate,
    last_known: &mut HashMap<(NaiveDate, OrderedFloat<f64>, OptionType), QuoteSnapshot>,
) {
    if let Some(keys) = date_index.get(&today) {
        for key in keys {
            if let Some(snap) = price_table.get(key) {
                let carry_key = (key.1, key.2, key.3);
                last_known.insert(carry_key, snap.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::price_table::build_date_index;

    fn make_price_table_simple() -> (PriceTable, Vec<NaiveDate>, DateIndex) {
        let mut table = PriceTable::with_hasher(rustc_hash::FxBuildHasher);
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 22).unwrap();
        let d3 = NaiveDate::from_ymd_opt(2024, 1, 29).unwrap();
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let strike = 100.0;

        table.insert(
            (d1, exp, OrderedFloat(strike), OptionType::Call),
            QuoteSnapshot {
                bid: 5.0,
                ask: 5.50,
                delta: 0.50,
            },
        );
        table.insert(
            (d2, exp, OrderedFloat(strike), OptionType::Call),
            QuoteSnapshot {
                bid: 3.0,
                ask: 3.50,
                delta: 0.35,
            },
        );
        table.insert(
            (d3, exp, OrderedFloat(strike), OptionType::Call),
            QuoteSnapshot {
                bid: 2.0,
                ask: 2.50,
                delta: 0.25,
            },
        );

        let days = vec![d1, d2, d3];
        let date_index = build_date_index(&table);
        (table, days, date_index)
    }

    fn make_test_params(slippage: Slippage, multiplier: i32) -> BacktestParams {
        BacktestParams {
            strategy: "test".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.30,
                min: 0.20,
                max: 0.40,
            }],
            entry_dte: DteRange {
                target: 45,
                min: 10,
                max: 60,
            },
            exit_dte: 5,
            slippage,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 100_000.0,
            quantity: 1,
            sizing: None,
            multiplier,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: std::collections::HashMap::new(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: None,
        }
    }

    fn make_test_strategy_def() -> StrategyDef {
        StrategyDef {
            name: "long_call".to_string(),
            category: "singles".to_string(),
            description: "Long call option".to_string(),
            legs: vec![LegDef {
                side: Side::Long,
                option_type: OptionType::Call,
                delta: TargetRange {
                    target: 0.50,
                    min: 0.30,
                    max: 0.70,
                },
                qty: 1,
                expiration_cycle: ExpirationCycle::Primary,
            }],
            strict_strike_order: true,
            direction: Direction::Bullish,
            has_stock_leg: false,
        }
    }

    fn make_covered_call_def() -> StrategyDef {
        StrategyDef {
            name: "covered_call".to_string(),
            category: "singles".to_string(),
            description: "Covered call".to_string(),
            legs: vec![LegDef {
                side: Side::Short,
                option_type: OptionType::Call,
                delta: TargetRange {
                    target: 0.30,
                    min: 0.20,
                    max: 0.40,
                },
                qty: 1,
                expiration_cycle: ExpirationCycle::Primary,
            }],
            strict_strike_order: true,
            direction: Direction::Bullish,
            has_stock_leg: true,
        }
    }

    fn make_short_call_def() -> StrategyDef {
        StrategyDef {
            name: "short_call".to_string(),
            category: "singles".to_string(),
            description: "Short call".to_string(),
            legs: vec![LegDef {
                side: Side::Short,
                option_type: OptionType::Call,
                delta: TargetRange {
                    target: 0.30,
                    min: 0.20,
                    max: 0.40,
                },
                qty: 1,
                expiration_cycle: ExpirationCycle::Primary,
            }],
            strict_strike_order: true,
            direction: Direction::Bearish,
            has_stock_leg: false,
        }
    }

    #[test]
    fn mark_to_market_long_call() {
        let (table, _, _) = make_price_table_simple();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 22).unwrap();
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let last_known = LastKnown::new();
        let params = make_test_params(Slippage::Mid, 100);
        let sd = make_test_strategy_def();
        let empty_idx = DateIndex::new();
        let ctx = SimContext {
            price_table: &table,
            date_index: &empty_idx,
            params: &params,
            strategy_def: &sd,
            ohlcv_closes: None,
        };
        let position = Position {
            id: 1,
            entry_date: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            expiration: exp,
            secondary_expiration: None,
            legs: vec![PositionLeg {
                leg_index: 0,
                side: Side::Long,
                option_type: OptionType::Call,
                strike: 100.0,
                expiration: exp,
                entry_price: 5.25,
                qty: 1,
                closed: false,
                close_price: None,
                close_date: None,
            }],
            entry_cost: 525.0,
            quantity: 1,
            multiplier: 100,
            status: PositionStatus::Open,
            stock_entry_price: None,
        };
        let mtm = mark_to_market(&position, d2, &ctx, &last_known);
        assert!((mtm - (-200.0)).abs() < 1e-10, "MTM was {mtm}");
    }

    #[test]
    fn mark_to_market_short_put() {
        let mut table = PriceTable::with_hasher(rustc_hash::FxBuildHasher);
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 22).unwrap();
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        table.insert(
            (d1, exp, OrderedFloat(100.0), OptionType::Put),
            QuoteSnapshot {
                bid: 4.0,
                ask: 4.50,
                delta: -0.40,
            },
        );
        table.insert(
            (d2, exp, OrderedFloat(100.0), OptionType::Put),
            QuoteSnapshot {
                bid: 3.0,
                ask: 3.50,
                delta: -0.30,
            },
        );
        let last_known = LastKnown::new();
        let params = make_test_params(Slippage::Mid, 100);
        let sd = make_test_strategy_def();
        let empty_idx = DateIndex::new();
        let ctx = SimContext {
            price_table: &table,
            date_index: &empty_idx,
            params: &params,
            strategy_def: &sd,
            ohlcv_closes: None,
        };
        let position = Position {
            id: 1,
            entry_date: d1,
            expiration: exp,
            secondary_expiration: None,
            legs: vec![PositionLeg {
                leg_index: 0,
                side: Side::Short,
                option_type: OptionType::Put,
                strike: 100.0,
                expiration: exp,
                entry_price: 4.25,
                qty: 1,
                closed: false,
                close_price: None,
                close_date: None,
            }],
            entry_cost: -425.0,
            quantity: 1,
            multiplier: 100,
            status: PositionStatus::Open,
            stock_entry_price: None,
        };
        let mtm = mark_to_market(&position, d2, &ctx, &last_known);
        assert!((mtm - 100.0).abs() < 1e-10, "MTM was {mtm}");
    }

    #[test]
    fn close_position_records_fills() {
        let (table, _, _) = make_price_table_simple();
        let d3 = NaiveDate::from_ymd_opt(2024, 1, 29).unwrap();
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let last_known = LastKnown::new();
        let params = make_test_params(Slippage::Mid, 100);
        let sd = make_test_strategy_def();
        let empty_idx = DateIndex::new();
        let ctx = SimContext {
            price_table: &table,
            date_index: &empty_idx,
            params: &params,
            strategy_def: &sd,
            ohlcv_closes: None,
        };
        let mut position = Position {
            id: 1,
            entry_date: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            expiration: exp,
            secondary_expiration: None,
            legs: vec![PositionLeg {
                leg_index: 0,
                side: Side::Long,
                option_type: OptionType::Call,
                strike: 100.0,
                expiration: exp,
                entry_price: 5.25,
                qty: 1,
                closed: false,
                close_price: None,
                close_date: None,
            }],
            entry_cost: 525.0,
            quantity: 1,
            multiplier: 100,
            status: PositionStatus::Open,
            stock_entry_price: None,
        };
        let pnl = close_position(&mut position, d3, &ctx, &last_known, ExitType::DteExit);
        assert!(position.legs[0].closed);
        assert!(position.legs[0].close_price.is_some());
        assert_eq!(position.legs[0].close_date, Some(d3));
        assert!((pnl - (-300.0)).abs() < 1e-10, "PnL was {pnl}");
    }

    fn make_ohlcv_closes() -> std::collections::BTreeMap<NaiveDate, f64> {
        let mut closes = std::collections::BTreeMap::new();
        closes.insert(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), 100.0);
        closes.insert(NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(), 105.0);
        closes.insert(NaiveDate::from_ymd_opt(2024, 1, 29).unwrap(), 110.0);
        closes
    }

    fn make_stock_leg_position() -> Position {
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        Position {
            id: 1,
            entry_date: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            expiration: exp,
            secondary_expiration: None,
            legs: vec![PositionLeg {
                leg_index: 0,
                side: Side::Short,
                option_type: OptionType::Call,
                strike: 100.0,
                expiration: exp,
                entry_price: 5.25,
                qty: 1,
                closed: false,
                close_price: None,
                close_date: None,
            }],
            entry_cost: 9475.0,
            quantity: 1,
            multiplier: 100,
            status: PositionStatus::Open,
            stock_entry_price: Some(100.0),
        }
    }

    #[test]
    fn mark_to_market_with_stock_leg() {
        let (table, _, _) = make_price_table_simple();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 22).unwrap();
        let last_known = LastKnown::new();
        let closes = make_ohlcv_closes();
        let position = make_stock_leg_position();
        let params = make_test_params(Slippage::Mid, 100);
        let sd = make_test_strategy_def();
        let empty_idx = DateIndex::new();
        let ctx = SimContext {
            price_table: &table,
            date_index: &empty_idx,
            params: &params,
            strategy_def: &sd,
            ohlcv_closes: Some(&closes),
        };
        let mtm = mark_to_market(&position, d2, &ctx, &last_known);
        assert!((mtm - 700.0).abs() < 1e-10, "MTM was {mtm}");
    }

    #[test]
    fn mark_to_market_stock_leg_carry_forward() {
        let (table, _, _) = make_price_table_simple();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 22).unwrap();
        let last_known = LastKnown::new();
        let mut closes = std::collections::BTreeMap::new();
        closes.insert(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), 100.0);
        let position = make_stock_leg_position();
        let params = make_test_params(Slippage::Mid, 100);
        let sd = make_test_strategy_def();
        let empty_idx = DateIndex::new();
        let ctx = SimContext {
            price_table: &table,
            date_index: &empty_idx,
            params: &params,
            strategy_def: &sd,
            ohlcv_closes: Some(&closes),
        };
        let mtm = mark_to_market(&position, d2, &ctx, &last_known);
        assert!((mtm - 200.0).abs() < 1e-10, "MTM was {mtm}");
    }

    #[test]
    fn mark_to_market_stock_leg_no_ohlcv() {
        let (table, _, _) = make_price_table_simple();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 22).unwrap();
        let last_known = LastKnown::new();
        let position = make_stock_leg_position();
        let params = make_test_params(Slippage::Mid, 100);
        let sd = make_test_strategy_def();
        let empty_idx = DateIndex::new();
        let ctx = SimContext {
            price_table: &table,
            date_index: &empty_idx,
            params: &params,
            strategy_def: &sd,
            ohlcv_closes: None,
        };
        let mtm = mark_to_market(&position, d2, &ctx, &last_known);
        assert!((mtm - 200.0).abs() < 1e-10, "MTM was {mtm}");
    }

    #[test]
    fn close_position_with_stock_leg() {
        let (table, _, _) = make_price_table_simple();
        let d3 = NaiveDate::from_ymd_opt(2024, 1, 29).unwrap();
        let last_known = LastKnown::new();
        let closes = make_ohlcv_closes();
        let mut position = make_stock_leg_position();
        let params = make_test_params(Slippage::Mid, 100);
        let sd = make_test_strategy_def();
        let empty_idx = DateIndex::new();
        let ctx = SimContext {
            price_table: &table,
            date_index: &empty_idx,
            params: &params,
            strategy_def: &sd,
            ohlcv_closes: Some(&closes),
        };
        let pnl = close_position(&mut position, d3, &ctx, &last_known, ExitType::DteExit);
        assert!((pnl - 1300.0).abs() < 1e-10, "PnL was {pnl}");
    }

    #[test]
    fn close_position_stock_leg_carry_forward() {
        let (table, _, _) = make_price_table_simple();
        let d3 = NaiveDate::from_ymd_opt(2024, 1, 29).unwrap();
        let last_known = LastKnown::new();
        let mut closes = std::collections::BTreeMap::new();
        closes.insert(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), 100.0);
        closes.insert(NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(), 108.0);
        let mut position = make_stock_leg_position();
        let params = make_test_params(Slippage::Mid, 100);
        let sd = make_test_strategy_def();
        let empty_idx = DateIndex::new();
        let ctx = SimContext {
            price_table: &table,
            date_index: &empty_idx,
            params: &params,
            strategy_def: &sd,
            ohlcv_closes: Some(&closes),
        };
        let pnl = close_position(&mut position, d3, &ctx, &last_known, ExitType::DteExit);
        assert!((pnl - 1100.0).abs() < 1e-10, "PnL was {pnl}");
    }

    #[test]
    fn net_delta_with_stock_leg() {
        // Covered call: long stock (+1.0) + short call (-0.35) = +0.65
        let (table, _, _) = make_price_table_simple();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 22).unwrap();
        let last_known = LastKnown::new();
        let params = make_test_params(Slippage::Mid, 100);
        let sd = make_test_strategy_def();
        let empty_idx = DateIndex::new();
        let ctx = SimContext {
            price_table: &table,
            date_index: &empty_idx,
            params: &params,
            strategy_def: &sd,
            ohlcv_closes: None,
        };
        let position = make_stock_leg_position(); // short call, stock_entry_price = Some(100.0)
        let nd = compute_position_net_delta(&position, d2, &ctx, &last_known);
        // Stock: +1.0, Short call delta=0.35: 0.35 * (-1) * 1 = -0.35
        // Net: 1.0 - 0.35 = 0.65
        assert!(
            (nd - 0.65).abs() < 1e-10,
            "Net delta was {nd}, expected 0.65"
        );
    }

    #[test]
    fn net_delta_without_stock_leg() {
        // Plain short call: no stock leg, just option delta
        let (table, _, _) = make_price_table_simple();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 22).unwrap();
        let last_known = LastKnown::new();
        let params = make_test_params(Slippage::Mid, 100);
        let sd = make_test_strategy_def();
        let empty_idx = DateIndex::new();
        let ctx = SimContext {
            price_table: &table,
            date_index: &empty_idx,
            params: &params,
            strategy_def: &sd,
            ohlcv_closes: None,
        };
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let position = Position {
            id: 1,
            entry_date: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            expiration: exp,
            secondary_expiration: None,
            legs: vec![PositionLeg {
                leg_index: 0,
                side: Side::Short,
                option_type: OptionType::Call,
                strike: 100.0,
                expiration: exp,
                entry_price: 5.25,
                qty: 1,
                closed: false,
                close_price: None,
                close_date: None,
            }],
            entry_cost: -525.0,
            quantity: 1,
            multiplier: 100,
            status: PositionStatus::Open,
            stock_entry_price: None, // no stock leg
        };
        let nd = compute_position_net_delta(&position, d2, &ctx, &last_known);
        // Short call delta=0.35: 0.35 * (-1) * 1 = -0.35
        assert!(
            (nd - (-0.35)).abs() < 1e-10,
            "Net delta was {nd}, expected -0.35"
        );
    }

    #[test]
    fn open_position_stock_leg_no_ohlcv_returns_none() {
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let table = PriceTable::with_hasher(rustc_hash::FxBuildHasher);
        let candidate = EntryCandidate {
            entry_date: d1,
            expiration: exp,
            secondary_expiration: None,
            legs: vec![CandidateLeg {
                option_type: OptionType::Call,
                strike: 100.0,
                expiration: exp,
                bid: 5.0,
                ask: 5.50,
                delta: 0.50,
            }],
            net_premium: -5.25,
            net_delta: -0.50,
        };
        let strategy_def = make_covered_call_def();
        let mut params = make_test_params(Slippage::Mid, 100);
        params.strategy = "covered_call".to_string();
        let empty_idx = DateIndex::new();
        let ctx = SimContext {
            price_table: &table,
            date_index: &empty_idx,
            params: &params,
            strategy_def: &strategy_def,
            ohlcv_closes: None,
        };
        let result = open_position(&candidate, d1, &ctx, 1, None);
        assert!(
            result.is_none(),
            "Should return None when no OHLCV data for stock-leg strategy"
        );
    }

    #[test]
    fn open_position_stock_leg_with_ohlcv() {
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let table = PriceTable::with_hasher(rustc_hash::FxBuildHasher);
        let candidate = EntryCandidate {
            entry_date: d1,
            expiration: exp,
            secondary_expiration: None,
            legs: vec![CandidateLeg {
                option_type: OptionType::Call,
                strike: 100.0,
                expiration: exp,
                bid: 5.0,
                ask: 5.50,
                delta: 0.50,
            }],
            net_premium: -5.25,
            net_delta: -0.50,
        };
        let strategy_def = make_covered_call_def();
        let mut params = make_test_params(Slippage::Mid, 100);
        params.strategy = "covered_call".to_string();
        let closes = make_ohlcv_closes();
        let empty_idx = DateIndex::new();
        let ctx = SimContext {
            price_table: &table,
            date_index: &empty_idx,
            params: &params,
            strategy_def: &strategy_def,
            ohlcv_closes: Some(&closes),
        };
        let result = open_position(&candidate, d1, &ctx, 1, None);
        assert!(
            result.is_some(),
            "Should return Some when OHLCV data available"
        );
        let pos = result.unwrap();
        assert_eq!(pos.stock_entry_price, Some(100.0));
        assert!(
            (pos.entry_cost - 9475.0).abs() < 1e-10,
            "entry_cost was {}",
            pos.entry_cost
        );
    }

    #[test]
    fn open_position_no_stock_leg_ignores_ohlcv() {
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let table = PriceTable::with_hasher(rustc_hash::FxBuildHasher);
        let candidate = EntryCandidate {
            entry_date: d1,
            expiration: exp,
            secondary_expiration: None,
            legs: vec![CandidateLeg {
                option_type: OptionType::Call,
                strike: 100.0,
                expiration: exp,
                bid: 5.0,
                ask: 5.50,
                delta: 0.50,
            }],
            net_premium: -5.25,
            net_delta: -0.50,
        };
        let strategy_def = make_short_call_def();
        let mut params = make_test_params(Slippage::Mid, 100);
        params.strategy = "short_call".to_string();
        let empty_idx = DateIndex::new();
        let ctx = SimContext {
            price_table: &table,
            date_index: &empty_idx,
            params: &params,
            strategy_def: &strategy_def,
            ohlcv_closes: None,
        };
        let result = open_position(&candidate, d1, &ctx, 1, None);
        assert!(
            result.is_some(),
            "Non-stock-leg strategy should always open"
        );
        let pos = result.unwrap();
        assert!(pos.stock_entry_price.is_none());
    }
}
