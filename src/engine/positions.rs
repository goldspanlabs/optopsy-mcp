//! Position lifecycle management: opening, closing, mark-to-market,
//! candidate selection, delta computation, and last-known price caching.

use std::collections::HashMap;

use chrono::NaiveDate;
use ordered_float::OrderedFloat;

use super::pricing;
#[allow(clippy::wildcard_imports)]
use super::types::*;

/// Create a new position from an entry candidate.
pub(crate) fn open_position(
    candidate: &EntryCandidate,
    date: NaiveDate,
    strategy_def: &StrategyDef,
    params: &BacktestParams,
    id: usize,
) -> Position {
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

        let contracts = leg_def.qty * params.quantity;
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

    Position {
        id,
        entry_date: date,
        expiration: candidate.expiration,
        secondary_expiration: candidate.secondary_expiration,
        legs,
        entry_cost,
        quantity: params.quantity,
        multiplier: params.multiplier,
        status: PositionStatus::Open,
    }
}

/// Close a position, setting leg close prices from current market.
/// Returns realized P&L for the position.
pub(crate) fn close_position(
    position: &mut Position,
    date: NaiveDate,
    price_table: &PriceTable,
    last_known: &HashMap<(NaiveDate, OrderedFloat<f64>, OptionType), QuoteSnapshot>,
    slippage: &Slippage,
    commission: &Commission,
    exit_type: ExitType,
) -> f64 {
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

    // Apply commission (entry + exit) on all legs including adjustment-closed ones
    pnl -= commission.calculate(total_contracts) * 2.0;

    position.status = PositionStatus::Closed(exit_type);
    pnl
}

/// Calculate unrealized P&L for a position at current market prices.
#[allow(clippy::implicit_hasher)]
pub fn mark_to_market(
    position: &Position,
    date: NaiveDate,
    price_table: &PriceTable,
    last_known: &HashMap<(NaiveDate, OrderedFloat<f64>, OptionType), QuoteSnapshot>,
    slippage: &Slippage,
    multiplier: i32,
) -> f64 {
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

    mtm
}

/// Compute the current signed net position delta from live option quotes.
/// Returns sum of (delta × `side_multiplier` × qty) for all open legs.
/// Falls back to the last-known quote when no live price exists.
pub(crate) fn compute_position_net_delta(
    position: &Position,
    today: NaiveDate,
    price_table: &PriceTable,
    last_known: &HashMap<(NaiveDate, OrderedFloat<f64>, OptionType), QuoteSnapshot>,
) -> f64 {
    let mut net_delta = 0.0;
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
        let snap = price_table.get(&key).or_else(|| {
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
#[allow(clippy::too_many_arguments)]
pub(crate) fn lookup_fill_price(
    leg_exp: NaiveDate,
    leg_strike: f64,
    leg_opt_type: OptionType,
    fill_side: Side,
    today: NaiveDate,
    price_table: &PriceTable,
    last_known: &HashMap<(NaiveDate, OrderedFloat<f64>, OptionType), QuoteSnapshot>,
    slippage: &Slippage,
) -> f64 {
    let key = (today, leg_exp, OrderedFloat(leg_strike), leg_opt_type);
    let snap = price_table
        .get(&key)
        .or_else(|| last_known.get(&(leg_exp, OrderedFloat(leg_strike), leg_opt_type)));
    snap.map_or(0.0, |s| {
        pricing::fill_price(s.bid, s.ask, fill_side, slippage)
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

        // Day 1: entry day
        table.insert(
            (d1, exp, OrderedFloat(strike), OptionType::Call),
            QuoteSnapshot {
                bid: 5.0,
                ask: 5.50,
                delta: 0.50,
            },
        );
        // Day 2: mid-trade
        table.insert(
            (d2, exp, OrderedFloat(strike), OptionType::Call),
            QuoteSnapshot {
                bid: 3.0,
                ask: 3.50,
                delta: 0.35,
            },
        );
        // Day 3: near exit
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

    #[test]
    fn mark_to_market_long_call() {
        let (table, _, _) = make_price_table_simple();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 22).unwrap();
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let last_known = HashMap::new();

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
                entry_price: 5.25, // mid of 5.0/5.50
                qty: 1,
                closed: false,
                close_price: None,
                close_date: None,
            }],
            entry_cost: 525.0, // 5.25 * 1 * 100
            quantity: 1,
            multiplier: 100,
            status: PositionStatus::Open,
        };

        let mtm = mark_to_market(&position, d2, &table, &last_known, &Slippage::Mid, 100);
        // Long call: entered at 5.25, current mid = 3.25
        // MTM = (3.25 - 5.25) * 1.0 * 1 * 100 = -200.0
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

        let last_known = HashMap::new();
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
                entry_price: 4.25, // mid of 4.0/4.50
                qty: 1,
                closed: false,
                close_price: None,
                close_date: None,
            }],
            entry_cost: -425.0, // short receives premium
            quantity: 1,
            multiplier: 100,
            status: PositionStatus::Open,
        };

        let mtm = mark_to_market(&position, d2, &table, &last_known, &Slippage::Mid, 100);
        // Short put: sold at 4.25, current mid = 3.25 (to buy back)
        // MTM = (3.25 - 4.25) * (-1.0) * 1 * 100 = +100.0
        assert!((mtm - 100.0).abs() < 1e-10, "MTM was {mtm}");
    }

    #[test]
    fn close_position_records_fills() {
        let (table, _, _) = make_price_table_simple();
        let d3 = NaiveDate::from_ymd_opt(2024, 1, 29).unwrap();
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let last_known = HashMap::new();
        let commission = Commission::default();

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
        };

        let pnl = close_position(
            &mut position,
            d3,
            &table,
            &last_known,
            &Slippage::Mid,
            &commission,
            ExitType::DteExit,
        );

        assert!(position.legs[0].closed);
        assert!(position.legs[0].close_price.is_some());
        assert_eq!(position.legs[0].close_date, Some(d3));
        // Close at mid of 2.0/2.50 = 2.25
        // PnL = (2.25 - 5.25) * 1.0 * 1 * 100 = -300
        assert!((pnl - (-300.0)).abs() < 1e-10, "PnL was {pnl}");
    }
}
