//! Position adjustment logic: trigger evaluation and action execution.
//!
//! Supports defensive rolls, calendar rolls, delta drift adjustments,
//! leg closes, and adding new legs to open positions.

use std::collections::HashMap;

use chrono::NaiveDate;
use ordered_float::OrderedFloat;

use super::positions::{close_leg, lookup_fill_price, mark_to_market};
#[allow(clippy::wildcard_imports)]
use super::types::*;

/// Check whether an adjustment trigger fires for a position.
pub(crate) fn trigger_fires(
    trigger: &AdjustmentTrigger,
    pos: &Position,
    today: NaiveDate,
    price_table: &PriceTable,
    last_known: &HashMap<(NaiveDate, OrderedFloat<f64>, OptionType), QuoteSnapshot>,
    slippage: &Slippage,
    multiplier: i32,
) -> bool {
    match trigger {
        AdjustmentTrigger::DefensiveRoll { loss_threshold } => {
            let mtm = mark_to_market(
                pos,
                today,
                price_table,
                last_known,
                slippage,
                multiplier,
                None,
            );
            mtm < -(loss_threshold * pos.entry_cost.abs())
        }
        AdjustmentTrigger::CalendarRoll { dte_trigger, .. } => {
            (pos.expiration - today).num_days() <= i64::from(*dte_trigger)
        }
        AdjustmentTrigger::DeltaDrift {
            leg_index,
            max_delta,
        } => pos.legs.get(*leg_index).is_some_and(|leg| {
            if leg.closed {
                return false;
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
            snap.is_some_and(|s| s.delta.abs() > *max_delta)
        }),
    }
}

/// Returns the `position_id` encoded in an `AdjustmentAction`.
/// A value of `0` is treated as a wildcard (matches any position).
pub(crate) fn action_position_id(action: &AdjustmentAction) -> usize {
    match action {
        AdjustmentAction::Close { position_id, .. }
        | AdjustmentAction::Roll { position_id, .. }
        | AdjustmentAction::Add { position_id, .. } => *position_id,
    }
}

/// Execute an adjustment action on a position.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) fn execute_adjustment(
    action: &AdjustmentAction,
    pos: &mut Position,
    today: NaiveDate,
    price_table: &PriceTable,
    last_known: &HashMap<(NaiveDate, OrderedFloat<f64>, OptionType), QuoteSnapshot>,
    params: &BacktestParams,
    trade_log: &mut Vec<TradeRecord>,
    trade_id: &mut usize,
    realized_equity: &mut f64,
) {
    match action {
        AdjustmentAction::Close { leg_index, .. } => {
            if let Some(leg) = pos.legs.get_mut(*leg_index) {
                if !leg.closed {
                    let exit_side = leg.side.flip();
                    let cp = lookup_fill_price(
                        leg.expiration,
                        leg.strike,
                        leg.option_type,
                        exit_side,
                        today,
                        price_table,
                        last_known,
                        &params.slippage,
                    );
                    // Update entry_cost to reflect the closed leg's cashflow
                    pos.entry_cost -= leg.entry_price
                        * leg.side.multiplier()
                        * f64::from(leg.qty)
                        * f64::from(pos.multiplier);
                    close_leg(leg, today, cp);
                }
            }
            finalize_if_all_closed(
                pos,
                today,
                price_table,
                last_known,
                params,
                trade_log,
                trade_id,
                realized_equity,
            );
        }
        AdjustmentAction::Roll {
            leg_index,
            new_strike,
            new_expiration,
            ..
        } => {
            let new_leg_info = if let Some(leg) = pos.legs.get_mut(*leg_index) {
                if leg.closed {
                    None
                } else {
                    let exit_side = leg.side.flip();
                    let cp = lookup_fill_price(
                        leg.expiration,
                        leg.strike,
                        leg.option_type,
                        exit_side,
                        today,
                        price_table,
                        last_known,
                        &params.slippage,
                    );
                    let old_entry_price = leg.entry_price;
                    let info = (leg.side, leg.option_type, leg.qty, leg.expiration);
                    // Remove old leg's cost basis from entry_cost
                    pos.entry_cost -= old_entry_price
                        * leg.side.multiplier()
                        * f64::from(leg.qty)
                        * f64::from(pos.multiplier);
                    close_leg(leg, today, cp);
                    Some(info)
                }
            } else {
                None
            };

            if let Some((leg_side, leg_opt_type, leg_qty, old_exp)) = new_leg_info {
                let ep = lookup_fill_price(
                    *new_expiration,
                    *new_strike,
                    leg_opt_type,
                    leg_side,
                    today,
                    price_table,
                    last_known,
                    &params.slippage,
                );
                // Update entry_cost: add the new leg's cost basis
                pos.entry_cost +=
                    ep * leg_side.multiplier() * f64::from(leg_qty) * f64::from(pos.multiplier);
                let new_leg = PositionLeg {
                    leg_index: *leg_index,
                    side: leg_side,
                    option_type: leg_opt_type,
                    strike: *new_strike,
                    expiration: *new_expiration,
                    entry_price: ep,
                    qty: leg_qty,
                    closed: false,
                    close_price: None,
                    close_date: None,
                };
                // Replace in-place so DeltaDrift and other index-based triggers
                // continue to operate on the rolled leg.
                if let Some(slot) = pos.legs.get_mut(*leg_index) {
                    *slot = new_leg;
                } else {
                    pos.legs.push(new_leg);
                }
                // Keep position-level expiration fields in sync so that DTE-exit
                // and expiration-exit logic sees the updated expiration after a roll.
                // Note: for multi-leg positions where legs may carry different expirations,
                // only the primary and secondary expiration fields are updated here.
                // If old_exp matches neither field the rolled leg's expiration is tracked
                // solely through the leg itself, which is correct for single-expiration
                // strategies and standard calendar/diagonal rolls.
                if pos.expiration == old_exp {
                    pos.expiration = *new_expiration;
                } else if pos.secondary_expiration == Some(old_exp) {
                    pos.secondary_expiration = Some(*new_expiration);
                }
            }
        }
        AdjustmentAction::Add {
            leg: cand_leg,
            side,
            qty,
            ..
        } => {
            let ep = lookup_fill_price(
                cand_leg.expiration,
                cand_leg.strike,
                cand_leg.option_type,
                *side,
                today,
                price_table,
                last_known,
                &params.slippage,
            );
            pos.legs.push(PositionLeg {
                leg_index: pos.legs.len(),
                side: *side,
                option_type: cand_leg.option_type,
                strike: cand_leg.strike,
                expiration: cand_leg.expiration,
                entry_price: ep,
                qty: *qty,
                closed: false,
                close_price: None,
                close_date: None,
            });
            // Update entry_cost so SL/TP thresholds reflect the new cost basis
            pos.entry_cost += ep * side.multiplier() * f64::from(*qty) * f64::from(pos.multiplier);
        }
    }
}

/// If all legs of a position are closed, mark the position as closed with Adjustment exit.
#[allow(clippy::too_many_arguments)]
fn finalize_if_all_closed(
    pos: &mut Position,
    today: NaiveDate,
    price_table: &PriceTable,
    last_known: &HashMap<(NaiveDate, OrderedFloat<f64>, OptionType), QuoteSnapshot>,
    params: &BacktestParams,
    trade_log: &mut Vec<TradeRecord>,
    trade_id: &mut usize,
    realized_equity: &mut f64,
) {
    if !pos.legs.iter().all(|l| l.closed) {
        return;
    }
    let mut pnl = mark_to_market(
        pos,
        today,
        price_table,
        last_known,
        &params.slippage,
        pos.multiplier,
        None,
    );
    // Apply commission consistently with normal exits
    let total_contracts: i32 = pos.legs.iter().map(|l| l.qty.abs()).sum();
    let commission = params.commission.clone().unwrap_or_default();
    pnl -= commission.calculate(total_contracts) * 2.0;

    *realized_equity += pnl;
    pos.status = PositionStatus::Closed(ExitType::Adjustment);

    *trade_id += 1;
    let leg_details: Vec<LegDetail> = pos
        .legs
        .iter()
        .map(|l| LegDetail {
            side: l.side,
            option_type: l.option_type,
            strike: l.strike,
            expiration: l.expiration.to_string(),
            entry_price: l.entry_price,
            exit_price: l.close_price,
            qty: l.qty,
        })
        .collect();
    trade_log.push(TradeRecord::new(
        *trade_id,
        pos.entry_date
            .and_hms_opt(0, 0, 0)
            .expect("and_hms_opt(0,0,0) should never fail"),
        today
            .and_hms_opt(0, 0, 0)
            .expect("and_hms_opt(0,0,0) should never fail"),
        pos.entry_cost,
        pos.entry_cost + pnl,
        pnl,
        (today - pos.entry_date).num_days(),
        ExitType::Adjustment,
        leg_details,
    ));
}

/// Check adjustment rules against open positions and apply the first matching rule per position.
/// Runs between exit checks and new entries.
#[allow(clippy::too_many_arguments)]
pub(crate) fn check_and_apply_adjustments(
    positions: &mut [Position],
    today: NaiveDate,
    price_table: &PriceTable,
    last_known: &HashMap<(NaiveDate, OrderedFloat<f64>, OptionType), QuoteSnapshot>,
    params: &BacktestParams,
    trade_log: &mut Vec<TradeRecord>,
    trade_id: &mut usize,
    realized_equity: &mut f64,
) {
    for pos in positions.iter_mut() {
        if !matches!(pos.status, PositionStatus::Open) {
            continue;
        }
        for rule in &params.adjustment_rules {
            // Skip this rule if it targets a specific position that isn't the current one.
            // position_id == 0 is the wildcard (matches all positions).
            let target_id = action_position_id(&rule.action);
            if target_id != 0 && target_id != pos.id {
                continue;
            }
            if !trigger_fires(
                &rule.trigger,
                pos,
                today,
                price_table,
                last_known,
                &params.slippage,
                params.multiplier,
            ) {
                continue;
            }
            execute_adjustment(
                &rule.action,
                pos,
                today,
                price_table,
                last_known,
                params,
                trade_log,
                trade_id,
                realized_equity,
            );
            break; // First matching rule wins per position
        }
    }
}
