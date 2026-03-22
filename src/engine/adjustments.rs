//! Position adjustment logic: trigger evaluation and action execution.
//!
//! Supports defensive rolls, calendar rolls, delta drift adjustments,
//! leg closes, and adding new legs to open positions.

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
    ctx: &SimContext,
    last_known: &LastKnown,
) -> bool {
    match trigger {
        AdjustmentTrigger::DefensiveRoll { loss_threshold } => {
            let mtm = mark_to_market(pos, today, ctx, last_known);
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
            let snap = ctx.price_table.get(&key).or_else(|| {
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
        | AdjustmentAction::RollToTarget { position_id, .. }
        | AdjustmentAction::Add { position_id, .. } => *position_id,
    }
}

/// Execute an adjustment action on a position.
#[allow(clippy::too_many_lines)]
pub(crate) fn execute_adjustment(
    action: &AdjustmentAction,
    pos: &mut Position,
    today: NaiveDate,
    ctx: &SimContext,
    last_known: &LastKnown,
    state: &mut SimState,
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
                        ctx,
                        last_known,
                    );
                    pos.entry_cost -= leg.entry_price
                        * leg.side.multiplier()
                        * f64::from(leg.qty)
                        * f64::from(pos.multiplier);
                    close_leg(leg, today, cp);
                }
            }
            finalize_if_all_closed(pos, today, ctx, last_known, state);
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
                        ctx,
                        last_known,
                    );
                    let old_entry_price = leg.entry_price;
                    let info = (leg.side, leg.option_type, leg.qty, leg.expiration);
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
                    ctx,
                    last_known,
                );
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
                if let Some(slot) = pos.legs.get_mut(*leg_index) {
                    *slot = new_leg;
                } else {
                    pos.legs.push(new_leg);
                }
                if pos.expiration == old_exp {
                    pos.expiration = *new_expiration;
                } else if pos.secondary_expiration == Some(old_exp) {
                    pos.secondary_expiration = Some(*new_expiration);
                }
            }
        }
        AdjustmentAction::RollToTarget {
            leg_index,
            target_delta,
            target_dte,
            ..
        } => {
            // Close the old leg, then dynamically find the best replacement
            // by scanning the price table for contracts matching the target delta/DTE.
            let roll_info = if let Some(leg) = pos.legs.get_mut(*leg_index) {
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
                        ctx,
                        last_known,
                    );
                    let old_entry_price = leg.entry_price;
                    let info = (leg.side, leg.option_type, leg.qty, leg.expiration, cp, old_entry_price);
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

            if let Some((leg_side, leg_opt_type, leg_qty, old_exp, cp, old_entry_price)) = roll_info {
                // Realize P&L for the leg being closed as part of the roll.
                let realized_pnl = (cp - old_entry_price)
                    * leg_side.multiplier()
                    * f64::from(leg_qty)
                    * f64::from(pos.multiplier);
                state.realized_equity += realized_pnl;
                // Scan the date index for available contracts on today
                if let Some(found) = find_roll_target(
                    today,
                    leg_opt_type,
                    target_delta,
                    target_dte,
                    ctx,
                    last_known,
                ) {
                    let (new_strike, new_expiration) = found;
                    let ep = lookup_fill_price(
                        new_expiration,
                        new_strike,
                        leg_opt_type,
                        leg_side,
                        today,
                        ctx,
                        last_known,
                    );
                    pos.entry_cost +=
                        ep * leg_side.multiplier() * f64::from(leg_qty) * f64::from(pos.multiplier);
                    let new_leg = PositionLeg {
                        leg_index: *leg_index,
                        side: leg_side,
                        option_type: leg_opt_type,
                        strike: new_strike,
                        expiration: new_expiration,
                        entry_price: ep,
                        qty: leg_qty,
                        closed: false,
                        close_price: None,
                        close_date: None,
                    };
                    if let Some(slot) = pos.legs.get_mut(*leg_index) {
                        *slot = new_leg;
                    } else {
                        pos.legs.push(new_leg);
                    }
                    if pos.expiration == old_exp {
                        pos.expiration = new_expiration;
                    } else if pos.secondary_expiration == Some(old_exp) {
                        pos.secondary_expiration = Some(new_expiration);
                    }
                }
                // If no target found, the old leg is closed but no replacement opens.
                // The position may finalize if all legs are now closed.
                finalize_if_all_closed(pos, today, ctx, last_known, state);
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
                ctx,
                last_known,
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
            pos.entry_cost += ep * side.multiplier() * f64::from(*qty) * f64::from(pos.multiplier);
        }
    }
}

/// Scan the price table for the best roll target on `today`: a contract matching
/// the given option type within the DTE and delta range, closest to `target_delta.target`.
///
/// Returns `(strike, expiration)` of the best candidate, or `None` if nothing qualifies.
fn find_roll_target(
    today: NaiveDate,
    option_type: OptionType,
    target_delta: &TargetRange,
    target_dte: &DteRange,
    ctx: &SimContext,
    last_known: &LastKnown,
) -> Option<(f64, NaiveDate)> {
    let keys = ctx.date_index.get(&today)?;

    let mut best: Option<(f64, NaiveDate, f64)> = None; // (strike, exp, delta_dist)

    for key in keys {
        // key = (quote_date, expiration, strike, option_type)
        if key.3 != option_type {
            continue;
        }

        let dte = (key.1 - today).num_days() as i32;
        if dte < target_dte.min || dte > target_dte.max {
            continue;
        }

        let Some(snap) = ctx
            .price_table
            .get(key)
            .or_else(|| last_known.get(&(key.1, key.2, key.3)))
        else {
            continue;
        };

        // Filter by valid quotes (bid/ask > 0)
        if snap.bid <= 0.0 || snap.ask <= 0.0 {
            continue;
        }

        let abs_delta = snap.delta.abs();
        if abs_delta < target_delta.min || abs_delta > target_delta.max {
            continue;
        }

        let delta_dist = (abs_delta - target_delta.target).abs();

        // Among near-equal delta distances, prefer the DTE closest to target
        let is_better = match &best {
            None => true,
            Some((_, _, best_dist)) => {
                if delta_dist < *best_dist - f64::EPSILON {
                    true
                } else if (delta_dist - *best_dist).abs() < f64::EPSILON {
                    // Tiebreak on DTE proximity
                    (dte - target_dte.target).abs()
                        < ((best.as_ref().unwrap().1 - today).num_days() as i32 - target_dte.target)
                            .abs()
                } else {
                    false
                }
            }
        };

        if is_better {
            best = Some((key.2.into_inner(), key.1, delta_dist));
        }
    }

    best.map(|(strike, exp, _)| (strike, exp))
}

/// If all legs of a position are closed, mark the position as closed with Adjustment exit.
fn finalize_if_all_closed(
    pos: &mut Position,
    today: NaiveDate,
    ctx: &SimContext,
    last_known: &LastKnown,
    state: &mut SimState,
) {
    if !pos.legs.iter().all(|l| l.closed) {
        return;
    }
    let mut pnl = mark_to_market(pos, today, ctx, last_known);
    let total_contracts: i32 = pos.legs.iter().map(|l| l.qty.abs()).sum();
    let commission = ctx.params.commission.clone().unwrap_or_default();
    pnl -= commission.calculate(total_contracts) * 2.0;

    state.realized_equity += pnl;
    pos.status = PositionStatus::Closed(ExitType::Adjustment);

    state.trade_id += 1;
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
    state.trade_log.push(TradeRecord::new(
        state.trade_id,
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
pub(crate) fn check_and_apply_adjustments(
    positions: &mut [Position],
    today: NaiveDate,
    ctx: &SimContext,
    last_known: &LastKnown,
    state: &mut SimState,
) {
    for pos in positions.iter_mut() {
        if !matches!(pos.status, PositionStatus::Open) {
            continue;
        }
        for rule in &ctx.params.adjustment_rules {
            let target_id = action_position_id(&rule.action);
            if target_id != 0 && target_id != pos.id {
                continue;
            }
            if !trigger_fires(&rule.trigger, pos, today, ctx, last_known) {
                continue;
            }
            execute_adjustment(&rule.action, pos, today, ctx, last_known, state);
            break;
        }
    }
}
