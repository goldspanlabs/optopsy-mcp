//! Event-driven simulation loop and entry candidate discovery.
//!
//! Builds entry candidates from the Polars filter pipeline, then runs a
//! day-by-day event loop that opens, manages, and closes positions according
//! to the configured exit rules, adjustment rules, and signal filters.

use std::collections::BTreeMap;

use anyhow::{bail, Result};
use chrono::NaiveDate;
use polars::prelude::*;

use super::adjustments::check_and_apply_adjustments;
use super::filters;
use super::positions::{
    close_position, compute_position_net_delta, open_position, select_candidate, update_last_known,
};
use super::rules;
#[allow(clippy::wildcard_imports)]
use super::types::*;
use crate::data::parquet::DATETIME_COL;

// Re-export public API from extracted modules
pub use super::positions::mark_to_market;
pub use super::price_table::build_price_table;
pub(crate) use super::price_table::extract_date_from_column;

/// Pre-extracted per-leg column references for fast row iteration.
struct LegColumns<'a> {
    strikes: &'a Float64Chunked,
    bids: &'a Float64Chunked,
    asks: &'a Float64Chunked,
    deltas: &'a Float64Chunked,
}

/// Find entry candidates from the options data, grouped by date.
/// Reuses existing Polars filter pipeline but does NOT call `match_entry_exit`.
#[allow(clippy::too_many_lines)]
pub fn find_entry_candidates(
    df: &DataFrame,
    strategy_def: &StrategyDef,
    params: &BacktestParams,
) -> Result<BTreeMap<NaiveDate, Vec<EntryCandidate>>> {
    let num_legs = strategy_def.legs.len();
    let is_multi_exp = strategy_def.is_multi_expiration();
    let base_cols: &[&str] = &["strike", "bid", "ask", "delta"];

    // Process each leg through the existing filter pipeline
    let mut leg_dfs = Vec::new();
    for (i, (leg, delta_target)) in strategy_def
        .legs
        .iter()
        .zip(params.leg_deltas.iter())
        .enumerate()
    {
        // For entry candidates, use entry_dte.min as lower bound.
        // Secondary legs get wider DTE range to find far-term expirations
        let max_dte = if leg.expiration_cycle == ExpirationCycle::Secondary {
            params.entry_dte.max * 2
        } else {
            params.entry_dte.max
        };
        // Combined filter: option type + DTE + valid quotes in a single lazy pass
        let valid = filters::filter_leg_candidates(
            df,
            leg.option_type.as_str(),
            max_dte,
            params.entry_dte.min,
            params.min_bid_ask,
        )?;
        // Apply expiration type filter before delta selection
        let exp_filtered = filters::filter_expiration_type(&valid, &params.expiration_filter)?;
        let selected = filters::select_closest_delta(&exp_filtered, delta_target)?;

        if selected.height() == 0 {
            bail!(
                "No entry candidates found for leg {} of strategy '{}'",
                i,
                params.strategy
            );
        }

        // Select only needed columns and rename with leg index
        let prepared = if is_multi_exp {
            filters::prepare_leg_for_join_multi_exp(&selected, i, base_cols, leg.expiration_cycle)?
        } else {
            filters::prepare_leg_for_join(&selected, i, base_cols)?
        };

        leg_dfs.push((prepared, leg.expiration_cycle));
    }

    // Join all legs
    let combined = filters::join_legs(&leg_dfs, is_multi_exp)?;

    if combined.height() == 0 {
        return Ok(BTreeMap::new());
    }

    // Apply strike ordering rules for multi-leg strategies
    let combined = rules::filter_strike_order(
        &combined,
        num_legs,
        strategy_def.strict_strike_order,
        if is_multi_exp {
            Some(strategy_def)
        } else {
            None
        },
    )?;

    if combined.height() == 0 {
        return Ok(BTreeMap::new());
    }

    // Convert to EntryCandidate structs grouped by date
    let quote_dates = combined.column(DATETIME_COL)?;

    // Pre-extract per-leg columns outside the row loop to avoid repeated
    // column lookups (`.column()?.f64()?`) on every row × every leg.
    let leg_columns: Vec<LegColumns<'_>> = (0..num_legs)
        .map(|i| {
            Ok(LegColumns {
                strikes: combined.column(&format!("strike_{i}"))?.f64()?,
                bids: combined.column(&format!("bid_{i}"))?.f64()?,
                asks: combined.column(&format!("ask_{i}"))?.f64()?,
                deltas: combined.column(&format!("delta_{i}"))?.f64()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    // Pre-extract expiration columns once
    let exp_primary_col = if is_multi_exp {
        Some(combined.column("expiration_primary")?)
    } else {
        None
    };
    let exp_secondary_col = if is_multi_exp {
        Some(combined.column("expiration_secondary")?)
    } else {
        None
    };
    let exp_col = if is_multi_exp {
        None
    } else {
        Some(combined.column("expiration")?)
    };

    let mut candidates: BTreeMap<NaiveDate, Vec<EntryCandidate>> = BTreeMap::new();

    for row_idx in 0..combined.height() {
        let entry_date = extract_date_from_column(quote_dates, row_idx)?;

        // Extract expiration dates using pre-extracted columns
        let (primary_exp, secondary_exp) = if is_multi_exp {
            let prim = extract_date_from_column(exp_primary_col.unwrap(), row_idx)?;
            let sec = extract_date_from_column(exp_secondary_col.unwrap(), row_idx)?;
            (prim, Some(sec))
        } else {
            let exp = extract_date_from_column(exp_col.unwrap(), row_idx)?;
            (exp, None)
        };

        let mut legs = Vec::new();
        let mut net_premium = 0.0;
        let mut net_delta = 0.0;

        let mut skip_row = false;
        for (i, leg_def) in strategy_def.legs.iter().enumerate() {
            let lc = &leg_columns[i];
            let Some(strike) = lc.strikes.get(row_idx) else {
                tracing::debug!(
                    row = row_idx,
                    leg = i,
                    "Skipping entry candidate: null strike value"
                );
                skip_row = true;
                break;
            };
            let Some(bid) = lc.bids.get(row_idx) else {
                tracing::debug!(
                    row = row_idx,
                    leg = i,
                    "Skipping entry candidate: null bid value"
                );
                skip_row = true;
                break;
            };
            let Some(ask) = lc.asks.get(row_idx) else {
                tracing::debug!(
                    row = row_idx,
                    leg = i,
                    "Skipping entry candidate: null ask value"
                );
                skip_row = true;
                break;
            };
            let Some(delta) = lc.deltas.get(row_idx) else {
                tracing::debug!(
                    row = row_idx,
                    leg = i,
                    "Skipping entry candidate: null delta value"
                );
                skip_row = true;
                break;
            };

            let mid = f64::midpoint(bid, ask);
            // Long pays mid, short receives mid
            net_premium += mid * leg_def.side.multiplier() * f64::from(leg_def.qty);
            // Net delta: delta × side_multiplier × qty
            net_delta += delta * leg_def.side.multiplier() * f64::from(leg_def.qty);

            // Set each leg's expiration based on its cycle
            let leg_exp = match leg_def.expiration_cycle {
                ExpirationCycle::Primary => primary_exp,
                ExpirationCycle::Secondary => secondary_exp.unwrap_or(primary_exp),
            };

            legs.push(CandidateLeg {
                option_type: leg_def.option_type,
                strike,
                expiration: leg_exp,
                bid,
                ask,
                delta,
            });
        }

        if skip_row {
            continue;
        }

        // Apply net premium filter
        let abs_premium = net_premium.abs();
        if let Some(min_p) = params.min_net_premium {
            if abs_premium < min_p {
                continue;
            }
        }
        if let Some(max_p) = params.max_net_premium {
            if abs_premium > max_p {
                continue;
            }
        }

        // Apply net delta filter
        if let Some(min_d) = params.min_net_delta {
            if net_delta < min_d {
                continue;
            }
        }
        if let Some(max_d) = params.max_net_delta {
            if net_delta > max_d {
                continue;
            }
        }

        candidates
            .entry(entry_date)
            .or_default()
            .push(EntryCandidate {
                entry_date,
                expiration: primary_exp,
                secondary_expiration: secondary_exp,
                legs,
                net_premium,
                net_delta,
            });
    }

    Ok(candidates)
}

/// Build a `TradeRecord` from a closed position.
#[allow(clippy::too_many_arguments)]
fn build_trade_record(
    position: &Position,
    today: NaiveDate,
    trade_id: usize,
    pnl: f64,
    exit_type: ExitType,
    sizing_active: bool,
    entry_equity: f64,
    ohlcv_closes: Option<&BTreeMap<NaiveDate, f64>>,
) -> TradeRecord {
    let entry_dt = position
        .entry_date
        .and_hms_opt(0, 0, 0)
        .expect("and_hms_opt(0,0,0) should never fail");
    let exit_dt = today
        .and_hms_opt(0, 0, 0)
        .expect("and_hms_opt(0,0,0) should never fail");
    let days_held = (today - position.entry_date).num_days();

    let leg_details: Vec<LegDetail> = position
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

    let mut record = TradeRecord::new(
        trade_id,
        entry_dt,
        exit_dt,
        position.entry_cost,
        position.entry_cost + pnl,
        pnl,
        days_held,
        exit_type,
        leg_details,
    );

    if sizing_active {
        record.computed_quantity = Some(position.quantity);
        record.entry_equity = Some(entry_equity);
    }

    // Stock leg fields
    if let Some(entry_price) = position.stock_entry_price {
        let exit_price = ohlcv_closes
            .and_then(|c| c.range(..=today).next_back().map(|(_, &v)| v))
            .unwrap_or(entry_price);
        let shares = f64::from(position.quantity) * f64::from(position.multiplier);
        record.stock_entry_price = Some(entry_price);
        record.stock_exit_price = Some(exit_price);
        record.stock_pnl = Some((exit_price - entry_price) * shares);
    }

    record
}

/// Reservoir-sample entry spread percentages, capped at `max_samples`.
///
/// Uses a simple LCG hash for deterministic reservoir sampling so that
/// memory usage is bounded regardless of the number of trades.
fn sample_entry_spreads(
    candidate: &EntryCandidate,
    spread_pcts: &mut Vec<f64>,
    sample_count: &mut u64,
    max_samples: usize,
) {
    for leg in &candidate.legs {
        if leg.bid > 0.0 && leg.ask > 0.0 {
            let mid = f64::midpoint(leg.bid, leg.ask);
            let spread_pct = (leg.ask - leg.bid) / mid * 100.0;
            *sample_count += 1;
            if spread_pcts.len() < max_samples {
                spread_pcts.push(spread_pct);
            } else {
                let hash = (*sample_count)
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let j = (hash % *sample_count) as usize;
                if j < max_samples {
                    spread_pcts[j] = spread_pct;
                }
            }
        }
    }
}

/// Compute unrealized P&L for all open positions via mark-to-market.
fn compute_unrealized_pnl(
    positions: &[Position],
    today: NaiveDate,
    ctx: &SimContext,
    last_known: &LastKnown,
) -> f64 {
    positions
        .iter()
        .filter(|p| matches!(p.status, PositionStatus::Open))
        .map(|p| mark_to_market(p, today, ctx, last_known))
        .sum()
}

/// Run the event-driven simulation loop.
#[allow(clippy::too_many_lines, clippy::implicit_hasher)]
pub fn run_event_loop(
    ctx: &SimContext,
    candidates: &BTreeMap<NaiveDate, Vec<EntryCandidate>>,
    trading_days: &[NaiveDate],
    exit_dates: Option<&std::collections::HashSet<NaiveDate>>,
    date_index: &DateIndex,
) -> (Vec<TradeRecord>, Vec<EquityPoint>, BacktestQualityStats) {
    // Capped reservoir sample for spread percentages to bound memory.
    // 10 000 samples is enough for an accurate median estimate.
    const MAX_SPREAD_SAMPLES: usize = 10_000;

    let params = ctx.params;
    let price_table = ctx.price_table;
    let strategy_def = ctx.strategy_def;
    let ohlcv_closes = ctx.ohlcv_closes;

    let mut positions: Vec<Position> = Vec::new();
    let mut trade_log: Vec<TradeRecord> = Vec::new();
    let mut equity_curve: Vec<EquityPoint> = Vec::new();

    let mut realized_equity = params.capital;
    let mut next_id = 1usize;
    let mut trade_id = 0usize;

    // Quality tracking
    let trading_days_total = trading_days.len();
    let mut trading_days_with_data = std::collections::HashSet::new();
    let mut total_candidates = 0usize;
    let mut positions_opened = 0usize;
    let mut entry_spread_pcts: Vec<f64> = Vec::new();
    let mut spread_sample_count: u64 = 0;

    // Stagger: track the last date on which a new position was opened
    let mut last_entry_date: Option<NaiveDate> = None;

    // Count unique trading dates in price table
    for (quote_date, _, _, _) in price_table.keys() {
        trading_days_with_data.insert(*quote_date);
    }

    // Last known prices for carry-forward on gaps
    let mut last_known: LastKnown = LastKnown::new();

    for &today in trading_days {
        // Phase 1: Check exits on open positions
        let mut i = 0;
        while i < positions.len() {
            if !matches!(positions[i].status, PositionStatus::Open) {
                i += 1;
                continue;
            }

            // Signal-based exit takes priority — check before other triggers
            let signal_exit = exit_dates
                .is_some_and(|dates| dates.contains(&today))
                .then_some(ExitType::Signal);

            let exit_type =
                signal_exit.or_else(|| check_exit_triggers(&positions[i], today, ctx, &last_known));

            if let Some(exit_type) = exit_type {
                // Capture equity before this trade's P&L for accurate sizing audit
                let equity_before_close = realized_equity;
                let pnl = close_position(
                    &mut positions[i],
                    today,
                    ctx,
                    &last_known,
                    exit_type.clone(),
                );
                realized_equity += pnl;

                trade_id += 1;
                trade_log.push(build_trade_record(
                    &positions[i],
                    today,
                    trade_id,
                    pnl,
                    exit_type,
                    params.sizing.is_some(),
                    equity_before_close,
                    ohlcv_closes,
                ));
            }

            i += 1;
        }

        // Remove closed positions
        positions.retain(|p| matches!(p.status, PositionStatus::Open));

        // Adjustment phase: check rules against remaining open positions
        if !params.adjustment_rules.is_empty() {
            let mut adj_state = SimState {
                trade_log: std::mem::take(&mut trade_log),
                trade_id,
                realized_equity,
            };
            check_and_apply_adjustments(&mut positions, today, ctx, &last_known, &mut adj_state);
            trade_log = adj_state.trade_log;
            trade_id = adj_state.trade_id;
            realized_equity = adj_state.realized_equity;
            positions.retain(|p| matches!(p.status, PositionStatus::Open));
        }

        // Phase 2: Enter new positions
        let open_count = positions.len();

        // Stagger check: skip entry if we entered too recently
        let stagger_ok = match (params.min_days_between_entries, last_entry_date) {
            (Some(min_days), Some(last)) => (today - last).num_days() >= i64::from(min_days),
            _ => true,
        };

        if open_count < params.max_positions as usize && stagger_ok {
            if let Some(day_candidates) = candidates.get(&today) {
                // Track candidates
                total_candidates += day_candidates.len();

                // Filter out candidates with expirations we already hold
                let available: Vec<&EntryCandidate> = day_candidates
                    .iter()
                    .filter(|c| {
                        !positions.iter().any(|p| {
                            matches!(p.status, PositionStatus::Open)
                                && p.expiration == c.expiration
                                && p.secondary_expiration == c.secondary_expiration
                        })
                    })
                    .collect();
                if let Some(candidate) =
                    select_candidate(&available, &params.selector, params.entry_dte.target)
                {
                    sample_entry_spreads(
                        candidate,
                        &mut entry_spread_pcts,
                        &mut spread_sample_count,
                        MAX_SPREAD_SAMPLES,
                    );

                    // Dynamic position sizing
                    let effective_qty = params.sizing.as_ref().and_then(|cfg| {
                        let stock_px = if strategy_def.has_stock_leg {
                            ohlcv_closes.and_then(|c| c.get(&today).copied())
                        } else {
                            None
                        };
                        let ml = super::sizing::max_loss_per_contract(
                            strategy_def,
                            candidate,
                            params,
                            stock_px,
                        )?;
                        if ml <= 0.0 {
                            return None;
                        }
                        let vol = super::sizing::vol_lookback(cfg).and_then(|lookback| {
                            let closes = ohlcv_closes?;
                            let vals: Vec<f64> = closes.range(..=today).map(|(_, &v)| v).collect();
                            super::sizing::compute_realized_vol(&vals, lookback, 252.0)
                        });
                        Some(super::sizing::compute_quantity(
                            cfg,
                            realized_equity,
                            ml,
                            &trade_log,
                            vol,
                            params.multiplier,
                            params.quantity,
                        ))
                    });

                    if let Some(position) =
                        open_position(candidate, today, ctx, next_id, effective_qty)
                    {
                        next_id += 1;
                        positions.push(position);
                        positions_opened += 1;
                        last_entry_date = Some(today);
                    }
                }
            }
        }

        // Update last known prices for all quotes on this day
        update_last_known(price_table, date_index, today, &mut last_known);

        // Phase 3: Daily mark-to-market
        let unrealized = compute_unrealized_pnl(&positions, today, ctx, &last_known);

        equity_curve.push(EquityPoint {
            datetime: today
                .and_hms_opt(0, 0, 0)
                .expect("and_hms_opt(0,0,0) should never fail"),
            equity: realized_equity + unrealized,
        });
    }

    let quality = BacktestQualityStats {
        trading_days_total,
        trading_days_with_data: trading_days_with_data.len(),
        total_candidates,
        positions_opened,
        entry_spread_pcts,
    };

    (trade_log, equity_curve, quality)
}

/// Check if any exit trigger fires for a position on the given day.
fn check_exit_triggers(
    position: &Position,
    today: NaiveDate,
    ctx: &SimContext,
    last_known: &LastKnown,
) -> Option<ExitType> {
    let params = ctx.params;
    // Expiration check
    if today >= position.expiration {
        return Some(ExitType::Expiration);
    }

    // DTE exit check — use the earliest expiration across all legs
    let earliest_exp = position
        .secondary_expiration
        .map_or(position.expiration, |sec| position.expiration.min(sec));
    let dte = (earliest_exp - today).num_days();
    if dte <= i64::from(params.exit_dte) {
        return Some(ExitType::DteExit);
    }

    // Max hold days check
    if let Some(max_days) = params.max_hold_days {
        let days_held = (today - position.entry_date).num_days();
        if days_held >= i64::from(max_days) {
            return Some(ExitType::MaxHold);
        }
    }

    // Stop loss / take profit checks based on current MTM
    let mtm = mark_to_market(position, today, ctx, last_known);

    if let Some(sl) = params.stop_loss {
        let loss_threshold = position.entry_cost.abs() * sl;
        if mtm < -loss_threshold {
            return Some(ExitType::StopLoss);
        }
    }

    if let Some(tp) = params.take_profit {
        let profit_threshold = position.entry_cost.abs() * tp;
        if mtm > profit_threshold {
            return Some(ExitType::TakeProfit);
        }
    }

    // Net delta exit: exit when abs(net_delta) exceeds threshold
    if let Some(delta_thresh) = params.exit_net_delta {
        let net_delta = compute_position_net_delta(position, today, ctx, last_known);
        if net_delta.abs() > delta_thresh {
            return Some(ExitType::DeltaExit);
        }
    }

    None
}
