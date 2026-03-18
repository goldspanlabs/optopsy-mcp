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

    let mut candidates: BTreeMap<NaiveDate, Vec<EntryCandidate>> = BTreeMap::new();

    for row_idx in 0..combined.height() {
        let entry_date = extract_date_from_column(quote_dates, row_idx)?;

        // Extract expiration dates
        let (primary_exp, secondary_exp) = if is_multi_exp {
            let prim = extract_date_from_column(combined.column("expiration_primary")?, row_idx)?;
            let sec = extract_date_from_column(combined.column("expiration_secondary")?, row_idx)?;
            (prim, Some(sec))
        } else {
            let exp = extract_date_from_column(combined.column("expiration")?, row_idx)?;
            (exp, None)
        };

        let mut legs = Vec::new();
        let mut net_premium = 0.0;
        let mut net_delta = 0.0;

        let mut skip_row = false;
        for (i, leg_def) in strategy_def.legs.iter().enumerate() {
            let Some(strike) = combined.column(&format!("strike_{i}"))?.f64()?.get(row_idx) else {
                tracing::debug!(
                    row = row_idx,
                    leg = i,
                    "Skipping entry candidate: null strike value"
                );
                skip_row = true;
                break;
            };
            let Some(bid) = combined.column(&format!("bid_{i}"))?.f64()?.get(row_idx) else {
                tracing::debug!(
                    row = row_idx,
                    leg = i,
                    "Skipping entry candidate: null bid value"
                );
                skip_row = true;
                break;
            };
            let Some(ask) = combined.column(&format!("ask_{i}"))?.f64()?.get(row_idx) else {
                tracing::debug!(
                    row = row_idx,
                    leg = i,
                    "Skipping entry candidate: null ask value"
                );
                skip_row = true;
                break;
            };
            let Some(delta) = combined.column(&format!("delta_{i}"))?.f64()?.get(row_idx) else {
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
                        let ml =
                            super::sizing::max_loss_per_contract(strategy_def, candidate, params)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::price_table::build_date_index;
    use ordered_float::OrderedFloat;
    use std::collections::HashMap;

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

    /// Helper: build a synthetic daily options `DataFrame` for testing.
    fn make_daily_df() -> DataFrame {
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 22).unwrap();
        let d3 = NaiveDate::from_ymd_opt(2024, 1, 29).unwrap();
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();

        let quote_dates = vec![
            d1.and_hms_opt(0, 0, 0).unwrap(),
            d2.and_hms_opt(0, 0, 0).unwrap(),
            d3.and_hms_opt(0, 0, 0).unwrap(),
        ];
        let expirations = [exp, exp, exp];

        let mut df = df! {
            DATETIME_COL => &quote_dates,
            "option_type" => &["call", "call", "call"],
            "strike" => &[100.0f64, 100.0, 100.0],
            "bid" => &[5.0f64, 3.0, 2.0],
            "ask" => &[5.50f64, 3.50, 2.50],
            "delta" => &[0.50f64, 0.35, 0.25],
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations).into_column(),
        )
        .unwrap();
        df
    }

    #[test]
    fn run_event_loop_single_trade() {
        let (table, days, date_idx) = make_price_table_simple();
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();

        let mut candidates = BTreeMap::new();
        candidates.insert(
            d1,
            vec![EntryCandidate {
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
                net_delta: 0.50,
            }],
        );

        let strategy_def = crate::strategies::find_strategy("long_call").unwrap();

        let params = BacktestParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.50,
                min: 0.20,
                max: 0.80,
            }],
            entry_dte: DteRange {
                target: 45,
                min: 10,
                max: 60,
            },
            exit_dte: 15,
            slippage: Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            sizing: None,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: None,
        };

        let (_trade_log, equity_curve, _) = {
            let ctx = SimContext {
                price_table: &table,
                params: &params,
                strategy_def: &strategy_def,
                ohlcv_closes: None,
            };
            run_event_loop(&ctx, &candidates, &days, None, &date_idx)
        };

        assert_eq!(equity_curve.len(), 3, "Should have 3 equity points");
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn run_event_loop_stop_loss() {
        let mut table = PriceTable::with_hasher(rustc_hash::FxBuildHasher);
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 16).unwrap();
        let d3 = NaiveDate::from_ymd_opt(2024, 1, 17).unwrap();
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();

        table.insert(
            (d1, exp, OrderedFloat(100.0), OptionType::Call),
            QuoteSnapshot {
                bid: 5.0,
                ask: 5.50,
                delta: 0.50,
            },
        );
        table.insert(
            (d2, exp, OrderedFloat(100.0), OptionType::Call),
            QuoteSnapshot {
                bid: 4.0,
                ask: 4.50,
                delta: 0.45,
            },
        );
        table.insert(
            (d3, exp, OrderedFloat(100.0), OptionType::Call),
            QuoteSnapshot {
                bid: 1.0,
                ask: 1.50,
                delta: 0.15,
            },
        );

        let days = vec![d1, d2, d3];
        let date_idx = build_date_index(&table);
        let mut candidates = BTreeMap::new();
        candidates.insert(
            d1,
            vec![EntryCandidate {
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
                net_delta: 0.50,
            }],
        );

        let strategy_def = crate::strategies::find_strategy("long_call").unwrap();

        let params = BacktestParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.50,
                min: 0.20,
                max: 0.80,
            }],
            entry_dte: DteRange {
                target: 45,
                min: 10,
                max: 60,
            },
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: Some(0.50),
            take_profit: None,
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            sizing: None,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: None,
        };

        let (trade_log, _, _) = {
            let ctx = SimContext {
                price_table: &table,
                params: &params,
                strategy_def: &strategy_def,
                ohlcv_closes: None,
            };
            run_event_loop(&ctx, &candidates, &days, None, &date_idx)
        };

        assert_eq!(trade_log.len(), 1);
        assert!(
            matches!(trade_log[0].exit_type, ExitType::StopLoss),
            "Expected StopLoss, got {:?}",
            trade_log[0].exit_type
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn run_event_loop_take_profit() {
        let mut table = PriceTable::with_hasher(rustc_hash::FxBuildHasher);
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 16).unwrap();
        let d3 = NaiveDate::from_ymd_opt(2024, 1, 17).unwrap();
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();

        table.insert(
            (d1, exp, OrderedFloat(100.0), OptionType::Call),
            QuoteSnapshot {
                bid: 5.0,
                ask: 5.50,
                delta: 0.50,
            },
        );
        table.insert(
            (d2, exp, OrderedFloat(100.0), OptionType::Call),
            QuoteSnapshot {
                bid: 6.0,
                ask: 6.50,
                delta: 0.55,
            },
        );
        table.insert(
            (d3, exp, OrderedFloat(100.0), OptionType::Call),
            QuoteSnapshot {
                bid: 10.0,
                ask: 10.50,
                delta: 0.70,
            },
        );

        let days = vec![d1, d2, d3];
        let date_idx = build_date_index(&table);
        let mut candidates = BTreeMap::new();
        candidates.insert(
            d1,
            vec![EntryCandidate {
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
                net_delta: 0.50,
            }],
        );

        let strategy_def = crate::strategies::find_strategy("long_call").unwrap();

        let params = BacktestParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.50,
                min: 0.20,
                max: 0.80,
            }],
            entry_dte: DteRange {
                target: 45,
                min: 10,
                max: 60,
            },
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: None,
            take_profit: Some(0.50),
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            sizing: None,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: None,
        };

        let (trade_log, _, _) = {
            let ctx = SimContext {
                price_table: &table,
                params: &params,
                strategy_def: &strategy_def,
                ohlcv_closes: None,
            };
            run_event_loop(&ctx, &candidates, &days, None, &date_idx)
        };

        assert_eq!(trade_log.len(), 1);
        assert!(
            matches!(trade_log[0].exit_type, ExitType::TakeProfit),
            "Expected TakeProfit, got {:?}",
            trade_log[0].exit_type
        );
    }

    #[test]
    fn run_event_loop_max_positions() {
        let mut table = PriceTable::with_hasher(rustc_hash::FxBuildHasher);
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 16).unwrap();
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();

        for d in [d1, d2] {
            table.insert(
                (d, exp, OrderedFloat(100.0), OptionType::Call),
                QuoteSnapshot {
                    bid: 5.0,
                    ask: 5.50,
                    delta: 0.50,
                },
            );
            table.insert(
                (d, exp, OrderedFloat(105.0), OptionType::Call),
                QuoteSnapshot {
                    bid: 3.0,
                    ask: 3.50,
                    delta: 0.40,
                },
            );
        }

        let days = vec![d1, d2];
        let date_idx = build_date_index(&table);

        let make_cand = |date: NaiveDate, strike: f64, bid: f64, ask: f64| -> EntryCandidate {
            EntryCandidate {
                entry_date: date,
                expiration: exp,
                secondary_expiration: None,
                legs: vec![CandidateLeg {
                    option_type: OptionType::Call,
                    strike,
                    expiration: exp,
                    bid,
                    ask,
                    delta: 0.50,
                }],
                net_premium: -(bid + ask) / 2.0,
                net_delta: 0.50,
            }
        };

        let mut candidates = BTreeMap::new();
        candidates.insert(d1, vec![make_cand(d1, 100.0, 5.0, 5.50)]);
        candidates.insert(d2, vec![make_cand(d2, 105.0, 3.0, 3.50)]);

        let strategy_def = crate::strategies::find_strategy("long_call").unwrap();

        let params = BacktestParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.50,
                min: 0.20,
                max: 0.80,
            }],
            entry_dte: DteRange {
                target: 45,
                min: 10,
                max: 60,
            },
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            sizing: None,
            multiplier: 100,
            max_positions: 1,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: None,
        };

        let (trade_log, _, _) = {
            let ctx = SimContext {
                price_table: &table,
                params: &params,
                strategy_def: &strategy_def,
                ohlcv_closes: None,
            };
            run_event_loop(&ctx, &candidates, &days, None, &date_idx)
        };

        assert_eq!(trade_log.len(), 0, "No trades should close in 2 days");
    }

    #[test]
    fn run_event_loop_daily_equity() {
        let (table, days, date_idx) = make_price_table_simple();
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();

        let mut candidates = BTreeMap::new();
        candidates.insert(
            d1,
            vec![EntryCandidate {
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
                net_delta: 0.50,
            }],
        );

        let strategy_def = crate::strategies::find_strategy("long_call").unwrap();

        let params = BacktestParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.50,
                min: 0.20,
                max: 0.80,
            }],
            entry_dte: DteRange {
                target: 45,
                min: 10,
                max: 60,
            },
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            sizing: None,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: None,
        };

        let (_, equity_curve, _) = {
            let ctx = SimContext {
                price_table: &table,
                params: &params,
                strategy_def: &strategy_def,
                ohlcv_closes: None,
            };
            run_event_loop(&ctx, &candidates, &days, None, &date_idx)
        };

        assert_eq!(
            equity_curve.len(),
            days.len(),
            "One equity point per trading day"
        );

        assert!(
            (equity_curve[0].equity - 10000.0).abs() < 1e-10,
            "Day 1 equity should be 10000, got {}",
            equity_curve[0].equity
        );

        assert!(
            (equity_curve[1].equity - 9800.0).abs() < 1e-10,
            "Day 2 equity should be 9800, got {}",
            equity_curve[1].equity
        );

        assert!(
            (equity_curve[2].equity - 9700.0).abs() < 1e-10,
            "Day 3 equity should be 9700, got {}",
            equity_curve[2].equity
        );
    }

    #[test]
    fn find_entry_candidates_single_leg() {
        let df = make_daily_df();
        let strategy_def = crate::strategies::find_strategy("long_call").unwrap();

        let params = BacktestParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.50,
                min: 0.10,
                max: 0.80,
            }],
            entry_dte: DteRange {
                target: 45,
                min: 10,
                max: 60,
            },
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            sizing: None,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: None,
        };

        let candidates = find_entry_candidates(&df, &strategy_def, &params).unwrap();
        assert!(
            !candidates.is_empty(),
            "Should find at least one date with candidates"
        );

        for cands in candidates.values() {
            assert_eq!(cands[0].legs.len(), 1);
        }
    }

    #[test]
    fn find_entry_candidates_three_legs_no_duplicate_columns() {
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        let mut df = df! {
            DATETIME_COL => &[d1, d1, d1],
            "option_type" => &["call", "call", "call"],
            "strike" => &[100.0f64, 105.0, 110.0],
            "bid" => &[5.0f64, 3.0, 1.5],
            "ask" => &[5.50f64, 3.50, 2.0],
            "delta" => &[0.50f64, 0.35, 0.20],
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), [exp, exp, exp])
                .into_column(),
        )
        .unwrap();

        let strategy_def = crate::strategies::find_strategy("long_call_butterfly").unwrap();
        let params = BacktestParams {
            strategy: "long_call_butterfly".to_string(),
            leg_deltas: vec![
                TargetRange {
                    target: 0.50,
                    min: 0.01,
                    max: 0.99,
                },
                TargetRange {
                    target: 0.35,
                    min: 0.01,
                    max: 0.99,
                },
                TargetRange {
                    target: 0.20,
                    min: 0.01,
                    max: 0.99,
                },
            ],
            entry_dte: DteRange {
                target: 45,
                min: 10,
                max: 60,
            },
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            sizing: None,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: None,
        };

        let candidates = find_entry_candidates(&df, &strategy_def, &params).unwrap();
        assert!(
            !candidates.is_empty(),
            "Should find entry candidates for 3-leg butterfly"
        );
        for cands in candidates.values() {
            assert_eq!(cands[0].legs.len(), 3, "Butterfly should have 3 legs");
        }
    }

    #[test]
    fn find_entry_candidates_skips_rows_with_null_strike() {
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        let null_strike: Vec<Option<f64>> = vec![None];
        let strike_col = Series::new("strike".into(), null_strike).into_column();

        let mut df = df! {
            DATETIME_COL => &[d1],
            "option_type" => &["call"],
            "bid" => &[5.0f64],
            "ask" => &[5.50f64],
            "delta" => &[0.50f64],
        }
        .unwrap();
        df.with_column(strike_col).unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), [exp]).into_column(),
        )
        .unwrap();

        let strategy_def = crate::strategies::find_strategy("long_call").unwrap();
        let params = BacktestParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.50,
                min: 0.10,
                max: 0.90,
            }],
            entry_dte: DteRange {
                target: 45,
                min: 10,
                max: 60,
            },
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            sizing: None,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: None,
        };

        let candidates = find_entry_candidates(&df, &strategy_def, &params).unwrap();
        assert!(
            candidates.is_empty(),
            "Row with null strike should be skipped; expected no candidates"
        );
    }

    #[test]
    fn net_premium_filter_excludes_low_premium() {
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        let mut df = df! {
            DATETIME_COL => &[d1],
            "option_type" => &["call"],
            "strike" => &[100.0f64],
            "bid" => &[0.30f64],
            "ask" => &[0.40f64],
            "delta" => &[0.50f64],
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), [exp]).into_column(),
        )
        .unwrap();

        let strategy_def = crate::strategies::find_strategy("long_call").unwrap();
        let params = BacktestParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.50,
                min: 0.10,
                max: 0.99,
            }],
            entry_dte: DteRange {
                target: 45,
                min: 10,
                max: 60,
            },
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            sizing: None,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_net_premium: Some(1.0),
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: None,
        };

        let candidates = find_entry_candidates(&df, &strategy_def, &params).unwrap();
        assert!(
            candidates.is_empty(),
            "Candidates with premium < min_net_premium should be excluded"
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn stagger_days_asserts_trade_count() {
        let mut table = PriceTable::with_hasher(rustc_hash::FxBuildHasher);
        let exp = NaiveDate::from_ymd_opt(2024, 1, 19).unwrap();
        let days: Vec<NaiveDate> = (0..10)
            .map(|i| NaiveDate::from_ymd_opt(2024, 1, 10 + i).unwrap())
            .collect();

        for &d in &days {
            table.insert(
                (d, exp, OrderedFloat(100.0), OptionType::Call),
                QuoteSnapshot {
                    bid: 5.0,
                    ask: 5.50,
                    delta: 0.50,
                },
            );
        }

        let date_idx = build_date_index(&table);
        let mut candidates = BTreeMap::new();
        for &d in &days {
            candidates.insert(
                d,
                vec![EntryCandidate {
                    entry_date: d,
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
                    net_delta: 0.50,
                }],
            );
        }

        let strategy_def = crate::strategies::find_strategy("long_call").unwrap();

        let params_no_stagger = BacktestParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.50,
                min: 0.20,
                max: 0.80,
            }],
            entry_dte: DteRange {
                target: 45,
                min: 10,
                max: 60,
            },
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 100_000.0,
            quantity: 1,
            sizing: None,
            multiplier: 100,
            max_positions: 10,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: None,
        };
        let (trades_no_stagger, _, _) = {
            let ctx = SimContext {
                price_table: &table,
                params: &params_no_stagger,
                strategy_def: &strategy_def,
                ohlcv_closes: None,
            };
            run_event_loop(&ctx, &candidates, &days, None, &date_idx)
        };

        let params_stagger = BacktestParams {
            min_days_between_entries: Some(3),
            ..params_no_stagger.clone()
        };
        let (trades_stagger, _, _) = {
            let ctx = SimContext {
                price_table: &table,
                params: &params_stagger,
                strategy_def: &strategy_def,
                ohlcv_closes: None,
            };
            run_event_loop(&ctx, &candidates, &days, None, &date_idx)
        };

        assert!(
            trades_no_stagger.len() > trades_stagger.len(),
            "Stagger should reduce trade count: {} without vs {} with",
            trades_no_stagger.len(),
            trades_stagger.len(),
        );
        assert!(
            trades_stagger.len() <= 4,
            "With 10 days and stagger=3, at most 4 entries: got {}",
            trades_stagger.len(),
        );
    }

    #[test]
    fn max_net_premium_filter_excludes_expensive() {
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        let mut df = df! {
            DATETIME_COL => &[d1],
            "option_type" => &["call"],
            "strike" => &[100.0f64],
            "bid" => &[5.0f64],
            "ask" => &[5.50f64],
            "delta" => &[0.50f64],
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), [exp]).into_column(),
        )
        .unwrap();

        let strategy_def = crate::strategies::find_strategy("long_call").unwrap();
        let params = BacktestParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.50,
                min: 0.10,
                max: 0.99,
            }],
            entry_dte: DteRange {
                target: 45,
                min: 10,
                max: 60,
            },
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            sizing: None,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_net_premium: None,
            max_net_premium: Some(1.0),
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: None,
        };

        let candidates = find_entry_candidates(&df, &strategy_def, &params).unwrap();
        assert!(
            candidates.is_empty(),
            "Candidates with premium > max_net_premium should be excluded"
        );
    }

    #[test]
    fn net_delta_filter_excludes_high_delta() {
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        let mut df = df! {
            DATETIME_COL => &[d1],
            "option_type" => &["call"],
            "strike" => &[100.0f64],
            "bid" => &[5.0f64],
            "ask" => &[5.50f64],
            "delta" => &[0.70f64],
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), [exp]).into_column(),
        )
        .unwrap();

        let strategy_def = crate::strategies::find_strategy("long_call").unwrap();

        let params = BacktestParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.70,
                min: 0.10,
                max: 0.99,
            }],
            entry_dte: DteRange {
                target: 45,
                min: 10,
                max: 60,
            },
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            sizing: None,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: Some(0.50),
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: None,
        };

        let candidates = find_entry_candidates(&df, &strategy_def, &params).unwrap();
        assert!(
            candidates.is_empty(),
            "Candidates with net_delta > max_net_delta should be excluded"
        );
    }

    #[test]
    fn net_delta_filter_passes_within_range() {
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        let mut df = df! {
            DATETIME_COL => &[d1],
            "option_type" => &["call"],
            "strike" => &[100.0f64],
            "bid" => &[5.0f64],
            "ask" => &[5.50f64],
            "delta" => &[0.30f64],
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), [exp]).into_column(),
        )
        .unwrap();

        let strategy_def = crate::strategies::find_strategy("long_call").unwrap();
        let params = BacktestParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.30,
                min: 0.10,
                max: 0.99,
            }],
            entry_dte: DteRange {
                target: 45,
                min: 10,
                max: 60,
            },
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            sizing: None,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: Some(0.10),
            max_net_delta: Some(0.50),
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: None,
        };

        let candidates = find_entry_candidates(&df, &strategy_def, &params).unwrap();
        assert!(
            !candidates.is_empty(),
            "Candidate with net_delta=0.30 should pass [0.10, 0.50] filter"
        );
    }

    #[test]
    fn entry_candidate_net_delta_computed_correctly() {
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        let mut df = df! {
            DATETIME_COL => &[d1],
            "option_type" => &["call"],
            "strike" => &[100.0f64],
            "bid" => &[5.0f64],
            "ask" => &[5.50f64],
            "delta" => &[0.45f64],
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), [exp]).into_column(),
        )
        .unwrap();

        let strategy_def = crate::strategies::find_strategy("long_call").unwrap();
        let params = BacktestParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.45,
                min: 0.10,
                max: 0.99,
            }],
            entry_dte: DteRange {
                target: 45,
                min: 10,
                max: 60,
            },
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            sizing: None,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: None,
        };

        let candidates = find_entry_candidates(&df, &strategy_def, &params).unwrap();
        assert!(!candidates.is_empty());
        let cand = &candidates.values().next().unwrap()[0];
        assert!(
            (cand.net_delta - 0.45).abs() < 1e-10,
            "Expected net_delta=0.45, got {}",
            cand.net_delta,
        );
    }

    #[test]
    fn delta_exit_triggers_when_threshold_exceeded() {
        let mut table = PriceTable::with_hasher(rustc_hash::FxBuildHasher);
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let days: Vec<NaiveDate> = (0..5)
            .map(|i| NaiveDate::from_ymd_opt(2024, 1, 15 + i).unwrap())
            .collect();

        for (i, &d) in days.iter().enumerate() {
            let delta = if i < 2 { 0.30 } else { 0.80 };
            table.insert(
                (d, exp, OrderedFloat(100.0), OptionType::Call),
                QuoteSnapshot {
                    bid: 5.0,
                    ask: 5.50,
                    delta,
                },
            );
        }

        let mut candidates = BTreeMap::new();
        let date_idx = build_date_index(&table);
        candidates.insert(
            days[0],
            vec![EntryCandidate {
                entry_date: days[0],
                expiration: exp,
                secondary_expiration: None,
                legs: vec![CandidateLeg {
                    option_type: OptionType::Call,
                    strike: 100.0,
                    expiration: exp,
                    bid: 5.0,
                    ask: 5.50,
                    delta: 0.30,
                }],
                net_premium: -5.25,
                net_delta: 0.30,
            }],
        );

        let strategy_def = crate::strategies::find_strategy("long_call").unwrap();
        let params = BacktestParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.30,
                min: 0.10,
                max: 0.99,
            }],
            entry_dte: DteRange {
                target: 45,
                min: 10,
                max: 60,
            },
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 100_000.0,
            quantity: 1,
            sizing: None,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: Some(0.50),
        };

        let (trade_log, _, _) = {
            let ctx = SimContext {
                price_table: &table,
                params: &params,
                strategy_def: &strategy_def,
                ohlcv_closes: None,
            };
            run_event_loop(&ctx, &candidates, &days, None, &date_idx)
        };

        assert_eq!(trade_log.len(), 1, "Should have exactly 1 trade");
        assert_eq!(
            trade_log[0].exit_type,
            ExitType::DeltaExit,
            "Trade should exit via DeltaExit, got {:?}",
            trade_log[0].exit_type,
        );
        assert_eq!(
            trade_log[0].days_held, 2,
            "Should exit on day 2 (delta spikes)"
        );
    }

    #[test]
    fn delta_exit_does_not_trigger_within_threshold() {
        let mut table = PriceTable::with_hasher(rustc_hash::FxBuildHasher);
        let exp = NaiveDate::from_ymd_opt(2024, 1, 18).unwrap();
        let days: Vec<NaiveDate> = (0..5)
            .map(|i| NaiveDate::from_ymd_opt(2024, 1, 15 + i).unwrap())
            .collect();

        for &d in &days {
            table.insert(
                (d, exp, OrderedFloat(100.0), OptionType::Call),
                QuoteSnapshot {
                    bid: 5.0,
                    ask: 5.50,
                    delta: 0.30,
                },
            );
        }

        let mut candidates = BTreeMap::new();
        let date_idx = build_date_index(&table);
        candidates.insert(
            days[0],
            vec![EntryCandidate {
                entry_date: days[0],
                expiration: exp,
                secondary_expiration: None,
                legs: vec![CandidateLeg {
                    option_type: OptionType::Call,
                    strike: 100.0,
                    expiration: exp,
                    bid: 5.0,
                    ask: 5.50,
                    delta: 0.30,
                }],
                net_premium: -5.25,
                net_delta: 0.30,
            }],
        );

        let strategy_def = crate::strategies::find_strategy("long_call").unwrap();
        let params = BacktestParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.30,
                min: 0.10,
                max: 0.99,
            }],
            entry_dte: DteRange {
                target: 45,
                min: 1,
                max: 60,
            },
            exit_dte: 0,
            slippage: Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 100_000.0,
            quantity: 1,
            sizing: None,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: Some(0.50),
        };

        let (trade_log, _, _) = {
            let ctx = SimContext {
                price_table: &table,
                params: &params,
                strategy_def: &strategy_def,
                ohlcv_closes: None,
            };
            run_event_loop(&ctx, &candidates, &days, None, &date_idx)
        };

        assert!(!trade_log.is_empty(), "Should have at least 1 closed trade");
        for trade in &trade_log {
            assert_ne!(
                trade.exit_type,
                ExitType::DeltaExit,
                "Should NOT trigger DeltaExit when delta stays below threshold",
            );
        }
    }
}
