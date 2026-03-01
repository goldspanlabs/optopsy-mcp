use std::collections::{BTreeMap, HashMap};

use anyhow::{bail, Result};
use chrono::NaiveDate;
use ordered_float::OrderedFloat;
use polars::prelude::*;

use super::filters;
use super::pricing;
use super::rules;
#[allow(clippy::wildcard_imports)]
use super::types::*;
use crate::data::parquet::QUOTE_DATETIME_COL;

/// Build a price lookup table from the raw options `DataFrame`.
/// Returns the table and a sorted list of unique trading dates.
pub fn build_price_table(df: &DataFrame) -> Result<(PriceTable, Vec<NaiveDate>)> {
    let quote_dates = df.column(QUOTE_DATETIME_COL)?;
    let expirations = df.column("expiration")?;
    let strikes = df.column("strike")?.f64()?;
    let option_types = df.column("option_type")?.str()?;
    let bids = df.column("bid")?.f64()?;
    let asks = df.column("ask")?.f64()?;
    let deltas = df.column("delta")?.f64()?;

    let mut table = PriceTable::new();
    let mut dates_set = std::collections::BTreeSet::new();

    for i in 0..df.height() {
        let quote_date = extract_date_from_column(quote_dates, i)?;
        let exp_date = extract_date_from_column(expirations, i)?;
        let strike = strikes.get(i).unwrap_or(0.0);
        let opt_type_str = option_types.get(i).unwrap_or("");
        let opt_type = match opt_type_str {
            "call" => OptionType::Call,
            "put" => OptionType::Put,
            _ => continue,
        };
        let bid = bids.get(i).unwrap_or(0.0);
        let ask = asks.get(i).unwrap_or(0.0);
        let delta = deltas.get(i).unwrap_or(0.0);

        let key = (quote_date, exp_date, OrderedFloat(strike), opt_type);
        table.insert(key, QuoteSnapshot { bid, ask, delta });
        dates_set.insert(quote_date);
    }

    let trading_days: Vec<NaiveDate> = dates_set.into_iter().collect();
    Ok((table, trading_days))
}

/// Extract a `NaiveDate` from a column value at a given index.
/// Handles both Date and Datetime column types.
fn extract_date_from_column(col: &Column, idx: usize) -> Result<NaiveDate> {
    match col.dtype() {
        DataType::Date => {
            let days = col.date()?.phys.get(idx);
            match days {
                Some(d) => {
                    let date = chrono::NaiveDate::from_num_days_from_ce_opt(
                        d + 719_163, // epoch offset: days from CE to 1970-01-01
                    )
                    .ok_or_else(|| anyhow::anyhow!("Invalid date at index {idx}"))?;
                    Ok(date)
                }
                None => bail!("Null date at index {idx}"),
            }
        }
        DataType::Datetime(tu, _) => {
            let val = col.datetime()?.phys.get(idx);
            match val {
                Some(v) => {
                    let ndt = match tu {
                        TimeUnit::Milliseconds => {
                            chrono::DateTime::from_timestamp_millis(v).map(|dt| dt.naive_utc())
                        }
                        TimeUnit::Microseconds => {
                            chrono::DateTime::from_timestamp_micros(v).map(|dt| dt.naive_utc())
                        }
                        TimeUnit::Nanoseconds => {
                            let secs = v / 1_000_000_000;
                            #[allow(clippy::cast_sign_loss)]
                            let nsecs = (v % 1_000_000_000) as u32;
                            chrono::DateTime::from_timestamp(secs, nsecs).map(|dt| dt.naive_utc())
                        }
                    };
                    match ndt {
                        Some(dt) => Ok(dt.date()),
                        None => bail!("Invalid datetime value at index {idx}"),
                    }
                }
                None => bail!("Null datetime at index {idx}"),
            }
        }
        other => bail!("Unsupported column type for date extraction: {other:?}"),
    }
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

    // Process each leg through the existing filter pipeline
    let mut leg_dfs = Vec::new();
    for (i, (leg, delta_target)) in strategy_def
        .legs
        .iter()
        .zip(params.leg_deltas.iter())
        .enumerate()
    {
        let option_type_str = match leg.option_type {
            OptionType::Call => "call",
            OptionType::Put => "put",
        };

        let filtered = filters::filter_option_type(df, option_type_str)?;
        let with_dte = filters::compute_dte(&filtered)?;
        // For entry candidates, only consider rows where DTE is in the upper entry range.
        // The event loop handles exits via DTE check, so we don't need exit-range rows.
        // Use exit_dte + 1 as minimum to avoid entering trades that would immediately exit.
        let min_entry_dte = params.exit_dte + 1;
        let dte_filtered =
            filters::filter_dte_range(&with_dte, params.max_entry_dte, min_entry_dte)?;
        let valid = filters::filter_valid_quotes(&dte_filtered)?;
        let selected = filters::select_closest_delta(&valid, delta_target)?;

        if selected.height() == 0 {
            bail!(
                "No entry candidates found for leg {} of strategy '{}'",
                i,
                params.strategy
            );
        }

        // Rename columns to avoid conflicts when joining legs
        let renamed = selected
            .lazy()
            .rename(
                ["strike", "bid", "ask", "delta"],
                [
                    format!("strike_{i}"),
                    format!("bid_{i}"),
                    format!("ask_{i}"),
                    format!("delta_{i}"),
                ],
                true,
            )
            .collect()?;

        leg_dfs.push(renamed);
    }

    // Join all legs on (quote_datetime, expiration)
    let mut combined = leg_dfs[0].clone();
    for leg_df in leg_dfs.iter().skip(1) {
        let join_cols: Vec<&str> = vec![QUOTE_DATETIME_COL, "expiration"];
        combined = combined
            .lazy()
            .join(
                leg_df.clone().lazy(),
                join_cols.iter().map(|c| col(*c)).collect::<Vec<_>>(),
                join_cols.iter().map(|c| col(*c)).collect::<Vec<_>>(),
                JoinArgs::new(JoinType::Inner),
            )
            .collect()?;
    }

    if combined.height() == 0 {
        return Ok(BTreeMap::new());
    }

    // Apply strike ordering rules for multi-leg strategies
    let combined = rules::filter_strike_order(&combined, num_legs)?;

    if combined.height() == 0 {
        return Ok(BTreeMap::new());
    }

    // Convert to EntryCandidate structs grouped by date
    let quote_dates = combined.column(QUOTE_DATETIME_COL)?;
    let expirations = combined.column("expiration")?;

    let mut candidates: BTreeMap<NaiveDate, Vec<EntryCandidate>> = BTreeMap::new();

    for row_idx in 0..combined.height() {
        let entry_date = extract_date_from_column(quote_dates, row_idx)?;
        let exp_date = extract_date_from_column(expirations, row_idx)?;

        let mut legs = Vec::new();
        let mut net_premium = 0.0;

        for (i, leg_def) in strategy_def.legs.iter().enumerate() {
            let strike = combined
                .column(&format!("strike_{i}"))?
                .f64()?
                .get(row_idx)
                .unwrap_or(0.0);
            let bid = combined
                .column(&format!("bid_{i}"))?
                .f64()?
                .get(row_idx)
                .unwrap_or(0.0);
            let ask = combined
                .column(&format!("ask_{i}"))?
                .f64()?
                .get(row_idx)
                .unwrap_or(0.0);
            let delta = combined
                .column(&format!("delta_{i}"))?
                .f64()?
                .get(row_idx)
                .unwrap_or(0.0);

            let mid = f64::midpoint(bid, ask);
            // Long pays mid, short receives mid
            net_premium += mid * leg_def.side.multiplier() * f64::from(leg_def.qty);

            legs.push(CandidateLeg {
                option_type: leg_def.option_type,
                strike,
                expiration: exp_date,
                bid,
                ask,
                delta,
            });
        }

        candidates
            .entry(entry_date)
            .or_default()
            .push(EntryCandidate {
                entry_date,
                expiration: exp_date,
                legs,
                net_premium,
            });
    }

    Ok(candidates)
}

/// Run the event-driven simulation loop.
pub fn run_event_loop(
    price_table: &PriceTable,
    candidates: &BTreeMap<NaiveDate, Vec<EntryCandidate>>,
    trading_days: &[NaiveDate],
    params: &BacktestParams,
    strategy_def: &StrategyDef,
) -> (Vec<TradeRecord>, Vec<EquityPoint>) {
    let mut positions: Vec<Position> = Vec::new();
    let mut trade_log: Vec<TradeRecord> = Vec::new();
    let mut equity_curve: Vec<EquityPoint> = Vec::new();

    let mut realized_equity = params.capital;
    let mut next_id = 1usize;
    let mut trade_id = 0usize;

    // Last known prices for carry-forward on gaps
    let mut last_known: HashMap<(NaiveDate, OrderedFloat<f64>, OptionType), QuoteSnapshot> =
        HashMap::new();

    for &today in trading_days {
        // Phase 1: Check exits on open positions
        let mut i = 0;
        while i < positions.len() {
            if !matches!(positions[i].status, PositionStatus::Open) {
                i += 1;
                continue;
            }

            let exit_type =
                check_exit_triggers(&positions[i], today, price_table, &last_known, params);

            if let Some(exit_type) = exit_type {
                let pnl = close_position(
                    &mut positions[i],
                    today,
                    price_table,
                    &last_known,
                    &params.slippage,
                    &params.commission.clone().unwrap_or_default(),
                    exit_type.clone(),
                );
                realized_equity += pnl;

                trade_id += 1;
                let entry_dt = positions[i].entry_date.and_hms_opt(0, 0, 0).unwrap();
                let exit_dt = today.and_hms_opt(0, 0, 0).unwrap();
                let days_held = (today - positions[i].entry_date).num_days();

                trade_log.push(TradeRecord {
                    trade_id,
                    entry_datetime: entry_dt,
                    exit_datetime: exit_dt,
                    entry_cost: positions[i].entry_cost,
                    exit_proceeds: positions[i].entry_cost + pnl,
                    pnl,
                    days_held,
                    exit_type,
                });
            }

            i += 1;
        }

        // Remove closed positions
        positions.retain(|p| matches!(p.status, PositionStatus::Open));

        // Phase 2: Enter new positions
        let open_count = positions.len();
        #[allow(clippy::cast_sign_loss)]
        if open_count < params.max_positions as usize {
            if let Some(day_candidates) = candidates.get(&today) {
                // Filter out candidates with expirations we already hold
                let available: Vec<&EntryCandidate> = day_candidates
                    .iter()
                    .filter(|c| {
                        !positions.iter().any(|p| {
                            matches!(p.status, PositionStatus::Open) && p.expiration == c.expiration
                        })
                    })
                    .collect();
                if let Some(candidate) = select_candidate(&available, &params.selector) {
                    let position = open_position(candidate, today, strategy_def, params, next_id);
                    next_id += 1;
                    positions.push(position);
                }
            }
        }

        // Update last known prices for all quotes on this day
        update_last_known(price_table, today, &mut last_known);

        // Phase 3: Daily mark-to-market
        let unrealized: f64 = positions
            .iter()
            .filter(|p| matches!(p.status, PositionStatus::Open))
            .map(|p| {
                mark_to_market(
                    p,
                    today,
                    price_table,
                    &last_known,
                    &params.slippage,
                    params.multiplier,
                )
            })
            .sum();

        equity_curve.push(EquityPoint {
            datetime: today.and_hms_opt(0, 0, 0).unwrap(),
            equity: realized_equity + unrealized,
        });
    }

    (trade_log, equity_curve)
}

/// Check if any exit trigger fires for a position on the given day.
fn check_exit_triggers(
    position: &Position,
    today: NaiveDate,
    price_table: &PriceTable,
    last_known: &HashMap<(NaiveDate, OrderedFloat<f64>, OptionType), QuoteSnapshot>,
    params: &BacktestParams,
) -> Option<ExitType> {
    // Expiration check
    if today >= position.expiration {
        return Some(ExitType::Expiration);
    }

    // DTE exit check
    let dte = (position.expiration - today).num_days();
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
    let mtm = mark_to_market(
        position,
        today,
        price_table,
        last_known,
        &params.slippage,
        params.multiplier,
    );

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

    None
}

/// Calculate unrealized P&L for a position at current market prices.
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

/// Close a position, setting leg close prices from current market.
/// Returns realized P&L for the position.
fn close_position(
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
        total_contracts += leg.qty.abs();

        leg.closed = true;
        leg.close_price = Some(close_price);
        leg.close_date = Some(date);
    }

    // Apply commission (entry + exit)
    pnl -= commission.calculate(total_contracts) * 2.0;

    position.status = PositionStatus::Closed(exit_type);
    pnl
}

/// Create a new position from an entry candidate.
fn open_position(
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
        legs,
        entry_cost,
        quantity: params.quantity,
        multiplier: params.multiplier,
        status: PositionStatus::Open,
    }
}

/// Select the best candidate based on `TradeSelector`.
fn select_candidate<'a>(
    candidates: &[&'a EntryCandidate],
    selector: &TradeSelector,
) -> Option<&'a EntryCandidate> {
    if candidates.is_empty() {
        return None;
    }

    match selector {
        TradeSelector::First | TradeSelector::Nearest => candidates.first().copied(),
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

/// Update the last-known price cache for carry-forward on gaps.
fn update_last_known(
    price_table: &PriceTable,
    today: NaiveDate,
    last_known: &mut HashMap<(NaiveDate, OrderedFloat<f64>, OptionType), QuoteSnapshot>,
) {
    for (key, snap) in price_table {
        if key.0 == today {
            let carry_key = (key.1, key.2, key.3);
            last_known.insert(carry_key, snap.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_price_table_simple() -> (PriceTable, Vec<NaiveDate>) {
        let mut table = PriceTable::new();
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
        (table, days)
    }

    #[test]
    fn build_price_table_basic() {
        let df = make_daily_df();
        let (table, days) = build_price_table(&df).unwrap();
        assert!(!table.is_empty());
        assert!(!days.is_empty());
        // Verify a specific key lookup
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let key = (d1, exp, OrderedFloat(100.0), OptionType::Call);
        assert!(table.contains_key(&key));
        let snap = table.get(&key).unwrap();
        assert!((snap.bid - 5.0).abs() < 1e-10);
    }

    #[test]
    fn mark_to_market_long_call() {
        let (table, _) = make_price_table_simple();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 22).unwrap();
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let last_known = HashMap::new();

        let position = Position {
            id: 1,
            entry_date: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            expiration: exp,
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
        let mut table = PriceTable::new();
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
        let (table, _) = make_price_table_simple();
        let d3 = NaiveDate::from_ymd_opt(2024, 1, 29).unwrap();
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let last_known = HashMap::new();
        let commission = Commission::default();

        let mut position = Position {
            id: 1,
            entry_date: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            expiration: exp,
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

    #[test]
    fn run_event_loop_single_trade() {
        let (table, days) = make_price_table_simple();
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();

        let mut candidates = BTreeMap::new();
        candidates.insert(
            d1,
            vec![EntryCandidate {
                entry_date: d1,
                expiration: exp,
                legs: vec![CandidateLeg {
                    option_type: OptionType::Call,
                    strike: 100.0,
                    expiration: exp,
                    bid: 5.0,
                    ask: 5.50,
                    delta: 0.50,
                }],
                net_premium: -5.25,
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
            max_entry_dte: 45,
            exit_dte: 15, // DTE exit at 15 days
            slippage: Slippage::Mid,
            commission: None,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
        };

        let (_trade_log, equity_curve) =
            run_event_loop(&table, &candidates, &days, &params, &strategy_def);

        // Should have 1 trade, closed by DTE exit on day 3 (DTE = 18, which is > 15, so no DTE exit)
        // Actually DTE on Jan 29 = Feb 16 - Jan 29 = 18 days, exit_dte=15, so 18 > 15 → no DTE exit
        // Trade stays open through all 3 days
        // The trade will only close if DTE <= exit_dte, which doesn't happen in our 3-day window
        assert_eq!(equity_curve.len(), 3, "Should have 3 equity points");
    }

    #[test]
    fn run_event_loop_stop_loss() {
        let mut table = PriceTable::new();
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 16).unwrap();
        let d3 = NaiveDate::from_ymd_opt(2024, 1, 17).unwrap();
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();

        // Entry day: price = 5.0/5.50
        table.insert(
            (d1, exp, OrderedFloat(100.0), OptionType::Call),
            QuoteSnapshot {
                bid: 5.0,
                ask: 5.50,
                delta: 0.50,
            },
        );
        // Day 2: small drop
        table.insert(
            (d2, exp, OrderedFloat(100.0), OptionType::Call),
            QuoteSnapshot {
                bid: 4.0,
                ask: 4.50,
                delta: 0.45,
            },
        );
        // Day 3: big drop → stop loss should fire
        table.insert(
            (d3, exp, OrderedFloat(100.0), OptionType::Call),
            QuoteSnapshot {
                bid: 1.0,
                ask: 1.50,
                delta: 0.15,
            },
        );

        let days = vec![d1, d2, d3];
        let mut candidates = BTreeMap::new();
        candidates.insert(
            d1,
            vec![EntryCandidate {
                entry_date: d1,
                expiration: exp,
                legs: vec![CandidateLeg {
                    option_type: OptionType::Call,
                    strike: 100.0,
                    expiration: exp,
                    bid: 5.0,
                    ask: 5.50,
                    delta: 0.50,
                }],
                net_premium: -5.25,
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
            max_entry_dte: 45,
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            stop_loss: Some(0.50), // 50% stop loss
            take_profit: None,
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
        };

        let (trade_log, _) = run_event_loop(&table, &candidates, &days, &params, &strategy_def);

        // Stop loss: entry_cost = 5.25 * 100 = 525, threshold = 525 * 0.5 = 262.5
        // Day 2 MTM: (4.25 - 5.25) * 100 = -100 → no trigger
        // Day 3 MTM: (1.25 - 5.25) * 100 = -400 → exceeds -262.5 → stop loss fires
        assert_eq!(trade_log.len(), 1);
        assert!(
            matches!(trade_log[0].exit_type, ExitType::StopLoss),
            "Expected StopLoss, got {:?}",
            trade_log[0].exit_type
        );
    }

    #[test]
    fn run_event_loop_take_profit() {
        let mut table = PriceTable::new();
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
        // Big jump → take profit
        table.insert(
            (d3, exp, OrderedFloat(100.0), OptionType::Call),
            QuoteSnapshot {
                bid: 10.0,
                ask: 10.50,
                delta: 0.70,
            },
        );

        let days = vec![d1, d2, d3];
        let mut candidates = BTreeMap::new();
        candidates.insert(
            d1,
            vec![EntryCandidate {
                entry_date: d1,
                expiration: exp,
                legs: vec![CandidateLeg {
                    option_type: OptionType::Call,
                    strike: 100.0,
                    expiration: exp,
                    bid: 5.0,
                    ask: 5.50,
                    delta: 0.50,
                }],
                net_premium: -5.25,
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
            max_entry_dte: 45,
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            stop_loss: None,
            take_profit: Some(0.50), // 50% take profit
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
        };

        let (trade_log, _) = run_event_loop(&table, &candidates, &days, &params, &strategy_def);

        // Take profit: entry_cost = 525, threshold = 525 * 0.5 = 262.5
        // Day 2 MTM: (6.25 - 5.25) * 100 = 100 → no trigger
        // Day 3 MTM: (10.25 - 5.25) * 100 = 500 → exceeds 262.5 → take profit
        assert_eq!(trade_log.len(), 1);
        assert!(
            matches!(trade_log[0].exit_type, ExitType::TakeProfit),
            "Expected TakeProfit, got {:?}",
            trade_log[0].exit_type
        );
    }

    #[test]
    fn run_event_loop_max_positions() {
        let mut table = PriceTable::new();
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

        // Candidates on both days
        let make_cand = |date: NaiveDate, strike: f64, bid: f64, ask: f64| -> EntryCandidate {
            EntryCandidate {
                entry_date: date,
                expiration: exp,
                legs: vec![CandidateLeg {
                    option_type: OptionType::Call,
                    strike,
                    expiration: exp,
                    bid,
                    ask,
                    delta: 0.50,
                }],
                net_premium: -(bid + ask) / 2.0,
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
            max_entry_dte: 45,
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            multiplier: 100,
            max_positions: 1, // Only 1 position allowed
            selector: TradeSelector::First,
            adjustment_rules: vec![],
        };

        let (trade_log, _) = run_event_loop(&table, &candidates, &days, &params, &strategy_def);

        // Max positions = 1, first position stays open, second rejected
        assert_eq!(trade_log.len(), 0, "No trades should close in 2 days");
    }

    #[test]
    fn run_event_loop_daily_equity() {
        let (table, days) = make_price_table_simple();
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();

        let mut candidates = BTreeMap::new();
        candidates.insert(
            d1,
            vec![EntryCandidate {
                entry_date: d1,
                expiration: exp,
                legs: vec![CandidateLeg {
                    option_type: OptionType::Call,
                    strike: 100.0,
                    expiration: exp,
                    bid: 5.0,
                    ask: 5.50,
                    delta: 0.50,
                }],
                net_premium: -5.25,
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
            max_entry_dte: 45,
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
        };

        let (_, equity_curve) = run_event_loop(&table, &candidates, &days, &params, &strategy_def);

        // Should have one equity point per trading day
        assert_eq!(
            equity_curve.len(),
            days.len(),
            "One equity point per trading day"
        );

        // Day 1: just entered, MTM = 0 (entry price = current price)
        // Actually on entry day, position is opened after MTM phase, so MTM includes the position
        // Entry at mid 5.25. Current price on d1 is also mid 5.25. MTM = 0.
        assert!(
            (equity_curve[0].equity - 10000.0).abs() < 1e-10,
            "Day 1 equity should be 10000, got {}",
            equity_curve[0].equity
        );

        // Day 2: mid = 3.25, MTM = (3.25 - 5.25) * 100 = -200
        assert!(
            (equity_curve[1].equity - 9800.0).abs() < 1e-10,
            "Day 2 equity should be 9800, got {}",
            equity_curve[1].equity
        );

        // Day 3: mid = 2.25, MTM = (2.25 - 5.25) * 100 = -300
        assert!(
            (equity_curve[2].equity - 9700.0).abs() < 1e-10,
            "Day 3 equity should be 9700, got {}",
            equity_curve[2].equity
        );
    }

    /// Helper: build a synthetic daily options `DataFrame` for testing `build_price_table`
    /// and `find_entry_candidates`.
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
            QUOTE_DATETIME_COL => &quote_dates,
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
            max_entry_dte: 45,
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
        };

        let candidates = find_entry_candidates(&df, &strategy_def, &params).unwrap();
        assert!(
            !candidates.is_empty(),
            "Should find at least one date with candidates"
        );

        // Each date group should have exactly 1 candidate (1 strike per date)
        for cands in candidates.values() {
            assert_eq!(cands[0].legs.len(), 1);
        }
    }
}
