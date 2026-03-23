//! Wheel strategy simulation: sell puts -> assignment -> sell covered calls -> repeat.
//!
//! Provides single-leg candidate finding for puts and calls, which is the
//! building block for the wheel strategy's state machine. The event loop
//! transitions through three states:
//!
//! 1. **`SellingPuts`** — sell a short put; if it expires OTM we keep premium,
//!    if ITM we get assigned stock.
//! 2. **`HoldingStock`** — we own shares, looking for a call to sell.
//! 3. **`SellingCalls`** — sell a covered call; if OTM we keep premium and
//!    return to `HoldingStock`, if ITM we are called away and return to `SellingPuts`.

use std::collections::{BTreeMap, HashSet};

use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::*;

use super::filters;
use super::price_table::extract_date_from_column;
use super::pricing;
use super::types::{
    BacktestQualityStats, Commission, DteRange, EquityPoint, ExitType, LegDetail, OptionType, Side,
    Slippage, TargetRange, TradeRecord,
};
use crate::data::parquet::DATETIME_COL;
use crate::tools::response_types::wheel::WheelCycle;

/// A single-leg option candidate for wheel entry.
#[derive(Debug, Clone)]
pub struct SingleLegCandidate {
    pub date: NaiveDate,
    pub expiration: NaiveDate,
    pub strike: f64,
    pub bid: f64,
    pub ask: f64,
    pub delta: f64,
    pub option_type: OptionType,
}

// ---------------------------------------------------------------------------
// State types
// ---------------------------------------------------------------------------

/// Current phase of the wheel strategy state machine.
#[derive(Debug, Clone, PartialEq)]
pub enum WheelState {
    /// Selling cash-secured puts, waiting for assignment or OTM expiry.
    SellingPuts,
    /// Holding assigned stock, looking to sell a covered call.
    HoldingStock,
    /// Sold a covered call on held stock, waiting for expiry.
    SellingCalls,
}

/// Tracks shares acquired via put assignment.
///
/// Two cost basis values:
/// - `entry_price`: raw strike price, used for stock P&L calculation (no double-counting)
/// - `adjusted_basis`: rolling value (strike - put premium - call premiums), used for
///   the `min_call_strike_at_cost` floor and stop-loss trigger
#[derive(Debug, Clone)]
pub struct StockHolding {
    /// Raw assignment strike — used for stock P&L calculation.
    pub entry_price: f64,
    /// Rolling adjusted basis — decreases with each premium collected.
    /// Used for call strike floor and stop-loss trigger.
    pub adjusted_basis: f64,
    pub entry_date: NaiveDate,
    pub quantity: i32,
    pub multiplier: i32,
}

/// Engine-level parameters for the wheel simulation (converted from MCP params).
pub struct WheelParams {
    pub put_delta: TargetRange,
    pub put_dte: DteRange,
    pub call_delta: TargetRange,
    pub call_dte: DteRange,
    /// When true, only sell calls at or above the cost basis.
    pub min_call_strike_at_cost: bool,
    pub capital: f64,
    pub quantity: i32,
    pub multiplier: i32,
    pub slippage: Slippage,
    pub commission: Option<Commission>,
    /// Fraction of stock value; e.g. 0.10 = stop if stock drops 10% below cost basis.
    pub stop_loss: Option<f64>,
    /// Minimum bid/ask filter for option candidates.
    pub min_bid_ask: f64,
}

/// Tracks a single put → (assignment → call(s)) → called-away cycle.
#[derive(Debug, Clone)]
struct CycleTracker {
    cycle_id: usize,
    put_entry_date: NaiveDate,
    put_strike: f64,
    put_premium: f64,
    put_pnl: f64,
    assigned: bool,
    cost_basis: Option<f64>,
    call_pnls: Vec<f64>,
    call_premiums: Vec<f64>,
    stock_pnl: Option<f64>,
    called_away_date: Option<NaiveDate>,
    called_away_strike: Option<f64>,
}

/// Active option position tracked by the wheel loop (lighter than `Position`).
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ActiveOption {
    entry_date: NaiveDate,
    expiration: NaiveDate,
    strike: f64,
    fill_price: f64,
    option_type: OptionType,
    side: Side,
}

/// Result of a wheel backtest run.
pub struct WheelResult {
    pub trade_log: Vec<TradeRecord>,
    pub equity_curve: Vec<EquityPoint>,
    pub cycles: Vec<WheelCycle>,
    pub quality: BacktestQualityStats,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Check whether today is a valid entry date (if signal dates are provided).
fn signal_allows(today: NaiveDate, entry_dates: Option<&HashSet<NaiveDate>>) -> bool {
    entry_dates.is_none_or(|dates| dates.contains(&today))
}

/// Build a `TradeRecord` for a wheel option trade.
#[allow(clippy::too_many_arguments)]
fn wheel_trade_record(
    trade_id: usize,
    entry_date: NaiveDate,
    exit_date: NaiveDate,
    entry_price: f64,
    exit_value: f64,
    pnl: f64,
    exit_type: ExitType,
    option_type: OptionType,
    strike: f64,
    expiration: NaiveDate,
    qty: i32,
    multiplier: i32,
) -> TradeRecord {
    let entry_dt = entry_date
        .and_hms_opt(0, 0, 0)
        .expect("and_hms_opt should never fail");
    let exit_dt = exit_date
        .and_hms_opt(0, 0, 0)
        .expect("and_hms_opt should never fail");
    let days_held = (exit_date - entry_date).num_days();

    // For short options: entry_cost is negative (credit received)
    let entry_cost = -entry_price * f64::from(qty) * f64::from(multiplier);
    let exit_proceeds = entry_cost + pnl;

    TradeRecord::new(
        trade_id,
        entry_dt,
        exit_dt,
        entry_cost,
        exit_proceeds,
        pnl,
        days_held,
        exit_type,
        vec![LegDetail {
            side: Side::Short,
            option_type,
            strike,
            expiration: expiration.to_string(),
            entry_price,
            exit_price: Some(exit_value),
            qty,
        }],
    )
}

// ---------------------------------------------------------------------------
// Main event loop
// ---------------------------------------------------------------------------

/// Run the wheel strategy backtest.
///
/// Transitions: `SellingPuts` → `HoldingStock` → `SellingCalls` → repeat.
/// Options are normally held to expiration; however a stop-loss on the underlying
/// stock can force-close an active covered call early, and end-of-data can close
/// options before their expiration date.
#[allow(clippy::too_many_lines, clippy::implicit_hasher)]
pub fn run_wheel_backtest(
    options_df: &DataFrame,
    ohlcv_closes: &BTreeMap<NaiveDate, f64>,
    params: &WheelParams,
    entry_dates: Option<&HashSet<NaiveDate>>,
    trading_days: &[NaiveDate],
) -> Result<WheelResult> {
    let commission = params.commission.clone().unwrap_or_default();
    let qty = params.quantity;
    let mult = params.multiplier;
    let qty_f = f64::from(qty);
    let mult_f = f64::from(mult);

    // Pre-build put candidates (call candidates depend on per-cycle cost basis)
    let put_candidates = find_put_candidates(
        options_df,
        &params.put_delta,
        &params.put_dte,
        params.min_bid_ask,
    )?;

    // Pre-build call candidates when min_call_strike_at_cost is false (no cost basis filter).
    // When true, we rebuild per-assignment with the actual cost basis as min_strike.
    let mut call_candidates = if params.min_call_strike_at_cost {
        BTreeMap::new() // will be populated per-assignment
    } else {
        find_call_candidates(
            options_df,
            &params.call_delta,
            &params.call_dte,
            params.min_bid_ask,
            None,
        )?
    };

    let mut state = WheelState::SellingPuts;
    let mut active_option: Option<ActiveOption> = None;
    let mut stock_holding: Option<StockHolding> = None;
    let mut realized_pnl = 0.0_f64;
    let mut trade_id = 0_usize;

    let mut trade_log: Vec<TradeRecord> = Vec::new();
    let mut equity_curve: Vec<EquityPoint> = Vec::with_capacity(trading_days.len());
    let mut cycles: Vec<WheelCycle> = Vec::new();
    let mut cycle_tracker: Option<CycleTracker> = None;
    let mut next_cycle_id = 1_usize;

    // Quality tracking
    let mut last_known_close: Option<f64> = None;

    for &today in trading_days {
        // Carry-forward underlying close
        let underlying_close = if let Some(&px) = ohlcv_closes.get(&today) {
            last_known_close = Some(px);
            px
        } else if let Some(px) = last_known_close {
            px
        } else {
            // No price data yet — skip day
            equity_curve.push(EquityPoint {
                datetime: today
                    .and_hms_opt(0, 0, 0)
                    .expect("and_hms_opt should never fail"),
                equity: params.capital + realized_pnl,
            });
            continue;
        };

        // ==== Phase 1: Check option expiration ====
        if let Some(ref opt) = active_option {
            if opt.expiration == today {
                match state {
                    WheelState::SellingPuts => {
                        let itm = underlying_close <= opt.strike;
                        if itm {
                            // Assigned: stock acquired at strike price.
                            // Put premium is separate realized P&L (not folded into cost basis).
                            let premium_pnl = opt.fill_price * qty_f * mult_f;
                            let comm = commission.calculate(qty) * 2.0;
                            let option_pnl = premium_pnl - comm;
                            realized_pnl += option_pnl;

                            // Rolling cost basis: strike minus put premium per share
                            let cost_basis = opt.strike - opt.fill_price;

                            trade_id += 1;
                            trade_log.push(wheel_trade_record(
                                trade_id,
                                opt.entry_date,
                                today,
                                opt.fill_price,
                                0.0, // option expires ITM, exercised
                                option_pnl,
                                ExitType::Assignment,
                                OptionType::Put,
                                opt.strike,
                                opt.expiration,
                                qty,
                                mult,
                            ));

                            stock_holding = Some(StockHolding {
                                entry_price: opt.strike,
                                adjusted_basis: cost_basis,
                                entry_date: today,
                                quantity: qty,
                                multiplier: mult,
                            });

                            // Rebuild call candidates with cost basis floor
                            if params.min_call_strike_at_cost {
                                call_candidates = find_call_candidates(
                                    options_df,
                                    &params.call_delta,
                                    &params.call_dte,
                                    params.min_bid_ask,
                                    Some(cost_basis),
                                )?;
                            }

                            // Start cycle tracker (cost_basis stores adjusted_basis for display)
                            if let Some(ref mut ct) = cycle_tracker {
                                ct.put_pnl = option_pnl;
                                ct.assigned = true;
                                ct.cost_basis = Some(cost_basis); // adjusted_basis value
                            }

                            active_option = None;
                            state = WheelState::HoldingStock;
                        } else {
                            // OTM: keep full premium
                            let premium_pnl = opt.fill_price * qty_f * mult_f;
                            let comm = commission.calculate(qty) * 2.0;
                            let option_pnl = premium_pnl - comm;
                            realized_pnl += option_pnl;

                            trade_id += 1;
                            trade_log.push(wheel_trade_record(
                                trade_id,
                                opt.entry_date,
                                today,
                                opt.fill_price,
                                0.0,
                                option_pnl,
                                ExitType::Expiration,
                                OptionType::Put,
                                opt.strike,
                                opt.expiration,
                                qty,
                                mult,
                            ));

                            // Complete cycle as put-only
                            if let Some(mut ct) = cycle_tracker.take() {
                                ct.put_pnl = option_pnl;
                                ct.assigned = false;
                                let total_pnl = ct.put_pnl;
                                let days = (today - ct.put_entry_date).num_days() as i32;
                                cycles.push(WheelCycle {
                                    cycle_id: ct.cycle_id,
                                    put_entry_date: ct.put_entry_date.to_string(),
                                    put_strike: ct.put_strike,
                                    put_premium: ct.put_premium,
                                    put_pnl: ct.put_pnl,
                                    assigned: false,
                                    cost_basis: None,
                                    calls_sold: 0,
                                    call_pnls: vec![],
                                    call_premiums: vec![],
                                    stock_pnl: None,
                                    called_away_date: None,
                                    called_away_strike: None,
                                    total_pnl,
                                    total_premium: ct.put_premium,
                                    days_in_cycle: days,
                                });
                            }

                            active_option = None;
                            // Stay in SellingPuts
                        }
                    }
                    WheelState::SellingCalls => {
                        let itm = underlying_close >= opt.strike;
                        let call_premium_pnl = opt.fill_price * qty_f * mult_f;
                        let comm = commission.calculate(qty) * 2.0;
                        let call_pnl = call_premium_pnl - comm;
                        realized_pnl += call_pnl;

                        if itm {
                            // Called away
                            let sh = stock_holding
                                .as_ref()
                                .expect("must hold stock in SellingCalls");
                            let stock_pnl = (opt.strike - sh.entry_price) * qty_f * mult_f;
                            realized_pnl += stock_pnl;

                            trade_id += 1;
                            let mut record = wheel_trade_record(
                                trade_id,
                                opt.entry_date,
                                today,
                                opt.fill_price,
                                0.0,
                                call_pnl + stock_pnl,
                                ExitType::CalledAway,
                                OptionType::Call,
                                opt.strike,
                                opt.expiration,
                                qty,
                                mult,
                            );
                            record.stock_entry_price = Some(sh.entry_price);
                            record.stock_exit_price = Some(opt.strike);
                            record.stock_pnl = Some(stock_pnl);
                            trade_log.push(record);

                            // Complete cycle
                            if let Some(mut ct) = cycle_tracker.take() {
                                ct.call_pnls.push(call_pnl);
                                ct.call_premiums.push(opt.fill_price * qty_f * mult_f);
                                ct.stock_pnl = Some(stock_pnl);
                                ct.called_away_date = Some(today);
                                ct.called_away_strike = Some(opt.strike);

                                let total_premium: f64 =
                                    ct.put_premium + ct.call_premiums.iter().sum::<f64>();
                                let total_pnl =
                                    ct.put_pnl + ct.call_pnls.iter().sum::<f64>() + stock_pnl;
                                let days = (today - ct.put_entry_date).num_days() as i32;

                                cycles.push(WheelCycle {
                                    cycle_id: ct.cycle_id,
                                    put_entry_date: ct.put_entry_date.to_string(),
                                    put_strike: ct.put_strike,
                                    put_premium: ct.put_premium,
                                    put_pnl: ct.put_pnl,
                                    assigned: true,
                                    cost_basis: ct.cost_basis,
                                    calls_sold: ct.call_pnls.len(),
                                    call_pnls: ct.call_pnls,
                                    call_premiums: ct.call_premiums,
                                    stock_pnl: Some(stock_pnl),
                                    called_away_date: Some(today.to_string()),
                                    called_away_strike: Some(opt.strike),
                                    total_pnl,
                                    total_premium,
                                    days_in_cycle: days,
                                });
                            }

                            stock_holding = None;
                            active_option = None;
                            state = WheelState::SellingPuts;
                        } else {
                            // OTM: keep call premium, still holding stock
                            trade_id += 1;
                            trade_log.push(wheel_trade_record(
                                trade_id,
                                opt.entry_date,
                                today,
                                opt.fill_price,
                                0.0,
                                call_pnl,
                                ExitType::Expiration,
                                OptionType::Call,
                                opt.strike,
                                opt.expiration,
                                qty,
                                mult,
                            ));

                            if let Some(ref mut ct) = cycle_tracker {
                                ct.call_pnls.push(call_pnl);
                                ct.call_premiums.push(opt.fill_price * qty_f * mult_f);
                            }

                            // Rolling adjusted basis: reduce by call premium per share
                            if let Some(ref mut sh) = stock_holding {
                                sh.adjusted_basis -= opt.fill_price;
                                // Update cycle tracker with new adjusted basis
                                if let Some(ref mut ct) = cycle_tracker {
                                    ct.cost_basis = Some(sh.adjusted_basis);
                                }
                                // Rebuild call candidates if using cost basis floor
                                if params.min_call_strike_at_cost {
                                    call_candidates = find_call_candidates(
                                        options_df,
                                        &params.call_delta,
                                        &params.call_dte,
                                        params.min_bid_ask,
                                        Some(sh.adjusted_basis),
                                    )?;
                                }
                            }

                            active_option = None;
                            state = WheelState::HoldingStock;
                        }
                    }
                    WheelState::HoldingStock => {
                        // Should not have an active option in HoldingStock without SellingCalls
                        // but handle gracefully
                        active_option = None;
                    }
                }
            }
        }

        // ==== Phase 2: Stop loss check ====
        if let (Some(ref sh), Some(sl_pct)) = (&stock_holding, params.stop_loss) {
            if underlying_close < sh.adjusted_basis * (1.0 - sl_pct) {
                // Close active call if any
                if let Some(ref opt) = active_option {
                    // Force close the call at intrinsic value approximation
                    let call_intrinsic = (underlying_close - opt.strike).max(0.0);
                    let call_pnl = (opt.fill_price - call_intrinsic) * qty_f * mult_f
                        - commission.calculate(qty) * 2.0;
                    realized_pnl += call_pnl;

                    trade_id += 1;
                    trade_log.push(wheel_trade_record(
                        trade_id,
                        opt.entry_date,
                        today,
                        opt.fill_price,
                        call_intrinsic,
                        call_pnl,
                        ExitType::StopLoss,
                        opt.option_type,
                        opt.strike,
                        opt.expiration,
                        qty,
                        mult,
                    ));

                    if let Some(ref mut ct) = cycle_tracker {
                        ct.call_pnls.push(call_pnl);
                        ct.call_premiums.push(opt.fill_price * qty_f * mult_f);
                    }
                }
                active_option = None;

                // Sell stock at market
                let stock_pnl = (underlying_close - sh.entry_price) * qty_f * mult_f;
                realized_pnl += stock_pnl;

                // Complete cycle with stop loss
                if let Some(mut ct) = cycle_tracker.take() {
                    ct.stock_pnl = Some(stock_pnl);
                    let total_premium: f64 = ct.put_premium + ct.call_premiums.iter().sum::<f64>();
                    let total_pnl = ct.put_pnl + ct.call_pnls.iter().sum::<f64>() + stock_pnl;
                    let days = (today - ct.put_entry_date).num_days() as i32;

                    cycles.push(WheelCycle {
                        cycle_id: ct.cycle_id,
                        put_entry_date: ct.put_entry_date.to_string(),
                        put_strike: ct.put_strike,
                        put_premium: ct.put_premium,
                        put_pnl: ct.put_pnl,
                        assigned: true,
                        cost_basis: ct.cost_basis,
                        calls_sold: ct.call_pnls.len(),
                        call_pnls: ct.call_pnls,
                        call_premiums: ct.call_premiums,
                        stock_pnl: Some(stock_pnl),
                        called_away_date: None,
                        called_away_strike: None,
                        total_pnl,
                        total_premium,
                        days_in_cycle: days,
                    });
                }

                stock_holding = None;
                state = WheelState::SellingPuts;
            }
        }

        // ==== Phase 3: Open new option ====
        if active_option.is_none() && signal_allows(today, entry_dates) {
            match state {
                WheelState::SellingPuts => {
                    if let Some(day_cands) = put_candidates.get(&today) {
                        if let Some(cand) = day_cands.first() {
                            // Capital check: need enough to be assigned
                            let stock_value = stock_holding.as_ref().map_or(0.0, |sh| {
                                sh.entry_price * f64::from(sh.quantity) * f64::from(sh.multiplier)
                            });
                            let available_capital = params.capital + realized_pnl - stock_value;
                            let required = cand.strike * qty_f * mult_f;

                            if required <= available_capital {
                                let fp = pricing::fill_price(
                                    cand.bid,
                                    cand.ask,
                                    Side::Short,
                                    &params.slippage,
                                );
                                active_option = Some(ActiveOption {
                                    entry_date: today,
                                    expiration: cand.expiration,
                                    strike: cand.strike,
                                    fill_price: fp,
                                    option_type: OptionType::Put,
                                    side: Side::Short,
                                });

                                // Start a new cycle
                                cycle_tracker = Some(CycleTracker {
                                    cycle_id: next_cycle_id,
                                    put_entry_date: today,
                                    put_strike: cand.strike,
                                    put_premium: fp * qty_f * mult_f,
                                    put_pnl: 0.0,
                                    assigned: false,
                                    cost_basis: None,
                                    call_pnls: vec![],
                                    call_premiums: vec![],
                                    stock_pnl: None,
                                    called_away_date: None,
                                    called_away_strike: None,
                                });
                                next_cycle_id += 1;
                            }
                        }
                    }
                }
                WheelState::HoldingStock => {
                    if let Some(day_cands) = call_candidates.get(&today) {
                        if let Some(cand) = day_cands.first() {
                            let fp = pricing::fill_price(
                                cand.bid,
                                cand.ask,
                                Side::Short,
                                &params.slippage,
                            );
                            active_option = Some(ActiveOption {
                                entry_date: today,
                                expiration: cand.expiration,
                                strike: cand.strike,
                                fill_price: fp,
                                option_type: OptionType::Call,
                                side: Side::Short,
                            });
                            state = WheelState::SellingCalls;
                        }
                    }
                }
                WheelState::SellingCalls => {
                    // Should not reach here — we have no active option but state says SellingCalls
                    // Transition back to HoldingStock to try selling a call
                    state = WheelState::HoldingStock;
                }
            }
        }

        // ==== Phase 4: Mark-to-market equity ====
        let unrealized_stock = stock_holding.as_ref().map_or(0.0, |sh| {
            (underlying_close - sh.entry_price) * f64::from(sh.quantity) * f64::from(sh.multiplier)
        });
        // For the active option, we don't have a live price table lookup here,
        // so we approximate: the option's unrealized P&L is already reflected
        // through the premium received (which is part of realized_pnl upon close).
        // We treat the option as held-to-expiration so unrealized option = 0.
        let equity = params.capital + realized_pnl + unrealized_stock;

        equity_curve.push(EquityPoint {
            datetime: today
                .and_hms_opt(0, 0, 0)
                .expect("and_hms_opt should never fail"),
            equity,
        });
    }

    // ==== End of data: force close open positions ====
    #[allow(unused_assignments)]
    if let Some(ref opt) = active_option {
        let last_day = trading_days.last().copied().unwrap_or(opt.expiration);
        // Use last known close to compute intrinsic value approximation
        let last_close_for_opt = last_known_close.unwrap_or(opt.strike);
        let intrinsic = match opt.option_type {
            OptionType::Put => (opt.strike - last_close_for_opt).max(0.0),
            OptionType::Call => (last_close_for_opt - opt.strike).max(0.0),
        };
        let comm = commission.calculate(qty) * 2.0;
        let option_pnl = (opt.fill_price - intrinsic) * qty_f * mult_f - comm;
        realized_pnl += option_pnl;

        trade_id += 1;
        trade_log.push(wheel_trade_record(
            trade_id,
            opt.entry_date,
            last_day,
            opt.fill_price,
            intrinsic,
            option_pnl,
            ExitType::MaxHold,
            opt.option_type,
            opt.strike,
            opt.expiration,
            qty,
            mult,
        ));

        if let Some(ref mut ct) = cycle_tracker {
            if opt.option_type == OptionType::Put {
                ct.put_pnl = option_pnl;
            } else {
                ct.call_pnls.push(option_pnl);
                ct.call_premiums.push(opt.fill_price * qty_f * mult_f);
            }
        }
    }

    #[allow(unused_assignments)]
    if let Some(ref sh) = stock_holding {
        let last_day = *trading_days.last().unwrap_or(&sh.entry_date);
        let last_close = ohlcv_closes
            .range(..=last_day)
            .next_back()
            .map_or(sh.entry_price, |(_, &v)| v);
        let stock_pnl = (last_close - sh.entry_price) * qty_f * mult_f;
        realized_pnl += stock_pnl;

        if let Some(ref mut ct) = cycle_tracker {
            ct.stock_pnl = Some(stock_pnl);
        }
    }

    // Flush remaining cycle
    if let Some(ct) = cycle_tracker.take() {
        let last_day = trading_days.last().copied().unwrap_or(ct.put_entry_date);
        let total_premium: f64 = ct.put_premium + ct.call_premiums.iter().sum::<f64>();
        let total_pnl = ct.put_pnl + ct.call_pnls.iter().sum::<f64>() + ct.stock_pnl.unwrap_or(0.0);
        let days = (last_day - ct.put_entry_date).num_days() as i32;

        cycles.push(WheelCycle {
            cycle_id: ct.cycle_id,
            put_entry_date: ct.put_entry_date.to_string(),
            put_strike: ct.put_strike,
            put_premium: ct.put_premium,
            put_pnl: ct.put_pnl,
            assigned: ct.assigned,
            cost_basis: ct.cost_basis,
            calls_sold: ct.call_pnls.len(),
            call_pnls: ct.call_pnls,
            call_premiums: ct.call_premiums,
            stock_pnl: ct.stock_pnl,
            called_away_date: ct.called_away_date.map(|d| d.to_string()),
            called_away_strike: ct.called_away_strike,
            total_pnl,
            total_premium,
            days_in_cycle: days,
        });
    }

    let quality = BacktestQualityStats {
        trading_days_total: trading_days.len(),
        trading_days_with_data: ohlcv_closes
            .keys()
            .filter(|d| trading_days.contains(d))
            .count(),
        total_candidates: put_candidates.values().map(Vec::len).sum(),
        positions_opened: trade_log.len(),
        entry_spread_pcts: vec![],
    };

    Ok(WheelResult {
        trade_log,
        equity_curve,
        cycles,
        quality,
    })
}

// ---------------------------------------------------------------------------
// Candidate finding
// ---------------------------------------------------------------------------

/// Find single-leg option candidates grouped by trading date.
///
/// Filters the options `DataFrame` by option type, valid quotes, DTE range,
/// and delta range. Returns the best candidate per (date, expiration) pair,
/// grouped by date.
#[allow(clippy::similar_names)]
pub fn find_single_leg_candidates(
    df: &DataFrame,
    opt_type: OptionType,
    delta: &TargetRange,
    dte: &DteRange,
    min_bid_ask: f64,
    min_strike: Option<f64>,
) -> Result<BTreeMap<NaiveDate, Vec<SingleLegCandidate>>> {
    // Step 1: Combined filter — option type + DTE + valid quotes
    let filtered =
        filters::filter_leg_candidates(df, opt_type.as_str(), dte.max, dte.min, min_bid_ask)?;

    if filtered.height() == 0 {
        return Ok(BTreeMap::new());
    }

    // Step 2: Optional min strike filter (cost basis floor for covered calls)
    let filtered = if let Some(floor) = min_strike {
        filtered
            .clone()
            .lazy()
            .filter(col("strike").gt_eq(lit(floor)))
            .collect()?
    } else {
        filtered
    };

    if filtered.height() == 0 {
        return Ok(BTreeMap::new());
    }

    // Step 3: Select closest delta per (datetime, expiration)
    let selected = filters::select_closest_delta(filtered, delta)?;

    if selected.height() == 0 {
        return Ok(BTreeMap::new());
    }

    // Step 4: Extract rows into SingleLegCandidate structs grouped by date
    let dt_col = selected.column(DATETIME_COL)?;
    let exp_col = selected.column("expiration")?;
    let strikes = selected.column("strike")?.f64()?;
    let bids = selected.column("bid")?.f64()?;
    let asks = selected.column("ask")?.f64()?;
    let deltas = selected.column("delta")?.f64()?;

    let mut candidates: BTreeMap<NaiveDate, Vec<SingleLegCandidate>> = BTreeMap::new();

    for i in 0..selected.height() {
        let date = extract_date_from_column(dt_col, i)?;
        let expiration = extract_date_from_column(exp_col, i)?;

        let Some(strike) = strikes.get(i) else {
            continue;
        };
        let Some(bid) = bids.get(i) else {
            continue;
        };
        let Some(ask) = asks.get(i) else {
            continue;
        };
        let Some(delta_val) = deltas.get(i) else {
            continue;
        };

        candidates
            .entry(date)
            .or_default()
            .push(SingleLegCandidate {
                date,
                expiration,
                strike,
                bid,
                ask,
                delta: delta_val,
                option_type: opt_type,
            });
    }

    // Sort each date's candidates by proximity to target DTE
    for day_cands in candidates.values_mut() {
        day_cands.sort_by_key(|c| {
            let candidate_dte = (c.expiration - c.date).num_days();
            (candidate_dte - i64::from(dte.target)).abs()
        });
    }

    Ok(candidates)
}

/// Find short put candidates for the wheel strategy.
pub fn find_put_candidates(
    df: &DataFrame,
    delta: &TargetRange,
    dte: &DteRange,
    min_bid_ask: f64,
) -> Result<BTreeMap<NaiveDate, Vec<SingleLegCandidate>>> {
    find_single_leg_candidates(df, OptionType::Put, delta, dte, min_bid_ask, None)
}

/// Find covered call candidates, optionally filtering by minimum strike (cost basis floor).
pub fn find_call_candidates(
    df: &DataFrame,
    delta: &TargetRange,
    dte: &DteRange,
    min_bid_ask: f64,
    min_strike: Option<f64>,
) -> Result<BTreeMap<NaiveDate, Vec<SingleLegCandidate>>> {
    find_single_leg_candidates(df, OptionType::Call, delta, dte, min_bid_ask, min_strike)
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Helpers for state machine tests
    // -----------------------------------------------------------------------

    /// Create a simple options `DataFrame` with puts and calls on specific dates.
    ///
    /// Layout:
    /// - Day 1 (Jan 2): put at strike 100, bid 3.00 / ask 3.50, delta -0.30, exp Feb 16
    /// - Day 1 (Jan 2): call at strike 102, bid 2.00 / ask 2.50, delta 0.30, exp Mar 1
    /// - Exp day (Feb 16): put at strike 100 (for closing lookup)
    /// - Exp day (Mar 1): call at strike 102 (for closing lookup)
    fn make_wheel_options_df() -> DataFrame {
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let exp_put = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let _d_exp_put = exp_put.and_hms_opt(0, 0, 0).unwrap();
        // Day after put exp for call entry
        let d2 = NaiveDate::from_ymd_opt(2024, 2, 16)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let exp_call = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let d_exp_call = exp_call.and_hms_opt(0, 0, 0).unwrap();

        let dates = vec![d1, d2, d_exp_call];
        let expirations = [exp_put, exp_call, exp_call];
        let opt_types = vec!["p", "c", "c"];
        let strikes = vec![100.0_f64, 102.0, 102.0];
        let bids = vec![3.00_f64, 2.00, 0.10];
        let asks = vec![3.50_f64, 2.50, 0.20];
        let deltas = vec![-0.30_f64, 0.30, 0.02];

        let mut df = df! {
            DATETIME_COL => &dates,
            "option_type" => &opt_types,
            "strike" => &strikes,
            "bid" => &bids,
            "ask" => &asks,
            "delta" => &deltas,
        }
        .unwrap();

        let exp_col =
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations).into_column();
        df.with_column(exp_col).unwrap();
        df
    }

    fn make_default_wheel_params() -> WheelParams {
        WheelParams {
            put_delta: TargetRange {
                target: 0.30,
                min: 0.10,
                max: 0.50,
            },
            put_dte: DteRange {
                target: 45,
                min: 20,
                max: 60,
            },
            call_delta: TargetRange {
                target: 0.30,
                min: 0.10,
                max: 0.50,
            },
            call_dte: DteRange {
                target: 30,
                min: 15,
                max: 60,
            },
            min_call_strike_at_cost: false,
            capital: 100_000.0,
            quantity: 1,
            multiplier: 100,
            slippage: Slippage::Mid,
            commission: None,
            stop_loss: None,
            min_bid_ask: 0.0,
        }
    }

    #[test]
    fn put_expires_otm_keeps_premium() {
        // Put at strike 100 with underlying closing at 105 on expiration day
        let df = make_wheel_options_df();
        let params = make_default_wheel_params();

        let mut closes = BTreeMap::new();
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        closes.insert(d1, 105.0); // above strike on entry
        closes.insert(exp, 105.0); // above strike at expiration

        let trading_days = vec![d1, exp];
        let result = run_wheel_backtest(&df, &closes, &params, None, &trading_days).unwrap();

        // Should have 1 trade (the put expiring OTM)
        assert_eq!(result.trade_log.len(), 1);
        assert_eq!(result.trade_log[0].exit_type, ExitType::Expiration);

        // Premium = mid(3.00, 3.50) * 1 * 100 = 325.0
        assert!(result.trade_log[0].pnl > 0.0, "Should be profitable");
        assert!(
            (result.trade_log[0].pnl - 325.0).abs() < 1e-10,
            "PnL was {}, expected 325.0",
            result.trade_log[0].pnl
        );

        // Should have 1 cycle (put-only, not assigned)
        assert_eq!(result.cycles.len(), 1);
        assert!(!result.cycles[0].assigned);
        assert_eq!(result.cycles[0].calls_sold, 0);
    }

    #[test]
    fn put_assigned_then_called_away() {
        // Full wheel cycle: put assigned -> call sold -> called away
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let exp_put = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let exp_call = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();

        let df = make_wheel_options_df();
        let params = make_default_wheel_params();

        let mut closes = BTreeMap::new();
        closes.insert(d1, 101.0);
        // Put expiry: underlying at 98 (below strike 100 -> ITM -> assigned)
        closes.insert(exp_put, 98.0);
        // Call expiry: underlying at 105 (above strike 102 -> ITM -> called away)
        closes.insert(exp_call, 105.0);

        let trading_days = vec![d1, exp_put, exp_call];
        let result = run_wheel_backtest(&df, &closes, &params, None, &trading_days).unwrap();

        // Should have 2 trades: put assignment + call called-away
        assert_eq!(result.trade_log.len(), 2);
        assert_eq!(result.trade_log[0].exit_type, ExitType::Assignment);
        assert_eq!(result.trade_log[1].exit_type, ExitType::CalledAway);

        // Should have 1 complete cycle
        assert_eq!(result.cycles.len(), 1);
        let cycle = &result.cycles[0];
        assert!(cycle.assigned);
        assert_eq!(cycle.calls_sold, 1);
        assert!(cycle.called_away_date.is_some());

        // Rolling cost basis = strike - put premium = 100 - 3.25 = 96.75
        let cost_basis = cycle.cost_basis.unwrap();
        assert!(
            (cost_basis - 96.75).abs() < 1e-10,
            "Cost basis was {cost_basis}, expected 96.75"
        );

        // Stock P&L = (call_strike - entry_price) * qty * mult = (102 - 100) * 1 * 100 = 200
        let stock_pnl = cycle.stock_pnl.unwrap();
        assert!(
            (stock_pnl - 200.0).abs() < 1e-10,
            "Stock PnL was {stock_pnl}, expected 200.0"
        );
    }

    #[test]
    fn stop_loss_triggers_on_stock() {
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let exp_put = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let d_after = NaiveDate::from_ymd_opt(2024, 2, 20).unwrap();

        let df = make_wheel_options_df();
        let mut params = make_default_wheel_params();
        params.stop_loss = Some(0.10); // 10% stop loss

        let mut closes = BTreeMap::new();
        closes.insert(d1, 101.0);
        closes.insert(exp_put, 98.0); // ITM -> assigned, cost basis = 100
                                      // Stock drops to 80.0 — well below 100 * 0.90 = 90.0
        closes.insert(d_after, 80.0);

        let trading_days = vec![d1, exp_put, d_after];
        let result = run_wheel_backtest(&df, &closes, &params, None, &trading_days).unwrap();

        // Should have at least 2 trades: put assignment + stop loss exit
        assert!(!result.trade_log.is_empty());

        // Last cycle should have stock_pnl < 0
        let last_cycle = result.cycles.last().unwrap();
        assert!(last_cycle.assigned);
        let stock_pnl = last_cycle.stock_pnl.unwrap();
        assert!(
            stock_pnl < 0.0,
            "Stock PnL should be negative after stop loss"
        );

        // Stock PnL = (80 - entry_price) * 1 * 100 = (80 - 100) * 100 = -2000
        assert!(
            (stock_pnl - (-2000.0)).abs() < 1e-10,
            "Stock PnL was {stock_pnl}, expected -2000.0"
        );
    }

    #[test]
    fn cost_basis_is_strike_minus_premium() {
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let exp_put = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();

        let df = make_wheel_options_df();
        let params = make_default_wheel_params();

        let mut closes = BTreeMap::new();
        closes.insert(d1, 101.0);
        closes.insert(exp_put, 95.0); // ITM

        let trading_days = vec![d1, exp_put];
        let result = run_wheel_backtest(&df, &closes, &params, None, &trading_days).unwrap();

        assert!(!result.cycles.is_empty());
        let cycle = &result.cycles[0];
        assert!(cycle.assigned);

        // Rolling cost basis = strike - put premium = 100 - 3.25 = 96.75
        let cost_basis = cycle.cost_basis.unwrap();
        assert!(
            (cost_basis - 96.75).abs() < 1e-10,
            "Cost basis was {cost_basis}, expected 96.75"
        );
    }

    #[test]
    fn equity_curve_length_matches_trading_days() {
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 3).unwrap();
        let d3 = NaiveDate::from_ymd_opt(2024, 1, 4).unwrap();

        let df = make_wheel_options_df();
        let params = make_default_wheel_params();

        let mut closes = BTreeMap::new();
        closes.insert(d1, 100.0);
        closes.insert(d2, 101.0);
        closes.insert(d3, 102.0);

        let trading_days = vec![d1, d2, d3];
        let result = run_wheel_backtest(&df, &closes, &params, None, &trading_days).unwrap();

        assert_eq!(result.equity_curve.len(), 3);
    }

    // -----------------------------------------------------------------------
    // Original candidate-finding tests
    // -----------------------------------------------------------------------

    /// Build a minimal options `DataFrame` for testing.
    fn make_test_df() -> DataFrame {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 16)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ];
        let expirations = [
            NaiveDate::from_ymd_opt(2024, 2, 16).unwrap(), // 32 DTE from Jan 15
            NaiveDate::from_ymd_opt(2024, 2, 16).unwrap(), // 32 DTE from Jan 15
            NaiveDate::from_ymd_opt(2024, 2, 16).unwrap(), // 32 DTE from Jan 15
            NaiveDate::from_ymd_opt(2024, 2, 16).unwrap(), // 31 DTE from Jan 16
        ];

        let mut df = df! {
            DATETIME_COL => &dates,
            "option_type" => &["p", "p", "c", "p"],
            "strike" => &[95.0f64, 100.0, 105.0, 98.0],
            "bid" => &[2.0f64, 3.0, 1.5, 2.5],
            "ask" => &[2.50f64, 3.50, 2.0, 3.0],
            "delta" => &[-0.30f64, -0.40, 0.35, -0.32],
        }
        .unwrap();

        let exp_col =
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations).into_column();
        df.with_column(exp_col).unwrap();
        df
    }

    #[test]
    fn empty_df_returns_empty_map() {
        let df = df! {
            DATETIME_COL => Vec::<chrono::NaiveDateTime>::new(),
            "option_type" => Vec::<&str>::new(),
            "strike" => Vec::<f64>::new(),
            "bid" => Vec::<f64>::new(),
            "ask" => Vec::<f64>::new(),
            "delta" => Vec::<f64>::new(),
        }
        .unwrap();

        // Add empty expiration column with Date type
        let exp_col =
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), Vec::<NaiveDate>::new())
                .into_column();
        let mut df = df;
        df.with_column(exp_col).unwrap();

        let delta = TargetRange {
            target: 0.30,
            min: 0.20,
            max: 0.40,
        };
        let dte = DteRange {
            target: 45,
            min: 30,
            max: 60,
        };

        let result = find_put_candidates(&df, &delta, &dte, 0.0).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn filters_by_option_type_puts_only() {
        let df = make_test_df();
        let delta = TargetRange {
            target: 0.30,
            min: 0.20,
            max: 0.50,
        };
        let dte = DteRange {
            target: 32,
            min: 30,
            max: 45,
        };

        let result = find_put_candidates(&df, &delta, &dte, 0.0).unwrap();

        // All returned candidates must be puts
        for candidates in result.values() {
            for c in candidates {
                assert_eq!(c.option_type, OptionType::Put);
            }
        }
        // Should have found some put candidates
        assert!(!result.is_empty());
    }

    #[test]
    fn filters_by_option_type_calls_only() {
        let df = make_test_df();
        let delta = TargetRange {
            target: 0.35,
            min: 0.20,
            max: 0.50,
        };
        let dte = DteRange {
            target: 32,
            min: 30,
            max: 45,
        };

        let result = find_call_candidates(&df, &delta, &dte, 0.0, None).unwrap();

        for candidates in result.values() {
            for c in candidates {
                assert_eq!(c.option_type, OptionType::Call);
            }
        }
        // Should have found some call candidates
        assert!(!result.is_empty());
    }

    #[test]
    fn min_strike_filters_low_strikes() {
        let df = make_test_df();
        let delta = TargetRange {
            target: 0.35,
            min: 0.20,
            max: 0.50,
        };
        let dte = DteRange {
            target: 32,
            min: 30,
            max: 45,
        };

        // Set min_strike above all call strikes (105.0) — should still find the 105 call
        let result = find_call_candidates(&df, &delta, &dte, 0.0, Some(105.0)).unwrap();
        for candidates in result.values() {
            for c in candidates {
                assert!(c.strike >= 105.0);
            }
        }

        // Set min_strike above all strikes — should find nothing
        let result = find_call_candidates(&df, &delta, &dte, 0.0, Some(200.0)).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn dte_out_of_range_returns_empty() {
        let df = make_test_df();
        let delta = TargetRange {
            target: 0.30,
            min: 0.20,
            max: 0.50,
        };
        // DTE range that doesn't match any rows (all are ~31-32 DTE)
        let dte = DteRange {
            target: 60,
            min: 55,
            max: 90,
        };

        let result = find_put_candidates(&df, &delta, &dte, 0.0).unwrap();
        assert!(result.is_empty());
    }
}
