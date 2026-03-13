//! Dynamic position sizing: computes trade quantity based on equity, risk, and volatility.
//!
//! Supports five methods: `Fixed` (passthrough), `FixedFractional`, `RiskPerTrade`,
//! `Kelly` criterion, and `VolatilityTarget`. Each method computes a raw quantity
//! that is then clamped by `SizingConstraints`.

use super::pricing;
use super::types::{
    BacktestParams, LegDef, OptionType, PositionSizing, Side, SizingConfig, StrategyDef,
    TradeRecord,
};
use crate::engine::sim_types::EntryCandidate;

/// Minimum number of completed trades before Kelly sizing activates.
const KELLY_MIN_TRADES: usize = 20;

/// Compute the maximum loss per contract for a strategy candidate.
///
/// Returns `None` when max loss cannot be determined (fallback to fixed quantity).
/// The returned value is always positive (absolute loss).
pub fn max_loss_per_contract(
    strategy_def: &StrategyDef,
    candidate: &EntryCandidate,
    params: &BacktestParams,
) -> Option<f64> {
    let legs = &strategy_def.legs;
    let multiplier = f64::from(params.multiplier);

    // Compute slippage-adjusted entry prices for each leg
    let entry_prices: Vec<f64> = candidate
        .legs
        .iter()
        .zip(legs.iter())
        .map(|(cl, ld)| pricing::fill_price(cl.bid, cl.ask, ld.side, &params.slippage))
        .collect();

    match legs.len() {
        1 => max_loss_single_leg(&legs[0], entry_prices[0], multiplier, params.stop_loss),
        2 => max_loss_two_legs(
            legs,
            &candidate.legs,
            &entry_prices,
            multiplier,
            params.stop_loss,
        ),
        3 => max_loss_butterfly(&candidate.legs, multiplier),
        4 => max_loss_four_legs(legs, &candidate.legs, multiplier),
        _ => None,
    }
}

/// Single leg: long pays premium, short requires `stop_loss`.
fn max_loss_single_leg(
    leg: &LegDef,
    entry_price: f64,
    multiplier: f64,
    stop_loss: Option<f64>,
) -> Option<f64> {
    match leg.side {
        Side::Long => {
            // Max loss = premium paid
            Some(entry_price * multiplier)
        }
        Side::Short => {
            // Naked short: requires stop_loss
            let sl = stop_loss?;
            Some(entry_price.abs() * multiplier * sl)
        }
    }
}

/// Two legs: vertical spreads, straddles/strangles.
fn max_loss_two_legs(
    legs: &[LegDef],
    cand_legs: &[crate::engine::sim_types::CandidateLeg],
    entry_prices: &[f64],
    multiplier: f64,
    stop_loss: Option<f64>,
) -> Option<f64> {
    let same_type = legs[0].option_type == legs[1].option_type;
    let opposing_sides = legs[0].side != legs[1].side;

    if same_type && opposing_sides {
        // Defined-risk vertical spread: max loss = width × multiplier
        let width = (cand_legs[0].strike - cand_legs[1].strike).abs();
        Some(width * multiplier)
    } else if legs[0].side == legs[1].side {
        // Straddle/strangle (same side on both legs)
        let net_premium: f64 = entry_prices
            .iter()
            .zip(legs.iter())
            .map(|(&price, ld)| price * ld.side.multiplier())
            .sum();

        match legs[0].side {
            Side::Short => {
                let sl = stop_loss?;
                Some(net_premium.abs() * multiplier * sl)
            }
            Side::Long => Some(net_premium.abs() * multiplier),
        }
    } else {
        None
    }
}

/// Three-leg butterfly: max loss = width between outer strikes × multiplier.
fn max_loss_butterfly(
    cand_legs: &[crate::engine::sim_types::CandidateLeg],
    multiplier: f64,
) -> Option<f64> {
    let strikes: Vec<f64> = cand_legs.iter().map(|l| l.strike).collect();
    let min_strike = strikes.iter().copied().fold(f64::INFINITY, f64::min);
    let max_strike = strikes.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let width = max_strike - min_strike;
    if width > 0.0 {
        Some(width * multiplier)
    } else {
        None
    }
}

/// Four legs (iron condor/butterfly): max of the two wing widths × multiplier.
fn max_loss_four_legs(
    legs: &[LegDef],
    cand_legs: &[crate::engine::sim_types::CandidateLeg],
    multiplier: f64,
) -> Option<f64> {
    // Split into put side and call side
    let mut put_strikes = Vec::new();
    let mut call_strikes = Vec::new();
    for (i, ld) in legs.iter().enumerate() {
        match ld.option_type {
            OptionType::Put => put_strikes.push(cand_legs[i].strike),
            OptionType::Call => call_strikes.push(cand_legs[i].strike),
        }
    }

    let put_width = if put_strikes.len() == 2 {
        (put_strikes[0] - put_strikes[1]).abs()
    } else {
        0.0
    };
    let call_width = if call_strikes.len() == 2 {
        (call_strikes[0] - call_strikes[1]).abs()
    } else {
        0.0
    };

    let max_width = put_width.max(call_width);
    if max_width > 0.0 {
        Some(max_width * multiplier)
    } else {
        None
    }
}

/// Compute quantity from sizing config, dispatching per method.
///
/// Returns a clamped integer quantity (always >= `constraints.min_quantity`).
pub fn compute_quantity(
    config: &SizingConfig,
    equity: f64,
    max_loss: f64,
    trade_history: &[TradeRecord],
    recent_vol: Option<f64>,
    multiplier: i32,
    fallback_qty: i32,
) -> i32 {
    let raw = match &config.method {
        PositionSizing::Fixed => return fallback_qty,
        PositionSizing::FixedFractional { risk_pct } => {
            if max_loss <= 0.0 {
                return fallback_qty;
            }
            (equity * risk_pct / max_loss).floor() as i32
        }
        PositionSizing::RiskPerTrade { risk_amount } => {
            if max_loss <= 0.0 {
                return fallback_qty;
            }
            (risk_amount / max_loss).floor() as i32
        }
        PositionSizing::Kelly { fraction, lookback } => {
            let trades = match lookback {
                Some(lb) if trade_history.len() > *lb => &trade_history[trade_history.len() - lb..],
                _ => trade_history,
            };

            if trades.len() < KELLY_MIN_TRADES {
                return fallback_qty;
            }

            let winners: Vec<f64> = trades
                .iter()
                .filter(|t| t.pnl > 0.0)
                .map(|t| t.pnl)
                .collect();
            let losers: Vec<f64> = trades
                .iter()
                .filter(|t| t.pnl < 0.0)
                .map(|t| t.pnl)
                .collect();

            if winners.is_empty() || losers.is_empty() {
                return fallback_qty;
            }

            let win_rate = winners.len() as f64 / trades.len() as f64;
            let avg_win = winners.iter().sum::<f64>() / winners.len() as f64;
            let avg_loss = losers.iter().map(|l| l.abs()).sum::<f64>() / losers.len() as f64;

            if avg_loss <= 0.0 {
                return fallback_qty;
            }

            let kelly_f = win_rate - (1.0 - win_rate) / (avg_win / avg_loss);
            let kelly_f = kelly_f.clamp(0.0, 1.0);

            if max_loss <= 0.0 {
                return fallback_qty;
            }

            (equity * kelly_f * fraction / max_loss).floor() as i32
        }
        PositionSizing::VolatilityTarget {
            target_vol,
            lookback_days: _,
        } => {
            let vol = match recent_vol {
                Some(v) if v > 0.0 => v,
                _ => return fallback_qty,
            };

            // Per-contract value ≈ max_loss (the dollar risk per contract)
            let per_contract_value = if max_loss > 0.0 {
                max_loss
            } else {
                // Fallback: use multiplier as rough per-contract notional
                f64::from(multiplier)
            };

            // `vol` is already annualized (from compute_realized_vol)
            if per_contract_value <= 0.0 {
                return fallback_qty;
            }

            (equity * target_vol / (vol * per_contract_value)).floor() as i32
        }
    };

    apply_constraints(raw, &config.constraints)
}

/// Compute the maximum loss per share for a stock trade.
///
/// For longs: `entry_price * stop_loss` (or full price if no SL).
/// For shorts: `entry_price * stop_loss` (or full price if no SL).
pub fn max_loss_per_share(entry_price: f64, stop_loss: Option<f64>) -> f64 {
    entry_price * stop_loss.unwrap_or(1.0)
}

/// Compute annualized realized volatility from a slice of close prices.
///
/// Uses log returns → std dev → annualize (× √`bars_per_year`).
pub fn compute_realized_vol(closes: &[f64], lookback: usize, bars_per_year: f64) -> Option<f64> {
    let n = closes.len().min(lookback);
    if n < 2 {
        return None;
    }

    let slice = &closes[closes.len() - n..];
    let log_returns: Vec<f64> = slice
        .windows(2)
        .filter_map(|w| {
            if w[0] > 0.0 && w[1] > 0.0 {
                Some((w[1] / w[0]).ln())
            } else {
                None
            }
        })
        .collect();

    if log_returns.len() < 2 {
        return None;
    }

    let mean = log_returns.iter().sum::<f64>() / log_returns.len() as f64;
    let variance = log_returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>()
        / (log_returns.len() - 1) as f64;
    let daily_vol = variance.sqrt();
    let annualized = daily_vol * bars_per_year.sqrt();

    if annualized.is_finite() && annualized > 0.0 {
        Some(annualized)
    } else {
        None
    }
}

/// Apply min/max constraints to a computed quantity.
fn apply_constraints(raw: i32, constraints: &super::types::SizingConstraints) -> i32 {
    let min = constraints.min_quantity;
    let max = constraints.max_quantity.unwrap_or(i32::MAX);
    raw.max(min).min(max)
}

/// Extract the volatility lookback period from a sizing config.
/// Returns `Some(lookback)` only for `VolatilityTarget`; `None` for other methods.
pub fn vol_lookback(config: &SizingConfig) -> Option<usize> {
    match &config.method {
        PositionSizing::VolatilityTarget { lookback_days, .. } => Some(*lookback_days as usize),
        _ => None,
    }
}

/// Return a human-readable label for a sizing method.
pub fn sizing_method_label(config: &SizingConfig) -> String {
    match &config.method {
        PositionSizing::Fixed => "fixed".to_string(),
        PositionSizing::FixedFractional { risk_pct } => {
            format!("fixed_fractional({:.1}%)", risk_pct * 100.0)
        }
        PositionSizing::Kelly { fraction, .. } => format!("kelly({:.0}%)", fraction * 100.0),
        PositionSizing::RiskPerTrade { risk_amount } => {
            format!("risk_per_trade(${risk_amount:.0})")
        }
        PositionSizing::VolatilityTarget { target_vol, .. } => {
            format!("vol_target({:.0}%)", target_vol * 100.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::types::{ExitType, SizingConstraints};
    use chrono::NaiveDateTime;

    fn make_trade(pnl: f64) -> TradeRecord {
        let dt = NaiveDateTime::parse_from_str("2024-01-15 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap();
        TradeRecord::new(
            1,
            dt,
            dt,
            100.0,
            100.0 + pnl,
            pnl,
            1,
            ExitType::Expiration,
            vec![],
        )
    }

    // ── compute_realized_vol ─────────────────────────────────────────────

    #[test]
    fn vol_basic() {
        // Constant prices → zero vol
        let closes = vec![100.0; 30];
        let vol = compute_realized_vol(&closes, 30, 252.0);
        assert!(vol.is_none() || vol.unwrap().abs() < 1e-10);
    }

    #[test]
    fn vol_too_few_points() {
        assert!(compute_realized_vol(&[100.0], 10, 252.0).is_none());
        assert!(compute_realized_vol(&[], 10, 252.0).is_none());
    }

    #[test]
    fn vol_positive_for_varying_prices() {
        let closes: Vec<f64> = (0..60)
            .map(|i| 100.0 + (f64::from(i) * 0.1).sin() * 5.0)
            .collect();
        let vol = compute_realized_vol(&closes, 60, 252.0);
        assert!(vol.is_some());
        assert!(vol.unwrap() > 0.0);
    }

    // ── apply_constraints ────────────────────────────────────────────────

    #[test]
    fn constraints_clamp_low() {
        let c = SizingConstraints {
            min_quantity: 1,
            max_quantity: Some(10),
        };
        assert_eq!(apply_constraints(0, &c), 1);
        assert_eq!(apply_constraints(-5, &c), 1);
    }

    #[test]
    fn constraints_clamp_high() {
        let c = SizingConstraints {
            min_quantity: 1,
            max_quantity: Some(10),
        };
        assert_eq!(apply_constraints(15, &c), 10);
    }

    #[test]
    fn constraints_no_max() {
        let c = SizingConstraints {
            min_quantity: 1,
            max_quantity: None,
        };
        assert_eq!(apply_constraints(1000, &c), 1000);
    }

    // ── compute_quantity: Fixed ──────────────────────────────────────────

    #[test]
    fn fixed_returns_fallback() {
        let cfg = SizingConfig {
            method: PositionSizing::Fixed,
            constraints: SizingConstraints::default(),
        };
        assert_eq!(compute_quantity(&cfg, 10000.0, 500.0, &[], None, 100, 3), 3);
    }

    // ── compute_quantity: FixedFractional ────────────────────────────────

    #[test]
    fn fixed_fractional_basic() {
        let cfg = SizingConfig {
            method: PositionSizing::FixedFractional { risk_pct: 0.02 },
            constraints: SizingConstraints::default(),
        };
        // equity=10000, risk_pct=0.02, max_loss=500 → 10000*0.02/500 = 0.4 → floor=0 → clamp to min=1
        assert_eq!(compute_quantity(&cfg, 10000.0, 500.0, &[], None, 100, 1), 1);

        // equity=100000, risk_pct=0.02, max_loss=500 → 100000*0.02/500 = 4
        assert_eq!(
            compute_quantity(&cfg, 100_000.0, 500.0, &[], None, 100, 1),
            4
        );
    }

    #[test]
    fn fixed_fractional_zero_max_loss_fallback() {
        let cfg = SizingConfig {
            method: PositionSizing::FixedFractional { risk_pct: 0.02 },
            constraints: SizingConstraints::default(),
        };
        assert_eq!(compute_quantity(&cfg, 10000.0, 0.0, &[], None, 100, 5), 5);
    }

    // ── compute_quantity: RiskPerTrade ───────────────────────────────────

    #[test]
    fn risk_per_trade_basic() {
        let cfg = SizingConfig {
            method: PositionSizing::RiskPerTrade { risk_amount: 200.0 },
            constraints: SizingConstraints::default(),
        };
        // risk=200, max_loss=500 → 200/500 = 0.4 → floor=0 → clamp to 1
        assert_eq!(compute_quantity(&cfg, 10000.0, 500.0, &[], None, 100, 1), 1);

        // risk=1000, max_loss=500 → 1000/500 = 2
        let cfg2 = SizingConfig {
            method: PositionSizing::RiskPerTrade {
                risk_amount: 1000.0,
            },
            constraints: SizingConstraints::default(),
        };
        assert_eq!(
            compute_quantity(&cfg2, 10000.0, 500.0, &[], None, 100, 1),
            2
        );
    }

    // ── compute_quantity: Kelly cold start ───────────────────────────────

    #[test]
    fn kelly_cold_start_uses_fallback() {
        let cfg = SizingConfig {
            method: PositionSizing::Kelly {
                fraction: 0.5,
                lookback: None,
            },
            constraints: SizingConstraints::default(),
        };
        // Only 10 trades (< KELLY_MIN_TRADES=20) → fallback
        let trades: Vec<TradeRecord> = (0..10).map(|_| make_trade(50.0)).collect();
        assert_eq!(
            compute_quantity(&cfg, 50000.0, 500.0, &trades, None, 100, 3),
            3
        );
    }

    #[test]
    fn kelly_with_history() {
        let cfg = SizingConfig {
            method: PositionSizing::Kelly {
                fraction: 0.5,
                lookback: None,
            },
            constraints: SizingConstraints::default(),
        };
        // 15 winners (+100) and 10 losers (-50) = 25 trades
        let mut trades = Vec::new();
        for _ in 0..15 {
            trades.push(make_trade(100.0));
        }
        for _ in 0..10 {
            trades.push(make_trade(-50.0));
        }
        // win_rate=0.6, avg_win=100, avg_loss=50
        // kelly_f = 0.6 - 0.4/(100/50) = 0.6 - 0.2 = 0.4
        // qty = floor(50000 * 0.4 * 0.5 / 500) = floor(20) = 20
        let qty = compute_quantity(&cfg, 50000.0, 500.0, &trades, None, 100, 1);
        assert_eq!(qty, 20);
    }

    // ── compute_quantity: VolatilityTarget ───────────────────────────────

    #[test]
    fn vol_target_no_vol_fallback() {
        let cfg = SizingConfig {
            method: PositionSizing::VolatilityTarget {
                target_vol: 0.10,
                lookback_days: 20,
            },
            constraints: SizingConstraints::default(),
        };
        assert_eq!(compute_quantity(&cfg, 10000.0, 500.0, &[], None, 100, 2), 2);
    }

    #[test]
    fn vol_target_with_vol() {
        let cfg = SizingConfig {
            method: PositionSizing::VolatilityTarget {
                target_vol: 0.10,
                lookback_days: 20,
            },
            constraints: SizingConstraints::default(),
        };
        // vol=0.20 (annualized), max_loss=500
        // qty = floor(10000 * 0.10 / (0.20 * 500)) = floor(10)
        let qty = compute_quantity(&cfg, 10000.0, 500.0, &[], Some(0.20), 100, 1);
        assert_eq!(qty, 10);
    }

    // ── max_loss_per_share ──────────────────────────────────────────────

    #[test]
    fn max_loss_share_with_stop() {
        assert!((max_loss_per_share(100.0, Some(0.05)) - 5.0).abs() < 1e-10);
    }

    #[test]
    fn max_loss_share_without_stop() {
        assert!((max_loss_per_share(100.0, None) - 100.0).abs() < 1e-10);
    }

    // ── sizing_method_label ─────────────────────────────────────────────

    #[test]
    fn labels() {
        let cfg = SizingConfig {
            method: PositionSizing::FixedFractional { risk_pct: 0.02 },
            constraints: SizingConstraints::default(),
        };
        assert_eq!(sizing_method_label(&cfg), "fixed_fractional(2.0%)");
    }
}
