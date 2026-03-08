//! Integration tests for slippage models.
//!
//! Verifies that each slippage variant produces different fill prices and P&L
//! when run through the full backtest pipeline.

use optopsy_mcp::engine::core::run_backtest;
use optopsy_mcp::engine::types::Slippage;

mod common;
use common::{backtest_params, delta, make_multi_strike_df};

/// Run a `short_put` backtest with the given slippage model and return total P&L.
fn run_with_slippage(slippage: Slippage) -> f64 {
    let df = make_multi_strike_df();
    let mut params = backtest_params("short_put", vec![delta(0.20)]);
    params.slippage = slippage;
    let result = run_backtest(&df, &params).expect("backtest failed");
    assert!(!result.trade_log.is_empty(), "expected trades");
    result.total_pnl
}

#[test]
fn mid_and_spread_produce_different_pnl() {
    let pnl_mid = run_with_slippage(Slippage::Mid);
    let pnl_spread = run_with_slippage(Slippage::Spread);

    // Spread fills at worst case (bid for shorts, ask for longs),
    // so short put PnL should differ from mid.
    assert!(
        (pnl_mid - pnl_spread).abs() > f64::EPSILON,
        "Mid ({pnl_mid}) and Spread ({pnl_spread}) should produce different PnL"
    );
}

#[test]
fn bid_ask_travel_at_half_equals_mid() {
    let pnl_mid = run_with_slippage(Slippage::Mid);
    let pnl_bat_half = run_with_slippage(Slippage::BidAskTravel { pct: 0.5 });

    // BidAskTravel(0.5) = bid + (ask-bid)*0.5 = mid for longs,
    //                      ask - (ask-bid)*0.5 = mid for shorts.
    assert!(
        (pnl_mid - pnl_bat_half).abs() < 1e-6,
        "BidAskTravel(0.5) ({pnl_bat_half}) should equal Mid ({pnl_mid})"
    );
}

#[test]
fn bid_ask_travel_at_zero_and_one_bracket_mid() {
    let pnl_bat_0 = run_with_slippage(Slippage::BidAskTravel { pct: 0.0 });
    let pnl_mid = run_with_slippage(Slippage::Mid);
    let pnl_bat_1 = run_with_slippage(Slippage::BidAskTravel { pct: 1.0 });

    // For a short put:
    //   pct=0: sell at ask (best for seller), buy at bid (best for buyer) → best PnL
    //   pct=1: sell at bid (worst for seller), buy at ask (worst for buyer) → worst PnL
    // Mid should be between these extremes.
    let best = pnl_bat_0.max(pnl_bat_1);
    let worst = pnl_bat_0.min(pnl_bat_1);
    assert!(
        pnl_mid >= worst - 1e-6 && pnl_mid <= best + 1e-6,
        "Mid ({pnl_mid}) should be between BidAskTravel(0) ({pnl_bat_0}) and BidAskTravel(1) ({pnl_bat_1})"
    );
}

#[test]
fn per_leg_slippage_worse_than_mid() {
    let pnl_mid = run_with_slippage(Slippage::Mid);
    let pnl_per_leg = run_with_slippage(Slippage::PerLeg { per_leg: 0.10 });

    // PerLeg adds cost: mid+0.10 for longs, mid-0.10 for shorts.
    // For a short put (sell then buy), entry gets worse and exit gets worse → lower PnL.
    assert!(
        pnl_per_leg < pnl_mid,
        "PerLeg(0.10) ({pnl_per_leg}) should be worse than Mid ({pnl_mid})"
    );
}

#[test]
fn per_leg_zero_equals_mid() {
    let pnl_mid = run_with_slippage(Slippage::Mid);
    let pnl_per_leg_zero = run_with_slippage(Slippage::PerLeg { per_leg: 0.0 });

    assert!(
        (pnl_mid - pnl_per_leg_zero).abs() < 1e-6,
        "PerLeg(0.0) ({pnl_per_leg_zero}) should equal Mid ({pnl_mid})"
    );
}

#[test]
fn liquidity_fill_ratio_half_equals_mid() {
    let pnl_mid = run_with_slippage(Slippage::Mid);
    let pnl_liq = run_with_slippage(Slippage::Liquidity {
        fill_ratio: 0.5,
        ref_volume: 1000,
    });

    // Liquidity with fill_ratio=0.5: bid + spread*0.5 = mid (long), ask - spread*0.5 = mid (short)
    assert!(
        (pnl_mid - pnl_liq).abs() < 1e-6,
        "Liquidity(0.5) ({pnl_liq}) should equal Mid ({pnl_mid})"
    );
}

#[test]
fn liquidity_fill_ratio_zero_best_for_shorts() {
    let pnl_mid = run_with_slippage(Slippage::Mid);
    let pnl_liq_0 = run_with_slippage(Slippage::Liquidity {
        fill_ratio: 0.0,
        ref_volume: 1000,
    });

    // fill_ratio=0: long fills at bid, short fills at ask → best case for both sides
    // Short put: sell at ask, buy at bid → better than mid
    assert!(
        pnl_liq_0 >= pnl_mid - 1e-6,
        "Liquidity(0.0) ({pnl_liq_0}) should be >= Mid ({pnl_mid}) for short puts"
    );
}

#[test]
fn all_five_models_run_without_error() {
    let models = vec![
        Slippage::Mid,
        Slippage::Spread,
        Slippage::Liquidity {
            fill_ratio: 0.3,
            ref_volume: 5000,
        },
        Slippage::PerLeg { per_leg: 0.05 },
        Slippage::BidAskTravel { pct: 0.25 },
    ];

    for model in models {
        let df = make_multi_strike_df();
        let mut params = backtest_params("short_put", vec![delta(0.20)]);
        params.slippage = model.clone();
        let result = run_backtest(&df, &params);
        assert!(
            result.is_ok(),
            "Slippage model {model:?} should not error: {}",
            result.unwrap_err()
        );
    }
}
