use super::types::{Side, Slippage};

/// Calculate fill price based on slippage model
pub fn fill_price(bid: f64, ask: f64, side: Side, slippage: &Slippage) -> f64 {
    let mid = f64::midpoint(bid, ask);
    let spread = ask - bid;

    match slippage {
        Slippage::Mid => mid,
        Slippage::Spread => match side {
            Side::Long => ask,  // buy at ask
            Side::Short => bid, // sell at bid
        },
        Slippage::Liquidity { fill_ratio, .. } => match side {
            Side::Long => bid + spread * fill_ratio,
            Side::Short => ask - spread * fill_ratio,
        },
        Slippage::PerLeg { per_leg } => match side {
            Side::Long => mid + per_leg,
            Side::Short => mid - per_leg,
        },
    }
}

/// Calculate per-trade P&L for a single leg
#[allow(clippy::too_many_arguments)]
pub fn leg_pnl(
    entry_bid: f64,
    entry_ask: f64,
    exit_bid: f64,
    exit_ask: f64,
    side: Side,
    slippage: &Slippage,
    qty: i32,
    multiplier: i32,
) -> f64 {
    let entry_price = fill_price(entry_bid, entry_ask, side, slippage);

    // At exit, the side is reversed (closing the position)
    let exit_side = match side {
        Side::Long => Side::Short, // selling to close
        Side::Short => Side::Long, // buying to close
    };
    let exit_price = fill_price(exit_bid, exit_ask, exit_side, slippage);

    let direction = side.multiplier();
    (exit_price - entry_price) * direction * f64::from(qty) * f64::from(multiplier)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BID: f64 = 2.0;
    const ASK: f64 = 2.50;
    const MID: f64 = 2.25;

    // --- fill_price tests ---

    #[test]
    fn fill_price_mid_long() {
        assert_eq!(fill_price(BID, ASK, Side::Long, &Slippage::Mid), MID);
    }

    #[test]
    fn fill_price_mid_short() {
        assert_eq!(fill_price(BID, ASK, Side::Short, &Slippage::Mid), MID);
    }

    #[test]
    fn fill_price_spread_long_buys_at_ask() {
        assert_eq!(fill_price(BID, ASK, Side::Long, &Slippage::Spread), ASK);
    }

    #[test]
    fn fill_price_spread_short_sells_at_bid() {
        assert_eq!(fill_price(BID, ASK, Side::Short, &Slippage::Spread), BID);
    }

    #[test]
    fn fill_price_liquidity_long() {
        let slip = Slippage::Liquidity {
            fill_ratio: 0.75,
            ref_volume: 100,
        };
        // bid + spread * fill_ratio = 2.0 + 0.5 * 0.75 = 2.375
        assert!((fill_price(BID, ASK, Side::Long, &slip) - 2.375).abs() < 1e-10);
    }

    #[test]
    fn fill_price_liquidity_short() {
        let slip = Slippage::Liquidity {
            fill_ratio: 0.75,
            ref_volume: 100,
        };
        // ask - spread * fill_ratio = 2.50 - 0.5 * 0.75 = 2.125
        assert!((fill_price(BID, ASK, Side::Short, &slip) - 2.125).abs() < 1e-10);
    }

    #[test]
    fn fill_price_per_leg_long() {
        let slip = Slippage::PerLeg { per_leg: 0.05 };
        // mid + per_leg = 2.25 + 0.05 = 2.30
        assert!((fill_price(BID, ASK, Side::Long, &slip) - 2.30).abs() < 1e-10);
    }

    #[test]
    fn fill_price_per_leg_short() {
        let slip = Slippage::PerLeg { per_leg: 0.05 };
        // mid - per_leg = 2.25 - 0.05 = 2.20
        assert!((fill_price(BID, ASK, Side::Short, &slip) - 2.20).abs() < 1e-10);
    }

    // --- leg_pnl tests ---

    #[test]
    fn leg_pnl_long_profitable() {
        // Long: buy at mid 2.25, sell at mid 3.25 → +1.0 per unit
        // qty=1, multiplier=100 → pnl = 1.0 * 1.0 * 1 * 100 = 100
        let pnl = leg_pnl(2.0, 2.50, 3.0, 3.50, Side::Long, &Slippage::Mid, 1, 100);
        assert!((pnl - 100.0).abs() < 1e-10);
    }

    #[test]
    fn leg_pnl_short_profitable() {
        // Short: sell at mid 3.25, buy back at mid 2.25 → +1.0 per unit
        // entry_price = mid(3.0,3.50) = 3.25; exit uses reversed side (Long) → mid(2.0,2.50)=2.25
        // pnl = (2.25 - 3.25) * -1.0 * 1 * 100 = 100
        let pnl = leg_pnl(3.0, 3.50, 2.0, 2.50, Side::Short, &Slippage::Mid, 1, 100);
        assert!((pnl - 100.0).abs() < 1e-10);
    }

    #[test]
    fn leg_pnl_long_losing() {
        // Long: buy at mid 3.25, sell at mid 2.25 → -1.0 per unit
        let pnl = leg_pnl(3.0, 3.50, 2.0, 2.50, Side::Long, &Slippage::Mid, 1, 100);
        assert!((pnl - (-100.0)).abs() < 1e-10);
    }

    #[test]
    fn leg_pnl_quantity_scaling() {
        let pnl = leg_pnl(2.0, 2.50, 3.0, 3.50, Side::Long, &Slippage::Mid, 5, 100);
        assert!((pnl - 500.0).abs() < 1e-10);
    }

    #[test]
    fn leg_pnl_multiplier_scaling() {
        let pnl = leg_pnl(2.0, 2.50, 3.0, 3.50, Side::Long, &Slippage::Mid, 1, 50);
        assert!((pnl - 50.0).abs() < 1e-10);
    }
}
