//! name: IBS Mean Reversion
//! description: Stock mean reversion using IBS indicator with volatility-adjusted entry channel
//! category: stock

// Buys when price pulls back below a volatility-adjusted channel and IBS shows
// intraday weakness (close in bottom 30% of range). Exits when close breaks
// above yesterday's high.
//
// Entry: close < (highest_high(10) - 2.5 * (sma_high_25 - sma_low_25)) AND IBS < 0.3
// Exit:  close > yesterday's high
//
// Uses size_by_risk for position sizing: risks 2% of equity per trade with
// a stop at 2 ATR below entry. Falls back to 10% of equity if ATR unavailable.
//
// Required params: SYMBOL, CAPITAL

fn config() {
    #{
        symbol: params.SYMBOL,
        capital: params.CAPITAL,
        interval: "daily",
        data: #{
            ohlcv: true,
            options: false,
            indicators: ["atr:14"],
        },
    }
}

fn on_bar(ctx) {
    if ctx.bar_idx < 25 { return []; }
    if ctx.has_positions() { return []; }

    // IBS = (close - low) / (high - low)
    let range = ctx.high - ctx.low;
    if range <= 0.0 { return []; }
    let ibs = (ctx.close - ctx.low) / range;

    // Highest high over last 10 bars
    let hh10 = ctx.highest_high(10);

    // 25-day average high and average low (manual calc via lookback)
    let sum_high = 0.0;
    let sum_low = 0.0;
    for i in range(0, 25) {
        let h = ctx.high(i);
        let l = ctx.low(i);
        if h == () || l == () { return []; }
        sum_high += h;
        sum_low += l;
    }
    let avg_high = sum_high / 25.0;
    let avg_low = sum_low / 25.0;

    // Entry threshold
    let threshold = hh10 - 2.5 * (avg_high - avg_low);

    if ctx.close < threshold && ibs < 0.3 {
        // Position sizing: risk 2% of equity, stop at 2 ATR below entry
        let atr = ctx.atr(14);
        let qty = if atr != () && atr > 0.0 {
            ctx.size_by_risk(0.02, ctx.close - 2.0 * atr)
        } else {
            ctx.size_by_equity(0.10)
        };
        if qty <= 0 { return []; }
        [buy_stock(qty)]
    } else {
        []
    }
}

fn on_exit_check(ctx, pos) {
    // Exit when close > yesterday's high
    let prev_high = ctx.high(1);
    if prev_high == () { return hold_position(); }

    if ctx.close > prev_high {
        close_position("close_above_prev_high")
    } else {
        hold_position()
    }
}
