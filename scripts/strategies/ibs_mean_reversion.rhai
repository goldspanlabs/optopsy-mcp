//! name: IBS Mean Reversion
//! description: Stock mean reversion using IBS indicator with volatility-adjusted entry channel
//! category: stock
//! hypothesis: Low IBS readings (< threshold) in uptrending markets revert within days
//! tags: mean_reversion, options, short_put, ibs
//! regime: uptrend, low_volatility

// Buys when price pulls back below a volatility-adjusted channel and IBS shows
// intraday weakness (close in bottom 30% of range). Exits when close breaks
// above yesterday's high.
//
// Entry: close < (highest_high(LOOKBACK) - CHANNEL_MULT * (sma_high_25 - sma_low_25)) AND IBS < IBS_THRESHOLD
// Exit:  close > yesterday's high
//
// Uses size_by_risk for position sizing: risks RISK_PCT of equity per trade with
// a stop at ATR_MULT ATR below entry. Falls back to 10% of equity if ATR unavailable.
//
// Required params: CAPITAL
let symbol = extern_symbol("symbol", "SPY", "Ticker to trade");
let IBS_THRESHOLD = extern("IBS_THRESHOLD", 0.3, "IBS entry threshold");
let LOOKBACK = extern("LOOKBACK", 10, "Highest-high lookback period");
let CHANNEL_MULT = extern("CHANNEL_MULT", 2.5, "Channel width multiplier");
let RISK_PCT = extern("RISK_PCT", 0.02, "Fraction of equity to risk per trade");
let ATR_MULT = extern("ATR_MULT", 2.0, "ATR multiplier for stop distance");

fn config() {
    #{
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
    let bar_range = ctx.high - ctx.low;
    if bar_range <= 0.0 { return []; }
    let ibs = (ctx.close - ctx.low) / bar_range;

    // Highest high over last LOOKBACK bars
    let hh = ctx.highest_high(LOOKBACK);

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
    let threshold = hh - CHANNEL_MULT * (avg_high - avg_low);

    if ctx.close < threshold && ibs < IBS_THRESHOLD {
        // Position sizing: risk RISK_PCT of equity, stop at ATR_MULT ATR below entry
        let atr = ctx.atr(14);
        let qty = if atr != () && atr > 0.0 {
            ctx.size_by_risk(RISK_PCT, ctx.close - ATR_MULT * atr)
        } else {
            ctx.size_by_equity(0.10)
        };
        if qty <= 0 { return []; }
        [buy_stock(symbol, qty)]
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
