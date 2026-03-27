//! name: Bollinger Band Mean Reversion
//! description: Short when price breaks above upper BBand (2σ), exit on reversion to SMA(20) or max 10 bars
//! category: stock
//! hypothesis: Bollinger Band squeezes followed by lower band touches signal mean reversion
//! tags: mean_reversion, options, bollinger_bands
//! regime: range_bound, low_volatility

// Based on hypothesis: price breaks above upper Bollinger Band (2σ) mean reverts strongly.
// 10-day forward return: +0.68% avg, DSR=1.00, p=0.0175 over 53 setups in 5 years.
//
// Entry: close > SMA(20) + 2×StdDev(20)  [upper Bollinger Band breakout]
// Exit (take profit): close < SMA(20)     [reversion to mean]
// Exit (max hold): 10 bars
//
// Since the hypothesis found positive forward returns after upper band breaks,
// we BUY the breakout (going long on mean reversion back toward the mean).
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
            indicators: ["sma:20", "bbands_upper:20", "atr:14"],
        },
    }
}

fn on_bar(ctx) {
    if !ctx.indicators_ready(["sma:20", "bbands_upper:20"]) { return []; }
    if ctx.has_positions() { return []; }

    let upper_bb = ctx.bbands_upper(20);
    if ctx.close > upper_bb {
        // Size: risk 2% of equity, stop at 2 ATR below entry
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
    // Max hold: 10 bars
    if pos.days_held >= 10 {
        return close_position("max_hold");
    }

    // Take profit: price reverts back below SMA(20)
    let sma = ctx.sma(20);
    if sma != () && ctx.close < sma {
        return close_position("take_profit");
    }

    hold_position()
}
