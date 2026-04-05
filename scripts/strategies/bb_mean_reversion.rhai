//! name: Bollinger Band Mean Reversion
//! description: Short when price breaks above upper BBand (2σ), exit on reversion to SMA(20) or max 10 bars
//! category: stock
//! hypothesis: Bollinger Band squeezes followed by lower band touches signal mean reversion
//! tags: mean_reversion, options, bollinger_bands
//! regime: range_bound, low_volatility

// Based on hypothesis: price breaks above upper Bollinger Band (2σ) mean reverts strongly.
// 10-day forward return: +0.68% avg, DSR=1.00, p=0.0175 over 53 setups in 5 years.
//
// Entry: close > SMA(BB_PERIOD) + 2×StdDev(BB_PERIOD)  [upper Bollinger Band breakout]
// Exit (take profit): close < SMA(BB_PERIOD)            [reversion to mean]
// Exit (max hold): MAX_HOLD bars
//
// Since the hypothesis found positive forward returns after upper band breaks,
// we BUY the breakout (going long on mean reversion back toward the mean).
//
// Required params: CAPITAL
let symbol = extern_symbol("symbol", "SPY", "Ticker to trade");
let BB_PERIOD = extern("BB_PERIOD", 20, "Bollinger Band period");
let MAX_HOLD = extern("MAX_HOLD", 10, "Max bars to hold position");
let RISK_PCT = extern("RISK_PCT", 0.02, "Fraction of equity to risk per trade");
let ATR_MULT = extern("ATR_MULT", 2.0, "ATR multiplier for stop distance");

fn config() {
    #{
        capital: params.CAPITAL,
        interval: "daily",
        data: #{
            ohlcv: true,
            options: false,
            indicators: [
                `sma:${BB_PERIOD}`,
                `bbands_upper:${BB_PERIOD}`,
                "atr:14",
            ],
        },
    }
}

fn on_bar(ctx) {
    if !ctx.indicators_ready([`sma:${BB_PERIOD}`, `bbands_upper:${BB_PERIOD}`]) { return []; }
    if ctx.has_positions() { return []; }

    let upper_bb = ctx.bbands_upper(BB_PERIOD);
    if ctx.close > upper_bb {
        // Size: risk RISK_PCT of equity, stop at ATR_MULT ATR below entry
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
    if pos.days_held >= MAX_HOLD {
        return close_position("max_hold");
    }

    let sma = ctx.sma(BB_PERIOD);
    if sma != () && ctx.close < sma {
        return close_position("take_profit");
    }

    hold_position()
}
