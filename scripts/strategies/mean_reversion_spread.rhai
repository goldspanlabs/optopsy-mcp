// Mean-Reversion Bull Put Spread
//
// Sells bull put spreads on RSI dips within an uptrend, with volatility-adaptive
// delta selection and dynamic exit rules using multiple indicators.
//
// Required params: SYMBOL, CAPITAL
// Optional params: SLIPPAGE ("mid")

let consecutive_losses = 0;

fn config() {
    #{
        symbol: params.SYMBOL,
        capital: params.CAPITAL,
        interval: "daily",
        data: #{
            ohlcv: true,
            options: true,
            indicators: [
                "sma:50", "sma:200", "ema:20",
                "rsi:14", "atr:14",
                "bbands_upper:20", "bbands_lower:20", "bbands_mid:20",
                "stochastic:14",
            ],
        },
        engine: #{
            slippage: params.SLIPPAGE,
            expiration_filter: "monthly",
        },
    }
}

fn on_bar(ctx) {
    if ctx.bar_idx < 200 || ctx.has_positions() { return []; }

    // --- Check all indicators are warmed up ---
    if !ctx.indicators_ready(["sma:50", "sma:200", "rsi:14", "atr:14"]) { return []; }

    // --- Trend filter: uptrend ---
    if ctx.sma(50) <= ctx.sma(200) { return []; }

    // --- RSI dip ---
    if ctx.rsi(14) >= 40.0 { return []; }

    // --- Pause after 3 consecutive losses ---
    if consecutive_losses >= 3 { return []; }

    // --- Volatility-adaptive delta ---
    let bb_upper = ctx.bbands_upper(20);
    let bb_lower = ctx.bbands_lower(20);
    let bb_mid = ctx.bbands_mid(20);

    let high_vol = if bb_upper != () && bb_lower != () && bb_mid != () && bb_mid > 0.0 {
        (bb_upper - bb_lower) / bb_mid > 0.08
    } else {
        false
    };

    let short_put_delta = if high_vol { 0.20 } else { 0.30 };
    let long_put_delta = if high_vol { 0.10 } else { 0.15 };

    // --- ATR-adaptive DTE ---
    let atr_pct = ctx.atr(14) / ctx.close;
    let target_dte = if atr_pct > 0.02 { 45 } else { 30 };

    // --- Build bull put spread ---
    let spread = ctx.bull_put_spread(short_put_delta, long_put_delta, target_dte);
    if spread == () { return []; }

    [spread]
}

fn on_exit_check(ctx, pos) {
    // --- Take profit at 50% ---
    if pos.pnl_pct > 0.50 {
        return close_position("take_profit");
    }

    // --- RSI overbought + partial profit → take it ---
    let rsi = ctx.rsi(14);
    if rsi != () && rsi > 70.0 && pos.pnl_pct > 0.20 {
        return close_position("take_profit");
    }

    // --- Stop loss at 200% ---
    if pos.pnl_pct < -2.0 {
        return close_position("stop_loss");
    }

    // --- Trend reversal: death cross ---
    let sma50 = ctx.sma(50);
    let sma200 = ctx.sma(200);
    if sma50 != () && sma200 != () && sma50 < sma200 {
        return close_position("stop_loss");
    }

    // --- Channel breakdown: close below 20-bar low ---
    let channel_low = ctx.lowest_low(20);
    if ctx.close < channel_low * 0.98 {
        return close_position("stop_loss");
    }

    hold_position()
}

fn on_position_closed(ctx, pos, exit_type) {
    if pos.pnl < 0.0 {
        consecutive_losses += 1;
    } else {
        consecutive_losses = 0;
    }
}
