// Mean-Reversion Bull Put Spread
//
// Sells bull put spreads on RSI dips within an uptrend, with volatility-adaptive
// delta selection and dynamic exit rules using multiple indicators.
//
// Required params: SYMBOL, CAPITAL
// Optional params: SLIPPAGE ("mid")

// Top-level variables persist across bars. Use for tracking state between callbacks.
let consecutive_losses = 0;

// ---------------------------------------------------------------------------
// config() — Called once before the simulation starts.
//
// Returns a map with:
//   symbol (required)    — ticker to backtest
//   capital (required)   — starting cash
//   interval             — "daily", "1m", "5m", etc. (default: "daily")
//   data.ohlcv           — load price bars (default: true)
//   data.options         — load options chain (required for build_strategy)
//   data.indicators      — array of "name:period" strings to pre-compute
//   engine.slippage      — "mid", "spread", or #{ type: "per_leg", per_leg: 0.05 }
//   engine.expiration_filter — "any", "monthly", "weekly"
//   defaults             — arbitrary map readable via ctx.config.defaults
// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
// on_bar(ctx) — Called once per bar. Return an array of actions or [].
//
// Available ctx methods for entry logic:
//   Indicators:  ctx.sma(50), ctx.rsi(14), ctx.atr(14), ctx.bbands_upper(20), etc.
//   Lookback:    ctx.close(1), ctx.high(3), ctx.low(5) — N bars ago
//   Range:       ctx.highest_high(20), ctx.lowest_low(20), ctx.highest_close(10)
//   Portfolio:   ctx.equity, ctx.cash, ctx.position_count, ctx.has_positions()
//                ctx.unrealized_pnl, ctx.realized_pnl, ctx.total_exposure
//   Options:     ctx.build_strategy([...]) — resolve legs from the options chain
//   Price:       ctx.open, ctx.high, ctx.low, ctx.close, ctx.volume
//   Date/Time:   ctx.date, ctx.day_of_week, ctx.month, ctx.hour
//
// Indicator values return () during warmup — always null-check before using.
// ---------------------------------------------------------------------------
fn on_bar(ctx) {
    if ctx.bar_idx < 200 { return []; }
    if ctx.has_positions() { return []; }

    // --- Indicators ---
    let sma50 = ctx.sma(50);
    let sma200 = ctx.sma(200);
    let rsi = ctx.rsi(14);
    let atr = ctx.atr(14);
    if sma50 == () || sma200 == () || rsi == () || atr == () { return []; }

    // --- Trend filter: uptrend ---
    if sma50 <= sma200 { return []; }

    // --- RSI dip ---
    if rsi >= 40.0 { return []; }

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

    let short_delta = if high_vol { 0.20 } else { 0.30 };
    let long_delta = if high_vol { 0.10 } else { 0.15 };

    // --- ATR-adaptive DTE ---
    let atr_pct = atr / ctx.close;
    let target_dte = if atr_pct > 0.02 { 45 } else { 30 };

    // --- Build bull put spread ---
    let spread = ctx.build_strategy([
        #{ side: "short", option_type: "put", delta: short_delta, dte: target_dte },
        #{ side: "long", option_type: "put", delta: long_delta, dte: target_dte },
    ]);
    if spread == () { return []; }

    [#{ action: "open_spread", spread: spread }]
}

// ---------------------------------------------------------------------------
// on_exit_check(ctx, pos) — Called per open position per bar.
//
// Return #{ action: "hold" } or #{ action: "close", reason: "..." }.
//
// reason strings that map to built-in ExitTypes:
//   "take_profit" → TakeProfit    "stop_loss"  → StopLoss
//   "dte_exit"    → DteExit       "signal"     → Signal
//   "max_hold"    → MaxHold       (anything else → Signal)
//
// pos fields: .pnl, .pnl_pct (P&L as fraction of entry cost), .days_held,
//   .dte (days to expiration), .entry_cost, .legs (array), .side
// ---------------------------------------------------------------------------
fn on_exit_check(ctx, pos) {
    // --- Take profit at 50% ---
    if pos.pnl_pct > 0.50 {
        return #{ action: "close", reason: "take_profit" };
    }

    // --- RSI overbought + partial profit → take it ---
    let rsi = ctx.rsi(14);
    if rsi != () && rsi > 70.0 && pos.pnl_pct > 0.20 {
        return #{ action: "close", reason: "take_profit" };
    }

    // --- Stop loss at 200% ---
    if pos.pnl_pct < -2.0 {
        return #{ action: "close", reason: "stop_loss" };
    }

    // --- Trend reversal: death cross ---
    let sma50 = ctx.sma(50);
    let sma200 = ctx.sma(200);
    if sma50 != () && sma200 != () && sma50 < sma200 {
        return #{ action: "close", reason: "stop_loss" };
    }

    // --- Channel breakdown: close below 20-bar low ---
    let channel_low = ctx.lowest_low(20);
    if ctx.close < channel_low * 0.98 {
        return #{ action: "close", reason: "stop_loss" };
    }

    #{ action: "hold" }
}

// ---------------------------------------------------------------------------
// on_position_closed(ctx, pos, exit_type) — Called after a position closes.
//
// Fires for ALL close reasons. The position is already removed from
// ctx.positions. Use this for bookkeeping, state transitions, or risk
// adjustments — not for opening new positions (use on_bar for that).
//
// exit_type values: "expiration", "assignment", "called_away", "take_profit",
//   "stop_loss", "dte_exit", "signal", "end_of_data"
// ---------------------------------------------------------------------------
fn on_position_closed(ctx, pos, exit_type) {
    if pos.pnl < 0.0 {
        consecutive_losses += 1;
    } else {
        consecutive_losses = 0;
    }
}
