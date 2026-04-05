//! name: Mean-Reversion Bull Put Spread
//! description: Bull put spreads on RSI dips with volatility-adaptive delta and dynamic exits
//! category: options
//! hypothesis: RSI dips below 40 in uptrending markets revert, profiting bull put spreads
//! tags: mean_reversion, spreads, bull_put, rsi
//! regime: uptrend, moderate_volatility

// Sells bull put spreads on RSI dips within an uptrend, with volatility-adaptive
// delta selection and dynamic exit rules using multiple indicators.
//
// CAPITAL is universal — provided by the FE dialog via params map.
// symbol is set via extern_symbol() below; defaults to "SPY", overridable via params.
let symbol = extern_symbol("symbol", "SPY", "Ticker to trade");
let SLIPPAGE = extern("SLIPPAGE", "mid", "Slippage model", ["mid", "spread"]);

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
            slippage: SLIPPAGE,
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
//   Strategy:    ctx.bull_put_spread(sd, ld, dte), ctx.iron_condor(...), etc.
//   Price:       ctx.open, ctx.high, ctx.low, ctx.close, ctx.volume
//   Date/Time:   ctx.date, ctx.day_of_week, ctx.month, ctx.hour
//
// Indicator values return () during warmup — use ctx.indicators_ready([...]).
// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
// on_exit_check(ctx, pos) — Called per open position per bar.
//
// Return hold_position() or close_position("reason").
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
