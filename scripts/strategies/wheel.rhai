//! name: Wheel Strategy
//! description: Sell puts, get assigned, sell covered calls, repeat
//! category: options

// Required params: SYMBOL, CAPITAL
// Optional params: PUT_DELTA (0.30), PUT_DTE (45), CALL_DELTA (0.30),
//   CALL_DTE (30), SLIPPAGE ("mid"), MULTIPLIER (100)

// Top-level variables persist across bars (Rhai scope is preserved).
// Use these for state machines, counters, or tracking values between callbacks.
let state = "selling_puts";

// Defaults for optional params (set in config, used in on_bar/on_position_closed)
let PUT_DELTA = 0.30;
let PUT_DTE = 45;
let CALL_DELTA = 0.30;
let CALL_DTE = 30;
let SLIPPAGE = "mid";
let MULTIPLIER = 100;
let stock = #{
    entry_price: 0.0,
    adjusted_basis: 0.0,
    quantity: 0,
    entry_date: "",
    total_premium: 0.0,
};
let cycles_completed = 0;

// ---------------------------------------------------------------------------
// config() — Called once before the simulation starts.
//
// Returns a map describing what data to load and how the engine should behave.
// Required fields: symbol, capital.
// Optional: interval, data (ohlcv/options/indicators), engine (slippage,
//   commission, expiration_filter), defaults (script-readable values).
// ---------------------------------------------------------------------------
fn config() {
    // Override defaults from params if provided
    if params.contains("PUT_DELTA") { PUT_DELTA = params.PUT_DELTA; }
    if params.contains("PUT_DTE") { PUT_DTE = params.PUT_DTE; }
    if params.contains("CALL_DELTA") { CALL_DELTA = params.CALL_DELTA; }
    if params.contains("CALL_DTE") { CALL_DTE = params.CALL_DTE; }
    if params.contains("SLIPPAGE") { SLIPPAGE = params.SLIPPAGE; }
    if params.contains("MULTIPLIER") { MULTIPLIER = params.MULTIPLIER; }

    #{
        symbol: params.SYMBOL,
        capital: params.CAPITAL,
        interval: "daily",
        data: #{
            ohlcv: true,
            options: true,
        },
        engine: #{
            slippage: SLIPPAGE,
            expiration_filter: "monthly",
        },
        defaults: #{
            max_positions: 1,
        },
    }
}

// ---------------------------------------------------------------------------
// on_bar(ctx) — Called once per bar (or twice if exits happen: Phase A then B).
//
// This is where entry logic lives. Return an array of action maps:
//   [ctx.short_put(0.30, 45)]       — open a short put
//   [ctx.bull_put_spread(0.30, 0.15, 45)] — open a spread
//   [buy_stock(100)]                — open a stock position
//   [] — do nothing this bar
//
// ctx provides: .close, .high, .low, .open, .volume, .date, .bar_idx,
//   .equity, .cash, .has_positions(), .position_count, .sma(n), .rsi(n),
//   .highest_high(n), .lowest_low(n), .close(n), .build_strategy([...]), etc.
// ---------------------------------------------------------------------------
fn on_bar(ctx) {
    if state == "selling_puts" {
        if ctx.has_positions() { return []; }

        let spread = ctx.short_put(PUT_DELTA, PUT_DTE);
        if spread == () { return []; }

        return [spread];
    }

    if state == "holding_stock" {
        if ctx.has_positions() { return []; }

        let spread = ctx.covered_call(CALL_DELTA, CALL_DTE);
        if spread == () { return []; }

        // Only sell calls above adjusted cost basis
        let spread_map = spread;
        let spread_inner = spread_map["spread"];
        if spread_inner.legs[0].strike < stock.adjusted_basis { return []; }

        state = "selling_calls";
        return [spread_map];
    }

    // selling_calls — wait for expiration/assignment
    []
}

// ---------------------------------------------------------------------------
// on_exit_check(ctx, pos) — Called once per open position per bar (Phase A).
//
// Decides whether to close a position early. Return one of:
//   hold_position()                  — keep the position open
//   close_position("take_profit")    — close it (reason becomes exit_type)
//   close_position("stop_loss")
//
// Built-in reasons mapped to ExitType: "take_profit", "stop_loss", "dte_exit",
//   "signal", "max_hold". Custom strings map to ExitType::Signal.
//
// pos provides: .pnl, .pnl_pct, .entry_cost, .unrealized_pnl, .days_held,
//   .dte (days to expiration, () for stocks), .legs (array of leg maps),
//   .side, .entry_date.
//
// Note: positions at expiration are auto-closed by the engine (with ITM
// detection for assignment/called_away). This callback is for EARLY exits.
// ---------------------------------------------------------------------------
fn on_exit_check(ctx, pos) {
    // Hold till expiration — no early exits
    hold_position()
}

// ---------------------------------------------------------------------------
// on_position_closed(ctx, pos, exit_type) — Called after a position is closed.
//
// Fires for ALL close reasons: expiration, assignment, called_away, take_profit,
// stop_loss, dte_exit, signal, end_of_data, etc.
//
// The position is already removed from ctx.positions when this fires.
// pos is a snapshot of the closed position (same fields as on_exit_check).
// exit_type is a string matching the close reason.
//
// Common uses: state machine transitions, tracking win/loss streaks,
// accumulating statistics, adjusting risk after losses.
// ---------------------------------------------------------------------------
fn on_position_closed(ctx, pos, exit_type) {
    if state == "selling_puts" {
        if exit_type == "assignment" {
            // Short put expired ITM — we're assigned stock at the strike price.
            // The engine auto-creates an implicit long stock position.
            stock.entry_price = pos.legs[0].strike;
            stock.adjusted_basis = pos.legs[0].strike - pos.legs[0].entry_price;
            stock.quantity = MULTIPLIER;
            stock.entry_date = ctx.date;
            stock.total_premium += pos.legs[0].entry_price;
            state = "holding_stock";
        }
        // OTM expiration — stay in selling_puts
    } else if state == "selling_calls" {
        if exit_type == "called_away" {
            // Short call expired ITM — stock is sold at the strike price.
            // The engine auto-closes the implicit stock position.
            cycles_completed += 1;
            state = "selling_puts";
        } else {
            // Call expired OTM — reduce basis by premium collected, sell another
            stock.adjusted_basis -= pos.legs[0].entry_price;
            stock.total_premium += pos.legs[0].entry_price;
            state = "holding_stock";
        }
    }
}
