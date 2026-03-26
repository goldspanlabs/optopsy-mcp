// Wheel Strategy — sell puts -> assignment -> sell covered calls -> repeat
//
// Required params: SYMBOL, CAPITAL
// Optional params: PUT_DELTA (0.30), PUT_DTE (45), CALL_DELTA (0.30),
//   CALL_DTE (30), SLIPPAGE ("mid"), MULTIPLIER (100)

let state = "selling_puts";
let stock = #{
    entry_price: 0.0,
    adjusted_basis: 0.0,
    quantity: 0,
    entry_date: "",
    total_premium: 0.0,
};
let cycles_completed = 0;

fn config() {
    #{
        symbol: params.SYMBOL,
        capital: params.CAPITAL,
        interval: "daily",
        data: #{
            ohlcv: true,
            options: true,
        },
        engine: #{
            slippage: params.SLIPPAGE,
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

        let spread = ctx.short_put(params.PUT_DELTA, params.PUT_DTE);
        if spread == () { return []; }

        return [spread];
    }

    if state == "holding_stock" {
        if ctx.has_positions() { return []; }

        let spread = ctx.covered_call(params.CALL_DELTA, params.CALL_DTE);
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

fn on_position_closed(ctx, pos, exit_type) {
    if state == "selling_puts" {
        if exit_type == "assignment" {
            // Short put expired ITM — we're assigned stock at the strike price.
            // The engine auto-creates an implicit long stock position.
            stock.entry_price = pos.legs[0].strike;
            stock.adjusted_basis = pos.legs[0].strike - pos.legs[0].entry_price;
            stock.quantity = params.MULTIPLIER;
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
