// Wheel Strategy — sell puts -> assignment -> sell covered calls -> repeat
//
// Required params: SYMBOL, CAPITAL
// Optional params: PUT_DELTA (0.30), PUT_DTE (45), CALL_DELTA (0.30),
//   CALL_DTE (30), SLIPPAGE ("mid"), MULTIPLIER (100)
//
// Parameters are accessed via the `params` map (e.g., params.SYMBOL).

// State machine: selling_puts -> holding_stock -> selling_calls
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
            slippage: if params.SLIPPAGE != () { params.SLIPPAGE } else { "mid" },
            expiration_filter: "monthly",
        },
        defaults: #{
            max_positions: 1,
        },
    }
}

fn on_bar(ctx) {
    if state == "selling_puts" {
        if ctx.has_positions() { return []; }

        let spread = ctx.build_strategy([
            #{ side: "short", option_type: "put", delta: params.PUT_DELTA, dte: params.PUT_DTE },
        ]);
        if spread == () { return []; }

        return [#{ action: "open_spread", spread: spread }];
    }

    if state == "holding_stock" {
        if ctx.has_positions() { return []; }

        let spread = ctx.build_strategy([
            #{ side: "short", option_type: "call", delta: params.CALL_DELTA, dte: params.CALL_DTE },
        ]);
        if spread == () { return []; }

        // Only sell calls above adjusted cost basis
        if spread.legs[0].strike < stock.adjusted_basis { return []; }

        state = "selling_calls";
        return [#{ action: "open_spread", spread: spread }];
    }

    // selling_calls — wait for expiration/assignment
    []
}

fn on_exit_check(ctx, pos) {
    // Hold till expiration — no early exits
    #{ action: "hold" }
}

fn on_position_closed(ctx, pos, exit_type) {
    if state == "selling_puts" {
        if exit_type == "assignment" {
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
            cycles_completed += 1;
            state = "selling_puts";
        } else {
            // Call expired OTM — reduce basis, sell another
            stock.adjusted_basis -= pos.legs[0].entry_price;
            stock.total_premium += pos.legs[0].entry_price;
            state = "holding_stock";
        }
    }
}
