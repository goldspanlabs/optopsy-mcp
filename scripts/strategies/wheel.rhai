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

fn on_bar(account) {
    if state == "selling_puts" {
        if account.has_positions() { return []; }

        let spread = account.short_put(params.PUT_DELTA, params.PUT_DTE);
        if spread == () { return []; }

        return [spread];
    }

    if state == "holding_stock" {
        if account.has_positions() { return []; }

        let spread = account.covered_call(params.CALL_DELTA, params.CALL_DTE);
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

fn on_exit_check(account, pos) {
    // Hold till expiration — no early exits
    hold_position()
}

fn on_position_closed(account, pos, exit_type) {
    if state == "selling_puts" {
        if exit_type == "assignment" {
            // Short put expired ITM — we're assigned stock at the strike price.
            // The engine auto-creates an implicit long stock position.
            stock.entry_price = pos.legs[0].strike;
            stock.adjusted_basis = pos.legs[0].strike - pos.legs[0].entry_price;
            stock.quantity = params.MULTIPLIER;
            stock.entry_date = account.date;
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
