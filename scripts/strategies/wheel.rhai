// Wheel Strategy — sell puts -> assignment -> sell covered calls -> repeat
//
// Required params: SYMBOL, CAPITAL
// Optional params: PUT_DELTA (0.30), PUT_DTE (45), CALL_DELTA (0.30),
//   CALL_DTE (30), EXIT_DTE (21), SLIPPAGE ("mid"), MULTIPLIER (100),
//   STOP_LOSS (()), TAKE_PROFIT (0.50)

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
        symbol: SYMBOL,
        capital: CAPITAL,
        interval: "daily",
        data: #{
            ohlcv: true,
            options: true,
            indicators: ["rsi:14"],
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

fn on_bar(ctx) {
    if state == "selling_puts" {
        if ctx.has_positions() { return []; }

        let put = ctx.find_option("put", PUT_DELTA, PUT_DTE);
        if put == () { return []; }

        return [#{
            action: "open_options",
            legs: [#{
                side: "short",
                option_type: "put",
                strike: put.strike,
                expiration: put.expiration,
                bid: put.bid,
                ask: put.ask,
            }],
        }];
    }

    if state == "holding_stock" {
        // Don't sell a call if we already have one open
        if ctx.has_positions() { return []; }

        let call = ctx.find_option("call", CALL_DELTA, CALL_DTE);
        if call == () { return []; }

        // Only sell calls above adjusted cost basis
        if call.strike < stock.adjusted_basis { return []; }

        state = "selling_calls";
        return [#{
            action: "open_options",
            legs: [#{
                side: "short",
                option_type: "call",
                strike: call.strike,
                expiration: call.expiration,
                bid: call.bid,
                ask: call.ask,
            }],
        }];
    }

    // selling_calls — wait for expiration/assignment
    []
}

fn on_exit_check(ctx, pos) {
    // Early profit take on puts or calls
    if TAKE_PROFIT != () && pos.pnl_pct > TAKE_PROFIT {
        return #{ action: "close", reason: "early_profit_take" };
    }

    // Stop loss on puts
    if state == "selling_puts" && STOP_LOSS != () && pos.pnl_pct < -STOP_LOSS {
        return #{ action: "close", reason: "stop_loss" };
    }

    // DTE exit
    if pos.dte != () && pos.dte <= EXIT_DTE {
        return #{ action: "close", reason: "dte_exit" };
    }

    #{ action: "hold" }
}

fn on_position_closed(ctx, pos, exit_type) {
    if state == "selling_puts" {
        if exit_type == "assignment" {
            stock.entry_price = pos.legs[0].strike;
            stock.adjusted_basis = pos.legs[0].strike - pos.legs[0].entry_price;
            stock.quantity = MULTIPLIER;
            stock.entry_date = ctx.date;
            stock.total_premium += pos.legs[0].entry_price;
            state = "holding_stock";
        }
        // OTM expiration or early close — stay in selling_puts
    } else if state == "selling_calls" {
        if exit_type == "called_away" {
            cycles_completed += 1;
            state = "selling_puts";
        } else {
            // Call expired OTM or closed early — reduce basis, sell another
            stock.adjusted_basis -= pos.legs[0].entry_price;
            stock.total_premium += pos.legs[0].entry_price;
            state = "holding_stock";
        }
    }
}
