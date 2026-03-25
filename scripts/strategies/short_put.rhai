// Short Put — sell cash-secured puts to collect premium
//
// Required params: SYMBOL, CAPITAL
// Optional params: DELTA_TARGET (0.30), DTE_TARGET (45), EXIT_DTE (21),
//   MAX_POSITIONS (1), SLIPPAGE ("mid"), STOP_LOSS (()), TAKE_PROFIT (0.50)

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
            expiration_filter: "any",
        },
        defaults: #{
            max_positions: MAX_POSITIONS,
            stop_loss: STOP_LOSS,
            take_profit: TAKE_PROFIT,
        },
    }
}

fn on_bar(ctx) {
    if ctx.position_count() >= MAX_POSITIONS { return []; }

    let put = ctx.find_option("put", DELTA_TARGET, DTE_TARGET);
    if put == () { return []; }

    [#{
        action: "open_options",
        legs: [#{
            side: "short",
            option_type: "put",
            strike: put.strike,
            expiration: put.expiration,
            bid: put.bid,
            ask: put.ask,
        }],
    }]
}

fn on_exit_check(ctx, pos) {
    // DTE exit
    if pos.dte != () && pos.dte <= EXIT_DTE {
        return #{ action: "close", reason: "dte_exit" };
    }

    // Stop loss
    if STOP_LOSS != () && pos.pnl_pct < -STOP_LOSS {
        return #{ action: "close", reason: "stop_loss" };
    }

    // Take profit
    if TAKE_PROFIT != () && pos.pnl_pct > TAKE_PROFIT {
        return #{ action: "close", reason: "take_profit" };
    }

    #{ action: "hold" }
}
