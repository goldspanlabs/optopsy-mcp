// Iron Condor — sell an OTM put spread + OTM call spread for premium
//
// Required params: SYMBOL, CAPITAL
// Optional params: BODY_DELTA (0.30), WING_DELTA (0.10), DTE_TARGET (45),
//   EXIT_DTE (21), MAX_POSITIONS (1), SLIPPAGE ("mid"),
//   STOP_LOSS (2.0), TAKE_PROFIT (0.50)

fn config() {
    #{
        symbol: SYMBOL,
        capital: CAPITAL,
        interval: "daily",
        data: #{
            ohlcv: true,
            options: true,
            indicators: ["rsi:14", "atr:14"],
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

    let spread = ctx.iron_condor(BODY_DELTA, WING_DELTA, DTE_TARGET);
    if spread == () { return []; }

    [#{ action: "open_spread", spread: spread }]
}

fn on_exit_check(ctx, pos) {
    // DTE exit
    if pos.dte != () && pos.dte <= EXIT_DTE {
        return #{ action: "close", reason: "dte_exit" };
    }

    // Stop loss (as multiple of credit received)
    if STOP_LOSS != () && pos.pnl_pct < -STOP_LOSS {
        return #{ action: "close", reason: "stop_loss" };
    }

    // Take profit (as fraction of max profit)
    if TAKE_PROFIT != () && pos.pnl_pct > TAKE_PROFIT {
        return #{ action: "close", reason: "take_profit" };
    }

    #{ action: "hold" }
}
