//! name: SPY 200SMA Threshold
//! description: Enter when close > SMA(200) * 1.04, exit when close < SMA(200) * 0.97
//! category: stock
//! hypothesis: Sustained momentum above the 200-day SMA signals trend continuation
//! tags: trend_following, momentum, sma
//! regime: trending, bullish

// Replicates the TradingView "SPY 200SMA +4% Entry -3% Exit" strategy.
// Buys 100% of equity when price breaks 4% above the 200-day SMA.
// Sells when price drops 3% below the 200-day SMA.
// Plots the entry (+4%) and exit (-3%) threshold bands for charting.
//
// Required params: SYMBOL, CAPITAL

let entry_threshold = 0.04;  // +4% above SMA
let exit_threshold  = 0.03;  // -3% below SMA

fn config() {
    #{
        symbol: params.SYMBOL,
        capital: params.CAPITAL,
        interval: "daily",
        data: #{
            ohlcv: true,
            options: false,
            indicators: ["sma:200"],
        },
    }
}

fn on_bar(ctx) {
    let sma = ctx.sma(200);
    if sma == () { return []; }

    // Plot the threshold bands (overlay on price chart)
    let upper = sma * (1.0 + entry_threshold);
    let lower = sma * (1.0 - exit_threshold);
    ctx.plot("Entry Threshold (+4%)", upper);
    ctx.plot("Exit Threshold (-3%)", lower);

    if ctx.has_positions() { return []; }

    if ctx.close > upper {
        let qty = ctx.size_by_equity(1.0);
        if qty <= 0 { return []; }
        [buy_stock(qty)]
    } else {
        []
    }
}

fn on_exit_check(ctx, pos) {
    let sma = ctx.sma(200);
    if sma == () { return hold_position(); }

    let lower = sma * (1.0 - exit_threshold);

    if ctx.close < lower {
        close_position("below_sma_threshold")
    } else {
        hold_position()
    }
}
