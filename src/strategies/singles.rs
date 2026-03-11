use super::helpers::{
    call_leg, default_otm_delta, put_leg, strategy, strategy_with_stock, Side, StrategyDef,
};

pub fn long_call() -> StrategyDef {
    strategy(
        "long_call",
        "Singles",
        "Buy a call option",
        vec![call_leg(Side::Long, 1, default_otm_delta())],
    )
}

pub fn short_call() -> StrategyDef {
    strategy(
        "short_call",
        "Singles",
        "Sell a call option",
        vec![call_leg(Side::Short, 1, default_otm_delta())],
    )
}

pub fn long_put() -> StrategyDef {
    strategy(
        "long_put",
        "Singles",
        "Buy a put option",
        vec![put_leg(Side::Long, 1, default_otm_delta())],
    )
}

pub fn short_put() -> StrategyDef {
    strategy(
        "short_put",
        "Singles",
        "Sell a put option (cash-secured put)",
        vec![put_leg(Side::Short, 1, default_otm_delta())],
    )
}

pub fn covered_call() -> StrategyDef {
    strategy_with_stock(
        "covered_call",
        "Singles",
        "Sell a call against 100 shares of long stock per contract",
        vec![call_leg(Side::Short, 1, default_otm_delta())],
    )
}

pub fn all() -> Vec<StrategyDef> {
    vec![
        long_call(),
        short_call(),
        long_put(),
        short_put(),
        covered_call(),
    ]
}
