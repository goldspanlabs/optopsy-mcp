use super::helpers::{call_leg, default_otm_delta, put_leg, strategy, Side, StrategyDef};

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
    strategy(
        "covered_call",
        "Singles",
        "Sell a call (options-only; does not model the long stock leg)",
        vec![call_leg(Side::Short, 1, default_otm_delta())],
    )
}

pub fn cash_secured_put() -> StrategyDef {
    strategy(
        "cash_secured_put",
        "Singles",
        "Sell a put with cash collateral (identical to short_put; alias for intent)",
        vec![put_leg(Side::Short, 1, default_otm_delta())],
    )
}

pub fn all() -> Vec<StrategyDef> {
    vec![
        long_call(),
        short_call(),
        long_put(),
        short_put(),
        covered_call(),
        cash_secured_put(),
    ]
}
