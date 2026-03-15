use super::helpers::{
    call_leg, default_atm_delta, default_deep_otm_delta, default_otm_delta, put_leg, strategy,
    strategy_relaxed, Side, StrategyDef,
};

// Vertical spreads
pub fn bull_call_spread() -> StrategyDef {
    strategy(
        "bull_call_spread",
        "Spreads",
        "Buy lower strike call, sell higher strike call",
        vec![
            call_leg(Side::Long, 1, default_atm_delta()),
            call_leg(Side::Short, 1, default_deep_otm_delta()),
        ],
    )
}

pub fn bear_call_spread() -> StrategyDef {
    strategy(
        "bear_call_spread",
        "Spreads",
        "Sell lower strike call, buy higher strike call",
        vec![
            call_leg(Side::Short, 1, default_atm_delta()),
            call_leg(Side::Long, 1, default_deep_otm_delta()),
        ],
    )
}

pub fn bull_put_spread() -> StrategyDef {
    strategy(
        "bull_put_spread",
        "Spreads",
        "Sell higher strike put, buy lower strike put",
        vec![
            put_leg(Side::Long, 1, default_deep_otm_delta()),
            put_leg(Side::Short, 1, default_atm_delta()),
        ],
    )
}

pub fn bear_put_spread() -> StrategyDef {
    strategy(
        "bear_put_spread",
        "Spreads",
        "Buy higher strike put, sell lower strike put",
        vec![
            put_leg(Side::Short, 1, default_deep_otm_delta()),
            put_leg(Side::Long, 1, default_atm_delta()),
        ],
    )
}

// Straddles
pub fn long_straddle() -> StrategyDef {
    strategy_relaxed(
        "long_straddle",
        "Spreads",
        "Buy ATM call and put at same strike",
        vec![
            call_leg(Side::Long, 1, default_atm_delta()),
            put_leg(Side::Long, 1, default_atm_delta()),
        ],
    )
}

pub fn short_straddle() -> StrategyDef {
    strategy_relaxed(
        "short_straddle",
        "Spreads",
        "Sell ATM call and put at same strike",
        vec![
            call_leg(Side::Short, 1, default_atm_delta()),
            put_leg(Side::Short, 1, default_atm_delta()),
        ],
    )
}

// Strangles
pub fn long_strangle() -> StrategyDef {
    strategy(
        "long_strangle",
        "Spreads",
        "Buy OTM call and OTM put",
        vec![
            call_leg(Side::Long, 1, default_otm_delta()),
            put_leg(Side::Long, 1, default_otm_delta()),
        ],
    )
}

pub fn short_strangle() -> StrategyDef {
    strategy(
        "short_strangle",
        "Spreads",
        "Sell OTM call and OTM put",
        vec![
            call_leg(Side::Short, 1, default_otm_delta()),
            put_leg(Side::Short, 1, default_otm_delta()),
        ],
    )
}

pub fn all() -> Vec<StrategyDef> {
    vec![
        bull_call_spread(),
        bear_call_spread(),
        bull_put_spread(),
        bear_put_spread(),
        long_straddle(),
        short_straddle(),
        long_strangle(),
        short_strangle(),
    ]
}
