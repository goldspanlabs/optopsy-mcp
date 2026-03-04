use super::helpers::{
    call_leg, default_atm_delta, default_delta, default_otm_delta, put_leg, strategy,
    strategy_relaxed, Side, StrategyDef,
};

pub fn iron_condor() -> StrategyDef {
    strategy(
        "iron_condor",
        "Iron",
        "Sell OTM put spread + sell OTM call spread",
        vec![
            put_leg(Side::Long, 1, default_otm_delta()), // buy lower put (wing)
            put_leg(Side::Short, 1, default_delta()),    // sell higher put
            call_leg(Side::Short, 1, default_delta()),   // sell lower call
            call_leg(Side::Long, 1, default_otm_delta()), // buy higher call (wing)
        ],
    )
}

pub fn reverse_iron_condor() -> StrategyDef {
    strategy(
        "reverse_iron_condor",
        "Iron",
        "Buy OTM put spread + buy OTM call spread",
        vec![
            put_leg(Side::Short, 1, default_otm_delta()),
            put_leg(Side::Long, 1, default_delta()),
            call_leg(Side::Long, 1, default_delta()),
            call_leg(Side::Short, 1, default_otm_delta()),
        ],
    )
}

pub fn iron_butterfly() -> StrategyDef {
    strategy_relaxed(
        "iron_butterfly",
        "Iron",
        "Sell ATM straddle + buy OTM strangle",
        vec![
            put_leg(Side::Long, 1, default_otm_delta()), // buy lower put (wing)
            put_leg(Side::Short, 1, default_atm_delta()), // sell ATM put
            call_leg(Side::Short, 1, default_atm_delta()), // sell ATM call
            call_leg(Side::Long, 1, default_otm_delta()), // buy higher call (wing)
        ],
    )
}

pub fn reverse_iron_butterfly() -> StrategyDef {
    strategy_relaxed(
        "reverse_iron_butterfly",
        "Iron",
        "Buy ATM straddle + sell OTM strangle",
        vec![
            put_leg(Side::Short, 1, default_otm_delta()),
            put_leg(Side::Long, 1, default_atm_delta()),
            call_leg(Side::Long, 1, default_atm_delta()),
            call_leg(Side::Short, 1, default_otm_delta()),
        ],
    )
}

pub fn all() -> Vec<StrategyDef> {
    vec![
        iron_condor(),
        reverse_iron_condor(),
        iron_butterfly(),
        reverse_iron_butterfly(),
    ]
}
