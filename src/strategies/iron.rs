use super::helpers::*;

pub fn iron_condor() -> StrategyDef {
    strategy("iron_condor", "Iron", "Sell OTM put spread + sell OTM call spread", vec![
        put_leg(Side::Long, 1),   // buy lower put (wing)
        put_leg(Side::Short, 1),  // sell higher put
        call_leg(Side::Short, 1), // sell lower call
        call_leg(Side::Long, 1),  // buy higher call (wing)
    ])
}

pub fn reverse_iron_condor() -> StrategyDef {
    strategy("reverse_iron_condor", "Iron", "Buy OTM put spread + buy OTM call spread", vec![
        put_leg(Side::Short, 1),
        put_leg(Side::Long, 1),
        call_leg(Side::Long, 1),
        call_leg(Side::Short, 1),
    ])
}

pub fn iron_butterfly() -> StrategyDef {
    strategy("iron_butterfly", "Iron", "Sell ATM straddle + buy OTM strangle", vec![
        put_leg(Side::Long, 1),   // buy lower put (wing)
        put_leg(Side::Short, 1),  // sell ATM put
        call_leg(Side::Short, 1), // sell ATM call
        call_leg(Side::Long, 1),  // buy higher call (wing)
    ])
}

pub fn reverse_iron_butterfly() -> StrategyDef {
    strategy("reverse_iron_butterfly", "Iron", "Buy ATM straddle + sell OTM strangle", vec![
        put_leg(Side::Short, 1),
        put_leg(Side::Long, 1),
        call_leg(Side::Long, 1),
        call_leg(Side::Short, 1),
    ])
}

pub fn all() -> Vec<StrategyDef> {
    vec![
        iron_condor(), reverse_iron_condor(),
        iron_butterfly(), reverse_iron_butterfly(),
    ]
}
