use super::helpers::{call_leg, put_leg, strategy, Side, StrategyDef};

pub fn long_call_butterfly() -> StrategyDef {
    strategy(
        "long_call_butterfly",
        "Butterflies",
        "Buy 1 lower call, sell 2 middle calls, buy 1 upper call",
        vec![
            call_leg(Side::Long, 1),
            call_leg(Side::Short, 2),
            call_leg(Side::Long, 1),
        ],
    )
}

pub fn short_call_butterfly() -> StrategyDef {
    strategy(
        "short_call_butterfly",
        "Butterflies",
        "Sell 1 lower call, buy 2 middle calls, sell 1 upper call",
        vec![
            call_leg(Side::Short, 1),
            call_leg(Side::Long, 2),
            call_leg(Side::Short, 1),
        ],
    )
}

pub fn long_put_butterfly() -> StrategyDef {
    strategy(
        "long_put_butterfly",
        "Butterflies",
        "Buy 1 lower put, sell 2 middle puts, buy 1 upper put",
        vec![
            put_leg(Side::Long, 1),
            put_leg(Side::Short, 2),
            put_leg(Side::Long, 1),
        ],
    )
}

pub fn short_put_butterfly() -> StrategyDef {
    strategy(
        "short_put_butterfly",
        "Butterflies",
        "Sell 1 lower put, buy 2 middle puts, sell 1 upper put",
        vec![
            put_leg(Side::Short, 1),
            put_leg(Side::Long, 2),
            put_leg(Side::Short, 1),
        ],
    )
}

pub fn all() -> Vec<StrategyDef> {
    vec![
        long_call_butterfly(),
        short_call_butterfly(),
        long_put_butterfly(),
        short_put_butterfly(),
    ]
}
