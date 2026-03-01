use super::helpers::{
    call_leg, call_leg_secondary, put_leg, put_leg_secondary, strategy, strategy_relaxed, Side,
    StrategyDef,
};

pub fn call_calendar_spread() -> StrategyDef {
    strategy_relaxed(
        "call_calendar_spread",
        "Calendar",
        "Sell near-term call, buy far-term call at same strike",
        vec![
            call_leg(Side::Short, 1),          // near-term (Primary)
            call_leg_secondary(Side::Long, 1), // far-term (Secondary)
        ],
    )
}

pub fn put_calendar_spread() -> StrategyDef {
    strategy_relaxed(
        "put_calendar_spread",
        "Calendar",
        "Sell near-term put, buy far-term put at same strike",
        vec![
            put_leg(Side::Short, 1),          // near-term (Primary)
            put_leg_secondary(Side::Long, 1), // far-term (Secondary)
        ],
    )
}

pub fn call_diagonal_spread() -> StrategyDef {
    strategy(
        "call_diagonal_spread",
        "Calendar",
        "Sell near-term call, buy far-term call at different strike",
        vec![
            call_leg(Side::Short, 1),          // near-term (Primary)
            call_leg_secondary(Side::Long, 1), // far-term (Secondary)
        ],
    )
}

pub fn put_diagonal_spread() -> StrategyDef {
    strategy(
        "put_diagonal_spread",
        "Calendar",
        "Sell near-term put, buy far-term put at different strike",
        vec![
            put_leg(Side::Short, 1),          // near-term (Primary)
            put_leg_secondary(Side::Long, 1), // far-term (Secondary)
        ],
    )
}

pub fn double_calendar() -> StrategyDef {
    strategy_relaxed(
        "double_calendar",
        "Calendar",
        "Call calendar + put calendar at different strikes",
        vec![
            call_leg(Side::Short, 1),          // near-term call (Primary)
            call_leg_secondary(Side::Long, 1), // far-term call (Secondary)
            put_leg(Side::Short, 1),           // near-term put (Primary)
            put_leg_secondary(Side::Long, 1),  // far-term put (Secondary)
        ],
    )
}

pub fn double_diagonal() -> StrategyDef {
    strategy_relaxed(
        "double_diagonal",
        "Calendar",
        "Call diagonal + put diagonal at different strikes",
        vec![
            call_leg(Side::Short, 1),          // near-term call (Primary)
            call_leg_secondary(Side::Long, 1), // far-term call (Secondary)
            put_leg(Side::Short, 1),           // near-term put (Primary)
            put_leg_secondary(Side::Long, 1),  // far-term put (Secondary)
        ],
    )
}

pub fn all() -> Vec<StrategyDef> {
    vec![
        call_calendar_spread(),
        put_calendar_spread(),
        call_diagonal_spread(),
        put_diagonal_spread(),
        double_calendar(),
        double_diagonal(),
    ]
}
