use super::helpers::{call_leg, put_leg, strategy, Side, StrategyDef};

pub fn call_calendar_spread() -> StrategyDef {
    strategy(
        "call_calendar_spread",
        "Calendar",
        "Sell near-term call, buy far-term call at same strike",
        vec![
            call_leg(Side::Short, 1), // near-term (lower DTE)
            call_leg(Side::Long, 1),  // far-term (higher DTE)
        ],
    )
}

pub fn put_calendar_spread() -> StrategyDef {
    strategy(
        "put_calendar_spread",
        "Calendar",
        "Sell near-term put, buy far-term put at same strike",
        vec![put_leg(Side::Short, 1), put_leg(Side::Long, 1)],
    )
}

pub fn call_diagonal_spread() -> StrategyDef {
    strategy(
        "call_diagonal_spread",
        "Calendar",
        "Sell near-term call, buy far-term call at different strike",
        vec![call_leg(Side::Short, 1), call_leg(Side::Long, 1)],
    )
}

pub fn put_diagonal_spread() -> StrategyDef {
    strategy(
        "put_diagonal_spread",
        "Calendar",
        "Sell near-term put, buy far-term put at different strike",
        vec![put_leg(Side::Short, 1), put_leg(Side::Long, 1)],
    )
}

pub fn double_calendar() -> StrategyDef {
    strategy(
        "double_calendar",
        "Calendar",
        "Call calendar + put calendar at different strikes",
        vec![
            call_leg(Side::Short, 1),
            call_leg(Side::Long, 1),
            put_leg(Side::Short, 1),
            put_leg(Side::Long, 1),
        ],
    )
}

pub fn double_diagonal() -> StrategyDef {
    strategy(
        "double_diagonal",
        "Calendar",
        "Call diagonal + put diagonal at different strikes",
        vec![
            call_leg(Side::Short, 1),
            call_leg(Side::Long, 1),
            put_leg(Side::Short, 1),
            put_leg(Side::Long, 1),
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
