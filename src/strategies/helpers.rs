pub use crate::engine::types::{LegDef, OptionType, Side, StrategyDef, TargetRange};

pub fn leg(side: Side, option_type: OptionType, qty: i32) -> LegDef {
    LegDef {
        side,
        option_type,
        delta: TargetRange { target: 0.0, min: 0.0, max: 1.0 },
        qty,
    }
}

pub fn call_leg(side: Side, qty: i32) -> LegDef {
    leg(side, OptionType::Call, qty)
}

pub fn put_leg(side: Side, qty: i32) -> LegDef {
    leg(side, OptionType::Put, qty)
}

pub fn strategy(name: &str, category: &str, description: &str, legs: Vec<LegDef>) -> StrategyDef {
    StrategyDef {
        name: name.to_string(),
        category: category.to_string(),
        description: description.to_string(),
        legs,
    }
}
