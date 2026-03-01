pub use crate::engine::types::{
    ExpirationCycle, LegDef, OptionType, Side, StrategyDef, TargetRange,
};

pub fn leg(side: Side, option_type: OptionType, qty: i32) -> LegDef {
    LegDef {
        side,
        option_type,
        delta: TargetRange {
            target: 0.0,
            min: 0.0,
            max: 1.0,
        },
        qty,
        expiration_cycle: ExpirationCycle::Primary,
    }
}

fn leg_secondary(side: Side, option_type: OptionType, qty: i32) -> LegDef {
    LegDef {
        side,
        option_type,
        delta: TargetRange {
            target: 0.0,
            min: 0.0,
            max: 1.0,
        },
        qty,
        expiration_cycle: ExpirationCycle::Secondary,
    }
}

pub fn call_leg(side: Side, qty: i32) -> LegDef {
    leg(side, OptionType::Call, qty)
}

pub fn put_leg(side: Side, qty: i32) -> LegDef {
    leg(side, OptionType::Put, qty)
}

pub fn call_leg_secondary(side: Side, qty: i32) -> LegDef {
    leg_secondary(side, OptionType::Call, qty)
}

pub fn put_leg_secondary(side: Side, qty: i32) -> LegDef {
    leg_secondary(side, OptionType::Put, qty)
}

pub fn strategy(name: &str, category: &str, description: &str, legs: Vec<LegDef>) -> StrategyDef {
    StrategyDef {
        name: name.to_string(),
        category: category.to_string(),
        description: description.to_string(),
        legs,
        strict_strike_order: true,
    }
}

/// Build a strategy that allows adjacent legs to share the same strike
/// (e.g. straddles, iron butterflies, calendar spreads).
pub fn strategy_relaxed(
    name: &str,
    category: &str,
    description: &str,
    legs: Vec<LegDef>,
) -> StrategyDef {
    StrategyDef {
        name: name.to_string(),
        category: category.to_string(),
        description: description.to_string(),
        legs,
        strict_strike_order: false,
    }
}
