pub use crate::engine::types::{
    ExpirationCycle, LegDef, OptionType, Side, StrategyDef, TargetRange,
};

// --- Default delta constants (from original optopsy Python library) ---

/// Body/spread legs (e.g., short legs of condors, strangle legs)
pub fn default_otm_delta() -> TargetRange {
    TargetRange {
        target: 0.30,
        min: 0.20,
        max: 0.40,
    }
}

/// ATM legs (straddles, vertical spreads near the money)
pub fn default_atm_delta() -> TargetRange {
    TargetRange {
        target: 0.50,
        min: 0.40,
        max: 0.60,
    }
}

/// Far OTM wings (protective wings of condors/iron strategies)
pub fn default_deep_otm_delta() -> TargetRange {
    TargetRange {
        target: 0.10,
        min: 0.05,
        max: 0.20,
    }
}

/// Deep ITM delta (covered call stock proxy)
pub fn default_deep_itm_delta() -> TargetRange {
    TargetRange {
        target: 0.80,
        min: 0.70,
        max: 0.90,
    }
}

/// ITM delta (butterfly near wings, ITM legs)
pub fn default_itm_delta() -> TargetRange {
    TargetRange {
        target: 0.40,
        min: 0.30,
        max: 0.50,
    }
}

pub fn leg(side: Side, option_type: OptionType, qty: i32, delta: TargetRange) -> LegDef {
    LegDef {
        side,
        option_type,
        delta,
        qty,
        expiration_cycle: ExpirationCycle::Primary,
    }
}

fn leg_secondary(side: Side, option_type: OptionType, qty: i32, delta: TargetRange) -> LegDef {
    LegDef {
        side,
        option_type,
        delta,
        qty,
        expiration_cycle: ExpirationCycle::Secondary,
    }
}

pub fn call_leg(side: Side, qty: i32, delta: TargetRange) -> LegDef {
    leg(side, OptionType::Call, qty, delta)
}

pub fn put_leg(side: Side, qty: i32, delta: TargetRange) -> LegDef {
    leg(side, OptionType::Put, qty, delta)
}

pub fn call_leg_secondary(side: Side, qty: i32, delta: TargetRange) -> LegDef {
    leg_secondary(side, OptionType::Call, qty, delta)
}

pub fn put_leg_secondary(side: Side, qty: i32, delta: TargetRange) -> LegDef {
    leg_secondary(side, OptionType::Put, qty, delta)
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
