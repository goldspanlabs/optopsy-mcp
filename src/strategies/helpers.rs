pub use crate::engine::types::{
    Direction, ExpirationCycle, LegDef, OptionType, Side, StrategyDef, TargetRange,
};

// --- Default delta constants (from original optopsy Python library) ---

/// Body/spread legs (e.g., short legs of condors, strangle legs)
pub fn default_otm_delta() -> TargetRange {
    TargetRange {
        target: 0.30,
        min: 0.25,
        max: 0.35,
    }
}

/// ATM legs (straddles, vertical spreads near the money)
pub fn default_atm_delta() -> TargetRange {
    TargetRange {
        target: 0.50,
        min: 0.45,
        max: 0.55,
    }
}

/// Far OTM wings (protective wings of condors/iron strategies)
pub fn default_deep_otm_delta() -> TargetRange {
    TargetRange {
        target: 0.10,
        min: 0.05,
        max: 0.15,
    }
}

/// Deep ITM delta (covered call stock proxy)
pub fn default_deep_itm_delta() -> TargetRange {
    TargetRange {
        target: 0.80,
        min: 0.75,
        max: 0.85,
    }
}

/// ITM delta (butterfly near wings, ITM legs)
pub fn default_itm_delta() -> TargetRange {
    TargetRange {
        target: 0.40,
        min: 0.35,
        max: 0.45,
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
    let direction = infer_direction(name, &legs);
    StrategyDef {
        name: name.to_string(),
        category: category.to_string(),
        description: description.to_string(),
        legs,
        strict_strike_order: true,
        direction,
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
    let direction = infer_direction(name, &legs);
    StrategyDef {
        name: name.to_string(),
        category: category.to_string(),
        description: description.to_string(),
        legs,
        strict_strike_order: false,
        direction,
    }
}

/// Infer market direction from the strategy name and leg composition.
///
/// Rules:
/// 1. Name contains "bull" or is a long call / short put / covered call / cash-secured put → Bullish
/// 2. Name contains "bear" or is a long put / short call → Bearish
/// 3. Mixed calls and puts with net long exposure, or name starts with "reverse_" → Volatile (buying vol)
/// 4. Everything else (net-zero or net-short symmetric structures) → Neutral
fn infer_direction(name: &str, legs: &[LegDef]) -> Direction {
    // Explicit directional names
    if name.contains("bull") || name == "covered_call" || name == "cash_secured_put" {
        return Direction::Bullish;
    }
    if name.contains("bear") {
        return Direction::Bearish;
    }

    // Single-leg strategies
    if legs.len() == 1 {
        let leg = &legs[0];
        return match (leg.side, leg.option_type) {
            (Side::Long, OptionType::Call) | (Side::Short, OptionType::Put) => Direction::Bullish,
            (Side::Long, OptionType::Put) | (Side::Short, OptionType::Call) => Direction::Bearish,
        };
    }

    // Multi-leg: check if buying both calls and puts (volatile), or selling both (neutral)
    let has_calls = legs.iter().any(|l| l.option_type == OptionType::Call);
    let has_puts = legs.iter().any(|l| l.option_type == OptionType::Put);
    let net_side: i32 = legs
        .iter()
        .map(|l| l.side.multiplier() as i32 * l.qty)
        .sum();

    if has_calls && has_puts {
        // Mixed option types: net long = volatile, net short = neutral
        if net_side > 0 || name.starts_with("reverse_") {
            return Direction::Volatile;
        }
        return Direction::Neutral;
    }

    // Same option type, net long vs short for straddles/strangles
    if net_side > 0 {
        Direction::Volatile
    } else {
        Direction::Neutral
    }
}
