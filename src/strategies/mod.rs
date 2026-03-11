//! Built-in options strategy definitions.
//!
//! Contains 32 strategies across singles, spreads, butterflies, condors, iron,
//! and calendar categories. Each strategy defines its legs, delta targets, and
//! strike ordering rules.

pub mod butterflies;
pub mod calendar;
pub mod condors;
pub mod helpers;
pub mod iron;
pub mod singles;
pub mod spreads;

use std::sync::OnceLock;

use crate::engine::types::StrategyDef;

static STRATEGY_REGISTRY: OnceLock<Vec<StrategyDef>> = OnceLock::new();

/// Return the full strategy registry. The list is built once and cached for the
/// lifetime of the process, so subsequent calls are allocation-free O(1) lookups.
pub fn all_strategies() -> &'static [StrategyDef] {
    STRATEGY_REGISTRY.get_or_init(|| {
        let mut strategies = Vec::new();
        strategies.extend(singles::all());
        strategies.extend(spreads::all());
        strategies.extend(butterflies::all());
        strategies.extend(condors::all());
        strategies.extend(iron::all());
        strategies.extend(calendar::all());
        strategies
    })
}

/// Find a strategy by name (case-sensitive). Returns `None` if not found.
pub fn find_strategy(name: &str) -> Option<StrategyDef> {
    all_strategies().iter().find(|s| s.name == name).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::types::Side;

    #[test]
    fn all_strategies_returns_32() {
        assert_eq!(all_strategies().len(), 32);
    }

    #[test]
    fn find_strategy_known() {
        let s = find_strategy("long_call");
        assert!(s.is_some());
        let s = s.unwrap();
        assert_eq!(s.legs.len(), 1);
        assert_eq!(s.legs[0].side, Side::Long);
    }

    #[test]
    fn find_strategy_unknown_returns_none() {
        assert!(find_strategy("nonexistent_strategy").is_none());
    }

    #[test]
    fn all_strategies_have_at_least_one_leg() {
        for s in all_strategies() {
            assert!(!s.legs.is_empty(), "Strategy '{}' has no legs", s.name);
        }
    }

    #[test]
    fn all_strategies_have_unique_names() {
        let strategies = all_strategies();
        let mut names: Vec<&str> = strategies.iter().map(|s| s.name.as_str()).collect();
        let total = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), total, "Duplicate strategy names found");
    }

    #[test]
    fn spread_strategies_have_two_legs() {
        for s in all_strategies() {
            if s.category == "Spreads" {
                assert_eq!(
                    s.legs.len(),
                    2,
                    "Spread '{}' should have 2 legs, has {}",
                    s.name,
                    s.legs.len()
                );
            }
        }
    }

    #[test]
    fn butterfly_strategies_have_three_legs() {
        for s in all_strategies() {
            if s.category == "Butterflies" {
                assert_eq!(
                    s.legs.len(),
                    3,
                    "Butterfly '{}' should have 3 legs, has {}",
                    s.name,
                    s.legs.len()
                );
            }
        }
    }
}
