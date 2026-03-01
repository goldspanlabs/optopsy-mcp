pub mod butterflies;
pub mod calendar;
pub mod condors;
pub mod helpers;
pub mod iron;
pub mod singles;
pub mod spreads;

use crate::engine::types::StrategyDef;

pub fn all_strategies() -> Vec<StrategyDef> {
    let mut strategies = Vec::new();
    strategies.extend(singles::all());
    strategies.extend(spreads::all());
    strategies.extend(butterflies::all());
    strategies.extend(condors::all());
    strategies.extend(iron::all());
    strategies.extend(calendar::all());
    strategies
}

pub fn find_strategy(name: &str) -> Option<StrategyDef> {
    all_strategies().into_iter().find(|s| s.name == name)
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
        names.sort();
        names.dedup();
        assert_eq!(names.len(), total, "Duplicate strategy names found");
    }

    #[test]
    fn spread_strategies_have_two_legs() {
        for s in all_strategies() {
            if s.category == "spreads" {
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
            if s.category == "butterflies" {
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
