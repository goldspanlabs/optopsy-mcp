//! Strategy definition types and helpers.

use super::enums::{Direction, ExpirationCycle, OptionType, Side};
use super::pricing::TargetRange;

/// Definition of a single leg within a strategy template.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct LegDef {
    pub side: Side,
    pub option_type: OptionType,
    pub delta: TargetRange,
    /// Number of contracts per unit of the strategy (e.g. 2 for butterfly body).
    pub qty: i32,
    /// Which expiration cycle this leg belongs to (`Primary` for near-term,
    /// `Secondary` for far-term in calendar/diagonal strategies).
    pub expiration_cycle: ExpirationCycle,
}

/// Convert a `snake_case` strategy name to Title Case (e.g. `"short_put"` → `"Short Put"`).
pub fn to_display_name(name: &str) -> String {
    name.split('_')
        .map(|w| {
            let mut c = w.chars();
            match c.next() {
                None => String::new(),
                Some(f) => {
                    let upper: String = f.to_uppercase().collect();
                    upper + c.as_str()
                }
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Complete definition of a named options strategy with its leg templates.
#[derive(Debug, Clone)]
pub struct StrategyDef {
    /// Internal `snake_case` identifier (e.g. `"iron_condor"`).
    pub name: String,
    pub category: String,
    pub description: String,
    /// Ordered leg definitions; leg count determines the strategy structure.
    pub legs: Vec<LegDef>,
    /// When `false`, adjacent legs may share the same strike (e.g. straddles,
    /// iron butterflies). When `true` (default), strikes must be strictly ascending.
    pub strict_strike_order: bool,
    pub direction: Direction,
    /// When `true`, the strategy includes a long stock leg (e.g. covered call, protective put).
    /// The engine will track stock entry/exit prices and include stock P&L in the trade.
    pub has_stock_leg: bool,
}

impl StrategyDef {
    /// Returns true if this strategy has legs with different expiration cycles.
    pub fn is_multi_expiration(&self) -> bool {
        self.legs
            .iter()
            .any(|l| l.expiration_cycle == ExpirationCycle::Secondary)
    }

    /// Returns the per-leg default delta targets embedded in the strategy definition.
    pub fn default_deltas(&self) -> Vec<TargetRange> {
        self.legs.iter().map(|l| l.delta.clone()).collect()
    }
}

/// Look up the market direction bias for a named strategy, defaulting to `Neutral` if unknown.
pub fn strategy_direction(name: &str) -> Direction {
    if let Some(def) = crate::strategies::find_strategy(name) {
        return def.direction;
    }
    tracing::warn!(
        strategy = name,
        "Unknown strategy — defaulting to Neutral direction"
    );
    Direction::Neutral
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strategy_direction_bullish() {
        assert_eq!(strategy_direction("long_call"), Direction::Bullish);
        assert_eq!(strategy_direction("short_put"), Direction::Bullish);
        assert_eq!(strategy_direction("covered_call"), Direction::Bullish);
        assert_eq!(strategy_direction("bull_call_spread"), Direction::Bullish);
        assert_eq!(strategy_direction("bull_put_spread"), Direction::Bullish);
    }

    #[test]
    fn strategy_direction_bearish() {
        assert_eq!(strategy_direction("short_call"), Direction::Bearish);
        assert_eq!(strategy_direction("long_put"), Direction::Bearish);
        assert_eq!(strategy_direction("bear_call_spread"), Direction::Bearish);
        assert_eq!(strategy_direction("bear_put_spread"), Direction::Bearish);
    }

    #[test]
    fn strategy_direction_volatile() {
        assert_eq!(strategy_direction("long_straddle"), Direction::Volatile);
        assert_eq!(strategy_direction("long_strangle"), Direction::Volatile);
        assert_eq!(
            strategy_direction("reverse_iron_condor"),
            Direction::Volatile
        );
        assert_eq!(
            strategy_direction("reverse_iron_butterfly"),
            Direction::Volatile
        );
    }

    #[test]
    fn strategy_direction_neutral() {
        assert_eq!(strategy_direction("iron_condor"), Direction::Neutral);
        assert_eq!(strategy_direction("short_straddle"), Direction::Neutral);
        assert_eq!(
            strategy_direction("long_call_butterfly"),
            Direction::Neutral
        );
        assert_eq!(strategy_direction("short_put_condor"), Direction::Neutral);
    }

    #[test]
    fn strategy_direction_all_32_covered() {
        let all = crate::strategies::all_strategies();
        for s in all {
            let dir = strategy_direction(&s.name);
            assert!(
                matches!(
                    dir,
                    Direction::Bullish
                        | Direction::Bearish
                        | Direction::Neutral
                        | Direction::Volatile
                ),
                "strategy {} returned unexpected direction",
                s.name
            );
        }
    }
}
