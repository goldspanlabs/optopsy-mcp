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

    /// Validate that user-provided delta targets are compatible with this strategy's
    /// strike ordering requirements. Returns `Ok(())` if valid, or an `Err` with a
    /// diagnostic message explaining the expected delta ordering.
    ///
    /// For strategies with `strict_strike_order`, strikes must be ascending across legs.
    /// Since delta and strike have an inverse relationship for calls (higher strike =
    /// lower delta) and a direct relationship for puts (higher strike = higher delta),
    /// the expected delta ordering depends on the option types of consecutive legs.
    pub fn validate_delta_ordering(&self, leg_deltas: &[TargetRange]) -> Result<(), String> {
        if leg_deltas.len() != self.legs.len() {
            return Err(format!(
                "Strategy '{}' has {} leg(s) but {} delta target(s) were provided.",
                self.name,
                self.legs.len(),
                leg_deltas.len(),
            ));
        }

        // Only validate strategies with strict strike ordering and 2+ legs
        if !self.strict_strike_order || self.legs.len() < 2 {
            return Ok(());
        }

        // Skip multi-expiration strategies (calendar/diagonal) — strike ordering
        // is per-cycle, not sequential across all legs
        if self.is_multi_expiration() {
            return Ok(());
        }

        let mut warnings = Vec::new();

        for i in 0..self.legs.len() - 1 {
            let leg_a = &self.legs[i];
            let leg_b = &self.legs[i + 1];
            let delta_a = leg_deltas[i].target;
            let delta_b = leg_deltas[i + 1].target;

            // Strike ordering: strike[i] < strike[i+1]
            // For calls: higher strike = lower delta, so delta[i] > delta[i+1]
            // For puts:  higher strike = higher delta, so delta[i] < delta[i+1]
            // For mixed (put→call at the boundary): put delta < call delta is typical
            //   but depends on where the strikes fall — skip mixed-type pairs

            let (expected, relation) = match (leg_a.option_type, leg_b.option_type) {
                (OptionType::Call, OptionType::Call) => {
                    // Both calls, ascending strikes → descending deltas
                    (delta_a > delta_b, "greater than")
                }
                (OptionType::Put, OptionType::Put) => {
                    // Both puts, ascending strikes → ascending deltas
                    (delta_a < delta_b, "less than")
                }
                _ => {
                    // Mixed types (e.g., iron condor put→call boundary) — skip
                    continue;
                }
            };

            if !expected {
                let side_a = if leg_a.side == Side::Long {
                    "Long"
                } else {
                    "Short"
                };
                let side_b = if leg_b.side == Side::Long {
                    "Long"
                } else {
                    "Short"
                };
                let type_a = if leg_a.option_type == OptionType::Call {
                    "Call"
                } else {
                    "Put"
                };
                let type_b = if leg_b.option_type == OptionType::Call {
                    "Call"
                } else {
                    "Put"
                };
                warnings.push(format!(
                    "leg {i} ({side_a} {type_a}, delta={delta_a:.2}) should have delta {relation} \
                     leg {} ({side_b} {type_b}, delta={delta_b:.2}) \
                     to produce ascending strikes. \
                     The strategy's default deltas are: leg {i}={:.2}, leg {}={:.2}.",
                    i + 1,
                    leg_a.delta.target,
                    i + 1,
                    leg_b.delta.target,
                ));
            }
        }

        if warnings.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "Delta targets for '{}' will likely produce no valid entries due to \
                 strike ordering violations: {}",
                self.name,
                warnings.join("; "),
            ))
        }
    }
}

/// Look up the market direction bias for a named strategy.
///
/// The built-in strategy registry has been removed; this now always returns
/// `Neutral`. Scripting-engine strategies carry their own direction metadata.
pub fn strategy_direction(_name: &str) -> Direction {
    Direction::Neutral
}
