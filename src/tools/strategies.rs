//! List all built-in options strategies with metadata and default delta targets.
//!
//! Iterates the 32-strategy registry, collecting name, category, leg count,
//! description, and per-leg default deltas into a categorized response.

use crate::engine::types::to_display_name;
use crate::strategies::all_strategies;

use super::ai_format;
use super::response_types::{StrategiesResponse, StrategyInfo};

/// Collect all registered strategies and format them into a categorized response.
pub fn execute() -> StrategiesResponse {
    let strategies: Vec<StrategyInfo> = all_strategies()
        .iter()
        .map(|s| {
            let default_deltas = s.default_deltas();
            StrategyInfo {
                display_name: to_display_name(&s.name),
                name: s.name.clone(),
                category: s.category.clone(),
                legs: s.legs.len(),
                description: s.description.clone(),
                default_deltas,
            }
        })
        .collect();

    ai_format::format_strategies(strategies)
}
