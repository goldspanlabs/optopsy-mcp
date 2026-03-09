use crate::engine::types::to_display_name;
use crate::strategies::all_strategies;

use super::ai_format;
use super::response_types::{StrategiesResponse, StrategyInfo};

pub fn execute() -> StrategiesResponse {
    let strategies: Vec<StrategyInfo> = all_strategies()
        .into_iter()
        .map(|s| {
            let default_deltas = s.default_deltas();
            StrategyInfo {
                display_name: to_display_name(&s.name),
                name: s.name,
                category: s.category,
                legs: s.legs.len(),
                description: s.description,
                default_deltas,
            }
        })
        .collect();

    ai_format::format_strategies(strategies)
}
