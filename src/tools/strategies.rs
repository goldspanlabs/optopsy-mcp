use crate::strategies::all_strategies;

use super::ai_format;
use super::response_types::{StrategiesResponse, StrategyInfo};

pub fn execute() -> StrategiesResponse {
    let strategies: Vec<StrategyInfo> = all_strategies()
        .into_iter()
        .map(|s| StrategyInfo {
            name: s.name,
            category: s.category,
            legs: s.legs.len(),
            description: s.description,
        })
        .collect();

    ai_format::format_strategies(strategies)
}
