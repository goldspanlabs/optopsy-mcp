use anyhow::Result;
use serde_json::json;

use crate::strategies::all_strategies;

pub fn execute() -> Result<String> {
    let strategies: Vec<_> = all_strategies()
        .into_iter()
        .map(|s| {
            json!({
                "name": s.name,
                "category": s.category,
                "legs": s.legs.len(),
                "description": s.description,
            })
        })
        .collect();

    Ok(serde_json::to_string_pretty(&strategies)?)
}
