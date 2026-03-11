mod examples;
mod search;

use super::response_types::ConstructSignalResponse;
use crate::signals::registry::{SignalSpec, SIGNAL_CATALOG};
use schemars::schema_for;
use serde_json::{json, Value};

// OHLCV column name conventions from Yahoo Finance
const DEFAULT_CLOSE: &str = "adjclose";
const DEFAULT_OPEN: &str = "open";
const DEFAULT_HIGH: &str = "high";
const DEFAULT_LOW: &str = "low";
const DEFAULT_VOLUME: &str = "volume";

pub fn execute(prompt: &str) -> ConstructSignalResponse {
    // Fuzzy search SIGNAL_CATALOG for matches
    let (candidates, had_real_matches) = search::fuzzy_search(prompt);

    // Generate live JSON Schema for SignalSpec
    let schema = schema_for!(SignalSpec);
    let schema_value = match serde_json::to_value(&schema) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("Failed to serialize SignalSpec schema: {e}");
            Value::Null
        }
    };

    // Build column defaults
    let column_defaults = json!({
        "close": DEFAULT_CLOSE,
        "open": DEFAULT_OPEN,
        "high": DEFAULT_HIGH,
        "low": DEFAULT_LOW,
        "volume": DEFAULT_VOLUME,
    });

    // Build combinator examples from top 2 candidates
    let combinator_examples = if candidates.len() >= 2 {
        let left = candidates[0].example.clone();
        let right = candidates[1].example.clone();
        vec![
            json!({
                "type": "And",
                "left": left.clone(),
                "right": right.clone(),
            }),
            json!({
                "type": "Or",
                "left": left,
                "right": right,
            }),
        ]
    } else {
        vec![]
    };

    let summary = if had_real_matches {
        format!(
            "Found {} signal(s) matching '{}'.",
            candidates.len(),
            prompt
        )
    } else {
        format!(
            "No signals matched '{}'. Showing all {} available signals.",
            prompt,
            SIGNAL_CATALOG.len()
        )
    };

    let has_range_candidates =
        had_real_matches && candidates.iter().any(|c| c.name.ends_with("Range"));
    let mut suggested_next_steps = vec![
        "Pick a candidate from above or use the schema to construct a custom SignalSpec".to_string(),
        "Pass the JSON example as entry_signal or exit_signal in run_backtest — OHLCV data is auto-fetched when signals are used".to_string(),
    ];
    if has_range_candidates {
        suggested_next_steps.push("Range signals use the And combinator pattern. Adjust the lower/upper thresholds (left/right) in the example to define your range.".to_string());
    }

    ConstructSignalResponse {
        summary,
        had_real_matches,
        candidates,
        schema: schema_value,
        column_defaults,
        combinator_examples,
        suggested_next_steps,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execute_basic() {
        let response = execute("RSI below");
        assert!(!response.candidates.is_empty());
        assert!(response.schema != serde_json::Value::Null);
        assert_eq!(response.column_defaults["close"], "adjclose");
    }

    #[test]
    fn execute_rsi_range_shows_range_hint() {
        let response = execute("RSI range");
        let has_range = response.candidates.iter().any(|c| c.name == "RsiRange");
        assert!(has_range);
        assert!(response
            .suggested_next_steps
            .iter()
            .any(|s| s.contains("Range signals use the And combinator")));
    }

    #[test]
    fn execute_no_match_suppresses_range_hint() {
        let response = execute("xyzabc");
        assert!(!response
            .suggested_next_steps
            .iter()
            .any(|s| s.contains("Range signals")));
    }
}
