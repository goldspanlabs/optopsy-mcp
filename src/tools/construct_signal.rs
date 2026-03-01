use super::response_types::{ConstructSignalResponse, SignalCandidate};
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
    let (candidates, had_real_matches) = fuzzy_search(prompt);

    // Generate live JSON Schema for SignalSpec
    let schema = schema_for!(SignalSpec);
    let schema_value = serde_json::to_value(&schema).unwrap_or(json!({}));

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

    let suggested_next_steps = vec![
        format!("Pick a candidate from above or use the schema to construct a custom SignalSpec"),
        format!("Pass the JSON example in entry_signal or exit_signal parameter of run_backtest"),
        format!("Use And/Or combinators to merge multiple signals"),
    ];

    ConstructSignalResponse {
        summary,
        candidates,
        schema: schema_value,
        column_defaults,
        combinator_examples,
        suggested_next_steps,
    }
}

/// Split a CamelCase string into lowercase words.
/// E.g., `RsiOversold` → `["rsi", "oversold"]`
fn split_camel_case(s: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();

    for ch in s.chars() {
        if ch.is_uppercase() && !current.is_empty() {
            words.push(current.to_lowercase());
            current = ch.to_string();
        } else {
            current.push(ch);
        }
    }

    if !current.is_empty() {
        words.push(current.to_lowercase());
    }

    words
}

/// Fuzzy search `SIGNAL_CATALOG` for signals matching the prompt.
/// Returns `(candidates, had_real_matches)` where `had_real_matches` indicates
/// whether matches were found (vs. fallback to all signals).
/// Scoring:
/// - +3 if any token exactly matches a word in signal name
/// - +2 if any token is substring of signal name
/// - +1 if any token appears in description
fn fuzzy_search(prompt: &str) -> (Vec<SignalCandidate>, bool) {
    let prompt_lower = prompt.to_lowercase();
    let tokens: Vec<&str> = prompt_lower.split_whitespace().collect();

    let mut scored_signals: Vec<(usize, usize)> = SIGNAL_CATALOG
        .iter()
        .enumerate()
        .map(|(idx, info)| {
            let name_lower = info.name.to_lowercase();
            let name_words_str = split_camel_case(info.name);
            let desc_lower = info.description.to_lowercase();

            let mut score = 0;

            for token in &tokens {
                // +3 for exact word match (split on CamelCase boundaries)
                if name_words_str.iter().any(|w| w == token) {
                    score += 3;
                }
                // +2 for substring in name
                else if name_lower.contains(token) {
                    score += 2;
                }
                // +1 for substring in description
                if desc_lower.contains(token) {
                    score += 1;
                }
            }

            (idx, score)
        })
        .collect();

    // Sort by score descending, take top-5 with score > 0
    scored_signals.sort_by_key(|&(_, score)| std::cmp::Reverse(score));

    let has_matches = scored_signals.iter().any(|(_, score)| *score > 0);

    let results = if has_matches {
        scored_signals
            .iter()
            .filter(|(_, score)| *score > 0)
            .take(5)
            .map(|(idx, _)| *idx)
            .collect::<Vec<_>>()
    } else {
        // Fallback: return all signals if no matches
        (0..SIGNAL_CATALOG.len()).collect()
    };

    let candidates = results
        .iter()
        .map(|&idx| {
            let info = &SIGNAL_CATALOG[idx];
            let example = build_example(info.name);
            SignalCandidate {
                name: info.name.to_string(),
                category: info.category.to_string(),
                description: info.description.to_string(),
                params: info.params.to_string(),
                example,
            }
        })
        .collect();

    (candidates, has_matches)
}

/// Build a concrete JSON example for a signal given its name.
/// Note: New signals added to `SIGNAL_CATALOG` must also be added to this function
/// to generate concrete examples. This is a necessary manual step to provide Claude
/// with sensible default parameter values for each signal type.
#[allow(clippy::too_many_lines)]
fn build_example(signal_name: &str) -> Value {
    match signal_name {
        // Momentum
        "RsiOversold" => json!({
            "type": "RsiOversold",
            "column": DEFAULT_CLOSE,
            "threshold": 30.0,
        }),
        "RsiOverbought" => json!({
            "type": "RsiOverbought",
            "column": DEFAULT_CLOSE,
            "threshold": 70.0,
        }),
        "MacdBullish" => json!({
            "type": "MacdBullish",
            "column": DEFAULT_CLOSE,
        }),
        "MacdBearish" => json!({
            "type": "MacdBearish",
            "column": DEFAULT_CLOSE,
        }),
        "MacdCrossover" => json!({
            "type": "MacdCrossover",
            "column": DEFAULT_CLOSE,
        }),
        "StochasticOversold" => json!({
            "type": "StochasticOversold",
            "close_col": DEFAULT_CLOSE,
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "period": 14,
            "threshold": 20.0,
        }),
        "StochasticOverbought" => json!({
            "type": "StochasticOverbought",
            "close_col": DEFAULT_CLOSE,
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "period": 14,
            "threshold": 80.0,
        }),
        // Overlap
        "PriceAboveSma" => json!({
            "type": "PriceAboveSma",
            "column": DEFAULT_CLOSE,
            "period": 20,
        }),
        "PriceBelowSma" => json!({
            "type": "PriceBelowSma",
            "column": DEFAULT_CLOSE,
            "period": 20,
        }),
        "PriceAboveEma" => json!({
            "type": "PriceAboveEma",
            "column": DEFAULT_CLOSE,
            "period": 20,
        }),
        "PriceBelowEma" => json!({
            "type": "PriceBelowEma",
            "column": DEFAULT_CLOSE,
            "period": 20,
        }),
        "SmaCrossover" => json!({
            "type": "SmaCrossover",
            "column": DEFAULT_CLOSE,
            "fast_period": 50,
            "slow_period": 200,
        }),
        "SmaCrossunder" => json!({
            "type": "SmaCrossunder",
            "column": DEFAULT_CLOSE,
            "fast_period": 50,
            "slow_period": 200,
        }),
        "EmaCrossover" => json!({
            "type": "EmaCrossover",
            "column": DEFAULT_CLOSE,
            "fast_period": 12,
            "slow_period": 26,
        }),
        "EmaCrossunder" => json!({
            "type": "EmaCrossunder",
            "column": DEFAULT_CLOSE,
            "fast_period": 12,
            "slow_period": 26,
        }),
        // Trend
        "AroonUptrend" => json!({
            "type": "AroonUptrend",
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "period": 25,
        }),
        "AroonDowntrend" => json!({
            "type": "AroonDowntrend",
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "period": 25,
        }),
        "AroonUpAbove" => json!({
            "type": "AroonUpAbove",
            "high_col": DEFAULT_HIGH,
            "period": 25,
            "threshold": 70.0,
        }),
        "SupertrendBullish" => json!({
            "type": "SupertrendBullish",
            "close_col": DEFAULT_CLOSE,
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "period": 10,
            "multiplier": 3.0,
        }),
        "SupertrendBearish" => json!({
            "type": "SupertrendBearish",
            "close_col": DEFAULT_CLOSE,
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "period": 10,
            "multiplier": 3.0,
        }),
        // Volatility
        "AtrAbove" => json!({
            "type": "AtrAbove",
            "close_col": DEFAULT_CLOSE,
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "period": 14,
            "threshold": 1.0,
        }),
        "AtrBelow" => json!({
            "type": "AtrBelow",
            "close_col": DEFAULT_CLOSE,
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "period": 14,
            "threshold": 0.5,
        }),
        "BollingerLowerTouch" => json!({
            "type": "BollingerLowerTouch",
            "column": DEFAULT_CLOSE,
            "period": 20,
        }),
        "BollingerUpperTouch" => json!({
            "type": "BollingerUpperTouch",
            "column": DEFAULT_CLOSE,
            "period": 20,
        }),
        "KeltnerLowerBreak" => json!({
            "type": "KeltnerLowerBreak",
            "close_col": DEFAULT_CLOSE,
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "period": 20,
            "multiplier": 2.0,
        }),
        "KeltnerUpperBreak" => json!({
            "type": "KeltnerUpperBreak",
            "close_col": DEFAULT_CLOSE,
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "period": 20,
            "multiplier": 2.0,
        }),
        // Price
        "GapUp" => json!({
            "type": "GapUp",
            "open_col": DEFAULT_OPEN,
            "close_col": DEFAULT_CLOSE,
            "threshold": 0.02,
        }),
        "GapDown" => json!({
            "type": "GapDown",
            "open_col": DEFAULT_OPEN,
            "close_col": DEFAULT_CLOSE,
            "threshold": 0.02,
        }),
        "DrawdownBelow" => json!({
            "type": "DrawdownBelow",
            "column": DEFAULT_CLOSE,
            "window": 20,
            "threshold": 0.1,
        }),
        "ConsecutiveUp" => json!({
            "type": "ConsecutiveUp",
            "column": DEFAULT_CLOSE,
            "count": 3,
        }),
        "ConsecutiveDown" => json!({
            "type": "ConsecutiveDown",
            "column": DEFAULT_CLOSE,
            "count": 3,
        }),
        "RateOfChange" => json!({
            "type": "RateOfChange",
            "column": DEFAULT_CLOSE,
            "period": 14,
            "threshold": 0.02,
        }),
        // Volume
        "MfiOversold" => json!({
            "type": "MfiOversold",
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "close_col": DEFAULT_CLOSE,
            "volume_col": DEFAULT_VOLUME,
            "period": 14,
            "threshold": 20.0,
        }),
        "MfiOverbought" => json!({
            "type": "MfiOverbought",
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "close_col": DEFAULT_CLOSE,
            "volume_col": DEFAULT_VOLUME,
            "period": 14,
            "threshold": 80.0,
        }),
        "ObvRising" => json!({
            "type": "ObvRising",
            "price_col": DEFAULT_CLOSE,
            "volume_col": DEFAULT_VOLUME,
        }),
        "ObvFalling" => json!({
            "type": "ObvFalling",
            "price_col": DEFAULT_CLOSE,
            "volume_col": DEFAULT_VOLUME,
        }),
        "CmfPositive" => json!({
            "type": "CmfPositive",
            "close_col": DEFAULT_CLOSE,
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "volume_col": DEFAULT_VOLUME,
            "period": 20,
        }),
        "CmfNegative" => json!({
            "type": "CmfNegative",
            "close_col": DEFAULT_CLOSE,
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "volume_col": DEFAULT_VOLUME,
            "period": 20,
        }),
        // Fallback
        _ => json!({}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fuzzy_search_rsi_oversold() {
        let (result, had_matches) = fuzzy_search("rsi oversold");
        let names: Vec<&str> = result.iter().map(|c| c.name.as_str()).collect();
        assert!(had_matches);
        assert!(names.contains(&"RsiOversold"));
    }

    #[test]
    fn fuzzy_search_macd() {
        let (result, had_matches) = fuzzy_search("macd");
        let names: Vec<&str> = result.iter().map(|c| c.name.as_str()).collect();
        assert!(had_matches);
        assert!(
            names.contains(&"MacdBullish")
                || names.contains(&"MacdBearish")
                || names.contains(&"MacdCrossover")
        );
    }

    #[test]
    fn fuzzy_search_golden_cross() {
        let (result, had_matches) = fuzzy_search("golden cross");
        let names: Vec<&str> = result.iter().map(|c| c.name.as_str()).collect();
        assert!(had_matches);
        assert!(names.contains(&"SmaCrossover"));
    }

    #[test]
    fn fuzzy_search_bollinger_upper() {
        let (result, had_matches) = fuzzy_search("bollinger upper");
        let names: Vec<&str> = result.iter().map(|c| c.name.as_str()).collect();
        assert!(had_matches);
        assert!(names.contains(&"BollingerUpperTouch"));
    }

    #[test]
    fn fuzzy_search_no_match_fallback() {
        let (result, had_matches) = fuzzy_search("xyzabc");
        // Should fallback to all signals (had_matches = false)
        assert!(!had_matches);
        assert_eq!(result.len(), SIGNAL_CATALOG.len());
    }

    #[test]
    fn execute_basic() {
        let response = execute("RSI oversold");
        assert!(!response.candidates.is_empty());
        assert!(response.schema != serde_json::Value::Null);
        assert_eq!(response.column_defaults["close"], "adjclose");
    }

    #[test]
    fn build_example_rsi() {
        let example = build_example("RsiOversold");
        assert_eq!(example["type"], "RsiOversold");
        assert_eq!(example["threshold"], 30.0);
    }
}
