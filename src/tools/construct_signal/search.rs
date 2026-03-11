//! Fuzzy search over the signal catalog using token-based scoring.
//!
//! Splits the user's natural-language prompt into tokens, matches against
//! `CamelCase`-split signal names and keyword lists, and returns ranked
//! candidates with concrete JSON examples.

use crate::signals::registry::SIGNAL_CATALOG;
use crate::tools::response_types::SignalCandidate;

use super::examples::build_example;

/// Split a CamelCase string into lowercase words.
/// E.g., `RsiBelow` → `["rsi", "below"]`
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
pub fn fuzzy_search(prompt: &str) -> (Vec<SignalCandidate>, bool) {
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
                // +1 for substring in description only
                else if desc_lower.contains(token) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fuzzy_search_rsi_below() {
        let (result, had_matches) = fuzzy_search("rsi below");
        let names: Vec<&str> = result.iter().map(|c| c.name.as_str()).collect();
        assert!(had_matches);
        assert!(names.contains(&"RsiBelow"));
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
        assert!(!had_matches);
        assert_eq!(result.len(), SIGNAL_CATALOG.len());
    }

    #[test]
    fn fuzzy_search_rsi_range() {
        let (result, had_matches) = fuzzy_search("rsi range");
        let names: Vec<&str> = result.iter().map(|c| c.name.as_str()).collect();
        assert!(had_matches);
        assert!(names.contains(&"RsiRange"));
    }
}
