//! List all available TA signals grouped by category, including user-saved custom signals.
//!
//! Reads the built-in `SIGNAL_CATALOG` and any persisted custom signals from disk,
//! then formats them into a categorized listing for the `list_signals` MCP tool.

use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::signals::registry::SIGNAL_CATALOG;
use crate::signals::storage;

/// Convert a `PascalCase` name to spaced words (e.g. `RsiBelow` → "RSI Below").
/// Consecutive uppercase letters are kept together as acronyms.
fn to_display_name(name: &str) -> String {
    let mut result = String::with_capacity(name.len() + 4);
    let chars: Vec<char> = name.chars().collect();
    for (i, &c) in chars.iter().enumerate() {
        if i > 0 && c.is_uppercase() {
            // Insert space before uppercase that follows a lowercase,
            // or before the last char of an uppercase run followed by lowercase
            let prev_lower = chars[i - 1].is_lowercase();
            let next_lower = chars.get(i + 1).is_some_and(|n| n.is_lowercase());
            if prev_lower || (next_lower && chars[i - 1].is_uppercase()) {
                result.push(' ');
            }
        }
        result.push(c);
    }
    result
}

/// A single entry in the signal catalog with display name, description, and parameter info.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SignalCatalogEntry {
    pub name: String,
    pub display_name: String,
    pub description: String,
    pub params: String,
    pub formula_example: String,
}

/// Response for `list_signals`, containing all signals grouped by category with combinator hints.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SignalsResponse {
    pub summary: String,
    pub total: usize,
    pub categories: HashMap<String, Vec<SignalCatalogEntry>>,
    pub ohlcv_columns: Vec<String>,
    pub combinators: Vec<String>,
    pub suggested_next_steps: Vec<String>,
}

/// Build and return the full signal catalog, including any user-saved custom signals.
pub fn execute() -> SignalsResponse {
    let mut categories: HashMap<String, Vec<SignalCatalogEntry>> = HashMap::new();

    for info in SIGNAL_CATALOG {
        categories
            .entry(info.category.to_string())
            .or_default()
            .push(SignalCatalogEntry {
                display_name: to_display_name(info.name),
                name: info.name.to_string(),
                description: info.description.to_string(),
                params: info.params.to_string(),
                formula_example: info.formula_example.to_string(),
            });
    }

    // Include user-saved custom signals
    if let Ok(saved) = storage::list_saved_signals() {
        if !saved.is_empty() {
            let custom: Vec<SignalCatalogEntry> = saved
                .into_iter()
                .map(|s| SignalCatalogEntry {
                    display_name: to_display_name(&s.name),
                    name: s.name,
                    formula_example: s.formula.clone().unwrap_or_default(),
                    description: s
                        .description
                        .unwrap_or_else(|| s.formula.unwrap_or_default()),
                    params: "custom formula".to_string(),
                })
                .collect();
            categories
                .entry("custom".to_string())
                .or_default()
                .extend(custom);
        }
    }

    let total: usize = categories.values().map(Vec::len).sum();

    SignalsResponse {
        summary: format!(
            "{total} signals available across {} categories. \
             Use entry_signal/exit_signal in run_options_backtest to filter trades by TA conditions.",
            categories.len()
        ),
        total,
        categories,
        ohlcv_columns: vec![
            "date".into(),
            "open".into(),
            "high".into(),
            "low".into(),
            "close".into(),
            "adjclose".into(),
            "volume".into(),
        ],
        combinators: vec![
            r#"{"type": "And", "left": <signal>, "right": <signal>} — both must be true"#.into(),
            r#"{"type": "Or", "left": <signal>, "right": <signal>} — either must be true"#.into(),
        ],
        suggested_next_steps: vec![
            "[NEXT] Call build_signal({ action: \"search\", prompt: \"<signal_name>\" }) to get the JSON spec for a signal".into(),
            "[THEN] Pass the signal JSON as entry_signal or exit_signal in run_options_backtest — OHLCV data is loaded from cache".into(),
        ],
    }
}
