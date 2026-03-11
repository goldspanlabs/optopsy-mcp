use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::signals::registry::SIGNAL_CATALOG;
use crate::signals::storage;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SignalCatalogEntry {
    pub name: String,
    pub description: String,
    pub params: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SignalsResponse {
    pub summary: String,
    pub total: usize,
    pub categories: HashMap<String, Vec<SignalCatalogEntry>>,
    pub ohlcv_columns: Vec<String>,
    pub combinators: Vec<String>,
    pub suggested_next_steps: Vec<String>,
}

pub fn execute() -> SignalsResponse {
    let mut categories: HashMap<String, Vec<SignalCatalogEntry>> = HashMap::new();

    for info in SIGNAL_CATALOG {
        categories
            .entry(info.category.to_string())
            .or_default()
            .push(SignalCatalogEntry {
                name: info.name.to_string(),
                description: info.description.to_string(),
                params: info.params.to_string(),
            });
    }

    // Include user-saved custom signals
    if let Ok(saved) = storage::list_saved_signals() {
        if !saved.is_empty() {
            let custom: Vec<SignalCatalogEntry> = saved
                .into_iter()
                .map(|s| SignalCatalogEntry {
                    name: s.name,
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

    let total: usize = categories.values().map(|v| v.len()).sum();

    SignalsResponse {
        summary: format!(
            "{total} signals available across {} categories. \
             Use entry_signal/exit_signal in run_backtest to filter trades by TA conditions.",
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
            "[THEN] Pass the signal JSON as entry_signal or exit_signal in run_backtest — OHLCV data is auto-fetched".into(),
        ],
    }
}
