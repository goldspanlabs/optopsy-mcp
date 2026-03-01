use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::signals::registry::SIGNAL_CATALOG;

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

    let total = SIGNAL_CATALOG.len();

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
            "And { left: <signal>, right: <signal> } — both must be true".into(),
            "Or { left: <signal>, right: <signal> } — either must be true".into(),
        ],
        suggested_next_steps: vec![
            "Use entry_signal in run_backtest to only enter on signal days".into(),
            "Use exit_signal in run_backtest to trigger early exits on signal days".into(),
            "Combine signals with And/Or for compound conditions".into(),
        ],
    }
}
