//! Response types for `build_signal` and `construct_signal` tools.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::signals::registry::SignalSpec;

/// Entry representing a saved signal in the `list` action response.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SavedSignalEntry {
    pub name: String,
    pub formula: Option<String>,
    pub description: Option<String>,
    /// JSON snippet showing how to reference this signal as a `Saved` spec.
    pub usage: SavedSignalUsage,
}

/// Usage hint embedded in each `SavedSignalEntry`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SavedSignalUsage {
    #[serde(rename = "type")]
    pub kind: String,
    pub name: String,
}

/// Formula syntax reference returned when a validation error occurs.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FormulaHelp {
    pub columns: Vec<String>,
    pub lookback: String,
    pub functions: HashMap<String, String>,
    pub operators: Vec<String>,
    pub comparisons: Vec<String>,
    pub logical: Vec<String>,
    pub examples: Vec<String>,
}

/// Response for `build_signal`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BuildSignalResponse {
    pub summary: String,
    /// Whether the operation succeeded
    pub success: bool,
    /// The resolved signal spec (for create/get actions)
    pub signal_spec: Option<SignalSpec>,
    /// List of saved signals (for list action); empty when not applicable
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub saved_signals: Vec<SavedSignalEntry>,
    /// Formula syntax help (shown on validation errors)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub formula_help: Option<FormulaHelp>,
    /// Signal candidates from catalog search (action="search" only)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub candidates: Vec<SignalCandidate>,
    /// JSON Schema for `SignalSpec` enum (action="search" only)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,
    /// Default OHLCV column names (action="search" only)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column_defaults: Option<serde_json::Value>,
    /// Example And/Or combinator structures (action="search" only)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub combinator_examples: Vec<serde_json::Value>,
    /// Full signal catalog grouped by category (action="catalog" only)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<crate::tools::signals::SignalsResponse>,
    pub suggested_next_steps: Vec<String>,
}

/// Signal candidate from `build_signal` action="search"
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SignalCandidate {
    pub name: String,
    pub category: String,
    pub description: String,
    pub params: String,
    /// Concrete JSON example for this signal with sensible default parameters
    pub example: serde_json::Value,
}

/// Internal response from signal catalog search (used by `build_signal` action="search")
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConstructSignalResponse {
    pub summary: String,
    /// Whether the search found real matches (false = fallback showing all signals)
    pub had_real_matches: bool,
    pub candidates: Vec<SignalCandidate>,
    /// JSON Schema for `SignalSpec` enum, describing all valid signal types and their parameters
    pub schema: serde_json::Value,
    /// Default column names for OHLCV data (e.g., {"close": "adjclose", "high": "high"})
    pub column_defaults: serde_json::Value,
    /// Example JSON structures showing how to combine signals using And/Or operators
    pub combinator_examples: Vec<serde_json::Value>,
    pub suggested_next_steps: Vec<String>,
}
