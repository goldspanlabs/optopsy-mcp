//! Shared response types used across multiple tool modules.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// OHLCV price bar for overlaying the underlying's price on charts.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct UnderlyingPrice {
    pub date: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume: Option<u64>,
}

/// Correlation entry for portfolio analysis.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CorrelationEntry {
    pub strategy_a: String,
    pub strategy_b: String,
    pub correlation: f64,
}
