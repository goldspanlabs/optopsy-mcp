//! OHLCV price bar — shared across engine and tools layers.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A single OHLCV price bar with epoch timestamp.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PriceBar {
    pub date: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    /// Adjusted close price (if available)
    pub adjclose: Option<f64>,
    pub volume: u64,
}
