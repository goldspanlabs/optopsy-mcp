//! Response types for walk-forward optimization.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::engine::types::{EquityPoint, PerformanceMetrics};

/// A single walk-forward window result.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WalkForwardWindowResult {
    pub window_idx: usize,
    pub train_start: String,
    pub train_end: String,
    pub test_start: String,
    pub test_end: String,
    pub best_params: HashMap<String, serde_json::Value>,
    pub in_sample_metric: f64,
    pub out_of_sample_metric: f64,
}

/// Unified walk-forward optimization response (used by both MCP and REST).
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct WalkForwardResponse {
    pub summary: String,
    pub strategy: String,
    pub symbol: String,
    pub mode: String,
    pub objective: String,
    pub n_windows: usize,
    pub efficiency_ratio: f64,
    pub windows: Vec<WalkForwardWindowResult>,
    pub stitched_equity: Vec<EquityPoint>,
    pub stitched_metrics: PerformanceMetrics,
    pub execution_time_ms: u64,
    pub key_findings: Vec<String>,
    pub suggested_next_steps: Vec<String>,
}
