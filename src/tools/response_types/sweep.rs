//! Response types for `parameter_sweep` and `bayesian_optimize`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A single parameter combination result.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SweepResult {
    pub rank: usize,
    pub params: HashMap<String, serde_json::Value>,
    pub sharpe: f64,
    pub sortino: f64,
    pub pnl: f64,
    pub trades: usize,
    pub win_rate: f64,
    pub max_drawdown: f64,
    pub profit_factor: f64,
    pub cagr: f64,
    pub calmar: f64,
}

/// Per-value stats for a single swept parameter.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DimensionStat {
    pub value: String,
    pub avg_metric: f64,
    pub count: usize,
}

/// Shared response shape for both grid sweep and Bayesian optimization.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct SweepResponse {
    pub mode: String,
    pub objective: String,
    pub combinations_total: usize,
    pub combinations_run: usize,
    pub combinations_failed: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub best_result: Option<SweepResult>,
    pub ranked_results: Vec<SweepResult>,
    pub dimension_sensitivity: HashMap<String, Vec<DimensionStat>>,
    /// Bayesian only: best objective value after each evaluation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub convergence_trace: Option<Vec<f64>>,
    pub execution_time_ms: u64,
    /// Full backtest responses per combo — parallel to `ranked_results`.
    /// Skipped from MCP serialization (too large); used by REST handler for DB storage.
    #[serde(skip)]
    #[schemars(skip)]
    pub full_results: Vec<crate::scripting::engine::ScriptBacktestResult>,
}
