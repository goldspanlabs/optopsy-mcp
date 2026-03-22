//! Response types for the wheel strategy backtest tool.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::engine::types::{PerformanceMetrics, TradeRecord};

use super::backtest::{BacktestDataQuality, UnderlyingPrice};

/// One wheel cycle: put → (optional assignment) → covered call(s) → called away.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WheelCycle {
    pub cycle_id: usize,
    pub put_entry_date: String,
    pub put_strike: f64,
    pub put_premium: f64,
    pub put_pnl: f64,
    pub assigned: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_basis: Option<f64>,
    pub calls_sold: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub call_pnls: Vec<f64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub call_premiums: Vec<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stock_pnl: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub called_away_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub called_away_strike: Option<f64>,
    pub total_pnl: f64,
    pub total_premium: f64,
    pub days_in_cycle: i32,
}

/// Aggregate statistics across all wheel cycles.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WheelCycleSummary {
    pub total_cycles: usize,
    pub completed_cycles: usize,
    pub put_only_cycles: usize,
    pub stopped_out_cycles: usize,
    pub avg_cycle_pnl: f64,
    pub avg_cycle_days: f64,
    pub avg_calls_per_assignment: f64,
    pub total_put_premium: f64,
    pub total_call_premium: f64,
    pub total_premium_collected: f64,
    pub total_stock_pnl: f64,
    pub assignment_rate: f64,
}

/// AI-enriched response for run_wheel_backtest.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WheelBacktestResponse {
    pub summary: String,
    pub assessment: String,
    pub key_findings: Vec<String>,
    pub metrics: PerformanceMetrics,
    pub trade_log: Vec<TradeRecord>,
    pub cycles: Vec<WheelCycle>,
    pub cycle_summary: WheelCycleSummary,
    pub data_quality: BacktestDataQuality,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub underlying_prices: Vec<UnderlyingPrice>,
    pub suggested_next_steps: Vec<String>,
}
