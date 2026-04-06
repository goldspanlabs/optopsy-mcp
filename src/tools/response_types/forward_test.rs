//! Response types for forward test tools: start, step, status.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ── Start forward test ──────────────────────────────────────────────────

/// Response from `start_forward_test`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StartForwardTestResponse {
    pub summary: String,
    pub session_id: String,
    pub strategy: String,
    pub symbol: String,
    pub capital: f64,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub baseline_sharpe: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub baseline_win_rate: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub baseline_max_dd: Option<f64>,
    pub key_findings: Vec<String>,
    pub suggested_next_steps: Vec<String>,
}

// ── Step forward test ───────────────────────────────────────────────────

/// A trade event that occurred during the step.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ForwardTestTradeEvent {
    pub date: String,
    pub action: String,
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pnl: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_type: Option<String>,
}

/// An open position in the forward test.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ForwardTestPosition {
    pub id: usize,
    pub symbol: String,
    pub entry_date: String,
    pub position_type: String,
    pub entry_cost: f64,
    pub unrealized_pnl: f64,
    pub days_held: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Response from `step_forward_test`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StepForwardTestResponse {
    pub summary: String,
    pub session_id: String,
    pub bars_processed: usize,
    pub date_range: String,
    pub current_equity: f64,
    pub daily_pnl: f64,
    pub cumulative_pnl: f64,
    pub trades: Vec<ForwardTestTradeEvent>,
    pub open_positions: Vec<ForwardTestPosition>,
    pub total_trades: i64,
    pub key_findings: Vec<String>,
    pub suggested_next_steps: Vec<String>,
}

// ── Forward test status ─────────────────────────────────────────────────

/// Equity curve point for the forward test.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ForwardTestEquityPoint {
    pub date: String,
    pub equity: f64,
    pub daily_pnl: f64,
}

/// Drift detection results comparing forward test to backtest baseline.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DriftAnalysis {
    pub forward_sharpe: Option<f64>,
    pub baseline_sharpe: Option<f64>,
    pub sharpe_drift: Option<f64>,
    pub forward_win_rate: Option<f64>,
    pub baseline_win_rate: Option<f64>,
    pub win_rate_drift: Option<f64>,
    pub forward_max_dd: Option<f64>,
    pub baseline_max_dd: Option<f64>,
    pub max_dd_drift: Option<f64>,
    pub status: String,
    pub assessment: String,
}

/// Response from `forward_test_status`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ForwardTestStatusResponse {
    pub summary: String,
    pub session_id: String,
    pub strategy: String,
    pub symbol: String,
    pub status: String,
    pub capital: f64,
    pub current_equity: f64,
    pub total_return_pct: f64,
    pub total_trades: i64,
    pub realized_pnl: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_bar_date: Option<String>,
    pub days_running: i64,
    pub equity_curve: Vec<ForwardTestEquityPoint>,
    pub recent_trades: Vec<ForwardTestTradeEvent>,
    pub open_positions: Vec<ForwardTestPosition>,
    pub drift: Option<DriftAnalysis>,
    pub confidence_level: String,
    pub key_findings: Vec<String>,
    pub suggested_next_steps: Vec<String>,
}
