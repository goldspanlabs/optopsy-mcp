//! Response types for portfolio backtest.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::engine::types::{EquityPoint, PerformanceMetrics};

use super::backtest::TradeSummary;

/// Result for a single strategy within a portfolio backtest.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PortfolioStrategyResult {
    pub label: String,
    pub symbol: String,
    pub side: String,
    pub allocation_pct: f64,
    pub metrics: PerformanceMetrics,
    pub trade_summary: TradeSummary,
    pub contribution_pct: f64,
    pub total_pnl: f64,
}

/// Pairwise Pearson correlation between two strategies' daily returns.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CorrelationEntry {
    pub strategy_a: String,
    pub strategy_b: String,
    pub correlation: f64,
}

/// Full response from the `portfolio_backtest` tool.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PortfolioBacktestResponse {
    pub summary: String,
    pub assessment: String,
    pub key_findings: Vec<String>,
    pub capital: f64,
    pub portfolio_metrics: PerformanceMetrics,
    pub strategy_results: Vec<PortfolioStrategyResult>,
    pub correlation_matrix: Vec<CorrelationEntry>,
    pub equity_curve: Vec<EquityPoint>,
    pub suggested_next_steps: Vec<String>,
}
