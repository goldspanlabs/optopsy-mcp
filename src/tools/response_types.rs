use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::engine::types::{
    CompareResult, EquityPoint, GroupStats, PerformanceMetrics, TradeRecord,
};

/// AI-enriched response for `run_backtest`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BacktestResponse {
    pub summary: String,
    pub assessment: String,
    pub key_findings: Vec<String>,
    pub metrics: PerformanceMetrics,
    pub trade_summary: TradeSummary,
    pub equity_curve_summary: EquityCurveSummary,
    pub equity_curve: Vec<EquityPoint>,
    pub trade_log: Vec<TradeRecord>,
    pub suggested_next_steps: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TradeSummary {
    pub total: usize,
    pub winners: usize,
    pub losers: usize,
    pub avg_pnl: f64,
    pub avg_winner: f64,
    pub avg_loser: f64,
    pub avg_days_held: f64,
    pub exit_breakdown: HashMap<String, usize>,
    pub best_trade: Option<TradeStat>,
    pub worst_trade: Option<TradeStat>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TradeStat {
    pub pnl: f64,
    pub date: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EquityCurveSummary {
    pub start_equity: f64,
    pub end_equity: f64,
    pub total_return_pct: f64,
    pub peak_equity: f64,
    pub trough_equity: f64,
    pub num_points: usize,
    pub sampled_curve: Vec<EquityPoint>,
}

/// AI-enriched response for `evaluate_strategy`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EvaluateResponse {
    pub summary: String,
    pub total_buckets: usize,
    pub total_trades: usize,
    pub best_bucket: Option<GroupStats>,
    pub worst_bucket: Option<GroupStats>,
    pub highest_win_rate_bucket: Option<GroupStats>,
    pub groups: Vec<GroupStats>,
    pub suggested_next_steps: Vec<String>,
}

/// AI-enriched response for `compare_strategies`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CompareResponse {
    pub summary: String,
    pub ranking_by_sharpe: Vec<String>,
    pub ranking_by_pnl: Vec<String>,
    pub best_overall: Option<String>,
    pub results: Vec<CompareResult>,
    pub suggested_next_steps: Vec<String>,
}

/// AI-enriched response for `download_options_data`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DownloadResponse {
    pub summary: String,
    pub symbol: String,
    pub new_rows: usize,
    pub total_rows: usize,
    pub was_resumed: bool,
    pub api_requests: u32,
    pub date_range: DateRange,
    pub suggested_next_steps: Vec<String>,
}

/// AI-enriched response for `load_data`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LoadDataResponse {
    pub summary: String,
    pub rows: usize,
    pub symbols: Vec<String>,
    pub date_range: DateRange,
    pub columns: Vec<String>,
    pub suggested_next_steps: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DateRange {
    pub start: Option<String>,
    pub end: Option<String>,
}

/// AI-enriched response for `list_strategies`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StrategiesResponse {
    pub summary: String,
    pub total: usize,
    pub categories: HashMap<String, usize>,
    pub strategies: Vec<StrategyInfo>,
    pub suggested_next_steps: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StrategyInfo {
    pub name: String,
    pub category: String,
    pub legs: usize,
    pub description: String,
}

/// Response for `check_cache_status`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CheckCacheResponse {
    pub summary: String,
    pub exists: bool,
    pub last_updated: Option<String>,
    pub file_path: String,
    pub suggested_next_steps: Vec<String>,
}

/// Response for `fetch_to_parquet`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FetchResponse {
    pub summary: String,
    pub rows: usize,
    pub symbol: String,
    pub file_path: String,
    pub date_range: DateRange,
    pub columns: Vec<String>,
    pub suggested_next_steps: Vec<String>,
}
