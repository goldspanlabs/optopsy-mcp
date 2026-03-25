//! Shared response types used across multiple tool modules.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::engine::types::{
    Commission, DteRange, ExpirationFilter, SizingConfig, Slippage, TargetRange, TradeSelector,
};

/// Data quality report included in backtest responses.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BacktestDataQuality {
    pub trading_days_total: usize,
    pub trading_days_with_price_data: usize,
    pub price_data_coverage_pct: f64,
    pub total_entry_candidates: usize,
    pub total_positions_opened: usize,
    pub fill_rate_pct: f64,
    pub median_entry_spread_pct: Option<f64>,
    pub warnings: Vec<String>,
}

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

/// Summary of backtest parameters echoed in optimization responses.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BacktestParamsSummary {
    pub strategy: String,
    pub display_name: String,
    pub leg_deltas: Vec<TargetRange>,
    pub entry_dte: DteRange,
    pub exit_dte: i32,
    pub slippage: Slippage,
    pub commission: Option<Commission>,
    pub capital: f64,
    pub quantity: i32,
    pub multiplier: i32,
    pub max_positions: i32,
    pub stop_loss: Option<f64>,
    pub take_profit: Option<f64>,
    pub max_hold_days: Option<i32>,
    pub selector: TradeSelector,
    pub entry_signal: Option<serde_json::Value>,
    pub exit_signal: Option<serde_json::Value>,
    pub min_net_premium: Option<f64>,
    pub max_net_premium: Option<f64>,
    pub min_net_delta: Option<f64>,
    pub max_net_delta: Option<f64>,
    pub min_days_between_entries: Option<i32>,
    pub expiration_filter: ExpirationFilter,
    pub exit_net_delta: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sizing: Option<SizingConfig>,
}

/// Aggregate statistics for all trades in a backtest.
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

/// P&L and date for a single notable trade (best or worst).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TradeStat {
    pub pnl: f64,
    pub date: String,
}

/// Summary of dynamic position sizing behavior.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SizingSummary {
    pub method: String,
    pub avg_quantity: f64,
    pub min_quantity: i32,
    pub max_quantity: i32,
    pub final_equity: f64,
}

/// Correlation entry for portfolio analysis.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CorrelationEntry {
    pub strategy_a: String,
    pub strategy_b: String,
    pub correlation: f64,
}

/// Single wheel strategy cycle (put → optional assignment → optional covered calls).
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
