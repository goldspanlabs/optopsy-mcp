//! Response types for options and stock backtest tools, plus shared helpers.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::engine::types::{
    Commission, ConflictResolution, DteRange, ExpirationFilter, PerformanceMetrics, Side,
    SizingConfig, Slippage, TargetRange, TradeRecord, TradeSelector,
};
use crate::signals::helpers::IndicatorData;

/// Data quality report included in backtest responses, summarizing price coverage
/// and fill statistics to help assess result reliability.
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

/// Summary of dynamic position sizing behavior across all trades.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SizingSummary {
    /// Sizing method used (e.g. `"fixed_fractional(2.0%)"`).
    pub method: String,
    /// Average computed quantity across all trades
    pub avg_quantity: f64,
    /// Minimum computed quantity
    pub min_quantity: i32,
    /// Maximum computed quantity
    pub max_quantity: i32,
    /// Final portfolio equity at end of simulation
    pub final_equity: f64,
}

/// AI-enriched response for `run_options_backtest`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BacktestResponse {
    pub summary: String,
    pub assessment: String,
    pub key_findings: Vec<String>,
    /// Parameters used for this backtest (for context in follow-up questions)
    pub parameters: BacktestParamsSummary,
    pub metrics: PerformanceMetrics,
    pub trade_summary: TradeSummary,
    pub trade_log: Vec<TradeRecord>,
    pub data_quality: BacktestDataQuality,
    /// Dynamic position sizing summary (only present when sizing is active)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sizing_summary: Option<SizingSummary>,
    /// Underlying close prices for the backtest period (empty if OHLCV data not cached)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub underlying_prices: Vec<UnderlyingPrice>,
    /// Raw indicator values for charting (RSI line, SMA curve, etc.)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub indicator_data: Vec<IndicatorData>,
    pub suggested_next_steps: Vec<String>,
}

/// Summary of backtest parameters echoed in responses so callers have full context
/// for follow-up questions without needing to re-send the original request.
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
    /// Position sizing configuration (only present when sizing is active)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sizing: Option<SizingConfig>,
}

/// Summary of stock backtest parameters echoed in responses.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StockBacktestParamsSummary {
    pub symbol: String,
    pub side: Side,
    pub capital: f64,
    pub quantity: i32,
    pub max_positions: i32,
    pub slippage: Slippage,
    pub commission: Option<Commission>,
    pub stop_loss: Option<f64>,
    pub take_profit: Option<f64>,
    pub max_hold_days: Option<i32>,
    /// Maximum bars to hold (intraday)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_hold_bars: Option<i32>,
    pub entry_signal: Option<serde_json::Value>,
    pub exit_signal: Option<serde_json::Value>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    /// Bar interval used
    pub interval: String,
    /// Position sizing configuration (only present when sizing is active)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sizing: Option<SizingConfig>,
    /// SL/TP conflict resolution strategy (omitted when default `StopLossFirst`)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conflict_resolution: Option<ConflictResolution>,
}

/// AI-enriched response for `run_stock_backtest`, matching options backtest output shape.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StockBacktestResponse {
    pub summary: String,
    pub assessment: String,
    pub key_findings: Vec<String>,
    pub parameters: StockBacktestParamsSummary,
    pub metrics: PerformanceMetrics,
    pub trade_summary: TradeSummary,
    pub trade_log: Vec<TradeRecord>,
    /// Dynamic position sizing summary (only present when sizing is active)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sizing_summary: Option<SizingSummary>,
    /// Underlying close prices for charting
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub underlying_prices: Vec<UnderlyingPrice>,
    /// Raw indicator values for charting (RSI line, SMA curve, etc.)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub indicator_data: Vec<IndicatorData>,
    /// Diagnostic warnings (e.g. entries skipped due to insufficient capital).
    /// When non-empty, the LLM should address these before interpreting results.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    pub suggested_next_steps: Vec<String>,
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
