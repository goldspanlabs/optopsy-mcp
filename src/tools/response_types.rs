use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::engine::sweep::{DimensionStats, OosResult};
use crate::engine::types::{
    Commission, CompareResult, DteRange, PerformanceMetrics, Slippage, SweepResult, TargetRange,
    TradeRecord, TradeSelector,
};
use crate::signals::registry::SignalSpec;

/// Data quality report for `run_backtest`
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

/// AI-enriched response for `run_backtest`
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
    pub suggested_next_steps: Vec<String>,
}

/// Summary of backtest parameters for reference in responses
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BacktestParamsSummary {
    pub strategy: String,
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
    /// Trade selector used (`Nearest`, `HighestPremium`, `LowestPremium`, or `First`)
    pub selector: TradeSelector,
    /// Entry signal specification, if any
    pub entry_signal: Option<serde_json::Value>,
    /// Exit signal specification, if any
    pub exit_signal: Option<serde_json::Value>,
}

/// Status of currently loaded data
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StatusResponse {
    pub summary: String,
    /// Symbols currently loaded in memory
    pub loaded_symbols: Vec<String>,
    /// Total number of rows across all loaded symbols, if any
    pub rows: Option<usize>,
    /// Date range of loaded data, if available.
    /// Note: currently not populated by `tools::status::execute` and may be `None`.
    pub date_range: Option<DateRange>,
    /// Available columns in loaded data (from first symbol when sorted lexicographically)
    pub columns: Vec<String>,
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

/// Parameters for a single strategy comparison entry
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CompareStrategyEntry {
    pub name: String,
    pub leg_deltas: Vec<TargetRange>,
    pub entry_dte: DteRange,
    pub exit_dte: i32,
    pub slippage: Slippage,
    pub commission: Option<Commission>,
}

/// AI-enriched response for `compare_strategies`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CompareResponse {
    pub summary: String,
    /// The strategies and parameters that were compared (for reference in follow-up questions)
    pub strategies_compared: Vec<CompareStrategyEntry>,
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
    /// The symbol that was loaded (for reference in follow-up questions)
    pub symbol: String,
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
    /// Default per-leg delta targets for this strategy (used when `leg_deltas` is omitted)
    pub default_deltas: Vec<TargetRange>,
}

/// Response for `check_cache_status`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CheckCacheResponse {
    pub summary: String,
    /// The symbol that was checked (for reference in follow-up questions)
    pub symbol: String,
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

/// A single OHLCV price bar for `get_raw_prices`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PriceBar {
    pub date: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    /// Adjusted close price (if available)
    pub adjclose: Option<f64>,
    pub volume: u64,
}

/// Response for `get_raw_prices` — returns actual price data points for charting
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RawPricesResponse {
    pub summary: String,
    pub symbol: String,
    /// Total rows in the cached dataset (before sampling)
    pub total_rows: usize,
    /// Number of price bars returned in this response
    pub returned_rows: usize,
    /// Whether the data was down-sampled to fit the limit
    pub sampled: bool,
    pub date_range: DateRange,
    /// Raw OHLCV price bars — use directly for chart generation
    pub prices: Vec<PriceBar>,
    pub suggested_next_steps: Vec<String>,
}

/// AI-enriched response for `suggest_parameters`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SuggestResponse {
    pub summary: String,
    pub strategy: String,
    pub leg_deltas: Vec<TargetRange>,
    pub entry_dte: DteRange,
    pub exit_dte: i32,
    pub slippage: Slippage,
    pub rationale: String,
    pub confidence: f64,
    pub data_coverage: DataCoverage,
    pub suggested_next_steps: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DataCoverage {
    pub total_rows: usize,
    pub liquid_rows: usize,
    pub dte_range: String,
    pub expiration_count: usize,
    pub warnings: Vec<String>,
}

/// Entry representing a saved signal in the `list` action response.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SavedSignalEntry {
    pub name: String,
    pub formula: Option<String>,
    pub description: Option<String>,
    /// JSON snippet showing how to reference this signal as a `Saved` spec.
    pub usage: SavedSignalUsage,
}

/// Usage hint embedded in each `SavedSignalEntry`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SavedSignalUsage {
    #[serde(rename = "type")]
    pub kind: String,
    pub name: String,
}

/// Formula syntax reference returned when a validation error occurs.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FormulaHelp {
    pub columns: Vec<String>,
    pub lookback: String,
    pub functions: HashMap<String, String>,
    pub operators: Vec<String>,
    pub comparisons: Vec<String>,
    pub logical: Vec<String>,
    pub examples: Vec<String>,
}

/// Response for `build_signal`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BuildSignalResponse {
    pub summary: String,
    /// Whether the operation succeeded
    pub success: bool,
    /// The resolved signal spec (for create/get actions)
    pub signal_spec: Option<SignalSpec>,
    /// List of saved signals (for list action); empty when not applicable
    pub saved_signals: Vec<SavedSignalEntry>,
    /// Formula syntax help (shown on validation errors)
    pub formula_help: Option<FormulaHelp>,
    pub suggested_next_steps: Vec<String>,
}

/// Signal candidate from `construct_signal`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SignalCandidate {
    pub name: String,
    pub category: String,
    pub description: String,
    pub params: String,
    /// Concrete JSON example for this signal with sensible default parameters
    pub example: serde_json::Value,
}

/// Response for `construct_signal`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConstructSignalResponse {
    pub summary: String,
    pub candidates: Vec<SignalCandidate>,
    /// JSON Schema for `SignalSpec` enum, describing all valid signal types and their parameters
    pub schema: serde_json::Value,
    /// Default column names for OHLCV data from Yahoo Finance (e.g., {"close": "adjclose", "high": "high"})
    pub column_defaults: serde_json::Value,
    /// Example JSON structures showing how to combine signals using And/Or operators
    pub combinator_examples: Vec<serde_json::Value>,
    pub suggested_next_steps: Vec<String>,
}

/// OOS validation summary
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct OosValidation {
    pub top_n_validated: usize,
    pub results: Vec<OosResult>,
}

/// AI-enriched response for `parameter_sweep`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SweepResponse {
    pub summary: String,
    pub combinations_total: usize,
    pub combinations_run: usize,
    pub combinations_skipped: usize,
    pub best_combination: Option<SweepResult>,
    pub dimension_sensitivity: HashMap<String, HashMap<String, DimensionStats>>,
    pub out_of_sample: Option<OosValidation>,
    pub ranked_results: Vec<SweepResult>,
    pub suggested_next_steps: Vec<String>,
}
