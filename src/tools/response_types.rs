//! Response types returned by MCP tool handlers.
//!
//! Every struct here derives `Serialize`, `Deserialize`, and `JsonSchema` so it can be
//! serialized to JSON for the MCP wire format and introspected by schema-aware clients.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::engine::multiple_comparisons::MultipleComparisonsResult;
use crate::engine::permutation::MetricPermutationResult;
use crate::engine::sweep::{DimensionStats, OosResult, StabilityScore};
use crate::engine::types::{
    Commission, CompareResult, DteRange, ExpirationFilter, PerformanceMetrics, Side, SizingConfig,
    Slippage, SweepResult, TargetRange, TradeRecord, TradeSelector,
};
use crate::signals::registry::SignalSpec;

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

/// A date + close price pair for overlaying the underlying's price on equity curve charts.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct UnderlyingPrice {
    pub date: String,
    pub close: f64,
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
    pub entry_signal: Option<serde_json::Value>,
    pub exit_signal: Option<serde_json::Value>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    /// Position sizing configuration (only present when sizing is active)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sizing: Option<SizingConfig>,
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
    /// Diagnostic warnings (e.g. entries skipped due to insufficient capital).
    /// When non-empty, the LLM should address these before interpreting results.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    pub suggested_next_steps: Vec<String>,
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

/// Parameters for a single strategy comparison entry
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CompareStrategyEntry {
    pub name: String,
    pub display_name: String,
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

/// Start and end date strings for a data range.
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

/// Metadata for a single strategy, including leg count and default deltas.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StrategyInfo {
    pub name: String,
    pub display_name: String,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub saved_signals: Vec<SavedSignalEntry>,
    /// Formula syntax help (shown on validation errors)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub formula_help: Option<FormulaHelp>,
    /// Signal candidates from catalog search (action="search" only)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub candidates: Vec<SignalCandidate>,
    /// JSON Schema for `SignalSpec` enum (action="search" only)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,
    /// Default OHLCV column names (action="search" only)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column_defaults: Option<serde_json::Value>,
    /// Example And/Or combinator structures (action="search" only)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub combinator_examples: Vec<serde_json::Value>,
    /// Full signal catalog grouped by category (action="catalog" only)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<crate::tools::signals::SignalsResponse>,
    pub suggested_next_steps: Vec<String>,
}

/// Signal candidate from `build_signal` action="search"
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SignalCandidate {
    pub name: String,
    pub category: String,
    pub description: String,
    pub params: String,
    /// Concrete JSON example for this signal with sensible default parameters
    pub example: serde_json::Value,
}

/// Internal response from signal catalog search (used by `build_signal` action="search")
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConstructSignalResponse {
    pub summary: String,
    /// Whether the search found real matches (false = fallback showing all signals)
    pub had_real_matches: bool,
    pub candidates: Vec<SignalCandidate>,
    /// JSON Schema for `SignalSpec` enum, describing all valid signal types and their parameters
    pub schema: serde_json::Value,
    /// Default column names for OHLCV data from Yahoo Finance (e.g., {"close": "adjclose", "high": "high"})
    pub column_defaults: serde_json::Value,
    /// Example JSON structures showing how to combine signals using And/Or operators
    pub combinator_examples: Vec<serde_json::Value>,
    pub suggested_next_steps: Vec<String>,
}

/// AI-enriched response for `permutation_test`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PermutationTestResponse {
    pub summary: String,
    pub assessment: String,
    pub key_findings: Vec<String>,
    pub parameters: BacktestParamsSummary,
    pub num_permutations: usize,
    pub num_completed: usize,
    pub real_metrics: PerformanceMetrics,
    pub real_trade_count: usize,
    pub real_total_pnl: f64,
    pub metric_tests: Vec<MetricPermutationResult>,
    /// Whether all primary metrics (Sharpe, `PnL`) have p-value < 0.05
    pub is_significant: bool,
    pub suggested_next_steps: Vec<String>,
}

/// OOS validation summary
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct OosValidation {
    pub top_n_validated: usize,
    pub results: Vec<OosResult>,
}

/// Per-window result from walk-forward analysis
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WalkForwardWindowResult {
    pub window_number: usize,
    pub train_start: String,
    pub train_end: String,
    pub test_start: String,
    pub test_end: String,
    pub train_sharpe: f64,
    pub test_sharpe: f64,
    pub train_pnl: f64,
    pub test_pnl: f64,
    pub train_trades: usize,
    pub test_trades: usize,
    pub train_win_rate: f64,
    pub test_win_rate: f64,
}

/// Aggregate statistics across all walk-forward windows.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WalkForwardAggregate {
    /// Windows that completed successfully; excludes `failed_windows`.
    pub successful_windows: usize,
    /// Windows excluded from aggregates (backtest errors, empty slices, etc.).
    pub failed_windows: usize,
    pub avg_test_sharpe: f64,
    pub std_test_sharpe: f64,
    pub avg_test_pnl: f64,
    pub pct_profitable_windows: f64,
    /// Average train-minus-test Sharpe delta; larger values suggest overfitting.
    pub avg_train_test_sharpe_decay: f64,
    pub total_test_pnl: f64,
}

/// AI-enriched response for `walk_forward`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WalkForwardResponse {
    pub summary: String,
    pub windows: Vec<WalkForwardWindowResult>,
    pub aggregate: WalkForwardAggregate,
    pub key_findings: Vec<String>,
    pub suggested_next_steps: Vec<String>,
}

/// Multiple comparisons corrections applied to sweep Sharpe p-values
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MultipleComparisonsCorrection {
    /// Bonferroni correction (conservative; controls family-wise error rate)
    pub bonferroni: MultipleComparisonsResult,
    /// Benjamini-Hochberg FDR correction (less conservative; controls false discovery rate)
    pub benjamini_hochberg: MultipleComparisonsResult,
}

/// AI-enriched response for `parameter_sweep`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SweepResponse {
    pub summary: String,
    pub combinations_total: usize,
    pub combinations_run: usize,
    /// Pre-filter skips (delta ordering, deduplication)
    pub combinations_skipped: usize,
    /// Backtests that errored at runtime (after being selected to run)
    pub combinations_failed: usize,
    /// Number of signal combinations swept (entry × exit), if signal sweep was used
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal_combinations: Option<usize>,
    pub best_combination: Option<SweepResult>,
    pub dimension_sensitivity: HashMap<String, HashMap<String, DimensionStats>>,
    pub out_of_sample: Option<OosValidation>,
    /// Parameter stability scores for the top-ranked results
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stability: Option<Vec<StabilityScore>>,
    /// Multiple comparisons correction (Bonferroni + BH-FDR) applied to per-combo Sharpe
    /// p-values. Populated only when `num_permutations` is set in sweep params and there
    /// are at least two sweep results; otherwise this will be `None` even if permutations
    /// were run and per-result `p_value` was computed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub multiple_comparisons: Option<MultipleComparisonsCorrection>,
    pub ranked_results: Vec<SweepResult>,
    pub suggested_next_steps: Vec<String>,
}
