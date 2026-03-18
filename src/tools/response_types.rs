//! Response types returned by MCP tool handlers.
//!
//! Most structs here derive `Serialize`, `Deserialize`, and `JsonSchema` so they can be
//! serialized to JSON for the MCP wire format and introspected by schema-aware clients.
//! This module also contains a small number of input/parameter types (e.g.,
//! `DistributionSource`, `CorrelationSeries`) that are shared across the tool and server
//! layers and derive `Deserialize` and `JsonSchema` (but not `Serialize`).

use garde::Validate;
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
use crate::signals::helpers::IndicatorData;
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
    /// SL/TP conflict resolution strategy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conflict_resolution: Option<String>,
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
    /// `"stock"` when produced by stock-mode compare; absent for options mode.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// The strategies and parameters that were compared (for reference in follow-up questions)
    pub strategies_compared: Vec<CompareStrategyEntry>,
    pub ranking_by_sharpe: Vec<String>,
    pub ranking_by_pnl: Vec<String>,
    pub best_overall: Option<String>,
    pub results: Vec<CompareResult>,
    pub suggested_next_steps: Vec<String>,
}

/// Start and end date strings for a data range.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DateRange {
    pub start: Option<i64>,
    pub end: Option<i64>,
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

/// A single OHLCV price bar for `get_raw_prices`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PriceBar {
    pub date: i64,
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
    /// Default column names for OHLCV data (e.g., {"close": "adjclose", "high": "high"})
    pub column_defaults: serde_json::Value,
    /// Example JSON structures showing how to combine signals using And/Or operators
    pub combinator_examples: Vec<serde_json::Value>,
    pub suggested_next_steps: Vec<String>,
}

/// AI-enriched response for `permutation_test`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PermutationTestResponse {
    pub summary: String,
    /// `"stock"` when produced by stock-mode permutation test; absent for options mode.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
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
    /// `"stock"` when produced by stock-mode walk-forward; absent for options mode.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
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
    /// `"stock"` when produced by stock-mode sweep; absent for options mode.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
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

// ── Analysis tool shared types ──────────────────────────────────────────────

/// Default years of history for analysis tools.
fn default_analysis_years() -> u32 {
    5
}

/// Default label for trade P&L source.
fn default_pnl_label() -> String {
    "Trade P&L".to_string()
}

/// Default correlation field.
fn default_corr_field() -> String {
    "return".to_string()
}

/// Source for distribution analysis: either price returns or raw trade P&L values.
#[derive(Debug, Clone, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
#[serde(tag = "type")]
pub enum DistributionSource {
    /// Compute returns from OHLCV price data
    #[serde(rename = "price_returns")]
    PriceReturns {
        /// Ticker symbol
        #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
        symbol: String,
        /// Years of history (default: 5)
        #[serde(default = "default_analysis_years")]
        #[garde(range(min = 1, max = 50))]
        years: u32,
    },
    /// Use pre-computed values (e.g., trade P&L array from a backtest)
    #[serde(rename = "trade_pnl")]
    TradePnl {
        /// Array of P&L values
        #[garde(length(min = 1))]
        values: Vec<f64>,
        /// Label for this dataset
        #[serde(default = "default_pnl_label")]
        #[garde(length(min = 1))]
        label: String,
    },
}

/// Series specification for correlation analysis.
#[derive(Debug, Clone, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct CorrelationSeries {
    /// Ticker symbol
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Price field: "close", "open", "high", "low", "volume", "return" (default)
    #[serde(default = "default_corr_field")]
    #[garde(length(min = 1))]
    pub field: String,
}

// ── Analysis tool response types ──────────────────────────────────────────────

/// A single bucket of aggregated price statistics.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AggregateBucket {
    /// Bucket label (e.g. "Monday", "January", "Q1", "2023")
    pub label: String,
    pub count: usize,
    pub mean: f64,
    pub median: f64,
    pub std_dev: f64,
    pub min: f64,
    pub max: f64,
    pub total: f64,
    pub positive_pct: f64,
    /// One-sample t-test p-value vs zero (null: mean = 0)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p_value: Option<f64>,
}

/// Response for `aggregate_prices`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AggregatePricesResponse {
    pub summary: String,
    pub symbol: String,
    pub group_by: String,
    pub metric: String,
    pub total_bars: usize,
    pub date_range: DateRange,
    pub buckets: Vec<AggregateBucket>,
    pub key_findings: Vec<String>,
    pub warnings: Vec<String>,
    pub suggested_next_steps: Vec<String>,
}

/// Normality test result for distribution analysis.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct NormalityTest {
    pub test_name: String,
    pub statistic: f64,
    pub p_value: f64,
    pub is_normal: bool,
}

/// Tail ratio for distribution analysis.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TailRatio {
    /// Ratio of extreme left tail (< -2σ) to extreme right tail (> +2σ)
    pub left_tail_pct: f64,
    pub right_tail_pct: f64,
    pub ratio: f64,
    pub interpretation: String,
}

/// A histogram bin for serialization.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct HistogramBin {
    pub lower: f64,
    pub upper: f64,
    pub count: usize,
    pub frequency: f64,
}

/// Response for `distribution`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DistributionResponse {
    pub summary: String,
    pub source: String,
    pub n_observations: usize,
    pub mean: f64,
    pub std_dev: f64,
    pub median: f64,
    pub skewness: f64,
    pub kurtosis: f64,
    pub min: f64,
    pub max: f64,
    pub percentile_5: f64,
    pub percentile_25: f64,
    pub percentile_75: f64,
    pub percentile_95: f64,
    pub histogram: Vec<HistogramBin>,
    pub normality: Option<NormalityTest>,
    pub tail_ratio: Option<TailRatio>,
    pub key_findings: Vec<String>,
    pub suggested_next_steps: Vec<String>,
}

/// A point in a rolling correlation series.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RollingCorrelationPoint {
    pub date: String,
    pub correlation: f64,
}

/// A scatter point for correlation visualization.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ScatterPoint {
    pub x: f64,
    pub y: f64,
    pub date: String,
}

/// A single point in a cross-correlogram (lag vs correlation).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LagCorrelationPoint {
    /// Lag in bars (positive = series B leads A).
    pub lag: i32,
    pub pearson: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p_value: Option<f64>,
}

/// Result of a Granger causality F-test.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GrangerResult {
    /// Direction of tested causality (e.g. "VIX → SPY").
    pub direction: String,
    pub f_statistic: f64,
    pub p_value: f64,
    pub lag_order: usize,
    /// Whether p < 0.05.
    pub is_significant: bool,
}

/// Lead/lag cross-correlation analysis results.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LagAnalysis {
    /// Cross-correlogram: Pearson at each lag.
    pub correlogram: Vec<LagCorrelationPoint>,
    /// Lag with highest absolute correlation.
    pub optimal_lag: i32,
    /// Pearson correlation at optimal lag.
    pub optimal_correlation: f64,
    /// Granger causality tests in both directions.
    pub granger_tests: Vec<GrangerResult>,
}

/// Response for `correlate`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CorrelateResponse {
    pub summary: String,
    pub series_a: String,
    pub series_b: String,
    pub n_observations: usize,
    pub pearson: f64,
    pub spearman: f64,
    pub r_squared: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p_value: Option<f64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rolling_correlation: Vec<RollingCorrelationPoint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scatter: Vec<ScatterPoint>,
    /// Lead/lag analysis (present when `lag_range` is provided).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lag_analysis: Option<LagAnalysis>,
    pub key_findings: Vec<String>,
    pub suggested_next_steps: Vec<String>,
}

/// A single point in a rolling metric series.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RollingPoint {
    pub date: String,
    pub value: f64,
}

/// Summary statistics for a rolling metric series.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RollingStats {
    pub current: f64,
    pub mean: f64,
    pub min: f64,
    pub max: f64,
    pub std_dev: f64,
    /// Trend direction: "rising", "falling", or "flat"
    pub trend: String,
}

/// Response for `rolling_metric`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RollingMetricResponse {
    pub summary: String,
    pub symbol: String,
    pub metric: String,
    pub window: usize,
    pub n_observations: usize,
    pub stats: RollingStats,
    pub series: Vec<RollingPoint>,
    pub key_findings: Vec<String>,
    pub suggested_next_steps: Vec<String>,
}

/// Information about a detected market regime.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RegimeInfo {
    pub label: String,
    pub count: usize,
    pub pct_of_total: f64,
    pub mean_return: f64,
    pub std_dev: f64,
    pub mean_vol: f64,
    /// HMM emission mean (only for method="hmm").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub emission_mean: Option<f64>,
    /// HMM emission std dev (only for method="hmm").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub emission_std: Option<f64>,
}

/// A date-labeled regime assignment point.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RegimeSeriesPoint {
    pub date: String,
    pub regime: String,
}

/// Response for `regime_detect`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RegimeDetectResponse {
    pub summary: String,
    pub symbol: String,
    pub method: String,
    pub n_regimes: usize,
    pub total_bars: usize,
    pub classified_bars: usize,
    pub regimes: Vec<RegimeInfo>,
    pub transition_matrix: Vec<Vec<f64>>,
    pub regime_series: Vec<RegimeSeriesPoint>,
    pub key_findings: Vec<String>,
    pub suggested_next_steps: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lag_analysis_serde_round_trip() {
        let la = LagAnalysis {
            correlogram: vec![
                LagCorrelationPoint {
                    lag: -2,
                    pearson: 0.85,
                    p_value: Some(0.001),
                },
                LagCorrelationPoint {
                    lag: 0,
                    pearson: 0.5,
                    p_value: Some(0.05),
                },
            ],
            optimal_lag: -2,
            optimal_correlation: 0.85,
            granger_tests: vec![GrangerResult {
                direction: "VIX → SPY".into(),
                f_statistic: 5.2,
                p_value: 0.003,
                lag_order: 2,
                is_significant: true,
            }],
        };
        let json = serde_json::to_string(&la).unwrap();
        let parsed: LagAnalysis = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.optimal_lag, -2);
        assert_eq!(parsed.granger_tests.len(), 1);
        assert!(parsed.granger_tests[0].is_significant);
    }

    #[test]
    fn regime_info_emission_params_skip_when_none() {
        let ri = RegimeInfo {
            label: "Low Vol".into(),
            count: 100,
            pct_of_total: 50.0,
            mean_return: 0.05,
            std_dev: 0.8,
            mean_vol: 12.0,
            emission_mean: None,
            emission_std: None,
        };
        let json = serde_json::to_string(&ri).unwrap();
        assert!(!json.contains("emission_mean"));
        assert!(!json.contains("emission_std"));
    }

    #[test]
    fn regime_info_emission_params_present_for_hmm() {
        let ri = RegimeInfo {
            label: "Bear / High Vol".into(),
            count: 80,
            pct_of_total: 40.0,
            mean_return: -0.02,
            std_dev: 1.5,
            mean_vol: 20.0,
            emission_mean: Some(-0.015),
            emission_std: Some(1.2),
        };
        let json = serde_json::to_string(&ri).unwrap();
        assert!(json.contains("emission_mean"));
        assert!(json.contains("emission_std"));
        let parsed: RegimeInfo = serde_json::from_str(&json).unwrap();
        assert!((parsed.emission_mean.unwrap() - (-0.015)).abs() < 1e-10);
    }

    #[test]
    fn correlate_response_lag_analysis_optional() {
        // Without lag_analysis — field should be omitted from JSON
        let resp = CorrelateResponse {
            summary: "test".into(),
            series_a: "SPY return".into(),
            series_b: "VIX return".into(),
            n_observations: 100,
            pearson: 0.5,
            spearman: 0.48,
            r_squared: 0.25,
            p_value: Some(0.001),
            rolling_correlation: vec![],
            scatter: vec![],
            lag_analysis: None,
            key_findings: vec![],
            suggested_next_steps: vec![],
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(!json.contains("lag_analysis"));

        // With lag_analysis — field should appear
        let resp_with_lag = CorrelateResponse {
            lag_analysis: Some(LagAnalysis {
                correlogram: vec![],
                optimal_lag: 3,
                optimal_correlation: 0.9,
                granger_tests: vec![],
            }),
            ..resp
        };
        let json2 = serde_json::to_string(&resp_with_lag).unwrap();
        assert!(json2.contains("lag_analysis"));
        assert!(json2.contains("optimal_lag"));
    }
}
