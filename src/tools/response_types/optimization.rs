//! Response types for optimization tools: compare, sweep, `walk_forward`, permutation.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::engine::multiple_comparisons::MultipleComparisonsResult;
use crate::engine::permutation::MetricPermutationResult;
use crate::engine::sweep::{DimensionStats, OosResult, StabilityScore};
use crate::engine::types::{
    Commission, CompareResult, DteRange, PerformanceMetrics, Slippage, SweepResult, TargetRange,
};

use super::backtest::BacktestParamsSummary;

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
    /// Number of signal combinations swept (entry x exit), if signal sweep was used
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
