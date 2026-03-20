//! Response types for risk tools: drawdown, cointegration, `monte_carlo`,
//! `factor_attribution`, benchmark, `portfolio_optimize`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::stats::HistogramBin;

// ── Drawdown analysis types ──────────────────────────────────────────────

/// A single drawdown episode (peak-to-trough-to-recovery).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DrawdownEpisode {
    /// Start of drawdown (Unix timestamp)
    pub start_date: i64,
    /// Date of deepest point (Unix timestamp)
    pub trough_date: i64,
    /// Date of recovery to prior peak (None if still in drawdown)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery_date: Option<i64>,
    /// Maximum depth as percentage of peak
    pub depth_pct: f64,
    /// Total duration in bars (start to recovery or end of data)
    pub duration_bars: usize,
    /// Bars from trough to recovery (0 if unrecovered)
    pub recovery_bars: usize,
    /// Equity at the peak before this drawdown
    pub peak_equity: f64,
    /// Equity at the trough
    pub trough_equity: f64,
}

/// A point on the underwater (drawdown) curve.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct UnderwaterPoint {
    /// Unix timestamp
    pub date: i64,
    /// Drawdown percentage (negative values = underwater)
    pub drawdown_pct: f64,
}

/// Aggregate drawdown distribution statistics.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DrawdownStats {
    pub total_episodes: usize,
    pub avg_depth_pct: f64,
    pub max_depth_pct: f64,
    pub avg_duration_bars: f64,
    pub max_duration_bars: usize,
    pub avg_recovery_bars: f64,
    /// Percentage of episodes that fully recovered
    pub pct_recovered: f64,
    /// Root-mean-square of drawdown percentages (measures sustained pain)
    pub ulcer_index: f64,
}

/// AI-enriched response for `drawdown_analysis`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DrawdownAnalysisResponse {
    pub summary: String,
    pub symbol: String,
    pub total_bars: usize,
    pub stats: DrawdownStats,
    /// Top drawdown episodes ranked by depth (max 20)
    pub episodes: Vec<DrawdownEpisode>,
    /// Underwater curve for charting (max 500 points)
    pub underwater: Vec<UnderwaterPoint>,
    pub key_findings: Vec<String>,
    pub suggested_next_steps: Vec<String>,
}

// ── Cointegration types ──────────────────────────────────────────────

/// Result of an Augmented Dickey-Fuller (ADF) test.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AdfTestResult {
    /// ADF test statistic
    pub statistic: f64,
    /// Approximate p-value
    pub p_value: f64,
    /// Number of lags used
    pub lags: usize,
    /// Number of observations
    pub n_obs: usize,
    /// Whether the series is stationary at 5% significance
    pub is_stationary: bool,
    /// Critical values at 1%, 5%, 10%
    pub critical_values: CriticalValues,
}

/// Critical values for ADF test at standard significance levels.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CriticalValues {
    pub pct_1: f64,
    pub pct_5: f64,
    pub pct_10: f64,
}

/// Spread statistics for cointegrated pairs.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SpreadStats {
    pub mean: f64,
    pub std_dev: f64,
    pub current: f64,
    pub z_score: f64,
    /// Current spread position relative to history
    pub percentile: f64,
    pub half_life: Option<f64>,
}

/// A single point on the spread time series.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SpreadPoint {
    pub date: String,
    pub spread: f64,
    pub z_score: f64,
}

/// AI-enriched response for `cointegration_test`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CointegrationResponse {
    pub summary: String,
    pub series_a: String,
    pub series_b: String,
    pub n_observations: usize,
    /// Cointegrating regression: B = alpha + beta * A
    pub hedge_ratio: f64,
    pub intercept: f64,
    /// Regression R²
    pub r_squared: f64,
    /// ADF test on the residuals (spread)
    pub adf_test: AdfTestResult,
    /// Whether the pair is cointegrated at 5% significance
    pub is_cointegrated: bool,
    pub spread_stats: SpreadStats,
    /// Spread time series for charting (max 500 points)
    pub spread_series: Vec<SpreadPoint>,
    pub key_findings: Vec<String>,
    pub suggested_next_steps: Vec<String>,
}

// ── Monte Carlo types ──────────────────────────────────────────────

/// A single percentile path from Monte Carlo simulation.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MonteCarloPercentilePath {
    /// Percentile label (e.g., "5th", "25th", "50th", "75th", "95th")
    pub label: String,
    pub percentile: f64,
    /// Terminal equity value at this percentile
    pub terminal_value: f64,
    /// Total return at this percentile
    pub total_return_pct: f64,
}

/// Ruin probability analysis from Monte Carlo.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RuinAnalysis {
    /// Probability of losing X% of capital (at various thresholds)
    pub prob_loss_10pct: f64,
    pub prob_loss_25pct: f64,
    pub prob_loss_50pct: f64,
    /// Probability of negative total return
    pub prob_negative_return: f64,
}

/// Max drawdown distribution from Monte Carlo.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DrawdownDistribution {
    pub mean: f64,
    pub median: f64,
    pub percentile_5: f64,
    pub percentile_95: f64,
    pub worst: f64,
}

/// AI-enriched response for `monte_carlo`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MonteCarloResponse {
    pub summary: String,
    pub symbol: String,
    pub n_simulations: usize,
    pub horizon_days: usize,
    pub initial_capital: f64,
    pub percentile_paths: Vec<MonteCarloPercentilePath>,
    pub ruin_analysis: RuinAnalysis,
    pub drawdown_distribution: DrawdownDistribution,
    /// Terminal equity distribution histogram
    pub terminal_histogram: Vec<HistogramBin>,
    pub key_findings: Vec<String>,
    pub suggested_next_steps: Vec<String>,
}

// ── Factor attribution types ──────────────────────────────────────────────

/// A single factor's attribution result.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FactorExposure {
    /// Factor name (e.g., "Market", "SMB", "HML", "Momentum")
    pub factor: String,
    /// Beta (regression coefficient) to this factor
    pub beta: f64,
    /// T-statistic for significance
    pub t_stat: f64,
    /// P-value for significance
    pub p_value: f64,
    /// Whether significant at 5%
    pub is_significant: bool,
    /// Contribution to total return (beta x factor mean return)
    pub return_contribution_pct: f64,
}

/// AI-enriched response for `factor_attribution`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FactorAttributionResponse {
    pub summary: String,
    pub symbol: String,
    pub n_observations: usize,
    /// Annualized alpha (intercept, the unexplained return)
    pub alpha: f64,
    /// Alpha t-statistic
    pub alpha_t_stat: f64,
    /// Whether alpha is significant at 5%
    pub alpha_significant: bool,
    /// R² of the multi-factor regression
    pub r_squared: f64,
    /// Adjusted R²
    pub adj_r_squared: f64,
    /// Per-factor exposures
    pub factors: Vec<FactorExposure>,
    /// Percentage of return explained by factors vs alpha
    pub pct_explained_by_factors: f64,
    pub key_findings: Vec<String>,
    pub suggested_next_steps: Vec<String>,
}

// ── Portfolio optimization types ──────────────────────────────────────────────

/// A single asset's allocation in the optimized portfolio.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct OptimalWeight {
    pub symbol: String,
    /// Optimal weight (0.0 to 1.0)
    pub weight: f64,
    pub weight_pct: f64,
}

/// Portfolio optimization result for one method.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct OptimizationResult {
    /// Method name (e.g., "`risk_parity`", "`min_variance`", "`max_sharpe`")
    pub method: String,
    pub weights: Vec<OptimalWeight>,
    /// Expected annualized return
    pub expected_return: f64,
    /// Expected annualized volatility
    pub expected_volatility: f64,
    /// Expected Sharpe ratio
    pub expected_sharpe: f64,
}

/// AI-enriched response for `portfolio_optimize`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PortfolioOptimizeResponse {
    pub summary: String,
    pub symbols: Vec<String>,
    pub n_observations: usize,
    /// Correlation matrix (`NxN` as flat array with labels)
    pub correlation_matrix: Vec<CorrelationEntry>,
    /// Individual asset statistics
    pub asset_stats: Vec<AssetStats>,
    /// Optimization results for each method
    pub optimizations: Vec<OptimizationResult>,
    pub key_findings: Vec<String>,
    pub suggested_next_steps: Vec<String>,
}

/// Per-asset return/risk statistics for portfolio optimization.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AssetStats {
    pub symbol: String,
    pub annualized_return: f64,
    pub annualized_volatility: f64,
    pub sharpe: f64,
}

// ── Benchmark-relative metrics types ──────────────────────────────────────

/// AI-enriched response for `benchmark_analysis`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BenchmarkAnalysisResponse {
    pub summary: String,
    pub symbol: String,
    pub benchmark: String,
    pub n_observations: usize,
    /// Jensen's alpha (annualized)
    pub alpha: f64,
    pub alpha_t_stat: f64,
    pub alpha_significant: bool,
    /// Portfolio beta to benchmark
    pub beta: f64,
    /// Treynor-like ratio: annualized mean return / beta (no risk-free subtraction)
    pub treynor: f64,
    /// Information ratio: annualized mean excess return / tracking error
    pub information_ratio: f64,
    /// Tracking error (annualized std of excess returns)
    pub tracking_error: f64,
    /// R² of returns vs benchmark
    pub r_squared: f64,
    /// Up capture ratio: performance during up markets
    pub up_capture: f64,
    /// Down capture ratio: performance during down markets
    pub down_capture: f64,
    pub key_findings: Vec<String>,
    pub suggested_next_steps: Vec<String>,
}

// Re-export CorrelationEntry here since it's used by both portfolio_optimize and portfolio_backtest
use super::portfolio::CorrelationEntry;
