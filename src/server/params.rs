//! MCP tool parameter structs with validation.
//!
//! Each struct corresponds to a tool's input schema, deriving `JsonSchema` for
//! MCP schema generation and `garde::Validate` for runtime validation. Common
//! base parameters are shared via `BacktestBaseParams` to eliminate field
//! duplication across `run_options_backtest`, `walk_forward`, and `permutation_test`.

use garde::Validate;
use schemars::JsonSchema;
use serde::Deserialize;

use std::collections::HashMap;

use crate::engine::types::Interval;

/// Format a garde validation error with the originating tool name for easier debugging.
pub(crate) fn validation_err(tool: &str, e: impl std::fmt::Display) -> String {
    format!("[{tool}] Validation error: {e}")
}

/// Validate that `end_date >= start_date` when both are present.
#[allow(clippy::ref_option)]
fn validate_end_date_after_start(
    start_date: &Option<String>,
) -> impl FnOnce(&Option<String>, &()) -> garde::Result + '_ {
    move |end_date: &Option<String>, (): &()| {
        if let (Some(start), Some(end)) = (start_date, end_date) {
            if end < start {
                return Err(garde::Error::new(format!(
                    "end_date ({end}) must be >= start_date ({start})"
                )));
            }
        }
        Ok(())
    }
}

/// Format a tool execution error for MCP responses.
pub(crate) fn tool_err(e: impl std::fmt::Display) -> String {
    format!("Error: {e}")
}

// ── Analysis tool parameter structs ──────────────────────────────────────────

/// Default years of history to fetch.
fn default_years() -> u32 {
    crate::constants::DEFAULT_ANALYSIS_YEARS
}

/// Default number of histogram bins.
fn default_n_bins() -> usize {
    30
}

/// Default rolling window size.
fn default_window() -> usize {
    21
}

/// Default number of regimes.
fn default_n_regimes() -> usize {
    3
}

/// Default lookback window for regime detection.
fn default_lookback_window() -> usize {
    21
}

// ── Typed enums for tool parameters ─────────────────────────────────────────

/// Grouping dimension for `aggregate_prices`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
pub enum GroupBy {
    #[serde(rename = "day_of_week")]
    DayOfWeek,
    #[serde(rename = "month")]
    Month,
    #[serde(rename = "quarter")]
    Quarter,
    #[serde(rename = "year")]
    Year,
    #[serde(rename = "hour_of_day")]
    HourOfDay,
}

impl GroupBy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DayOfWeek => "day_of_week",
            Self::Month => "month",
            Self::Quarter => "quarter",
            Self::Year => "year",
            Self::HourOfDay => "hour_of_day",
        }
    }
}

/// Aggregation metric for `aggregate_prices`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, JsonSchema)]
pub enum AggMetric {
    /// Close-to-close percentage change
    #[default]
    #[serde(rename = "return")]
    Return,
    /// High-low range as percentage of low
    #[serde(rename = "range")]
    Range,
    /// Raw volume
    #[serde(rename = "volume")]
    Volume,
    /// Open vs previous close percentage gap
    #[serde(rename = "gap")]
    Gap,
}

impl AggMetric {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Return => "return",
            Self::Range => "range",
            Self::Volume => "volume",
            Self::Gap => "gap",
        }
    }
}

/// Correlation mode for `correlate`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, JsonSchema)]
pub enum CorrelateMode {
    /// Full-period correlation (default)
    #[default]
    #[serde(rename = "full")]
    Full,
    /// Rolling window correlation
    #[serde(rename = "rolling")]
    Rolling,
}

impl CorrelateMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Rolling => "rolling",
        }
    }
}

/// Rolling metric type for `rolling_metric`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
pub enum RollingMetric {
    #[serde(rename = "volatility")]
    Volatility,
    #[serde(rename = "sharpe")]
    Sharpe,
    #[serde(rename = "mean_return")]
    MeanReturn,
    #[serde(rename = "max_drawdown")]
    MaxDrawdown,
    #[serde(rename = "beta")]
    Beta,
    #[serde(rename = "correlation")]
    Correlation,
}

impl RollingMetric {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Volatility => "volatility",
            Self::Sharpe => "sharpe",
            Self::MeanReturn => "mean_return",
            Self::MaxDrawdown => "max_drawdown",
            Self::Beta => "beta",
            Self::Correlation => "correlation",
        }
    }

    pub fn requires_benchmark(self) -> bool {
        matches!(self, Self::Beta | Self::Correlation)
    }
}

/// Regime detection method for `regime_detect`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, JsonSchema)]
pub enum RegimeMethod {
    /// Quantile-based volatility clustering (default)
    #[default]
    #[serde(rename = "volatility_cluster")]
    VolatilityCluster,
    /// SMA crossover trend state
    #[serde(rename = "trend_state")]
    TrendState,
    /// Gaussian Hidden Markov Model
    #[serde(rename = "hmm")]
    Hmm,
}

impl RegimeMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::VolatilityCluster => "volatility_cluster",
            Self::TrendState => "trend_state",
            Self::Hmm => "hmm",
        }
    }
}

/// Parameters for the `aggregate_prices` tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct AggregatePricesParams {
    /// Ticker symbol (e.g. "SPY")
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Years of history (default: 5)
    #[serde(default = "default_years")]
    #[garde(range(min = 1, max = 50))]
    pub years: u32,
    /// Grouping dimension: `"day_of_week"`, `"month"`, `"quarter"`, `"year"`, `"hour_of_day"` (for intraday data)
    #[garde(skip)]
    pub group_by: GroupBy,
    /// Metric to aggregate: "return" (default: close-to-close pct change), "range", "volume", "gap" (open vs prev close pct)
    #[serde(default)]
    #[garde(skip)]
    pub metric: AggMetric,
    /// Bar interval. Defaults to "daily" (auto-selects "1h" when `group_by="hour_of_day"`).
    /// Intraday data must be available in the cache — daily-only data cannot be resampled to intraday.
    #[serde(default)]
    #[garde(skip)]
    pub interval: Option<crate::engine::types::Interval>,
    /// Start date filter (YYYY-MM-DD)
    #[serde(default)]
    #[garde(inner(pattern(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")))]
    pub start_date: Option<String>,
    /// End date filter (YYYY-MM-DD)
    #[serde(default)]
    #[garde(inner(pattern(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")), custom(validate_end_date_after_start(&self.start_date)))]
    pub end_date: Option<String>,
}

pub use crate::tools::response_types::DistributionSource;

/// Parameters for the `distribution` tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct DistributionParams {
    /// Data source for the distribution
    #[garde(dive)]
    pub source: DistributionSource,
    /// Number of histogram bins (default: 30)
    #[serde(default = "default_n_bins")]
    #[garde(range(min = 5, max = 200))]
    pub n_bins: usize,
}

pub use crate::tools::response_types::CorrelationSeries;

/// Lag range for cross-correlation analysis.
#[derive(Debug, Clone, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct LagRange {
    /// Minimum lag (negative = x leads y). Range: -60..0
    #[garde(range(min = -60, max = 0))]
    pub min: i32,
    /// Maximum lag (positive = y leads x). Range: 0..60
    #[garde(range(min = 0, max = 60))]
    pub max: i32,
}

/// Parameters for the `correlate` tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct CorrelateParams {
    /// First data series
    #[garde(dive)]
    pub series_a: CorrelationSeries,
    /// Second data series
    #[garde(dive)]
    pub series_b: CorrelationSeries,
    /// Correlation mode: "full" (default), "rolling"
    #[serde(default)]
    #[garde(skip)]
    pub mode: CorrelateMode,
    /// Rolling window size in bars/observations at the selected interval (for mode="rolling")
    #[serde(default = "default_window")]
    #[garde(range(min = 5, max = 504))]
    pub window: usize,
    /// Years of history (default: 5)
    #[serde(default = "default_years")]
    #[garde(range(min = 1, max = 50))]
    pub years: u32,
    /// Optional lag range for cross-correlation and Granger causality analysis.
    /// When provided, computes a correlogram across the lag range and tests for lead/lag relationships.
    #[serde(default)]
    #[garde(dive)]
    pub lag_range: Option<LagRange>,
    /// Bar interval for both series: "daily" (default), "weekly", "monthly", or intraday
    /// ("1m", "5m", "10m", "15m", "30m", "1h", "4h"). Both series are resampled to this interval before
    /// computing correlation and lag analysis.
    #[serde(default)]
    #[garde(skip)]
    pub interval: Option<Interval>,
}

/// Parameters for the `rolling_metric` tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct RollingMetricParams {
    /// Ticker symbol
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Metric to compute: `"volatility"`, `"sharpe"`, `"mean_return"`, `"max_drawdown"`, `"beta"`, `"correlation"`
    #[garde(skip)]
    pub metric: RollingMetric,
    /// Rolling window size in trading days (default: 21)
    #[serde(default = "default_window")]
    #[garde(range(min = 5, max = 504))]
    pub window: usize,
    /// Benchmark symbol (required for "beta" and "correlation" metrics)
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$")))]
    pub benchmark: Option<String>,
    /// Years of history (default: 5)
    #[serde(default = "default_years")]
    #[garde(range(min = 1, max = 50))]
    pub years: u32,
}

/// Parameters for the `regime_detect` tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct RegimeDetectParams {
    /// Ticker symbol
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Detection method: `"volatility_cluster"` (default), `"trend_state"`, or `"hmm"` (Gaussian HMM)
    #[serde(default)]
    #[garde(skip)]
    pub method: RegimeMethod,
    /// Number of regimes to detect (default: 3, range: 2-4)
    #[serde(default = "default_n_regimes")]
    #[garde(range(min = 2, max = 4))]
    pub n_regimes: usize,
    /// Years of history (default: 5)
    #[serde(default = "default_years")]
    #[garde(range(min = 1, max = 50))]
    pub years: u32,
    /// Lookback window for rolling volatility/trend calculation (default: 21 bars at the selected interval)
    #[serde(default = "default_lookback_window")]
    #[garde(range(min = 5, max = 252))]
    pub lookback_window: usize,
    /// Bar interval: "daily" (default), "weekly", "monthly", or intraday ("1m", "5m", "10m", "15m", "30m", "1h", "4h").
    /// OHLCV data is resampled to this interval before applying the detection method.
    #[serde(default)]
    #[garde(skip)]
    pub interval: Option<Interval>,
}

// ── Default helpers for new quant tools ─────────────────────────────────

fn default_analysis_years() -> u32 {
    crate::constants::DEFAULT_ANALYSIS_YEARS
}

fn default_n_simulations() -> usize {
    10_000
}

fn default_horizon_days() -> usize {
    252
}

fn default_monte_carlo_capital() -> f64 {
    10_000.0
}

/// Parameters for the `drawdown_analysis` tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct DrawdownAnalysisParams {
    /// Ticker symbol
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Years of history to analyze (default: 5)
    #[serde(default = "default_analysis_years")]
    #[garde(range(min = 1, max = 50))]
    pub years: u32,
}

/// Parameters for the `cointegration_test` tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct CointegrationParams {
    /// First symbol (independent variable in hedge ratio regression; model is `symbol_b = α + β × symbol_a`)
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol_a: String,
    /// Second symbol (dependent variable in hedge ratio regression)
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol_b: String,
    /// Years of history (default: 5)
    #[serde(default = "default_analysis_years")]
    #[garde(range(min = 1, max = 50))]
    pub years: u32,
}

/// Parameters for the `monte_carlo` tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct MonteCarloParams {
    /// Ticker symbol to base simulations on
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Number of simulations (default: 10000)
    #[serde(default = "default_n_simulations")]
    #[garde(range(min = 100, max = 100_000))]
    pub n_simulations: usize,
    /// Forecast horizon in trading days (default: 252)
    #[serde(default = "default_horizon_days")]
    #[garde(range(min = 5, max = 2520))]
    pub horizon_days: usize,
    /// Starting capital (default: 10000)
    #[serde(default = "default_monte_carlo_capital")]
    #[garde(range(min = 1.0))]
    pub initial_capital: f64,
    /// Years of historical data to fit from (default: 5)
    #[serde(default = "default_analysis_years")]
    #[garde(range(min = 1, max = 50))]
    pub years: u32,
    /// Optional random seed for reproducibility
    #[serde(default)]
    #[garde(skip)]
    pub seed: Option<u64>,
}

/// Parameters for the `factor_attribution` tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct FactorAttributionParams {
    /// Symbol to analyze
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Market benchmark symbol (default: "SPY")
    #[serde(default = "default_benchmark")]
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub benchmark: String,
    /// Additional factor proxy symbols. Default factors use:
    /// Market=benchmark, SMB=IWM-SPY, HML=IWD-IWF, Momentum=MTUM
    #[serde(default)]
    #[garde(dive)]
    pub factor_proxies: Option<FactorProxies>,
    /// Years of history (default: 5)
    #[serde(default = "default_analysis_years")]
    #[garde(range(min = 1, max = 50))]
    pub years: u32,
}

fn default_benchmark() -> String {
    "SPY".to_string()
}

/// Custom factor proxy symbols.
#[derive(Debug, Clone, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct FactorProxies {
    /// Small-cap proxy (default: "IWM")
    #[garde(inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$")))]
    pub small_cap: Option<String>,
    /// Large-cap growth proxy (default: "IWF")
    #[garde(inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$")))]
    pub growth: Option<String>,
    /// Large-cap value proxy (default: "IWD")
    #[garde(inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$")))]
    pub value: Option<String>,
    /// Momentum factor proxy (default: "MTUM")
    #[garde(inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$")))]
    pub momentum: Option<String>,
}

/// Parameters for the `portfolio_optimize` tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct PortfolioOptimizeParams {
    /// Symbols to include in portfolio (2-20)
    #[garde(
        length(min = 2, max = 20),
        inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))
    )]
    pub symbols: Vec<String>,
    /// Optimization methods to run (default: all three)
    #[serde(default)]
    #[garde(skip)]
    pub methods: Option<Vec<String>>,
    /// Years of history (default: 5)
    #[serde(default = "default_analysis_years")]
    #[garde(range(min = 1, max = 50))]
    pub years: u32,
    /// Risk-free rate for Sharpe calculation (default: 0.05 = 5%)
    #[serde(default = "default_risk_free")]
    #[garde(range(min = 0.0, max = 0.2))]
    pub risk_free_rate: f64,
}

fn default_risk_free() -> f64 {
    0.05
}

/// Parameters for the `benchmark_analysis` tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct BenchmarkAnalysisParams {
    /// Symbol to analyze
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Benchmark symbol (default: "SPY")
    #[serde(default = "default_benchmark")]
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub benchmark: String,
    /// Years of history (default: 5)
    #[serde(default = "default_analysis_years")]
    #[garde(range(min = 1, max = 50))]
    pub years: u32,
}

// ── Walk-forward defaults ────────────────────────────────────────────────

fn default_wf_capital() -> f64 {
    100_000.0
}

fn default_wf_n_windows() -> usize {
    5
}

fn default_wf_train_pct() -> f64 {
    0.70
}

/// Parameters for the `walk_forward` MCP tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct WalkForwardToolParams {
    /// Strategy script name (filename without `.rhai` extension from `scripts/strategies/`).
    #[garde(length(min = 1))]
    pub strategy: String,

    /// Ticker symbol for data loading.
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,

    /// Starting capital for each window's backtest.
    #[serde(default = "default_wf_capital")]
    #[garde(range(min = 1.0))]
    pub capital: f64,

    /// Parameter grid: keys are param names, values are lists of values to sweep.
    /// Example: `{ "DELTA_TARGET": [0.20, 0.30, 0.40], "DTE_TARGET": [30, 45, 60] }`
    #[garde(skip)]
    pub params_grid: HashMap<String, Vec<serde_json::Value>>,

    /// Objective metric to optimize: `sharpe` (default), `sortino`, `profit_factor`, `cagr`.
    #[serde(default)]
    #[garde(skip)]
    pub objective: Option<String>,

    /// Number of walk-forward windows (default: 5).
    #[serde(default = "default_wf_n_windows")]
    #[garde(range(min = 1, max = 50))]
    pub n_windows: usize,

    /// Walk-forward mode: `rolling` (default) or `anchored`.
    #[serde(default)]
    #[garde(skip)]
    pub mode: Option<String>,

    /// Fraction of each window used for training (default: 0.70).
    #[serde(default = "default_wf_train_pct")]
    #[garde(range(min = 0.1, max = 0.95))]
    pub train_pct: f64,

    /// Start date filter (YYYY-MM-DD). Optional.
    #[serde(default)]
    #[garde(skip)]
    pub start_date: Option<String>,

    /// End date filter (YYYY-MM-DD). Optional.
    #[serde(default)]
    #[garde(skip)]
    pub end_date: Option<String>,

    /// Profile name for parameter presets. Optional.
    #[serde(default)]
    #[garde(skip)]
    pub profile: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── CorrelateParams interval deserialization ────────────────────────────

    #[test]
    fn correlate_params_interval_defaults_to_none() {
        let json = serde_json::json!({
            "series_a": { "symbol": "SPY", "field": "close" },
            "series_b": { "symbol": "QQQ", "field": "close" }
        });
        let p: CorrelateParams = serde_json::from_value(json).unwrap();
        assert!(p.interval.is_none(), "interval should default to None");
        p.validate().unwrap();
    }

    #[test]
    fn correlate_params_all_intervals_parse() {
        let cases = [
            ("1m", Interval::Min1),
            ("5m", Interval::Min5),
            ("30m", Interval::Min30),
            ("1h", Interval::Hour1),
            ("daily", Interval::Daily),
            ("weekly", Interval::Weekly),
            ("monthly", Interval::Monthly),
        ];
        for (s, expected) in cases {
            let json = serde_json::json!({
                "series_a": { "symbol": "SPY", "field": "close" },
                "series_b": { "symbol": "QQQ", "field": "close" },
                "interval": s
            });
            let p: CorrelateParams = serde_json::from_value(json)
                .unwrap_or_else(|e| panic!("interval={s} should parse: {e}"));
            assert_eq!(p.interval, Some(expected), "interval={s}");
        }
    }

    // ─── RegimeDetectParams interval deserialization ─────────────────────────

    #[test]
    fn regime_detect_params_interval_defaults_to_none() {
        let json = serde_json::json!({ "symbol": "SPY" });
        let p: RegimeDetectParams = serde_json::from_value(json).unwrap();
        assert!(p.interval.is_none(), "interval should default to None");
        p.validate().unwrap();
    }

    #[test]
    fn regime_detect_params_all_intervals_parse() {
        let cases = [
            ("1m", Interval::Min1),
            ("5m", Interval::Min5),
            ("30m", Interval::Min30),
            ("1h", Interval::Hour1),
            ("daily", Interval::Daily),
            ("weekly", Interval::Weekly),
            ("monthly", Interval::Monthly),
        ];
        for (s, expected) in cases {
            let json = serde_json::json!({
                "symbol": "SPY",
                "interval": s
            });
            let p: RegimeDetectParams = serde_json::from_value(json)
                .unwrap_or_else(|e| panic!("interval={s} should parse: {e}"));
            assert_eq!(p.interval, Some(expected), "interval={s}");
        }
    }

    // ─── WalkForwardToolParams validation ────────────────────────────────

    #[test]
    fn walk_forward_params_defaults_applied() {
        let json = serde_json::json!({
            "strategy": "short_put",
            "symbol": "SPY",
            "params_grid": { "DELTA_TARGET": [0.20, 0.30] }
        });
        let p: WalkForwardToolParams = serde_json::from_value(json).unwrap();
        assert_eq!(p.capital, 100_000.0);
        assert_eq!(p.n_windows, 5);
        assert!((p.train_pct - 0.70).abs() < f64::EPSILON);
        assert!(p.objective.is_none());
        assert!(p.mode.is_none());
        assert!(p.start_date.is_none());
        assert!(p.end_date.is_none());
        assert!(p.profile.is_none());
        p.validate().unwrap();
    }

    #[test]
    fn walk_forward_params_valid_full() {
        let json = serde_json::json!({
            "strategy": "short_put",
            "symbol": "SPY",
            "capital": 50000.0,
            "params_grid": { "DTE": [30, 45], "DELTA": [0.2, 0.3] },
            "objective": "sortino",
            "n_windows": 10,
            "mode": "anchored",
            "train_pct": 0.80,
            "start_date": "2020-01-01",
            "end_date": "2024-01-01"
        });
        let p: WalkForwardToolParams = serde_json::from_value(json).unwrap();
        p.validate().unwrap();
        assert_eq!(p.n_windows, 10);
        assert!((p.train_pct - 0.80).abs() < f64::EPSILON);
    }

    #[test]
    fn walk_forward_params_rejects_empty_strategy() {
        let json = serde_json::json!({
            "strategy": "",
            "symbol": "SPY",
            "params_grid": { "X": [1] }
        });
        let p: WalkForwardToolParams = serde_json::from_value(json).unwrap();
        assert!(p.validate().is_err());
    }

    #[test]
    fn walk_forward_params_rejects_invalid_symbol() {
        let json = serde_json::json!({
            "strategy": "short_put",
            "symbol": "../../etc",
            "params_grid": { "X": [1] }
        });
        let p: WalkForwardToolParams = serde_json::from_value(json).unwrap();
        assert!(p.validate().is_err());
    }

    #[test]
    fn walk_forward_params_rejects_train_pct_out_of_range() {
        let json = serde_json::json!({
            "strategy": "short_put",
            "symbol": "SPY",
            "params_grid": { "X": [1] },
            "train_pct": 0.99
        });
        let p: WalkForwardToolParams = serde_json::from_value(json).unwrap();
        assert!(p.validate().is_err());
    }

    #[test]
    fn walk_forward_params_rejects_zero_capital() {
        let json = serde_json::json!({
            "strategy": "short_put",
            "symbol": "SPY",
            "capital": 0.0,
            "params_grid": { "X": [1] }
        });
        let p: WalkForwardToolParams = serde_json::from_value(json).unwrap();
        assert!(p.validate().is_err());
    }

    #[test]
    fn walk_forward_params_rejects_zero_windows() {
        let json = serde_json::json!({
            "strategy": "short_put",
            "symbol": "SPY",
            "params_grid": { "X": [1] },
            "n_windows": 0
        });
        let p: WalkForwardToolParams = serde_json::from_value(json).unwrap();
        assert!(p.validate().is_err());
    }
}
