//! Response types for hypothesis generation.

use garde::Validate;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::signals::registry::SignalSpec;

use super::inputs::{default_analysis_years, default_dedup_threshold, default_significance};

/// Default forward return horizons (trading days).
fn default_forward_horizons() -> Vec<usize> {
    vec![5, 10, 20]
}

/// Parameters for `generate_hypotheses`.
#[derive(Debug, Clone, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct HypothesisParams {
    /// Symbols to scan (1-10). First symbol is the primary target; additional symbols
    /// are used as cross-asset inputs for lead/lag analysis.
    #[garde(
        length(min = 1, max = 10),
        inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))
    )]
    pub symbols: Vec<String>,
    /// Dimensions to scan (None = all applicable given available data)
    #[serde(default)]
    #[garde(skip)]
    pub dimensions: Option<Vec<crate::engine::types::HypothesisDimension>>,
    /// Significance threshold for BH-FDR correction (default: 0.05)
    #[serde(default = "default_significance")]
    #[garde(range(min = 0.001, max = 0.2))]
    pub significance: f64,
    /// Forward return horizons in trading days (default: [5, 10, 20])
    #[serde(default = "default_forward_horizons")]
    #[garde(length(min = 1, max = 5), inner(range(min = 1, max = 252)))]
    pub forward_horizons: Vec<usize>,
    /// Years of history to scan (default: 5)
    #[serde(default = "default_analysis_years")]
    #[garde(range(min = 1, max = 50))]
    pub years: u32,
    /// Jaccard similarity threshold for deduplication (default: 0.5)
    #[serde(default = "default_dedup_threshold")]
    #[garde(range(min = 0.1, max = 1.0))]
    pub dedup_threshold: f64,
}

/// A single discovered pattern from hypothesis scanning.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DiscoveredPattern {
    /// Scanning dimension that produced this pattern
    pub dimension: String,
    /// Human-readable description of the pattern
    pub description: String,
    /// Structural basis (known market mechanism or `"empirical_only"`)
    pub structural_basis: String,
    /// Human-readable explanation of why this pattern might exist
    pub structural_explanation: String,
    /// Deployable signal spec for backtesting
    pub signal_spec: SignalSpec,
    /// Forward return horizon (trading days) this pattern targets
    pub forward_horizon: usize,
    /// Raw p-value from t-test
    pub p_value: f64,
    /// BH-FDR adjusted p-value
    pub adjusted_p_value: f64,
    /// Mean forward return (effect size)
    pub effect_size: f64,
    /// Number of signal occurrences
    pub occurrence_count: usize,
    /// Annualized Sharpe ratio of pattern returns
    pub sharpe: f64,
    /// Deflated Sharpe Ratio (adjusted for multiple testing and non-normality)
    pub dsr: f64,
    /// Regime stability score (lower = more stable across regimes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub regime_stability: Option<f64>,
    /// Deduplication cluster ID
    pub cluster_id: usize,
    /// Up to 10 example signal dates
    pub sample_dates: Vec<String>,
}

/// AI-enriched response for `generate_hypotheses`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct HypothesisResponse {
    pub summary: String,
    pub symbols: Vec<String>,
    /// Total number of raw patterns generated across all dimensions (before filtering)
    pub total_trials: usize,
    /// Significance threshold used for BH-FDR correction
    pub significance_threshold: f64,
    /// Number of patterns that had sufficient data (>= 5 observations) for t-testing
    pub patterns_tested: usize,
    /// Number surviving BH-FDR correction
    pub patterns_significant: usize,
    /// Number after deduplication
    pub patterns_after_dedup: usize,
    /// Ranked hypotheses (best first)
    pub hypotheses: Vec<DiscoveredPattern>,
    pub key_findings: Vec<String>,
    pub suggested_next_steps: Vec<String>,
}
