//! Response types for statistics tools: `aggregate_prices`, distribution, correlate,
//! `rolling_metric`, `regime_detect`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::data::DateRange;

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
