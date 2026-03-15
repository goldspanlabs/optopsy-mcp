//! AI response formatting for statistical analysis tools.
//!
//! Provides structured response builders for `aggregate_prices`, `distribution`,
//! `correlate`, `rolling_metric`, and `regime_detect`, following the same
//! pattern as the backtest and data formatters.

use crate::tools::response_types::{
    AggregateBucket, AggregatePricesResponse, CorrelateResponse, DateRange, DistributionResponse,
    HistogramBin, NormalityTest, RegimeDetectResponse, RegimeInfo, RegimeSeriesPoint,
    RollingCorrelationPoint, RollingMetricResponse, RollingPoint, RollingStats, ScatterPoint,
    TailRatio,
};

/// Format the result of an `aggregate_prices` analysis.
#[allow(clippy::too_many_arguments)]
pub fn format_aggregate_prices(
    symbol: &str,
    group_by: &str,
    metric: &str,
    total_bars: usize,
    date_range: DateRange,
    buckets: Vec<AggregateBucket>,
    warnings: Vec<String>,
) -> AggregatePricesResponse {
    let upper = symbol.to_uppercase();

    let sig_buckets: Vec<&AggregateBucket> = buckets
        .iter()
        .filter(|b| b.p_value.is_some_and(|p| p < 0.05))
        .collect();

    let summary = if sig_buckets.is_empty() {
        format!(
            "Aggregated {metric} for {upper} by {group_by} across {total_bars} bars. \
             No buckets show statistically significant deviations from zero (p<0.05).",
        )
    } else {
        let sig_names: Vec<&str> = sig_buckets.iter().map(|b| b.label.as_str()).collect();
        let sig_joined = sig_names.join(", ");
        format!(
            "Aggregated {metric} for {upper} by {group_by} across {total_bars} bars. \
             Statistically significant buckets (p<0.05): {sig_joined}.",
        )
    };

    let suggested_next_steps = vec![
        format!(
            "[NEXT] Call distribution(source={{\"type\":\"price_returns\",\"symbol\":\"{upper}\"}}) to analyze return distribution shape"
        ),
        format!(
            "[THEN] Call regime_detect(symbol=\"{upper}\") to identify market regimes"
        ),
    ];

    AggregatePricesResponse {
        summary,
        symbol: upper,
        group_by: group_by.to_string(),
        metric: metric.to_string(),
        total_bars,
        date_range,
        buckets,
        warnings,
        suggested_next_steps,
    }
}

/// Format the result of a `distribution` analysis.
#[allow(clippy::too_many_arguments, clippy::similar_names)]
pub fn format_distribution(
    source_label: String,
    n: usize,
    mean: f64,
    std_dev: f64,
    median: f64,
    skewness: f64,
    kurtosis: f64,
    min: f64,
    max: f64,
    percentile_5: f64,
    percentile_25: f64,
    percentile_75: f64,
    percentile_95: f64,
    histogram: Vec<HistogramBin>,
    normality: Option<NormalityTest>,
    tail_ratio: Option<TailRatio>,
) -> DistributionResponse {
    let normal_text = normality
        .as_ref()
        .map_or("normality test not available", |nt| {
            if nt.is_normal {
                "consistent with normal distribution"
            } else {
                "significantly non-normal"
            }
        });

    // Extract symbol from source label for next steps (best effort)
    let symbol_hint = source_label
        .split_whitespace()
        .next()
        .unwrap_or("symbol")
        .to_uppercase();

    let summary = format!(
        "Distribution of {source_label}: {n} observations, mean={mean:.4}, std={std_dev:.4}, \
         skew={skewness:.3}, kurtosis={kurtosis:.3}. {normal_text}.",
    );

    let suggested_next_steps = vec![
        format!(
            "[NEXT] Call aggregate_prices(symbol=\"{symbol_hint}\") to check for seasonal patterns"
        ),
        format!(
            "[THEN] Call rolling_metric(symbol=\"{symbol_hint}\", metric=\"volatility\") to see how risk changes over time"
        ),
    ];

    DistributionResponse {
        summary,
        source: source_label,
        n_observations: n,
        mean,
        std_dev,
        median,
        skewness,
        kurtosis,
        min,
        max,
        percentile_5,
        percentile_25,
        percentile_75,
        percentile_95,
        histogram,
        normality,
        tail_ratio,
        suggested_next_steps,
    }
}

/// Format the result of a `correlate` analysis.
#[allow(clippy::too_many_arguments)]
pub fn format_correlate(
    label_a: String,
    label_b: String,
    n: usize,
    pearson: f64,
    spearman: f64,
    r_squared: f64,
    p_value: Option<f64>,
    rolling_correlation: Vec<RollingCorrelationPoint>,
    scatter: Vec<ScatterPoint>,
    symbol_a_upper: &str,
) -> CorrelateResponse {
    let strength = if pearson.abs() > 0.7 {
        "strong"
    } else if pearson.abs() > 0.4 {
        "moderate"
    } else if pearson.abs() > 0.2 {
        "weak"
    } else {
        "negligible"
    };
    let direction = if pearson >= 0.0 {
        "positive"
    } else {
        "negative"
    };

    let summary = format!(
        "Correlation between {label_a} and {label_b}: Pearson={pearson:.3} ({strength} {direction}), \
         Spearman={spearman:.3}, R²={r_squared:.3} over {n} observations.",
    );

    let suggested_next_steps = vec![
        format!(
            "[NEXT] Call rolling_metric(symbol=\"{symbol_a_upper}\", metric=\"volatility\") to compare vol regimes"
        ),
        "[THEN] Call regime_detect to see if correlation changes across market regimes".to_string(),
    ];

    CorrelateResponse {
        summary,
        series_a: label_a,
        series_b: label_b,
        n_observations: n,
        pearson,
        spearman,
        r_squared,
        p_value,
        rolling_correlation,
        scatter,
        suggested_next_steps,
    }
}

/// Format the result of a `rolling_metric` analysis.
#[allow(clippy::too_many_arguments)]
pub fn format_rolling_metric(
    symbol: &str,
    metric: &str,
    window: usize,
    n_observations: usize,
    stats: RollingStats,
    series: Vec<RollingPoint>,
) -> RollingMetricResponse {
    let upper = symbol.to_uppercase();
    let current = stats.current;
    let s_mean = stats.mean;
    let s_min = stats.min;
    let s_max = stats.max;
    let trend = &stats.trend;

    let summary = format!(
        "Rolling {window}-day {metric} for {upper}: current={current:.4}, mean={s_mean:.4}, \
         range=[{s_min:.4}, {s_max:.4}], trend={trend}.",
    );

    let suggested_next_steps = vec![
        format!(
            "[NEXT] Call distribution(source={{\"type\":\"price_returns\",\"symbol\":\"{upper}\"}}) for return distribution"
        ),
        format!("[THEN] Call regime_detect(symbol=\"{upper}\") to identify market regimes"),
    ];

    RollingMetricResponse {
        summary,
        symbol: upper,
        metric: metric.to_string(),
        window,
        n_observations,
        stats,
        series,
        suggested_next_steps,
    }
}

/// Format the result of a `regime_detect` analysis.
#[allow(clippy::too_many_arguments)]
pub fn format_regime_detect(
    symbol: &str,
    method: &str,
    n_regimes: usize,
    total_bars: usize,
    regimes: Vec<RegimeInfo>,
    transition_matrix: Vec<Vec<f64>>,
    regime_series: Vec<RegimeSeriesPoint>,
) -> RegimeDetectResponse {
    let upper = symbol.to_uppercase();

    let summary = format!(
        "Detected {} regimes for {upper} using {method} over {} bars. {}",
        n_regimes,
        total_bars,
        regimes
            .iter()
            .map(|r| format!("{}: {:.1}%", r.label, r.pct_of_total))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let suggested_next_steps = vec![
        format!(
            "[NEXT] Call rolling_metric(symbol=\"{upper}\", metric=\"volatility\") to visualize vol over time"
        ),
        format!(
            "[THEN] Call aggregate_prices(symbol=\"{upper}\", group_by=\"month\") to see seasonal patterns"
        ),
    ];

    RegimeDetectResponse {
        summary,
        symbol: upper,
        method: method.to_string(),
        n_regimes,
        total_bars,
        regimes,
        transition_matrix,
        regime_series,
        suggested_next_steps,
    }
}
