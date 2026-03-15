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

    let summary = if metric == "return" {
        // Return metric: include significance information
        if sig_buckets.is_empty() {
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
        }
    } else {
        // Non-return metrics (volume, range): omit significance language
        format!("Aggregated {metric} for {upper} by {group_by} across {total_bars} bars.",)
    };

    let mut key_findings = Vec::new();
    if metric == "return" {
        if let Some(best) = buckets.iter().max_by(|a, b| {
            a.mean
                .partial_cmp(&b.mean)
                .unwrap_or(std::cmp::Ordering::Equal)
        }) {
            key_findings.push(format!(
                "Highest mean {metric}: {} ({:.4})",
                best.label, best.mean
            ));
        }
        if let Some(worst) = buckets.iter().min_by(|a, b| {
            a.mean
                .partial_cmp(&b.mean)
                .unwrap_or(std::cmp::Ordering::Equal)
        }) {
            key_findings.push(format!(
                "Lowest mean {metric}: {} ({:.4})",
                worst.label, worst.mean
            ));
        }
        if sig_buckets.is_empty() {
            key_findings
                .push("No statistically significant seasonal patterns detected".to_string());
        } else {
            key_findings.push(format!(
                "{} of {} buckets show statistically significant deviations (p<0.05)",
                sig_buckets.len(),
                buckets.len()
            ));
        }
    } else {
        if let Some(best) = buckets.iter().max_by(|a, b| {
            a.mean
                .partial_cmp(&b.mean)
                .unwrap_or(std::cmp::Ordering::Equal)
        }) {
            key_findings.push(format!(
                "Highest mean {metric}: {} ({:.4})",
                best.label, best.mean
            ));
        }
        if let Some(worst) = buckets.iter().min_by(|a, b| {
            a.mean
                .partial_cmp(&b.mean)
                .unwrap_or(std::cmp::Ordering::Equal)
        }) {
            key_findings.push(format!(
                "Lowest mean {metric}: {} ({:.4})",
                worst.label, worst.mean
            ));
        }
    }

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
        key_findings,
        warnings,
        suggested_next_steps,
    }
}

/// Format the result of a `distribution` analysis.
#[allow(clippy::too_many_arguments, clippy::similar_names)]
pub fn format_distribution(
    source_label: String,
    symbol: Option<&str>,
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

    let summary = format!(
        "Distribution of {source_label}: {n} observations, mean={mean:.4}, std={std_dev:.4}, \
         skew={skewness:.3}, kurtosis={kurtosis:.3}. {normal_text}.",
    );

    // Only emit symbol-based next steps when the source is price data (symbol is known).
    let suggested_next_steps = if let Some(sym) = symbol {
        let sym_upper = sym.to_uppercase();
        vec![
            format!(
                "[NEXT] Call aggregate_prices(symbol=\"{sym_upper}\", group_by=\"month\") to check for seasonal patterns"
            ),
            format!(
                "[THEN] Call rolling_metric(symbol=\"{sym_upper}\", metric=\"volatility\") to see how risk changes over time"
            ),
        ]
    } else {
        vec![
            "[NEXT] Call aggregate_prices with a price symbol to check for seasonal patterns"
                .to_string(),
            "[THEN] Call rolling_metric with a price symbol to see how risk changes over time"
                .to_string(),
        ]
    };

    let mut key_findings = vec![
        format!(
            "Mean={mean:.4}, Median={median:.4} — {}",
            if (mean - median).abs() / std_dev.max(1e-10) > 0.5 {
                "notable mean-median divergence (skewed)"
            } else {
                "mean and median are close (symmetric)"
            }
        ),
        format!(
            "Skewness={skewness:.3}, Kurtosis={kurtosis:.3} — {}",
            if kurtosis > 1.0 {
                "fat tails present"
            } else if kurtosis < -0.5 {
                "thin tails"
            } else {
                "near-normal tail behavior"
            }
        ),
    ];
    if let Some(ref nt) = normality {
        key_findings.push(format!(
            "Jarque-Bera p={:.4} — {}",
            nt.p_value,
            if nt.is_normal {
                "consistent with normality"
            } else {
                "significantly non-normal"
            }
        ));
    }
    if let Some(ref tr) = tail_ratio {
        key_findings.push(format!(
            "Tail ratio: {:.2} — {}",
            tr.ratio, tr.interpretation
        ));
    }

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
        key_findings,
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
        format!("[THEN] Call regime_detect(symbol=\"{symbol_a_upper}\") to see if correlation changes across market regimes"),
    ];

    let mut key_findings = vec![
        format!("{strength} {direction} correlation (Pearson={pearson:.3})"),
        format!(
            "R²={r_squared:.3} — {:.1}% of variance explained",
            r_squared * 100.0
        ),
    ];
    if let Some(p) = p_value {
        key_findings.push(format!(
            "p-value={p:.4} — {}",
            if p < 0.01 {
                "highly significant"
            } else if p < 0.05 {
                "significant"
            } else {
                "not significant"
            }
        ));
    }
    if (pearson - spearman).abs() > 0.15 {
        key_findings.push(format!(
            "Pearson-Spearman gap={:.3} — possible nonlinear relationship",
            (pearson - spearman).abs()
        ));
    }

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
        key_findings,
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

    let key_findings = vec![
        format!(
            "Current {metric}={current:.4} vs mean={s_mean:.4} — {}",
            if current > s_mean {
                "above average"
            } else {
                "below average"
            }
        ),
        format!("Range: [{s_min:.4}, {s_max:.4}], trend is {trend}"),
        format!("{window}-day rolling window over {n_observations} observations"),
    ];

    RollingMetricResponse {
        summary,
        symbol: upper,
        metric: metric.to_string(),
        window,
        n_observations,
        stats,
        series,
        key_findings,
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
    classified_bars: usize,
    regimes: Vec<RegimeInfo>,
    transition_matrix: Vec<Vec<f64>>,
    regime_series: Vec<RegimeSeriesPoint>,
) -> RegimeDetectResponse {
    let upper = symbol.to_uppercase();

    let summary = format!(
        "Detected {} regimes for {upper} using {method} over {} bars ({} classified). {}",
        n_regimes,
        total_bars,
        classified_bars,
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

    let mut key_findings: Vec<String> = regimes
        .iter()
        .map(|r| {
            format!(
                "{}: {:.1}% of bars, mean return={:.4}, return std={:.4}, realized vol={:.4}",
                r.label, r.pct_of_total, r.mean_return, r.std_dev, r.mean_vol
            )
        })
        .collect();
    if let Some(dominant) = regimes.iter().max_by(|a, b| {
        a.pct_of_total
            .partial_cmp(&b.pct_of_total)
            .unwrap_or(std::cmp::Ordering::Equal)
    }) {
        key_findings.push(format!(
            "Dominant regime: {} ({:.1}% of time)",
            dominant.label, dominant.pct_of_total
        ));
    }

    RegimeDetectResponse {
        summary,
        symbol: upper,
        method: method.to_string(),
        n_regimes,
        total_bars,
        classified_bars,
        regimes,
        transition_matrix,
        regime_series,
        key_findings,
        suggested_next_steps,
    }
}
