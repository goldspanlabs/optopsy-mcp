//! Correlation analysis between two price series.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;

use crate::data::cache::CachedStore;
use crate::stats;
use crate::tools::response_types::CorrelationSeries;
use crate::tools::response_types::{CorrelateResponse, RollingCorrelationPoint, ScatterPoint};

/// Execute the correlate analysis.
#[allow(clippy::too_many_lines, clippy::similar_names)]
pub async fn execute(
    cache: &Arc<CachedStore>,
    series_a: &CorrelationSeries,
    series_b: &CorrelationSeries,
    mode: &str,
    window: usize,
    years: u32,
) -> Result<CorrelateResponse> {
    if mode != "full" && mode != "rolling" {
        anyhow::bail!("Invalid mode: \"{mode}\". Must be \"full\" or \"rolling\".");
    }

    let cutoff = chrono::Utc::now().date_naive() - chrono::Duration::days(i64::from(years) * 365);
    let cutoff_str = cutoff.format("%Y-%m-%d").to_string();

    // Load both series
    let (prices_a, prices_b) = {
        let a = crate::tools::raw_prices::load_and_execute(
            cache,
            &series_a.symbol.to_uppercase(),
            Some(&cutoff_str),
            None,
            None,
            crate::engine::types::Interval::Daily,
        )
        .await
        .context(format!("Failed to load data for {}", series_a.symbol))?;

        let b = crate::tools::raw_prices::load_and_execute(
            cache,
            &series_b.symbol.to_uppercase(),
            Some(&cutoff_str),
            None,
            None,
            crate::engine::types::Interval::Daily,
        )
        .await
        .context(format!("Failed to load data for {}", series_b.symbol))?;

        (a.prices, b.prices)
    };

    // Build date-indexed maps
    let map_a: HashMap<&str, usize> = prices_a
        .iter()
        .enumerate()
        .map(|(i, p)| (p.date.as_str(), i))
        .collect();

    // Align by date (inner join)
    let mut aligned_dates: Vec<String> = Vec::new();
    let mut vals_a_raw: Vec<(f64, f64, f64, f64, f64)> = Vec::new();
    let mut vals_b_raw: Vec<(f64, f64, f64, f64, f64)> = Vec::new();

    for pb in &prices_b {
        if let Some(&idx_a) = map_a.get(pb.date.as_str()) {
            let pa = &prices_a[idx_a];
            aligned_dates.push(pb.date.clone());
            vals_a_raw.push((pa.close, pa.open, pa.high, pa.low, pa.volume as f64));
            vals_b_raw.push((pb.close, pb.open, pb.high, pb.low, pb.volume as f64));
        }
    }

    if aligned_dates.len() < 2 {
        anyhow::bail!(
            "Insufficient overlapping dates between {} and {} (found {})",
            series_a.symbol,
            series_b.symbol,
            aligned_dates.len()
        );
    }

    // Extract the requested field
    let extract_field = |raw: &[(f64, f64, f64, f64, f64)], field: &str| -> Result<Vec<f64>> {
        match field {
            "close" => Ok(raw.iter().map(|r| r.0).collect()),
            "open" => Ok(raw.iter().map(|r| r.1).collect()),
            "high" => Ok(raw.iter().map(|r| r.2).collect()),
            "low" => Ok(raw.iter().map(|r| r.3).collect()),
            "volume" => Ok(raw.iter().map(|r| r.4).collect()),
            "return" => {
                let closes: Vec<f64> = raw.iter().map(|r| r.0).collect();
                Ok(closes
                    .windows(2)
                    .map(|w| if w[0] == 0.0 { 0.0 } else { (w[1] - w[0]) / w[0] * 100.0 })
                    .collect())
            }
            _ => anyhow::bail!("Invalid field: \"{field}\". Must be one of: close, open, high, low, volume, return"),
        }
    };

    let field_a = extract_field(&vals_a_raw, &series_a.field)?;
    let field_b = extract_field(&vals_b_raw, &series_b.field)?;

    // If using returns, dates shift by 1
    let dates = if series_a.field == "return" || series_b.field == "return" {
        aligned_dates[1..].to_vec()
    } else {
        aligned_dates.clone()
    };

    // Trim to common length
    let n = field_a.len().min(field_b.len()).min(dates.len());
    let fa = &field_a[..n];
    let fb = &field_b[..n];
    let dates = &dates[..n];

    // Compute full-period stats
    let pearson = stats::pearson(fa, fb);
    let spearman = stats::spearman(fa, fb);
    let r_squared = pearson * pearson;

    // P-value for pearson (t-test approximation)
    let p_value = if n > 2 {
        let t_stat = pearson * ((n as f64 - 2.0) / (1.0 - r_squared)).sqrt();
        let dist = statrs::distribution::StudentsT::new(0.0, 1.0, (n - 2) as f64).ok();
        dist.map(|d| {
            use statrs::distribution::ContinuousCDF;
            2.0 * (1.0 - d.cdf(t_stat.abs()))
        })
    } else {
        None
    };

    // Rolling correlation
    let rolling_correlation = if mode == "rolling" && n >= window {
        let mut points = Vec::with_capacity(n - window + 1);
        for i in (window - 1)..n {
            let start = i + 1 - window;
            let r = stats::pearson(&fa[start..=i], &fb[start..=i]);
            points.push(RollingCorrelationPoint {
                date: dates[i].clone(),
                correlation: r,
            });
        }
        // Subsample to max 500 points
        subsample(points, 500)
    } else {
        vec![]
    };

    // Scatter data (subsample to max 500)
    let scatter: Vec<ScatterPoint> = {
        let all: Vec<ScatterPoint> = (0..n)
            .map(|i| ScatterPoint {
                x: fa[i],
                y: fb[i],
                date: dates[i].clone(),
            })
            .collect();
        subsample(all, 500)
    };

    let label_a = format!("{} {}", series_a.symbol.to_uppercase(), series_a.field);
    let label_b = format!("{} {}", series_b.symbol.to_uppercase(), series_b.field);

    let strength = if pearson.abs() > 0.7 {
        "strong"
    } else if pearson.abs() > 0.4 {
        "moderate"
    } else if pearson.abs() > 0.2 {
        "weak"
    } else {
        "negligible"
    };
    let direction = if pearson > 0.0 {
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
            "[NEXT] Call rolling_metric(symbol=\"{}\", metric=\"volatility\") to compare vol regimes",
            series_a.symbol.to_uppercase()
        ),
        "[THEN] Call regime_detect to see if correlation changes across market regimes".to_string(),
    ];

    Ok(CorrelateResponse {
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
    })
}

/// Subsample a Vec to at most `max` elements using evenly-spaced indices.
fn subsample<T: Clone>(data: Vec<T>, max: usize) -> Vec<T> {
    let n = data.len();
    if n <= max {
        return data;
    }
    let mut indices: Vec<usize> = (0..max).map(|i| i * (n - 1) / (max - 1)).collect();
    indices.dedup();
    indices.iter().map(|&i| data[i].clone()).collect()
}
