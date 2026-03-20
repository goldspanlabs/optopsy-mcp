//! Benchmark-relative performance analysis tool.
//!
//! Computes Jensen's alpha, beta, Treynor ratio, Information Ratio,
//! tracking error, and up/down capture ratios by comparing asset returns
//! to a benchmark.

use anyhow::{Context, Result};
use std::sync::Arc;

use crate::data::cache::CachedStore;
use crate::tools::response_types::BenchmarkAnalysisResponse;

/// Execute benchmark-relative analysis.
#[allow(clippy::too_many_lines)]
pub async fn execute(
    cache: &Arc<CachedStore>,
    symbol: &str,
    benchmark: &str,
    years: u32,
) -> Result<BenchmarkAnalysisResponse> {
    let upper = symbol.to_uppercase();
    let bench_upper = benchmark.to_uppercase();
    let cutoff = chrono::Utc::now().date_naive() - chrono::Duration::days(i64::from(years) * 365);
    let cutoff_str = cutoff.format("%Y-%m-%d").to_string();

    // Load both return series
    let asset_returns = load_returns(cache, &upper, &cutoff_str).await?;
    let bench_returns = load_returns(cache, &bench_upper, &cutoff_str).await?;

    // Align to minimum length from end
    let min_len = asset_returns.len().min(bench_returns.len());
    if min_len < 30 {
        anyhow::bail!("Insufficient aligned observations: {min_len} (need at least 30)");
    }

    let asset_ret = &asset_returns[asset_returns.len() - min_len..];
    let bench_ret = &bench_returns[bench_returns.len() - min_len..];
    let n = min_len;

    // CAPM regression: R_asset = alpha + beta * R_benchmark + epsilon
    let mean_a = asset_ret.iter().sum::<f64>() / n as f64;
    let mean_b = bench_ret.iter().sum::<f64>() / n as f64;

    let cov_ab: f64 = asset_ret
        .iter()
        .zip(bench_ret.iter())
        .map(|(a, b)| (a - mean_a) * (b - mean_b))
        .sum::<f64>()
        / (n - 1) as f64;

    let var_b: f64 = bench_ret.iter().map(|b| (b - mean_b).powi(2)).sum::<f64>() / (n - 1) as f64;

    let beta = if var_b > 0.0 { cov_ab / var_b } else { 0.0 };
    let alpha_daily = mean_a - beta * mean_b;
    let alpha_annualized = alpha_daily * 252.0;

    // R² and residual analysis
    let ss_tot: f64 = asset_ret.iter().map(|a| (a - mean_a).powi(2)).sum();
    let mut sse = 0.0;
    for (a, b) in asset_ret.iter().zip(bench_ret.iter()) {
        let predicted = alpha_daily + beta * b;
        let resid = a - predicted;
        sse += resid * resid;
    }
    let r_squared = if ss_tot > 0.0 {
        1.0 - sse / ss_tot
    } else {
        0.0
    };

    // Alpha significance
    let sigma_sq = sse / (n - 2) as f64;
    let sum_b_sq: f64 = bench_ret.iter().map(|b| b.powi(2)).sum();
    let sum_b: f64 = bench_ret.iter().sum();
    let denom = n as f64 * sum_b_sq - sum_b.powi(2);
    let (_alpha_se, alpha_t_stat, alpha_significant) = if sigma_sq > 0.0 && denom.abs() > 1e-12 {
        let var_alpha = sigma_sq * (1.0 / n as f64 + mean_b.powi(2) * n as f64 / denom);
        let se = var_alpha.abs().sqrt();
        if se > 0.0 {
            let t = alpha_daily / se;
            (se, t, t.abs() > 1.96)
        } else {
            (0.0, 0.0, false)
        }
    } else {
        // Degenerate benchmark (constant returns): cannot compute stable alpha t-stat
        (0.0, 0.0, false)
    };

    // Tracking error: annualized std of excess returns
    let excess: Vec<f64> = asset_ret
        .iter()
        .zip(bench_ret.iter())
        .map(|(a, b)| a - b)
        .collect();
    let excess_mean = excess.iter().sum::<f64>() / n as f64;
    let tracking_error_daily = (excess
        .iter()
        .map(|e| (e - excess_mean).powi(2))
        .sum::<f64>()
        / (n - 1) as f64)
        .sqrt();
    let tracking_error = tracking_error_daily * 252.0_f64.sqrt();

    // Information Ratio = annualized excess return / tracking error
    let information_ratio = if tracking_error > 0.0 {
        (excess_mean * 252.0) / tracking_error
    } else {
        0.0
    };

    // Treynor ratio = annualized mean return / beta (no risk-free subtraction)
    let treynor = if beta.abs() > 1e-10 {
        (mean_a * 252.0) / beta
    } else {
        0.0
    };

    // Up/Down capture ratios
    let (up_capture, down_capture) = compute_capture_ratios(asset_ret, bench_ret);

    // Summary
    let summary = format!(
        "Benchmark analysis for {upper} vs {bench_upper} ({n} obs): \
         alpha={:.2}% ({}), beta={beta:.3}, IR={information_ratio:.3}, \
         tracking error={:.2}%.",
        alpha_annualized * 100.0,
        if alpha_significant {
            "significant"
        } else {
            "not significant"
        },
        tracking_error * 100.0,
    );

    let key_findings = vec![
        format!(
            "Jensen's alpha: {:.2}% annualized (t={:.2}) — {}",
            alpha_annualized * 100.0,
            alpha_t_stat,
            if alpha_significant {
                "significant outperformance"
            } else if alpha_annualized > 0.0 {
                "positive but not statistically significant"
            } else {
                "no outperformance detected"
            }
        ),
        format!(
            "Beta: {:.3} — {}",
            beta,
            if beta > 1.05 {
                "more volatile than benchmark (aggressive)"
            } else if beta < 0.95 {
                "less volatile than benchmark (defensive)"
            } else {
                "similar volatility to benchmark"
            }
        ),
        format!(
            "Information Ratio: {:.3} — {}",
            information_ratio,
            if information_ratio > 0.5 {
                "excellent risk-adjusted excess return"
            } else if information_ratio > 0.2 {
                "good risk-adjusted excess return"
            } else if information_ratio > 0.0 {
                "modest excess return"
            } else {
                "negative excess return"
            }
        ),
        format!(
            "Up capture: {:.1}%, Down capture: {:.1}% — {}",
            up_capture * 100.0,
            down_capture * 100.0,
            if up_capture > down_capture {
                "asymmetric capture (desirable)"
            } else {
                "symmetric or unfavorable capture"
            }
        ),
    ];

    let suggested_next_steps = vec![
        format!(
            "[NEXT] Call factor_attribution(symbol=\"{upper}\") to decompose alpha into factor exposures"
        ),
        format!(
            "[THEN] Call rolling_metric(symbol=\"{upper}\", metric=\"beta\", benchmark=\"{bench_upper}\") to check beta stability"
        ),
        format!(
            "[TIP] IR > 0.5 is excellent, > 1.0 is exceptional. Current: {information_ratio:.3}"
        ),
    ];

    Ok(BenchmarkAnalysisResponse {
        summary,
        symbol: upper,
        benchmark: bench_upper,
        n_observations: n,
        alpha: alpha_annualized,
        alpha_t_stat,
        alpha_significant,
        beta,
        treynor,
        information_ratio,
        tracking_error,
        r_squared,
        up_capture,
        down_capture,
        key_findings,
        suggested_next_steps,
    })
}

/// Load daily returns for a symbol.
async fn load_returns(
    cache: &Arc<CachedStore>,
    symbol: &str,
    cutoff_str: &str,
) -> Result<Vec<f64>> {
    let resp = crate::tools::raw_prices::load_and_execute(
        cache,
        symbol,
        Some(cutoff_str),
        None,
        None,
        crate::engine::types::Interval::Daily,
        None,
    )
    .await
    .context(format!("Failed to load OHLCV data for {symbol}"))?;

    let returns: Vec<f64> = resp
        .prices
        .windows(2)
        .filter_map(|w| {
            if w[0].close == 0.0 {
                None
            } else {
                Some((w[1].close - w[0].close) / w[0].close)
            }
        })
        .filter(|r| r.is_finite())
        .collect();

    Ok(returns)
}

/// Compute up and down capture ratios.
///
/// Up capture = `mean(asset_ret | bench > 0) / mean(bench_ret | bench > 0)`
/// Down capture = `mean(asset_ret | bench < 0) / mean(bench_ret | bench < 0)`
fn compute_capture_ratios(asset: &[f64], bench: &[f64]) -> (f64, f64) {
    let mut up_asset = Vec::new();
    let mut up_bench = Vec::new();
    let mut down_asset = Vec::new();
    let mut down_bench = Vec::new();

    for (a, b) in asset.iter().zip(bench.iter()) {
        if *b > 0.0 {
            up_asset.push(*a);
            up_bench.push(*b);
        } else if *b < 0.0 {
            down_asset.push(*a);
            down_bench.push(*b);
        }
    }

    let up_capture = if up_bench.is_empty() {
        1.0
    } else {
        let mean_up_asset = up_asset.iter().sum::<f64>() / up_asset.len() as f64;
        let mean_up_bench = up_bench.iter().sum::<f64>() / up_bench.len() as f64;
        if mean_up_bench.abs() > 1e-15 {
            mean_up_asset / mean_up_bench
        } else {
            1.0
        }
    };

    let down_capture = if down_bench.is_empty() {
        1.0
    } else {
        let mean_down_asset = down_asset.iter().sum::<f64>() / down_asset.len() as f64;
        let mean_down_bench = down_bench.iter().sum::<f64>() / down_bench.len() as f64;
        if mean_down_bench.abs() > 1e-15 {
            mean_down_asset / mean_down_bench
        } else {
            1.0
        }
    };

    (up_capture, down_capture)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_ratios_perfect_tracking() {
        // Asset exactly matches benchmark → up=1.0, down=1.0
        let bench = vec![0.01, -0.02, 0.015, -0.005, 0.03, -0.01];
        let asset = bench.clone();
        let (up, down) = compute_capture_ratios(&asset, &bench);
        assert!((up - 1.0).abs() < 1e-10, "up={up}");
        assert!((down - 1.0).abs() < 1e-10, "down={down}");
    }

    #[test]
    fn capture_ratios_double_returns() {
        // Asset = 2x benchmark → up=2.0, down=2.0
        let bench = vec![0.01, -0.02, 0.015, -0.005, 0.03, -0.01];
        let asset: Vec<f64> = bench.iter().map(|b| b * 2.0).collect();
        let (up, down) = compute_capture_ratios(&asset, &bench);
        assert!((up - 2.0).abs() < 1e-10, "up={up}");
        assert!((down - 2.0).abs() < 1e-10, "down={down}");
    }

    #[test]
    fn capture_ratios_inverse_asset() {
        // Asset moves opposite to benchmark
        let bench = vec![0.01, -0.02, 0.015, -0.005];
        let asset: Vec<f64> = bench.iter().map(|b| -b).collect();
        let (up, down) = compute_capture_ratios(&asset, &bench);
        assert!((up - (-1.0)).abs() < 1e-10, "up={up}");
        assert!((down - (-1.0)).abs() < 1e-10, "down={down}");
    }

    #[test]
    fn capture_ratios_all_up_days() {
        let bench = vec![0.01, 0.02, 0.015, 0.005];
        let asset = vec![0.02, 0.03, 0.01, 0.01];
        let (up, down) = compute_capture_ratios(&asset, &bench);
        // mean_asset_up = (0.02+0.03+0.01+0.01)/4 = 0.0175
        // mean_bench_up = (0.01+0.02+0.015+0.005)/4 = 0.0125 → up = 1.4
        assert!((up - 1.4).abs() < 1e-10, "up={up}");
        // No down days → down = 1.0 (default)
        assert!((down - 1.0).abs() < 1e-10, "down={down}");
    }

    #[test]
    fn capture_ratios_all_down_days() {
        let bench = vec![-0.01, -0.02, -0.015, -0.005];
        let asset = vec![-0.005, -0.01, -0.0075, -0.0025];
        let (up, down) = compute_capture_ratios(&asset, &bench);
        // No up days → up = 1.0 (default)
        assert!((up - 1.0).abs() < 1e-10, "up={up}");
        // Asset captures half of downside → down = 0.5
        assert!((down - 0.5).abs() < 1e-10, "down={down}");
    }

    #[test]
    fn capture_ratios_asymmetric_desirable() {
        // Asset captures more upside, less downside
        let bench = vec![0.02, -0.02, 0.01, -0.01];
        let asset = vec![0.03, -0.01, 0.015, -0.005]; // 1.5x up, 0.5x down
        let (up, down) = compute_capture_ratios(&asset, &bench);
        assert!(up > 1.0, "up={up}");
        assert!(down < 1.0, "down={down}");
        assert!(up > down, "up capture should exceed down capture");
    }

    #[test]
    fn capture_ratios_zero_bench_days_ignored() {
        // Days where bench=0 should be skipped
        let bench = vec![0.01, 0.0, -0.02, 0.0, 0.015];
        let asset = vec![0.02, 0.005, -0.01, 0.003, 0.02];
        let (up, down) = compute_capture_ratios(&asset, &bench);
        // Only bench > 0 days: (0.01, 0.015) with asset (0.02, 0.02)
        // mean_asset = 0.02, mean_bench = 0.0125 → up = 1.6
        assert!((up - 1.6).abs() < 1e-10, "up={up}");
        // Only bench < 0 days: (-0.02) with asset (-0.01)
        // down = -0.01 / -0.02 = 0.5
        assert!((down - 0.5).abs() < 1e-10, "down={down}");
    }
}
