//! Cointegration testing tool using the Engle-Granger two-step method.
//!
//! Tests whether two price series share a long-run equilibrium (cointegration)
//! by fitting a cointegrating regression and testing the residuals for stationarity
//! via an Augmented Dickey-Fuller (ADF) test. Cointegrated pairs are candidates
//! for mean-reversion / statistical arbitrage strategies.

use anyhow::{Context, Result};
use std::sync::Arc;

use crate::constants::CALENDAR_DAYS_PER_YEAR;

use crate::data::cache::CachedStore;
use crate::tools::response_types::{
    AdfTestResult, CointegrationResponse, CriticalValues, SpreadPoint, SpreadStats,
};

/// Execute the cointegration test.
#[allow(clippy::too_many_lines)]
pub async fn execute(
    cache: &Arc<CachedStore>,
    symbol_a: &str,
    symbol_b: &str,
    years: u32,
) -> Result<CointegrationResponse> {
    let upper_a = symbol_a.to_uppercase();
    let upper_b = symbol_b.to_uppercase();
    let cutoff = chrono::Utc::now().date_naive()
        - chrono::Duration::days(i64::from(years) * CALENDAR_DAYS_PER_YEAR);
    let cutoff_str = cutoff.format("%Y-%m-%d").to_string();

    // Load both price series
    let resp_a = crate::tools::raw_prices::load_and_execute(
        cache,
        &upper_a,
        Some(&cutoff_str),
        None,
        None,
        crate::engine::types::Interval::Daily,
        None,
    )
    .await
    .context(format!("Failed to load OHLCV data for {upper_a}"))?;

    let resp_b = crate::tools::raw_prices::load_and_execute(
        cache,
        &upper_b,
        Some(&cutoff_str),
        None,
        None,
        crate::engine::types::Interval::Daily,
        None,
    )
    .await
    .context(format!("Failed to load OHLCV data for {upper_b}"))?;

    // Align by date (inner join)
    let mut map_a = std::collections::HashMap::new();
    for p in &resp_a.prices {
        map_a.insert(p.date, p.close);
    }

    let mut prices_a = Vec::new();
    let mut prices_b = Vec::new();
    let mut dates = Vec::new();
    for p in &resp_b.prices {
        if let Some(&close_a) = map_a.get(&p.date) {
            prices_a.push(close_a);
            prices_b.push(p.close);
            dates.push(p.date);
        }
    }

    let n = prices_a.len();
    if n < 30 {
        anyhow::bail!(
            "Insufficient aligned observations for {upper_a}/{upper_b}: {n} (need at least 30)"
        );
    }

    // Step 1: OLS regression — B = alpha + beta * A
    let (intercept, hedge_ratio, r_squared) = ols_regression(&prices_a, &prices_b);

    // Step 2: Compute residuals (spread)
    let spread: Vec<f64> = prices_a
        .iter()
        .zip(prices_b.iter())
        .map(|(a, b)| b - intercept - hedge_ratio * a)
        .collect();

    // Step 3: ADF test on residuals
    let adf = adf_test(&spread);

    let is_cointegrated = adf.is_stationary;

    // Spread statistics
    let spread_mean = spread.iter().sum::<f64>() / spread.len() as f64;
    let spread_std = {
        let variance = spread
            .iter()
            .map(|s| (s - spread_mean).powi(2))
            .sum::<f64>()
            / (n - 1) as f64;
        variance.sqrt()
    };
    let current_spread = *spread.last().unwrap_or(&0.0);
    let z_score = if spread_std > 0.0 {
        (current_spread - spread_mean) / spread_std
    } else {
        0.0
    };

    // Percentile of current spread
    let count_below = spread.iter().filter(|&&s| s <= current_spread).count();
    let percentile = count_below as f64 / n as f64 * 100.0;

    // Half-life of mean reversion (from AR(1) fit on spread)
    let half_life = compute_half_life(&spread);

    let spread_stats = SpreadStats {
        mean: spread_mean,
        std_dev: spread_std,
        current: current_spread,
        z_score,
        percentile,
        half_life,
    };

    // Build spread time series (subsample to max 500)
    let date_strs: Vec<String> = dates
        .iter()
        .map(|&d| {
            chrono::DateTime::from_timestamp(d, 0)
                .map_or_else(|| d.to_string(), |dt| dt.format("%Y-%m-%d").to_string())
        })
        .collect();

    let spread_series: Vec<SpreadPoint> = if spread.len() > 500 {
        let step = spread.len() / 500;
        spread
            .iter()
            .zip(date_strs.iter())
            .enumerate()
            .filter(|(i, _)| i % step == 0)
            .take(500)
            .map(|(_, (s, d))| {
                let z = if spread_std > 0.0 {
                    (s - spread_mean) / spread_std
                } else {
                    0.0
                };
                SpreadPoint {
                    date: d.clone(),
                    spread: *s,
                    z_score: z,
                }
            })
            .collect()
    } else {
        spread
            .iter()
            .zip(date_strs.iter())
            .map(|(s, d)| {
                let z = if spread_std > 0.0 {
                    (s - spread_mean) / spread_std
                } else {
                    0.0
                };
                SpreadPoint {
                    date: d.clone(),
                    spread: *s,
                    z_score: z,
                }
            })
            .collect()
    };

    // Build response
    let summary = if is_cointegrated {
        format!(
            "{upper_a} and {upper_b} are COINTEGRATED (ADF stat={:.3}, p={:.4}). \
             Hedge ratio={hedge_ratio:.4}, spread z-score={z_score:.2}, \
             half-life={}. Suitable for pairs trading.",
            adf.statistic,
            adf.p_value,
            half_life.map_or("N/A".to_string(), |h| format!("{h:.1} days")),
        )
    } else {
        format!(
            "{upper_a} and {upper_b} are NOT cointegrated (ADF stat={:.3}, p={:.4}). \
             The spread is not reliably mean-reverting.",
            adf.statistic, adf.p_value,
        )
    };

    let mut key_findings = vec![
        format!(
            "Hedge ratio: {hedge_ratio:.4} (1 unit of {upper_b} hedged with {hedge_ratio:.4} units of {upper_a})"
        ),
        format!(
            "ADF test: statistic={:.3}, p-value={:.4} — {}",
            adf.statistic,
            adf.p_value,
            if is_cointegrated {
                "rejects unit root (spread is stationary)"
            } else {
                "fails to reject unit root (spread may be random walk)"
            }
        ),
        format!("Spread z-score: {z_score:.2} (current spread at {percentile:.0}th percentile)"),
    ];
    if let Some(hl) = half_life {
        key_findings.push(format!(
            "Half-life: {hl:.1} days — {}",
            if hl < 10.0 {
                "fast reversion, suitable for short-term trading"
            } else if hl < 30.0 {
                "moderate reversion speed"
            } else {
                "slow reversion, may need longer holding periods"
            }
        ));
    }
    if z_score.abs() > 2.0 && is_cointegrated {
        key_findings.push(format!(
            "SIGNAL: Spread at {z_score:.2}σ — potential {} entry",
            if z_score > 0.0 {
                "short spread"
            } else {
                "long spread"
            }
        ));
    }

    let suggested_next_steps = vec![
        format!(
            "[NEXT] Call run_stock_backtest with a mean-reversion signal based on spread z-score crossing ±2σ"
        ),
        format!(
            "[THEN] Call rolling_metric(symbol=\"{upper_a}\", metric=\"correlation\", benchmark=\"{upper_b}\") to check correlation stability"
        ),
        format!(
            "[TIP] Hedge ratio of {hedge_ratio:.4} means: go long 1 unit {upper_b}, short {hedge_ratio:.4} units {upper_a}"
        ),
    ];

    Ok(CointegrationResponse {
        summary,
        series_a: upper_a,
        series_b: upper_b,
        n_observations: n,
        hedge_ratio,
        intercept,
        r_squared,
        adf_test: adf,
        is_cointegrated,
        spread_stats,
        spread_series,
        key_findings,
        suggested_next_steps,
    })
}

/// OLS regression: y = alpha + beta * x. Returns (intercept, slope, R²).
#[allow(clippy::similar_names)]
fn ols_regression(x: &[f64], y: &[f64]) -> (f64, f64, f64) {
    let n = x.len() as f64;
    let sum_x: f64 = x.iter().sum();
    let sum_y: f64 = y.iter().sum();
    let sum_xy: f64 = x.iter().zip(y.iter()).map(|(a, b)| a * b).sum();
    let sum_x2: f64 = x.iter().map(|a| a * a).sum();

    let mean_x = sum_x / n;
    let mean_y = sum_y / n;

    let ss_xy = sum_xy - n * mean_x * mean_y;
    let ss_xx = sum_x2 - n * mean_x * mean_x;

    let beta = if ss_xx > 0.0 { ss_xy / ss_xx } else { 0.0 };
    let alpha = mean_y - beta * mean_x;

    // R²
    let ss_tot: f64 = y.iter().map(|yi| (yi - mean_y).powi(2)).sum();
    let ss_res: f64 = x
        .iter()
        .zip(y.iter())
        .map(|(xi, yi)| (yi - alpha - beta * xi).powi(2))
        .sum();
    let r_squared = if ss_tot > 0.0 {
        1.0 - ss_res / ss_tot
    } else {
        0.0
    };

    (alpha, beta, r_squared)
}

/// Augmented Dickey-Fuller test for stationarity.
///
/// Tests H0: series has a unit root (non-stationary) vs H1: stationary.
#[allow(clippy::doc_markdown)]
/// Uses a simple ADF(1) specification: Δy_t = a + γ * y_{t-1} + δ * Δy_{t-1} + ε_t
/// and compares the t-statistic of γ to MacKinnon critical values.
#[allow(clippy::similar_names)]
fn adf_test(series: &[f64]) -> AdfTestResult {
    let n = series.len();
    if n < 10 {
        return AdfTestResult {
            statistic: 0.0,
            p_value: 1.0,
            lags: 0,
            n_obs: n,
            is_stationary: false,
            critical_values: CriticalValues {
                pct_1: -3.43,
                pct_5: -2.86,
                pct_10: -2.57,
            },
        };
    }

    // Compute differences
    let dy: Vec<f64> = series.windows(2).map(|w| w[1] - w[0]).collect();
    let lag_dy: Vec<f64> = dy[..dy.len() - 1].to_vec();
    let lag_y: Vec<f64> = series[1..series.len() - 1].to_vec();
    let current_dy: Vec<f64> = dy[1..].to_vec();

    let obs = current_dy.len();
    if obs < 5 {
        return AdfTestResult {
            statistic: 0.0,
            p_value: 1.0,
            lags: 1,
            n_obs: obs,
            is_stationary: false,
            critical_values: CriticalValues {
                pct_1: -3.43,
                pct_5: -2.86,
                pct_10: -2.57,
            },
        };
    }

    // OLS: Δy_t = c + γ * y_{t-1} + δ * Δy_{t-1}
    // Build X matrix: [1, y_{t-1}, Δy_{t-1}] for each t
    let k = 3; // number of regressors (constant, lag_y, lag_dy)

    // X'X matrix (3x3) and X'y vector (3x1)
    let mut xtx = vec![0.0_f64; k * k];
    let mut xty = vec![0.0_f64; k];

    for i in 0..obs {
        let row = [1.0, lag_y[i], lag_dy[i]];
        let yi = current_dy[i];
        for r in 0..k {
            for c in 0..k {
                xtx[r * k + c] += row[r] * row[c];
            }
            xty[r] += row[r] * yi;
        }
    }

    let xtx_mat = nalgebra::Matrix3::from_row_slice(&xtx);
    let xty_vec = nalgebra::Vector3::from_row_slice(&xty);
    let coeffs = match xtx_mat.try_inverse() {
        Some(inv) => inv * xty_vec,
        None => nalgebra::Vector3::zeros(),
    };

    let gamma = coeffs[1]; // coefficient on y_{t-1}

    // Compute residuals and standard error of gamma
    let mut sse = 0.0;
    for i in 0..obs {
        let predicted = coeffs[0] + coeffs[1] * lag_y[i] + coeffs[2] * lag_dy[i];
        let resid = current_dy[i] - predicted;
        sse += resid * resid;
    }

    let sigma_sq = sse / (obs - k) as f64;

    // Variance of gamma is sigma^2 * (X'X)^{-1}[1,1]
    let var_gamma = match xtx_mat.try_inverse() {
        Some(inv) => sigma_sq * inv[(1, 1)],
        None => 0.0,
    };
    let se_gamma = var_gamma.abs().sqrt();

    let t_stat = if se_gamma > 0.0 {
        gamma / se_gamma
    } else {
        0.0
    };

    // MacKinnon approximate critical values (for constant, no trend, Engle-Granger residuals)
    let critical_values = CriticalValues {
        pct_1: -3.43,
        pct_5: -2.86,
        pct_10: -2.57,
    };

    // Approximate p-value using MacKinnon response surface (simplified)
    let p_value = approximate_adf_pvalue(t_stat, obs);

    AdfTestResult {
        statistic: t_stat,
        p_value,
        lags: 1,
        n_obs: obs,
        is_stationary: t_stat < critical_values.pct_5,
        critical_values,
    }
}

/// Approximate ADF p-value using `MacKinnon` (1994) response surface.
/// This is a simplified interpolation for the "constant, no trend" case.
fn approximate_adf_pvalue(t_stat: f64, _n_obs: usize) -> f64 {
    // Simplified lookup based on MacKinnon critical values:
    // t < -3.43 → p < 0.01
    // t < -2.86 → p < 0.05
    // t < -2.57 → p < 0.10
    // Linear interpolation between these points
    if t_stat < -3.96 {
        0.001
    } else if t_stat < -3.43 {
        // Interpolate between 0.001 and 0.01
        0.001 + (0.01 - 0.001) * (t_stat - (-3.96)) / (-3.43 - (-3.96))
    } else if t_stat < -2.86 {
        // Interpolate between 0.01 and 0.05
        0.01 + (0.05 - 0.01) * (t_stat - (-3.43)) / (-2.86 - (-3.43))
    } else if t_stat < -2.57 {
        // Interpolate between 0.05 and 0.10
        0.05 + (0.10 - 0.05) * (t_stat - (-2.86)) / (-2.57 - (-2.86))
    } else if t_stat < -1.94 {
        // Interpolate between 0.10 and 0.30
        0.10 + (0.30 - 0.10) * (t_stat - (-2.57)) / (-1.94 - (-2.57))
    } else if t_stat < -1.62 {
        // Interpolate between 0.30 and 0.50
        0.30 + (0.50 - 0.30) * (t_stat - (-1.94)) / (-1.62 - (-1.94))
    } else {
        // t_stat >= -1.62 → p > 0.50
        0.50 + 0.50 * (1.0 - (-t_stat / 5.0).exp())
    }
    .clamp(0.0001, 0.9999)
}

/// Compute the half-life of mean reversion from an AR(1) model on the spread.
///
#[allow(clippy::doc_markdown)]
/// Fits: `spread_t` = phi * `spread_{t-1}` + `epsilon_t`
/// Half-life = -ln(2) / ln(phi) days (only defined when 0 < phi < 1).
#[allow(clippy::similar_names)]
fn compute_half_life(spread: &[f64]) -> Option<f64> {
    if spread.len() < 10 {
        return None;
    }

    let y: Vec<f64> = spread[1..].to_vec();
    let x: Vec<f64> = spread[..spread.len() - 1].to_vec();

    let n = y.len() as f64;
    let sum_x: f64 = x.iter().sum();
    let sum_y: f64 = y.iter().sum();
    let sum_xy: f64 = x.iter().zip(y.iter()).map(|(a, b)| a * b).sum();
    let sum_x2: f64 = x.iter().map(|a| a * a).sum();

    let ss_xx = sum_x2 - n * (sum_x / n) * (sum_x / n);
    let ss_xy = sum_xy - n * (sum_x / n) * (sum_y / n);

    if ss_xx.abs() < 1e-15 {
        return None;
    }

    let phi = ss_xy / ss_xx;

    // Half-life only meaningful for mean-reverting processes (0 < phi < 1)
    if phi > 0.0 && phi < 1.0 {
        Some(-2.0_f64.ln() / phi.ln())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── ols_regression ─────────────────────────────────────────────

    #[test]
    fn ols_perfect_linear() {
        // y = 2 + 3*x → intercept=2, slope=3, R²=1
        let x = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let y: Vec<f64> = x.iter().map(|xi| 2.0 + 3.0 * xi).collect();
        let (alpha, beta, r2) = ols_regression(&x, &y);
        assert!((alpha - 2.0).abs() < 1e-10, "intercept={alpha}");
        assert!((beta - 3.0).abs() < 1e-10, "slope={beta}");
        assert!((r2 - 1.0).abs() < 1e-10, "R²={r2}");
    }

    #[test]
    fn ols_with_noise() {
        // x=[1,2,3,4,5], y=[3,5,8,9,11] (perfect y=1+2x would be [3,5,7,9,11])
        // Exact OLS: alpha=1.2, beta=2.0, R²=0.98039...
        let x = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let y = vec![3.0, 5.0, 8.0, 9.0, 11.0];
        let (alpha, beta, r2) = ols_regression(&x, &y);
        assert!((beta - 2.0).abs() < 1e-10, "slope={beta}");
        assert!((alpha - 1.2).abs() < 1e-10, "intercept={alpha}");
        assert!((r2 - 50.0 / 51.0).abs() < 1e-10, "R²={r2}");
    }

    #[test]
    fn ols_flat_x_returns_zero_slope() {
        let x = vec![5.0; 10];
        let y = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let (_alpha, beta, _r2) = ols_regression(&x, &y);
        assert!((beta - 0.0).abs() < 1e-10);
    }

    // ─── approximate_adf_pvalue ──────────────────────────────────────

    #[test]
    fn adf_pvalue_very_negative_t_stat() {
        let p = approximate_adf_pvalue(-5.0, 100);
        assert!(p < 0.01, "p={p} should be <0.01 for very negative t");
    }

    #[test]
    fn adf_pvalue_moderate_t_stat() {
        let p = approximate_adf_pvalue(-3.0, 100);
        assert!(p > 0.01 && p < 0.10, "p={p} for t=-3.0");
    }

    #[test]
    fn adf_pvalue_near_zero_t_stat() {
        let p = approximate_adf_pvalue(-0.5, 100);
        assert!(p > 0.30, "p={p} should be >0.30 for t=-0.5");
    }

    #[test]
    fn adf_pvalue_positive_t_stat() {
        let p = approximate_adf_pvalue(2.0, 100);
        assert!(p > 0.50, "p={p} should be >0.50 for positive t");
    }

    #[test]
    fn adf_pvalue_clamped() {
        let p1 = approximate_adf_pvalue(-10.0, 100);
        let p2 = approximate_adf_pvalue(100.0, 100);
        assert!(p1 >= 0.0001, "p={p1} should be >=0.0001");
        assert!(p2 <= 0.9999, "p={p2} should be <=0.9999");
    }

    // ─── adf_test ────────────────────────────────────────────────────

    #[test]
    fn adf_test_short_series_not_stationary() {
        let series = vec![1.0; 5];
        let result = adf_test(&series);
        assert!(!result.is_stationary);
        assert_eq!(result.p_value, 1.0);
    }

    #[test]
    fn adf_test_stationary_mean_reverting() {
        // Construct a strongly mean-reverting series: x_t = 0.3 * x_{t-1} + noise
        let mut rng = 42u64;
        let mut series = vec![0.0_f64; 200];
        for i in 1..200 {
            rng = rng.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let noise = ((rng >> 33) as f64 / f64::from(u32::MAX) - 0.5) * 0.1;
            series[i] = 0.3 * series[i - 1] + noise;
        }
        let result = adf_test(&series);
        assert!(
            result.statistic < -2.0,
            "t-stat={} should be very negative for stationary series",
            result.statistic
        );
        assert!(
            result.is_stationary,
            "mean-reverting series should be stationary"
        );
    }

    #[test]
    fn adf_test_random_walk_not_stationary() {
        // Random walk: x_t = x_{t-1} + noise (unit root)
        let mut rng = 123u64;
        let mut series = vec![100.0_f64; 200];
        for i in 1..200 {
            rng = rng.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let noise = ((rng >> 33) as f64 / f64::from(u32::MAX) - 0.5) * 0.5;
            series[i] = series[i - 1] + noise;
        }
        let result = adf_test(&series);
        // t-stat should be near zero or mildly negative (fail to reject unit root)
        assert!(
            result.statistic > -2.86,
            "t-stat={} should not reject unit root for random walk",
            result.statistic
        );
    }

    // ─── compute_half_life ───────────────────────────────────────────

    #[test]
    fn half_life_short_series_returns_none() {
        assert!(compute_half_life(&[1.0; 5]).is_none());
    }

    #[test]
    fn half_life_mean_reverting_positive() {
        // phi=0.5 → half-life = -ln(2)/ln(0.5) = 1.0
        // Build series: x_t = 0.5 * x_{t-1}
        let mut series = vec![10.0];
        for i in 1..50 {
            series.push(0.5 * series[i - 1]);
        }
        let hl = compute_half_life(&series);
        assert!(hl.is_some(), "should have half-life");
        let hl = hl.unwrap();
        assert!(
            (hl - 1.0).abs() < 0.1,
            "half-life={hl:.4}, expected ~1.0 for phi=0.5"
        );
    }

    #[test]
    fn half_life_unit_root_returns_none() {
        // phi ~ 1 → no mean reversion
        let series: Vec<f64> = (0..50).map(|i| 100.0 + f64::from(i) * 0.1).collect();
        let hl = compute_half_life(&series);
        // phi should be >= 1 for a trending series → None
        assert!(hl.is_none(), "trending series should have no half-life");
    }
}
