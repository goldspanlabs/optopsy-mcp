//! Factor attribution tool using multi-factor regression.
//!
//! Decomposes asset returns into systematic factor exposures (Market, SMB, HML,
//! Momentum) using ETF proxies and OLS regression. Identifies whether returns
//! are driven by genuine alpha or by exposure to known risk premia.

use anyhow::Result;
use std::sync::Arc;

use crate::constants::{P_VALUE_THRESHOLD, TRADING_DAYS_PER_YEAR};
use crate::data::cache::CachedStore;
use crate::server::FactorProxies;
use crate::tools::ai_helpers::{compute_years_cutoff, load_returns};
use crate::tools::response_types::{FactorAttributionResponse, FactorExposure};

/// Execute the factor attribution analysis.
#[allow(clippy::too_many_lines)]
pub async fn execute(
    cache: &Arc<CachedStore>,
    symbol: &str,
    benchmark: &str,
    factor_proxies: Option<&FactorProxies>,
    years: u32,
) -> Result<FactorAttributionResponse> {
    let upper = symbol.to_uppercase();
    let bench_upper = benchmark.to_uppercase();
    let cutoff_str = compute_years_cutoff(years);

    // Load target returns
    let target_returns = load_returns(cache, &upper, &cutoff_str).await?;
    if target_returns.len() < 60 {
        anyhow::bail!("Insufficient data for {upper}: need at least 60 observations");
    }

    // Load benchmark (market) returns
    let market_returns = load_returns(cache, &bench_upper, &cutoff_str).await?;

    // Build factor matrix — start with just Market factor, add others if available
    let mut factor_names = vec!["Market".to_string()];
    let mut factor_series: Vec<Vec<f64>> = vec![market_returns.clone()];

    // Try to load SMB proxy (small cap - large cap)
    let small_cap = factor_proxies
        .and_then(|fp| fp.small_cap.as_deref())
        .unwrap_or("IWM");
    if let Ok(small_ret) = load_returns(cache, &small_cap.to_uppercase(), &cutoff_str).await {
        // SMB = small cap returns - market returns (approximate)
        let smb: Vec<f64> = small_ret
            .iter()
            .zip(market_returns.iter())
            .map(|(s, m)| s - m)
            .collect();
        factor_names.push("SMB (Size)".to_string());
        factor_series.push(smb);
    }

    // Try to load HML proxies (value - growth)
    let value_sym = factor_proxies
        .and_then(|fp| fp.value.as_deref())
        .unwrap_or("IWD");
    let growth_sym = factor_proxies
        .and_then(|fp| fp.growth.as_deref())
        .unwrap_or("IWF");
    if let (Ok(val_ret), Ok(grw_ret)) = (
        load_returns(cache, &value_sym.to_uppercase(), &cutoff_str).await,
        load_returns(cache, &growth_sym.to_uppercase(), &cutoff_str).await,
    ) {
        let hml: Vec<f64> = val_ret
            .iter()
            .zip(grw_ret.iter())
            .map(|(v, g)| v - g)
            .collect();
        factor_names.push("HML (Value)".to_string());
        factor_series.push(hml);
    }

    // Try to load Momentum proxy
    let mom_sym = factor_proxies
        .and_then(|fp| fp.momentum.as_deref())
        .unwrap_or("MTUM");
    if let Ok(mom_ret) = load_returns(cache, &mom_sym.to_uppercase(), &cutoff_str).await {
        let mom: Vec<f64> = mom_ret
            .iter()
            .zip(market_returns.iter())
            .map(|(m, mkt)| m - mkt)
            .collect();
        factor_names.push("Momentum".to_string());
        factor_series.push(mom);
    }

    // Align all series to minimum length
    let min_len = std::iter::once(target_returns.len())
        .chain(factor_series.iter().map(Vec::len))
        .min()
        .unwrap_or(0);

    if min_len < 30 {
        anyhow::bail!("Insufficient aligned observations: {min_len} (need at least 30)");
    }

    let y: Vec<f64> = target_returns[target_returns.len() - min_len..].to_vec();
    let factors: Vec<Vec<f64>> = factor_series
        .iter()
        .map(|f| f[f.len() - min_len..].to_vec())
        .collect();
    let n = y.len();
    let _k = factors.len() + 1; // +1 for intercept

    // Multi-factor OLS regression: y = alpha + sum(beta_i * factor_i) + epsilon
    let result = multi_factor_ols(&y, &factors);

    let alpha = result.coefficients[0];
    let alpha_annualized = alpha * TRADING_DAYS_PER_YEAR; // Annualize daily alpha

    // Build factor exposures
    let mut factor_exposures: Vec<FactorExposure> = Vec::new();
    let mut total_factor_contribution = 0.0;
    let total_mean_return = y.iter().sum::<f64>() / n as f64 * TRADING_DAYS_PER_YEAR;

    for (i, name) in factor_names.iter().enumerate() {
        let beta = result.coefficients[i + 1];
        let t_stat = result.t_stats[i + 1];
        let p_val = result.p_values[i + 1];
        let factor_mean = factors[i].iter().sum::<f64>() / factors[i].len() as f64;
        let contribution = beta * factor_mean * TRADING_DAYS_PER_YEAR; // Annualized
        total_factor_contribution += contribution;

        factor_exposures.push(FactorExposure {
            factor: name.clone(),
            beta,
            t_stat,
            p_value: p_val,
            is_significant: p_val < P_VALUE_THRESHOLD,
            return_contribution_pct: if total_mean_return.abs() > 1e-10 {
                contribution / total_mean_return * 100.0
            } else {
                0.0
            },
        });
    }

    let alpha_t_stat = result.t_stats[0];
    let alpha_p_value = result.p_values[0];
    let alpha_significant = alpha_p_value < P_VALUE_THRESHOLD;

    let pct_explained = if total_mean_return.abs() > 1e-10 {
        (total_factor_contribution / total_mean_return * 100.0).clamp(0.0, 100.0)
    } else {
        0.0
    };

    // Summary
    let sig_factors: Vec<&str> = factor_exposures
        .iter()
        .filter(|f| f.is_significant)
        .map(|f| f.factor.as_str())
        .collect();

    let summary = format!(
        "Factor attribution for {upper} ({n} obs): alpha={alpha_annualized:.4} ({}), \
         R²={:.3}, significant factors: {}.",
        if alpha_significant {
            "significant"
        } else {
            "not significant"
        },
        result.r_squared,
        if sig_factors.is_empty() {
            "none".to_string()
        } else {
            sig_factors.join(", ")
        },
    );

    let mut key_findings = vec![
        format!(
            "Alpha: {:.2}% annualized (t={:.2}, p={:.4}) — {}",
            alpha_annualized * 100.0,
            alpha_t_stat,
            alpha_p_value,
            if alpha_significant {
                "genuine alpha detected"
            } else {
                "no significant alpha (returns explained by factors)"
            }
        ),
        format!(
            "R²={:.3} — {:.1}% of return variance explained by factors",
            result.r_squared,
            result.r_squared * 100.0
        ),
    ];
    for fe in &factor_exposures {
        key_findings.push(format!(
            "{}: beta={:.3} (t={:.2}, p={:.4}) — {}",
            fe.factor,
            fe.beta,
            fe.t_stat,
            fe.p_value,
            if fe.is_significant {
                "significant exposure"
            } else {
                "not significant"
            }
        ));
    }

    let suggested_next_steps = vec![
        format!(
            "[NEXT] Call benchmark_analysis(symbol=\"{upper}\", benchmark=\"{bench_upper}\") for detailed benchmark-relative metrics"
        ),
        format!(
            "[THEN] Call rolling_metric(symbol=\"{upper}\", metric=\"beta\", benchmark=\"{bench_upper}\") to check if factor exposure is stable over time"
        ),
        "[TIP] If alpha is significant, validate with walk_forward and permutation_test before deploying".to_string(),
    ];

    Ok(FactorAttributionResponse {
        summary,
        symbol: upper,
        n_observations: n,
        alpha: alpha_annualized,
        alpha_t_stat,
        alpha_significant,
        r_squared: result.r_squared,
        adj_r_squared: result.adj_r_squared,
        factors: factor_exposures,
        pct_explained_by_factors: pct_explained,
        key_findings,
        suggested_next_steps,
    })
}

/// Result of a multi-factor OLS regression.
struct OlsResult {
    /// Coefficients: `[alpha, beta_1, beta_2, ...]`
    coefficients: Vec<f64>,
    /// T-statistics for each coefficient
    t_stats: Vec<f64>,
    /// P-values for each coefficient (approximate, using normal distribution)
    p_values: Vec<f64>,
    /// R²
    r_squared: f64,
    /// Adjusted R²
    adj_r_squared: f64,
}

/// Multi-factor OLS via normal equations: beta = (X'X)^{-1} X'y
///
/// X matrix includes a constant column (intercept).
#[allow(clippy::many_single_char_names)]
fn multi_factor_ols(y: &[f64], factors: &[Vec<f64>]) -> OlsResult {
    let n = y.len();
    let k = factors.len() + 1; // +1 for constant

    // Build X'X and X'y using nalgebra for matrix inversion
    let mut xtx = vec![0.0_f64; k * k];
    let mut xty = vec![0.0_f64; k];

    for i in 0..n {
        let mut row = vec![1.0]; // Constant
        for factor in factors {
            row.push(factor[i]);
        }

        for r in 0..k {
            for c in 0..k {
                xtx[r * k + c] += row[r] * row[c];
            }
            xty[r] += row[r] * y[i];
        }
    }

    // Invert X'X using nalgebra
    let xtx_mat = nalgebra::DMatrix::from_row_slice(k, k, &xtx);
    let xty_vec = nalgebra::DVector::from_column_slice(&xty);

    let coefficients = match xtx_mat.clone().try_inverse() {
        Some(inv) => {
            let beta = &inv * &xty_vec;
            beta.data.as_vec().clone()
        }
        None => vec![0.0; k],
    };

    // Compute residuals, SSE, SST
    let y_mean = y.iter().sum::<f64>() / n as f64;
    let mut sse = 0.0;
    let mut sst = 0.0;
    for i in 0..n {
        let mut predicted = coefficients[0]; // constant
        for (j, factor) in factors.iter().enumerate() {
            predicted += coefficients[j + 1] * factor[i];
        }
        let resid = y[i] - predicted;
        sse += resid * resid;
        sst += (y[i] - y_mean) * (y[i] - y_mean);
    }

    let r_squared = if sst > 0.0 { 1.0 - sse / sst } else { 0.0 };
    let adj_r_squared = if n > k && sst > 0.0 {
        1.0 - (1.0 - r_squared) * (n - 1) as f64 / (n - k) as f64
    } else {
        0.0
    };

    // Standard errors and t-stats
    let sigma_sq = if n > k { sse / (n - k) as f64 } else { 0.0 };

    let xtx_inv = match xtx_mat.try_inverse() {
        Some(inv) => inv,
        None => nalgebra::DMatrix::zeros(k, k),
    };

    let mut t_stats = Vec::with_capacity(k);
    let mut p_values = Vec::with_capacity(k);

    for j in 0..k {
        let var_j = sigma_sq * xtx_inv[(j, j)];
        let se_j = var_j.abs().sqrt();
        let t = if se_j > 0.0 {
            coefficients[j] / se_j
        } else {
            0.0
        };
        t_stats.push(t);

        // Approximate p-value using normal distribution (good for n > 30)
        let p = if t.abs() > 0.0 {
            2.0 * normal_cdf(-t.abs())
        } else {
            1.0
        };
        p_values.push(p);
    }

    OlsResult {
        coefficients,
        t_stats,
        p_values,
        r_squared,
        adj_r_squared,
    }
}

fn normal_cdf(x: f64) -> f64 {
    use statrs::distribution::{ContinuousCDF, Normal};
    Normal::new(0.0, 1.0).unwrap().cdf(x)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── normal_cdf ──────────────────────────────────────────────────

    #[test]
    fn normal_cdf_at_zero() {
        assert!((normal_cdf(0.0) - 0.5).abs() < 1e-6);
    }

    #[test]
    fn normal_cdf_symmetry() {
        // Φ(-x) = 1 - Φ(x)
        for &x in &[0.5, 1.0, 1.96, 2.5, 3.0] {
            let diff = (normal_cdf(-x) - (1.0 - normal_cdf(x))).abs();
            assert!(diff < 1e-6, "symmetry failed for x={x}: diff={diff}");
        }
    }

    #[test]
    fn normal_cdf_known_values() {
        // Φ(1.96) ≈ 0.975
        assert!(
            (normal_cdf(1.96) - 0.975).abs() < 0.001,
            "Φ(1.96)={}",
            normal_cdf(1.96)
        );
        // Φ(-1.96) ≈ 0.025
        assert!((normal_cdf(-1.96) - 0.025).abs() < 0.001);
        // Φ(1.0) ≈ 0.8413
        assert!((normal_cdf(1.0) - 0.8413).abs() < 0.001);
    }

    #[test]
    fn normal_cdf_extreme_values() {
        assert!(normal_cdf(-10.0) < 1e-6);
        assert!(normal_cdf(10.0) > 1.0 - 1e-6);
    }

    // ─── multi_factor_ols ────────────────────────────────────────────

    #[test]
    fn ols_single_factor_perfect_fit() {
        // y = 0.01 + 1.5 * factor
        let factor: Vec<f64> = (0..100).map(|i| f64::from(i) * 0.001).collect();
        let y: Vec<f64> = factor.iter().map(|f| 0.01 + 1.5 * f).collect();
        let result = multi_factor_ols(&y, &[factor]);
        assert!(
            (result.coefficients[0] - 0.01).abs() < 1e-8,
            "alpha={}",
            result.coefficients[0]
        );
        assert!(
            (result.coefficients[1] - 1.5).abs() < 1e-8,
            "beta={}",
            result.coefficients[1]
        );
        assert!(
            (result.r_squared - 1.0).abs() < 1e-8,
            "R²={}",
            result.r_squared
        );
    }

    #[test]
    fn ols_two_factors() {
        // y = 0.005 + 1.0 * f1 + 0.5 * f2
        let n = 200;
        let f1: Vec<f64> = (0..n)
            .map(|i| (f64::from(i) * 7.0 % 100.0) * 0.001)
            .collect();
        let f2: Vec<f64> = (0..n)
            .map(|i| (f64::from(i) * 13.0 % 100.0) * 0.001)
            .collect();
        let y: Vec<f64> = f1
            .iter()
            .zip(f2.iter())
            .map(|(a, b)| 0.005 + 1.0 * a + 0.5 * b)
            .collect();
        let result = multi_factor_ols(&y, &[f1, f2]);
        assert!(
            (result.coefficients[0] - 0.005).abs() < 1e-6,
            "alpha={}",
            result.coefficients[0]
        );
        assert!(
            (result.coefficients[1] - 1.0).abs() < 1e-6,
            "beta1={}",
            result.coefficients[1]
        );
        assert!(
            (result.coefficients[2] - 0.5).abs() < 1e-6,
            "beta2={}",
            result.coefficients[2]
        );
        assert!((result.r_squared - 1.0).abs() < 1e-6);
    }

    #[test]
    fn ols_r_squared_between_zero_and_one() {
        // Noisy data should give 0 < R² < 1
        let n = 100;
        #[allow(clippy::cast_lossless)]
        let factor: Vec<f64> = (0..n).map(|i| (i as f64).sin() * 0.01).collect();
        #[allow(clippy::cast_lossless)]
        let y: Vec<f64> = (0..n)
            .map(|i| factor[i] * 0.8 + (i as f64 * 17.0).sin() * 0.005)
            .collect();
        let result = multi_factor_ols(&y, &[factor]);
        assert!(
            result.r_squared > 0.0 && result.r_squared < 1.0,
            "R²={}",
            result.r_squared
        );
    }

    #[test]
    fn ols_t_stats_and_p_values_coherent() {
        let n = 100;
        let factor: Vec<f64> = (0..n).map(|i| f64::from(i) * 0.001).collect();
        let y: Vec<f64> = factor.iter().map(|f| 0.01 + 2.0 * f).collect();
        let result = multi_factor_ols(&y, &[factor]);
        // With perfect fit, t-stats should be very large
        assert!(
            result.t_stats[1].abs() > 10.0,
            "t_stat={}",
            result.t_stats[1]
        );
        // p-value for significant coefficient should be near 0
        assert!(result.p_values[1] < 0.001, "p_value={}", result.p_values[1]);
    }

    #[test]
    fn ols_adj_r_squared_lte_r_squared() {
        let n = 50;
        let f1: Vec<f64> = (0..n).map(|i| f64::from(i).sin() * 0.01).collect();
        let f2: Vec<f64> = (0..n).map(|i| f64::from(i).cos() * 0.01).collect();
        let y: Vec<f64> = f1.iter().map(|f| f * 0.5 + 0.001).collect();
        let result = multi_factor_ols(&y, &[f1, f2]);
        assert!(
            result.adj_r_squared <= result.r_squared + 1e-10,
            "adj_R²={} > R²={}",
            result.adj_r_squared,
            result.r_squared
        );
    }
}
