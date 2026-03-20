//! Portfolio optimization tool: risk parity, minimum variance, and maximum Sharpe.
//!
//! Takes multiple symbols, computes the covariance matrix from historical returns,
//! and finds optimal portfolio weights under three different objective functions.

use anyhow::{Context, Result};
use std::sync::Arc;

use crate::data::cache::CachedStore;
use crate::tools::response_types::{
    AssetStats, CorrelationEntry, OptimalWeight, OptimizationResult, PortfolioOptimizeResponse,
};

/// Default methods to run if none specified.
const DEFAULT_METHODS: &[&str] = &["risk_parity", "min_variance", "max_sharpe"];

/// Execute portfolio optimization.
#[allow(clippy::too_many_lines)]
pub async fn execute(
    cache: &Arc<CachedStore>,
    symbols: &[String],
    methods: Option<&[String]>,
    years: u32,
    risk_free_rate: f64,
) -> Result<PortfolioOptimizeResponse> {
    let cutoff = chrono::Utc::now().date_naive() - chrono::Duration::days(i64::from(years) * 365);
    let cutoff_str = cutoff.format("%Y-%m-%d").to_string();

    // Load returns for all symbols
    let mut all_returns: Vec<Vec<f64>> = Vec::new();
    let mut upper_symbols: Vec<String> = Vec::new();

    for sym in symbols {
        let upper = sym.to_uppercase();
        let resp = crate::tools::raw_prices::load_and_execute(
            cache,
            &upper,
            Some(&cutoff_str),
            None,
            None,
            crate::engine::types::Interval::Daily,
            None,
        )
        .await
        .context(format!("Failed to load OHLCV data for {upper}"))?;

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

        if returns.len() < 30 {
            anyhow::bail!(
                "Insufficient data for {upper}: {} observations (need 30)",
                returns.len()
            );
        }

        all_returns.push(returns);
        upper_symbols.push(upper);
    }

    let n_assets = upper_symbols.len();

    // Align all return series to minimum length (from the end)
    let min_len = all_returns.iter().map(Vec::len).min().unwrap_or(0);
    if min_len < 30 {
        anyhow::bail!("Insufficient aligned observations: {min_len}");
    }

    let aligned: Vec<Vec<f64>> = all_returns
        .iter()
        .map(|r| r[r.len() - min_len..].to_vec())
        .collect();

    // Compute mean returns and covariance matrix
    let means: Vec<f64> = aligned
        .iter()
        .map(|r| r.iter().sum::<f64>() / r.len() as f64)
        .collect();

    let annualized_returns: Vec<f64> = means.iter().map(|m| m * 252.0).collect();
    let annualized_vols: Vec<f64> = aligned
        .iter()
        .map(|r| {
            let m = r.iter().sum::<f64>() / r.len() as f64;
            let var = r.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (r.len() - 1) as f64;
            var.sqrt() * 252.0_f64.sqrt()
        })
        .collect();

    // Covariance matrix (annualized)
    let mut cov_matrix = vec![vec![0.0_f64; n_assets]; n_assets];
    for i in 0..n_assets {
        for j in 0..n_assets {
            let cov: f64 = aligned[i]
                .iter()
                .zip(aligned[j].iter())
                .map(|(a, b)| (a - means[i]) * (b - means[j]))
                .sum::<f64>()
                / (min_len - 1) as f64;
            cov_matrix[i][j] = cov * 252.0; // Annualize
        }
    }

    // Correlation matrix
    let mut correlation_entries = Vec::new();
    for i in 0..n_assets {
        for j in (i + 1)..n_assets {
            let corr = if annualized_vols[i] > 0.0 && annualized_vols[j] > 0.0 {
                cov_matrix[i][j] / (annualized_vols[i] * annualized_vols[j])
            } else {
                0.0
            };
            correlation_entries.push(CorrelationEntry {
                strategy_a: upper_symbols[i].clone(),
                strategy_b: upper_symbols[j].clone(),
                correlation: corr,
            });
        }
    }

    // Asset statistics
    let asset_stats: Vec<AssetStats> = (0..n_assets)
        .map(|i| {
            let sharpe = if annualized_vols[i] > 0.0 {
                (annualized_returns[i] - risk_free_rate) / annualized_vols[i]
            } else {
                0.0
            };
            AssetStats {
                symbol: upper_symbols[i].clone(),
                annualized_return: annualized_returns[i],
                annualized_volatility: annualized_vols[i],
                sharpe,
            }
        })
        .collect();

    // Run requested optimization methods
    let method_list: Vec<&str> = methods.map_or_else(
        || DEFAULT_METHODS.to_vec(),
        |m| m.iter().map(String::as_str).collect(),
    );

    let mut optimizations = Vec::new();

    for method in &method_list {
        let weights = match *method {
            "risk_parity" => risk_parity_weights(&cov_matrix),
            "min_variance" => min_variance_weights(&cov_matrix),
            "max_sharpe" => max_sharpe_weights(&annualized_returns, &cov_matrix, risk_free_rate),
            other => {
                anyhow::bail!(
                    "Unknown optimization method: '{other}'. Valid methods: risk_parity, min_variance, max_sharpe"
                );
            }
        };

        // Portfolio return and vol
        let port_return: f64 = weights
            .iter()
            .zip(annualized_returns.iter())
            .map(|(w, r)| w * r)
            .sum();
        let port_vol = portfolio_volatility(&weights, &cov_matrix);
        let port_sharpe = if port_vol > 0.0 {
            (port_return - risk_free_rate) / port_vol
        } else {
            0.0
        };

        let optimal_weights: Vec<OptimalWeight> = weights
            .iter()
            .enumerate()
            .map(|(i, &w)| OptimalWeight {
                symbol: upper_symbols[i].clone(),
                weight: w,
                weight_pct: w * 100.0,
            })
            .collect();

        optimizations.push(OptimizationResult {
            method: method.to_string(),
            weights: optimal_weights,
            expected_return: port_return,
            expected_volatility: port_vol,
            expected_sharpe: port_sharpe,
        });
    }

    // Summary
    let best = optimizations.iter().max_by(|a, b| {
        a.expected_sharpe
            .partial_cmp(&b.expected_sharpe)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let summary = format!(
        "Portfolio optimization for {} assets over {} observations. \
         Best Sharpe: {:.3} ({} method).",
        n_assets,
        min_len,
        best.map_or(0.0, |b| b.expected_sharpe),
        best.map_or("N/A", |b| b.method.as_str()),
    );

    let mut key_findings = Vec::new();
    for opt in &optimizations {
        let top_alloc = opt
            .weights
            .iter()
            .max_by(|a, b| {
                a.weight
                    .partial_cmp(&b.weight)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|w| format!("{}: {:.1}%", w.symbol, w.weight_pct))
            .unwrap_or_default();
        key_findings.push(format!(
            "{}: Sharpe={:.3}, return={:.2}%, vol={:.2}%, top alloc={}",
            opt.method,
            opt.expected_sharpe,
            opt.expected_return * 100.0,
            opt.expected_volatility * 100.0,
            top_alloc,
        ));
    }

    let sym_list = upper_symbols.join(", ");
    let suggested_next_steps = vec![
        format!(
            "[NEXT] Call portfolio_backtest with the optimal weights to validate out-of-sample"
        ),
        format!(
            "[THEN] Call correlate to investigate pairwise relationships between {sym_list}"
        ),
        "[TIP] Risk parity is more robust out-of-sample; max Sharpe can be sensitive to estimation error"
            .to_string(),
    ];

    Ok(PortfolioOptimizeResponse {
        summary,
        symbols: upper_symbols,
        n_observations: min_len,
        correlation_matrix: correlation_entries,
        asset_stats,
        optimizations,
        key_findings,
        suggested_next_steps,
    })
}

/// Compute portfolio volatility given weights and covariance matrix.
fn portfolio_volatility(weights: &[f64], cov: &[Vec<f64>]) -> f64 {
    let n = weights.len();
    let mut variance = 0.0;
    for i in 0..n {
        for j in 0..n {
            variance += weights[i] * weights[j] * cov[i][j];
        }
    }
    variance.max(0.0).sqrt()
}

/// Risk parity: weights inversely proportional to asset volatility,
/// then iteratively adjusted so each asset contributes equal risk.
///
/// Uses the inverse-volatility heuristic as a closed-form approximation
/// (exact risk parity requires iterative solving).
fn risk_parity_weights(cov: &[Vec<f64>]) -> Vec<f64> {
    let n = cov.len();
    // Inverse-volatility weighting
    let vols: Vec<f64> = (0..n).map(|i| cov[i][i].max(0.0).sqrt()).collect();
    let inv_vols: Vec<f64> = vols
        .iter()
        .map(|v| if *v > 1e-10 { 1.0 / v } else { 0.0 })
        .collect();
    let total: f64 = inv_vols.iter().sum();
    if total > 0.0 {
        inv_vols.iter().map(|v| v / total).collect()
    } else {
        vec![1.0 / n as f64; n]
    }
}

/// Minimum variance portfolio using the analytical solution:
/// w* = (Σ^{-1} * 1) / (1' * Σ^{-1} * 1)
fn min_variance_weights(cov: &[Vec<f64>]) -> Vec<f64> {
    let n = cov.len();
    let cov_mat = nalgebra::DMatrix::from_fn(n, n, |i, j| cov[i][j]);

    match cov_mat.try_inverse() {
        Some(inv) => {
            let ones = nalgebra::DVector::from_element(n, 1.0);
            let inv_ones = &inv * &ones;
            let denom: f64 = ones.dot(&inv_ones);
            if denom > 1e-15 {
                let w = &inv_ones / denom;
                // Clamp negative weights to 0 (long-only) and renormalize
                let raw: Vec<f64> = w.iter().map(|&x| x.max(0.0)).collect();
                let total: f64 = raw.iter().sum();
                if total > 0.0 {
                    raw.iter().map(|x| x / total).collect()
                } else {
                    vec![1.0 / n as f64; n]
                }
            } else {
                vec![1.0 / n as f64; n]
            }
        }
        None => vec![1.0 / n as f64; n],
    }
}

/// Maximum Sharpe ratio portfolio (tangency portfolio):
/// `w* = (Σ^{-1} * (μ - r_f)) / (1' * Σ^{-1} * (μ - r_f))`
fn max_sharpe_weights(expected_returns: &[f64], cov: &[Vec<f64>], risk_free: f64) -> Vec<f64> {
    let n = cov.len();
    let excess: Vec<f64> = expected_returns.iter().map(|r| r - risk_free).collect();
    let excess_vec = nalgebra::DVector::from_column_slice(&excess);
    let cov_mat = nalgebra::DMatrix::from_fn(n, n, |i, j| cov[i][j]);

    match cov_mat.try_inverse() {
        Some(inv) => {
            let inv_excess = &inv * &excess_vec;
            let ones = nalgebra::DVector::from_element(n, 1.0);
            let denom: f64 = ones.dot(&inv_excess);
            if denom.abs() > 1e-15 {
                let w = &inv_excess / denom;
                // Clamp to long-only
                let raw: Vec<f64> = w.iter().map(|&x| x.max(0.0)).collect();
                let total: f64 = raw.iter().sum();
                if total > 0.0 {
                    raw.iter().map(|x| x / total).collect()
                } else {
                    vec![1.0 / n as f64; n]
                }
            } else {
                vec![1.0 / n as f64; n]
            }
        }
        None => vec![1.0 / n as f64; n],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── portfolio_volatility ────────────────────────────────────────

    #[test]
    fn portfolio_vol_single_asset() {
        let cov = vec![vec![0.04]]; // vol = 0.2
        let weights = vec![1.0];
        let vol = portfolio_volatility(&weights, &cov);
        assert!((vol - 0.2).abs() < 1e-10, "vol={vol}");
    }

    #[test]
    fn portfolio_vol_equal_weight_uncorrelated() {
        // Two uncorrelated assets with var=0.04 each, equal weight
        // port_var = 0.5^2 * 0.04 + 0.5^2 * 0.04 = 0.02
        let cov = vec![vec![0.04, 0.0], vec![0.0, 0.04]];
        let weights = vec![0.5, 0.5];
        let vol = portfolio_volatility(&weights, &cov);
        let expected = 0.02_f64.sqrt();
        assert!(
            (vol - expected).abs() < 1e-10,
            "vol={vol}, expected={expected}"
        );
    }

    #[test]
    fn portfolio_vol_perfectly_correlated() {
        // Two assets perfectly correlated, same vol (0.2)
        let cov = vec![vec![0.04, 0.04], vec![0.04, 0.04]];
        let weights = vec![0.5, 0.5];
        let vol = portfolio_volatility(&weights, &cov);
        assert!((vol - 0.2).abs() < 1e-10, "vol={vol}");
    }

    // ─── risk_parity_weights ─────────────────────────────────────────

    #[test]
    fn risk_parity_equal_vol_equal_weights() {
        let cov = vec![vec![0.04, 0.0], vec![0.0, 0.04]];
        let w = risk_parity_weights(&cov);
        assert!((w[0] - 0.5).abs() < 1e-10, "w0={}", w[0]);
        assert!((w[1] - 0.5).abs() < 1e-10, "w1={}", w[1]);
    }

    #[test]
    fn risk_parity_unequal_vol() {
        // Asset 1: vol=0.2, Asset 2: vol=0.4
        // inv_vol: [5, 2.5], total=7.5 → weights [2/3, 1/3]
        let cov = vec![vec![0.04, 0.0], vec![0.0, 0.16]];
        let w = risk_parity_weights(&cov);
        assert!((w[0] - 2.0 / 3.0).abs() < 1e-10, "w0={}", w[0]);
        assert!((w[1] - 1.0 / 3.0).abs() < 1e-10, "w1={}", w[1]);
    }

    #[test]
    fn risk_parity_sums_to_one() {
        let cov = vec![
            vec![0.04, 0.01, 0.005],
            vec![0.01, 0.09, 0.02],
            vec![0.005, 0.02, 0.16],
        ];
        let w = risk_parity_weights(&cov);
        let sum: f64 = w.iter().sum();
        assert!((sum - 1.0).abs() < 1e-10, "sum={sum}");
    }

    // ─── min_variance_weights ────────────────────────────────────────

    #[test]
    fn min_variance_uncorrelated_equal_vol() {
        let cov = vec![vec![0.04, 0.0], vec![0.0, 0.04]];
        let w = min_variance_weights(&cov);
        assert!((w[0] - 0.5).abs() < 1e-6, "w0={}", w[0]);
        assert!((w[1] - 0.5).abs() < 1e-6, "w1={}", w[1]);
    }

    #[test]
    fn min_variance_sums_to_one() {
        let cov = vec![
            vec![0.04, 0.01, 0.005],
            vec![0.01, 0.09, 0.02],
            vec![0.005, 0.02, 0.16],
        ];
        let w = min_variance_weights(&cov);
        let sum: f64 = w.iter().sum();
        assert!((sum - 1.0).abs() < 1e-6, "sum={sum}");
    }

    #[test]
    fn min_variance_no_negative_weights() {
        let cov = vec![
            vec![0.04, 0.01, 0.005],
            vec![0.01, 0.09, 0.02],
            vec![0.005, 0.02, 0.16],
        ];
        let w = min_variance_weights(&cov);
        for (i, &wi) in w.iter().enumerate() {
            assert!(wi >= 0.0, "w[{i}]={wi} is negative");
        }
    }

    #[test]
    fn min_variance_prefers_low_vol_asset() {
        let cov = vec![vec![0.01, 0.0], vec![0.0, 0.16]];
        let w = min_variance_weights(&cov);
        assert!(w[0] > w[1], "w0={} should be > w1={}", w[0], w[1]);
    }

    // ─── max_sharpe_weights ──────────────────────────────────────────

    #[test]
    fn max_sharpe_sums_to_one() {
        let returns = vec![0.10, 0.15, 0.08];
        let cov = vec![
            vec![0.04, 0.01, 0.005],
            vec![0.01, 0.09, 0.02],
            vec![0.005, 0.02, 0.16],
        ];
        let w = max_sharpe_weights(&returns, &cov, 0.02);
        let sum: f64 = w.iter().sum();
        assert!((sum - 1.0).abs() < 1e-6, "sum={sum}");
    }

    #[test]
    fn max_sharpe_no_negative_weights() {
        let returns = vec![0.10, 0.15];
        let cov = vec![vec![0.04, 0.01], vec![0.01, 0.09]];
        let w = max_sharpe_weights(&returns, &cov, 0.02);
        for (i, &wi) in w.iter().enumerate() {
            assert!(wi >= 0.0, "w[{i}]={wi}");
        }
    }

    #[test]
    fn max_sharpe_favors_high_sharpe_asset() {
        // Asset 1: return=0.20, vol=0.20 → Sharpe=0.90
        // Asset 2: return=0.10, vol=0.30 → Sharpe=0.27
        let returns = vec![0.20, 0.10];
        let cov = vec![vec![0.04, 0.005], vec![0.005, 0.09]];
        let w = max_sharpe_weights(&returns, &cov, 0.02);
        assert!(w[0] > w[1], "w0={} should be > w1={}", w[0], w[1]);
    }
}
