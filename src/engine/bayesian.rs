//! Bayesian optimization using Gaussian Process with Expected Improvement.

use std::collections::HashMap;
use std::f64::consts::{FRAC_1_SQRT_2, PI};
use std::time::Instant;

use anyhow::Result;
use rand::Rng;
use serde_json::Value;

use crate::engine::sweep::{extract_objective, sort_by_objective};
use crate::scripting::engine::{run_script_backtest, DataLoader};
use crate::tools::response_types::sweep::{SweepResponse, SweepResult};

/// Configuration for Bayesian optimization.
pub struct BayesianConfig {
    pub script_source: String,
    pub base_params: HashMap<String, Value>,
    /// Each entry: (`param_name`, min, max).
    pub continuous_params: Vec<(String, f64, f64)>,
    pub max_evaluations: usize,
    pub initial_samples: usize,
    pub objective: String,
}

/// Run Bayesian optimization with GP-EI.
#[allow(clippy::too_many_lines)]
pub async fn run_bayesian(
    config: &BayesianConfig,
    data_loader: &dyn DataLoader,
) -> Result<SweepResponse> {
    let start = Instant::now();
    let dim = config.continuous_params.len();
    let mut xs: Vec<Vec<f64>> = Vec::new();
    let mut ys: Vec<f64> = Vec::new();
    let mut results: Vec<SweepResult> = Vec::new();
    let mut failed = 0usize;
    let mut convergence_trace: Vec<f64> = Vec::new();
    let mut best_so_far = f64::NEG_INFINITY;

    // Phase 1: Random initial samples
    for _ in 0..config.initial_samples {
        let x = random_point(dim);
        let swept = decode_params(&x, &config.continuous_params);
        if let Ok(result) = evaluate(
            &config.script_source,
            &config.base_params,
            &swept,
            data_loader,
        )
        .await
        {
            let obj = extract_objective(&result, &config.objective);
            if obj.is_finite() {
                xs.push(x);
                ys.push(obj);
            }
            if obj.is_finite() && obj > best_so_far {
                best_so_far = obj;
            }
            convergence_trace.push(if best_so_far.is_finite() {
                best_so_far
            } else {
                0.0
            });
            results.push(result);
        } else {
            failed += 1;
            convergence_trace.push(if best_so_far.is_finite() {
                best_so_far
            } else {
                0.0
            });
        }
    }

    // Phase 2: GP-EI loop
    let remaining = config
        .max_evaluations
        .saturating_sub(config.initial_samples);
    for _ in 0..remaining {
        if xs.len() < 2 {
            // Not enough data for GP, do another random sample
            let x = random_point(dim);
            let swept = decode_params(&x, &config.continuous_params);
            if let Ok(result) = evaluate(
                &config.script_source,
                &config.base_params,
                &swept,
                data_loader,
            )
            .await
            {
                let obj = extract_objective(&result, &config.objective);
                if obj.is_finite() {
                    xs.push(x);
                    ys.push(obj);
                }
                if obj.is_finite() && obj > best_so_far {
                    best_so_far = obj;
                }
                convergence_trace.push(if best_so_far.is_finite() {
                    best_so_far
                } else {
                    0.0
                });
                results.push(result);
            } else {
                failed += 1;
                convergence_trace.push(if best_so_far.is_finite() {
                    best_so_far
                } else {
                    0.0
                });
            }
            continue;
        }

        // Fit GP and find next point via EI
        let gp = GaussianProcess::fit(&xs, &ys);
        let next_x = maximize_ei(&gp, best_so_far, dim);
        let swept = decode_params(&next_x, &config.continuous_params);

        if let Ok(result) = evaluate(
            &config.script_source,
            &config.base_params,
            &swept,
            data_loader,
        )
        .await
        {
            let obj = extract_objective(&result, &config.objective);
            if obj.is_finite() {
                xs.push(next_x);
                ys.push(obj);
            }
            if obj.is_finite() && obj > best_so_far {
                best_so_far = obj;
            }
            convergence_trace.push(if best_so_far.is_finite() {
                best_so_far
            } else {
                0.0
            });
            results.push(result);
        } else {
            failed += 1;
            convergence_trace.push(if best_so_far.is_finite() {
                best_so_far
            } else {
                0.0
            });
        }
    }

    let total = config.max_evaluations;
    sort_by_objective(&mut results, &config.objective);
    for (i, r) in results.iter_mut().enumerate() {
        r.rank = i + 1;
    }

    Ok(SweepResponse {
        mode: "bayesian".to_string(),
        objective: config.objective.clone(),
        combinations_total: total,
        combinations_run: results.len(),
        combinations_failed: failed,
        best_result: results.first().cloned(),
        ranked_results: results,
        dimension_sensitivity: HashMap::new(),
        convergence_trace: Some(convergence_trace),
        execution_time_ms: start.elapsed().as_millis() as u64,
    })
}

// ---------------------------------------------------------------------------
// Gaussian Process
// ---------------------------------------------------------------------------

/// Gaussian Process with RBF kernel.
struct GaussianProcess {
    x_train: Vec<Vec<f64>>,
    alpha: Vec<f64>,
    length_scale: f64,
    signal_var: f64,
    noise_var: f64,
    y_mean: f64,
}

impl GaussianProcess {
    /// Fit GP to training data. Returns a trained GP.
    fn fit(xs: &[Vec<f64>], ys: &[f64]) -> Self {
        let n = xs.len();
        let y_mean = ys.iter().sum::<f64>() / n as f64;
        let y_centered: Vec<f64> = ys.iter().map(|y| y - y_mean).collect();

        // Estimate signal variance from data
        let signal_var = if n > 1 {
            let var = y_centered.iter().map(|y| y * y).sum::<f64>() / (n - 1) as f64;
            var.max(1e-6)
        } else {
            1.0
        };

        // Heuristic length scale: median pairwise distance
        let length_scale = Self::median_distance(xs).max(1e-4);
        let noise_var = signal_var * 0.01; // 1% noise

        // Build kernel matrix K + noise*I
        let mut k_mat = vec![0.0; n * n];
        for i in 0..n {
            for j in 0..n {
                k_mat[i * n + j] = rbf_kernel(&xs[i], &xs[j], length_scale, signal_var);
                if i == j {
                    k_mat[i * n + j] += noise_var;
                }
            }
        }

        // Solve K * alpha = y_centered via Cholesky
        let alpha = cholesky_solve(&k_mat, &y_centered, n);

        Self {
            x_train: xs.to_vec(),
            alpha,
            length_scale,
            signal_var,
            noise_var,
            y_mean,
        }
    }

    /// Predict mean and variance at a test point.
    fn predict(&self, x: &[f64]) -> (f64, f64) {
        let n = self.x_train.len();
        let k_star: Vec<f64> = (0..n)
            .map(|i| rbf_kernel(x, &self.x_train[i], self.length_scale, self.signal_var))
            .collect();

        // Mean = k* . alpha + y_mean
        let mean = k_star
            .iter()
            .zip(&self.alpha)
            .map(|(k, a)| k * a)
            .sum::<f64>()
            + self.y_mean;

        // Variance = k(x,x) - k*^T K^{-1} k*
        // We approximate K^{-1} k* using the stored Cholesky factor approach
        // For simplicity, use the diagonal approximation:
        let k_xx = self.signal_var + self.noise_var;

        // Build K matrix and solve K * v = k_star for variance
        let mut k_mat = vec![0.0; n * n];
        for i in 0..n {
            for j in 0..n {
                k_mat[i * n + j] = rbf_kernel(
                    &self.x_train[i],
                    &self.x_train[j],
                    self.length_scale,
                    self.signal_var,
                );
                if i == j {
                    k_mat[i * n + j] += self.noise_var;
                }
            }
        }
        let v = cholesky_solve(&k_mat, &k_star, n);
        let var_reduction: f64 = k_star.iter().zip(&v).map(|(k, vi)| k * vi).sum();
        let var = (k_xx - var_reduction).max(1e-10);

        (mean, var)
    }

    fn median_distance(xs: &[Vec<f64>]) -> f64 {
        let n = xs.len();
        if n < 2 {
            return 1.0;
        }
        let mut dists = Vec::new();
        for i in 0..n {
            for j in (i + 1)..n {
                let d: f64 = xs[i]
                    .iter()
                    .zip(&xs[j])
                    .map(|(a, b)| (a - b).powi(2))
                    .sum::<f64>()
                    .sqrt();
                dists.push(d);
            }
        }
        dists.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        dists[dists.len() / 2]
    }
}

/// RBF (squared exponential) kernel.
fn rbf_kernel(x1: &[f64], x2: &[f64], length_scale: f64, signal_var: f64) -> f64 {
    let sq_dist: f64 = x1.iter().zip(x2).map(|(a, b)| (a - b).powi(2)).sum();
    signal_var * (-0.5 * sq_dist / (length_scale * length_scale)).exp()
}

/// Solve K * x = b via Cholesky factorization, with fallback to diagonal solve.
#[allow(clippy::many_single_char_names)]
fn cholesky_solve(k: &[f64], b: &[f64], n: usize) -> Vec<f64> {
    // Attempt Cholesky decomposition K = L * L^T
    if let Some(l) = cholesky_decompose(k, n) {
        // Forward solve L * y = b
        let mut y = vec![0.0; n];
        for i in 0..n {
            let mut s = b[i];
            for j in 0..i {
                s -= l[i * n + j] * y[j];
            }
            y[i] = s / l[i * n + i];
        }
        // Backward solve L^T * x = y
        let mut x = vec![0.0; n];
        for i in (0..n).rev() {
            let mut s = y[i];
            for j in (i + 1)..n {
                s -= l[j * n + i] * x[j];
            }
            x[i] = s / l[i * n + i];
        }
        x
    } else {
        // Fallback: diagonal solve
        (0..n)
            .map(|i| {
                let diag = k[i * n + i];
                if diag.abs() > 1e-12 {
                    b[i] / diag
                } else {
                    0.0
                }
            })
            .collect()
    }
}

/// Cholesky decomposition. Returns L such that K = L * L^T, or None if not PD.
fn cholesky_decompose(k: &[f64], n: usize) -> Option<Vec<f64>> {
    let mut l = vec![0.0; n * n];
    for i in 0..n {
        for j in 0..=i {
            let s: f64 = (0..j).map(|m| l[i * n + m] * l[j * n + m]).sum();
            if i == j {
                let diag = k[i * n + i] - s;
                if diag <= 0.0 {
                    return None;
                }
                l[i * n + j] = diag.sqrt();
            } else {
                let denom = l[j * n + j];
                if denom.abs() < 1e-15 {
                    return None;
                }
                l[i * n + j] = (k[i * n + j] - s) / denom;
            }
        }
    }
    Some(l)
}

// ---------------------------------------------------------------------------
// Expected Improvement
// ---------------------------------------------------------------------------

/// Compute Expected Improvement at a point given GP predictions.
fn expected_improvement(mean: f64, var: f64, best_y: f64) -> f64 {
    let sigma = var.sqrt();
    if sigma < 1e-12 {
        return 0.0;
    }
    let z = (mean - best_y) / sigma;
    let ei = (mean - best_y) * normal_cdf(z) + sigma * normal_pdf(z);
    ei.max(0.0)
}

/// Find the point that maximizes EI by random candidate sampling (1000 candidates).
fn maximize_ei(gp: &GaussianProcess, best_y: f64, dim: usize) -> Vec<f64> {
    let n_candidates = 1000;
    let mut best_ei = f64::NEG_INFINITY;
    let mut best_x = random_point(dim);

    for _ in 0..n_candidates {
        let x = random_point(dim);
        let (mean, var) = gp.predict(&x);
        let ei = expected_improvement(mean, var, best_y);
        if ei > best_ei {
            best_ei = ei;
            best_x = x;
        }
    }

    best_x
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Decode normalized [0,1]^dim values to actual parameter values.
pub fn decode_params(x: &[f64], continuous: &[(String, f64, f64)]) -> HashMap<String, Value> {
    x.iter()
        .zip(continuous)
        .map(|(xi, (name, min, max))| {
            let val = min + xi * (max - min);
            let rounded = (val * 10_000.0).round() / 10_000.0;
            // Emit whole numbers as integers so Rhai string concat works
            let json_val = if rounded.fract() == 0.0 && rounded.abs() < i64::MAX as f64 {
                serde_json::json!(rounded as i64)
            } else {
                serde_json::json!(rounded)
            };
            (name.clone(), json_val)
        })
        .collect()
}

/// Generate a random point in [0,1]^dim.
pub fn random_point(dim: usize) -> Vec<f64> {
    let mut rng = rand::rng();
    (0..dim).map(|_| rng.random::<f64>()).collect()
}

/// Evaluate a single parameter combination.
async fn evaluate(
    script_source: &str,
    base_params: &HashMap<String, Value>,
    swept_params: &HashMap<String, Value>,
    data_loader: &dyn DataLoader,
) -> Result<SweepResult> {
    let mut run_params = base_params.clone();
    run_params.extend(swept_params.clone());

    let bt = run_script_backtest(script_source, &run_params, data_loader).await?;
    let m = &bt.result.metrics;

    Ok(SweepResult {
        rank: 0,
        params: swept_params.clone(),
        sharpe: m.sharpe,
        sortino: m.sortino,
        pnl: bt.result.total_pnl,
        trades: bt.result.trade_count,
        win_rate: m.win_rate,
        max_dd: m.max_drawdown,
        profit_factor: m.profit_factor,
        calmar: m.calmar,
    })
}

// ---------------------------------------------------------------------------
// Math utilities
// ---------------------------------------------------------------------------

/// Standard normal PDF.
fn normal_pdf(x: f64) -> f64 {
    (-0.5 * x * x).exp() / (2.0 * PI).sqrt()
}

/// Standard normal CDF using erf approximation.
fn normal_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x * FRAC_1_SQRT_2))
}

/// Error function approximation (Abramowitz & Stegun 7.1.26).
/// Maximum error: 1.5e-7.
fn erf(x: f64) -> f64 {
    let sign = if x >= 0.0 { 1.0 } else { -1.0 };
    let x = x.abs();
    let t = 1.0 / (1.0 + 0.327_591_1 * x);
    let t2 = t * t;
    let t3 = t2 * t;
    let t4 = t3 * t;
    let t5 = t4 * t;
    let poly = 0.254_829_592 * t - 0.284_496_736 * t2 + 1.421_413_741 * t3 - 1.453_152_027 * t4
        + 1.061_405_429 * t5;
    sign * (1.0 - poly * (-x * x).exp())
}
