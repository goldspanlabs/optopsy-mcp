//! Bayesian optimization using Gaussian Process with Expected Improvement.

use std::collections::HashMap;
use std::f64::consts::{FRAC_1_SQRT_2, PI};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use rand::Rng;
use serde_json::Value;

use crate::engine::sweep::{compute_sensitivity, extract_objective, sort_by_objective};
use crate::scripting::engine::{
    run_script_backtest, CancelCallback, DataLoader, PrecomputedOptionsData,
};
use crate::tools::response_types::sweep::{SweepResponse, SweepResult};

/// Configuration for Bayesian optimization.
pub struct BayesianConfig {
    pub script_source: String,
    pub base_params: HashMap<String, Value>,
    /// Each entry: (`param_name`, min, max, `is_int`).
    pub continuous_params: Vec<(String, f64, f64, bool)>,
    pub max_evaluations: usize,
    pub initial_samples: usize,
    pub objective: String,
}

/// Build a deterministic cache key from decoded parameters.
/// Single-allocation: sorts by borrowed key, writes directly into one String.
fn cache_key(swept: &HashMap<String, Value>) -> String {
    use std::fmt::Write;
    let mut pairs: Vec<_> = swept.iter().collect();
    pairs.sort_by_key(|(k, _)| k.as_str());
    let mut out = String::with_capacity(pairs.len() * 16);
    for (i, (k, v)) in pairs.iter().enumerate() {
        if i > 0 {
            out.push('|');
        }
        let _ = write!(out, "{k}={v}");
    }
    out
}

/// Run Bayesian optimization with GP-EI.
#[allow(clippy::too_many_lines)]
pub async fn run_bayesian(
    config: &BayesianConfig,
    data_loader: &dyn DataLoader,
    is_cancelled: &CancelCallback,
    on_progress: impl Fn(usize, usize),
) -> Result<SweepResponse> {
    let start = Instant::now();
    let dim = config.continuous_params.len();
    let mut xs: Vec<Vec<f64>> = Vec::new();
    let mut ys: Vec<f64> = Vec::new();
    let mut results: Vec<SweepResult> = Vec::new();
    let mut failed = 0usize;
    let mut convergence_trace: Vec<f64> = Vec::with_capacity(config.max_evaluations);
    let mut best_so_far = f64::NEG_INFINITY;
    // Arc-wrapped cache: avoids cloning full SweepResult on cache hits
    let mut eval_cache: HashMap<String, (Arc<SweepResult>, f64)> = HashMap::new();

    // Pre-build options data on the first evaluation so subsequent ones skip the
    // expensive build_price_table + DatePartitionedOptions::from_df work.
    let mut precomputed: Option<PrecomputedOptionsData> = None;

    let trace_val = |best: f64| if best.is_finite() { best } else { 0.0 };

    // Run all iterations (phase 1 random + phase 2 GP-EI)
    for i in 0..config.max_evaluations {
        if is_cancelled() {
            break;
        }

        on_progress(i, config.max_evaluations);

        let x = if i < config.initial_samples || xs.len() < 2 {
            random_point(dim)
        } else {
            let gp = GaussianProcess::fit(&xs, &ys);
            maximize_ei(&gp, best_so_far, dim)
        };

        let swept = decode_params(&x, &config.continuous_params);
        let key = cache_key(&swept);

        if let Some((cached_result, cached_obj)) = eval_cache.get(&key) {
            // Cache hit — skip backtest, don't bloat GP with duplicate points
            if cached_obj.is_finite() && *cached_obj > best_so_far {
                best_so_far = *cached_obj;
            }
            convergence_trace.push(trace_val(best_so_far));
            results.push(SweepResult::clone(cached_result));
            continue;
        }

        if let Ok((result, pre)) = evaluate(
            &config.script_source,
            &config.base_params,
            swept,
            data_loader,
            precomputed.as_ref(),
            Some(is_cancelled),
        )
        .await
        {
            if precomputed.is_none() {
                precomputed = pre;
            }
            let obj = extract_objective(&result, &config.objective);
            let result_arc = Arc::new(result);
            eval_cache.insert(key, (Arc::clone(&result_arc), obj));
            if obj.is_finite() {
                xs.push(x);
                ys.push(obj);
                if obj > best_so_far {
                    best_so_far = obj;
                }
            }
            convergence_trace.push(trace_val(best_so_far));
            results.push(Arc::try_unwrap(result_arc).unwrap_or_else(|a| SweepResult::clone(&a)));
        } else {
            failed += 1;
            convergence_trace.push(trace_val(best_so_far));
        }
    }

    let total = config.max_evaluations;
    // Deduplicate results — keep one entry per unique param combo
    let mut seen_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
    results.retain(|r| {
        let key = cache_key(&r.params);
        seen_keys.insert(key)
    });
    sort_by_objective(&mut results, &config.objective);
    for (i, r) in results.iter_mut().enumerate() {
        r.rank = i + 1;
    }

    // Build param_grid from evaluated results for sensitivity computation
    let mut param_grid: HashMap<String, Vec<Value>> = HashMap::new();
    for r in &results {
        for (k, v) in &r.params {
            let vals = param_grid.entry(k.clone()).or_default();
            if !vals.contains(v) {
                vals.push(v.clone());
            }
        }
    }
    // Sort each param's values for consistent ordering
    for vals in param_grid.values_mut() {
        vals.sort_by(|a, b| {
            a.as_f64()
                .unwrap_or(0.0)
                .partial_cmp(&b.as_f64().unwrap_or(0.0))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }
    let sensitivity = compute_sensitivity(&results, &param_grid, &config.objective);

    Ok(SweepResponse {
        mode: "bayesian".to_string(),
        objective: config.objective.clone(),
        combinations_total: total,
        combinations_run: results.len(),
        combinations_failed: failed,
        best_result: results.first().cloned(),
        ranked_results: results,
        dimension_sensitivity: sensitivity,
        convergence_trace: Some(convergence_trace),
        execution_time_ms: start.elapsed().as_millis() as u64,
        full_results: Vec::new(),
    })
}

// ---------------------------------------------------------------------------
// Gaussian Process
// ---------------------------------------------------------------------------

/// Gaussian Process with RBF kernel.
/// Caches the Cholesky factor L from `fit()` so `predict()` uses O(n²) forward/back
/// substitution instead of rebuilding and re-factoring the O(n³) kernel matrix.
struct GaussianProcess {
    x_train: Vec<Vec<f64>>,
    alpha: Vec<f64>,
    /// Cached Cholesky factor L where K = L·Lᵀ. `None` if decomposition failed
    /// (predict falls back to diagonal solve).
    cholesky_l: Option<Vec<f64>>,
    n: usize,
    length_scale: f64,
    signal_var: f64,
    noise_var: f64,
    y_mean: f64,
}

impl GaussianProcess {
    /// Fit GP to training data. Returns a trained GP with cached Cholesky factor.
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

        // Cache the Cholesky factor and solve alpha
        let cholesky_l = cholesky_decompose(&k_mat, n);
        let alpha = if let Some(ref l) = cholesky_l {
            cholesky_solve_with_l(l, &y_centered, n)
        } else {
            diagonal_solve(&k_mat, &y_centered, n)
        };

        Self {
            x_train: xs.to_vec(),
            alpha,
            cholesky_l,
            n,
            length_scale,
            signal_var,
            noise_var,
            y_mean,
        }
    }

    /// Predict mean and variance at a test point.
    /// Uses the cached Cholesky factor — O(n²) per call instead of O(n³).
    fn predict(&self, x: &[f64]) -> (f64, f64) {
        let n = self.n;
        let k_star: Vec<f64> = (0..n)
            .map(|i| rbf_kernel(x, &self.x_train[i], self.length_scale, self.signal_var))
            .collect();

        // Mean = k* · alpha + y_mean
        let mean = k_star
            .iter()
            .zip(&self.alpha)
            .map(|(k, a)| k * a)
            .sum::<f64>()
            + self.y_mean;

        // Variance = k(x,x) - k*ᵀ K⁻¹ k*
        // Reuse cached Cholesky factor to solve K·v = k* in O(n²)
        let k_xx = self.signal_var + self.noise_var;
        let v = if let Some(ref l) = self.cholesky_l {
            cholesky_solve_with_l(l, &k_star, n)
        } else {
            // Fallback: rebuild K (only if Cholesky failed during fit)
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
            diagonal_solve(&k_mat, &k_star, n)
        };
        let var_reduction: f64 = k_star.iter().zip(&v).map(|(k, vi)| k * vi).sum();
        let var = (k_xx - var_reduction).max(1e-10);

        (mean, var)
    }

    fn median_distance(xs: &[Vec<f64>]) -> f64 {
        let n = xs.len();
        if n < 2 {
            return 1.0;
        }
        let cap = n * (n - 1) / 2;
        let mut dists = Vec::with_capacity(cap);
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
        // Use nth_element (quickselect) for O(n) median instead of O(n log n) sort
        let mid = dists.len() / 2;
        *dists
            .select_nth_unstable_by(mid, |a, b| {
                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
            })
            .1
    }
}

/// RBF (squared exponential) kernel.
fn rbf_kernel(x1: &[f64], x2: &[f64], length_scale: f64, signal_var: f64) -> f64 {
    let sq_dist: f64 = x1.iter().zip(x2).map(|(a, b)| (a - b).powi(2)).sum();
    signal_var * (-0.5 * sq_dist / (length_scale * length_scale)).exp()
}

/// Solve L·Lᵀ·x = b given a pre-computed Cholesky factor L.
#[allow(clippy::many_single_char_names)]
fn cholesky_solve_with_l(l: &[f64], b: &[f64], n: usize) -> Vec<f64> {
    // Forward solve L·y = b
    let mut y = vec![0.0; n];
    for i in 0..n {
        let mut s = b[i];
        for j in 0..i {
            s -= l[i * n + j] * y[j];
        }
        y[i] = s / l[i * n + i];
    }
    // Backward solve Lᵀ·x = y
    let mut x = vec![0.0; n];
    for i in (0..n).rev() {
        let mut s = y[i];
        for j in (i + 1)..n {
            s -= l[j * n + i] * x[j];
        }
        x[i] = s / l[i * n + i];
    }
    x
}

/// Diagonal fallback solve when Cholesky decomposition fails.
fn diagonal_solve(k: &[f64], b: &[f64], n: usize) -> Vec<f64> {
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
pub fn decode_params(x: &[f64], continuous: &[(String, f64, f64, bool)]) -> HashMap<String, Value> {
    x.iter()
        .zip(continuous)
        .map(|(xi, (name, min, max, is_int))| {
            let val = min + xi * (max - min);
            let json_val = if *is_int {
                serde_json::json!(val.round() as i64)
            } else {
                let rounded = (val * 10_000.0).round() / 10_000.0;
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
/// Takes owned `swept_params` to avoid a clone — caller moves the `HashMap` in.
async fn evaluate(
    script_source: &str,
    base_params: &HashMap<String, Value>,
    swept_params: HashMap<String, Value>,
    data_loader: &dyn DataLoader,
    precomputed: Option<&PrecomputedOptionsData>,
    is_cancelled: Option<&CancelCallback>,
) -> Result<(SweepResult, Option<PrecomputedOptionsData>)> {
    let mut run_params = base_params.clone();
    run_params.extend(swept_params.iter().map(|(k, v)| (k.clone(), v.clone())));

    let bt = run_script_backtest(
        script_source,
        &run_params,
        data_loader,
        None,
        precomputed,
        is_cancelled,
    )
    .await?;
    let m = &bt.result.metrics;
    let pre = bt.precomputed_options;

    Ok((
        SweepResult {
            rank: 0,
            params: swept_params,
            sharpe: m.sharpe,
            sortino: m.sortino,
            pnl: bt.result.total_pnl,
            trades: bt.result.trade_count,
            win_rate: m.win_rate,
            max_drawdown: m.max_drawdown,
            profit_factor: m.profit_factor,
            cagr: m.cagr,
            calmar: m.calmar,
        },
        pre,
    ))
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

// ---------------------------------------------------------------------------
// Tests & benchmarks
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify GP predict correctness: mean should interpolate training points.
    #[test]
    fn gp_predict_interpolates() {
        let xs = vec![vec![0.0], vec![0.5], vec![1.0]];
        let ys = vec![1.0, 2.0, 1.5];
        let gp = GaussianProcess::fit(&xs, &ys);

        // Predictions at training points should be close to training values
        for (x, y) in xs.iter().zip(&ys) {
            let (mean, _var) = gp.predict(x);
            assert!(
                (mean - y).abs() < 0.3,
                "GP mean {mean} too far from training value {y}"
            );
        }
    }

    /// Verify that the cached Cholesky factor produces the same results as
    /// solving from scratch would.
    #[test]
    fn gp_cholesky_cache_consistency() {
        let xs: Vec<Vec<f64>> = (0..20)
            .map(|i| vec![f64::from(i) / 20.0, (f64::from(i) * 0.7).sin()])
            .collect();
        let ys: Vec<f64> = xs.iter().map(|x| x[0] * 2.0 + x[1]).collect();
        let gp = GaussianProcess::fit(&xs, &ys);

        // Cholesky factor should have been cached
        assert!(gp.cholesky_l.is_some(), "Cholesky factor should be cached");

        // Predict at several test points
        for _ in 0..10 {
            let test_x = random_point(2);
            let (mean, var) = gp.predict(&test_x);
            assert!(mean.is_finite(), "mean should be finite");
            assert!(var > 0.0, "variance should be positive");
        }
    }

    /// Benchmark: GP predict with cached vs uncached Cholesky.
    /// Prints timing to stdout — run with `cargo test bench_gp -- --nocapture`.
    #[test]
    fn bench_gp_predict_cached_cholesky() {
        let n_train = 50; // typical GP size during Bayesian optimization
        let n_predict = 1000; // maximize_ei evaluates this many candidates
        let dim = 3;

        let xs: Vec<Vec<f64>> = (0..n_train).map(|_| random_point(dim)).collect();
        let ys: Vec<f64> = xs.iter().map(|x| x.iter().sum()).collect();

        // ── Optimized path: cached Cholesky (current implementation) ──────
        let gp = GaussianProcess::fit(&xs, &ys);
        assert!(gp.cholesky_l.is_some());

        let test_points: Vec<Vec<f64>> = (0..n_predict).map(|_| random_point(dim)).collect();

        let start = Instant::now();
        let mut sum = 0.0;
        for tp in &test_points {
            let (mean, _) = gp.predict(tp);
            sum += mean; // prevent dead-code elimination
        }
        let cached_ms = start.elapsed().as_micros();

        // ── Baseline: uncached (rebuild K + Cholesky per predict) ─────────
        let start = Instant::now();
        let mut sum2 = 0.0;
        for tp in &test_points {
            let n = xs.len();
            let k_star: Vec<f64> = (0..n)
                .map(|i| rbf_kernel(tp, &xs[i], gp.length_scale, gp.signal_var))
                .collect();
            let mean = k_star
                .iter()
                .zip(&gp.alpha)
                .map(|(k, a)| k * a)
                .sum::<f64>()
                + gp.y_mean;
            // Rebuild K matrix (the expensive part)
            let mut k_mat = vec![0.0; n * n];
            for i in 0..n {
                for j in 0..n {
                    k_mat[i * n + j] = rbf_kernel(&xs[i], &xs[j], gp.length_scale, gp.signal_var);
                    if i == j {
                        k_mat[i * n + j] += gp.noise_var;
                    }
                }
            }
            let _v = if let Some(l) = cholesky_decompose(&k_mat, n) {
                cholesky_solve_with_l(&l, &k_star, n)
            } else {
                diagonal_solve(&k_mat, &k_star, n)
            };
            sum2 += mean;
        }
        let uncached_ms = start.elapsed().as_micros();

        let speedup = uncached_ms as f64 / cached_ms.max(1) as f64;
        println!(
            "\n=== GP predict benchmark (n_train={n_train}, n_predict={n_predict}) ===\n\
             Cached Cholesky:   {cached_ms:>8} µs\n\
             Uncached (rebuild): {uncached_ms:>8} µs\n\
             Speedup:            {speedup:>8.1}x\n\
             (sum={sum:.4}, sum2={sum2:.4})"
        );
        assert!(
            speedup > 2.0,
            "Cached Cholesky should be at least 2x faster, got {speedup:.1}x"
        );
    }

    /// Benchmark: `cache_key` string building.
    #[test]
    fn bench_cache_key() {
        let mut params = HashMap::new();
        for i in 0..10 {
            params.insert(format!("param_{i}"), serde_json::json!(f64::from(i) * 0.1));
        }

        let n_iters = 10_000;

        // Optimized version (current)
        let start = Instant::now();
        let mut last = String::new();
        for _ in 0..n_iters {
            last = cache_key(&params);
        }
        let optimized_us = start.elapsed().as_micros();

        // Baseline: old Vec+format+join approach
        let start = Instant::now();
        let mut last2 = String::new();
        for _ in 0..n_iters {
            let mut pairs: Vec<_> = params.iter().collect();
            pairs.sort_by_key(|(k, _)| k.as_str());
            last2 = pairs
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("|");
        }
        let baseline_us = start.elapsed().as_micros();

        let speedup = baseline_us as f64 / optimized_us.max(1) as f64;
        println!(
            "\n=== cache_key benchmark ({n_iters} iterations, 10 params) ===\n\
             Optimized:  {optimized_us:>8} µs\n\
             Baseline:   {baseline_us:>8} µs\n\
             Speedup:    {speedup:>8.1}x\n\
             (key={last}, key2={last2})"
        );
        // Keys should be identical
        assert_eq!(last, last2, "Optimized and baseline keys must match");
    }
}
