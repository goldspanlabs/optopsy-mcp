//! Bayesian optimization engine using Gaussian Process surrogate with Expected Improvement.
//!
//! Replaces exhaustive grid search with an iterative approach:
//! 1. Evaluate a small initial batch of random configurations
//! 2. Fit a GP surrogate model to observed (params → Sharpe) pairs
//! 3. Maximize Expected Improvement to pick the next most informative config
//! 4. Evaluate, update, repeat until budget exhausted
//!
//! Handles mixed parameter spaces (continuous deltas/DTEs + categorical strategies/slippage)
//! via one-hot encoding of categoricals in the GP feature space.

use std::collections::HashMap;

use anyhow::{bail, Result};
use nalgebra::{DMatrix, DVector};
use polars::prelude::*;
use rand::prelude::*;

use super::core::run_backtest;
use super::sweep::{
    count_independent_entry_periods, delta_target_to_range, dte_target_to_range, split_by_date,
};
use super::sweep_analysis::{build_sweep_label, compute_sensitivity, DimensionStats, OosResult};
use super::types::{
    default_min_bid_ask, to_display_name, BacktestParams, ExpirationFilter, SimParams, Slippage,
    SweepResult, TargetRange,
};
use crate::signals::registry::SignalSpec;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Parameters for Bayesian optimization.
#[derive(Debug, Clone)]
pub struct BayesianParams {
    /// Strategy name to optimize (single strategy for now).
    pub strategy: String,
    /// Number of legs and their delta bounds: `[(min, max)]` per leg.
    pub leg_delta_bounds: Vec<(f64, f64)>,
    /// Entry DTE range to search: `(min, max)`.
    pub entry_dte_bounds: (i32, i32),
    /// Exit DTE candidates (categorical).
    pub exit_dtes: Vec<i32>,
    /// Slippage models to consider (categorical).
    pub slippage_models: Vec<Slippage>,
    /// Simulation parameters shared across all evaluations.
    pub sim_params: SimParams,
    /// Maximum number of backtest evaluations (budget). Default: 50.
    pub max_evaluations: usize,
    /// Number of initial random samples before GP kicks in. Default: 10.
    pub initial_samples: usize,
    /// Out-of-sample percentage [0, 1). 0 disables. Default: 0.3.
    pub out_of_sample_pct: f64,
    /// RNG seed for reproducibility.
    pub seed: Option<u64>,
    /// Objective metric to maximize. Default: Sharpe.
    pub objective: Objective,
    /// Entry signal (fixed, not swept).
    pub entry_signal: Option<SignalSpec>,
    /// Exit signal (fixed, not swept).
    pub exit_signal: Option<SignalSpec>,
}

/// Objective metric to maximize during optimization.
#[derive(Debug, Clone, Copy, Default)]
pub enum Objective {
    #[default]
    Sharpe,
    Sortino,
    Calmar,
    ProfitFactor,
}

impl Objective {
    fn extract(self, result: &SweepResult) -> f64 {
        match self {
            Self::Sharpe => result.sharpe,
            Self::Sortino => result.sortino,
            Self::Calmar => result.calmar,
            Self::ProfitFactor => result.profit_factor,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Sharpe => "Sharpe",
            Self::Sortino => "Sortino",
            Self::Calmar => "Calmar",
            Self::ProfitFactor => "Profit Factor",
        }
    }
}

/// Output of Bayesian optimization, compatible with existing `SweepOutput` for formatting.
#[derive(Debug, Clone)]
pub struct BayesianOutput {
    pub mode: Option<String>,
    pub total_evaluations: usize,
    pub failed_evaluations: usize,
    pub objective: String,
    pub convergence_trace: Vec<f64>,
    pub ranked_results: Vec<SweepResult>,
    pub dimension_sensitivity: HashMap<String, HashMap<String, DimensionStats>>,
    pub oos_results: Vec<OosResult>,
    pub stability_scores: Vec<super::sweep_analysis::StabilityScore>,
}

// ---------------------------------------------------------------------------
// Gaussian Process
// ---------------------------------------------------------------------------

/// RBF (squared exponential) kernel: k(x, x') = σ² exp(-‖x-x'‖² / (2l²))
struct RbfKernel {
    length_scale: f64,
    signal_variance: f64,
}

impl RbfKernel {
    fn new(length_scale: f64, signal_variance: f64) -> Self {
        Self {
            length_scale,
            signal_variance,
        }
    }

    fn compute(&self, x1: &DVector<f64>, x2: &DVector<f64>) -> f64 {
        let diff = x1 - x2;
        let sq_dist = diff.dot(&diff);
        self.signal_variance * (-sq_dist / (2.0 * self.length_scale * self.length_scale)).exp()
    }

    fn matrix(&self, x: &DMatrix<f64>) -> DMatrix<f64> {
        let n = x.nrows();
        let mut k = DMatrix::zeros(n, n);
        for i in 0..n {
            let xi = x.row(i).transpose();
            for j in i..n {
                let xj = x.row(j).transpose();
                let val = self.compute(&xi, &xj);
                k[(i, j)] = val;
                k[(j, i)] = val;
            }
        }
        k
    }

    fn cross(&self, x_train: &DMatrix<f64>, x_test: &DMatrix<f64>) -> DMatrix<f64> {
        let n_train = x_train.nrows();
        let n_test = x_test.nrows();
        let mut k = DMatrix::zeros(n_test, n_train);
        for i in 0..n_test {
            let xi = x_test.row(i).transpose();
            for j in 0..n_train {
                let xj = x_train.row(j).transpose();
                k[(i, j)] = self.compute(&xi, &xj);
            }
        }
        k
    }
}

/// Minimal GP for surrogate modeling.
struct GaussianProcess {
    kernel: RbfKernel,
    noise: f64,
    x_train: Option<DMatrix<f64>>,
    y_train: Option<DVector<f64>>,
    y_mean: f64,
    y_std: f64,
    alpha: Option<DVector<f64>>,
    k_inv: Option<DMatrix<f64>>,
}

impl GaussianProcess {
    fn new(length_scale: f64, signal_variance: f64, noise: f64) -> Self {
        Self {
            kernel: RbfKernel::new(length_scale, signal_variance),
            noise,
            x_train: None,
            y_train: None,
            y_mean: 0.0,
            y_std: 1.0,
            alpha: None,
            k_inv: None,
        }
    }

    /// Fit the GP to observed data. Normalizes y internally.
    fn fit(&mut self, x: DMatrix<f64>, y: &DVector<f64>) {
        let n = y.len();
        self.y_mean = y.mean();
        self.y_std = if n > 1 {
            let var = y.iter().map(|&v| (v - self.y_mean).powi(2)).sum::<f64>() / (n - 1) as f64;
            var.sqrt().max(1e-8)
        } else {
            1.0
        };

        let y_norm = y.map(|v| (v - self.y_mean) / self.y_std);

        let mut k = self.kernel.matrix(&x);
        // Add noise (jitter) to diagonal for numerical stability
        for i in 0..n {
            k[(i, i)] += self.noise;
        }

        // Solve K * alpha = y_norm via Cholesky
        if let Some(chol) = nalgebra::linalg::Cholesky::new(k.clone()) {
            let alpha = chol.solve(&y_norm);
            let k_inv = chol.inverse();
            self.alpha = Some(alpha);
            self.k_inv = Some(k_inv);
        } else {
            // Fallback: add more jitter
            for i in 0..n {
                k[(i, i)] += 1e-4;
            }
            if let Some(chol) = nalgebra::linalg::Cholesky::new(k) {
                let alpha = chol.solve(&y_norm);
                let k_inv = chol.inverse();
                self.alpha = Some(alpha);
                self.k_inv = Some(k_inv);
            }
        }

        self.x_train = Some(x);
        self.y_train = Some(y_norm);
    }

    /// Predict mean and variance at new points.
    fn predict(&self, x_test: &DMatrix<f64>) -> (DVector<f64>, DVector<f64>) {
        let x_train = self.x_train.as_ref().expect("GP not fitted");
        let alpha = self.alpha.as_ref().expect("GP not fitted");
        let k_inv = self.k_inv.as_ref().expect("GP not fitted");

        let k_star = self.kernel.cross(x_train, x_test);
        let mean_norm = &k_star * alpha;

        let n_test = x_test.nrows();
        let mut var = DVector::zeros(n_test);
        for i in 0..n_test {
            let k_ss = self.kernel.signal_variance;
            let k_s = k_star.row(i).transpose();
            let v = k_ss - (k_s.transpose() * k_inv * &k_s)[(0, 0)];
            var[i] = v.max(1e-10);
        }

        // Un-normalize
        let mean = mean_norm.map(|v| v * self.y_std + self.y_mean);
        let var_unnorm = var.map(|v| v * self.y_std * self.y_std);

        (mean, var_unnorm)
    }
}

/// Standard normal CDF using the error function approximation.
fn standard_normal_cdf(x: f64) -> f64 {
    // Abramowitz & Stegun approximation (|error| < 1.5e-7)
    let a1 = 0.254_829_592;
    let a2 = -0.284_496_736;
    let a3 = 1.421_413_741;
    let a4 = -1.453_152_027;
    let a5 = 1.061_405_429;
    let p = 0.327_591_1;

    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x_abs = x.abs() / std::f64::consts::SQRT_2;
    let t = 1.0 / (1.0 + p * x_abs);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x_abs * x_abs).exp();
    0.5 * (1.0 + sign * y)
}

/// Standard normal PDF.
fn standard_normal_pdf(x: f64) -> f64 {
    (-0.5 * x * x).exp() / (2.0 * std::f64::consts::PI).sqrt()
}

/// Expected Improvement acquisition function.
/// `EI(x) = (μ(x) - f_best) Φ(z) + σ(x) φ(z)` where `z = (μ(x) - f_best) / σ(x)`
fn expected_improvement(mean: f64, variance: f64, f_best: f64) -> f64 {
    let sigma = variance.sqrt();
    if sigma < 1e-10 {
        return 0.0;
    }
    let z = (mean - f_best) / sigma;
    (mean - f_best) * standard_normal_cdf(z) + sigma * standard_normal_pdf(z)
}

// ---------------------------------------------------------------------------
// Parameter space encoding
// ---------------------------------------------------------------------------

/// Encodes the mixed parameter space into a continuous feature vector for the GP.
struct ParameterSpace {
    n_legs: usize,
    leg_delta_bounds: Vec<(f64, f64)>,
    entry_dte_bounds: (i32, i32),
    exit_dte_values: Vec<i32>,
    slippage_values: Vec<Slippage>,
}

/// A decoded configuration ready to run as a backtest.
#[derive(Debug, Clone)]
struct Configuration {
    leg_deltas: Vec<f64>,
    entry_dte: i32,
    exit_dte: i32,
    slippage: Slippage,
}

impl ParameterSpace {
    fn new(params: &BayesianParams) -> Self {
        Self {
            n_legs: params.leg_delta_bounds.len(),
            leg_delta_bounds: params.leg_delta_bounds.clone(),
            entry_dte_bounds: params.entry_dte_bounds,
            exit_dte_values: params.exit_dtes.clone(),
            slippage_values: params.slippage_models.clone(),
        }
    }

    /// Dimensionality of the encoded feature vector.
    fn dim(&self) -> usize {
        self.n_legs // continuous delta per leg
            + 1     // continuous entry DTE (normalized)
            + self.exit_dte_values.len()    // one-hot exit DTE
            + self.slippage_values.len() // one-hot slippage
    }

    /// Encode a configuration into a feature vector.
    fn encode(&self, config: &Configuration) -> DVector<f64> {
        let mut v = DVector::zeros(self.dim());
        let mut idx = 0;

        // Continuous: normalized deltas [0, 1]
        for (i, &d) in config.leg_deltas.iter().enumerate() {
            let (lo, hi) = self.leg_delta_bounds[i];
            v[idx] = if (hi - lo).abs() < 1e-10 {
                0.5
            } else {
                (d - lo) / (hi - lo)
            };
            idx += 1;
        }

        // Continuous: normalized entry DTE [0, 1]
        let (dte_lo, dte_hi) = self.entry_dte_bounds;
        v[idx] = if dte_hi == dte_lo {
            0.5
        } else {
            f64::from(config.entry_dte - dte_lo) / f64::from(dte_hi - dte_lo)
        };
        idx += 1;

        // One-hot: exit DTE
        for (i, &edt) in self.exit_dte_values.iter().enumerate() {
            v[idx + i] = if edt == config.exit_dte { 1.0 } else { 0.0 };
        }
        idx += self.exit_dte_values.len();

        // One-hot: slippage
        for (i, sl) in self.slippage_values.iter().enumerate() {
            v[idx + i] = if slippage_eq(sl, &config.slippage) {
                1.0
            } else {
                0.0
            };
        }

        v
    }

    /// Sample a random configuration.
    fn sample_random(&self, rng: &mut impl Rng) -> Configuration {
        let leg_deltas: Vec<f64> = self
            .leg_delta_bounds
            .iter()
            .map(|&(lo, hi)| rng.random_range(lo..=hi))
            .collect();

        let entry_dte = rng.random_range(self.entry_dte_bounds.0..=self.entry_dte_bounds.1);

        let exit_dte = self.exit_dte_values[rng.random_range(0..self.exit_dte_values.len())];

        let slippage =
            self.slippage_values[rng.random_range(0..self.slippage_values.len())].clone();

        Configuration {
            leg_deltas,
            entry_dte,
            exit_dte,
            slippage,
        }
    }

    /// Generate candidate configurations for acquisition function maximization.
    /// Uses random sampling + local perturbation around the current best.
    fn generate_candidates(
        &self,
        rng: &mut impl Rng,
        n: usize,
        best: Option<&Configuration>,
    ) -> Vec<Configuration> {
        let mut candidates = Vec::with_capacity(n);

        // Half random, half local perturbations around best
        let n_random = if best.is_some() { n / 2 } else { n };
        for _ in 0..n_random {
            candidates.push(self.sample_random(rng));
        }

        if let Some(best_config) = best {
            for _ in n_random..n {
                candidates.push(self.perturb(rng, best_config));
            }
        }

        candidates
    }

    /// Perturb a configuration slightly (for local search around best).
    fn perturb(&self, rng: &mut impl Rng, config: &Configuration) -> Configuration {
        let mut leg_deltas = config.leg_deltas.clone();
        for (i, d) in leg_deltas.iter_mut().enumerate() {
            let (lo, hi) = self.leg_delta_bounds[i];
            let noise: f64 = rng.random_range(-0.05..=0.05);
            *d = (*d + noise).clamp(lo, hi);
        }

        let dte_noise: i32 = rng.random_range(-5..=5);
        let entry_dte =
            (config.entry_dte + dte_noise).clamp(self.entry_dte_bounds.0, self.entry_dte_bounds.1);

        // Randomly keep or switch categorical values
        let exit_dte = if rng.random_bool(0.3) {
            self.exit_dte_values[rng.random_range(0..self.exit_dte_values.len())]
        } else {
            config.exit_dte
        };

        let slippage = if rng.random_bool(0.3) {
            self.slippage_values[rng.random_range(0..self.slippage_values.len())].clone()
        } else {
            config.slippage.clone()
        };

        Configuration {
            leg_deltas,
            entry_dte,
            exit_dte,
            slippage,
        }
    }
}

fn slippage_eq(a: &Slippage, b: &Slippage) -> bool {
    matches!(
        (a, b),
        (Slippage::Mid, Slippage::Mid)
            | (Slippage::Spread, Slippage::Spread)
            | (Slippage::Liquidity { .. }, Slippage::Liquidity { .. })
            | (Slippage::PerLeg { .. }, Slippage::PerLeg { .. })
            | (Slippage::BidAskTravel { .. }, Slippage::BidAskTravel { .. })
    )
}

// ---------------------------------------------------------------------------
// Main optimization loop
// ---------------------------------------------------------------------------

/// Run Bayesian optimization over the options parameter space.
#[allow(clippy::too_many_lines)]
pub fn run_bayesian_optimization(
    df: &DataFrame,
    params: &BayesianParams,
) -> Result<BayesianOutput> {
    if params.max_evaluations < params.initial_samples {
        bail!(
            "max_evaluations ({}) must be >= initial_samples ({})",
            params.max_evaluations,
            params.initial_samples
        );
    }
    if params.leg_delta_bounds.is_empty() {
        bail!("leg_delta_bounds must have at least one leg");
    }
    if params.exit_dtes.is_empty() {
        bail!("exit_dtes must have at least one value");
    }
    if params.slippage_models.is_empty() {
        bail!("slippage_models must have at least one model");
    }
    if params.entry_dte_bounds.0 > params.entry_dte_bounds.1 {
        bail!("entry_dte_bounds min must be <= max");
    }

    let space = ParameterSpace::new(params);
    let mut rng_instance = match params.seed {
        Some(seed) => SmallRng::seed_from_u64(seed),
        None => SmallRng::from_os_rng(),
    };

    // Split data for OOS if needed
    let (train_df, test_df) = if params.out_of_sample_pct > 0.0 {
        let (train, test) = split_by_date(df, params.out_of_sample_pct)?;
        (train, Some(test))
    } else {
        (df.clone(), None)
    };

    let mut all_configs: Vec<Configuration> = Vec::new();
    let mut all_results: Vec<SweepResult> = Vec::new();
    let mut all_objectives: Vec<f64> = Vec::new();
    let mut convergence_trace: Vec<f64> = Vec::new();
    let mut failed: usize = 0;
    let mut best_so_far = f64::NEG_INFINITY;

    // Phase 1: Random initial sampling
    tracing::info!(
        initial = params.initial_samples,
        budget = params.max_evaluations,
        "Bayesian optimization: starting initial random sampling"
    );

    for i in 0..params.initial_samples {
        let config = space.sample_random(&mut rng_instance);
        match evaluate_config(&train_df, params, &config) {
            Ok(result) => {
                let obj = params.objective.extract(&result);
                if obj > best_so_far {
                    best_so_far = obj;
                }
                convergence_trace.push(best_so_far);
                all_configs.push(config);
                all_objectives.push(obj);
                all_results.push(result);
            }
            Err(e) => {
                failed += 1;
                tracing::warn!("Bayesian eval {i} failed: {e}");
                convergence_trace.push(best_so_far);
            }
        }
    }

    if all_results.is_empty() {
        bail!("All initial samples failed — check strategy/delta/DTE configuration");
    }

    // Phase 2: GP-guided optimization
    let remaining = params.max_evaluations - params.initial_samples;
    let n_candidates = 200; // Number of candidate points to evaluate EI over

    // Auto-tune GP hyperparameters based on feature dimensionality
    let length_scale = (space.dim() as f64).sqrt() * 0.5;
    let mut gp = GaussianProcess::new(length_scale, 1.0, 1e-3);

    for i in 0..remaining {
        // Build training matrix from observed points
        let n_obs = all_configs.len();
        let d = space.dim();
        let mut x_mat = DMatrix::zeros(n_obs, d);
        for (row, config) in all_configs.iter().enumerate() {
            let encoded = space.encode(config);
            for col in 0..d {
                x_mat[(row, col)] = encoded[col];
            }
        }
        let y_vec = DVector::from_vec(all_objectives.clone());

        // Fit GP
        gp.fit(x_mat, &y_vec);

        // Find best current config for local search
        let best_idx = all_objectives
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(idx, _)| idx);

        let best_config = best_idx.map(|idx| &all_configs[idx]);

        // Generate candidates and evaluate Expected Improvement
        let candidates = space.generate_candidates(&mut rng_instance, n_candidates, best_config);
        let n_cand = candidates.len();
        let mut x_cand = DMatrix::zeros(n_cand, d);
        for (row, config) in candidates.iter().enumerate() {
            let encoded = space.encode(config);
            for col in 0..d {
                x_cand[(row, col)] = encoded[col];
            }
        }

        let (means, variances) = gp.predict(&x_cand);

        // Find candidate with highest EI
        let mut best_ei = f64::NEG_INFINITY;
        let mut best_cand_idx = 0;
        for j in 0..n_cand {
            let ei = expected_improvement(means[j], variances[j], best_so_far);
            if ei > best_ei {
                best_ei = ei;
                best_cand_idx = j;
            }
        }

        let next_config = candidates[best_cand_idx].clone();

        // Evaluate the chosen configuration
        match evaluate_config(&train_df, params, &next_config) {
            Ok(result) => {
                let obj = params.objective.extract(&result);
                if obj > best_so_far {
                    best_so_far = obj;
                    tracing::info!(
                        iteration = params.initial_samples + i + 1,
                        objective = format!("{:.4}", obj),
                        "New best found"
                    );
                }
                convergence_trace.push(best_so_far);
                all_configs.push(next_config);
                all_objectives.push(obj);
                all_results.push(result);
            }
            Err(e) => {
                failed += 1;
                tracing::warn!(
                    "Bayesian eval {} failed: {e}",
                    params.initial_samples + i + 1
                );
                convergence_trace.push(best_so_far);
            }
        }
    }

    // Sort results by objective descending
    all_results.sort_by(|a, b| {
        let obj_a = params.objective.extract(a);
        let obj_b = params.objective.extract(b);
        obj_b
            .partial_cmp(&obj_a)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // Compute sensitivity (reuse sweep analysis)
    let dimension_sensitivity = compute_sensitivity(&all_results);
    // Note: stability scoring requires grid-based SweepParams for neighbor detection,
    // which doesn't apply to Bayesian optimization's continuous search.
    let stability_scores = vec![];

    // OOS validation on top 3
    let mut oos_results = Vec::new();
    if let Some(ref test_df) = test_df {
        let top_n = all_results.len().min(3);
        for r in all_results.iter().take(top_n) {
            let config = Configuration {
                leg_deltas: r.leg_deltas.iter().map(|d| d.target).collect(),
                entry_dte: r.entry_dte.target,
                exit_dte: r.exit_dte,
                slippage: r.slippage.clone(),
            };
            if let Ok(test_result) = evaluate_config(test_df, params, &config) {
                oos_results.push(OosResult {
                    label: r.label.clone(),
                    train_sharpe: r.sharpe,
                    test_sharpe: test_result.sharpe,
                    train_pnl: r.pnl,
                    test_pnl: test_result.pnl,
                });
            }
        }
    }

    Ok(BayesianOutput {
        mode: None,
        total_evaluations: params.max_evaluations,
        failed_evaluations: failed,
        objective: params.objective.name().to_string(),
        convergence_trace,
        ranked_results: all_results,
        dimension_sensitivity,
        oos_results,
        stability_scores,
    })
}

/// Evaluate a single configuration by running a backtest.
fn evaluate_config(
    df: &DataFrame,
    params: &BayesianParams,
    config: &Configuration,
) -> Result<SweepResult> {
    let leg_deltas: Vec<TargetRange> = config
        .leg_deltas
        .iter()
        .map(|&d| delta_target_to_range(d))
        .collect();
    let entry_dte = dte_target_to_range(config.entry_dte);

    // Validate exit_dte < entry_dte.min
    if config.exit_dte >= entry_dte.min {
        bail!(
            "exit_dte {} >= entry_dte.min {}",
            config.exit_dte,
            entry_dte.min
        );
    }

    let label = build_sweep_label(
        &params.strategy,
        &leg_deltas,
        config.entry_dte,
        config.exit_dte,
        &config.slippage,
    );

    let backtest_params = BacktestParams {
        strategy: params.strategy.clone(),
        leg_deltas: leg_deltas.clone(),
        entry_dte: entry_dte.clone(),
        exit_dte: config.exit_dte,
        slippage: config.slippage.clone(),
        commission: None,
        min_bid_ask: default_min_bid_ask(),
        stop_loss: params.sim_params.stop_loss,
        take_profit: params.sim_params.take_profit,
        max_hold_days: params.sim_params.max_hold_days,
        capital: params.sim_params.capital,
        quantity: params.sim_params.quantity,
        sizing: params.sim_params.sizing.clone(),
        multiplier: params.sim_params.multiplier,
        max_positions: params.sim_params.max_positions,
        selector: params.sim_params.selector.clone(),
        adjustment_rules: vec![],
        entry_signal: params.entry_signal.clone(),
        exit_signal: params.exit_signal.clone(),
        ohlcv_path: params.sim_params.ohlcv_path.clone(),
        cross_ohlcv_paths: params.sim_params.cross_ohlcv_paths.clone(),
        min_net_premium: None,
        max_net_premium: None,
        min_net_delta: None,
        max_net_delta: None,
        min_days_between_entries: params.sim_params.min_days_between_entries,
        expiration_filter: ExpirationFilter::Any,
        exit_net_delta: params.sim_params.exit_net_delta,
    };

    let bt = run_backtest(df, &backtest_params)?;
    let independent_periods = count_independent_entry_periods(&bt.trade_log);

    Ok(SweepResult {
        label,
        strategy: params.strategy.clone(),
        display_name: to_display_name(&params.strategy),
        leg_deltas,
        entry_dte,
        exit_dte: config.exit_dte,
        slippage: config.slippage.clone(),
        trades: bt.trade_count,
        pnl: bt.total_pnl,
        sharpe: bt.metrics.sharpe,
        sortino: bt.metrics.sortino,
        max_dd: bt.metrics.max_drawdown,
        win_rate: bt.metrics.win_rate,
        profit_factor: bt.metrics.profit_factor,
        calmar: bt.metrics.calmar,
        total_return_pct: bt.metrics.total_return_pct,
        independent_entry_periods: independent_periods,
        entry_signal: params.entry_signal.clone(),
        exit_signal: params.exit_signal.clone(),
        signal_dim_keys: vec![],
        p_value: None,
        sizing: None,
    })
}
