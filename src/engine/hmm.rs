//! Gaussian Hidden Markov Model with Baum-Welch EM and Viterbi decoding.
//!
//! Each hidden state emits observations from a univariate Gaussian distribution.
//! Used by `regime_detect` to discover latent market regimes from return series.

/// Fitted Gaussian HMM parameters.
#[derive(Debug, Clone)]
pub struct GaussianHmm {
    pub n_states: usize,
    /// Initial state probabilities (pi).
    pub initial: Vec<f64>,
    /// Transition matrix A\[i\]\[j\] = P(state j | state i).
    pub transition: Vec<Vec<f64>>,
    /// Emission mean per state.
    pub means: Vec<f64>,
    /// Emission variance per state (always >= `VARIANCE_FLOOR`).
    pub variances: Vec<f64>,
}

const VARIANCE_FLOOR: f64 = 1e-10;
const DEFAULT_MAX_ITER: usize = 100;
const DEFAULT_TOL: f64 = 1e-6;

/// Log of Gaussian PDF: log N(x | mu, sigma^2).
fn log_gaussian(x: f64, mean: f64, variance: f64) -> f64 {
    let v = variance.max(VARIANCE_FLOOR);
    -0.5 * ((x - mean).powi(2) / v + v.ln() + std::f64::consts::TAU.ln())
}

/// Fit a Gaussian HMM to observed data using Baum-Welch (EM).
///
/// States are sorted by ascending emission mean after fitting, so state 0 is
/// always the lowest-return regime.
pub fn fit(observations: &[f64], n_states: usize) -> GaussianHmm {
    fit_with_params(observations, n_states, DEFAULT_MAX_ITER, DEFAULT_TOL)
}

/// Fit with explicit iteration and tolerance parameters.
#[allow(clippy::too_many_lines, clippy::needless_range_loop)]
pub fn fit_with_params(
    observations: &[f64],
    n_states: usize,
    max_iter: usize,
    tol: f64,
) -> GaussianHmm {
    let t = observations.len();
    assert!(t >= 2, "need at least 2 observations");
    assert!(n_states >= 2, "need at least 2 states");

    // ── Initialization: sort data, split into n_states chunks ──
    let mut sorted = observations.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let mut means = Vec::with_capacity(n_states);
    let mut variances = Vec::with_capacity(n_states);
    for s in 0..n_states {
        let start = s * t / n_states;
        let end = ((s + 1) * t / n_states).min(t);
        let chunk = &sorted[start..end];
        let m = chunk.iter().sum::<f64>() / chunk.len() as f64;
        let v = chunk.iter().map(|&x| (x - m).powi(2)).sum::<f64>() / chunk.len() as f64;
        means.push(m);
        variances.push(v.max(VARIANCE_FLOOR));
    }

    // Uniform initial probs, diagonal-biased transition
    let mut initial = vec![1.0 / n_states as f64; n_states];
    let off_diag = 0.3 / (n_states - 1).max(1) as f64;
    let mut transition: Vec<Vec<f64>> = (0..n_states)
        .map(|i| {
            (0..n_states)
                .map(|j| if i == j { 0.7 } else { off_diag })
                .collect()
        })
        .collect();

    // ── EM iterations ──
    let mut alpha = vec![vec![0.0_f64; n_states]; t];
    let mut beta = vec![vec![0.0_f64; n_states]; t];
    let mut scale = vec![0.0_f64; t];
    let mut gamma = vec![vec![0.0_f64; n_states]; t];

    let mut prev_ll = f64::NEG_INFINITY;

    for _iter in 0..max_iter {
        // ── E-step: scaled forward-backward ──

        // Forward pass (index loops are clearer for matrix math)
        let mut s0 = 0.0;
        for j in 0..n_states {
            alpha[0][j] = initial[j] * log_gaussian(observations[0], means[j], variances[j]).exp();
            s0 += alpha[0][j];
        }
        scale[0] = if s0 > 0.0 { 1.0 / s0 } else { 1.0 };
        for j in 0..n_states {
            alpha[0][j] *= scale[0];
        }

        for tt in 1..t {
            let mut st = 0.0;
            for j in 0..n_states {
                let mut sum = 0.0;
                for i in 0..n_states {
                    sum += alpha[tt - 1][i] * transition[i][j];
                }
                alpha[tt][j] = sum * log_gaussian(observations[tt], means[j], variances[j]).exp();
                st += alpha[tt][j];
            }
            scale[tt] = if st > 0.0 { 1.0 / st } else { 1.0 };
            for j in 0..n_states {
                alpha[tt][j] *= scale[tt];
            }
        }

        // Log-likelihood from scaling factors
        let ll: f64 = scale.iter().map(|&c| -(c.ln())).sum();

        // Backward pass
        for j in 0..n_states {
            beta[t - 1][j] = scale[t - 1];
        }
        for tt in (0..t - 1).rev() {
            for i in 0..n_states {
                let mut sum = 0.0;
                for j in 0..n_states {
                    sum += transition[i][j]
                        * log_gaussian(observations[tt + 1], means[j], variances[j]).exp()
                        * beta[tt + 1][j];
                }
                beta[tt][i] = sum * scale[tt];
            }
        }

        // Gamma (state posteriors)
        for tt in 0..t {
            let mut denom = 0.0;
            for j in 0..n_states {
                gamma[tt][j] = alpha[tt][j] * beta[tt][j];
                denom += gamma[tt][j];
            }
            if denom > 0.0 {
                for j in 0..n_states {
                    gamma[tt][j] /= denom;
                }
            }
        }

        // ── M-step ──

        // Update initial
        let gamma_sum: f64 = gamma[0].iter().sum();
        for j in 0..n_states {
            initial[j] = if gamma_sum > 0.0 {
                gamma[0][j] / gamma_sum
            } else {
                1.0 / n_states as f64
            };
        }

        // Update transition
        for i in 0..n_states {
            let row_denom: f64 = gamma.iter().take(t - 1).map(|g| g[i]).sum();
            for j in 0..n_states {
                let mut numer = 0.0;
                for tt in 0..t - 1 {
                    numer += alpha[tt][i]
                        * transition[i][j]
                        * log_gaussian(observations[tt + 1], means[j], variances[j]).exp()
                        * beta[tt + 1][j];
                }
                transition[i][j] = if row_denom > 0.0 {
                    (numer / row_denom).max(1e-10)
                } else {
                    1.0 / n_states as f64
                };
            }
            // Normalize row
            let row_sum: f64 = transition[i].iter().sum();
            if row_sum > 0.0 {
                for val in &mut transition[i] {
                    *val /= row_sum;
                }
            }
        }

        // Update emissions
        for j in 0..n_states {
            let mut gamma_sum_j = 0.0;
            let mut weighted_sum = 0.0;
            for tt in 0..t {
                gamma_sum_j += gamma[tt][j];
                weighted_sum += gamma[tt][j] * observations[tt];
            }
            if gamma_sum_j > 1e-15 {
                means[j] = weighted_sum / gamma_sum_j;
                let mut var_sum = 0.0;
                for tt in 0..t {
                    var_sum += gamma[tt][j] * (observations[tt] - means[j]).powi(2);
                }
                variances[j] = (var_sum / gamma_sum_j).max(VARIANCE_FLOOR);
            }
        }

        // Check convergence
        if (ll - prev_ll).abs() < tol {
            break;
        }
        prev_ll = ll;
    }

    // Sort states by ascending mean for consistent labeling
    let mut order: Vec<usize> = (0..n_states).collect();
    order.sort_by(|&a, &b| {
        means[a]
            .partial_cmp(&means[b])
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let sorted_means: Vec<f64> = order.iter().map(|&i| means[i]).collect();
    let sorted_variances: Vec<f64> = order.iter().map(|&i| variances[i]).collect();
    let sorted_initial: Vec<f64> = order.iter().map(|&i| initial[i]).collect();
    let sorted_transition: Vec<Vec<f64>> = order
        .iter()
        .map(|&i| order.iter().map(|&j| transition[i][j]).collect())
        .collect();

    GaussianHmm {
        n_states,
        initial: sorted_initial,
        transition: sorted_transition,
        means: sorted_means,
        variances: sorted_variances,
    }
}

/// Viterbi algorithm: find the most likely state sequence in log-space.
#[allow(clippy::needless_range_loop)]
pub fn viterbi(hmm: &GaussianHmm, observations: &[f64]) -> Vec<usize> {
    let t = observations.len();
    let k = hmm.n_states;
    if t == 0 {
        return vec![];
    }

    let mut v = vec![vec![f64::NEG_INFINITY; k]; t];
    let mut bt = vec![vec![0usize; k]; t];

    // Initialize
    for j in 0..k {
        v[0][j] = hmm.initial[j].max(1e-300).ln()
            + log_gaussian(observations[0], hmm.means[j], hmm.variances[j]);
    }

    // Recurse
    for tt in 1..t {
        let emit = observations[tt];
        for j in 0..k {
            let log_emit = log_gaussian(emit, hmm.means[j], hmm.variances[j]);
            let mut best_val = f64::NEG_INFINITY;
            let mut best_i = 0;
            for i in 0..k {
                let val = v[tt - 1][i] + hmm.transition[i][j].max(1e-300).ln();
                if val > best_val {
                    best_val = val;
                    best_i = i;
                }
            }
            v[tt][j] = best_val + log_emit;
            bt[tt][j] = best_i;
        }
    }

    // Backtrace
    let mut path = vec![0usize; t];
    path[t - 1] = v[t - 1]
        .iter()
        .enumerate()
        .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .map_or(0, |(i, _)| i);

    for tt in (0..t - 1).rev() {
        path[tt] = bt[tt + 1][path[tt + 1]];
    }

    path
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Generate synthetic data from two known Gaussians with switching.
    fn two_state_data(n: usize) -> Vec<f64> {
        let mut seed: u64 = 42;
        let mut next = || -> f64 {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let u = (seed >> 11) as f64 / (1u64 << 53) as f64;
            u - 0.5
        };

        let mut data = Vec::with_capacity(n);
        for i in 0..n {
            if i < n / 2 {
                data.push(-0.01 + next() * 0.005);
            } else {
                data.push(0.02 + next() * 0.02);
            }
        }
        data
    }

    #[test]
    fn test_fit_two_states_mean_recovery() {
        let data = two_state_data(400);
        let hmm = fit(&data, 2);

        assert_eq!(hmm.n_states, 2);
        assert!(
            hmm.means[0] < hmm.means[1],
            "state 0 mean ({}) should be < state 1 mean ({})",
            hmm.means[0],
            hmm.means[1]
        );
        assert!(
            (hmm.means[0] - (-0.01)).abs() < 0.05,
            "state 0 mean {} not close to -0.01",
            hmm.means[0]
        );
        assert!(
            (hmm.means[1] - 0.02).abs() < 0.05,
            "state 1 mean {} not close to 0.02",
            hmm.means[1]
        );
    }

    #[test]
    fn test_viterbi_state_sequence() {
        let data = two_state_data(400);
        let hmm = fit(&data, 2);
        let path = viterbi(&hmm, &data);

        assert_eq!(path.len(), data.len());

        let first_half_state0 = path[..200].iter().filter(|&&s| s == 0).count();
        let second_half_state1 = path[200..].iter().filter(|&&s| s == 1).count();

        assert!(
            first_half_state0 as f64 / 200.0 > 0.6,
            "first half should be mostly state 0: {first_half_state0}/200"
        );
        assert!(
            second_half_state1 as f64 / 200.0 > 0.6,
            "second half should be mostly state 1: {second_half_state1}/200"
        );
    }

    #[test]
    fn test_transition_matrix_rows_sum_to_one() {
        let data = two_state_data(200);
        let hmm = fit(&data, 2);
        for (i, row) in hmm.transition.iter().enumerate() {
            let s: f64 = row.iter().sum();
            assert!(
                (s - 1.0).abs() < 1e-6,
                "transition row {i} sums to {s}, expected 1.0"
            );
        }
    }

    #[test]
    fn test_initial_probs_sum_to_one() {
        let data = two_state_data(200);
        let hmm = fit(&data, 2);
        let s: f64 = hmm.initial.iter().sum();
        assert!(
            (s - 1.0).abs() < 1e-6,
            "initial probs sum to {s}, expected 1.0"
        );
    }

    #[test]
    fn test_three_states() {
        let data: Vec<f64> = (0..300)
            .map(|i| {
                if i < 100 {
                    -0.02 + (f64::from(i) * 0.001)
                } else if i < 200 {
                    f64::from(i) * 0.0001
                } else {
                    0.03 + (f64::from(i) * 0.001)
                }
            })
            .collect();
        let hmm = fit(&data, 3);
        assert_eq!(hmm.n_states, 3);
        assert_eq!(hmm.means.len(), 3);
        assert!(hmm.means[0] < hmm.means[2], "means should be sorted");
    }

    #[test]
    fn test_viterbi_empty() {
        let hmm = fit(&[1.0, 2.0, 3.0, 4.0], 2);
        let path = viterbi(&hmm, &[]);
        assert!(path.is_empty());
    }

    #[test]
    fn test_variance_recovery() {
        // State 0: low variance, State 1: high variance
        let data = two_state_data(400);
        let hmm = fit(&data, 2);

        // State 0 (low mean) should have lower variance than state 1
        assert!(
            hmm.variances[0] < hmm.variances[1],
            "state 0 var ({}) should be < state 1 var ({})",
            hmm.variances[0],
            hmm.variances[1]
        );
        // Both variances should be positive
        assert!(hmm.variances[0] > 0.0);
        assert!(hmm.variances[1] > 0.0);
    }

    #[test]
    fn test_convergence_noisy_data() {
        // Ensure HMM converges (doesn't panic) with realistic noisy returns
        let mut seed: u64 = 314;
        let mut data = Vec::with_capacity(500);
        for i in 0..500 {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let noise = (seed >> 11) as f64 / (1u64 << 53) as f64 * 0.04 - 0.02;
            // Regime switch at midpoint
            let mean = if i < 250 { -0.005 } else { 0.01 };
            data.push(mean + noise);
        }

        let hmm = fit(&data, 2);
        // Should converge without NaN/Inf
        for &m in &hmm.means {
            assert!(m.is_finite(), "mean should be finite: {m}");
        }
        for &v in &hmm.variances {
            assert!(
                v.is_finite() && v > 0.0,
                "variance should be finite positive: {v}"
            );
        }
        for row in &hmm.transition {
            let s: f64 = row.iter().sum();
            assert!((s - 1.0).abs() < 1e-6, "transition row should sum to 1");
        }
    }
}
