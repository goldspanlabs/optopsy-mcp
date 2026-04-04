//! Permutation test for backtest significance.
//!
//! Uses a **sign-flip** null hypothesis: under the null, each trade's P&L is
//! equally likely to be positive or negative (i.e., the strategy has no edge).
//! Each permutation randomly flips the sign of each trade P&L, recomputes the
//! objective metric, and the p-value is the fraction of permuted metrics that
//! meet or exceed the observed value.
//!
//! Designed as a composable gate that transforms a `SweepResponse` without
//! coupling to the sweep engine itself.

use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use rayon::prelude::*;

use crate::constants::{MAX_PROFIT_FACTOR, P_VALUE_THRESHOLD};
use crate::engine::multiple_comparisons;
use crate::tools::response_types::sweep::SweepResponse;

/// Minimum number of trades required for a meaningful permutation test.
/// Below this threshold the null distribution is too coarse.
const MIN_TRADES: usize = 10;

/// Maximum allowed permutations to prevent resource exhaustion.
const MAX_PERMUTATIONS: usize = 100_000;

// ─────────────────────────────────────────────────────────────────────────────
// Core: compute p-value from trade P&Ls via sign-flip test
// ─────────────────────────────────────────────────────────────────────────────

/// Minimum permutation count to justify parallelism overhead.
const PAR_THRESHOLD: usize = 1_000;

/// Run sign-flip permutations for indices `[start_idx, start_idx + count)`, each
/// seeded deterministically from `base_seed + perm_index`. Returns how many
/// permuted metrics meet or exceed `observed_metric`.
///
/// When `base_seed` is `None`, uses OS entropy (non-deterministic).
fn run_signflip_chunk(
    pnls: &[f64],
    observed_metric: f64,
    objective: &str,
    base_seed: Option<u64>,
    start_idx: usize,
    count: usize,
) -> usize {
    // Seed from the first permutation index in this chunk so that each
    // permutation's draws are identical regardless of how chunks are split.
    let mut rng = match base_seed {
        Some(s) => StdRng::seed_from_u64(s.wrapping_add(start_idx as u64)),
        None => StdRng::from_os_rng(),
    };
    let mut flipped = pnls.to_vec();
    let mut hits = 0usize;
    for _ in 0..count {
        for (i, &original) in pnls.iter().enumerate() {
            flipped[i] = if rng.random_bool(0.5) {
                -original
            } else {
                original
            };
        }
        if compute_metric_from_pnls(&flipped, objective) >= observed_metric {
            hits += 1;
        }
    }
    hits
}

/// Compute a p-value sequentially (no internal rayon). Used when the caller
/// already parallelizes at a higher level (e.g., across combos).
fn permutation_p_value_seq(
    pnls: &[f64],
    observed_metric: f64,
    objective: &str,
    n_perms: usize,
    seed: Option<u64>,
) -> f64 {
    if pnls.len() < MIN_TRADES || n_perms == 0 {
        return 1.0;
    }
    let count_ge = run_signflip_chunk(pnls, observed_metric, objective, seed, 0, n_perms);
    (count_ge as f64 + 1.0) / (n_perms as f64 + 1.0)
}

/// Compute a one-sided p-value using a sign-flip permutation test.
///
/// For each permutation, randomly flips the sign of each trade P&L (with 50%
/// probability), recomputes the objective metric, and counts how often the
/// permuted metric meets or exceeds the observed value. This tests the null
/// hypothesis that the strategy has no directional edge.
///
/// When a `seed` is provided, results are deterministic and independent of the
/// Rayon thread pool size — each chunk seeds its RNG from `seed + start_index`,
/// producing identical draws regardless of how permutations are partitioned.
pub fn permutation_p_value(
    pnls: &[f64],
    observed_metric: f64,
    objective: &str,
    n_perms: usize,
    seed: Option<u64>,
) -> f64 {
    if pnls.len() < MIN_TRADES || n_perms == 0 {
        return 1.0;
    }

    let count_ge = if n_perms >= PAR_THRESHOLD {
        // Cap chunks so each has meaningful work (avoids empty tasks on large pools).
        let n_chunks = rayon::current_num_threads().min(n_perms).max(1);
        let chunk_size = n_perms / n_chunks;
        let remainder = n_perms % n_chunks;

        (0..n_chunks)
            .into_par_iter()
            .map(|t| {
                let perms_this_chunk = chunk_size + usize::from(t < remainder);
                // Compute the starting permutation index for this chunk.
                let start_idx = t * chunk_size + t.min(remainder);
                run_signflip_chunk(
                    pnls,
                    observed_metric,
                    objective,
                    seed,
                    start_idx,
                    perms_this_chunk,
                )
            })
            .sum::<usize>()
    } else {
        run_signflip_chunk(pnls, observed_metric, objective, seed, 0, n_perms)
    };

    // Add 1 to numerator and denominator (conservative estimator)
    (count_ge as f64 + 1.0) / (n_perms as f64 + 1.0)
}

/// Compute an objective metric directly from a vector of trade P&Ls.
///
/// These are intentionally simple, non-annualized versions — the same function
/// computes both the observed and permuted metrics, so relative comparison is
/// consistent regardless of annualization differences with the sweep engine.
pub fn compute_metric_from_pnls(pnls: &[f64], objective: &str) -> f64 {
    if pnls.is_empty() {
        return 0.0;
    }

    match objective {
        "sortino" => sortino_from_pnls(pnls),
        "calmar" => calmar_from_pnls(pnls),
        "profit_factor" => profit_factor_from_pnls(pnls),
        _ => sharpe_from_pnls(pnls), // default: sharpe
    }
}

/// Sharpe ratio from trade P&Ls: `mean / std_dev`.
fn sharpe_from_pnls(pnls: &[f64]) -> f64 {
    let n = pnls.len();
    if n < 2 {
        return 0.0;
    }
    let nf = n as f64;
    let mean = pnls.iter().sum::<f64>() / nf;
    let variance = pnls.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (nf - 1.0);
    let std = variance.sqrt();
    if std < 1e-12 {
        return 0.0;
    }
    mean / std
}

/// Sortino ratio from trade P&Ls: `mean / downside_deviation`.
fn sortino_from_pnls(pnls: &[f64]) -> f64 {
    let n = pnls.len() as f64;
    let mean = pnls.iter().sum::<f64>() / n;
    let downside_sq_sum: f64 = pnls.iter().filter(|&&x| x < 0.0).map(|x| x.powi(2)).sum();
    let downside_count = pnls.iter().filter(|&&x| x < 0.0).count();
    if downside_count == 0 {
        return if mean > 0.0 { f64::MAX } else { 0.0 };
    }
    let downside_dev = (downside_sq_sum / downside_count as f64).sqrt();
    if downside_dev < 1e-12 {
        return 0.0;
    }
    mean / downside_dev
}

/// Calmar ratio from trade P&Ls: `total_return / max_drawdown` of cumulative P&L.
fn calmar_from_pnls(pnls: &[f64]) -> f64 {
    let total: f64 = pnls.iter().sum();
    let mut cumulative = 0.0;
    let mut peak = 0.0_f64;
    let mut max_dd = 0.0_f64;

    for &pnl in pnls {
        cumulative += pnl;
        if cumulative > peak {
            peak = cumulative;
        }
        let dd = peak - cumulative;
        if dd > max_dd {
            max_dd = dd;
        }
    }

    if max_dd < 1e-12 {
        return if total > 0.0 { f64::MAX } else { 0.0 };
    }
    total / max_dd
}

/// Profit factor from trade P&Ls: `sum(winners) / |sum(losers)|`.
fn profit_factor_from_pnls(pnls: &[f64]) -> f64 {
    let gross_profit: f64 = pnls.iter().filter(|&&x| x > 0.0).sum();
    let gross_loss: f64 = pnls.iter().filter(|&&x| x < 0.0).sum::<f64>().abs();
    if gross_loss < 1e-12 {
        return if gross_profit > 0.0 {
            MAX_PROFIT_FACTOR
        } else {
            0.0
        };
    }
    gross_profit / gross_loss
}

// ─────────────────────────────────────────────────────────────────────────────
// Gate: enrich a SweepResponse with permutation p-values
// ─────────────────────────────────────────────────────────────────────────────

/// Clamp `num_permutations` to the allowed range.
pub fn clamp_permutations(n: usize) -> usize {
    n.min(MAX_PERMUTATIONS)
}

/// Apply the permutation gate to a completed sweep response.
///
/// For each combo with sufficient trade data, extracts trade P&Ls from
/// `full_results`, runs a sign-flip permutation test, populates `p_value` and
/// `significant` on each `SweepResult`, and attaches BH-FDR and Bonferroni
/// corrections to the response.
///
/// Combos with fewer than `MIN_TRADES` trades receive `p_value = None` and are
/// excluded from multiple comparisons corrections.
///
/// Returns the response unchanged if `n_perms == 0`, results are empty, or
/// `full_results` is empty.
pub fn apply_permutation_gate(
    mut response: SweepResponse,
    n_perms: usize,
    objective: &str,
    seed: Option<u64>,
) -> SweepResponse {
    if n_perms == 0 || response.ranked_results.is_empty() || response.full_results.is_empty() {
        return response;
    }

    let n_perms = clamp_permutations(n_perms);
    let n_combos = response.ranked_results.len();

    // Phase 1: Extract trade P&Ls per combo (cheap, sequential).
    let combo_pnls: Vec<Vec<f64>> = (0..n_combos)
        .map(|i| {
            if i < response.full_results.len() {
                response.full_results[i]
                    .result
                    .trade_log
                    .iter()
                    .map(|t| t.pnl)
                    .collect()
            } else {
                Vec::new()
            }
        })
        .collect();

    // Phase 2: Compute p-values — choose a single parallelism level to avoid
    // nested rayon overhead. With multiple combos, parallelize across combos and
    // run each permutation test sequentially. With a single combo, let
    // permutation_p_value parallelize internally across permutations.
    let testable_count = combo_pnls.iter().filter(|p| p.len() >= MIN_TRADES).count();

    let p_values: Vec<Option<f64>> = if testable_count > 1 {
        // Many combos: parallelize across combos, sequential per-combo.
        combo_pnls
            .par_iter()
            .enumerate()
            .map(|(i, pnls)| {
                if pnls.len() < MIN_TRADES {
                    return None;
                }
                let observed = compute_metric_from_pnls(pnls, objective);
                let combo_seed = seed.map(|s| s.wrapping_add(i as u64));
                Some(permutation_p_value_seq(
                    pnls, observed, objective, n_perms, combo_seed,
                ))
            })
            .collect()
    } else {
        // Single combo (or none): let permutation_p_value parallelize internally.
        combo_pnls
            .iter()
            .enumerate()
            .map(|(i, pnls)| {
                if pnls.len() < MIN_TRADES {
                    return None;
                }
                let observed = compute_metric_from_pnls(pnls, objective);
                let combo_seed = seed.map(|s| s.wrapping_add(i as u64));
                Some(permutation_p_value(
                    pnls, observed, objective, n_perms, combo_seed,
                ))
            })
            .collect()
    };

    // Phase 3: Write p-values onto results (mutable pass)
    for (i, p) in p_values.iter().enumerate() {
        response.ranked_results[i].p_value = *p;
    }

    // Phase 4: Multiple comparisons corrections on tested combos only
    let tested: Vec<(usize, f64)> = p_values
        .iter()
        .enumerate()
        .filter_map(|(i, p)| p.map(|v| (i, v)))
        .collect();

    if tested.is_empty() {
        return response;
    }

    let labels: Vec<String> = tested
        .iter()
        .map(|(i, _)| {
            let rank = response.ranked_results[*i].rank;
            format!("rank_{rank} ({i})")
        })
        .collect();
    let tested_p_values: Vec<f64> = tested.iter().map(|(_, p)| *p).collect();

    let bh = multiple_comparisons::benjamini_hochberg(&labels, &tested_p_values, P_VALUE_THRESHOLD);
    let bonf = multiple_comparisons::bonferroni(&labels, &tested_p_values, P_VALUE_THRESHOLD);

    // Set significance flags based on BH-FDR (less conservative, preferred)
    for (j, &(i, _)) in tested.iter().enumerate() {
        response.ranked_results[i].significant = Some(bh.results[j].is_significant);
    }

    response.multiple_comparisons = Some(vec![bh, bonf]);

    // Update best_result to reflect p-value/significance from ranked_results[0]
    response.best_result = response.ranked_results.first().cloned();

    response
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    // ── compute_metric_from_pnls ────────────────────────────────────

    #[test]
    fn sharpe_positive_pnls() {
        let pnls = vec![100.0, 200.0, 150.0, 50.0, 300.0];
        let s = sharpe_from_pnls(&pnls);
        assert!(s > 0.0, "positive P&Ls should yield positive Sharpe");
    }

    #[test]
    fn sharpe_mixed_pnls() {
        let pnls = vec![100.0, -50.0, 200.0, -100.0, 150.0];
        let s = sharpe_from_pnls(&pnls);
        assert!(s > 0.0);
    }

    #[test]
    fn sharpe_zero_std() {
        let pnls = vec![100.0, 100.0, 100.0];
        let s = sharpe_from_pnls(&pnls);
        assert!((s - 0.0).abs() < 1e-10, "zero std should return 0");
    }

    #[test]
    fn sharpe_empty() {
        assert!((compute_metric_from_pnls(&[], "sharpe") - 0.0).abs() < 1e-10);
    }

    #[test]
    fn sortino_no_losers() {
        let pnls = vec![100.0, 200.0, 300.0];
        let s = sortino_from_pnls(&pnls);
        assert_eq!(s, f64::MAX, "no losers should yield MAX");
    }

    #[test]
    fn sortino_mixed() {
        let pnls = vec![100.0, -50.0, 200.0, -30.0];
        let s = sortino_from_pnls(&pnls);
        assert!(s > 0.0, "net positive with some losers");
    }

    #[test]
    fn profit_factor_no_losers() {
        let pnls = vec![100.0, 200.0];
        let pf = profit_factor_from_pnls(&pnls);
        assert!((pf - MAX_PROFIT_FACTOR).abs() < 1e-10);
    }

    #[test]
    fn profit_factor_mixed() {
        let pnls = vec![200.0, -100.0];
        let pf = profit_factor_from_pnls(&pnls);
        assert!((pf - 2.0).abs() < 1e-10);
    }

    #[test]
    fn calmar_no_drawdown() {
        let pnls = vec![100.0, 100.0, 100.0];
        let c = calmar_from_pnls(&pnls);
        assert_eq!(c, f64::MAX);
    }

    #[test]
    fn calmar_with_drawdown() {
        let pnls = vec![100.0, -100.0, 200.0];
        let c = calmar_from_pnls(&pnls);
        assert!(
            (c - 2.0).abs() < 1e-10,
            "total=200, max_dd=100, calmar=2.0, got {c}"
        );
    }

    #[test]
    fn default_objective_is_sharpe() {
        let pnls = vec![100.0, -50.0, 200.0];
        let s = compute_metric_from_pnls(&pnls, "sharpe");
        let u = compute_metric_from_pnls(&pnls, "unknown_metric");
        assert!(
            (s - u).abs() < 1e-10,
            "unknown objective should default to sharpe"
        );
    }

    // ── sign-flip permutation_p_value ────────────────────────────────

    #[test]
    fn pvalue_strong_edge_low_pvalue() {
        // All-positive P&Ls with high Sharpe — sign-flipping should rarely
        // produce an equally good metric, so p-value should be low.
        let pnls = vec![
            100.0, 120.0, 130.0, 110.0, 140.0, 150.0, 160.0, 105.0, 115.0, 125.0, 135.0, 145.0,
            155.0, 108.0, 118.0, 128.0, 138.0, 148.0, 142.0, 152.0,
        ];
        let observed = compute_metric_from_pnls(&pnls, "sharpe");
        let p = permutation_p_value(&pnls, observed, "sharpe", 1000, Some(42));
        assert!(
            p < 0.05,
            "strong all-positive edge should have low p-value, got {p}"
        );
    }

    #[test]
    fn pvalue_no_edge_high_pvalue() {
        // Symmetric P&Ls around zero — no directional edge.
        let pnls = vec![
            100.0, -100.0, 50.0, -50.0, 80.0, -80.0, 30.0, -30.0, 60.0, -60.0,
        ];
        let observed = compute_metric_from_pnls(&pnls, "sharpe");
        let p = permutation_p_value(&pnls, observed, "sharpe", 1000, Some(42));
        assert!(
            p > 0.3,
            "symmetric P&Ls (no edge) should have high p-value, got {p}"
        );
    }

    #[test]
    fn pvalue_too_few_trades() {
        let pnls = vec![100.0, 200.0, -50.0];
        let observed = compute_metric_from_pnls(&pnls, "sharpe");
        let p = permutation_p_value(&pnls, observed, "sharpe", 1000, Some(42));
        assert!((p - 1.0).abs() < 1e-10, "< MIN_TRADES should return 1.0");
    }

    #[test]
    fn pvalue_zero_perms() {
        let pnls = vec![100.0; 20];
        let p = permutation_p_value(&pnls, 1.0, "sharpe", 0, Some(42));
        assert!((p - 1.0).abs() < 1e-10, "0 perms should return 1.0");
    }

    #[test]
    fn pvalue_deterministic_with_seed() {
        let pnls = vec![
            100.0, -50.0, 200.0, -30.0, 150.0, -80.0, 120.0, -40.0, 180.0, -60.0, 90.0, -20.0,
            160.0, -70.0, 110.0, -45.0, 130.0, -55.0, 140.0, -35.0,
        ];
        let observed = compute_metric_from_pnls(&pnls, "sharpe");
        let p1 = permutation_p_value(&pnls, observed, "sharpe", 500, Some(99));
        let p2 = permutation_p_value(&pnls, observed, "sharpe", 500, Some(99));
        assert!(
            (p1 - p2).abs() < 1e-10,
            "same seed should produce same p-value: {p1} vs {p2}"
        );
    }

    #[test]
    fn pvalue_conservative_estimator() {
        // With the (count+1)/(n+1) formula, p-value is always > 0
        let pnls = vec![
            1000.0, -1.0, 1000.0, -1.0, 1000.0, -1.0, 1000.0, -1.0, 1000.0, -1.0, 1000.0, -1.0,
            1000.0, -1.0, 1000.0, -1.0, 1000.0, -1.0, 1000.0, -1.0,
        ];
        let observed = compute_metric_from_pnls(&pnls, "sharpe");
        let p = permutation_p_value(&pnls, observed, "sharpe", 100, Some(42));
        assert!(
            p > 0.0,
            "conservative estimator: p should never be exactly 0"
        );
    }

    #[test]
    fn clamp_permutations_caps_at_max() {
        assert_eq!(clamp_permutations(1_000_000), MAX_PERMUTATIONS);
        assert_eq!(clamp_permutations(500), 500);
        assert_eq!(clamp_permutations(0), 0);
    }

    // ── apply_permutation_gate ──────────────────────────────────────

    /// Build an empty `SweepResponse` with optional ranked results and no `full_results`.
    fn empty_response(
        ranked: Vec<crate::tools::response_types::sweep::SweepResult>,
    ) -> SweepResponse {
        SweepResponse {
            mode: "grid".into(),
            objective: "sharpe".into(),
            combinations_total: ranked.len(),
            combinations_run: ranked.len(),
            combinations_failed: 0,
            best_result: None,
            ranked_results: ranked,
            dimension_sensitivity: HashMap::default(),
            convergence_trace: None,
            execution_time_ms: 0,
            multiple_comparisons: None,
            full_results: Vec::new(),
        }
    }

    #[test]
    fn gate_noop_when_zero_perms() {
        let response = empty_response(Vec::new());
        let result = apply_permutation_gate(response, 0, "sharpe", None);
        assert!(result.multiple_comparisons.is_none());
    }

    #[test]
    fn gate_noop_when_no_full_results() {
        let response = empty_response(vec![crate::tools::response_types::sweep::SweepResult {
            rank: 1,
            params: HashMap::default(),
            sharpe: 1.0,
            sortino: 1.0,
            pnl: 100.0,
            trades: 10,
            win_rate: 0.6,
            max_drawdown: 0.1,
            profit_factor: 2.0,
            cagr: 0.1,
            calmar: 1.0,
            p_value: None,
            significant: None,
        }]);
        let result = apply_permutation_gate(response, 1000, "sharpe", Some(42));
        assert!(
            result.multiple_comparisons.is_none(),
            "should be no-op with empty full_results"
        );
        assert!(result.ranked_results[0].p_value.is_none());
    }
}
