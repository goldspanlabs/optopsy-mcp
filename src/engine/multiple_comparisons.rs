//! Multiple comparisons correction for parameter sweep p-values.
//!
//! When testing many strategy/parameter configurations, the probability of at least one
//! false-positive result increases with the number of tests. This module applies
//! Bonferroni and Benjamini-Hochberg (BH-FDR) corrections to control for this.
//!
//! Bonferroni is more conservative (controls family-wise error rate); BH-FDR is
//! less conservative and controls the expected proportion of false discoveries.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A single configuration's corrected p-value entry.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CorrectedPValue {
    /// Label identifying the configuration (e.g. sweep combo label)
    pub label: String,
    /// Raw (unadjusted) p-value from the permutation test
    pub original_p_value: f64,
    /// Adjusted p-value after multiple comparisons correction
    pub adjusted_p_value: f64,
    /// Whether the configuration remains significant after correction (adjusted p < alpha)
    pub is_significant: bool,
}

/// Result of applying one multiple comparisons correction method to a set of p-values.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MultipleComparisonsResult {
    /// Correction method used: `"bonferroni"` or `"benjamini_hochberg"`
    pub method: String,
    /// Significance threshold (alpha), typically 0.05
    pub alpha: f64,
    /// Total number of tests (configurations) evaluated
    pub num_tests: usize,
    /// Number of configurations that remain significant after correction
    pub num_significant: usize,
    /// Per-configuration corrected p-values and significance flags
    pub results: Vec<CorrectedPValue>,
}

/// Apply Bonferroni correction.
///
/// Adjusted p-value = min(p × m, 1.0) where m is the number of tests.
/// A configuration is significant if its adjusted p-value < alpha.
pub fn bonferroni(
    labels: &[String],
    p_values: &[f64],
    alpha: f64,
) -> MultipleComparisonsResult {
    debug_assert_eq!(
        labels.len(),
        p_values.len(),
        "bonferroni: labels and p_values must have the same length ({} vs {})",
        labels.len(),
        p_values.len()
    );
    let m = p_values.len();
    let results: Vec<CorrectedPValue> = labels
        .iter()
        .zip(p_values.iter())
        .map(|(label, &p)| {
            let adjusted = (p * m as f64).min(1.0);
            CorrectedPValue {
                label: label.clone(),
                original_p_value: p,
                adjusted_p_value: adjusted,
                is_significant: adjusted < alpha,
            }
        })
        .collect();

    let num_significant = results.iter().filter(|r| r.is_significant).count();
    MultipleComparisonsResult {
        method: "bonferroni".to_string(),
        alpha,
        num_tests: m,
        num_significant,
        results,
    }
}

/// Apply Benjamini-Hochberg (BH) FDR correction.
///
/// Ranks p-values from smallest to largest; the adjusted p-value for rank k is
/// min over all ranks ≥ k of (`p_k` × m / k), capped at 1.0.
/// A configuration is significant if its adjusted p-value < alpha.
pub fn benjamini_hochberg(
    labels: &[String],
    p_values: &[f64],
    alpha: f64,
) -> MultipleComparisonsResult {
    debug_assert_eq!(
        labels.len(),
        p_values.len(),
        "benjamini_hochberg: labels and p_values must have the same length ({} vs {})",
        labels.len(),
        p_values.len()
    );
    let m = p_values.len();
    if m == 0 {
        return MultipleComparisonsResult {
            method: "benjamini_hochberg".to_string(),
            alpha,
            num_tests: 0,
            num_significant: 0,
            results: vec![],
        };
    }

    // Build sorted indices (ascending p-value order)
    let mut indexed: Vec<(usize, f64)> = p_values
        .iter()
        .copied()
        .enumerate()
        .collect();
    indexed.sort_by(|a, b| {
        a.1.partial_cmp(&b.1)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // BH step-up: traverse from largest rank downward, propagating the running minimum
    let mut adjusted = vec![0.0f64; m];
    let mut running_min = f64::INFINITY;
    for (rank, &(orig_idx, p)) in indexed.iter().enumerate().rev() {
        let rank1 = rank + 1; // 1-based rank
        let bh_adj = (p * m as f64 / rank1 as f64).min(1.0);
        running_min = running_min.min(bh_adj);
        adjusted[orig_idx] = running_min;
    }

    let results: Vec<CorrectedPValue> = labels
        .iter()
        .zip(p_values.iter())
        .enumerate()
        .map(|(i, (label, &p))| CorrectedPValue {
            label: label.clone(),
            original_p_value: p,
            adjusted_p_value: adjusted[i],
            is_significant: adjusted[i] < alpha,
        })
        .collect();

    let num_significant = results.iter().filter(|r| r.is_significant).count();
    MultipleComparisonsResult {
        method: "benjamini_hochberg".to_string(),
        alpha,
        num_tests: m,
        num_significant,
        results,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------------------
    // Bonferroni tests
    // ---------------------------------------------------------------------------

    #[test]
    fn bonferroni_adjusts_p_values_correctly() {
        let labels: Vec<String> = vec!["a".into(), "b".into(), "c".into(), "d".into()];
        let p_values = vec![0.01, 0.04, 0.10, 0.20];
        let result = bonferroni(&labels, &p_values, 0.05);

        assert_eq!(result.method, "bonferroni");
        assert_eq!(result.num_tests, 4);
        assert_eq!(result.alpha, 0.05);

        // Adjusted = p * m = p * 4
        assert!((result.results[0].adjusted_p_value - 0.04).abs() < 1e-10);
        assert!((result.results[1].adjusted_p_value - 0.16).abs() < 1e-10);
        assert!((result.results[2].adjusted_p_value - 0.40).abs() < 1e-10);
        assert!((result.results[3].adjusted_p_value - 0.80).abs() < 1e-10);

        // Only first is significant (0.04 < 0.05)
        assert_eq!(result.num_significant, 1);
        assert!(result.results[0].is_significant);
        assert!(!result.results[1].is_significant);
    }

    #[test]
    fn bonferroni_caps_adjusted_p_value_at_one() {
        let labels: Vec<String> = vec!["a".into(), "b".into()];
        let p_values = vec![0.8, 0.9];
        let result = bonferroni(&labels, &p_values, 0.05);
        for r in &result.results {
            assert!(r.adjusted_p_value <= 1.0);
        }
    }

    #[test]
    fn bonferroni_empty_input() {
        let result = bonferroni(&[], &[], 0.05);
        assert_eq!(result.num_tests, 0);
        assert_eq!(result.num_significant, 0);
        assert!(result.results.is_empty());
    }

    #[test]
    fn bonferroni_single_test_unchanged() {
        let labels: Vec<String> = vec!["x".into()];
        let p_values = vec![0.03];
        let result = bonferroni(&labels, &p_values, 0.05);
        // With 1 test, adjusted = p * 1 = p
        assert!((result.results[0].adjusted_p_value - 0.03).abs() < 1e-10);
        assert!(result.results[0].is_significant);
    }

    // ---------------------------------------------------------------------------
    // Benjamini-Hochberg tests
    // ---------------------------------------------------------------------------

    #[test]
    fn bh_fdr_example_from_literature() {
        // Classic BH example: 4 hypotheses, alpha = 0.05
        // Raw p: 0.001, 0.008, 0.039, 0.041 (already sorted ascending)
        // Ranks:   1       2       3       4     (m = 4)
        // BH crit: 0.05*1/4=0.0125, 0.05*2/4=0.025, 0.05*3/4=0.0375, 0.05*4/4=0.05
        // Reject:  yes,    yes,      no,           no  (standard BH)
        let labels: Vec<String> = vec!["a".into(), "b".into(), "c".into(), "d".into()];
        let p_values = vec![0.001, 0.008, 0.039, 0.041];
        let result = benjamini_hochberg(&labels, &p_values, 0.05);

        assert_eq!(result.method, "benjamini_hochberg");
        assert_eq!(result.num_tests, 4);
        assert_eq!(result.alpha, 0.05);

        // Adjusted p-values should all be ≤ 1
        for r in &result.results {
            assert!(
                r.adjusted_p_value <= 1.0,
                "adjusted_p_value {} > 1.0",
                r.adjusted_p_value
            );
            assert!(r.adjusted_p_value >= 0.0);
        }

        // Smallest p-value should be most significant
        assert!(result.results[0].is_significant);
        // BH is less conservative than Bonferroni; at least 2 should survive
        assert!(result.num_significant >= 2);
    }

    #[test]
    fn bh_fdr_adjusted_p_values_monotone() {
        // Adjusted BH p-values must be non-decreasing when inputs are sorted ascending
        let labels: Vec<String> = vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into()];
        let p_values = vec![0.01, 0.02, 0.05, 0.10, 0.20];
        let result = benjamini_hochberg(&labels, &p_values, 0.05);
        for w in result.results.windows(2) {
            assert!(
                w[0].adjusted_p_value <= w[1].adjusted_p_value + 1e-12,
                "BH adjusted p-values are not monotone: {} > {}",
                w[0].adjusted_p_value,
                w[1].adjusted_p_value,
            );
        }
    }

    #[test]
    fn bh_fdr_empty_input() {
        let result = benjamini_hochberg(&[], &[], 0.05);
        assert_eq!(result.num_tests, 0);
        assert_eq!(result.num_significant, 0);
        assert!(result.results.is_empty());
    }

    #[test]
    fn bh_fdr_single_test_matches_raw_p() {
        let labels: Vec<String> = vec!["x".into()];
        let p_values = vec![0.03];
        let result = benjamini_hochberg(&labels, &p_values, 0.05);
        // With 1 test BH adjusted = p * 1 / 1 = p
        assert!((result.results[0].adjusted_p_value - 0.03).abs() < 1e-10);
        assert!(result.results[0].is_significant);
    }

    #[test]
    fn bh_fdr_all_significant() {
        let labels: Vec<String> = vec!["a".into(), "b".into(), "c".into()];
        let p_values = vec![0.001, 0.002, 0.003];
        let result = benjamini_hochberg(&labels, &p_values, 0.05);
        assert_eq!(result.num_significant, 3);
    }

    #[test]
    fn bh_fdr_none_significant() {
        let labels: Vec<String> = vec!["a".into(), "b".into(), "c".into()];
        let p_values = vec![0.5, 0.6, 0.7];
        let result = benjamini_hochberg(&labels, &p_values, 0.05);
        assert_eq!(result.num_significant, 0);
    }

    #[test]
    fn bh_less_conservative_than_bonferroni() {
        // BH should retain at least as many significant results as Bonferroni
        let labels: Vec<String> = (0..5).map(|i| format!("combo_{i}")).collect();
        let p_values = vec![0.001, 0.01, 0.03, 0.04, 0.05];
        let bon = bonferroni(&labels, &p_values, 0.05);
        let bh = benjamini_hochberg(&labels, &p_values, 0.05);
        assert!(
            bh.num_significant >= bon.num_significant,
            "BH ({}) should retain ≥ significant results as Bonferroni ({})",
            bh.num_significant,
            bon.num_significant
        );
    }

    #[test]
    fn original_p_values_preserved_in_results() {
        let labels: Vec<String> = vec!["a".into(), "b".into()];
        let p_values = vec![0.02, 0.07];
        for result in [
            bonferroni(&labels, &p_values, 0.05),
            benjamini_hochberg(&labels, &p_values, 0.05),
        ] {
            for (r, &orig) in result.results.iter().zip(p_values.iter()) {
                assert!(
                    (r.original_p_value - orig).abs() < 1e-10,
                    "original p-value not preserved"
                );
            }
        }
    }
}
