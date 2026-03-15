//! Correlation and covariance functions for paired data series.

use super::descriptive::{mean, std_dev};

/// Pearson correlation coefficient. Returns 0.0 if series have different lengths,
/// fewer than 2 elements, or zero variance.
pub fn pearson(x: &[f64], y: &[f64]) -> f64 {
    if x.len() != y.len() || x.len() < 2 {
        return 0.0;
    }
    let cov = covariance(x, y);
    let sx = std_dev(x);
    let sy = std_dev(y);
    if sx == 0.0 || sy == 0.0 {
        return 0.0;
    }
    cov / (sx * sy)
}

/// Spearman rank correlation using average (fractional) ranks for ties.
/// Returns 0.0 if series have different lengths or fewer than 2 elements.
pub fn spearman(x: &[f64], y: &[f64]) -> f64 {
    if x.len() != y.len() || x.len() < 2 {
        return 0.0;
    }
    let rx = fractional_ranks(x);
    let ry = fractional_ranks(y);
    pearson(&rx, &ry)
}

/// Sample covariance (n-1 denominator). Returns 0.0 if series differ in length
/// or have fewer than 2 elements.
pub fn covariance(x: &[f64], y: &[f64]) -> f64 {
    if x.len() != y.len() || x.len() < 2 {
        return 0.0;
    }
    let mx = mean(x);
    let my = mean(y);
    let n = x.len() as f64;
    x.iter()
        .zip(y.iter())
        .map(|(xi, yi)| (xi - mx) * (yi - my))
        .sum::<f64>()
        / (n - 1.0)
}

/// Compute fractional (average) ranks, handling ties.
fn fractional_ranks(data: &[f64]) -> Vec<f64> {
    let n = data.len();
    let mut indexed: Vec<(usize, f64)> = data.iter().copied().enumerate().collect();
    indexed.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    let mut ranks = vec![0.0; n];
    let mut i = 0;
    while i < n {
        let mut j = i;
        // Find all elements with same value (ties)
        while j < n && (indexed[j].1 - indexed[i].1).abs() < f64::EPSILON {
            j += 1;
        }
        // Average rank for the tie group (1-based ranks)
        let avg_rank = (i + 1..=j).sum::<usize>() as f64 / (j - i) as f64;
        for item in indexed.iter().take(j).skip(i) {
            ranks[item.0] = avg_rank;
        }
        i = j;
    }
    ranks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pearson_perfect() {
        let x = [1.0, 2.0, 3.0, 4.0, 5.0];
        let y = [2.0, 4.0, 6.0, 8.0, 10.0];
        assert!((pearson(&x, &y) - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_pearson_inverse() {
        let x = [1.0, 2.0, 3.0, 4.0, 5.0];
        let y = [10.0, 8.0, 6.0, 4.0, 2.0];
        assert!((pearson(&x, &y) + 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_pearson_edge_cases() {
        assert_eq!(pearson(&[], &[]), 0.0);
        assert_eq!(pearson(&[1.0], &[2.0]), 0.0);
        assert_eq!(pearson(&[1.0, 2.0], &[1.0]), 0.0); // mismatched length
    }

    #[test]
    fn test_spearman_perfect() {
        let x = [1.0, 2.0, 3.0, 4.0, 5.0];
        let y = [10.0, 20.0, 30.0, 40.0, 50.0];
        assert!((spearman(&x, &y) - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_spearman_with_ties() {
        let x = [1.0, 2.0, 2.0, 4.0];
        let y = [1.0, 3.0, 3.0, 5.0];
        let r = spearman(&x, &y);
        assert!(
            r > 0.9,
            "Expected strong positive correlation with ties, got {r}"
        );
    }

    #[test]
    fn test_covariance() {
        // numpy: np.cov([1,2,3,4,5],[2,4,6,8,10])[0,1] = 5.0
        let x = [1.0, 2.0, 3.0, 4.0, 5.0];
        let y = [2.0, 4.0, 6.0, 8.0, 10.0];
        assert!((covariance(&x, &y) - 5.0).abs() < 1e-10);
    }

    #[test]
    fn test_fractional_ranks() {
        let data = [3.0, 1.0, 4.0, 1.0, 5.0];
        let ranks = fractional_ranks(&data);
        // 1.0 appears twice → tied at positions 1,2 → avg rank 1.5
        assert!((ranks[1] - 1.5).abs() < 1e-10);
        assert!((ranks[3] - 1.5).abs() < 1e-10);
        // 3.0 is rank 3
        assert!((ranks[0] - 3.0).abs() < 1e-10);
    }
}
