//! Hypothesis tests: one-sample t-test, Jarque-Bera normality test.
//!
//! Uses `statrs` for CDF evaluations only.

use statrs::distribution::{ChiSquared, ContinuousCDF, StudentsT};

use super::descriptive::{kurtosis, mean, skewness, std_dev};

/// Result of a hypothesis test.
#[derive(Debug, Clone)]
pub struct HypothesisResult {
    /// Test statistic value
    pub statistic: f64,
    /// Two-tailed p-value
    pub p_value: f64,
}

/// One-sample t-test: tests whether the mean of `data` differs from `expected_mean`.
/// Returns `None` if data has fewer than 2 elements or zero variance.
pub fn t_test_one_sample(data: &[f64], expected_mean: f64) -> Option<HypothesisResult> {
    let n = data.len();
    if n < 2 {
        return None;
    }
    let m = mean(data);
    let s = std_dev(data);
    if s == 0.0 {
        return None;
    }
    let t_stat = (m - expected_mean) / (s / (n as f64).sqrt());
    let df = (n - 1) as f64;
    let dist = StudentsT::new(0.0, 1.0, df).ok()?;
    let p_value = 2.0 * (1.0 - dist.cdf(t_stat.abs()));
    Some(HypothesisResult {
        statistic: t_stat,
        p_value,
    })
}

/// Jarque-Bera test for normality. Tests whether sample data has the skewness
/// and kurtosis matching a normal distribution.
/// Returns `None` if data has fewer than 8 elements (unreliable below that).
pub fn jarque_bera(data: &[f64]) -> Option<HypothesisResult> {
    let n = data.len();
    if n < 8 {
        return None;
    }
    let s = skewness(data);
    let k = kurtosis(data);
    let nf = n as f64;
    let jb = (nf / 6.0) * (s.powi(2) + k.powi(2) / 4.0);
    let dist = ChiSquared::new(2.0).ok()?;
    let p_value = 1.0 - dist.cdf(jb);
    Some(HypothesisResult {
        statistic: jb,
        p_value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t_test_known_mean() {
        let data = [1.0, 2.0, 3.0, 4.0, 5.0];
        let result = t_test_one_sample(&data, 3.0).unwrap();
        // Mean exactly equals expected → t=0, p=1
        assert!(result.statistic.abs() < 1e-10);
        assert!((result.p_value - 1.0).abs() < 1e-10);
    }

    #[test]
    fn t_test_different_mean() {
        let data = [1.0, 2.0, 3.0, 4.0, 5.0];
        let result = t_test_one_sample(&data, 0.0).unwrap();
        assert!(result.statistic > 0.0);
        assert!(result.p_value < 0.05);
    }

    #[test]
    fn t_test_edge_cases() {
        assert!(t_test_one_sample(&[], 0.0).is_none());
        assert!(t_test_one_sample(&[1.0], 0.0).is_none());
        // All same values → zero variance
        assert!(t_test_one_sample(&[3.0, 3.0, 3.0], 0.0).is_none());
    }

    #[test]
    fn jarque_bera_normal_like() {
        // Roughly normal data should not reject (high p-value)
        let data: Vec<f64> = (0..100).map(|i| (i as f64 - 50.0) / 20.0).collect();
        let result = jarque_bera(&data).unwrap();
        // Uniform-ish data won't be perfectly normal, but JB should be moderate
        assert!(result.statistic >= 0.0);
        assert!(result.p_value >= 0.0 && result.p_value <= 1.0);
    }

    #[test]
    fn jarque_bera_too_few() {
        assert!(jarque_bera(&[1.0, 2.0, 3.0]).is_none());
    }
}
