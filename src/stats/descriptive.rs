//! Descriptive statistics: mean, standard deviation, median, percentile, skewness, kurtosis.

/// Arithmetic mean. Returns 0.0 for empty slices.
pub fn mean(data: &[f64]) -> f64 {
    if data.is_empty() {
        return 0.0;
    }
    data.iter().sum::<f64>() / data.len() as f64
}

/// Sample standard deviation (n-1 denominator). Returns 0.0 for fewer than 2 elements.
pub fn std_dev(data: &[f64]) -> f64 {
    if data.len() < 2 {
        return 0.0;
    }
    let m = mean(data);
    let variance = data.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (data.len() - 1) as f64;
    variance.sqrt()
}

/// Median value. Returns 0.0 for empty slices. Filters NaN values before sorting.
pub fn median(data: &[f64]) -> f64 {
    let mut sorted: Vec<f64> = data.iter().copied().filter(|x| x.is_finite()).collect();
    if sorted.is_empty() {
        return 0.0;
    }
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mid = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        f64::midpoint(sorted[mid - 1], sorted[mid])
    } else {
        sorted[mid]
    }
}

/// Percentile (0-100 scale) using linear interpolation. Returns 0.0 for empty slices.
/// Clamps `pct` to [0.0, 100.0] to prevent out-of-bounds panics.
pub fn percentile(data: &[f64], pct: f64) -> f64 {
    let mut sorted: Vec<f64> = data.iter().copied().filter(|x| x.is_finite()).collect();
    if sorted.is_empty() {
        return 0.0;
    }
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = sorted.len();
    if n == 1 {
        return sorted[0];
    }
    let pct = pct.clamp(0.0, 100.0);
    let rank = (pct / 100.0) * (n - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper {
        sorted[lower]
    } else {
        let frac = rank - lower as f64;
        sorted[lower] * (1.0 - frac) + sorted[upper] * frac
    }
}

/// Sample skewness (Fisher's definition, bias-corrected). Returns 0.0 for fewer than 3 elements.
pub fn skewness(data: &[f64]) -> f64 {
    let n = data.len();
    if n < 3 {
        return 0.0;
    }
    let m = mean(data);
    let s = std_dev(data);
    if s == 0.0 {
        return 0.0;
    }
    let nf = n as f64;
    let m3 = data.iter().map(|x| ((x - m) / s).powi(3)).sum::<f64>() / nf;
    // Bias correction factor
    let correction = (nf * (nf - 1.0)).sqrt() / (nf - 2.0);
    correction * m3
}

/// Excess kurtosis (Fisher's definition, bias-corrected). Returns 0.0 for fewer than 4 elements.
pub fn kurtosis(data: &[f64]) -> f64 {
    let n = data.len();
    if n < 4 {
        return 0.0;
    }
    let m = mean(data);
    let s = std_dev(data);
    if s == 0.0 {
        return 0.0;
    }
    let nf = n as f64;
    let m4 = data.iter().map(|x| ((x - m) / s).powi(4)).sum::<f64>() / nf;
    // Bias-corrected excess kurtosis
    let term1 = (nf - 1.0) / ((nf - 2.0) * (nf - 3.0));
    let term2 = (nf + 1.0) * m4 - 3.0 * (nf - 1.0);
    term1 * term2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mean() {
        assert_eq!(mean(&[]), 0.0);
        assert_eq!(mean(&[5.0]), 5.0);
        assert!((mean(&[1.0, 2.0, 3.0, 4.0, 5.0]) - 3.0).abs() < 1e-10);
    }

    #[test]
    fn test_std_dev() {
        assert_eq!(std_dev(&[]), 0.0);
        assert_eq!(std_dev(&[5.0]), 0.0);
        // numpy: np.std([1,2,3,4,5], ddof=1) = 1.5811388300841898
        let s = std_dev(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        assert!((s - 1.581_138_830_084_189_8).abs() < 1e-10);
    }

    #[test]
    fn test_median() {
        assert_eq!(median(&[]), 0.0);
        assert_eq!(median(&[3.0, 1.0, 2.0]), 2.0);
        assert_eq!(median(&[4.0, 1.0, 3.0, 2.0]), 2.5);
    }

    #[test]
    fn test_percentile() {
        assert_eq!(percentile(&[], 50.0), 0.0);
        let data = [1.0, 2.0, 3.0, 4.0, 5.0];
        assert!((percentile(&data, 0.0) - 1.0).abs() < 1e-10);
        assert!((percentile(&data, 100.0) - 5.0).abs() < 1e-10);
        assert!((percentile(&data, 50.0) - 3.0).abs() < 1e-10);
        // 25th percentile: np.percentile([1,2,3,4,5], 25) = 2.0
        assert!((percentile(&data, 25.0) - 2.0).abs() < 1e-10);
    }

    #[test]
    fn test_skewness() {
        assert_eq!(skewness(&[]), 0.0);
        assert_eq!(skewness(&[1.0, 2.0]), 0.0);
        // Symmetric distribution → skewness ≈ 0
        let symmetric = [1.0, 2.0, 3.0, 4.0, 5.0];
        assert!(skewness(&symmetric).abs() < 1e-10);
    }

    #[test]
    fn test_kurtosis() {
        assert_eq!(kurtosis(&[]), 0.0);
        assert_eq!(kurtosis(&[1.0, 2.0, 3.0]), 0.0);
        // Uniform-like: excess kurtosis should be negative
        let data = [1.0, 2.0, 3.0, 4.0, 5.0];
        let k = kurtosis(&data);
        assert!(
            k < 0.0,
            "Uniform-like data should have negative excess kurtosis, got {k}"
        );
    }

    #[test]
    fn test_identical_values() {
        let data = [3.0, 3.0, 3.0, 3.0, 3.0];
        assert_eq!(std_dev(&data), 0.0);
        assert_eq!(skewness(&data), 0.0);
        assert_eq!(kurtosis(&data), 0.0);
    }
}
