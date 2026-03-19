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
    fn test_skewness_exact_asymmetric() {
        // Data: [1.0, 2.0, 2.0, 3.0, 10.0] — right-skewed
        // n = 5, mean = (1+2+2+3+10)/5 = 3.6
        // deviations: [-2.6, -1.6, -1.6, -0.6, 6.4]
        // sum(d^2) = 6.76 + 2.56 + 2.56 + 0.36 + 40.96 = 53.2
        // s = sqrt(53.2 / 4) = sqrt(13.3) = 3.646916505762094
        // z = d / s: [-0.71285, -0.43868, -0.43868, -0.16451, 1.75472]
        // z^3: [-0.36229, -0.08432, -0.08432, -0.00445, 5.40727]
        // m3 = sum(z^3) / n = 4.87189 / 5 = 0.97438
        // correction = sqrt(n*(n-1)) / (n-2) = sqrt(20) / 3 = 1.49071
        // skewness = correction * m3 = 1.49071 * 0.97438 = 1.45162
        let data = [1.0, 2.0, 2.0, 3.0, 10.0];
        let result = skewness(&data);
        assert!(
            (result - 1.451_618_911_406_210_6).abs() < 1e-6,
            "Expected skewness ≈ 1.451619, got {result}"
        );
    }

    #[test]
    fn test_kurtosis_exact_uniform_like() {
        // Data: [1.0, 2.0, 3.0, 4.0, 5.0]
        // n = 5, mean = 3.0
        // s = sqrt(10/4) = 1.5811388300841898 (sample std dev, n-1 denom)
        // z = (x - mean) / s: [-1.26491, -0.63246, 0.0, 0.63246, 1.26491]
        // z^4: [2.56, 0.16, 0.0, 0.16, 2.56]
        // m4 = sum(z^4) / n = 5.44 / 5 = 1.088
        // term1 = (n-1) / ((n-2)*(n-3)) = 4 / (2*3) = 0.66667
        // term2 = (n+1)*m4 - 3*(n-1) = 6*1.088 - 3*4 = 6.528 - 12 = -5.472
        // kurtosis = term1 * term2 = 0.66667 * -5.472 = -3.648
        let data = [1.0, 2.0, 3.0, 4.0, 5.0];
        let result = kurtosis(&data);
        assert!(
            (result - (-3.648)).abs() < 1e-6,
            "Expected kurtosis ≈ -3.648, got {result}"
        );
    }

    #[test]
    fn test_kurtosis_exact_heavy_tail() {
        // Data: [1.0, 1.0, 1.0, 1.0, 10.0] — one outlier, heavy tail
        // n = 5, mean = (1+1+1+1+10)/5 = 2.8
        // deviations: [-1.8, -1.8, -1.8, -1.8, 7.2]
        // sum(d^2) = 4*3.24 + 51.84 = 12.96 + 51.84 = 64.8
        // s = sqrt(64.8 / 4) = sqrt(16.2) = 4.024922359499621
        // z = d / s: [-0.44721, -0.44721, -0.44721, -0.44721, 1.78885]
        // z^4: [0.04, 0.04, 0.04, 0.04, 10.24] (approximately)
        // m4 = sum(z^4) / n: let's be precise:
        //   z_low = -1.8 / 4.024922 = -0.447214
        //   z_low^4 = 0.447214^4 = 0.04
        //   z_high = 7.2 / 4.024922 = 1.788854
        //   z_high^4 = 1.788854^4 = 10.24
        //   m4 = (4 * 0.04 + 10.24) / 5 = 10.4 / 5 = 2.08
        // term1 = 4 / 6 = 0.66667
        // term2 = 6 * 2.08 - 12 = 12.48 - 12 = 0.48
        // kurtosis = 0.66667 * 0.48 = 0.32
        let data = [1.0, 1.0, 1.0, 1.0, 10.0];
        let result = kurtosis(&data);
        assert!(
            (result - 0.32).abs() < 1e-6,
            "Expected kurtosis ≈ 0.32, got {result}"
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
