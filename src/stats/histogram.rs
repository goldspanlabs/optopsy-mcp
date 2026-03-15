//! Histogram construction for distribution analysis.

/// A single histogram bucket with its range and statistics.
#[derive(Debug, Clone)]
pub struct HistogramBucket {
    /// Lower bound (inclusive)
    pub lower: f64,
    /// Upper bound (exclusive, except for last bucket which is inclusive)
    pub upper: f64,
    /// Number of values in this bucket
    pub count: usize,
    /// Fraction of total values in this bucket
    pub frequency: f64,
}

/// Build a histogram with `n_bins` equal-width bins from the data.
/// Filters out non-finite values. Returns empty vec for empty/all-NaN data or `n_bins`=0.
pub fn histogram(data: &[f64], n_bins: usize) -> Vec<HistogramBucket> {
    if n_bins == 0 {
        return vec![];
    }
    let finite: Vec<f64> = data.iter().copied().filter(|x| x.is_finite()).collect();
    if finite.is_empty() {
        return vec![];
    }
    let min = finite.iter().copied().fold(f64::INFINITY, f64::min);
    let max = finite.iter().copied().fold(f64::NEG_INFINITY, f64::max);

    // Handle all-same-value case
    if (max - min).abs() < f64::EPSILON {
        return vec![HistogramBucket {
            lower: min,
            upper: max,
            count: finite.len(),
            frequency: 1.0,
        }];
    }

    let width = (max - min) / n_bins as f64;
    let total = finite.len() as f64;
    let mut counts = vec![0usize; n_bins];

    for &v in &finite {
        let idx = ((v - min) / width).floor() as usize;
        // Clamp to last bin (handles max value exactly)
        let idx = idx.min(n_bins - 1);
        counts[idx] += 1;
    }

    counts
        .into_iter()
        .enumerate()
        .map(|(i, count)| HistogramBucket {
            lower: min + i as f64 * width,
            upper: min + (i + 1) as f64 * width,
            count,
            frequency: count as f64 / total,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram_basic() {
        let data = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let bins = histogram(&data, 5);
        assert_eq!(bins.len(), 5);
        let total_count: usize = bins.iter().map(|b| b.count).sum();
        assert_eq!(total_count, 10);
        let total_freq: f64 = bins.iter().map(|b| b.frequency).sum();
        assert!((total_freq - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_histogram_empty() {
        assert!(histogram(&[], 10).is_empty());
        assert!(histogram(&[1.0], 0).is_empty());
    }

    #[test]
    fn test_histogram_single_value() {
        let bins = histogram(&[5.0, 5.0, 5.0], 10);
        assert_eq!(bins.len(), 1);
        assert_eq!(bins[0].count, 3);
    }

    #[test]
    fn test_histogram_max_value_included() {
        // The max value should be in the last bin, not overflow
        let data = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0];
        let bins = histogram(&data, 5);
        let total_count: usize = bins.iter().map(|b| b.count).sum();
        assert_eq!(total_count, 6);
    }
}
