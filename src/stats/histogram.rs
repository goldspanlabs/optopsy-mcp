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

    #[test]
    fn histogram_exact_bin_counts() {
        // Data: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0], 5 bins
        //
        // Hand computation:
        //   min = 1.0, max = 10.0, range = 9.0, width = 9.0 / 5 = 1.8
        //
        //   Bin 0: lower=1.0, upper=2.8  -> [1.0, 2.8): values 1.0, 2.0       -> count=2
        //   Bin 1: lower=2.8, upper=4.6  -> [2.8, 4.6): values 3.0, 4.0       -> count=2
        //   Bin 2: lower=4.6, upper=6.4  -> [4.6, 6.4): values 5.0, 6.0       -> count=2
        //   Bin 3: lower=6.4, upper=8.2  -> [6.4, 8.2): values 7.0, 8.0       -> count=2
        //   Bin 4: lower=8.2, upper=10.0 -> [8.2, 10.0]: values 9.0, 10.0     -> count=2
        //     (10.0 maps to idx = floor((10.0-1.0)/1.8) = floor(5.0) = 5,
        //      clamped to n_bins-1 = 4)
        //
        //   Each frequency = count / total = 2 / 10 = 0.2

        let data = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let bins = histogram(&data, 5);

        assert_eq!(bins.len(), 5);

        let expected_counts = [2, 2, 2, 2, 2];
        let expected_freq = 0.2;
        let width = 1.8;

        for (i, bin) in bins.iter().enumerate() {
            // Verify exact count
            assert_eq!(
                bin.count, expected_counts[i],
                "Bin {i} count: expected {}, got {}",
                expected_counts[i], bin.count
            );

            // Verify exact frequency: count / total = 2 / 10 = 0.2
            assert!(
                (bin.frequency - expected_freq).abs() < 1e-10,
                "Bin {i} frequency: expected {expected_freq}, got {}",
                bin.frequency
            );

            // Verify bin edges: lower = min + i * width, upper = min + (i+1) * width
            let expected_lower = 1.0 + i as f64 * width;
            let expected_upper = 1.0 + (i + 1) as f64 * width;
            assert!(
                (bin.lower - expected_lower).abs() < 1e-10,
                "Bin {i} lower: expected {expected_lower}, got {}",
                bin.lower
            );
            assert!(
                (bin.upper - expected_upper).abs() < 1e-10,
                "Bin {i} upper: expected {expected_upper}, got {}",
                bin.upper
            );
        }
    }

    #[test]
    fn histogram_exact_bin_counts_non_uniform() {
        // Data: [1.0, 1.0, 1.0, 5.0, 10.0], 3 bins — skewed distribution
        //
        // Hand computation:
        //   min = 1.0, max = 10.0, range = 9.0, width = 9.0 / 3 = 3.0
        //
        //   Bin 0: lower=1.0, upper=4.0  -> [1.0, 4.0)
        //     values: 1.0 (idx=floor(0/3)=0), 1.0 (0), 1.0 (0)  -> count=3
        //   Bin 1: lower=4.0, upper=7.0  -> [4.0, 7.0)
        //     values: 5.0 (idx=floor(4/3)=floor(1.333)=1)        -> count=1
        //   Bin 2: lower=7.0, upper=10.0 -> [7.0, 10.0]
        //     values: 10.0 (idx=floor(9/3)=floor(3.0)=3, clamped to 2) -> count=1
        //
        //   Frequencies: 3/5=0.6, 1/5=0.2, 1/5=0.2

        let data = [1.0, 1.0, 1.0, 5.0, 10.0];
        let bins = histogram(&data, 3);

        assert_eq!(bins.len(), 3);

        let expected_counts = [3, 1, 1];
        let expected_freqs = [0.6, 0.2, 0.2];
        let width = 3.0;

        for (i, bin) in bins.iter().enumerate() {
            assert_eq!(
                bin.count, expected_counts[i],
                "Bin {i} count: expected {}, got {}",
                expected_counts[i], bin.count
            );

            assert!(
                (bin.frequency - expected_freqs[i]).abs() < 1e-10,
                "Bin {i} frequency: expected {}, got {}",
                expected_freqs[i],
                bin.frequency
            );

            let expected_lower = 1.0 + i as f64 * width;
            let expected_upper = 1.0 + (i + 1) as f64 * width;
            assert!(
                (bin.lower - expected_lower).abs() < 1e-10,
                "Bin {i} lower: expected {expected_lower}, got {}",
                bin.lower
            );
            assert!(
                (bin.upper - expected_upper).abs() < 1e-10,
                "Bin {i} upper: expected {expected_upper}, got {}",
                bin.upper
            );
        }
    }
}
