// Momentum signals: Stochastic (compute function for formula DSL)

/// Computes the stochastic oscillator values over a rolling window.
/// Formula: (close - `lowest_low`) / (`highest_high` - `lowest_low`) * 100
pub(crate) fn compute_stochastic(
    close: &[f64],
    high: &[f64],
    low: &[f64],
    period: usize,
) -> Vec<f64> {
    let n = close.len();
    if n < period {
        return vec![];
    }
    (0..=n.saturating_sub(period))
        .map(|i| {
            let end = i + period;
            let close_last = close[end - 1];
            let lowest_low = low[i..end].iter().copied().fold(f64::INFINITY, f64::min);
            let highest_high = high[i..end]
                .iter()
                .copied()
                .fold(f64::NEG_INFINITY, f64::max);
            if (highest_high - lowest_low).abs() < f64::EPSILON {
                0.0
            } else {
                100.0 * (close_last - lowest_low) / (highest_high - lowest_low)
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_stochastic_basic() {
        // With a simple uptrend, the stochastic should be near 100
        let close = vec![100.0, 101.0, 102.0, 103.0, 104.0];
        let high = vec![101.0, 102.0, 103.0, 104.0, 105.0];
        let low = vec![99.0, 100.0, 101.0, 102.0, 103.0];
        let result = compute_stochastic(&close, &high, &low, 3);
        assert!(!result.is_empty());
        // Last value: close=104, lowest_low=101, highest_high=105
        // (104 - 101) / (105 - 101) * 100 = 75.0
        let last = *result.last().unwrap();
        assert!((last - 75.0).abs() < 1.0);
    }

    #[test]
    fn compute_stochastic_insufficient() {
        let result = compute_stochastic(&[100.0], &[101.0], &[99.0], 5);
        assert!(result.is_empty());
    }

    #[test]
    fn compute_stochastic_flat_range() {
        // When high == low, should return 0
        let close = vec![100.0, 100.0, 100.0];
        let high = vec![100.0, 100.0, 100.0];
        let low = vec![100.0, 100.0, 100.0];
        let result = compute_stochastic(&close, &high, &low, 2);
        assert!(result.iter().all(|&v| v == 0.0));
    }
}
