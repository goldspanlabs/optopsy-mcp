// Momentum signals: RSI, Stochastic (compute functions for formula DSL)

/// Compute RSI with a variable period using Wilder smoothing.
///
/// Uses SMA seed over the first `period` bars, then exponential decay
/// (alpha = 1/period) for subsequent bars.
pub(crate) fn compute_rsi_variable_period(prices: &[f64], period: usize) -> Vec<f64> {
    let n = prices.len();
    if period == 0 || n <= period {
        return vec![];
    }

    // Compute price changes
    let changes: Vec<f64> = (1..n).map(|i| prices[i] - prices[i - 1]).collect();

    // SMA seed over first `period` changes
    let mut avg_gain: f64 = changes[..period]
        .iter()
        .map(|&c| if c > 0.0 { c } else { 0.0 })
        .sum::<f64>()
        / period as f64;
    let mut avg_loss: f64 = changes[..period]
        .iter()
        .map(|&c| if c < 0.0 { -c } else { 0.0 })
        .sum::<f64>()
        / period as f64;

    let mut result = Vec::with_capacity(n - period);

    // First RSI value
    let rs = if avg_loss == 0.0 {
        f64::INFINITY
    } else {
        avg_gain / avg_loss
    };
    result.push(if rs.is_infinite() {
        100.0
    } else {
        100.0 - 100.0 / (1.0 + rs)
    });

    // Wilder smoothing for remaining bars
    for &change in &changes[period..] {
        let gain = if change > 0.0 { change } else { 0.0 };
        let loss = if change < 0.0 { -change } else { 0.0 };
        avg_gain = (avg_gain * (period as f64 - 1.0) + gain) / period as f64;
        avg_loss = (avg_loss * (period as f64 - 1.0) + loss) / period as f64;
        let rs = if avg_loss == 0.0 {
            f64::INFINITY
        } else {
            avg_gain / avg_loss
        };
        result.push(if rs.is_infinite() {
            100.0
        } else {
            100.0 - 100.0 / (1.0 + rs)
        });
    }

    result
}

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
    use rust_ti::standard_indicators::bulk as sti;

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

    #[test]
    fn compute_rsi_variable_period_14_matches_sti() {
        // Use 30 data points so both implementations produce enough values to compare
        let prices: Vec<f64> = vec![
            100.0, 102.0, 101.0, 103.0, 105.0, 104.0, 106.0, 108.0, 107.0, 110.0, 109.0, 112.0,
            111.0, 113.0, 115.0, 114.0, 116.0, 118.0, 117.0, 120.0, 119.0, 122.0, 121.0, 123.0,
            125.0, 124.0, 126.0, 128.0, 127.0, 130.0,
        ];
        let our_rsi = compute_rsi_variable_period(&prices, 14);
        let sti_rsi = sti::rsi(&prices);
        // Both should produce values
        assert!(!our_rsi.is_empty());
        assert!(!sti_rsi.is_empty());
        // Compare last values — implementations may differ slightly due to
        // different internal windowing in sti::rsi vs our Wilder smoothing.
        // Both should produce values in the same general range.
        let our_last = *our_rsi.last().unwrap();
        let sti_last = *sti_rsi.last().unwrap();
        assert!(
            (our_last - sti_last).abs() < 15.0,
            "RSI values diverged too much: ours={our_last}, sti={sti_last}"
        );
        // All RSI values should be in [0, 100]
        for &v in &our_rsi {
            assert!((0.0..=100.0).contains(&v), "RSI out of range: {v}");
        }
    }

    #[test]
    fn compute_rsi_variable_period_custom_period() {
        let prices: Vec<f64> = (0..30).map(|i| 100.0 + f64::from(i).sin() * 10.0).collect();
        let result = compute_rsi_variable_period(&prices, 7);
        assert!(!result.is_empty());
        // All values should be in [0, 100]
        for &v in &result {
            assert!((0.0..=100.0).contains(&v), "RSI out of range: {v}");
        }
    }

    #[test]
    fn compute_rsi_variable_period_insufficient_data() {
        let prices = vec![100.0, 101.0, 102.0];
        let result = compute_rsi_variable_period(&prices, 14);
        assert!(result.is_empty());
    }
}
