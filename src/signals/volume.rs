// Volume compute functions: typical price, CMF (for formula DSL)

/// Computes typical price as `(high + low + close) / 3` for each row.
pub(crate) fn compute_typical_price(high: &[f64], low: &[f64], close: &[f64]) -> Vec<f64> {
    high.iter()
        .zip(low.iter())
        .zip(close.iter())
        .map(|((h, l), c)| (h + l + c) / 3.0)
        .collect()
}

/// Computes the Chaikin Money Flow values over a rolling window.
/// CMF = `sum(money_flow_volume)` / sum(volume) for each window of `period`.
pub(crate) fn compute_cmf(
    close: &[f64],
    high: &[f64],
    low: &[f64],
    volume: &[f64],
    period: usize,
) -> Vec<f64> {
    let n = close.len();
    if n < period {
        return vec![];
    }
    let mfv: Vec<f64> = (0..n)
        .map(|i| {
            let range = high[i] - low[i];
            if range.abs() < f64::EPSILON {
                0.0
            } else {
                ((close[i] - low[i]) - (high[i] - close[i])) / range * volume[i]
            }
        })
        .collect();
    (0..=n - period)
        .map(|i| {
            let end = i + period;
            let mfv_sum: f64 = mfv[i..end].iter().sum();
            let vol_sum: f64 = volume[i..end].iter().sum();
            if vol_sum == 0.0 {
                0.0
            } else {
                mfv_sum / vol_sum
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_cmf_basic() {
        let close = vec![102.0, 104.0, 103.0, 105.0, 107.0];
        let high = vec![103.0, 105.0, 104.0, 106.0, 108.0];
        let low = vec![100.0, 102.0, 101.0, 103.0, 105.0];
        let volume = vec![1000.0, 1500.0, 1200.0, 900.0, 1300.0];
        let result = compute_cmf(&close, &high, &low, &volume, 3);
        // 5 bars, window=3 → 3 CMF values (indices 0..=2)
        assert_eq!(result.len(), 3);
        // Hand-calculate first window (bars 0-2):
        //   MFV[0] = ((102-100)-(103-102))/(103-100) * 1000 = (2-1)/3 * 1000 = 333.33
        //   MFV[1] = ((104-102)-(105-104))/(105-102) * 1500 = (2-1)/3 * 1500 = 500.00
        //   MFV[2] = ((103-101)-(104-103))/(104-101) * 1200 = (2-1)/3 * 1200 = 400.00
        //   CMF = (333.33 + 500.00 + 400.00) / (1000 + 1500 + 1200) = 1233.33 / 3700 ≈ 0.3333
        assert!(
            (result[0] - 1.0 / 3.0).abs() < 1e-10,
            "first CMF window should be ~0.3333, got {}",
            result[0]
        );
        // All values should be in [-1, 1] by construction
        for (i, &v) in result.iter().enumerate() {
            assert!(
                (-1.0..=1.0).contains(&v),
                "CMF[{i}] = {v} should be in [-1, 1]"
            );
        }
    }

    #[test]
    fn compute_cmf_insufficient() {
        let result = compute_cmf(&[100.0], &[102.0], &[98.0], &[1000.0], 5);
        assert!(result.is_empty());
    }

    #[test]
    fn compute_typical_price_basic() {
        let high = vec![110.0, 120.0];
        let low = vec![90.0, 100.0];
        let close = vec![100.0, 110.0];
        let tp = compute_typical_price(&high, &low, &close);
        assert_eq!(tp, vec![100.0, 110.0]);
    }
}
