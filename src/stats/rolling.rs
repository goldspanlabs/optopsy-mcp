//! Generic rolling window computation.

/// Apply a function over a rolling window of size `window` across the data.
///
/// Returns a `Vec<f64>` of length `data.len()`. The first `window - 1` values
/// are `f64::NAN` (insufficient data for a full window).
///
/// # Arguments
/// * `data` — Input slice
/// * `window` — Window size (must be >= 1)
/// * `f` — Function applied to each window slice, producing a single value
pub fn rolling_apply<F>(data: &[f64], window: usize, f: F) -> Vec<f64>
where
    F: Fn(&[f64]) -> f64,
{
    if window == 0 || data.is_empty() {
        return vec![f64::NAN; data.len()];
    }
    let mut result = Vec::with_capacity(data.len());
    for i in 0..data.len() {
        if i + 1 < window {
            result.push(f64::NAN);
        } else {
            let start = i + 1 - window;
            result.push(f(&data[start..=i]));
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rolling_mean() {
        let data = [1.0, 2.0, 3.0, 4.0, 5.0];
        let result = rolling_apply(&data, 3, |w| w.iter().sum::<f64>() / w.len() as f64);
        assert!(result[0].is_nan());
        assert!(result[1].is_nan());
        assert!((result[2] - 2.0).abs() < 1e-10); // mean(1,2,3)
        assert!((result[3] - 3.0).abs() < 1e-10); // mean(2,3,4)
        assert!((result[4] - 4.0).abs() < 1e-10); // mean(3,4,5)
    }

    #[test]
    fn test_rolling_window_1() {
        let data = [10.0, 20.0, 30.0];
        let result = rolling_apply(&data, 1, |w| w[0]);
        assert_eq!(result, vec![10.0, 20.0, 30.0]);
    }

    #[test]
    fn test_rolling_empty() {
        let result: Vec<f64> = rolling_apply(&[], 3, |_| 0.0);
        assert!(result.is_empty());
    }

    #[test]
    fn test_rolling_window_zero() {
        let data = [1.0, 2.0];
        let result = rolling_apply(&data, 0, |_| 0.0);
        assert!(result[0].is_nan());
        assert!(result[1].is_nan());
    }
}
