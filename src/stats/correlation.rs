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

/// Pearson correlation with series `y` shifted by `lag` positions relative to `x`.
///
/// Positive lag: y is shifted forward (y leads x by `lag` bars).
/// Negative lag: y is shifted backward (x leads y by `|lag|` bars).
/// Returns 0.0 if insufficient overlapping data after shift.
pub fn lagged_pearson(x: &[f64], y: &[f64], lag: i32) -> f64 {
    let n = x.len().min(y.len());
    if n < 2 {
        return 0.0;
    }
    let (x_slice, y_slice) = if lag >= 0 {
        let l = lag as usize;
        if l >= n {
            return 0.0;
        }
        (&x[l..n], &y[..n - l])
    } else {
        let l = lag.unsigned_abs() as usize;
        if l >= n {
            return 0.0;
        }
        (&x[..n - l], &y[l..n])
    };
    pearson(x_slice, y_slice)
}

/// Granger causality F-test: does `cause` Granger-cause `effect`?
///
/// Fits two OLS models with `lag_order` lags:
/// - Restricted: effect\[t\] = a0 + a1*effect\[t-1\] + ... + ap*effect\[t-p\]
/// - Unrestricted: effect\[t\] = a0 + ... + ap*effect\[t-p\] + b1*cause\[t-1\] + ... + bp*cause\[t-p\]
///
/// Returns `Some((f_statistic, p_value))` or `None` if the regression is degenerate.
pub fn granger_f_test(cause: &[f64], effect: &[f64], lag_order: usize) -> Option<(f64, f64)> {
    let n = cause.len().min(effect.len());
    if lag_order == 0 || n <= lag_order + 1 {
        return None;
    }

    let t_start = lag_order;
    let n_obs = n - t_start;
    if n_obs < lag_order * 2 + 2 {
        return None; // need enough observations for unrestricted model
    }

    // Build design matrices
    let p = lag_order;

    // Restricted: 1 + p regressors (intercept + p lags of effect)
    let n_restricted = 1 + p;
    // Unrestricted: 1 + 2p regressors (intercept + p lags of effect + p lags of cause)
    let n_unrestricted = 1 + 2 * p;

    let mut x_restricted = Vec::with_capacity(n_obs * n_restricted);
    let mut x_unrestricted = Vec::with_capacity(n_obs * n_unrestricted);
    let mut y_vec = Vec::with_capacity(n_obs);

    for t in t_start..n {
        y_vec.push(effect[t]);

        // Restricted row: [1, effect[t-1], ..., effect[t-p]]
        x_restricted.push(1.0);
        for lag in 1..=p {
            x_restricted.push(effect[t - lag]);
        }

        // Unrestricted row: [1, effect[t-1], ..., effect[t-p], cause[t-1], ..., cause[t-p]]
        x_unrestricted.push(1.0);
        for lag in 1..=p {
            x_unrestricted.push(effect[t - lag]);
        }
        for lag in 1..=p {
            x_unrestricted.push(cause[t - lag]);
        }
    }

    let rss_r = ols_rss(&x_restricted, n_restricted, &y_vec)?;
    let rss_u = ols_rss(&x_unrestricted, n_unrestricted, &y_vec)?;

    if rss_u <= 0.0 {
        return None; // perfect fit, degenerate
    }

    let df1 = p as f64; // additional regressors
    let df2 = n_obs as f64 - n_unrestricted as f64;
    if df2 <= 0.0 {
        return None;
    }

    let f_stat = ((rss_r - rss_u) / df1) / (rss_u / df2);
    if !f_stat.is_finite() || f_stat < 0.0 {
        return None;
    }

    // P-value from F-distribution
    let f_dist = statrs::distribution::FisherSnedecor::new(df1, df2).ok()?;
    let p_value = 1.0 - statrs::distribution::ContinuousCDF::cdf(&f_dist, f_stat);

    Some((f_stat, p_value))
}

/// Solve OLS via SVD decomposition (nalgebra) and return RSS.
///
/// `x_flat` is the design matrix in row-major order, `n_cols` columns per row.
/// SVD is numerically stable for near-collinear regressors, unlike normal equations.
///
/// Returns `None` if the matrix is rank-deficient (singular) or the solve fails,
/// preserving the caller's assumption that `None` means degenerate data.
fn ols_rss(x_flat: &[f64], n_cols: usize, y: &[f64]) -> Option<f64> {
    let n_rows = y.len();
    if n_rows < n_cols {
        return None;
    }

    let x_mat = nalgebra::DMatrix::from_fn(n_rows, n_cols, |r, c| x_flat[r * n_cols + c]);
    let y_vec = nalgebra::DVector::from_row_slice(y);

    // SVD-based least squares: numerically stable even for near-singular X.
    let svd = x_mat.clone().svd(true, true);

    // Check effective rank — if the matrix is rank-deficient, return None so
    // callers (Granger F-test) don't produce meaningless statistics.
    let rank = svd.rank(1e-10);
    if rank < n_cols {
        return None;
    }

    let beta = svd.solve(&y_vec, 1e-12).ok()?;

    // Compute RSS = ||y - X * beta||^2
    let residuals = y_vec - x_mat * beta;
    Some(residuals.dot(&residuals))
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

    #[test]
    fn test_lagged_pearson_zero_lag() {
        let x = [1.0, 2.0, 3.0, 4.0, 5.0];
        let y = [2.0, 4.0, 6.0, 8.0, 10.0];
        let r = lagged_pearson(&x, &y, 0);
        assert!((r - pearson(&x, &y)).abs() < 1e-10);
    }

    #[test]
    fn test_lagged_pearson_positive_lag() {
        // y leads x: shift y forward by 1
        // x = [1, 2, 3, 4, 5], y = [10, 20, 30, 40, 50]
        // lag=1: correlate x[1..5] with y[0..4]
        let x = [1.0, 2.0, 3.0, 4.0, 5.0];
        let y = [10.0, 20.0, 30.0, 40.0, 50.0];
        let r = lagged_pearson(&x, &y, 1);
        // Both are linear so correlation should be ~1
        assert!(r > 0.99, "expected ~1.0, got {r}");
    }

    #[test]
    fn test_lagged_pearson_finds_shifted_peak() {
        // Create a signal where y is a shifted version of x
        let n = 100i32;
        let x: Vec<f64> = (0..n).map(|i| (f64::from(i) * 0.2).sin()).collect();
        // y is x shifted by 3 positions
        let y: Vec<f64> = (0..n).map(|i| ((f64::from(i) - 3.0) * 0.2).sin()).collect();

        // Find peak correlation across lags
        let mut best_lag = 0i32;
        let mut best_r = f64::NEG_INFINITY;
        for lag in -10..=10 {
            let r = lagged_pearson(&x, &y, lag);
            if r > best_r {
                best_r = r;
                best_lag = lag;
            }
        }
        // y = x shifted right by 3 → x leads y → peak at negative lag
        assert_eq!(best_lag, -3, "peak should be at lag=-3, got {best_lag}");
        assert!(
            best_r > 0.95,
            "peak correlation should be high, got {best_r}"
        );
    }

    #[test]
    fn test_lagged_pearson_edge_cases() {
        assert_eq!(lagged_pearson(&[], &[], 0), 0.0);
        assert_eq!(lagged_pearson(&[1.0, 2.0], &[3.0, 4.0], 5), 0.0);
        assert_eq!(lagged_pearson(&[1.0, 2.0], &[3.0, 4.0], -5), 0.0);
    }

    #[test]
    fn test_granger_causal_series() {
        // Create y[t] = 0.5 * y[t-1] + 0.8 * x[t-1] + noise
        let n: usize = 200;
        let mut seed: u64 = 123;
        let mut noise = || -> f64 {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            (seed >> 11) as f64 / (1u64 << 53) as f64 * 0.02 - 0.01
        };

        #[allow(clippy::cast_precision_loss)]
        let x: Vec<f64> = (0..n).map(|i| (i as f64 * 0.1).sin() * 0.05).collect();
        let mut y = vec![0.0; n];
        for t in 1..n {
            y[t] = 0.5 * y[t - 1] + 0.8 * x[t - 1] + noise();
        }

        let result = granger_f_test(&x, &y, 2);
        assert!(result.is_some(), "should produce a result");
        let (f_stat, p_val) = result.unwrap();
        assert!(f_stat > 1.0, "F-stat should be significant, got {f_stat}");
        assert!(p_val < 0.05, "p-value should be < 0.05, got {p_val}");
    }

    #[test]
    fn test_granger_independent_series() {
        // Two independent pseudo-random series should not show Granger causality
        let n: usize = 200;
        let mut seed_x: u64 = 12345;
        let mut seed_y: u64 = 67890;
        let lcg = |seed: &mut u64| -> f64 {
            *seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            (*seed >> 11) as f64 / (1u64 << 53) as f64 * 0.1 - 0.05
        };

        let x: Vec<f64> = (0..n).map(|_| lcg(&mut seed_x)).collect();
        let y: Vec<f64> = (0..n).map(|_| lcg(&mut seed_y)).collect();

        let result = granger_f_test(&x, &y, 2);
        assert!(result.is_some(), "should produce a result");
        let (_f_stat, p_val) = result.unwrap();
        // Independent series: p-value should generally be > 0.05
        assert!(
            p_val > 0.01,
            "independent series should have high p-value, got {p_val}"
        );
    }

    #[test]
    fn test_granger_edge_cases() {
        assert!(granger_f_test(&[1.0, 2.0], &[3.0, 4.0], 0).is_none());
        assert!(granger_f_test(&[1.0], &[2.0], 1).is_none());
    }

    #[test]
    fn test_lagged_pearson_negative_lag() {
        // Negative lag: x leads y
        let x = [1.0, 2.0, 3.0, 4.0, 5.0];
        let y = [10.0, 20.0, 30.0, 40.0, 50.0];
        // lag=-1: correlate x[0..4] with y[1..5]
        let r = lagged_pearson(&x, &y, -1);
        assert!(
            r > 0.99,
            "linear series should correlate at any lag, got {r}"
        );
    }

    #[test]
    fn test_ols_rss_known_regression() {
        // y = 2x + 1, fit with intercept + slope → RSS should be ~0
        let n = 50;
        let mut x_flat = Vec::with_capacity(n * 2);
        let mut y = Vec::with_capacity(n);
        for i in 0..n {
            x_flat.push(1.0); // intercept
            x_flat.push(i as f64); // x
            y.push(2.0 * i as f64 + 1.0); // y = 2x + 1
        }
        let rss = ols_rss(&x_flat, 2, &y);
        assert!(rss.is_some(), "should solve");
        assert!(rss.unwrap() < 1e-10, "perfect fit should have RSS ≈ 0");
    }

    #[test]
    fn test_ols_rss_singular_matrix() {
        // All-identical rows → rank-deficient X^T X.
        // ols_rss checks effective rank and returns None for degenerate matrices,
        // preventing callers (Granger F-test) from producing meaningless results.
        let x_flat = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0]; // 3 rows × 2 cols, all same
        let y = vec![1.0, 2.0, 3.0];
        let rss = ols_rss(&x_flat, 2, &y);
        assert!(rss.is_none(), "singular matrix should return None");
    }

    #[test]
    fn test_ols_rss_svd_solves_2x2() {
        // y = x + 3 for x = [1, 2, 3, 4, 5]
        let x_flat = vec![1.0, 1.0, 1.0, 2.0, 1.0, 3.0, 1.0, 4.0, 1.0, 5.0];
        let y = vec![4.0, 5.0, 6.0, 7.0, 8.0];
        let rss = ols_rss(&x_flat, 2, &y);
        assert!(rss.is_some(), "should solve");
        assert!(rss.unwrap() < 1e-10, "perfect fit should have RSS ≈ 0");
    }

    #[test]
    fn test_ols_rss_high_condition_number() {
        // Three regressors with high condition number: intercept, x, x^2.
        // Not collinear but ill-conditioned for large x.
        let n = 100;
        let mut x_flat = Vec::with_capacity(n * 3);
        let mut y = Vec::with_capacity(n);
        for i in 0..n {
            let xi = i as f64 * 0.1;
            x_flat.push(1.0);
            x_flat.push(xi);
            x_flat.push(xi * xi);
            y.push(1.0 + 2.0 * xi + 0.5 * xi * xi); // y = 1 + 2x + 0.5x^2
        }
        let rss = ols_rss(&x_flat, 3, &y);
        assert!(rss.is_some(), "SVD should handle ill-conditioned matrices");
        assert!(rss.unwrap().is_finite(), "RSS should be finite");
        assert!(
            rss.unwrap() < 1e-6,
            "RSS should be small for exact polynomial fit"
        );
    }

    #[test]
    fn test_ols_rss_truly_collinear_returns_none() {
        // Column 3 = column 2 + tiny epsilon → effectively rank-deficient
        let n = 100;
        let mut x_flat = Vec::with_capacity(n * 3);
        let mut y = Vec::with_capacity(n);
        for i in 0..n {
            let xi = i as f64 * 0.01;
            x_flat.push(1.0);
            x_flat.push(xi);
            x_flat.push(xi + 1e-12);
            y.push(2.0 * xi + 1.0);
        }
        let rss = ols_rss(&x_flat, 3, &y);
        assert!(rss.is_none(), "truly collinear matrix should return None");
    }

    #[test]
    fn test_granger_bidirectional() {
        // Create mutually causal series: x depends on y's past AND y depends on x's past
        let n: usize = 300;
        let mut seed: u64 = 777;
        let noise = |s: &mut u64| -> f64 {
            *s = s.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            (*s >> 11) as f64 / (1u64 << 53) as f64 * 0.02 - 0.01
        };

        let mut x = vec![0.0; n];
        let mut y = vec![0.0; n];
        for t in 1..n {
            x[t] = 0.3 * x[t - 1] + 0.5 * y[t - 1] + noise(&mut seed);
            y[t] = 0.3 * y[t - 1] + 0.5 * x[t - 1] + noise(&mut seed);
        }

        // Both directions should be significant
        let ab = granger_f_test(&x, &y, 2);
        let ba = granger_f_test(&y, &x, 2);
        assert!(ab.is_some() && ba.is_some());
        let (_, p_ab) = ab.unwrap();
        let (_, p_ba) = ba.unwrap();
        assert!(p_ab < 0.05, "x→y should be significant, p={p_ab}");
        assert!(p_ba < 0.05, "y→x should be significant, p={p_ba}");
    }
}
