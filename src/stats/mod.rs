//! Shared statistical primitives for analysis tools.
//!
//! All functions operate on `&[f64]` slices for zero-copy access from Polars columns.
//! Uses `statrs` only for CDF evaluations (t-distribution, chi-squared); all
//! descriptive statistics are hand-rolled for simplicity and zero allocation overhead.

pub mod correlation;
pub mod descriptive;
pub mod histogram;
pub mod hypothesis;
pub mod rolling;

pub use correlation::{covariance, pearson, spearman};
pub use descriptive::{kurtosis, mean, median, percentile, skewness, std_dev};
pub use histogram::{histogram, HistogramBucket};
pub use hypothesis::{jarque_bera, t_test_one_sample, HypothesisResult};
pub use rolling::rolling_apply;
