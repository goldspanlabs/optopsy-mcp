use polars::prelude::*;

/// A signal function takes a `DataFrame` and returns a boolean Series
/// indicating which rows meet the signal criteria.
pub trait SignalFn: Send + Sync {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError>;
    #[allow(dead_code)]
    fn name(&self) -> &'static str;
}

/// Extract a column from a `DataFrame` as a `Vec<f64>`.
/// Returns an error if the column doesn't exist or can't be cast to f64.
/// Null values are preserved as `f64::NAN` to maintain row alignment.
pub fn column_to_f64(df: &DataFrame, col_name: &str) -> Result<Vec<f64>, PolarsError> {
    let s = df.column(col_name)?.cast(&DataType::Float64)?;
    let ca = s.f64()?;
    Ok(ca.iter().map(|opt| opt.unwrap_or(f64::NAN)).collect())
}

/// Pad a computed indicator series (shorter than the original) with NaN at the front,
/// then produce a boolean Series by applying a predicate to each value.
/// Rows with NaN (from the padding) are set to `false`.
pub fn pad_and_compare(
    values: &[f64],
    original_len: usize,
    pred: impl Fn(f64) -> bool,
    name: &str,
) -> Series {
    debug_assert!(
        values.len() <= original_len,
        "indicator output longer than input: {} > {}",
        values.len(),
        original_len
    );
    let pad = original_len.saturating_sub(values.len());
    let bools: Vec<bool> = std::iter::repeat_n(false, pad)
        .chain(values.iter().map(|&v| pred(v)))
        .collect();
    BooleanChunked::new(name.into(), &bools).into_series()
}

/// Pad indicator output to match original length, filling front with `f64::NAN`.
pub fn pad_series(values: &[f64], original_len: usize) -> Vec<f64> {
    debug_assert!(
        values.len() <= original_len,
        "indicator output longer than input: {} > {}",
        values.len(),
        original_len
    );
    let pad = original_len.saturating_sub(values.len());
    let mut result = vec![f64::NAN; pad];
    result.extend_from_slice(values);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_to_f64_basic() {
        let df = df! {
            "price" => &[1.0, 2.5, 3.0]
        }
        .unwrap();
        let vals = column_to_f64(&df, "price").unwrap();
        assert_eq!(vals, vec![1.0, 2.5, 3.0]);
    }

    #[test]
    fn column_to_f64_integer_column() {
        let df = df! {
            "count" => &[1i64, 2, 3]
        }
        .unwrap();
        let vals = column_to_f64(&df, "count").unwrap();
        assert_eq!(vals, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn column_to_f64_missing_column() {
        let df = df! {
            "price" => &[1.0, 2.0]
        }
        .unwrap();
        assert!(column_to_f64(&df, "nonexistent").is_err());
    }

    #[test]
    fn column_to_f64_nulls_become_nan() {
        let s = Series::new("val".into(), &[Some(1.0), None, Some(3.0)]);
        let df = DataFrame::new(3, vec![s.into()]).unwrap();
        let vals = column_to_f64(&df, "val").unwrap();
        assert_eq!(vals[0], 1.0);
        assert!(vals[1].is_nan());
        assert_eq!(vals[2], 3.0);
    }

    #[test]
    fn pad_and_compare_basic() {
        let values = [10.0, 20.0, 30.0];
        let result = pad_and_compare(&values, 5, |v| v > 15.0, "test");
        let bools = result.bool().unwrap();
        // First 2 positions are padded with false
        assert!(!bools.get(0).unwrap());
        assert!(!bools.get(1).unwrap());
        // values: 10 -> false, 20 -> true, 30 -> true
        assert!(!bools.get(2).unwrap());
        assert!(bools.get(3).unwrap());
        assert!(bools.get(4).unwrap());
    }

    #[test]
    fn pad_and_compare_no_padding_needed() {
        let values = [1.0, 2.0, 3.0];
        let result = pad_and_compare(&values, 3, |v| v >= 2.0, "test");
        let bools = result.bool().unwrap();
        assert!(!bools.get(0).unwrap());
        assert!(bools.get(1).unwrap());
        assert!(bools.get(2).unwrap());
    }

    #[test]
    fn pad_and_compare_empty_values() {
        let values: [f64; 0] = [];
        let result = pad_and_compare(&values, 3, |_| true, "test");
        let bools = result.bool().unwrap();
        assert_eq!(bools.len(), 3);
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn pad_series_basic() {
        let values = [10.0, 20.0];
        let result = pad_series(&values, 4);
        assert_eq!(result.len(), 4);
        assert!(result[0].is_nan());
        assert!(result[1].is_nan());
        assert_eq!(result[2], 10.0);
        assert_eq!(result[3], 20.0);
    }

    #[test]
    fn pad_series_same_length() {
        let values = [1.0, 2.0, 3.0];
        let result = pad_series(&values, 3);
        assert_eq!(result, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn pad_series_empty() {
        let values: [f64; 0] = [];
        let result = pad_series(&values, 3);
        assert_eq!(result.len(), 3);
        assert!(result.iter().all(|v| v.is_nan()));
    }
}
