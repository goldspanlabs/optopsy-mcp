use polars::prelude::*;

/// A signal function takes a `DataFrame` and returns a boolean Series
/// indicating which rows meet the signal criteria.
pub trait SignalFn: Send + Sync {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError>;
    fn name(&self) -> &str;
}

/// Extract a column from a DataFrame as a Vec<f64>.
/// Returns an error if the column doesn't exist or can't be cast to f64.
pub fn column_to_f64(df: &DataFrame, col_name: &str) -> Result<Vec<f64>, PolarsError> {
    let s = df.column(col_name)?.cast(&DataType::Float64)?;
    let ca = s.f64()?;
    Ok(ca.into_no_null_iter().collect())
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
    let pad = original_len.saturating_sub(values.len());
    let bools: Vec<bool> = std::iter::repeat(false)
        .take(pad)
        .chain(values.iter().map(|&v| pred(v)))
        .collect();
    BooleanChunked::new(name.into(), &bools).into_series()
}

/// Pad indicator output to match original length, filling front with f64::NAN.
pub fn pad_series(values: &[f64], original_len: usize) -> Vec<f64> {
    let pad = original_len.saturating_sub(values.len());
    let mut result = vec![f64::NAN; pad];
    result.extend_from_slice(values);
    result
}
