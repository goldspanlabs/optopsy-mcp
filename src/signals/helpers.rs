use polars::prelude::*;

/// A signal function takes a `DataFrame` and returns a boolean Series
/// indicating which rows meet the signal criteria
#[allow(dead_code)]
pub trait SignalFn: Send + Sync {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError>;
    fn name(&self) -> &str;
}
