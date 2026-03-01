use super::helpers::SignalFn;
use polars::prelude::*;

#[allow(dead_code)]
pub struct AndSignal {
    pub left: Box<dyn SignalFn>,
    pub right: Box<dyn SignalFn>,
}

impl SignalFn for AndSignal {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let left = self.left.evaluate(df)?;
        let right = self.right.evaluate(df)?;
        let left_bool = left.bool()?;
        let right_bool = right.bool()?;
        Ok((left_bool & right_bool).into_series())
    }
    fn name(&self) -> &'static str {
        "and"
    }
}

#[allow(dead_code)]
pub struct OrSignal {
    pub left: Box<dyn SignalFn>,
    pub right: Box<dyn SignalFn>,
}

impl SignalFn for OrSignal {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let left = self.left.evaluate(df)?;
        let right = self.right.evaluate(df)?;
        let left_bool = left.bool()?;
        let right_bool = right.bool()?;
        Ok((left_bool | right_bool).into_series())
    }
    fn name(&self) -> &'static str {
        "or"
    }
}
