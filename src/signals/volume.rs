// Volume signals: MFI, OBV, CMF

use super::helpers::{column_to_f64, pad_and_compare, pad_series, SignalFn};
use polars::prelude::*;

/// Computes typical price as `(high + low + close) / 3` for each row.
fn compute_typical_price(high: &[f64], low: &[f64], close: &[f64]) -> Vec<f64> {
    high.iter()
        .zip(low.iter())
        .zip(close.iter())
        .map(|((h, l), c)| (h + l + c) / 3.0)
        .collect()
}

/// Signal: Money Flow Index is below a threshold (oversold by volume-weighted momentum).
/// Typical price is computed internally as `(high + low + close) / 3`.
pub struct MfiOversold {
    pub high_col: String,
    pub low_col: String,
    pub close_col: String,
    pub volume_col: String,
    pub period: usize,
    pub threshold: f64,
}

impl SignalFn for MfiOversold {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let high = column_to_f64(df, &self.high_col)?;
        let low = column_to_f64(df, &self.low_col)?;
        let close = column_to_f64(df, &self.close_col)?;
        let volume = column_to_f64(df, &self.volume_col)?;
        let typical = compute_typical_price(&high, &low, &close);
        let n = typical.len();
        if n < self.period {
            return Ok(BooleanChunked::new("mfi_oversold".into(), vec![false; n]).into_series());
        }
        let mfi_values =
            rust_ti::momentum_indicators::bulk::money_flow_index(&typical, &volume, self.period);
        Ok(pad_and_compare(
            &mfi_values,
            n,
            |v| v < self.threshold,
            "mfi_oversold",
        ))
    }
    fn name(&self) -> &'static str {
        "mfi_oversold"
    }
}

/// Signal: Money Flow Index is above a threshold (overbought).
/// Typical price is computed internally as `(high + low + close) / 3`.
pub struct MfiOverbought {
    pub high_col: String,
    pub low_col: String,
    pub close_col: String,
    pub volume_col: String,
    pub period: usize,
    pub threshold: f64,
}

impl SignalFn for MfiOverbought {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let high = column_to_f64(df, &self.high_col)?;
        let low = column_to_f64(df, &self.low_col)?;
        let close = column_to_f64(df, &self.close_col)?;
        let volume = column_to_f64(df, &self.volume_col)?;
        let typical = compute_typical_price(&high, &low, &close);
        let n = typical.len();
        if n < self.period {
            return Ok(BooleanChunked::new("mfi_overbought".into(), vec![false; n]).into_series());
        }
        let mfi_values =
            rust_ti::momentum_indicators::bulk::money_flow_index(&typical, &volume, self.period);
        Ok(pad_and_compare(
            &mfi_values,
            n,
            |v| v > self.threshold,
            "mfi_overbought",
        ))
    }
    fn name(&self) -> &'static str {
        "mfi_overbought"
    }
}

/// Signal: On-Balance Volume is rising (current OBV > previous OBV).
pub struct ObvRising {
    pub price_col: String,
    pub volume_col: String,
}

impl SignalFn for ObvRising {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.price_col)?;
        let volume = column_to_f64(df, &self.volume_col)?;
        let n = prices.len();
        if n < 2 {
            return Ok(BooleanChunked::new("obv_rising".into(), vec![false; n]).into_series());
        }
        let obv_values =
            rust_ti::momentum_indicators::bulk::on_balance_volume(&prices, &volume, 0.0);
        let padded = pad_series(&obv_values, n);
        let mut bools = vec![false; n];
        for i in 1..n {
            if !padded[i].is_nan() && !padded[i - 1].is_nan() {
                bools[i] = padded[i] > padded[i - 1];
            }
        }
        Ok(BooleanChunked::new("obv_rising".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "obv_rising"
    }
}

/// Signal: On-Balance Volume is falling (current OBV < previous OBV).
pub struct ObvFalling {
    pub price_col: String,
    pub volume_col: String,
}

impl SignalFn for ObvFalling {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.price_col)?;
        let volume = column_to_f64(df, &self.volume_col)?;
        let n = prices.len();
        if n < 2 {
            return Ok(BooleanChunked::new("obv_falling".into(), vec![false; n]).into_series());
        }
        let obv_values =
            rust_ti::momentum_indicators::bulk::on_balance_volume(&prices, &volume, 0.0);
        let padded = pad_series(&obv_values, n);
        let mut bools = vec![false; n];
        for i in 1..n {
            if !padded[i].is_nan() && !padded[i - 1].is_nan() {
                bools[i] = padded[i] < padded[i - 1];
            }
        }
        Ok(BooleanChunked::new("obv_falling".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "obv_falling"
    }
}

/// Computes the Chaikin Money Flow values over a rolling window.
/// CMF = `sum(money_flow_volume)` / sum(volume) for each window of `period`.
fn compute_cmf(
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

/// Signal: Chaikin Money Flow is positive (buying pressure).
/// CMF = `sum(money_flow_volume)` / `sum(volume)` over a rolling window.
pub struct CmfPositive {
    pub close_col: String,
    pub high_col: String,
    pub low_col: String,
    pub volume_col: String,
    pub period: usize,
}

impl SignalFn for CmfPositive {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let close = column_to_f64(df, &self.close_col)?;
        let high = column_to_f64(df, &self.high_col)?;
        let low = column_to_f64(df, &self.low_col)?;
        let volume = column_to_f64(df, &self.volume_col)?;
        let n = close.len();
        if n < self.period {
            return Ok(BooleanChunked::new("cmf_positive".into(), vec![false; n]).into_series());
        }
        let cmf = compute_cmf(&close, &high, &low, &volume, self.period);
        Ok(pad_and_compare(&cmf, n, |v| v > 0.0, "cmf_positive"))
    }
    fn name(&self) -> &'static str {
        "cmf_positive"
    }
}

/// Signal: Chaikin Money Flow is negative (selling pressure).
pub struct CmfNegative {
    pub close_col: String,
    pub high_col: String,
    pub low_col: String,
    pub volume_col: String,
    pub period: usize,
}

impl SignalFn for CmfNegative {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let close = column_to_f64(df, &self.close_col)?;
        let high = column_to_f64(df, &self.high_col)?;
        let low = column_to_f64(df, &self.low_col)?;
        let volume = column_to_f64(df, &self.volume_col)?;
        let n = close.len();
        if n < self.period {
            return Ok(BooleanChunked::new("cmf_negative".into(), vec![false; n]).into_series());
        }
        let cmf = compute_cmf(&close, &high, &low, &volume, self.period);
        Ok(pad_and_compare(&cmf, n, |v| v < 0.0, "cmf_negative"))
    }
    fn name(&self) -> &'static str {
        "cmf_negative"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn obv_rising_detects_increase() {
        let df = df! {
            "close" => &[100.0, 102.0, 104.0, 103.0, 105.0],
            "volume" => &[1000.0, 1500.0, 1200.0, 900.0, 1300.0],
        }
        .unwrap();
        let signal = ObvRising {
            price_col: "close".into(),
            volume_col: "volume".into(),
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert_eq!(result.len(), 5);
        // OBV returns n-1 values; the padded first entry is NaN so bools[0] and bools[1] are
        // always false (bools[1] compares padded[1] > padded[0] where padded[0] is NaN).
        // The first meaningful comparison is at index 2: prices 102->104 (up), rising OBV.
        assert!(!bools.get(1).unwrap());
        assert!(bools.get(2).unwrap());
    }

    #[test]
    fn cmf_positive_correct_length() {
        let df = df! {
            "close" => &[102.0, 104.0, 103.0, 105.0, 107.0, 106.0, 108.0],
            "high" => &[103.0, 105.0, 104.0, 106.0, 108.0, 107.0, 109.0],
            "low" => &[100.0, 102.0, 101.0, 103.0, 105.0, 104.0, 106.0],
            "volume" => &[1000.0, 1500.0, 1200.0, 900.0, 1300.0, 1100.0, 1400.0],
        }
        .unwrap();
        let signal = CmfPositive {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            volume_col: "volume".into(),
            period: 3,
        };
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 7);
    }
}
