// Trend signals: Aroon, Supertrend

use super::helpers::{column_to_f64, pad_series, SignalFn};
use polars::prelude::*;

/// Signal: Aroon oscillator is positive, indicating an uptrend.
/// Aroon oscillator = Aroon Up - Aroon Down.
#[allow(dead_code)]
pub struct AroonUptrend {
    pub high_col: String,
    pub low_col: String,
    pub period: usize,
}

impl SignalFn for AroonUptrend {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let highs = column_to_f64(df, &self.high_col)?;
        let lows = column_to_f64(df, &self.low_col)?;
        let n = highs.len();
        if n < self.period + 1 {
            return Ok(BooleanChunked::new("aroon_uptrend".into(), vec![false; n]).into_series());
        }
        let aroon_values: Vec<(f64, f64, f64)> = (0..(n - self.period))
            .map(|i| {
                let end = i + self.period + 1;
                rust_ti::trend_indicators::single::aroon_indicator(&highs[i..end], &lows[i..end])
            })
            .collect();
        let oscillators: Vec<f64> = aroon_values.iter().map(|t| t.2).collect();
        let padded = pad_series(&oscillators, n);
        let bools: Vec<bool> = padded.iter().map(|&v| !v.is_nan() && v > 0.0).collect();
        Ok(BooleanChunked::new("aroon_uptrend".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "aroon_uptrend"
    }
}

/// Signal: Aroon oscillator is negative, indicating a downtrend.
#[allow(dead_code)]
pub struct AroonDowntrend {
    pub high_col: String,
    pub low_col: String,
    pub period: usize,
}

impl SignalFn for AroonDowntrend {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let highs = column_to_f64(df, &self.high_col)?;
        let lows = column_to_f64(df, &self.low_col)?;
        let n = highs.len();
        if n < self.period + 1 {
            return Ok(BooleanChunked::new("aroon_downtrend".into(), vec![false; n]).into_series());
        }
        let aroon_values: Vec<(f64, f64, f64)> = (0..(n - self.period))
            .map(|i| {
                let end = i + self.period + 1;
                rust_ti::trend_indicators::single::aroon_indicator(&highs[i..end], &lows[i..end])
            })
            .collect();
        let oscillators: Vec<f64> = aroon_values.iter().map(|t| t.2).collect();
        let padded = pad_series(&oscillators, n);
        let bools: Vec<bool> = padded.iter().map(|&v| !v.is_nan() && v < 0.0).collect();
        Ok(BooleanChunked::new("aroon_downtrend".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "aroon_downtrend"
    }
}

/// Signal: Aroon Up is above a threshold (strong uptrend with recent highs).
#[allow(dead_code)]
pub struct AroonUpAbove {
    pub high_col: String,
    pub period: usize,
    pub threshold: f64,
}

impl SignalFn for AroonUpAbove {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let highs = column_to_f64(df, &self.high_col)?;
        let n = highs.len();
        if n < self.period + 1 {
            return Ok(BooleanChunked::new("aroon_up_above".into(), vec![false; n]).into_series());
        }
        let aroon_up_values: Vec<f64> = (0..(n - self.period))
            .map(|i| {
                let end = i + self.period + 1;
                rust_ti::trend_indicators::single::aroon_up(&highs[i..end])
            })
            .collect();
        let padded = pad_series(&aroon_up_values, n);
        let bools: Vec<bool> = padded
            .iter()
            .map(|&v| !v.is_nan() && v > self.threshold)
            .collect();
        Ok(BooleanChunked::new("aroon_up_above".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "aroon_up_above"
    }
}

/// Signal: price is below the supertrend line (bearish trend).
#[allow(dead_code)]
pub struct SupertrendBearish {
    pub close_col: String,
    pub high_col: String,
    pub low_col: String,
    pub period: usize,
    pub multiplier: f64,
}

impl SignalFn for SupertrendBearish {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let close = column_to_f64(df, &self.close_col)?;
        let high = column_to_f64(df, &self.high_col)?;
        let low = column_to_f64(df, &self.low_col)?;
        let n = close.len();
        if n < self.period {
            return Ok(
                BooleanChunked::new("supertrend_bearish".into(), vec![false; n]).into_series(),
            );
        }
        let st = rust_ti::candle_indicators::bulk::supertrend(
            &high,
            &low,
            &close,
            rust_ti::ConstantModelType::SimpleMovingAverage,
            self.multiplier,
            self.period,
        );
        let padded = pad_series(&st, n);
        let bools: Vec<bool> = close
            .iter()
            .zip(padded.iter())
            .map(|(&c, &s)| !s.is_nan() && c < s)
            .collect();
        Ok(BooleanChunked::new("supertrend_bearish".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "supertrend_bearish"
    }
}

/// Signal: price is above the supertrend line (bullish trend).
#[allow(dead_code)]
pub struct SupertrendBullish {
    pub close_col: String,
    pub high_col: String,
    pub low_col: String,
    pub period: usize,
    pub multiplier: f64,
}

impl SignalFn for SupertrendBullish {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let close = column_to_f64(df, &self.close_col)?;
        let high = column_to_f64(df, &self.high_col)?;
        let low = column_to_f64(df, &self.low_col)?;
        let n = close.len();
        if n < self.period {
            return Ok(
                BooleanChunked::new("supertrend_bullish".into(), vec![false; n]).into_series(),
            );
        }
        let st = rust_ti::candle_indicators::bulk::supertrend(
            &high,
            &low,
            &close,
            rust_ti::ConstantModelType::SimpleMovingAverage,
            self.multiplier,
            self.period,
        );
        let padded = pad_series(&st, n);
        let bools: Vec<bool> = close
            .iter()
            .zip(padded.iter())
            .map(|(&c, &s)| !s.is_nan() && c > s)
            .collect();
        Ok(BooleanChunked::new("supertrend_bullish".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "supertrend_bullish"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aroon_uptrend_correct_length() {
        let n = 30;
        let high: Vec<f64> = (0..30_i32).map(|i| 100.0 + f64::from(i) + 2.0).collect();
        let low: Vec<f64> = (0..30_i32).map(|i| 100.0 + f64::from(i) - 2.0).collect();
        let df = df! {
            "high" => &high,
            "low" => &low,
        }
        .unwrap();
        let signal = AroonUptrend {
            high_col: "high".into(),
            low_col: "low".into(),
            period: 5,
        };
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), n);
    }

    #[test]
    fn supertrend_insufficient_data() {
        let df = df! {
            "close" => &[100.0, 102.0, 101.0],
            "high" => &[103.0, 104.0, 103.0],
            "low" => &[97.0, 99.0, 98.0],
        }
        .unwrap();
        let signal = SupertrendBullish {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 10,
            multiplier: 3.0,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }
}
