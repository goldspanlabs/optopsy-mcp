// Volatility signals: ATR, Bollinger Bands, Keltner Channels

use super::helpers::{column_to_f64, pad_series, SignalFn};
use polars::prelude::*;

/// Signal: ATR is above a threshold, indicating high volatility.
/// Requires `close_col`, `high_col`, and `low_col` columns.
pub struct AtrAbove {
    pub close_col: String,
    pub high_col: String,
    pub low_col: String,
    pub period: usize,
    pub threshold: f64,
}

impl SignalFn for AtrAbove {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let close = column_to_f64(df, &self.close_col)?;
        let high = column_to_f64(df, &self.high_col)?;
        let low = column_to_f64(df, &self.low_col)?;
        let n = close.len();
        if n < self.period {
            return Ok(BooleanChunked::new("atr_above".into(), vec![false; n]).into_series());
        }
        let atr_values: Vec<f64> = (0..=n - self.period)
            .map(|i| {
                let end = i + self.period;
                rust_ti::other_indicators::single::average_true_range(
                    &close[i..end],
                    &high[i..end],
                    &low[i..end],
                    rust_ti::ConstantModelType::SimpleMovingAverage,
                )
            })
            .collect();
        let padded = pad_series(&atr_values, n);
        let bools: Vec<bool> = padded
            .iter()
            .map(|&v| !v.is_nan() && v > self.threshold)
            .collect();
        Ok(BooleanChunked::new("atr_above".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "atr_above"
    }
}

/// Signal: ATR is below a threshold, indicating low volatility.
pub struct AtrBelow {
    pub close_col: String,
    pub high_col: String,
    pub low_col: String,
    pub period: usize,
    pub threshold: f64,
}

impl SignalFn for AtrBelow {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let close = column_to_f64(df, &self.close_col)?;
        let high = column_to_f64(df, &self.high_col)?;
        let low = column_to_f64(df, &self.low_col)?;
        let n = close.len();
        if n < self.period {
            return Ok(BooleanChunked::new("atr_below".into(), vec![false; n]).into_series());
        }
        let atr_values: Vec<f64> = (0..=n - self.period)
            .map(|i| {
                let end = i + self.period;
                rust_ti::other_indicators::single::average_true_range(
                    &close[i..end],
                    &high[i..end],
                    &low[i..end],
                    rust_ti::ConstantModelType::SimpleMovingAverage,
                )
            })
            .collect();
        let padded = pad_series(&atr_values, n);
        let bools: Vec<bool> = padded
            .iter()
            .map(|&v| !v.is_nan() && v < self.threshold)
            .collect();
        Ok(BooleanChunked::new("atr_below".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "atr_below"
    }
}

/// Signal: price touches or crosses below the lower Bollinger Band.
/// Uses SMA center with 2×standard deviation bands over a configurable period.
pub struct BollingerLowerTouch {
    pub column: String,
    pub period: usize,
}

impl SignalFn for BollingerLowerTouch {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.column)?;
        let n = prices.len();
        if n < self.period {
            return Ok(
                BooleanChunked::new("bollinger_lower_touch".into(), vec![false; n]).into_series(),
            );
        }
        let bbands = rust_ti::candle_indicators::bulk::moving_constant_bands(
            &prices,
            rust_ti::ConstantModelType::SimpleMovingAverage,
            rust_ti::DeviationModel::StandardDeviation,
            2.0,
            self.period,
        );
        let lower: Vec<f64> = bbands.iter().map(|t| t.0).collect();
        let lower_padded = pad_series(&lower, n);
        let bools: Vec<bool> = prices
            .iter()
            .zip(lower_padded.iter())
            .map(|(&p, &l)| !l.is_nan() && p <= l)
            .collect();
        Ok(BooleanChunked::new("bollinger_lower_touch".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "bollinger_lower_touch"
    }
}

/// Signal: price touches or crosses above the upper Bollinger Band.
/// Uses SMA center with 2×standard deviation bands over a configurable period.
pub struct BollingerUpperTouch {
    pub column: String,
    pub period: usize,
}

impl SignalFn for BollingerUpperTouch {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.column)?;
        let n = prices.len();
        if n < self.period {
            return Ok(
                BooleanChunked::new("bollinger_upper_touch".into(), vec![false; n]).into_series(),
            );
        }
        let bbands = rust_ti::candle_indicators::bulk::moving_constant_bands(
            &prices,
            rust_ti::ConstantModelType::SimpleMovingAverage,
            rust_ti::DeviationModel::StandardDeviation,
            2.0,
            self.period,
        );
        let upper: Vec<f64> = bbands.iter().map(|t| t.2).collect();
        let upper_padded = pad_series(&upper, n);
        let bools: Vec<bool> = prices
            .iter()
            .zip(upper_padded.iter())
            .map(|(&p, &u)| !u.is_nan() && p >= u)
            .collect();
        Ok(BooleanChunked::new("bollinger_upper_touch".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "bollinger_upper_touch"
    }
}

/// Signal: price is below the lower Keltner Channel.
/// Uses EMA for center and SMA-based ATR with a configurable multiplier.
pub struct KeltnerLowerBreak {
    pub close_col: String,
    pub high_col: String,
    pub low_col: String,
    pub period: usize,
    pub multiplier: f64,
}

impl SignalFn for KeltnerLowerBreak {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let close = column_to_f64(df, &self.close_col)?;
        let high = column_to_f64(df, &self.high_col)?;
        let low = column_to_f64(df, &self.low_col)?;
        let n = close.len();
        if n < self.period {
            return Ok(
                BooleanChunked::new("keltner_lower_break".into(), vec![false; n]).into_series(),
            );
        }
        let kc = rust_ti::candle_indicators::bulk::keltner_channel(
            &high,
            &low,
            &close,
            rust_ti::ConstantModelType::ExponentialMovingAverage,
            rust_ti::ConstantModelType::SimpleMovingAverage,
            self.multiplier,
            self.period,
        );
        let lower: Vec<f64> = kc.iter().map(|t| t.0).collect();
        let lower_padded = pad_series(&lower, n);
        let bools: Vec<bool> = close
            .iter()
            .zip(lower_padded.iter())
            .map(|(&p, &l)| !l.is_nan() && p < l)
            .collect();
        Ok(BooleanChunked::new("keltner_lower_break".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "keltner_lower_break"
    }
}

/// Signal: price is above the upper Keltner Channel.
pub struct KeltnerUpperBreak {
    pub close_col: String,
    pub high_col: String,
    pub low_col: String,
    pub period: usize,
    pub multiplier: f64,
}

impl SignalFn for KeltnerUpperBreak {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let close = column_to_f64(df, &self.close_col)?;
        let high = column_to_f64(df, &self.high_col)?;
        let low = column_to_f64(df, &self.low_col)?;
        let n = close.len();
        if n < self.period {
            return Ok(
                BooleanChunked::new("keltner_upper_break".into(), vec![false; n]).into_series(),
            );
        }
        let kc = rust_ti::candle_indicators::bulk::keltner_channel(
            &high,
            &low,
            &close,
            rust_ti::ConstantModelType::ExponentialMovingAverage,
            rust_ti::ConstantModelType::SimpleMovingAverage,
            self.multiplier,
            self.period,
        );
        let upper: Vec<f64> = kc.iter().map(|t| t.2).collect();
        let upper_padded = pad_series(&upper, n);
        let bools: Vec<bool> = close
            .iter()
            .zip(upper_padded.iter())
            .map(|(&p, &u)| !u.is_nan() && p > u)
            .collect();
        Ok(BooleanChunked::new("keltner_upper_break".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "keltner_upper_break"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bollinger_insufficient_data() {
        let df = df! { "close" => &[100.0; 10] }.unwrap();
        let signal = BollingerLowerTouch {
            column: "close".into(),
            period: 20,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn atr_above_correct_length() {
        let close: Vec<f64> = (0..20).map(|i| 100.0 + f64::from(i)).collect();
        let high: Vec<f64> = close.iter().map(|c| c + 2.0).collect();
        let low: Vec<f64> = close.iter().map(|c| c - 2.0).collect();
        let df = df! {
            "close" => &close,
            "high" => &high,
            "low" => &low,
        }
        .unwrap();
        let signal = AtrAbove {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 5,
            threshold: 1.0,
        };
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 20);
    }

    #[test]
    fn atr_below_correct_length() {
        let close: Vec<f64> = (0..20).map(|i| 100.0 + f64::from(i)).collect();
        let high: Vec<f64> = close.iter().map(|c| c + 2.0).collect();
        let low: Vec<f64> = close.iter().map(|c| c - 2.0).collect();
        let df = df! {
            "close" => &close,
            "high" => &high,
            "low" => &low,
        }
        .unwrap();
        let signal = AtrBelow {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 5,
            threshold: 10.0,
        };
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 20);
    }

    #[test]
    fn atr_above_insufficient_data() {
        let df = df! {
            "close" => &[100.0, 101.0],
            "high" => &[103.0, 104.0],
            "low" => &[97.0, 98.0],
        }
        .unwrap();
        let signal = AtrAbove {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 10,
            threshold: 1.0,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn atr_below_insufficient_data() {
        let df = df! {
            "close" => &[100.0, 101.0],
            "high" => &[103.0, 104.0],
            "low" => &[97.0, 98.0],
        }
        .unwrap();
        let signal = AtrBelow {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 10,
            threshold: 10.0,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn atr_below_detects_low_volatility() {
        // With constant high-low range of 4, ATR should be around 4
        let close: Vec<f64> = vec![100.0; 20];
        let high: Vec<f64> = vec![102.0; 20];
        let low: Vec<f64> = vec![98.0; 20];
        let df = df! {
            "close" => &close,
            "high" => &high,
            "low" => &low,
        }
        .unwrap();
        let signal = AtrBelow {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 5,
            threshold: 10.0,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        // ATR ~4 < threshold 10, so should have some trues
        let true_count = bools.into_no_null_iter().filter(|&b| b).count();
        assert!(true_count > 0);
    }

    #[test]
    fn bollinger_upper_touch_correct_length() {
        let prices: Vec<f64> = (0..30).map(|i| 100.0 + f64::from(i) * 2.0).collect();
        let df = df! { "close" => &prices }.unwrap();
        let signal = BollingerUpperTouch {
            column: "close".into(),
            period: 10,
        };
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 30);
    }

    #[test]
    fn bollinger_upper_touch_insufficient_data() {
        let df = df! { "close" => &[100.0; 5] }.unwrap();
        let signal = BollingerUpperTouch {
            column: "close".into(),
            period: 20,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn keltner_lower_break_correct_length() {
        let close: Vec<f64> = (0..30).map(|i| 100.0 + f64::from(i)).collect();
        let high: Vec<f64> = close.iter().map(|c| c + 2.0).collect();
        let low: Vec<f64> = close.iter().map(|c| c - 2.0).collect();
        let df = df! {
            "close" => &close,
            "high" => &high,
            "low" => &low,
        }
        .unwrap();
        let signal = KeltnerLowerBreak {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 5,
            multiplier: 2.0,
        };
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 30);
    }

    #[test]
    fn keltner_lower_break_insufficient_data() {
        let df = df! {
            "close" => &[100.0, 101.0],
            "high" => &[102.0, 103.0],
            "low" => &[98.0, 99.0],
        }
        .unwrap();
        let signal = KeltnerLowerBreak {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 10,
            multiplier: 2.0,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn keltner_upper_break_correct_length() {
        let close: Vec<f64> = (0..30).map(|i| 100.0 + f64::from(i)).collect();
        let high: Vec<f64> = close.iter().map(|c| c + 2.0).collect();
        let low: Vec<f64> = close.iter().map(|c| c - 2.0).collect();
        let df = df! {
            "close" => &close,
            "high" => &high,
            "low" => &low,
        }
        .unwrap();
        let signal = KeltnerUpperBreak {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 5,
            multiplier: 2.0,
        };
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 30);
    }

    #[test]
    fn keltner_upper_break_insufficient_data() {
        let df = df! {
            "close" => &[100.0, 101.0],
            "high" => &[102.0, 103.0],
            "low" => &[98.0, 99.0],
        }
        .unwrap();
        let signal = KeltnerUpperBreak {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 10,
            multiplier: 2.0,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn atr_above_name() {
        let signal = AtrAbove {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 5,
            threshold: 1.0,
        };
        assert_eq!(signal.name(), "atr_above");
    }

    #[test]
    fn atr_below_name() {
        let signal = AtrBelow {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 5,
            threshold: 1.0,
        };
        assert_eq!(signal.name(), "atr_below");
    }

    #[test]
    fn bollinger_lower_touch_name() {
        let signal = BollingerLowerTouch {
            column: "close".into(),
            period: 20,
        };
        assert_eq!(signal.name(), "bollinger_lower_touch");
    }

    #[test]
    fn bollinger_upper_touch_name() {
        let signal = BollingerUpperTouch {
            column: "close".into(),
            period: 20,
        };
        assert_eq!(signal.name(), "bollinger_upper_touch");
    }

    #[test]
    fn keltner_lower_break_name() {
        let signal = KeltnerLowerBreak {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 5,
            multiplier: 2.0,
        };
        assert_eq!(signal.name(), "keltner_lower_break");
    }

    #[test]
    fn keltner_upper_break_name() {
        let signal = KeltnerUpperBreak {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 5,
            multiplier: 2.0,
        };
        assert_eq!(signal.name(), "keltner_upper_break");
    }

    #[test]
    fn bollinger_upper_touch_detects_extreme_high() {
        // The last price equals the upper Bollinger Band:
        //   window [100,100,100,100,200]: mean=120, pop-std=40, upper=120+2*40=200.
        // price[4]=200 >= upper → should be true.
        let prices = vec![100.0f64, 100.0, 100.0, 100.0, 200.0];
        let df = df! { "close" => &prices }.unwrap();
        let signal = BollingerUpperTouch {
            column: "close".into(),
            period: 5,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(
            bools.get(4).unwrap(),
            "price at upper Bollinger Band should be detected"
        );
    }

    #[test]
    fn bollinger_lower_touch_detects_extreme_low() {
        // The last price equals the lower Bollinger Band:
        //   window [200,200,200,200,100]: mean=180, pop-std=40, lower=180-2*40=100.
        // price[4]=100 <= lower → should be true.
        let prices = vec![200.0f64, 200.0, 200.0, 200.0, 100.0];
        let df = df! { "close" => &prices }.unwrap();
        let signal = BollingerLowerTouch {
            column: "close".into(),
            period: 5,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(
            bools.get(4).unwrap(),
            "price at lower Bollinger Band should be detected"
        );
    }
}
