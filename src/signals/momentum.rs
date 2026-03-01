// Momentum signals: RSI, MACD, Stochastic

use super::helpers::{column_to_f64, pad_and_compare, pad_series, SignalFn};
use polars::prelude::*;
use rust_ti::standard_indicators::bulk as sti;

/// Standard RSI lookback period.
const RSI_PERIOD: usize = 14;

/// Minimum number of data points required to compute MACD (slow EMA + signal line).
const MACD_MIN_PERIODS: usize = 34;

/// Signal: RSI is below a threshold (oversold condition).
/// Uses the standard 14-period RSI.
pub struct RsiOversold {
    pub column: String,
    pub threshold: f64,
}

impl SignalFn for RsiOversold {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.column)?;
        let n = prices.len();
        if n <= RSI_PERIOD {
            return Ok(BooleanChunked::new("rsi_oversold".into(), vec![false; n]).into_series());
        }
        let rsi_values = sti::rsi(&prices);
        Ok(pad_and_compare(
            &rsi_values,
            n,
            |v| v < self.threshold,
            "rsi_oversold",
        ))
    }
    fn name(&self) -> &'static str {
        "rsi_oversold"
    }
}

/// Signal: RSI is above a threshold (overbought condition).
pub struct RsiOverbought {
    pub column: String,
    pub threshold: f64,
}

impl SignalFn for RsiOverbought {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.column)?;
        let n = prices.len();
        if n <= RSI_PERIOD {
            return Ok(BooleanChunked::new("rsi_overbought".into(), vec![false; n]).into_series());
        }
        let rsi_values = sti::rsi(&prices);
        Ok(pad_and_compare(
            &rsi_values,
            n,
            |v| v > self.threshold,
            "rsi_overbought",
        ))
    }
    fn name(&self) -> &'static str {
        "rsi_overbought"
    }
}

/// Signal: MACD histogram is positive (bullish momentum).
/// Requires at least 34 data points.
pub struct MacdBullish {
    pub column: String,
}

impl SignalFn for MacdBullish {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.column)?;
        let n = prices.len();
        if n < MACD_MIN_PERIODS {
            return Ok(BooleanChunked::new("macd_bullish".into(), vec![false; n]).into_series());
        }
        let macd_values = sti::macd(&prices);
        let histograms: Vec<f64> = macd_values.iter().map(|t| t.2).collect();
        Ok(pad_and_compare(&histograms, n, |v| v > 0.0, "macd_bullish"))
    }
    fn name(&self) -> &'static str {
        "macd_bullish"
    }
}

/// Signal: MACD histogram is negative (bearish momentum).
pub struct MacdBearish {
    pub column: String,
}

impl SignalFn for MacdBearish {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.column)?;
        let n = prices.len();
        if n < MACD_MIN_PERIODS {
            return Ok(BooleanChunked::new("macd_bearish".into(), vec![false; n]).into_series());
        }
        let macd_values = sti::macd(&prices);
        let histograms: Vec<f64> = macd_values.iter().map(|t| t.2).collect();
        Ok(pad_and_compare(&histograms, n, |v| v < 0.0, "macd_bearish"))
    }
    fn name(&self) -> &'static str {
        "macd_bearish"
    }
}

/// Signal: MACD histogram crosses from negative to positive (bullish crossover).
pub struct MacdCrossover {
    pub column: String,
}

impl SignalFn for MacdCrossover {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.column)?;
        let n = prices.len();
        if n < MACD_MIN_PERIODS {
            return Ok(BooleanChunked::new("macd_crossover".into(), vec![false; n]).into_series());
        }
        let macd_values = sti::macd(&prices);
        let histograms: Vec<f64> = macd_values.iter().map(|t| t.2).collect();
        let padded = pad_series(&histograms, n);
        let mut bools = vec![false; n];
        for i in 1..n {
            if !padded[i].is_nan() && !padded[i - 1].is_nan() {
                bools[i] = padded[i] > 0.0 && padded[i - 1] <= 0.0;
            }
        }
        Ok(BooleanChunked::new("macd_crossover".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "macd_crossover"
    }
}

/// Computes the stochastic oscillator values over a rolling window.
/// Formula: (close - `lowest_low`) / (`highest_high` - `lowest_low`) * 100
fn compute_stochastic(close: &[f64], high: &[f64], low: &[f64], period: usize) -> Vec<f64> {
    let n = close.len();
    if n < period {
        return vec![];
    }
    (0..=n.saturating_sub(period))
        .map(|i| {
            let end = i + period;
            let close_last = close[end - 1];
            let lowest_low = low[i..end].iter().copied().fold(f64::INFINITY, f64::min);
            let highest_high = high[i..end]
                .iter()
                .copied()
                .fold(f64::NEG_INFINITY, f64::max);
            if (highest_high - lowest_low).abs() < f64::EPSILON {
                0.0
            } else {
                100.0 * (close_last - lowest_low) / (highest_high - lowest_low)
            }
        })
        .collect()
}

/// Signal: Stochastic oscillator is below threshold (oversold).
/// Uses the standard formula: (close - `lowest_low`) / (`highest_high` - `lowest_low`) * 100.
pub struct StochasticOversold {
    pub close_col: String,
    pub high_col: String,
    pub low_col: String,
    pub period: usize,
    pub threshold: f64,
}

impl SignalFn for StochasticOversold {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let close = column_to_f64(df, &self.close_col)?;
        let high = column_to_f64(df, &self.high_col)?;
        let low = column_to_f64(df, &self.low_col)?;
        let n = close.len();
        if self.period == 0 || n < self.period {
            return Ok(
                BooleanChunked::new("stochastic_oversold".into(), vec![false; n]).into_series(),
            );
        }
        let stoch_values = compute_stochastic(&close, &high, &low, self.period);
        Ok(pad_and_compare(
            &stoch_values,
            n,
            |v| v < self.threshold,
            "stochastic_oversold",
        ))
    }
    fn name(&self) -> &'static str {
        "stochastic_oversold"
    }
}

/// Signal: Stochastic oscillator is above threshold (overbought).
/// Uses the standard formula: (close - `lowest_low`) / (`highest_high` - `lowest_low`) * 100.
pub struct StochasticOverbought {
    pub close_col: String,
    pub high_col: String,
    pub low_col: String,
    pub period: usize,
    pub threshold: f64,
}

impl SignalFn for StochasticOverbought {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let close = column_to_f64(df, &self.close_col)?;
        let high = column_to_f64(df, &self.high_col)?;
        let low = column_to_f64(df, &self.low_col)?;
        let n = close.len();
        if self.period == 0 || n < self.period {
            return Ok(
                BooleanChunked::new("stochastic_overbought".into(), vec![false; n]).into_series(),
            );
        }
        let stoch_values = compute_stochastic(&close, &high, &low, self.period);
        Ok(pad_and_compare(
            &stoch_values,
            n,
            |v| v > self.threshold,
            "stochastic_overbought",
        ))
    }
    fn name(&self) -> &'static str {
        "stochastic_overbought"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_df() -> DataFrame {
        df! {
            "close" => &[
                100.0, 102.0, 101.0, 103.0, 105.0, 104.0, 106.0, 108.0, 107.0, 110.0,
                109.0, 112.0, 111.0, 113.0, 115.0
            ]
        }
        .unwrap()
    }

    #[test]
    fn rsi_oversold_produces_correct_length() {
        let df = sample_df();
        let signal = RsiOversold {
            column: "close".into(),
            threshold: 30.0,
        };
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 15);
    }

    #[test]
    fn stochastic_insufficient_data() {
        let df = df! {
            "close" => &[100.0, 102.0],
            "high" => &[103.0, 104.0],
            "low" => &[99.0, 101.0],
        }
        .unwrap();
        let signal = StochasticOversold {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 14,
            threshold: 20.0,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }
}
