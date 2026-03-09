// Momentum signals: RSI, MACD, Stochastic

use super::helpers::{column_to_f64, pad_and_compare, pad_series, SignalFn};
use polars::prelude::*;
use rust_ti::standard_indicators::bulk as sti;

/// Standard RSI lookback period.
const RSI_PERIOD: usize = 14;

/// Minimum number of data points required to compute MACD (slow EMA + signal line).
const MACD_MIN_PERIODS: usize = 34;

/// Generate a pair of threshold signal structs that share identical computation
/// logic but differ only in the comparison operator.
macro_rules! threshold_signal_pair {
    (
        $(#[$below_doc:meta])* $below:ident / $(#[$above_doc:meta])* $above:ident,
        fields { $($field:ident : $fty:ty),+ $(,)? },
        below_name = $below_name:expr,
        above_name = $above_name:expr,
        compute($self_:ident, $df:ident) -> ($values:ident, $n:ident, $min_period:expr) $body:block
    ) => {
        $(#[$below_doc])*
        pub struct $below { $(pub $field: $fty),+ }

        impl SignalFn for $below {
            fn evaluate(&$self_, $df: &DataFrame) -> Result<Series, PolarsError> {
                let ($values, $n) = $body;
                if $n < $min_period {
                    return Ok(BooleanChunked::new($below_name.into(), vec![false; $n]).into_series());
                }
                Ok(pad_and_compare(&$values, $n, |v| v < $self_.threshold, $below_name))
            }
            fn name(&self) -> &'static str { $below_name }
        }

        $(#[$above_doc])*
        pub struct $above { $(pub $field: $fty),+ }

        impl SignalFn for $above {
            fn evaluate(&$self_, $df: &DataFrame) -> Result<Series, PolarsError> {
                let ($values, $n) = $body;
                if $n < $min_period {
                    return Ok(BooleanChunked::new($above_name.into(), vec![false; $n]).into_series());
                }
                Ok(pad_and_compare(&$values, $n, |v| v > $self_.threshold, $above_name))
            }
            fn name(&self) -> &'static str { $above_name }
        }
    };
}

// Re-export the macro for use in other signal modules
pub(crate) use threshold_signal_pair;

threshold_signal_pair! {
    /// Signal: RSI is below a threshold. Uses the standard 14-period RSI.
    RsiBelow /
    /// Signal: RSI is above a threshold.
    RsiAbove,
    fields { column: String, threshold: f64 },
    below_name = "rsi_below",
    above_name = "rsi_above",
    compute(self, df) -> (rsi_values, n, RSI_PERIOD + 1) {
        let prices = column_to_f64(df, &self.column)?;
        let n = prices.len();
        let rsi_values = if n > RSI_PERIOD { sti::rsi(&prices) } else { vec![] };
        (rsi_values, n)
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

threshold_signal_pair! {
    /// Signal: Stochastic oscillator is below threshold.
    /// Uses the standard formula: (close - `lowest_low`) / (`highest_high` - `lowest_low`) * 100.
    StochasticBelow /
    /// Signal: Stochastic oscillator is above threshold.
    StochasticAbove,
    fields { close_col: String, high_col: String, low_col: String, period: usize, threshold: f64 },
    below_name = "stochastic_below",
    above_name = "stochastic_above",
    compute(self, df) -> (stoch_values, n, self.period.max(1)) {
        let close = column_to_f64(df, &self.close_col)?;
        let high = column_to_f64(df, &self.high_col)?;
        let low = column_to_f64(df, &self.low_col)?;
        let n = close.len();
        let stoch_values = if self.period > 0 && n >= self.period {
            compute_stochastic(&close, &high, &low, self.period)
        } else {
            vec![]
        };
        (stoch_values, n)
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
    fn rsi_below_produces_correct_length() {
        let df = sample_df();
        let signal = RsiBelow {
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
        let signal = StochasticBelow {
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

    #[test]
    fn rsi_above_produces_correct_length() {
        let df = sample_df();
        let signal = RsiAbove {
            column: "close".into(),
            threshold: 70.0,
        };
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 15);
    }

    #[test]
    fn rsi_below_insufficient_data() {
        let df = df! { "close" => &[100.0, 102.0, 101.0] }.unwrap();
        let signal = RsiBelow {
            column: "close".into(),
            threshold: 30.0,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn rsi_above_insufficient_data() {
        let df = df! { "close" => &[100.0, 102.0, 101.0] }.unwrap();
        let signal = RsiAbove {
            column: "close".into(),
            threshold: 70.0,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    fn large_sample_df() -> DataFrame {
        // 40 data points for MACD (needs 34+)
        let prices: Vec<f64> = (0..40).map(|i| 100.0 + f64::from(i) * 0.5).collect();
        df! { "close" => &prices }.unwrap()
    }

    #[test]
    fn macd_bullish_produces_correct_length() {
        let df = large_sample_df();
        let signal = MacdBullish {
            column: "close".into(),
        };
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 40);
    }

    #[test]
    fn macd_bearish_produces_correct_length() {
        let df = large_sample_df();
        let signal = MacdBearish {
            column: "close".into(),
        };
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 40);
    }

    #[test]
    fn macd_crossover_produces_correct_length() {
        let df = large_sample_df();
        let signal = MacdCrossover {
            column: "close".into(),
        };
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 40);
    }

    #[test]
    fn macd_bullish_insufficient_data() {
        let df = df! { "close" => &[100.0; 10] }.unwrap();
        let signal = MacdBullish {
            column: "close".into(),
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn macd_bearish_insufficient_data() {
        let df = df! { "close" => &[100.0; 10] }.unwrap();
        let signal = MacdBearish {
            column: "close".into(),
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn macd_crossover_insufficient_data() {
        let df = df! { "close" => &[100.0; 10] }.unwrap();
        let signal = MacdCrossover {
            column: "close".into(),
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn macd_crossover_first_row_always_false() {
        let df = large_sample_df();
        let signal = MacdCrossover {
            column: "close".into(),
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(!bools.get(0).unwrap());
    }

    #[test]
    fn stochastic_above_correct_length() {
        let close: Vec<f64> = (0..20).map(|i| 100.0 + f64::from(i)).collect();
        let high: Vec<f64> = close.iter().map(|c| c + 2.0).collect();
        let low: Vec<f64> = close.iter().map(|c| c - 2.0).collect();
        let df = df! {
            "close" => &close,
            "high" => &high,
            "low" => &low,
        }
        .unwrap();
        let signal = StochasticAbove {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 5,
            threshold: 80.0,
        };
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 20);
    }

    #[test]
    fn stochastic_above_insufficient_data() {
        let df = df! {
            "close" => &[100.0, 102.0],
            "high" => &[103.0, 104.0],
            "low" => &[99.0, 101.0],
        }
        .unwrap();
        let signal = StochasticAbove {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 14,
            threshold: 80.0,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn stochastic_zero_period() {
        let df = df! {
            "close" => &[100.0, 102.0, 101.0],
            "high" => &[103.0, 104.0, 103.0],
            "low" => &[99.0, 101.0, 100.0],
        }
        .unwrap();
        let signal = StochasticBelow {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 0,
            threshold: 20.0,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn compute_stochastic_basic() {
        // With a simple uptrend, the stochastic should be near 100
        let close = vec![100.0, 101.0, 102.0, 103.0, 104.0];
        let high = vec![101.0, 102.0, 103.0, 104.0, 105.0];
        let low = vec![99.0, 100.0, 101.0, 102.0, 103.0];
        let result = compute_stochastic(&close, &high, &low, 3);
        assert!(!result.is_empty());
        // Last value: close=104, lowest_low=101, highest_high=105
        // (104 - 101) / (105 - 101) * 100 = 75.0
        let last = *result.last().unwrap();
        assert!((last - 75.0).abs() < 1.0);
    }

    #[test]
    fn compute_stochastic_insufficient() {
        let result = compute_stochastic(&[100.0], &[101.0], &[99.0], 5);
        assert!(result.is_empty());
    }

    #[test]
    fn compute_stochastic_flat_range() {
        // When high == low, should return 0
        let close = vec![100.0, 100.0, 100.0];
        let high = vec![100.0, 100.0, 100.0];
        let low = vec![100.0, 100.0, 100.0];
        let result = compute_stochastic(&close, &high, &low, 2);
        assert!(result.iter().all(|&v| v == 0.0));
    }

    #[test]
    fn rsi_below_name() {
        let signal = RsiBelow {
            column: "close".into(),
            threshold: 30.0,
        };
        assert_eq!(signal.name(), "rsi_below");
    }

    #[test]
    fn rsi_above_name() {
        let signal = RsiAbove {
            column: "close".into(),
            threshold: 70.0,
        };
        assert_eq!(signal.name(), "rsi_above");
    }

    #[test]
    fn macd_bullish_name() {
        let signal = MacdBullish {
            column: "close".into(),
        };
        assert_eq!(signal.name(), "macd_bullish");
    }

    #[test]
    fn macd_bearish_name() {
        let signal = MacdBearish {
            column: "close".into(),
        };
        assert_eq!(signal.name(), "macd_bearish");
    }

    #[test]
    fn macd_crossover_name() {
        let signal = MacdCrossover {
            column: "close".into(),
        };
        assert_eq!(signal.name(), "macd_crossover");
    }

    #[test]
    fn stochastic_below_name() {
        let signal = StochasticBelow {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 14,
            threshold: 20.0,
        };
        assert_eq!(signal.name(), "stochastic_below");
    }

    #[test]
    fn stochastic_above_name() {
        let signal = StochasticAbove {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 14,
            threshold: 80.0,
        };
        assert_eq!(signal.name(), "stochastic_above");
    }
}
