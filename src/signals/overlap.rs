// Overlap signals: SMA, EMA, crossovers

use super::helpers::{column_to_f64, pad_series, SignalFn};
use polars::prelude::*;
use rust_ti::standard_indicators::bulk as sti;

/// Signal: price is above its Simple Moving Average.
pub struct PriceAboveSma {
    pub column: String,
    pub period: usize,
}

impl SignalFn for PriceAboveSma {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.column)?;
        let n = prices.len();
        if n < self.period {
            return Ok(BooleanChunked::new("price_above_sma".into(), vec![false; n]).into_series());
        }
        let sma = sti::simple_moving_average(&prices, self.period);
        let sma_padded = pad_series(&sma, n);
        let bools: Vec<bool> = prices
            .iter()
            .zip(sma_padded.iter())
            .map(|(&p, &s)| !s.is_nan() && p > s)
            .collect();
        Ok(BooleanChunked::new("price_above_sma".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "price_above_sma"
    }
}

/// Signal: price is below its Simple Moving Average.
pub struct PriceBelowSma {
    pub column: String,
    pub period: usize,
}

impl SignalFn for PriceBelowSma {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.column)?;
        let n = prices.len();
        if n < self.period {
            return Ok(BooleanChunked::new("price_below_sma".into(), vec![false; n]).into_series());
        }
        let sma = sti::simple_moving_average(&prices, self.period);
        let sma_padded = pad_series(&sma, n);
        let bools: Vec<bool> = prices
            .iter()
            .zip(sma_padded.iter())
            .map(|(&p, &s)| !s.is_nan() && p < s)
            .collect();
        Ok(BooleanChunked::new("price_below_sma".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "price_below_sma"
    }
}

/// Signal: price is above its Exponential Moving Average.
pub struct PriceAboveEma {
    pub column: String,
    pub period: usize,
}

impl SignalFn for PriceAboveEma {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.column)?;
        let n = prices.len();
        if n < self.period {
            return Ok(BooleanChunked::new("price_above_ema".into(), vec![false; n]).into_series());
        }
        let ema = sti::exponential_moving_average(&prices, self.period);
        let ema_padded = pad_series(&ema, n);
        let bools: Vec<bool> = prices
            .iter()
            .zip(ema_padded.iter())
            .map(|(&p, &e)| !e.is_nan() && p > e)
            .collect();
        Ok(BooleanChunked::new("price_above_ema".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "price_above_ema"
    }
}

/// Signal: price is below its Exponential Moving Average.
pub struct PriceBelowEma {
    pub column: String,
    pub period: usize,
}

impl SignalFn for PriceBelowEma {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.column)?;
        let n = prices.len();
        if n < self.period {
            return Ok(BooleanChunked::new("price_below_ema".into(), vec![false; n]).into_series());
        }
        let ema = sti::exponential_moving_average(&prices, self.period);
        let ema_padded = pad_series(&ema, n);
        let bools: Vec<bool> = prices
            .iter()
            .zip(ema_padded.iter())
            .map(|(&p, &e)| !e.is_nan() && p < e)
            .collect();
        Ok(BooleanChunked::new("price_below_ema".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "price_below_ema"
    }
}

/// Signal: fast SMA crosses above slow SMA (golden cross).
/// True on rows where fast > slow AND the previous row had fast <= slow.
pub struct SmaCrossover {
    pub column: String,
    pub fast_period: usize,
    pub slow_period: usize,
}

impl SignalFn for SmaCrossover {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.column)?;
        let n = prices.len();
        let min_period = self.fast_period.max(self.slow_period);
        if n < min_period {
            return Ok(BooleanChunked::new("sma_crossover".into(), vec![false; n]).into_series());
        }
        let fast = pad_series(&sti::simple_moving_average(&prices, self.fast_period), n);
        let slow = pad_series(&sti::simple_moving_average(&prices, self.slow_period), n);
        let mut bools = vec![false; n];
        for i in 1..n {
            let prev_valid = !fast[i - 1].is_nan() && !slow[i - 1].is_nan();
            let curr_valid = !fast[i].is_nan() && !slow[i].is_nan();
            if prev_valid && curr_valid {
                bools[i] = fast[i] > slow[i] && fast[i - 1] <= slow[i - 1];
            }
        }
        Ok(BooleanChunked::new("sma_crossover".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "sma_crossover"
    }
}

/// Signal: fast SMA crosses below slow SMA (death cross).
pub struct SmaCrossunder {
    pub column: String,
    pub fast_period: usize,
    pub slow_period: usize,
}

impl SignalFn for SmaCrossunder {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.column)?;
        let n = prices.len();
        let min_period = self.fast_period.max(self.slow_period);
        if n < min_period {
            return Ok(BooleanChunked::new("sma_crossunder".into(), vec![false; n]).into_series());
        }
        let fast = pad_series(&sti::simple_moving_average(&prices, self.fast_period), n);
        let slow = pad_series(&sti::simple_moving_average(&prices, self.slow_period), n);
        let mut bools = vec![false; n];
        for i in 1..n {
            let prev_valid = !fast[i - 1].is_nan() && !slow[i - 1].is_nan();
            let curr_valid = !fast[i].is_nan() && !slow[i].is_nan();
            if prev_valid && curr_valid {
                bools[i] = fast[i] < slow[i] && fast[i - 1] >= slow[i - 1];
            }
        }
        Ok(BooleanChunked::new("sma_crossunder".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "sma_crossunder"
    }
}

/// Signal: fast EMA crosses above slow EMA.
pub struct EmaCrossover {
    pub column: String,
    pub fast_period: usize,
    pub slow_period: usize,
}

impl SignalFn for EmaCrossover {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.column)?;
        let n = prices.len();
        let min_period = self.fast_period.max(self.slow_period);
        if n < min_period {
            return Ok(BooleanChunked::new("ema_crossover".into(), vec![false; n]).into_series());
        }
        let fast = pad_series(
            &sti::exponential_moving_average(&prices, self.fast_period),
            n,
        );
        let slow = pad_series(
            &sti::exponential_moving_average(&prices, self.slow_period),
            n,
        );
        let mut bools = vec![false; n];
        for i in 1..n {
            let prev_valid = !fast[i - 1].is_nan() && !slow[i - 1].is_nan();
            let curr_valid = !fast[i].is_nan() && !slow[i].is_nan();
            if prev_valid && curr_valid {
                bools[i] = fast[i] > slow[i] && fast[i - 1] <= slow[i - 1];
            }
        }
        Ok(BooleanChunked::new("ema_crossover".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "ema_crossover"
    }
}

/// Signal: fast EMA crosses below slow EMA.
pub struct EmaCrossunder {
    pub column: String,
    pub fast_period: usize,
    pub slow_period: usize,
}

impl SignalFn for EmaCrossunder {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.column)?;
        let n = prices.len();
        let min_period = self.fast_period.max(self.slow_period);
        if n < min_period {
            return Ok(BooleanChunked::new("ema_crossunder".into(), vec![false; n]).into_series());
        }
        let fast = pad_series(
            &sti::exponential_moving_average(&prices, self.fast_period),
            n,
        );
        let slow = pad_series(
            &sti::exponential_moving_average(&prices, self.slow_period),
            n,
        );
        let mut bools = vec![false; n];
        for i in 1..n {
            let prev_valid = !fast[i - 1].is_nan() && !slow[i - 1].is_nan();
            let curr_valid = !fast[i].is_nan() && !slow[i].is_nan();
            if prev_valid && curr_valid {
                bools[i] = fast[i] < slow[i] && fast[i - 1] >= slow[i - 1];
            }
        }
        Ok(BooleanChunked::new("ema_crossunder".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "ema_crossunder"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_df() -> DataFrame {
        df! {
            "close" => &[100.0, 102.0, 101.0, 103.0, 105.0, 104.0, 106.0, 108.0, 107.0, 110.0]
        }
        .unwrap()
    }

    #[test]
    fn price_above_sma_produces_correct_length() {
        let df = sample_df();
        let signal = PriceAboveSma {
            column: "close".into(),
            period: 3,
        };
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 10);
    }

    #[test]
    fn sma_crossover_first_row_always_false() {
        let df = sample_df();
        let signal = SmaCrossover {
            column: "close".into(),
            fast_period: 2,
            slow_period: 4,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(!bools.get(0).unwrap_or(true));
    }

    #[test]
    fn insufficient_data_returns_all_false() {
        let df = df! { "close" => &[100.0, 102.0] }.unwrap();
        let signal = PriceAboveSma {
            column: "close".into(),
            period: 5,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn price_below_sma_produces_correct_length() {
        let df = sample_df();
        let signal = PriceBelowSma {
            column: "close".into(),
            period: 3,
        };
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 10);
    }

    #[test]
    fn price_below_sma_insufficient_data() {
        let df = df! { "close" => &[100.0] }.unwrap();
        let signal = PriceBelowSma {
            column: "close".into(),
            period: 5,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn price_above_ema_produces_correct_length() {
        let df = sample_df();
        let signal = PriceAboveEma {
            column: "close".into(),
            period: 3,
        };
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 10);
    }

    #[test]
    fn price_above_ema_insufficient_data() {
        let df = df! { "close" => &[100.0, 102.0] }.unwrap();
        let signal = PriceAboveEma {
            column: "close".into(),
            period: 5,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn price_below_ema_produces_correct_length() {
        let df = sample_df();
        let signal = PriceBelowEma {
            column: "close".into(),
            period: 3,
        };
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 10);
    }

    #[test]
    fn price_below_ema_insufficient_data() {
        let df = df! { "close" => &[100.0, 102.0] }.unwrap();
        let signal = PriceBelowEma {
            column: "close".into(),
            period: 5,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn sma_crossunder_produces_correct_length() {
        let df = sample_df();
        let signal = SmaCrossunder {
            column: "close".into(),
            fast_period: 2,
            slow_period: 4,
        };
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 10);
    }

    #[test]
    fn sma_crossunder_insufficient_data() {
        let df = df! { "close" => &[100.0, 102.0] }.unwrap();
        let signal = SmaCrossunder {
            column: "close".into(),
            fast_period: 3,
            slow_period: 5,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn sma_crossover_insufficient_data() {
        let df = df! { "close" => &[100.0] }.unwrap();
        let signal = SmaCrossover {
            column: "close".into(),
            fast_period: 3,
            slow_period: 5,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn ema_crossover_produces_correct_length() {
        let df = sample_df();
        let signal = EmaCrossover {
            column: "close".into(),
            fast_period: 2,
            slow_period: 4,
        };
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 10);
    }

    #[test]
    fn ema_crossover_insufficient_data() {
        let df = df! { "close" => &[100.0, 102.0] }.unwrap();
        let signal = EmaCrossover {
            column: "close".into(),
            fast_period: 3,
            slow_period: 5,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn ema_crossunder_produces_correct_length() {
        let df = sample_df();
        let signal = EmaCrossunder {
            column: "close".into(),
            fast_period: 2,
            slow_period: 4,
        };
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 10);
    }

    #[test]
    fn ema_crossunder_insufficient_data() {
        let df = df! { "close" => &[100.0, 102.0] }.unwrap();
        let signal = EmaCrossunder {
            column: "close".into(),
            fast_period: 3,
            slow_period: 5,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn ema_crossover_first_row_always_false() {
        let df = sample_df();
        let signal = EmaCrossover {
            column: "close".into(),
            fast_period: 2,
            slow_period: 4,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(!bools.get(0).unwrap_or(true));
    }

    #[test]
    fn price_above_sma_name() {
        let signal = PriceAboveSma {
            column: "close".into(),
            period: 3,
        };
        assert_eq!(signal.name(), "price_above_sma");
    }

    #[test]
    fn price_below_sma_name() {
        let signal = PriceBelowSma {
            column: "close".into(),
            period: 3,
        };
        assert_eq!(signal.name(), "price_below_sma");
    }

    #[test]
    fn price_above_ema_name() {
        let signal = PriceAboveEma {
            column: "close".into(),
            period: 3,
        };
        assert_eq!(signal.name(), "price_above_ema");
    }

    #[test]
    fn price_below_ema_name() {
        let signal = PriceBelowEma {
            column: "close".into(),
            period: 3,
        };
        assert_eq!(signal.name(), "price_below_ema");
    }

    #[test]
    fn sma_crossover_name() {
        let signal = SmaCrossover {
            column: "close".into(),
            fast_period: 2,
            slow_period: 5,
        };
        assert_eq!(signal.name(), "sma_crossover");
    }

    #[test]
    fn sma_crossunder_name() {
        let signal = SmaCrossunder {
            column: "close".into(),
            fast_period: 2,
            slow_period: 5,
        };
        assert_eq!(signal.name(), "sma_crossunder");
    }

    #[test]
    fn ema_crossover_name() {
        let signal = EmaCrossover {
            column: "close".into(),
            fast_period: 2,
            slow_period: 5,
        };
        assert_eq!(signal.name(), "ema_crossover");
    }

    #[test]
    fn ema_crossunder_name() {
        let signal = EmaCrossunder {
            column: "close".into(),
            fast_period: 2,
            slow_period: 5,
        };
        assert_eq!(signal.name(), "ema_crossunder");
    }

    #[test]
    fn price_above_sma_detects_above() {
        // Trending up: last prices should be above SMA
        let prices: Vec<f64> = (0..10).map(|i| 100.0 + f64::from(i) * 5.0).collect();
        let df = df! { "close" => &prices }.unwrap();
        let signal = PriceAboveSma {
            column: "close".into(),
            period: 3,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        // In an uptrend, later values should be above the SMA
        assert!(bools.get(9).unwrap());
    }

    #[test]
    fn price_below_sma_detects_below() {
        // Trending down: later prices should be below SMA
        let prices: Vec<f64> = (0..10).map(|i| 150.0 - f64::from(i) * 5.0).collect();
        let df = df! { "close" => &prices }.unwrap();
        let signal = PriceBelowSma {
            column: "close".into(),
            period: 3,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.get(9).unwrap());
    }

    #[test]
    fn sma_crossover_detects_crossover() {
        // Sharp price jump causes fast SMA to cross above slow SMA at index 5.
        // First 5 rows at 100.0 → fast SMA == slow SMA; row 5 jumps to 200.0
        // so fast(3) = (100+100+200)/3 = 133.3 > slow(5) = (100+100+100+100+200)/5 = 120
        // while previous: fast(3) = slow(5) = 100 → fast[4] <= slow[4].
        let prices: Vec<f64> = [100.0, 100.0, 100.0, 100.0, 100.0, 200.0, 200.0, 200.0]
            .into_iter()
            .collect();
        let df = df! { "close" => &prices }.unwrap();
        let signal = SmaCrossover {
            column: "close".into(),
            fast_period: 3,
            slow_period: 5,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.get(5).unwrap(), "crossover should be detected at index 5");
        assert!(!bools.get(6).unwrap(), "after crossover row should be false");
    }

    #[test]
    fn sma_crossunder_detects_crossunder() {
        // Symmetric to crossover: price drops sharply → fast SMA crosses below slow SMA.
        let prices: Vec<f64> = [200.0, 200.0, 200.0, 200.0, 200.0, 100.0, 100.0, 100.0]
            .into_iter()
            .collect();
        let df = df! { "close" => &prices }.unwrap();
        let signal = SmaCrossunder {
            column: "close".into(),
            fast_period: 3,
            slow_period: 5,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.get(5).unwrap(), "crossunder should be detected at index 5");
        assert!(!bools.get(6).unwrap(), "after crossunder row should be false");
    }

    #[test]
    fn ema_crossover_detects_crossover() {
        // A sharp price jump makes the fast EMA rise above the slow EMA.
        let prices: Vec<f64> = [100.0, 100.0, 100.0, 100.0, 100.0, 200.0, 200.0, 200.0]
            .into_iter()
            .collect();
        let df = df! { "close" => &prices }.unwrap();
        let signal = EmaCrossover {
            column: "close".into(),
            fast_period: 3,
            slow_period: 5,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        // At least one crossover event must be detected after the price jump
        let has_crossover = bools.into_no_null_iter().any(|b| b);
        assert!(has_crossover, "EMA crossover should be detected after price jump");
    }

    #[test]
    fn ema_crossunder_detects_crossunder() {
        // Use a rising trend then a sharp drop: in an uptrend the fast EMA (shorter
        // period) stays above the slow EMA; when prices crash the fast EMA falls below
        // the slow EMA first, producing a clear crossunder signal.
        let prices: Vec<f64> = [
            100.0, 105.0, 110.0, 115.0, 120.0, 125.0, 130.0, 135.0, 140.0, 145.0, 50.0, 50.0,
            50.0, 50.0, 50.0,
        ]
        .into_iter()
        .collect();
        let df = df! { "close" => &prices }.unwrap();
        let signal = EmaCrossunder {
            column: "close".into(),
            fast_period: 3,
            slow_period: 5,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        let has_crossunder = bools.into_no_null_iter().any(|b| b);
        assert!(has_crossunder, "EMA crossunder should be detected after price drop");
    }
}
