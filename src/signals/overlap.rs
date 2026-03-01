// Overlap signals: SMA, EMA, crossovers

use super::helpers::{column_to_f64, pad_series, SignalFn};
use polars::prelude::*;
use rust_ti::standard_indicators::bulk as sti;

/// Signal: price is above its Simple Moving Average.
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
}
