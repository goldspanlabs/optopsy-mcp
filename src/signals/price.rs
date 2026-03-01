#![allow(dead_code)]
// Price signals: Gap, drawdown, consecutive moves

use super::helpers::{column_to_f64, SignalFn};
use polars::prelude::*;

/// Signal: gap up — current value opens significantly higher than previous close.
/// Detects when (current - previous) / |previous| > threshold.
pub struct GapUp {
    pub column: String,
    pub threshold: f64,
}

impl SignalFn for GapUp {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.column)?;
        let n = prices.len();
        let mut bools = vec![false; n];
        for i in 1..n {
            if prices[i - 1] != 0.0 {
                let gap_pct = (prices[i] - prices[i - 1]) / prices[i - 1].abs();
                bools[i] = gap_pct > self.threshold;
            }
        }
        Ok(BooleanChunked::new("gap_up".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "gap_up"
    }
}

/// Signal: gap down — current value drops significantly from previous.
pub struct GapDown {
    pub column: String,
    pub threshold: f64,
}

impl SignalFn for GapDown {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.column)?;
        let n = prices.len();
        let mut bools = vec![false; n];
        for i in 1..n {
            if prices[i - 1] != 0.0 {
                let gap_pct = (prices[i - 1] - prices[i]) / prices[i - 1].abs();
                bools[i] = gap_pct > self.threshold;
            }
        }
        Ok(BooleanChunked::new("gap_down".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "gap_down"
    }
}

/// Signal: drawdown from rolling maximum exceeds a threshold.
/// Drawdown = (price - `rolling_max`) / `rolling_max`.
/// True when drawdown < -threshold (e.g., threshold = 0.05 means 5% drawdown).
pub struct DrawdownBelow {
    pub column: String,
    pub window: usize,
    pub threshold: f64,
}

impl SignalFn for DrawdownBelow {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.column)?;
        let n = prices.len();
        let mut bools = vec![false; n];
        for i in 0..n {
            let start = i.saturating_sub(self.window.saturating_sub(1));
            let rolling_max = prices[start..=i]
                .iter()
                .copied()
                .fold(f64::NEG_INFINITY, f64::max);
            if rolling_max > 0.0 {
                let dd = (prices[i] - rolling_max) / rolling_max;
                bools[i] = dd < -self.threshold;
            }
        }
        Ok(BooleanChunked::new("drawdown_below".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "drawdown_below"
    }
}

/// Signal: N consecutive higher closes.
pub struct ConsecutiveUp {
    pub column: String,
    pub count: usize,
}

impl SignalFn for ConsecutiveUp {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.column)?;
        let n = prices.len();
        let mut bools = vec![false; n];
        let mut streak = 0usize;
        for i in 1..n {
            if prices[i] > prices[i - 1] {
                streak += 1;
            } else {
                streak = 0;
            }
            if streak >= self.count {
                bools[i] = true;
            }
        }
        Ok(BooleanChunked::new("consecutive_up".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "consecutive_up"
    }
}

/// Signal: N consecutive lower closes.
pub struct ConsecutiveDown {
    pub column: String,
    pub count: usize,
}

impl SignalFn for ConsecutiveDown {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.column)?;
        let n = prices.len();
        let mut bools = vec![false; n];
        let mut streak = 0usize;
        for i in 1..n {
            if prices[i] < prices[i - 1] {
                streak += 1;
            } else {
                streak = 0;
            }
            if streak >= self.count {
                bools[i] = true;
            }
        }
        Ok(BooleanChunked::new("consecutive_down".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "consecutive_down"
    }
}

/// Signal: price change from N periods ago exceeds a threshold (rate of change).
pub struct RateOfChange {
    pub column: String,
    pub period: usize,
    pub threshold: f64,
}

impl SignalFn for RateOfChange {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let prices = column_to_f64(df, &self.column)?;
        let n = prices.len();
        let mut bools = vec![false; n];
        for i in self.period..n {
            if prices[i - self.period] != 0.0 {
                let roc = (prices[i] - prices[i - self.period]) / prices[i - self.period].abs();
                bools[i] = roc > self.threshold;
            }
        }
        Ok(BooleanChunked::new("rate_of_change".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "rate_of_change"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gap_up_detects_gap() {
        let df = df! {
            "close" => &[100.0, 110.0, 111.0, 120.0, 121.0]
        }
        .unwrap();
        let signal = GapUp {
            column: "close".into(),
            threshold: 0.05,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        // 100 -> 110 = 10% gap, 111 -> 120 ~8% gap
        assert!(bools.get(1).unwrap());
        assert!(bools.get(3).unwrap());
        assert!(!bools.get(0).unwrap());
    }

    #[test]
    fn consecutive_up_detects_streak() {
        let df = df! {
            "close" => &[100.0, 101.0, 102.0, 103.0, 102.0, 103.0, 104.0, 105.0]
        }
        .unwrap();
        let signal = ConsecutiveUp {
            column: "close".into(),
            count: 3,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.get(3).unwrap());
        assert!(bools.get(7).unwrap());
        assert!(!bools.get(5).unwrap());
    }

    #[test]
    fn drawdown_detects_decline() {
        let df = df! {
            "close" => &[100.0, 110.0, 105.0, 100.0, 95.0]
        }
        .unwrap();
        let signal = DrawdownBelow {
            column: "close".into(),
            window: 5,
            threshold: 0.10,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        // 95 / 110 = 0.136 drawdown > 10%
        assert!(bools.get(4).unwrap());
    }
}
