//! Price-action signals: gap up/down, drawdown from rolling high, consecutive
//! rising/falling bars, and rate of change.
//!
//! Each struct implements `SignalFn` and produces a boolean series indicating
//! which bars satisfy the signal condition.

use super::helpers::{column_to_f64, SignalFn};
use polars::prelude::*;

/// Signal: gap up — open is significantly higher than the previous close.
/// Detects when `(open[i] - close[i-1]) / |close[i-1]| > threshold`.
pub struct GapUp {
    pub open_col: String,
    pub close_col: String,
    pub threshold: f64,
}

impl SignalFn for GapUp {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let open = column_to_f64(df, &self.open_col)?;
        let close = column_to_f64(df, &self.close_col)?;
        let n = close.len();
        if open.len() != n {
            return Err(PolarsError::ShapeMismatch(
                "open_col and close_col must have the same length".into(),
            ));
        }
        let mut bools = vec![false; n];
        for i in 1..n {
            if close[i - 1] != 0.0 {
                let gap_pct = (open[i] - close[i - 1]) / close[i - 1].abs();
                bools[i] = gap_pct > self.threshold;
            }
        }
        Ok(BooleanChunked::new("gap_up".into(), &bools).into_series())
    }
    fn name(&self) -> &'static str {
        "gap_up"
    }
}

/// Signal: gap down — open is significantly lower than the previous close.
/// Detects when `(close[i-1] - open[i]) / |close[i-1]| > threshold`.
pub struct GapDown {
    pub open_col: String,
    pub close_col: String,
    pub threshold: f64,
}

impl SignalFn for GapDown {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let open = column_to_f64(df, &self.open_col)?;
        let close = column_to_f64(df, &self.close_col)?;
        let n = close.len();
        if open.len() != n {
            return Err(PolarsError::ShapeMismatch(
                "open_col and close_col must have the same length".into(),
            ));
        }
        let mut bools = vec![false; n];
        for i in 1..n {
            if close[i - 1] != 0.0 {
                let gap_pct = (close[i - 1] - open[i]) / close[i - 1].abs();
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
            "open" => &[100.0, 111.0, 111.5, 121.0, 121.5],
            "close" => &[100.0, 110.0, 111.0, 120.0, 121.0]
        }
        .unwrap();
        let signal = GapUp {
            open_col: "open".into(),
            close_col: "close".into(),
            threshold: 0.05,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        // open[1]=111 vs close[0]=100: 11% gap up
        assert!(bools.get(1).unwrap());
        // open[3]=121 vs close[2]=111: ~8.1% gap up
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

    #[test]
    fn gap_down_detects_gap() {
        let df = df! {
            "open"  => &[100.0, 100.0, 100.0, 85.0, 95.0],
            "close" => &[100.0, 100.0, 100.0, 90.0, 96.0]
        }
        .unwrap();
        let signal = GapDown {
            open_col: "open".into(),
            close_col: "close".into(),
            threshold: 0.05,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        // open[3]=85 vs close[2]=100: (100-85)/100 = 15% gap down
        assert!(bools.get(3).unwrap());
        assert!(!bools.get(0).unwrap());
    }

    #[test]
    fn gap_up_first_row_always_false() {
        let df = df! {
            "open"  => &[200.0, 100.0],
            "close" => &[100.0, 100.0]
        }
        .unwrap();
        let signal = GapUp {
            open_col: "open".into(),
            close_col: "close".into(),
            threshold: 0.01,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(!bools.get(0).unwrap());
    }

    #[test]
    fn gap_down_first_row_always_false() {
        let df = df! {
            "open"  => &[50.0, 100.0],
            "close" => &[100.0, 100.0]
        }
        .unwrap();
        let signal = GapDown {
            open_col: "open".into(),
            close_col: "close".into(),
            threshold: 0.01,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(!bools.get(0).unwrap());
    }

    #[test]
    fn consecutive_down_detects_streak() {
        let df = df! {
            "close" => &[110.0, 109.0, 108.0, 107.0, 108.0, 107.0, 106.0, 105.0]
        }
        .unwrap();
        let signal = ConsecutiveDown {
            column: "close".into(),
            count: 3,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.get(3).unwrap());
        assert!(bools.get(7).unwrap());
        assert!(!bools.get(4).unwrap());
    }

    #[test]
    fn consecutive_down_no_streak() {
        let df = df! {
            "close" => &[100.0, 101.0, 102.0, 103.0]
        }
        .unwrap();
        let signal = ConsecutiveDown {
            column: "close".into(),
            count: 2,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn rate_of_change_detects_change() {
        let df = df! {
            "close" => &[100.0, 100.0, 100.0, 120.0, 130.0]
        }
        .unwrap();
        let signal = RateOfChange {
            column: "close".into(),
            period: 2,
            threshold: 0.15,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        // index 3: (120 - 100) / 100 = 0.20 > 0.15
        assert!(bools.get(3).unwrap());
        // index 4: (130 - 100) / 100 = 0.30 > 0.15
        assert!(bools.get(4).unwrap());
        // index 2: (100 - 100) / 100 = 0.0 < 0.15
        assert!(!bools.get(2).unwrap());
    }

    #[test]
    fn rate_of_change_insufficient_period() {
        let df = df! {
            "close" => &[100.0, 200.0]
        }
        .unwrap();
        let signal = RateOfChange {
            column: "close".into(),
            period: 5,
            threshold: 0.01,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn drawdown_no_drawdown_in_uptrend() {
        let df = df! {
            "close" => &[100.0, 101.0, 102.0, 103.0, 104.0]
        }
        .unwrap();
        let signal = DrawdownBelow {
            column: "close".into(),
            window: 3,
            threshold: 0.01,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn gap_up_name() {
        let signal = GapUp {
            open_col: "open".into(),
            close_col: "close".into(),
            threshold: 0.01,
        };
        assert_eq!(signal.name(), "gap_up");
    }

    #[test]
    fn gap_down_name() {
        let signal = GapDown {
            open_col: "open".into(),
            close_col: "close".into(),
            threshold: 0.01,
        };
        assert_eq!(signal.name(), "gap_down");
    }

    #[test]
    fn drawdown_below_name() {
        let signal = DrawdownBelow {
            column: "close".into(),
            window: 5,
            threshold: 0.10,
        };
        assert_eq!(signal.name(), "drawdown_below");
    }

    #[test]
    fn consecutive_up_name() {
        let signal = ConsecutiveUp {
            column: "close".into(),
            count: 3,
        };
        assert_eq!(signal.name(), "consecutive_up");
    }

    #[test]
    fn consecutive_down_name() {
        let signal = ConsecutiveDown {
            column: "close".into(),
            count: 3,
        };
        assert_eq!(signal.name(), "consecutive_down");
    }

    #[test]
    fn rate_of_change_name() {
        let signal = RateOfChange {
            column: "close".into(),
            period: 5,
            threshold: 0.05,
        };
        assert_eq!(signal.name(), "rate_of_change");
    }

    #[test]
    fn gap_up_zero_close_handled() {
        let df = df! {
            "open"  => &[0.0, 100.0],
            "close" => &[0.0, 100.0]
        }
        .unwrap();
        let signal = GapUp {
            open_col: "open".into(),
            close_col: "close".into(),
            threshold: 0.01,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        // close[0] == 0, so gap calculation should be skipped
        assert!(!bools.get(1).unwrap());
    }
}
