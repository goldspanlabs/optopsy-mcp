// Volatility signals: ATR, Bollinger Bands, Keltner Channels, IV Rank, IV Percentile

use super::helpers::{column_to_f64, pad_series, SignalFn};
use crate::data::parquet::QUOTE_DATETIME_COL;
use polars::prelude::*;

/// Extract the IV column with a descriptive error message.
fn iv_column(df: &DataFrame) -> Result<Vec<f64>, PolarsError> {
    column_to_f64(df, "iv").map_err(|_| {
        PolarsError::ColumnNotFound(
            "IV signals require an 'iv' column. Standard OHLCV data from fetch_to_parquet \
             does not include IV. You need pre-processed data with an 'iv' column."
                .into(),
        )
    })
}

/// Compute ATR values for a given period.
pub(crate) fn compute_atr(close: &[f64], high: &[f64], low: &[f64], period: usize) -> Vec<f64> {
    let n = close.len();
    if period == 0 || n < period {
        return vec![];
    }
    (0..=n - period)
        .map(|i| {
            let end = i + period;
            rust_ti::other_indicators::single::average_true_range(
                &close[i..end],
                &high[i..end],
                &low[i..end],
                rust_ti::ConstantModelType::SimpleMovingAverage,
            )
        })
        .collect()
}

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
        let atr_values = compute_atr(&close, &high, &low, self.period);
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
        let atr_values = compute_atr(&close, &high, &low, self.period);
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

/// Compute Bollinger Bands and return (lower, upper) band values.
pub(crate) fn compute_bollinger_bands(prices: &[f64], period: usize) -> (Vec<f64>, Vec<f64>) {
    if period == 0 || prices.len() < period {
        return (vec![], vec![]);
    }
    let bbands = rust_ti::candle_indicators::bulk::moving_constant_bands(
        prices,
        rust_ti::ConstantModelType::SimpleMovingAverage,
        rust_ti::DeviationModel::StandardDeviation,
        2.0,
        period,
    );
    let lower: Vec<f64> = bbands.iter().map(|t| t.0).collect();
    let upper: Vec<f64> = bbands.iter().map(|t| t.2).collect();
    (lower, upper)
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
        let (lower, _) = compute_bollinger_bands(&prices, self.period);
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
        let (_, upper) = compute_bollinger_bands(&prices, self.period);
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

/// Compute Keltner Channel and return (lower, upper) band values.
pub(crate) fn compute_keltner_channel(
    close: &[f64],
    high: &[f64],
    low: &[f64],
    period: usize,
    multiplier: f64,
) -> (Vec<f64>, Vec<f64>) {
    if period == 0 || close.len() < period {
        return (vec![], vec![]);
    }
    let kc = rust_ti::candle_indicators::bulk::keltner_channel(
        high,
        low,
        close,
        rust_ti::ConstantModelType::ExponentialMovingAverage,
        rust_ti::ConstantModelType::SimpleMovingAverage,
        multiplier,
        period,
    );
    let lower: Vec<f64> = kc.iter().map(|t| t.0).collect();
    let upper: Vec<f64> = kc.iter().map(|t| t.2).collect();
    (lower, upper)
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
        let (lower, _) = compute_keltner_channel(&close, &high, &low, self.period, self.multiplier);
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
        let (_, upper) = compute_keltner_channel(&close, &high, &low, self.period, self.multiplier);
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

/// Aggregate daily implied volatility from an options chain `DataFrame`.
///
/// Filters to near-ATM options (absolute delta between 0.30 and 0.70), then
/// computes the median `implied_volatility` per quote date. Returns a `DataFrame`
/// with columns `["date", "iv"]` sorted by date.
pub fn aggregate_daily_iv(options_df: &DataFrame) -> Result<DataFrame, PolarsError> {
    let columns: Vec<&str> = options_df
        .get_column_names()
        .iter()
        .map(|c| c.as_str())
        .collect();

    if !columns.contains(&"implied_volatility") {
        return Err(PolarsError::ComputeError(
            "Options data does not contain an 'implied_volatility' column. \
             IV Rank/Percentile signals require options data with implied volatility."
                .into(),
        ));
    }
    if !columns.contains(&QUOTE_DATETIME_COL) {
        return Err(PolarsError::ComputeError(
            format!(
                "Options data does not contain a '{QUOTE_DATETIME_COL}' column. \
                 IV aggregation requires '{QUOTE_DATETIME_COL}' to group by date."
            )
            .into(),
        ));
    }

    let has_delta = columns.contains(&"delta");

    let lazy = options_df.clone().lazy();

    // Filter to near-ATM options if delta column exists, otherwise use all rows
    let filtered = if has_delta {
        lazy.filter(
            col("delta")
                .abs()
                .gt_eq(lit(0.30))
                .and(col("delta").abs().lt_eq(lit(0.70))),
        )
    } else {
        lazy
    };

    // Extract date from quote_datetime and compute median IV per date
    let result = filtered
        .with_column(col(QUOTE_DATETIME_COL).cast(DataType::Date).alias("date"))
        .group_by([col("date")])
        .agg([col("implied_volatility").median().alias("iv")])
        .sort(["date"], SortMultipleOptions::default())
        .collect()?;

    Ok(result)
}

/// Signal: IV Rank is above a threshold.
///
/// `IV Rank = (current_iv - lookback_min) / (lookback_max - lookback_min) × 100`
/// Requires an `"iv"` column in the `DataFrame` (produced by `aggregate_daily_iv`).
pub struct IvRankAbove {
    pub lookback: usize,
    pub threshold: f64,
}

impl SignalFn for IvRankAbove {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let iv = iv_column(df)?;
        Ok(compute_iv_rank_signal(
            &iv,
            self.lookback,
            self.threshold,
            true,
        ))
    }
    fn name(&self) -> &'static str {
        "iv_rank_above"
    }
}

/// Signal: IV Rank is below a threshold.
pub struct IvRankBelow {
    pub lookback: usize,
    pub threshold: f64,
}

impl SignalFn for IvRankBelow {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let iv = iv_column(df)?;
        Ok(compute_iv_rank_signal(
            &iv,
            self.lookback,
            self.threshold,
            false,
        ))
    }
    fn name(&self) -> &'static str {
        "iv_rank_below"
    }
}

/// Signal: IV Percentile is above a threshold.
///
/// IV Percentile = % of days in lookback window with IV below current IV × 100
pub struct IvPercentileAbove {
    pub lookback: usize,
    pub threshold: f64,
}

impl SignalFn for IvPercentileAbove {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let iv = iv_column(df)?;
        Ok(compute_iv_percentile_signal(
            &iv,
            self.lookback,
            self.threshold,
            true,
        ))
    }
    fn name(&self) -> &'static str {
        "iv_percentile_above"
    }
}

/// Signal: IV Percentile is below a threshold.
pub struct IvPercentileBelow {
    pub lookback: usize,
    pub threshold: f64,
}

impl SignalFn for IvPercentileBelow {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let iv = iv_column(df)?;
        Ok(compute_iv_percentile_signal(
            &iv,
            self.lookback,
            self.threshold,
            false,
        ))
    }
    fn name(&self) -> &'static str {
        "iv_percentile_below"
    }
}

/// Compute IV Rank for each row and return a boolean `Series`.
/// `IV Rank = (current - window_min) / (window_max - window_min) × 100`
fn compute_iv_rank_signal(iv: &[f64], lookback: usize, threshold: f64, above: bool) -> Series {
    let n = iv.len();
    if lookback == 0 {
        let name = if above {
            "iv_rank_above"
        } else {
            "iv_rank_below"
        };
        return BooleanChunked::new(name.into(), vec![false; n]).into_series();
    }
    let bools: Vec<bool> = (0..n)
        .map(|i| {
            if i + 1 < lookback {
                return false;
            }
            let window = &iv[i + 1 - lookback..=i];
            let current = iv[i];
            if current.is_nan() {
                return false;
            }
            let mut min = f64::INFINITY;
            let mut max = f64::NEG_INFINITY;
            let mut valid_len = 0usize;
            for &v in window {
                if !v.is_nan() {
                    valid_len += 1;
                    if v < min {
                        min = v;
                    }
                    if v > max {
                        max = v;
                    }
                }
            }
            // Require at least half the lookback to have valid IV data
            if valid_len < lookback / 2 + 1 {
                return false;
            }
            let range = max - min;
            if range <= 0.0 {
                return false;
            }
            let rank = (current - min) / range * 100.0;
            if above {
                rank > threshold
            } else {
                rank < threshold
            }
        })
        .collect();
    let name = if above {
        "iv_rank_above"
    } else {
        "iv_rank_below"
    };
    BooleanChunked::new(name.into(), &bools).into_series()
}

/// Compute IV Percentile for each row and return a boolean `Series`.
/// `IV Percentile = count(window_values < current) / window_len × 100`
fn compute_iv_percentile_signal(
    iv: &[f64],
    lookback: usize,
    threshold: f64,
    above: bool,
) -> Series {
    let n = iv.len();
    if lookback == 0 {
        let name = if above {
            "iv_percentile_above"
        } else {
            "iv_percentile_below"
        };
        return BooleanChunked::new(name.into(), vec![false; n]).into_series();
    }
    let bools: Vec<bool> = (0..n)
        .map(|i| {
            if i < lookback {
                return false;
            }
            let window = &iv[i - lookback..i]; // excludes current, exactly `lookback` points
            let current = iv[i];
            if current.is_nan() {
                return false;
            }
            let mut valid_len = 0usize;
            let mut below_count = 0usize;
            for &v in window {
                if v.is_nan() {
                    continue;
                }
                valid_len += 1;
                if v < current {
                    below_count += 1;
                }
            }
            // Require at least half the lookback to have valid IV data
            if valid_len < lookback / 2 + 1 {
                return false;
            }
            let percentile = below_count as f64 / valid_len as f64 * 100.0;
            if above {
                percentile > threshold
            } else {
                percentile < threshold
            }
        })
        .collect();
    let name = if above {
        "iv_percentile_above"
    } else {
        "iv_percentile_below"
    };
    BooleanChunked::new(name.into(), &bools).into_series()
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

    #[test]
    fn iv_rank_above_basic() {
        // 5 values with lookback=3: window of exactly 3 points including current
        // iv = [10, 20, 30, 40, 50]
        // At i=2: window=[10,20,30], rank=(30-10)/(30-10)*100=100 > 50 → true
        // At i=3: window=[20,30,40], rank=(40-20)/(40-20)*100=100 > 50 → true
        // At i=4: window=[30,40,50], rank=(50-30)/(50-30)*100=100 > 50 → true
        let iv: Vec<f64> = vec![10.0, 20.0, 30.0, 40.0, 50.0];
        let df = df! { "iv" => &iv }.unwrap();
        let signal = IvRankAbove {
            lookback: 3,
            threshold: 50.0,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        // First 2 should be false (insufficient lookback)
        assert!(!bools.get(0).unwrap());
        assert!(!bools.get(1).unwrap());
        // At i=2,3,4: rank=100 > 50 → true
        assert!(bools.get(2).unwrap());
        assert!(bools.get(3).unwrap());
        assert!(bools.get(4).unwrap());
    }

    #[test]
    fn iv_rank_below_basic() {
        // iv = [50, 40, 30, 20, 10]
        // At i=2: window=[50,40,30], rank=(30-30)/(50-30)*100=0 < 50 → true
        // At i=3: window=[40,30,20], rank=(20-20)/(40-20)*100=0 < 50 → true
        // At i=4: window=[30,20,10], rank=(10-10)/(30-10)*100=0 < 50 → true
        let iv: Vec<f64> = vec![50.0, 40.0, 30.0, 20.0, 10.0];
        let df = df! { "iv" => &iv }.unwrap();
        let signal = IvRankBelow {
            lookback: 3,
            threshold: 50.0,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(!bools.get(0).unwrap());
        assert!(!bools.get(1).unwrap());
        assert!(bools.get(2).unwrap()); // rank=0 < 50
        assert!(bools.get(3).unwrap()); // rank=0 < 50
        assert!(bools.get(4).unwrap()); // rank=0 < 50
    }

    #[test]
    fn iv_percentile_above_basic() {
        // iv = [10, 20, 30, 40, 50]
        // At i=3 (lookback=3): window=[10,20,30] (excludes current 40)
        //   below_count=3 (all below 40), pct=3/3*100=100
        // At i=4: window=[20,30,40], below_count=3 (all below 50), pct=100
        let iv: Vec<f64> = vec![10.0, 20.0, 30.0, 40.0, 50.0];
        let df = df! { "iv" => &iv }.unwrap();
        let signal = IvPercentileAbove {
            lookback: 3,
            threshold: 50.0,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(!bools.get(0).unwrap());
        assert!(!bools.get(1).unwrap());
        assert!(!bools.get(2).unwrap());
        assert!(bools.get(3).unwrap()); // pct=100 > 50
        assert!(bools.get(4).unwrap()); // pct=100 > 50
    }

    #[test]
    fn iv_percentile_below_basic() {
        // iv = [50, 40, 30, 20, 10]
        // At i=3: window=[50,40,30], below 20? → 0, pct=0
        // At i=4: window=[40,30,20], below 10? → 0, pct=0
        let iv: Vec<f64> = vec![50.0, 40.0, 30.0, 20.0, 10.0];
        let df = df! { "iv" => &iv }.unwrap();
        let signal = IvPercentileBelow {
            lookback: 3,
            threshold: 50.0,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.get(3).unwrap()); // pct=0 < 50
        assert!(bools.get(4).unwrap()); // pct=0 < 50
    }

    #[test]
    fn iv_rank_insufficient_data() {
        let iv: Vec<f64> = vec![10.0, 20.0];
        let df = df! { "iv" => &iv }.unwrap();
        let signal = IvRankAbove {
            lookback: 5,
            threshold: 50.0,
        };
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn iv_rank_above_name() {
        let signal = IvRankAbove {
            lookback: 252,
            threshold: 50.0,
        };
        assert_eq!(signal.name(), "iv_rank_above");
    }

    #[test]
    fn iv_rank_below_name() {
        let signal = IvRankBelow {
            lookback: 252,
            threshold: 50.0,
        };
        assert_eq!(signal.name(), "iv_rank_below");
    }

    #[test]
    fn iv_percentile_above_name() {
        let signal = IvPercentileAbove {
            lookback: 252,
            threshold: 50.0,
        };
        assert_eq!(signal.name(), "iv_percentile_above");
    }

    #[test]
    fn iv_percentile_below_name() {
        let signal = IvPercentileBelow {
            lookback: 252,
            threshold: 50.0,
        };
        assert_eq!(signal.name(), "iv_percentile_below");
    }

    #[test]
    fn aggregate_daily_iv_basic() {
        use chrono::NaiveDate;
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 2)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ];
        let dt_chunked = DatetimeChunked::new(PlSmallStr::from("quote_datetime"), &dates);
        let df = DataFrame::new(
            3,
            vec![
                dt_chunked.into_series().into(),
                Series::new("implied_volatility".into(), &[0.20, 0.30, 0.25]).into(),
                Series::new("delta".into(), &[0.50, 0.45, 0.50]).into(),
            ],
        )
        .unwrap();

        let result = aggregate_daily_iv(&df).unwrap();
        assert_eq!(result.height(), 2); // 2 unique dates
        assert!(result.get_column_names().iter().any(|c| c.as_str() == "iv"));
    }

    #[test]
    fn aggregate_daily_iv_missing_column_errors() {
        let df = df! { "close" => &[100.0, 101.0] }.unwrap();
        let result = aggregate_daily_iv(&df);
        assert!(result.is_err());
    }
}
