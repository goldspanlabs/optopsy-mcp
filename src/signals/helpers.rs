use polars::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A signal function takes a `DataFrame` and returns a boolean Series
/// indicating which rows meet the signal criteria.
pub trait SignalFn: Send + Sync {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError>;
    #[allow(dead_code)]
    fn name(&self) -> &'static str;
}

/// How an indicator should be displayed on a chart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum DisplayType {
    /// Overlay on the price chart (e.g., SMA, Bollinger Bands)
    Overlay,
    /// Separate subchart below price (e.g., RSI, MACD)
    Subchart,
}

/// A single date + value point for an indicator series.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct IndicatorPoint {
    pub date: i64,
    pub value: f64,
}

/// A named series of indicator values (e.g., "SMA(20)" or "Upper Band").
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct IndicatorSeries {
    pub label: String,
    pub values: Vec<IndicatorPoint>,
}

/// Complete indicator data for charting, including display hints and threshold lines.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct IndicatorData {
    pub name: String,
    pub display_type: DisplayType,
    pub series: Vec<IndicatorSeries>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub thresholds: Vec<f64>,
    /// Number of raw points before sampling (only set when data was down-sampled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_points: Option<usize>,
}

/// Collect indicator data from entry and exit signal specs, deduplicating by name.
///
/// Shared helper used by both options and stock backtest tools to avoid
/// duplicating the collect-and-dedup pattern.
pub fn collect_indicator_data(
    entry_signal: Option<&crate::signals::registry::SignalSpec>,
    exit_signal: Option<&crate::signals::registry::SignalSpec>,
    ohlcv_df: &DataFrame,
    date_col: &str,
    chart_indicators: &[(String, crate::signals::storage::ChartConfig)],
) -> Vec<IndicatorData> {
    let mut indicator_data: Vec<IndicatorData> = vec![];
    if let Some(spec) = entry_signal {
        indicator_data.extend(crate::signals::indicators::compute_indicator_data(
            spec, ohlcv_df, date_col,
        ));
    }
    if let Some(spec) = exit_signal {
        extend_indicators_deduped(
            &mut indicator_data,
            crate::signals::indicators::compute_indicator_data(spec, ohlcv_df, date_col),
        );
    }

    // Evaluate chart indicators (custom formula indicators with ChartConfig)
    for (formula, chart) in chart_indicators {
        if indicator_data.iter().any(|ind| ind.name == chart.label) {
            continue;
        }
        if let Some(ind) = crate::signals::indicators::compute_formula_indicator(
            formula, chart, ohlcv_df, date_col,
        ) {
            indicator_data.push(ind);
        }
    }

    indicator_data
}

/// Extend a collection of `IndicatorData` with new entries, deduplicating by name.
///
/// Uses a `HashSet` for O(1) lookups instead of O(n) linear scan per insertion.
pub fn extend_indicators_deduped(target: &mut Vec<IndicatorData>, new: Vec<IndicatorData>) {
    use std::collections::HashSet;
    let mut seen: HashSet<String> = target.iter().map(|ind| ind.name.clone()).collect();
    for ind in new {
        if seen.insert(ind.name.clone()) {
            target.push(ind);
        }
    }
}

/// Extract a column from a `DataFrame` as a `Vec<f64>`.
/// Returns an error if the column doesn't exist or can't be cast to f64.
/// Null values are preserved as `f64::NAN` to maintain row alignment.
pub fn column_to_f64(df: &DataFrame, col_name: &str) -> Result<Vec<f64>, PolarsError> {
    let s = df.column(col_name)?.cast(&DataType::Float64)?;
    let ca = s.f64()?;
    Ok(ca.iter().map(|opt| opt.unwrap_or(f64::NAN)).collect())
}

/// Pad a computed indicator series (shorter than the original) with NaN at the front,
/// then produce a boolean Series by applying a predicate to each value.
/// Rows with NaN (from the padding) are set to `false`.
pub fn pad_and_compare(
    values: &[f64],
    original_len: usize,
    pred: impl Fn(f64) -> bool,
    name: &str,
) -> Series {
    debug_assert!(
        values.len() <= original_len,
        "indicator output longer than input: {} > {}",
        values.len(),
        original_len
    );
    let pad = original_len.saturating_sub(values.len());
    let bools: Vec<bool> = std::iter::repeat_n(false, pad)
        .chain(values.iter().map(|&v| pred(v)))
        .collect();
    BooleanChunked::new(name.into(), &bools).into_series()
}

/// Pad indicator output to match original length, filling front with `f64::NAN`.
pub fn pad_series(values: &[f64], original_len: usize) -> Vec<f64> {
    debug_assert!(
        values.len() <= original_len,
        "indicator output longer than input: {} > {}",
        values.len(),
        original_len
    );
    let pad = original_len.saturating_sub(values.len());
    let mut result = vec![f64::NAN; pad];
    result.extend_from_slice(values);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_to_f64_basic() {
        let df = df! {
            "price" => &[1.0, 2.5, 3.0]
        }
        .unwrap();
        let vals = column_to_f64(&df, "price").unwrap();
        assert_eq!(vals, vec![1.0, 2.5, 3.0]);
    }

    #[test]
    fn column_to_f64_integer_column() {
        let df = df! {
            "count" => &[1i64, 2, 3]
        }
        .unwrap();
        let vals = column_to_f64(&df, "count").unwrap();
        assert_eq!(vals, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn column_to_f64_missing_column() {
        let df = df! {
            "price" => &[1.0, 2.0]
        }
        .unwrap();
        assert!(column_to_f64(&df, "nonexistent").is_err());
    }

    #[test]
    fn column_to_f64_nulls_become_nan() {
        let s = Series::new("val".into(), &[Some(1.0), None, Some(3.0)]);
        let df = DataFrame::new(3, vec![s.into()]).unwrap();
        let vals = column_to_f64(&df, "val").unwrap();
        assert_eq!(vals[0], 1.0);
        assert!(vals[1].is_nan());
        assert_eq!(vals[2], 3.0);
    }

    #[test]
    fn pad_and_compare_basic() {
        let values = [10.0, 20.0, 30.0];
        let result = pad_and_compare(&values, 5, |v| v > 15.0, "test");
        let bools = result.bool().unwrap();
        // First 2 positions are padded with false
        assert!(!bools.get(0).unwrap());
        assert!(!bools.get(1).unwrap());
        // values: 10 -> false, 20 -> true, 30 -> true
        assert!(!bools.get(2).unwrap());
        assert!(bools.get(3).unwrap());
        assert!(bools.get(4).unwrap());
    }

    #[test]
    fn pad_and_compare_no_padding_needed() {
        let values = [1.0, 2.0, 3.0];
        let result = pad_and_compare(&values, 3, |v| v >= 2.0, "test");
        let bools = result.bool().unwrap();
        assert!(!bools.get(0).unwrap());
        assert!(bools.get(1).unwrap());
        assert!(bools.get(2).unwrap());
    }

    #[test]
    fn pad_and_compare_empty_values() {
        let values: [f64; 0] = [];
        let result = pad_and_compare(&values, 3, |_| true, "test");
        let bools = result.bool().unwrap();
        assert_eq!(bools.len(), 3);
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn pad_series_basic() {
        let values = [10.0, 20.0];
        let result = pad_series(&values, 4);
        assert_eq!(result.len(), 4);
        assert!(result[0].is_nan());
        assert!(result[1].is_nan());
        assert_eq!(result[2], 10.0);
        assert_eq!(result[3], 20.0);
    }

    #[test]
    fn pad_series_same_length() {
        let values = [1.0, 2.0, 3.0];
        let result = pad_series(&values, 3);
        assert_eq!(result, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn pad_series_empty() {
        let values: [f64; 0] = [];
        let result = pad_series(&values, 3);
        assert_eq!(result.len(), 3);
        assert!(result.iter().all(|v| v.is_nan()));
    }

    #[test]
    fn collect_indicator_data_with_chart_indicators() {
        use chrono::NaiveDate;

        let n = 30usize;
        let dates: Vec<NaiveDate> = (0..n)
            .map(|i| {
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Duration::days(i as i64)
            })
            .collect();
        let close: Vec<f64> = (0..n).map(|i| 100.0 + (i as f64) * 0.5).collect();
        let open: Vec<f64> = close.iter().map(|c| c - 0.5).collect();
        let high: Vec<f64> = close.iter().map(|c| c + 2.0).collect();
        let low: Vec<f64> = close.iter().map(|c| c - 2.0).collect();
        let volume: Vec<f64> = vec![1000.0; n];

        let df = df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates),
            "open" => &open,
            "high" => &high,
            "low" => &low,
            "close" => &close,
            "volume" => &volume,
        }
        .unwrap();

        let chart = crate::signals::storage::ChartConfig {
            display_type: DisplayType::Subchart,
            label: "Close/Open Ratio".to_string(),
            thresholds: vec![1.0],
            expression: None,
        };

        let result = collect_indicator_data(
            None,
            None,
            &df,
            "date",
            &[("close / open".to_string(), chart)],
        );
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "Close/Open Ratio");
        assert_eq!(result[0].thresholds, vec![1.0]);
    }
}
