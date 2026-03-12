// Volatility compute functions: ATR, Bollinger Bands, Keltner Channels, IV aggregation

use crate::data::parquet::QUOTE_DATETIME_COL;
use polars::prelude::*;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_atr_basic() {
        let close: Vec<f64> = (0..20).map(|i| 100.0 + f64::from(i)).collect();
        let high: Vec<f64> = close.iter().map(|c| c + 2.0).collect();
        let low: Vec<f64> = close.iter().map(|c| c - 2.0).collect();
        let result = compute_atr(&close, &high, &low, 5);
        assert!(!result.is_empty());
        assert_eq!(result.len(), 20 - 5 + 1);
    }

    #[test]
    fn compute_atr_insufficient() {
        let close = vec![100.0, 101.0];
        let high = vec![103.0, 104.0];
        let low = vec![97.0, 98.0];
        let result = compute_atr(&close, &high, &low, 10);
        assert!(result.is_empty());
    }

    #[test]
    fn compute_atr_zero_period() {
        let close = vec![100.0; 10];
        let high = vec![102.0; 10];
        let low = vec![98.0; 10];
        let result = compute_atr(&close, &high, &low, 0);
        assert!(result.is_empty());
    }

    #[test]
    fn compute_bollinger_bands_basic() {
        let prices: Vec<f64> = (0..30).map(|i| 100.0 + f64::from(i) * 2.0).collect();
        let (lower, upper) = compute_bollinger_bands(&prices, 10);
        assert!(!lower.is_empty());
        assert_eq!(lower.len(), upper.len());
        assert_eq!(lower.len(), 30 - 10 + 1);
        // Upper should always be >= lower
        for (l, u) in lower.iter().zip(upper.iter()) {
            assert!(u >= l);
        }
    }

    #[test]
    fn compute_bollinger_bands_insufficient() {
        let prices = vec![100.0; 5];
        let (lower, upper) = compute_bollinger_bands(&prices, 20);
        assert!(lower.is_empty());
        assert!(upper.is_empty());
    }

    #[test]
    fn compute_bollinger_bands_zero_period() {
        let prices = vec![100.0; 10];
        let (lower, upper) = compute_bollinger_bands(&prices, 0);
        assert!(lower.is_empty());
        assert!(upper.is_empty());
    }

    #[test]
    fn compute_keltner_channel_basic() {
        let close: Vec<f64> = (0..30).map(|i| 100.0 + f64::from(i)).collect();
        let high: Vec<f64> = close.iter().map(|c| c + 2.0).collect();
        let low: Vec<f64> = close.iter().map(|c| c - 2.0).collect();
        let (lower, upper) = compute_keltner_channel(&close, &high, &low, 5, 2.0);
        assert!(!lower.is_empty());
        assert_eq!(lower.len(), upper.len());
        // Upper should always be >= lower
        for (l, u) in lower.iter().zip(upper.iter()) {
            assert!(u >= l);
        }
    }

    #[test]
    fn compute_keltner_channel_insufficient() {
        let close = vec![100.0, 101.0];
        let high = vec![102.0, 103.0];
        let low = vec![98.0, 99.0];
        let (lower, upper) = compute_keltner_channel(&close, &high, &low, 10, 2.0);
        assert!(lower.is_empty());
        assert!(upper.is_empty());
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
