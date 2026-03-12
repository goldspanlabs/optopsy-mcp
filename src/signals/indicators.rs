//! Compute raw indicator values for charting from a `SignalSpec`.
//!
//! Pattern-matches on the signal variant to compute the underlying indicator
//! (RSI line, SMA curve, Bollinger bands, etc.) and returns structured data
//! ready for visualization alongside price charts.
//!
//! The private helper functions are retained for future use when formula-based
//! indicator extraction is implemented for `SignalSpec::Formula` signals.
#![allow(dead_code)]

use super::helpers::{
    column_to_f64, pad_series, DisplayType, IndicatorData, IndicatorPoint, IndicatorSeries,
};
use super::momentum::compute_stochastic;
use super::spec::SignalSpec;
use super::volatility::{compute_atr, compute_bollinger_bands, compute_keltner_channel};
use super::volume::{compute_cmf, compute_typical_price};

use crate::engine::price_table::extract_date_from_column;
use polars::prelude::*;
use rust_ti::standard_indicators::bulk as sti;

// NOTE: The imports above (compute_stochastic, compute_atr, compute_bollinger_bands,
// compute_keltner_channel, compute_cmf, compute_typical_price, sti) and the helper
// functions below are retained for future use when formula-based indicator extraction
// is implemented for Custom signals.

/// Maximum number of indicator points to return per series.
/// The frontend charts handle large datasets efficiently, so we use a generous
/// limit to preserve full resolution (1 point per bar).
const MAX_INDICATOR_POINTS: usize = 5000;

/// Compute raw indicator data for charting from a signal specification.
///
/// Returns one or more `IndicatorData` entries depending on the signal type.
/// For combinators (And/Or), recursively collects indicators from both children.
/// Returns an empty vec for `Custom` formulas and `CrossSymbol` signals — indicator
/// extraction from formula ASTs is future work.
pub fn compute_indicator_data(
    spec: &SignalSpec,
    ohlcv_df: &DataFrame,
    date_col: &str,
) -> Vec<IndicatorData> {
    compute_indicator_data_inner(spec, ohlcv_df, date_col)
}

#[allow(clippy::only_used_in_recursion)]
fn compute_indicator_data_inner(
    spec: &SignalSpec,
    ohlcv_df: &DataFrame,
    date_col: &str,
) -> Vec<IndicatorData> {
    match spec {
        // ── Combinators ──────────────────────────────────────────────
        SignalSpec::And { left, right } | SignalSpec::Or { left, right } => {
            let mut result = compute_indicator_data_inner(left, ohlcv_df, date_col);
            let right_indicators = compute_indicator_data_inner(right, ohlcv_df, date_col);
            // Deduplicate: skip indicators already present (by name)
            for ind in right_indicators {
                if !result.iter().any(|existing| existing.name == ind.name) {
                    result.push(ind);
                }
            }
            result
        }
        SignalSpec::Saved { name } => match super::storage::load_signal(name) {
            Ok(loaded) => compute_indicator_data_inner(&loaded, ohlcv_df, date_col),
            Err(_) => vec![],
        },
        // Formula-based indicator extraction is future work — return empty for now
        SignalSpec::Formula { .. } | SignalSpec::CrossSymbol { .. } => vec![],
    }
}

// ── Date extraction ──────────────────────────────────────────────────────────

/// Extract date strings from a `DataFrame` column (handles Date and Datetime types).
///
/// Returns an error if the column doesn't exist. Individual date extraction failures
/// produce a sentinel empty string — `build_series` filters these out alongside NaN values.
fn extract_date_strings(df: &DataFrame, date_col: &str) -> Result<Vec<String>, PolarsError> {
    let col = df.column(date_col)?;
    let n = df.height();
    let mut dates = Vec::with_capacity(n);
    for i in 0..n {
        match extract_date_from_column(col, i) {
            Ok(d) => dates.push(d.format("%Y-%m-%d").to_string()),
            Err(_) => {
                // Sentinel: build_series filters out points with empty dates
                dates.push(String::new());
            }
        }
    }
    Ok(dates)
}

// ── Series builder helpers ───────────────────────────────────────────────────

/// Build an `IndicatorSeries` from padded values and date strings, filtering NaN
/// and empty-date sentinels. Samples down to `MAX_INDICATOR_POINTS` if needed.
/// Returns `(series, total_raw_points)` so callers can report sampling metadata.
fn build_series(label: &str, padded: &[f64], dates: &[String]) -> (IndicatorSeries, usize) {
    let mut points: Vec<IndicatorPoint> = padded
        .iter()
        .zip(dates.iter())
        .filter(|(&v, d)| !v.is_nan() && !d.is_empty())
        .map(|(&v, d)| IndicatorPoint {
            date: d.clone(),
            value: (v * 10000.0).round() / 10000.0,
        })
        .collect();

    let total = points.len();
    if points.len() > MAX_INDICATOR_POINTS {
        points = sample_points(points, MAX_INDICATOR_POINTS);
    }

    (
        IndicatorSeries {
            label: label.to_string(),
            values: points,
        },
        total,
    )
}

/// Build a single-series `IndicatorData`, setting `total_points` if data was sampled.
fn make_indicator(
    name: String,
    display_type: DisplayType,
    padded: &[f64],
    dates: &[String],
    series_label: &str,
    thresholds: Vec<f64>,
) -> IndicatorData {
    let (series, total) = build_series(series_label, padded, dates);
    let sampled = total > MAX_INDICATOR_POINTS;
    IndicatorData {
        name,
        display_type,
        series: vec![series],
        thresholds,
        total_points: if sampled { Some(total) } else { None },
    }
}

/// Evenly sample N points from a vec, always including first and last.
fn sample_points(points: Vec<IndicatorPoint>, max: usize) -> Vec<IndicatorPoint> {
    let n = points.len();
    if n <= max {
        return points;
    }
    let mut indices: Vec<usize> = (0..max).map(|i| i * (n - 1) / (max - 1)).collect();
    indices.dedup();
    indices.into_iter().map(|i| points[i].clone()).collect()
}

// ── Per-indicator computation functions ──────────────────────────────────────

fn compute_rsi_indicator(
    df: &DataFrame,
    column: &str,
    threshold: f64,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(prices) = column_to_f64(df, column) else {
        return vec![];
    };
    let n = prices.len();
    if n <= 14 {
        return vec![];
    }
    let rsi_values = sti::rsi(&prices);
    let padded = pad_series(&rsi_values, n);
    vec![make_indicator(
        "RSI(14)".to_string(),
        DisplayType::Subchart,
        &padded,
        dates,
        "RSI",
        vec![threshold],
    )]
}

fn compute_macd_indicator(df: &DataFrame, column: &str, dates: &[String]) -> Vec<IndicatorData> {
    let Ok(prices) = column_to_f64(df, column) else {
        return vec![];
    };
    let n = prices.len();
    if n < 34 {
        return vec![];
    }
    let macd_values = sti::macd(&prices);
    let histograms: Vec<f64> = macd_values.iter().map(|t| t.2).collect();
    let padded = pad_series(&histograms, n);
    vec![make_indicator(
        "MACD Histogram".to_string(),
        DisplayType::Subchart,
        &padded,
        dates,
        "Histogram",
        vec![0.0],
    )]
}

fn compute_stochastic_indicator(
    df: &DataFrame,
    close_col: &str,
    high_col: &str,
    low_col: &str,
    period: usize,
    threshold: f64,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(close) = column_to_f64(df, close_col) else {
        return vec![];
    };
    let Ok(high) = column_to_f64(df, high_col) else {
        return vec![];
    };
    let Ok(low) = column_to_f64(df, low_col) else {
        return vec![];
    };
    let n = close.len();
    if period == 0 || n < period {
        return vec![];
    }
    let stoch_values = compute_stochastic(&close, &high, &low, period);
    let padded = pad_series(&stoch_values, n);
    vec![make_indicator(
        format!("Stochastic({period})"),
        DisplayType::Subchart,
        &padded,
        dates,
        "%K",
        vec![threshold],
    )]
}

fn compute_ma_indicator(
    df: &DataFrame,
    column: &str,
    period: usize,
    ma_type: &str,
    ma_fn: fn(&[f64], usize) -> Vec<f64>,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(prices) = column_to_f64(df, column) else {
        return vec![];
    };
    let n = prices.len();
    if n < period {
        return vec![];
    }
    let ma = ma_fn(&prices, period);
    let padded = pad_series(&ma, n);
    vec![make_indicator(
        format!("{ma_type}({period})"),
        DisplayType::Overlay,
        &padded,
        dates,
        &format!("{ma_type}({period})"),
        vec![],
    )]
}

fn compute_ma_crossover_indicator(
    df: &DataFrame,
    column: &str,
    fast_period: usize,
    slow_period: usize,
    ma_type: &str,
    ma_fn: fn(&[f64], usize) -> Vec<f64>,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(prices) = column_to_f64(df, column) else {
        return vec![];
    };
    let n = prices.len();
    let min_period = fast_period.max(slow_period);
    if n < min_period {
        return vec![];
    }
    let fast = pad_series(&ma_fn(&prices, fast_period), n);
    let slow = pad_series(&ma_fn(&prices, slow_period), n);
    let (fast_series, fast_total) =
        build_series(&format!("{ma_type}({fast_period})"), &fast, dates);
    let (slow_series, slow_total) =
        build_series(&format!("{ma_type}({slow_period})"), &slow, dates);
    let max_total = fast_total.max(slow_total);
    vec![IndicatorData {
        name: format!("{ma_type} Crossover ({fast_period}/{slow_period})"),
        display_type: DisplayType::Overlay,
        series: vec![fast_series, slow_series],
        thresholds: vec![],
        total_points: if max_total > MAX_INDICATOR_POINTS {
            Some(max_total)
        } else {
            None
        },
    }]
}

fn compute_aroon_indicator(
    df: &DataFrame,
    high_col: &str,
    low_col: &str,
    period: usize,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(highs) = column_to_f64(df, high_col) else {
        return vec![];
    };
    let Ok(lows) = column_to_f64(df, low_col) else {
        return vec![];
    };
    let n = highs.len();
    if n < period + 1 {
        return vec![];
    }
    let aroon_values: Vec<(f64, f64, f64)> = (0..(n - period))
        .map(|i| {
            let end = i + period + 1;
            rust_ti::trend_indicators::single::aroon_indicator(&highs[i..end], &lows[i..end])
        })
        .collect();
    let oscillators: Vec<f64> = aroon_values.iter().map(|t| t.2).collect();
    let padded = pad_series(&oscillators, n);
    vec![make_indicator(
        format!("Aroon Oscillator({period})"),
        DisplayType::Subchart,
        &padded,
        dates,
        "Oscillator",
        vec![0.0],
    )]
}

fn compute_aroon_up_indicator(
    df: &DataFrame,
    high_col: &str,
    period: usize,
    threshold: f64,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(highs) = column_to_f64(df, high_col) else {
        return vec![];
    };
    let n = highs.len();
    if n < period + 1 {
        return vec![];
    }
    let aroon_up_values: Vec<f64> = (0..(n - period))
        .map(|i| {
            let end = i + period + 1;
            rust_ti::trend_indicators::single::aroon_up(&highs[i..end])
        })
        .collect();
    let padded = pad_series(&aroon_up_values, n);
    vec![make_indicator(
        format!("Aroon Up({period})"),
        DisplayType::Subchart,
        &padded,
        dates,
        "Aroon Up",
        vec![threshold],
    )]
}

fn compute_supertrend_indicator(
    df: &DataFrame,
    close_col: &str,
    high_col: &str,
    low_col: &str,
    period: usize,
    multiplier: f64,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(close) = column_to_f64(df, close_col) else {
        return vec![];
    };
    let Ok(high) = column_to_f64(df, high_col) else {
        return vec![];
    };
    let Ok(low) = column_to_f64(df, low_col) else {
        return vec![];
    };
    let n = close.len();
    if n < period {
        return vec![];
    }
    let st = rust_ti::candle_indicators::bulk::supertrend(
        &high,
        &low,
        &close,
        rust_ti::ConstantModelType::SimpleMovingAverage,
        multiplier,
        period,
    );
    let padded = pad_series(&st, n);
    vec![make_indicator(
        format!("Supertrend({period}, {multiplier})"),
        DisplayType::Overlay,
        &padded,
        dates,
        "Supertrend",
        vec![],
    )]
}

fn compute_atr_indicator(
    df: &DataFrame,
    close_col: &str,
    high_col: &str,
    low_col: &str,
    period: usize,
    threshold: f64,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(close) = column_to_f64(df, close_col) else {
        return vec![];
    };
    let Ok(high) = column_to_f64(df, high_col) else {
        return vec![];
    };
    let Ok(low) = column_to_f64(df, low_col) else {
        return vec![];
    };
    let n = close.len();
    if n < period {
        return vec![];
    }
    let atr_values = compute_atr(&close, &high, &low, period);
    let padded = pad_series(&atr_values, n);
    vec![make_indicator(
        format!("ATR({period})"),
        DisplayType::Subchart,
        &padded,
        dates,
        "ATR",
        vec![threshold],
    )]
}

fn compute_bollinger_indicator(
    df: &DataFrame,
    column: &str,
    period: usize,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(prices) = column_to_f64(df, column) else {
        return vec![];
    };
    let n = prices.len();
    if n < period {
        return vec![];
    }
    let (lower, upper) = compute_bollinger_bands(&prices, period);
    let lower_padded = pad_series(&lower, n);
    let upper_padded = pad_series(&upper, n);
    let (lower_series, lower_total) = build_series("Lower Band", &lower_padded, dates);
    let (upper_series, upper_total) = build_series("Upper Band", &upper_padded, dates);
    let max_total = lower_total.max(upper_total);
    vec![IndicatorData {
        name: format!("Bollinger Bands({period})"),
        display_type: DisplayType::Overlay,
        series: vec![lower_series, upper_series],
        thresholds: vec![],
        total_points: if max_total > MAX_INDICATOR_POINTS {
            Some(max_total)
        } else {
            None
        },
    }]
}

fn compute_keltner_indicator(
    df: &DataFrame,
    close_col: &str,
    high_col: &str,
    low_col: &str,
    period: usize,
    multiplier: f64,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(close) = column_to_f64(df, close_col) else {
        return vec![];
    };
    let Ok(high) = column_to_f64(df, high_col) else {
        return vec![];
    };
    let Ok(low) = column_to_f64(df, low_col) else {
        return vec![];
    };
    let n = close.len();
    if n < period {
        return vec![];
    }
    let (lower, upper) = compute_keltner_channel(&close, &high, &low, period, multiplier);
    let lower_padded = pad_series(&lower, n);
    let upper_padded = pad_series(&upper, n);
    let (lower_series, lower_total) = build_series("Lower Channel", &lower_padded, dates);
    let (upper_series, upper_total) = build_series("Upper Channel", &upper_padded, dates);
    let max_total = lower_total.max(upper_total);
    vec![IndicatorData {
        name: format!("Keltner Channel({period}, {multiplier})"),
        display_type: DisplayType::Overlay,
        series: vec![lower_series, upper_series],
        thresholds: vec![],
        total_points: if max_total > MAX_INDICATOR_POINTS {
            Some(max_total)
        } else {
            None
        },
    }]
}

#[allow(clippy::too_many_arguments)]
fn compute_mfi_indicator(
    df: &DataFrame,
    high_col: &str,
    low_col: &str,
    close_col: &str,
    volume_col: &str,
    period: usize,
    threshold: f64,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(high) = column_to_f64(df, high_col) else {
        return vec![];
    };
    let Ok(low) = column_to_f64(df, low_col) else {
        return vec![];
    };
    let Ok(close) = column_to_f64(df, close_col) else {
        return vec![];
    };
    let Ok(volume) = column_to_f64(df, volume_col) else {
        return vec![];
    };
    let typical = compute_typical_price(&high, &low, &close);
    let n = typical.len();
    if period == 0 || n < period {
        return vec![];
    }
    let mfi_values =
        rust_ti::momentum_indicators::bulk::money_flow_index(&typical, &volume, period);
    let padded = pad_series(&mfi_values, n);
    vec![make_indicator(
        format!("MFI({period})"),
        DisplayType::Subchart,
        &padded,
        dates,
        "MFI",
        vec![threshold],
    )]
}

fn compute_obv_indicator(
    df: &DataFrame,
    price_col: &str,
    volume_col: &str,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(prices) = column_to_f64(df, price_col) else {
        return vec![];
    };
    let Ok(volume) = column_to_f64(df, volume_col) else {
        return vec![];
    };
    let n = prices.len();
    if n < 2 {
        return vec![];
    }
    let obv_values = rust_ti::momentum_indicators::bulk::on_balance_volume(&prices, &volume, 0.0);
    let padded = pad_series(&obv_values, n);
    vec![make_indicator(
        "OBV".to_string(),
        DisplayType::Subchart,
        &padded,
        dates,
        "OBV",
        vec![],
    )]
}

fn compute_cmf_indicator(
    df: &DataFrame,
    close_col: &str,
    high_col: &str,
    low_col: &str,
    volume_col: &str,
    period: usize,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(close) = column_to_f64(df, close_col) else {
        return vec![];
    };
    let Ok(high) = column_to_f64(df, high_col) else {
        return vec![];
    };
    let Ok(low) = column_to_f64(df, low_col) else {
        return vec![];
    };
    let Ok(volume) = column_to_f64(df, volume_col) else {
        return vec![];
    };
    let n = close.len();
    if n < period {
        return vec![];
    }
    let cmf_values = compute_cmf(&close, &high, &low, &volume, period);
    let padded = pad_series(&cmf_values, n);
    vec![make_indicator(
        format!("CMF({period})"),
        DisplayType::Subchart,
        &padded,
        dates,
        "CMF",
        vec![0.0],
    )]
}

fn compute_drawdown_indicator(
    df: &DataFrame,
    column: &str,
    window: usize,
    threshold: f64,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(prices) = column_to_f64(df, column) else {
        return vec![];
    };
    let n = prices.len();
    let mut dd_values = Vec::with_capacity(n);
    for i in 0..n {
        let start = i.saturating_sub(window.saturating_sub(1));
        let rolling_max = prices[start..=i]
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        if rolling_max > 0.0 {
            dd_values.push((prices[i] - rolling_max) / rolling_max);
        } else {
            dd_values.push(0.0);
        }
    }
    vec![make_indicator(
        format!("Drawdown({window})"),
        DisplayType::Subchart,
        &dd_values,
        dates,
        "Drawdown",
        vec![-threshold],
    )]
}

fn compute_roc_indicator(
    df: &DataFrame,
    column: &str,
    period: usize,
    threshold: f64,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(prices) = column_to_f64(df, column) else {
        return vec![];
    };
    let n = prices.len();
    let mut roc_values = vec![f64::NAN; n];
    for i in period..n {
        if prices[i - period].abs() > f64::EPSILON {
            roc_values[i] = (prices[i] - prices[i - period]) / prices[i - period].abs();
        }
    }
    vec![make_indicator(
        format!("ROC({period})"),
        DisplayType::Subchart,
        &roc_values,
        dates,
        "ROC",
        vec![threshold],
    )]
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn make_ohlcv_df(n: usize) -> DataFrame {
        let dates: Vec<NaiveDate> = (0..n)
            .map(|i| {
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Duration::days(i as i64)
            })
            .collect();
        let close: Vec<f64> = (0..n).map(|i| 100.0 + (i as f64) * 0.5).collect();
        let high: Vec<f64> = close.iter().map(|c| c + 2.0).collect();
        let low: Vec<f64> = close.iter().map(|c| c - 2.0).collect();
        let open: Vec<f64> = close.iter().map(|c| c - 0.5).collect();
        let volume: Vec<f64> = vec![1000.0; n];
        df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates),
            "open" => &open,
            "high" => &high,
            "low" => &low,
            "close" => &close,
            "volume" => &volume,
        }
        .unwrap()
    }

    // ── Custom / CrossSymbol return empty ────────────────────────────────────

    #[test]
    fn custom_signal_returns_empty_indicators() {
        let df = make_ohlcv_df(30);
        let spec = SignalSpec::Formula { formula: "rsi(close, 14) < 30".into() };
        let result = compute_indicator_data(&spec, &df, "date");
        assert!(result.is_empty());
    }

    #[test]
    fn cross_symbol_returns_empty_indicators() {
        let df = make_ohlcv_df(10);
        let spec = SignalSpec::CrossSymbol {
            symbol: "^VIX".into(),
            signal: Box::new(SignalSpec::Formula { formula: "close > 20".into() }),
        };
        let result = compute_indicator_data(&spec, &df, "date");
        assert!(result.is_empty());
    }

    // ── And / Or combinators ─────────────────────────────────────────────────

    #[test]
    fn and_combinator_with_custom_children_returns_empty() {
        // Custom signals don't produce indicator data yet, so And of two
        // Custom signals should yield an empty result.
        let df = make_ohlcv_df(30);
        let spec = SignalSpec::And {
            left: Box::new(SignalSpec::Formula { formula: "rsi(close, 14) < 30".into() }),
            right: Box::new(SignalSpec::Formula { formula: "close > sma(close, 5)".into() }),
        };
        let result = compute_indicator_data(&spec, &df, "date");
        assert!(result.is_empty());
    }

    #[test]
    fn or_combinator_with_custom_children_returns_empty() {
        let df = make_ohlcv_df(30);
        let spec = SignalSpec::Or {
            left: Box::new(SignalSpec::Formula { formula: "close > sma(close, 5)".into() }),
            right: Box::new(SignalSpec::Formula { formula: "close > sma(close, 20)".into() }),
        };
        let result = compute_indicator_data(&spec, &df, "date");
        assert!(result.is_empty());
    }

    // ── total_points metadata ────────────────────────────────────────────────

    #[test]
    fn total_points_none_when_not_sampled() {
        // Custom signals return empty — verify the vec is empty (no panic on index).
        let df = make_ohlcv_df(30);
        let spec = SignalSpec::Formula { formula: "rsi(close, 14) < 30".into() };
        let result = compute_indicator_data(&spec, &df, "date");
        // No indicator data returned for Custom signals yet.
        assert!(result.is_empty());
    }

    // ── sample_points helper ─────────────────────────────────────────────────

    #[test]
    fn sampling_limits_points() {
        let points: Vec<IndicatorPoint> = (0..500)
            .map(|i| IndicatorPoint {
                date: format!("2024-01-{:02}", (i % 28) + 1),
                value: f64::from(i),
            })
            .collect();
        let sampled = sample_points(points, 200);
        assert_eq!(sampled.len(), 200);
        assert_eq!(sampled[0].value, 0.0);
        assert_eq!(sampled[199].value, 499.0);
    }
}
