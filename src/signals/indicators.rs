//! Compute raw indicator values for charting from a `SignalSpec`.
//!
//! Pattern-matches on the signal variant to compute the underlying indicator
//! (RSI line, SMA curve, Bollinger bands, etc.) and returns structured data
//! ready for visualization alongside price charts.
//!
//! For `Formula` signals, uses `extract_indicator_calls()` to scan the formula
//! for recognized indicator functions and computes their chart overlay data.

use super::custom::extract_indicator_calls;
use super::helpers::{
    column_to_f64, pad_series, DisplayType, IndicatorData, IndicatorPoint, IndicatorSeries,
};
use super::momentum::compute_stochastic;
use super::spec::SignalSpec;
use super::volatility::{compute_atr, compute_bollinger_bands, compute_keltner_channel};
use super::volume::{compute_cmf, compute_typical_price};

use crate::engine::price_table::{extract_date_from_column, extract_datetime_from_column};
use polars::prelude::*;
use rust_ti::candle_indicators::bulk as cti;
use rust_ti::momentum_indicators::bulk as mti;
use rust_ti::standard_indicators::bulk as sti;
use rust_ti::strength_indicators::bulk as sti_strength;
use rust_ti::trend_indicators::bulk as tti;
use rust_ti::volatility_indicators::bulk as vti;

/// Maximum number of indicator points to return per series.
/// The frontend charts handle large datasets efficiently, so we use a generous
/// limit to preserve full resolution (1 point per bar).
const MAX_INDICATOR_POINTS: usize = 5000;

/// Compute SMA using `rust_ti`.
fn compute_sma(prices: &[f64], period: usize) -> Vec<f64> {
    if prices.len() < period || period == 0 {
        return vec![];
    }
    sti::simple_moving_average(prices, period)
}

/// Compute EMA using `rust_ti`.
fn compute_ema(prices: &[f64], period: usize) -> Vec<f64> {
    if prices.len() < period || period == 0 {
        return vec![];
    }
    sti::exponential_moving_average(prices, period)
}

/// Compute raw indicator data for charting from a signal specification.
///
/// Returns one or more `IndicatorData` entries depending on the signal type.
/// For `Formula` signals, extracts recognized indicator function calls from the
/// formula string and computes their chart overlay data.
/// For combinators (And/Or), recursively collects indicators from both children.
pub fn compute_indicator_data(
    spec: &SignalSpec,
    ohlcv_df: &DataFrame,
    date_col: &str,
) -> Vec<IndicatorData> {
    compute_indicator_data_inner(spec, ohlcv_df, date_col)
}

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
        SignalSpec::Formula { formula } => {
            extract_indicators_from_formula(formula, ohlcv_df, date_col)
        }
        SignalSpec::CrossSymbol { .. } => vec![],
    }
}

/// Extract indicator chart data from a formula string by scanning for recognized
/// indicator function calls and computing their series against the `DataFrame`.
fn extract_indicators_from_formula(
    formula: &str,
    df: &DataFrame,
    date_col: &str,
) -> Vec<IndicatorData> {
    let calls = extract_indicator_calls(formula);
    if calls.is_empty() {
        return vec![];
    }

    let Ok(dates) = extract_date_strings(df, date_col) else {
        return vec![];
    };

    let mut results = Vec::new();
    for call in &calls {
        let indicators = dispatch_indicator_call(call, df, &dates);
        for ind in indicators {
            if !results
                .iter()
                .any(|existing: &IndicatorData| existing.name == ind.name)
            {
                results.push(ind);
            }
        }
    }

    results
}

/// Dispatch a single `IndicatorCall` to the appropriate compute function.
#[allow(clippy::too_many_lines)]
fn dispatch_indicator_call(
    call: &super::custom::IndicatorCall,
    df: &DataFrame,
    dates: &[String],
) -> Vec<IndicatorData> {
    let col = call.col_args.first().map_or("close", String::as_str);
    match call.func_name.as_str() {
        "rsi" => {
            let period = call.period.unwrap_or(14);
            compute_rsi_indicator(df, col, period, dates)
        }
        "macd_hist" | "macd_signal" | "macd_line" => compute_macd_indicator(df, col, dates),
        "stochastic" => {
            let high = call.col_args.get(1).map_or("high", String::as_str);
            let low = call.col_args.get(2).map_or("low", String::as_str);
            let period = call.period.unwrap_or(14);
            compute_stochastic_indicator(df, col, high, low, period, dates)
        }
        "sma" => {
            let period = call.period.unwrap_or(20);
            compute_ma_indicator(df, col, period, "SMA", compute_sma, dates)
        }
        "ema" => {
            let period = call.period.unwrap_or(20);
            compute_ma_indicator(df, col, period, "EMA", compute_ema, dates)
        }
        "bbands_upper" | "bbands_lower" | "bbands_mid" => {
            let period = call.period.unwrap_or(20);
            compute_bollinger_indicator(df, col, period, dates)
        }
        "keltner_upper" | "keltner_lower" => {
            let high = call.col_args.get(1).map_or("high", String::as_str);
            let low = call.col_args.get(2).map_or("low", String::as_str);
            let period = call.period.unwrap_or(20);
            let mult = call.multiplier.unwrap_or(2.0);
            compute_keltner_indicator(df, col, high, low, period, mult, dates)
        }
        "atr" => {
            let high = call.col_args.get(1).map_or("high", String::as_str);
            let low = call.col_args.get(2).map_or("low", String::as_str);
            let period = call.period.unwrap_or(14);
            compute_atr_indicator(df, col, high, low, period, dates)
        }
        "aroon_up" => {
            let period = call.period.unwrap_or(25);
            compute_aroon_up_indicator(df, col, period, dates)
        }
        "aroon_down" | "aroon_osc" => {
            let low = call.col_args.get(1).map_or("low", String::as_str);
            let period = call.period.unwrap_or(25);
            compute_aroon_indicator(df, col, low, period, dates)
        }
        "supertrend" => {
            let high = call.col_args.get(1).map_or("high", String::as_str);
            let low = call.col_args.get(2).map_or("low", String::as_str);
            let period = call.period.unwrap_or(10);
            let mult = call.multiplier.unwrap_or(3.0);
            compute_supertrend_indicator(df, col, high, low, period, mult, dates)
        }
        "mfi" => {
            let high = call.col_args.get(1).map_or("high", String::as_str);
            let low = call.col_args.get(2).map_or("low", String::as_str);
            let vol = call.col_args.get(3).map_or("volume", String::as_str);
            let period = call.period.unwrap_or(14);
            compute_mfi_indicator(df, high, low, col, vol, period, dates)
        }
        "obv" => {
            let vol = call.col_args.get(1).map_or("volume", String::as_str);
            compute_obv_indicator(df, col, vol, dates)
        }
        "cmf" => {
            let high = call.col_args.get(1).map_or("high", String::as_str);
            let low = call.col_args.get(2).map_or("low", String::as_str);
            let vol = call.col_args.get(3).map_or("volume", String::as_str);
            let period = call.period.unwrap_or(20);
            compute_cmf_indicator(df, col, high, low, vol, period, dates)
        }
        "roc" => {
            let period = call.period.unwrap_or(10);
            compute_roc_indicator(df, col, period, dates)
        }
        "williams_r" => {
            let high = call.col_args.first().map_or("high", String::as_str);
            let low = call.col_args.get(1).map_or("low", String::as_str);
            let close = call.col_args.get(2).map_or("close", String::as_str);
            let period = call.period.unwrap_or(14);
            compute_williams_r_indicator(df, high, low, close, period, dates)
        }
        "cci" => {
            let period = call.period.unwrap_or(20);
            compute_cci_indicator(df, col, period, dates)
        }
        "ppo" => {
            let short = call.period.unwrap_or(12);
            let long = call.multiplier.map_or(26, |v| v as usize);
            compute_ppo_indicator(df, col, short, long, dates)
        }
        "cmo" => {
            let period = call.period.unwrap_or(14);
            compute_cmo_indicator(df, col, period, dates)
        }
        "adx" | "plus_di" | "minus_di" => {
            let high = call.col_args.first().map_or("high", String::as_str);
            let low = call.col_args.get(1).map_or("low", String::as_str);
            let close = call.col_args.get(2).map_or("close", String::as_str);
            let period = call.period.unwrap_or(14);
            compute_dms_indicator(df, high, low, close, period, call.func_name.as_str(), dates)
        }
        "psar" => {
            let high = call.col_args.first().map_or("high", String::as_str);
            let low = call.col_args.get(1).map_or("low", String::as_str);
            let accel = call.period.map_or(0.02, |v| v as f64 / 100.0);
            let max_accel = call.multiplier.unwrap_or(0.2);
            compute_psar_indicator(df, high, low, accel, max_accel, dates)
        }
        "tsi" => {
            let fast = call.period.unwrap_or(13);
            let slow = call.multiplier.map_or(25, |v| v as usize);
            compute_tsi_indicator(df, col, fast, slow, dates)
        }
        "vpt" => {
            let vol = call.col_args.get(1).map_or("volume", String::as_str);
            compute_vpt_indicator(df, col, vol, dates)
        }
        "donchian_upper" | "donchian_mid" | "donchian_lower" => {
            let high = call.col_args.first().map_or("high", String::as_str);
            let low = call.col_args.get(1).map_or("low", String::as_str);
            let period = call.period.unwrap_or(20);
            compute_donchian_indicator(df, high, low, period, dates)
        }
        "ichimoku_tenkan" | "ichimoku_kijun" | "ichimoku_senkou_a" | "ichimoku_senkou_b" => {
            let high = call.col_args.first().map_or("high", String::as_str);
            let low = call.col_args.get(1).map_or("low", String::as_str);
            let close = call.col_args.get(2).map_or("close", String::as_str);
            compute_ichimoku_indicator(df, high, low, close, dates)
        }
        "envelope_upper" | "envelope_lower" => {
            let period = call.period.unwrap_or(20);
            let pct = call.multiplier.unwrap_or(2.5);
            compute_envelope_indicator(df, col, period, pct, dates)
        }
        "ad" => {
            let high = call.col_args.first().map_or("high", String::as_str);
            let low = call.col_args.get(1).map_or("low", String::as_str);
            let close = call.col_args.get(2).map_or("close", String::as_str);
            let vol = call.col_args.get(3).map_or("volume", String::as_str);
            compute_ad_indicator(df, high, low, close, vol, dates)
        }
        "pvi" | "nvi" => {
            let vol = call.col_args.get(1).map_or("volume", String::as_str);
            compute_pvi_nvi_indicator(df, col, vol, &call.func_name, dates)
        }
        "ulcer" => {
            let period = call.period.unwrap_or(14);
            compute_ulcer_indicator(df, col, period, dates)
        }
        _ => vec![],
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
    let is_datetime = date_col == "datetime";
    let mut dates = Vec::with_capacity(n);
    for i in 0..n {
        if is_datetime {
            match extract_datetime_from_column(col, i) {
                Ok(dt) => {
                    if dt.time() == chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap() {
                        dates.push(dt.format("%Y-%m-%d").to_string());
                    } else {
                        dates.push(dt.format("%Y-%m-%dT%H:%M:%S").to_string());
                    }
                }
                Err(_) => dates.push(String::new()),
            }
        } else {
            match extract_date_from_column(col, i) {
                Ok(d) => dates.push(d.format("%Y-%m-%d").to_string()),
                Err(_) => dates.push(String::new()),
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
    period: usize,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(prices) = column_to_f64(df, column) else {
        return vec![];
    };
    let n = prices.len();
    if n <= period {
        return vec![];
    }
    let rsi_values = mti::relative_strength_index(
        &prices,
        rust_ti::ConstantModelType::SmoothedMovingAverage,
        period,
    );
    let padded = pad_series(&rsi_values, n);
    vec![make_indicator(
        format!("RSI({period})"),
        DisplayType::Subchart,
        &padded,
        dates,
        "RSI",
        vec![30.0, 70.0],
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
        vec![20.0, 80.0],
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
        vec![70.0],
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
        vec![],
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
        vec![20.0, 80.0],
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

fn compute_williams_r_indicator(
    df: &DataFrame,
    high_col: &str,
    low_col: &str,
    close_col: &str,
    period: usize,
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
    let n = close.len();
    if n < period {
        return vec![];
    }
    let vals = mti::williams_percent_r(&high, &low, &close, period);
    let padded = pad_series(&vals, n);
    vec![make_indicator(
        format!("Williams %R({period})"),
        DisplayType::Subchart,
        &padded,
        dates,
        "%R",
        vec![-20.0, -80.0],
    )]
}

fn compute_cci_indicator(
    df: &DataFrame,
    column: &str,
    period: usize,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(prices) = column_to_f64(df, column) else {
        return vec![];
    };
    let n = prices.len();
    if n <= period {
        return vec![];
    }
    let vals = mti::commodity_channel_index(
        &prices,
        rust_ti::ConstantModelType::SimpleMovingAverage,
        rust_ti::DeviationModel::MeanAbsoluteDeviation,
        0.015,
        period,
    );
    let padded = pad_series(&vals, n);
    vec![make_indicator(
        format!("CCI({period})"),
        DisplayType::Subchart,
        &padded,
        dates,
        "CCI",
        vec![-100.0, 100.0],
    )]
}

fn compute_ppo_indicator(
    df: &DataFrame,
    column: &str,
    short_period: usize,
    long_period: usize,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(prices) = column_to_f64(df, column) else {
        return vec![];
    };
    let n = prices.len();
    if n <= long_period {
        return vec![];
    }
    let vals = mti::percentage_price_oscillator(
        &prices,
        short_period,
        long_period,
        rust_ti::ConstantModelType::ExponentialMovingAverage,
    );
    let padded = pad_series(&vals, n);
    vec![make_indicator(
        format!("PPO({short_period},{long_period})"),
        DisplayType::Subchart,
        &padded,
        dates,
        "PPO",
        vec![0.0],
    )]
}

fn compute_cmo_indicator(
    df: &DataFrame,
    column: &str,
    period: usize,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(prices) = column_to_f64(df, column) else {
        return vec![];
    };
    let n = prices.len();
    if n <= period {
        return vec![];
    }
    let vals = mti::chande_momentum_oscillator(&prices, period);
    let padded = pad_series(&vals, n);
    vec![make_indicator(
        format!("CMO({period})"),
        DisplayType::Subchart,
        &padded,
        dates,
        "CMO",
        vec![-50.0, 50.0],
    )]
}

#[allow(clippy::too_many_arguments)]
fn compute_dms_indicator(
    df: &DataFrame,
    high_col: &str,
    low_col: &str,
    close_col: &str,
    period: usize,
    component: &str,
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
    let n = close.len();
    if n < period + 1 {
        return vec![];
    }
    let dms = tti::directional_movement_system(
        &high,
        &low,
        &close,
        period,
        rust_ti::ConstantModelType::SmoothedMovingAverage,
    );
    let (vals, label): (Vec<f64>, &str) = match component {
        "plus_di" => (dms.iter().map(|t| t.0).collect(), "+DI"),
        "minus_di" => (dms.iter().map(|t| t.1).collect(), "-DI"),
        _ => (dms.iter().map(|t| t.2).collect(), "ADX"),
    };
    let padded = pad_series(&vals, n);
    vec![make_indicator(
        format!("{label}({period})"),
        DisplayType::Subchart,
        &padded,
        dates,
        label,
        vec![20.0, 40.0],
    )]
}

fn compute_psar_indicator(
    df: &DataFrame,
    high_col: &str,
    low_col: &str,
    accel: f64,
    max_accel: f64,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(high) = column_to_f64(df, high_col) else {
        return vec![];
    };
    let Ok(low) = column_to_f64(df, low_col) else {
        return vec![];
    };
    let n = high.len();
    if n < 2 {
        return vec![];
    }
    let vals = tti::parabolic_time_price_system(
        &high,
        &low,
        accel,
        max_accel,
        accel,
        rust_ti::Position::Long,
        low[0],
    );
    let padded = pad_series(&vals, n);
    vec![make_indicator(
        format!("Parabolic SAR({accel},{max_accel})"),
        DisplayType::Overlay,
        &padded,
        dates,
        "SAR",
        vec![],
    )]
}

fn compute_tsi_indicator(
    df: &DataFrame,
    column: &str,
    fast: usize,
    slow: usize,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(prices) = column_to_f64(df, column) else {
        return vec![];
    };
    let n = prices.len();
    if n <= slow {
        return vec![];
    }
    let vals = tti::true_strength_index(
        &prices,
        rust_ti::ConstantModelType::ExponentialMovingAverage,
        fast,
        rust_ti::ConstantModelType::ExponentialMovingAverage,
        slow,
    );
    let padded = pad_series(&vals, n);
    vec![make_indicator(
        format!("TSI({fast},{slow})"),
        DisplayType::Subchart,
        &padded,
        dates,
        "TSI",
        vec![0.0],
    )]
}

fn compute_vpt_indicator(
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
    let vals = tti::volume_price_trend(&prices, &volume[1..], 0.0);
    let padded = pad_series(&vals, n);
    vec![make_indicator(
        "VPT".to_string(),
        DisplayType::Subchart,
        &padded,
        dates,
        "VPT",
        vec![],
    )]
}

fn compute_donchian_indicator(
    df: &DataFrame,
    high_col: &str,
    low_col: &str,
    period: usize,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(high) = column_to_f64(df, high_col) else {
        return vec![];
    };
    let Ok(low) = column_to_f64(df, low_col) else {
        return vec![];
    };
    let n = high.len();
    if n < period {
        return vec![];
    }
    let dc = cti::donchian_channels(&high, &low, period);
    let upper_vals: Vec<f64> = dc.iter().map(|t| t.0).collect();
    let mid_vals: Vec<f64> = dc.iter().map(|t| t.1).collect();
    let lower_vals: Vec<f64> = dc.iter().map(|t| t.2).collect();
    let upper_padded = pad_series(&upper_vals, n);
    let mid_padded = pad_series(&mid_vals, n);
    let lower_padded = pad_series(&lower_vals, n);
    let (upper_series, upper_total) = build_series("Upper", &upper_padded, dates);
    let (mid_series, mid_total) = build_series("Mid", &mid_padded, dates);
    let (lower_series, lower_total) = build_series("Lower", &lower_padded, dates);
    let max_total = upper_total.max(mid_total).max(lower_total);
    vec![IndicatorData {
        name: format!("Donchian({period})"),
        display_type: DisplayType::Overlay,
        series: vec![upper_series, mid_series, lower_series],
        thresholds: vec![],
        total_points: if max_total > MAX_INDICATOR_POINTS {
            Some(max_total)
        } else {
            None
        },
    }]
}

fn compute_ichimoku_indicator(
    df: &DataFrame,
    high_col: &str,
    low_col: &str,
    close_col: &str,
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
    let n = close.len();
    if n < 52 {
        return vec![];
    }
    let ich = cti::ichimoku_cloud(&high, &low, &close, 9, 26, 52);
    let tenkan: Vec<f64> = ich.iter().map(|t| t.0).collect();
    let kijun: Vec<f64> = ich.iter().map(|t| t.1).collect();
    let senkou_a: Vec<f64> = ich.iter().map(|t| t.2).collect();
    let senkou_b: Vec<f64> = ich.iter().map(|t| t.3).collect();
    let t_padded = pad_series(&tenkan, n);
    let k_padded = pad_series(&kijun, n);
    let a_padded = pad_series(&senkou_a, n);
    let b_padded = pad_series(&senkou_b, n);
    let (t_series, t_total) = build_series("Tenkan", &t_padded, dates);
    let (k_series, k_total) = build_series("Kijun", &k_padded, dates);
    let (a_series, a_total) = build_series("Senkou A", &a_padded, dates);
    let (b_series, b_total) = build_series("Senkou B", &b_padded, dates);
    let max_total = t_total.max(k_total).max(a_total).max(b_total);
    vec![IndicatorData {
        name: "Ichimoku Cloud".to_string(),
        display_type: DisplayType::Overlay,
        series: vec![t_series, k_series, a_series, b_series],
        thresholds: vec![],
        total_points: if max_total > MAX_INDICATOR_POINTS {
            Some(max_total)
        } else {
            None
        },
    }]
}

fn compute_envelope_indicator(
    df: &DataFrame,
    column: &str,
    period: usize,
    pct: f64,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(prices) = column_to_f64(df, column) else {
        return vec![];
    };
    let n = prices.len();
    if n < period {
        return vec![];
    }
    let env = cti::moving_constant_envelopes(
        &prices,
        rust_ti::ConstantModelType::SimpleMovingAverage,
        pct,
        period,
    );
    let upper_vals: Vec<f64> = env.iter().map(|t| t.2).collect();
    let lower_vals: Vec<f64> = env.iter().map(|t| t.0).collect();
    let upper_padded = pad_series(&upper_vals, n);
    let lower_padded = pad_series(&lower_vals, n);
    let (upper_series, upper_total) = build_series("Upper", &upper_padded, dates);
    let (lower_series, lower_total) = build_series("Lower", &lower_padded, dates);
    let max_total = upper_total.max(lower_total);
    vec![IndicatorData {
        name: format!("Envelope({period},{pct}%)"),
        display_type: DisplayType::Overlay,
        series: vec![upper_series, lower_series],
        thresholds: vec![],
        total_points: if max_total > MAX_INDICATOR_POINTS {
            Some(max_total)
        } else {
            None
        },
    }]
}

#[allow(clippy::too_many_arguments)]
fn compute_ad_indicator(
    df: &DataFrame,
    high_col: &str,
    low_col: &str,
    close_col: &str,
    volume_col: &str,
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
    let n = close.len();
    if n < 2 {
        return vec![];
    }
    let vals = sti_strength::accumulation_distribution(&high, &low, &close, &volume, 0.0);
    let padded = pad_series(&vals, n);
    vec![make_indicator(
        "A/D Line".to_string(),
        DisplayType::Subchart,
        &padded,
        dates,
        "A/D",
        vec![],
    )]
}

fn compute_pvi_nvi_indicator(
    df: &DataFrame,
    price_col: &str,
    volume_col: &str,
    func_name: &str,
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
    let (name, vals) = if func_name == "pvi" {
        (
            "PVI",
            sti_strength::positive_volume_index(&prices, &volume, 1000.0),
        )
    } else {
        (
            "NVI",
            sti_strength::negative_volume_index(&prices, &volume, 1000.0),
        )
    };
    let padded = pad_series(&vals, n);
    vec![make_indicator(
        name.to_string(),
        DisplayType::Subchart,
        &padded,
        dates,
        name,
        vec![],
    )]
}

fn compute_ulcer_indicator(
    df: &DataFrame,
    column: &str,
    period: usize,
    dates: &[String],
) -> Vec<IndicatorData> {
    let Ok(prices) = column_to_f64(df, column) else {
        return vec![];
    };
    let n = prices.len();
    if n <= period {
        return vec![];
    }
    let vals = vti::ulcer_index(&prices, period);
    let padded = pad_series(&vals, n);
    vec![make_indicator(
        format!("Ulcer Index({period})"),
        DisplayType::Subchart,
        &padded,
        dates,
        "Ulcer",
        vec![],
    )]
}

fn compute_roc_indicator(
    df: &DataFrame,
    column: &str,
    period: usize,
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
        vec![0.0],
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

    // ── Formula indicator extraction ────────────────────────────────────────

    #[test]
    fn formula_rsi_extracts_indicator() {
        let df = make_ohlcv_df(30);
        let spec = SignalSpec::Formula {
            formula: "rsi(close, 14) < 30".into(),
        };
        let result = compute_indicator_data(&spec, &df, "date");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "RSI(14)");
        assert_eq!(result[0].display_type, DisplayType::Subchart);
        assert_eq!(result[0].thresholds, vec![30.0, 70.0]);
    }

    #[test]
    fn formula_sma_extracts_overlay() {
        let df = make_ohlcv_df(30);
        let spec = SignalSpec::Formula {
            formula: "close > sma(close, 5)".into(),
        };
        let result = compute_indicator_data(&spec, &df, "date");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "SMA(5)");
        assert_eq!(result[0].display_type, DisplayType::Overlay);
    }

    #[test]
    fn formula_multiple_indicators_extracted() {
        let df = make_ohlcv_df(30);
        let spec = SignalSpec::Formula {
            formula: "rsi(close, 14) < 30 and close > sma(close, 5)".into(),
        };
        let result = compute_indicator_data(&spec, &df, "date");
        assert_eq!(result.len(), 2);
        let names: Vec<&str> = result.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"RSI(14)"));
        assert!(names.contains(&"SMA(5)"));
    }

    #[test]
    fn formula_no_indicator_functions_returns_empty() {
        let df = make_ohlcv_df(30);
        let spec = SignalSpec::Formula {
            formula: "close > 100".into(),
        };
        let result = compute_indicator_data(&spec, &df, "date");
        assert!(result.is_empty());
    }

    #[test]
    fn cross_symbol_returns_empty_indicators() {
        let df = make_ohlcv_df(10);
        let spec = SignalSpec::CrossSymbol {
            symbol: "^VIX".into(),
            signal: Box::new(SignalSpec::Formula {
                formula: "close > 20".into(),
            }),
        };
        let result = compute_indicator_data(&spec, &df, "date");
        assert!(result.is_empty());
    }

    // ── And / Or combinators ─────────────────────────────────────────────────

    #[test]
    fn and_combinator_extracts_from_both_children() {
        let df = make_ohlcv_df(30);
        let spec = SignalSpec::And {
            left: Box::new(SignalSpec::Formula {
                formula: "rsi(close, 14) < 30".into(),
            }),
            right: Box::new(SignalSpec::Formula {
                formula: "close > sma(close, 5)".into(),
            }),
        };
        let result = compute_indicator_data(&spec, &df, "date");
        assert_eq!(result.len(), 2);
        let names: Vec<&str> = result.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"RSI(14)"));
        assert!(names.contains(&"SMA(5)"));
    }

    #[test]
    fn or_combinator_deduplicates_indicators() {
        let df = make_ohlcv_df(30);
        let spec = SignalSpec::Or {
            left: Box::new(SignalSpec::Formula {
                formula: "close > sma(close, 5)".into(),
            }),
            right: Box::new(SignalSpec::Formula {
                formula: "close > sma(close, 20)".into(),
            }),
        };
        let result = compute_indicator_data(&spec, &df, "date");
        // Both are SMA but different periods — should produce 2 entries
        assert_eq!(result.len(), 2);
    }

    // ── total_points metadata ────────────────────────────────────────────────

    #[test]
    fn total_points_none_when_not_sampled() {
        let df = make_ohlcv_df(30);
        let spec = SignalSpec::Formula {
            formula: "rsi(close, 14) < 30".into(),
        };
        let result = compute_indicator_data(&spec, &df, "date");
        assert!(!result.is_empty());
        // 30 data points is well under MAX_INDICATOR_POINTS
        assert!(result[0].total_points.is_none());
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
