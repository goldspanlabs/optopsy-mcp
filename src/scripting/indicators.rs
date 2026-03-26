//! Pre-computed indicator store for O(1) per-bar lookups.
//!
//! Indicators are declared in `config().data.indicators` and/or auto-scanned
//! from the compiled AST. All values are batch-computed before the simulation
//! loop starts. Undeclared indicators return `()` at runtime.

use std::collections::HashMap;

use anyhow::{bail, Result};
use rust_ti::candle_indicators::bulk as cti;
use rust_ti::momentum_indicators::bulk as mti;
use rust_ti::other_indicators::bulk as oti;
use rust_ti::standard_indicators::bulk as sti;
use rust_ti::trend_indicators::bulk as tti;

use super::types::OhlcvBar;

/// Key identifying a specific pre-computed indicator series.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndicatorKey {
    pub name: String,
    pub params: Vec<IndicatorParam>,
}

/// A single parameter value for an indicator (for hashing).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IndicatorParam {
    Int(i64),
    Str(String),
}

/// Store of pre-computed indicator series, indexed by `IndicatorKey`.
///
/// Each value is a `Vec<f64>` aligned to the bar index — the value at
/// `values[bar_idx]` is the indicator value at that bar (NaN if before
/// the warmup period).
#[derive(Debug, Clone)]
pub struct IndicatorStore {
    cache: HashMap<IndicatorKey, Vec<f64>>,
}

impl IndicatorStore {
    /// Create a new empty store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    /// Get the indicator value at a specific bar index.
    ///
    /// Returns `None` if the indicator is not pre-computed or the index is
    /// out of bounds. Returns `Some(NaN)` for bars before the warmup period.
    pub fn get(&self, key: &IndicatorKey, bar_idx: usize) -> Option<f64> {
        self.cache.get(key).and_then(|v| v.get(bar_idx).copied())
    }

    /// Get the indicator value at `bar_idx - bars_ago`.
    ///
    /// Returns `None` if the lookback goes before the start of the series.
    pub fn get_at(&self, key: &IndicatorKey, bar_idx: usize, bars_ago: usize) -> Option<f64> {
        if bars_ago > bar_idx {
            return None;
        }
        self.get(key, bar_idx - bars_ago)
    }

    /// Insert a pre-computed indicator series.
    pub fn insert(&mut self, key: IndicatorKey, values: Vec<f64>) {
        self.cache.insert(key, values);
    }

    /// Check if an indicator has been pre-computed.
    pub fn contains(&self, key: &IndicatorKey) -> bool {
        self.cache.contains_key(key)
    }

    /// Export all indicator series as a map of declaration strings → values.
    /// Used to include indicator data in the backtest result for FE chart overlays.
    pub fn to_series_map(&self) -> HashMap<String, Vec<f64>> {
        self.cache
            .iter()
            .map(|(key, values)| {
                let decl = if key.params.is_empty() {
                    key.name.clone()
                } else {
                    let params_str: Vec<String> = key
                        .params
                        .iter()
                        .map(|p| match p {
                            IndicatorParam::Int(i) => i.to_string(),
                            IndicatorParam::Str(s) => s.clone(),
                        })
                        .collect();
                    format!("{}:{}", key.name, params_str.join(":"))
                };
                (decl, values.clone())
            })
            .collect()
    }

    /// Build the indicator store from declared indicators and OHLCV bars.
    ///
    /// Parses indicator declarations like `"sma:20"`, `"rsi:14"`, `"macd_line"`,
    /// and batch-computes full series using existing `rust_ti` functions.
    /// All indicators use **rolling windows only** (no lookahead bias).
    pub fn build(declarations: &[String], bars: &[OhlcvBar]) -> Result<Self> {
        let mut store = Self::new();

        if bars.is_empty() {
            return Ok(store);
        }

        let closes: Vec<f64> = bars.iter().map(|b| b.close).collect();
        let highs: Vec<f64> = bars.iter().map(|b| b.high).collect();
        let lows: Vec<f64> = bars.iter().map(|b| b.low).collect();
        let volumes: Vec<f64> = bars.iter().map(|b| b.volume).collect();

        for decl in declarations {
            let (name, params) = parse_indicator_declaration(decl)?;

            let key = IndicatorKey {
                name: name.clone(),
                params: params
                    .iter()
                    .map(|p| IndicatorParam::Int(*p as i64))
                    .collect(),
            };

            if store.contains(&key) {
                continue; // already computed (e.g., from AST scan + config overlap)
            }

            let values = compute_indicator(&name, &params, &closes, &highs, &lows, &volumes)?;
            store.insert(key, values);
        }

        Ok(store)
    }
}

impl Default for IndicatorStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse a declaration like "sma:20" into ("sma", [20]).
/// Applies default params for indicators with known defaults.
pub(super) fn parse_indicator_declaration(decl: &str) -> Result<(String, Vec<usize>)> {
    let parts: Vec<&str> = decl.split(':').collect();
    let name = parts[0].to_lowercase();
    let mut params: Vec<usize> = parts[1..].iter().filter_map(|s| s.parse().ok()).collect();

    // Apply/complete defaults for indicators with known default params
    match name.as_str() {
        "macd_line" | "macd_signal" | "macd_hist" if params.is_empty() => {
            params = vec![12, 26, 9];
        }
        "bbands_upper" | "bbands_lower" => {
            if params.is_empty() {
                params = vec![20, 20]; // period=20, std_mult*10=20 (=> 2.0)
            } else if params.len() == 1 {
                params.push(20); // default std_mult*10=20 (=> 2.0)
            }
        }
        "bbands_mid" if params.is_empty() => {
            params = vec![20];
        }
        "stochastic" => {
            if params.is_empty() {
                params = vec![14, 3];
            } else if params.len() == 1 {
                params.push(3); // default d_smooth=3
            }
        }
        "psar" if params.is_empty() => {
            params = vec![2, 20]; // accel*100=2 (0.02), max_accel*100=20 (0.20)
        }
        "supertrend" => {
            if params.is_empty() {
                params = vec![10, 30]; // period=10, mult*10=30 (3.0)
            } else if params.len() == 1 {
                params.push(30); // default mult*10=30
            }
        }
        "keltner_upper" | "keltner_lower" => {
            if params.is_empty() {
                params = vec![20, 20]; // period=20, mult*10=20 (2.0)
            } else if params.len() == 1 {
                params.push(20); // default mult*10=20
            }
        }
        _ => {}
    }

    Ok((name, params))
}

// ---------------------------------------------------------------------------
// Helper: pad a shorter rust_ti output to match bar count (NaN front-fill).
// ---------------------------------------------------------------------------

/// Pad a shorter vector with NaN at the front to align with the target length.
/// rust_ti bulk functions return vectors shorter than the input (missing warmup),
/// so we must pad to keep indices aligned with bar positions.
fn pad_front(vals: &[f64], target_len: usize) -> Vec<f64> {
    let pad = target_len.saturating_sub(vals.len());
    let mut result = vec![f64::NAN; pad];
    result.extend_from_slice(vals);
    result
}

/// Compute a full indicator series from OHLCV data.
///
/// Returns a `Vec<f64>` with one value per bar. Values before the warmup
/// period are `f64::NAN`. All computations are strictly causal (rolling
/// window only — no future data).
fn compute_indicator(
    name: &str,
    params: &[usize],
    closes: &[f64],
    highs: &[f64],
    lows: &[f64],
    volumes: &[f64],
) -> Result<Vec<f64>> {
    let n = closes.len();
    let period = params.first().copied().unwrap_or(14);

    // Multi-param indicators: extract additional params
    let param2 = params.get(1).copied();
    let param3 = params.get(2).copied();

    match name {
        // ── Standard indicators (SMA, EMA) ───────────────────────────────
        "sma" => {
            if period == 0 || n < period {
                return Ok(vec![f64::NAN; n]);
            }
            let vals = sti::simple_moving_average(closes, period);
            Ok(pad_front(&vals, n))
        }
        "ema" => {
            if period == 0 || n < period {
                return Ok(vec![f64::NAN; n]);
            }
            let vals = sti::exponential_moving_average(closes, period);
            Ok(pad_front(&vals, n))
        }

        // ── RSI ──────────────────────────────────────────────────────────
        "rsi" => {
            if period == 0 || n <= period {
                return Ok(vec![f64::NAN; n]);
            }
            let vals = mti::relative_strength_index(
                closes,
                rust_ti::ConstantModelType::SmoothedMovingAverage,
                period,
            );
            Ok(pad_front(&vals, n))
        }

        // ── ATR ──────────────────────────────────────────────────────────
        "atr" => {
            if period == 0 || n < period {
                return Ok(vec![f64::NAN; n]);
            }
            let vals = oti::average_true_range(
                closes,
                highs,
                lows,
                rust_ti::ConstantModelType::SmoothedMovingAverage,
                period,
            );
            Ok(pad_front(&vals, n))
        }

        // ── MACD: params = [fast, slow, signal] or defaults [12, 26, 9] ─
        "macd_line" | "macd_signal" | "macd_hist" => {
            let fast = period;
            let slow = param2.unwrap_or(26);
            let signal = param3.unwrap_or(9);

            // rust_ti::macd only supports fixed 12/26/9. Use it when defaults match.
            if fast == 12 && slow == 26 && signal == 9 && n >= 34 {
                let macd_values = sti::macd(closes);
                let extracted: Vec<f64> = macd_values
                    .iter()
                    .map(|t| match name {
                        "macd_line" => t.0,
                        "macd_signal" => t.1,
                        _ => t.2,
                    })
                    .collect();
                return Ok(pad_front(&extracted, n));
            }

            // Custom params: compute via EMA difference
            let warmup = slow.max(fast);
            if warmup == 0 || n < warmup {
                return Ok(vec![f64::NAN; n]);
            }
            let fast_ema = pad_front(&sti::exponential_moving_average(closes, fast), n);
            let slow_ema = pad_front(&sti::exponential_moving_average(closes, slow), n);

            let mut line = vec![f64::NAN; n];
            for i in 0..n {
                if !fast_ema[i].is_nan() && !slow_ema[i].is_nan() {
                    line[i] = fast_ema[i] - slow_ema[i];
                }
            }

            if name == "macd_line" {
                return Ok(line);
            }

            // Signal = EMA of MACD line
            let valid_line: Vec<f64> = line.iter().copied().filter(|v| !v.is_nan()).collect();
            if signal == 0 || valid_line.len() < signal {
                return Ok(vec![f64::NAN; n]);
            }
            let sig_ema = sti::exponential_moving_average(&valid_line, signal);
            let mut sig = vec![f64::NAN; n];
            let first_valid = line.iter().position(|v| !v.is_nan()).unwrap_or(0);
            let offset = first_valid + signal - 1;
            for (i, &v) in sig_ema.iter().enumerate() {
                let idx = offset + i;
                if idx < n {
                    sig[idx] = v;
                }
            }

            if name == "macd_signal" {
                return Ok(sig);
            }

            // Histogram = line - signal
            let mut hist = vec![f64::NAN; n];
            for i in 0..n {
                if !line[i].is_nan() && !sig[i].is_nan() {
                    hist[i] = line[i] - sig[i];
                }
            }
            Ok(hist)
        }

        // ── Bollinger Bands: params = [period, std_mult*10] ──────────────
        "bbands_upper" | "bbands_mid" | "bbands_lower" => {
            let std_mult = param2.map(|v| v as f64 / 10.0).unwrap_or(2.0);
            if period == 0 || n < period {
                return Ok(vec![f64::NAN; n]);
            }
            let bbands = cti::moving_constant_bands(
                closes,
                rust_ti::ConstantModelType::SimpleMovingAverage,
                rust_ti::DeviationModel::StandardDeviation,
                std_mult,
                period,
            );
            let extracted: Vec<f64> = bbands
                .iter()
                .map(|t| match name {
                    "bbands_lower" => t.0,
                    "bbands_mid" => t.1,
                    _ => t.2, // bbands_upper
                })
                .collect();
            Ok(pad_front(&extracted, n))
        }

        // ── Stochastic %K with D-period SMA smoothing ────────────────────
        "stochastic" => {
            let d_smooth = param2.unwrap_or(3);
            if period == 0 || n < period {
                return Ok(vec![f64::NAN; n]);
            }
            let raw_k = mti::stochastic_oscillator(closes, period);
            let padded_raw = pad_front(&raw_k, n);

            // Apply D-period SMA smoothing (d_smooth <= 1 means no smoothing)
            if d_smooth <= 1 {
                return Ok(padded_raw);
            }
            let first_valid = padded_raw.iter().position(|v| !v.is_nan());
            let Some(start) = first_valid else {
                return Ok(padded_raw);
            };
            let tail = &padded_raw[start..];
            if tail.len() < d_smooth {
                return Ok(padded_raw);
            }
            let smoothed_vals = sti::simple_moving_average(tail, d_smooth);
            let smoothed_start = start + d_smooth - 1;
            let mut result = vec![f64::NAN; n];
            for (i, &v) in smoothed_vals.iter().enumerate() {
                let idx = smoothed_start + i;
                if idx < n {
                    result[idx] = v;
                }
            }
            Ok(result)
        }

        // ── CCI: Commodity Channel Index ─────────────────────────────────
        "cci" => {
            if period == 0 || n < period {
                return Ok(vec![f64::NAN; n]);
            }
            // CCI needs typical price as input
            let tp: Vec<f64> = (0..n)
                .map(|i| (highs[i] + lows[i] + closes[i]) / 3.0)
                .collect();
            let vals = mti::commodity_channel_index(
                &tp,
                rust_ti::ConstantModelType::SimpleMovingAverage,
                rust_ti::DeviationModel::MeanAbsoluteDeviation,
                0.015,
                period,
            );
            Ok(pad_front(&vals, n))
        }

        // ── OBV: On-Balance Volume ───────────────────────────────────────
        "obv" => {
            if n < 2 {
                return Ok(vec![0.0; n]);
            }
            let vals = mti::on_balance_volume(closes, volumes, 0.0);
            Ok(pad_front(&vals, n))
        }

        // ── ADX / +DI / -DI: Directional Movement System ────────────────
        "adx" | "plus_di" | "minus_di" => {
            if period == 0 || n < period + 1 {
                return Ok(vec![f64::NAN; n]);
            }
            let dms = tti::directional_movement_system(
                highs,
                lows,
                closes,
                period,
                rust_ti::ConstantModelType::SmoothedMovingAverage,
            );
            let extracted: Vec<f64> = dms
                .iter()
                .map(|t| match name {
                    "plus_di" => t.0,
                    "minus_di" => t.1,
                    _ => t.2, // adx
                })
                .collect();
            Ok(pad_front(&extracted, n))
        }

        // ── PSAR: Parabolic SAR ─────────────────────────────────────────
        "psar" => {
            // params = [accel*100, max_accel*100] e.g. [2, 20] for 0.02/0.20
            let accel = period as f64 / 100.0; // period is first param
            let max_accel = param2.map(|v| v as f64 / 100.0).unwrap_or(0.20);
            if n < 2 {
                return Ok(vec![f64::NAN; n]);
            }
            let vals = tti::parabolic_time_price_system(
                highs,
                lows,
                accel,
                max_accel,
                accel,
                rust_ti::Position::Long,
                lows[0],
            );
            Ok(pad_front(&vals, n))
        }

        // ── Supertrend ──────────────────────────────────────────────────
        "supertrend" => {
            let mult = param2.map(|v| v as f64 / 10.0).unwrap_or(3.0);
            if period == 0 || n < period {
                return Ok(vec![f64::NAN; n]);
            }
            let vals = cti::supertrend(
                highs,
                lows,
                closes,
                rust_ti::ConstantModelType::SimpleMovingAverage,
                mult,
                period,
            );
            Ok(pad_front(&vals, n))
        }

        // ── Keltner Channels (hand-rolled — no rust_ti equivalent) ───────
        "keltner_upper" | "keltner_lower" => {
            let mult = param2.map(|v| v as f64 / 10.0).unwrap_or(2.0);
            let (upper, lower) = rolling_keltner(closes, highs, lows, period, mult);
            match name {
                "keltner_upper" => Ok(upper),
                _ => Ok(lower),
            }
        }

        // ── Donchian Channels ────────────────────────────────────────────
        "donchian_upper" | "donchian_mid" | "donchian_lower" => {
            if period == 0 || n < period {
                return Ok(vec![f64::NAN; n]);
            }
            let dc = cti::donchian_channels(highs, lows, period);
            let extracted: Vec<f64> = dc
                .iter()
                .map(|t| match name {
                    "donchian_upper" => t.0,
                    "donchian_mid" => t.1,
                    _ => t.2, // donchian_lower
                })
                .collect();
            Ok(pad_front(&extracted, n))
        }

        // ── True Range (no period — single-bar value) ────────────────────
        "tr" => {
            if n == 0 {
                return Ok(vec![f64::NAN; n]);
            }
            let vals = oti::true_range(closes, highs, lows);
            Ok(pad_front(&vals, n))
        }

        // ── Williams %R ──────────────────────────────────────────────────
        "williams_r" => {
            if period == 0 || n < period {
                return Ok(vec![f64::NAN; n]);
            }
            let vals = mti::williams_percent_r(highs, lows, closes, period);
            Ok(pad_front(&vals, n))
        }

        // ── PPO: Percentage Price Oscillator ─────────────────────────────
        "ppo" => {
            let long = param2.unwrap_or(26);
            if period == 0 || long == 0 || n <= long {
                return Ok(vec![f64::NAN; n]);
            }
            let vals = mti::percentage_price_oscillator(
                closes,
                period,
                long,
                rust_ti::ConstantModelType::ExponentialMovingAverage,
            );
            Ok(pad_front(&vals, n))
        }

        // ── CMO: Chande Momentum Oscillator ──────────────────────────────
        "cmo" => {
            if period == 0 || n <= period {
                return Ok(vec![f64::NAN; n]);
            }
            let vals = mti::chande_momentum_oscillator(closes, period);
            Ok(pad_front(&vals, n))
        }

        // ── MFI: Money Flow Index ────────────────────────────────────────
        "mfi" => {
            if period == 0 || n <= period {
                return Ok(vec![f64::NAN; n]);
            }
            let tp: Vec<f64> = (0..n)
                .map(|i| (highs[i] + lows[i] + closes[i]) / 3.0)
                .collect();
            let vals = mti::money_flow_index(&tp, volumes, period);
            Ok(pad_front(&vals, n))
        }

        // ── VPT: Volume Price Trend ──────────────────────────────────────
        "vpt" => {
            if n < 2 {
                return Ok(vec![0.0; n]);
            }
            let vals = tti::volume_price_trend(closes, &volumes[1..], 0.0);
            Ok(pad_front(&vals, n))
        }

        // ── ROC: Rate of Change (hand-rolled — no rust_ti equivalent) ────
        "roc" => Ok(rolling_roc(closes, period)),

        // ── Rank: Percentile rank (hand-rolled) ──────────────────────────
        "rank" => Ok(rolling_rank(closes, period)),

        // ── IV Rank: Min-max normalization (hand-rolled) ─────────────────
        "iv_rank" => Ok(rolling_iv_rank(closes, period)),

        // ── CMF: Chaikin Money Flow (hand-rolled) ────────────────────────
        "cmf" => Ok(rolling_cmf(highs, lows, closes, volumes, period)),

        // ── Transform functions (hand-rolled) ────────────────────────────
        "change" => Ok(rolling_change(closes, period)),
        "pct_change" => Ok(rolling_pct_change(closes, period)),
        "std" => Ok(rolling_std(closes, period)),
        "max" => Ok(rolling_max(closes, period)),
        "min" => Ok(rolling_min(closes, period)),
        "consecutive_up" => Ok(rolling_consecutive_up(closes)),
        "consecutive_down" => Ok(rolling_consecutive_down(closes)),

        _ => bail!(
            "Indicator '{name}' not recognized. See SCRIPTING_REFERENCE.md for the full list."
        ),
    }
}

// ---------------------------------------------------------------------------
// Hand-rolled implementations for indicators not available in rust_ti
// ---------------------------------------------------------------------------

/// Keltner Channels: returns (upper, lower).
fn rolling_keltner(
    closes: &[f64],
    highs: &[f64],
    lows: &[f64],
    period: usize,
    mult: f64,
) -> (Vec<f64>, Vec<f64>) {
    let n = closes.len();
    let mut upper = vec![f64::NAN; n];
    let mut lower = vec![f64::NAN; n];
    if period == 0 || n < period {
        return (upper, lower);
    }
    let ema_vals = sti::exponential_moving_average(closes, period);
    let ema = pad_front(&ema_vals, n);
    let atr_vals = oti::average_true_range(
        closes,
        highs,
        lows,
        rust_ti::ConstantModelType::SmoothedMovingAverage,
        period,
    );
    let atr = pad_front(&atr_vals, n);
    for i in 0..n {
        if !ema[i].is_nan() && !atr[i].is_nan() {
            upper[i] = ema[i] + mult * atr[i];
            lower[i] = ema[i] - mult * atr[i];
        }
    }
    (upper, lower)
}

/// Rolling maximum over a window.
fn rolling_max(data: &[f64], period: usize) -> Vec<f64> {
    let n = data.len();
    let mut result = vec![f64::NAN; n];
    if period == 0 || n < period {
        return result;
    }
    for i in (period - 1)..n {
        result[i] = data[(i + 1 - period)..=i]
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
    }
    result
}

/// Rolling minimum over a window.
fn rolling_min(data: &[f64], period: usize) -> Vec<f64> {
    let n = data.len();
    let mut result = vec![f64::NAN; n];
    if period == 0 || n < period {
        return result;
    }
    for i in (period - 1)..n {
        result[i] = data[(i + 1 - period)..=i]
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
    }
    result
}

/// ROC: Rate of Change (percentage).
fn rolling_roc(data: &[f64], period: usize) -> Vec<f64> {
    let n = data.len();
    let mut result = vec![f64::NAN; n];
    if period == 0 || n <= period {
        return result;
    }
    for i in period..n {
        if data[i - period].abs() > f64::EPSILON {
            result[i] = (data[i] - data[i - period]) / data[i - period] * 100.0;
        }
    }
    result
}

/// Percentile rank within rolling window (0-100).
fn rolling_rank(data: &[f64], period: usize) -> Vec<f64> {
    let n = data.len();
    let mut result = vec![f64::NAN; n];
    if period == 0 || n < period {
        return result;
    }
    for i in (period - 1)..n {
        let s = i + 1 - period;
        let cur = data[i];
        let below = data[s..=i].iter().filter(|&&v| v < cur).count();
        result[i] = below as f64 / (period - 1).max(1) as f64 * 100.0;
    }
    result
}

/// IV Rank: Min-max normalization within rolling window (0-100).
fn rolling_iv_rank(data: &[f64], period: usize) -> Vec<f64> {
    let n = data.len();
    let mut result = vec![f64::NAN; n];
    if period == 0 || n < period {
        return result;
    }
    for i in (period - 1)..n {
        let s = i + 1 - period;
        let sl = &data[s..=i];
        let mn = sl.iter().copied().fold(f64::INFINITY, f64::min);
        let mx = sl.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let r = mx - mn;
        result[i] = if r > f64::EPSILON {
            (data[i] - mn) / r * 100.0
        } else {
            50.0
        };
    }
    result
}

/// CMF: Chaikin Money Flow.
fn rolling_cmf(
    highs: &[f64],
    lows: &[f64],
    closes: &[f64],
    volumes: &[f64],
    period: usize,
) -> Vec<f64> {
    let n = closes.len();
    let mut result = vec![f64::NAN; n];
    if period == 0 || n < period {
        return result;
    }
    for i in (period - 1)..n {
        let s = i + 1 - period;
        let mut mfv = 0.0;
        let mut vol = 0.0;
        for j in s..=i {
            let hl = highs[j] - lows[j];
            let clv = if hl > f64::EPSILON {
                ((closes[j] - lows[j]) - (highs[j] - closes[j])) / hl
            } else {
                0.0
            };
            mfv += clv * volumes[j];
            vol += volumes[j];
        }
        result[i] = if vol > f64::EPSILON { mfv / vol } else { 0.0 };
    }
    result
}

/// Change: data[i] - data[i-period].
fn rolling_change(data: &[f64], period: usize) -> Vec<f64> {
    let n = data.len();
    let mut result = vec![f64::NAN; n];
    if period == 0 || n <= period {
        return result;
    }
    for i in period..n {
        result[i] = data[i] - data[i - period];
    }
    result
}

/// Pct Change: (data[i] - data[i-period]) / data[i-period].
fn rolling_pct_change(data: &[f64], period: usize) -> Vec<f64> {
    let n = data.len();
    let mut result = vec![f64::NAN; n];
    if period == 0 || n <= period {
        return result;
    }
    for i in period..n {
        if data[i - period].abs() > f64::EPSILON {
            result[i] = (data[i] - data[i - period]) / data[i - period];
        }
    }
    result
}

/// Rolling standard deviation.
fn rolling_std(data: &[f64], period: usize) -> Vec<f64> {
    let n = data.len();
    let mut result = vec![f64::NAN; n];
    if period == 0 || n < period {
        return result;
    }
    let sma_vals = sti::simple_moving_average(data, period);
    let sma = pad_front(&sma_vals, n);
    for i in (period - 1)..n {
        if sma[i].is_nan() {
            continue;
        }
        let sl = &data[(i + 1 - period)..=i];
        let var = sl.iter().map(|&x| (x - sma[i]).powi(2)).sum::<f64>() / period as f64;
        result[i] = var.sqrt();
    }
    result
}

/// Consecutive bars where data rises.
fn rolling_consecutive_up(data: &[f64]) -> Vec<f64> {
    let n = data.len();
    let mut result = vec![0.0; n];
    for i in 1..n {
        result[i] = if data[i] > data[i - 1] {
            result[i - 1] + 1.0
        } else {
            0.0
        };
    }
    result
}

/// Consecutive bars where data falls.
fn rolling_consecutive_down(data: &[f64]) -> Vec<f64> {
    let n = data.len();
    let mut result = vec![0.0; n];
    for i in 1..n {
        result[i] = if data[i] < data[i - 1] {
            result[i - 1] + 1.0
        } else {
            0.0
        };
    }
    result
}
