//! Pre-computed indicator store for O(1) per-bar lookups.
//!
//! Indicators are declared in `config().data.indicators` and/or auto-scanned
//! from the compiled AST. All values are batch-computed before the simulation
//! loop starts. No lazy fallback — undeclared indicators produce a runtime error.

use std::collections::HashMap;

use anyhow::{bail, Result};

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
fn parse_indicator_declaration(decl: &str) -> Result<(String, Vec<usize>)> {
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
        _ => {}
    }

    Ok((name, params))
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
    let _n = closes.len();
    let period = params.first().copied().unwrap_or(14);

    // Multi-param indicators: extract additional params
    let param2 = params.get(1).copied();
    let param3 = params.get(2).copied();

    match name {
        "sma" => Ok(rolling_sma(closes, period)),
        "ema" => Ok(rolling_ema(closes, period)),
        "rsi" => Ok(rolling_rsi(closes, period)),
        "atr" => Ok(rolling_atr(highs, lows, closes, period)),

        // MACD: params = [fast, slow, signal] or defaults [12, 26, 9]
        "macd_line" => {
            let fast = period; // or 12 from "macd_line" with no params
            let slow = param2.unwrap_or(26);
            let signal = param3.unwrap_or(9);
            let (line, _, _) = rolling_macd(closes, fast, slow, signal);
            Ok(line)
        }
        "macd_signal" => {
            let fast = period;
            let slow = param2.unwrap_or(26);
            let signal = param3.unwrap_or(9);
            let (_, sig, _) = rolling_macd(closes, fast, slow, signal);
            Ok(sig)
        }
        "macd_hist" => {
            let fast = period;
            let slow = param2.unwrap_or(26);
            let signal = param3.unwrap_or(9);
            let (_, _, hist) = rolling_macd(closes, fast, slow, signal);
            Ok(hist)
        }

        // Bollinger Bands: params = [period, std_mult*10] (e.g., 20 for std=2.0)
        "bbands_upper" => {
            let std_mult = param2.map(|v| v as f64 / 10.0).unwrap_or(2.0);
            let (upper, _, _) = rolling_bbands(closes, period, std_mult);
            Ok(upper)
        }
        "bbands_mid" => {
            let (_, mid, _) = rolling_bbands(closes, period, 2.0);
            Ok(mid)
        }
        "bbands_lower" => {
            let std_mult = param2.map(|v| v as f64 / 10.0).unwrap_or(2.0);
            let (_, _, lower) = rolling_bbands(closes, period, std_mult);
            Ok(lower)
        }

        // Stochastic %K: params = [k_period, d_smoothing]
        "stochastic" => {
            let d_smooth = param2.unwrap_or(3);
            Ok(rolling_stochastic_k(highs, lows, closes, period, d_smooth))
        }

        // CCI: Commodity Channel Index
        "cci" => Ok(rolling_cci(highs, lows, closes, period)),

        // OBV: On-Balance Volume (no period param)
        "obv" => Ok(rolling_obv(closes, volumes)),

        _ => bail!(
            "Indicator '{name}' not recognized. Supported: sma, ema, rsi, atr, \
             macd_line, macd_signal, macd_hist, bbands_upper, bbands_mid, bbands_lower, \
             stochastic, cci, obv"
        ),
    }
}

// ---------------------------------------------------------------------------
// Rolling indicator implementations (strictly causal, no lookahead)
// ---------------------------------------------------------------------------

fn rolling_sma(data: &[f64], period: usize) -> Vec<f64> {
    let mut result = vec![f64::NAN; data.len()];
    if period == 0 || data.len() < period {
        return result;
    }
    let mut sum: f64 = data[..period].iter().sum();
    result[period - 1] = sum / period as f64;
    for i in period..data.len() {
        sum += data[i] - data[i - period];
        result[i] = sum / period as f64;
    }
    result
}

fn rolling_ema(data: &[f64], period: usize) -> Vec<f64> {
    let mut result = vec![f64::NAN; data.len()];
    if period == 0 || data.len() < period {
        return result;
    }
    let alpha = 2.0 / (period as f64 + 1.0);
    // Seed with SMA
    let sma: f64 = data[..period].iter().sum::<f64>() / period as f64;
    result[period - 1] = sma;
    let mut prev = sma;
    for i in period..data.len() {
        let ema = alpha * data[i] + (1.0 - alpha) * prev;
        result[i] = ema;
        prev = ema;
    }
    result
}

fn rolling_rsi(data: &[f64], period: usize) -> Vec<f64> {
    let mut result = vec![f64::NAN; data.len()];
    if period == 0 || data.len() <= period {
        return result;
    }

    let mut gains = 0.0;
    let mut losses = 0.0;

    // Initial average gain/loss over the first `period` changes
    for i in 1..=period {
        let change = data[i] - data[i - 1];
        if change > 0.0 {
            gains += change;
        } else {
            losses -= change; // make positive
        }
    }

    let mut avg_gain = gains / period as f64;
    let mut avg_loss = losses / period as f64;

    let rsi = if avg_loss < f64::EPSILON {
        100.0
    } else {
        100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
    };
    result[period] = rsi;

    // Smoothed subsequent values (Wilder's smoothing)
    for i in (period + 1)..data.len() {
        let change = data[i] - data[i - 1];
        let (gain, loss) = if change > 0.0 {
            (change, 0.0)
        } else {
            (0.0, -change)
        };
        avg_gain = (avg_gain * (period as f64 - 1.0) + gain) / period as f64;
        avg_loss = (avg_loss * (period as f64 - 1.0) + loss) / period as f64;

        let rsi = if avg_loss < f64::EPSILON {
            100.0
        } else {
            100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
        };
        result[i] = rsi;
    }

    result
}

fn rolling_atr(highs: &[f64], lows: &[f64], closes: &[f64], period: usize) -> Vec<f64> {
    let n = highs.len();
    let mut result = vec![f64::NAN; n];
    if period == 0 || n <= period {
        return result;
    }

    // True Range
    let mut tr = vec![0.0; n];
    tr[0] = highs[0] - lows[0];
    for i in 1..n {
        let hl = highs[i] - lows[i];
        let hc = (highs[i] - closes[i - 1]).abs();
        let lc = (lows[i] - closes[i - 1]).abs();
        tr[i] = hl.max(hc).max(lc);
    }

    // Initial ATR is SMA of first `period` TRs
    let first_atr: f64 = tr[..period].iter().sum::<f64>() / period as f64;
    result[period - 1] = first_atr;
    let mut prev = first_atr;

    // Wilder's smoothing
    for i in period..n {
        let atr = (prev * (period as f64 - 1.0) + tr[i]) / period as f64;
        result[i] = atr;
        prev = atr;
    }

    result
}

/// MACD: returns (line, signal, histogram).
fn rolling_macd(
    data: &[f64],
    fast: usize,
    slow: usize,
    signal_period: usize,
) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let n = data.len();
    let mut line = vec![f64::NAN; n];
    let mut signal = vec![f64::NAN; n];
    let mut hist = vec![f64::NAN; n];

    let fast_ema = rolling_ema(data, fast);
    let slow_ema = rolling_ema(data, slow);

    // MACD line = fast EMA - slow EMA
    let warmup = slow.max(fast);
    for i in (warmup - 1)..n {
        if !fast_ema[i].is_nan() && !slow_ema[i].is_nan() {
            line[i] = fast_ema[i] - slow_ema[i];
        }
    }

    // Signal line = EMA of MACD line
    // Collect non-NaN MACD values for signal EMA seed
    let macd_values: Vec<f64> = line.iter().copied().filter(|v| !v.is_nan()).collect();
    if macd_values.len() >= signal_period {
        let alpha = 2.0 / (signal_period as f64 + 1.0);
        let seed: f64 = macd_values[..signal_period].iter().sum::<f64>() / signal_period as f64;

        let mut sig = seed;
        let mut macd_idx = 0;
        for i in 0..n {
            if line[i].is_nan() {
                continue;
            }
            if macd_idx < signal_period - 1 {
                macd_idx += 1;
                continue;
            }
            if macd_idx == signal_period - 1 {
                sig = seed;
            } else {
                sig = alpha * line[i] + (1.0 - alpha) * sig;
            }
            signal[i] = sig;
            hist[i] = line[i] - sig;
            macd_idx += 1;
        }
    }

    (line, signal, hist)
}

/// Bollinger Bands: returns (upper, middle, lower).
fn rolling_bbands(data: &[f64], period: usize, std_mult: f64) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let n = data.len();
    let mut upper = vec![f64::NAN; n];
    let mut mid = vec![f64::NAN; n];
    let mut lower = vec![f64::NAN; n];

    let sma = rolling_sma(data, period);

    for i in (period - 1)..n {
        if sma[i].is_nan() {
            continue;
        }
        // Rolling std dev
        let slice = &data[(i + 1 - period)..=i];
        let mean = sma[i];
        let variance = slice.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / period as f64;
        let std_dev = variance.sqrt();

        mid[i] = mean;
        upper[i] = mean + std_mult * std_dev;
        lower[i] = mean - std_mult * std_dev;
    }

    (upper, mid, lower)
}

/// Stochastic %K with D-period smoothing.
fn rolling_stochastic_k(
    highs: &[f64],
    lows: &[f64],
    closes: &[f64],
    k_period: usize,
    _d_smooth: usize,
) -> Vec<f64> {
    let n = closes.len();
    let mut result = vec![f64::NAN; n];

    for i in (k_period - 1)..n {
        let start = i + 1 - k_period;
        let highest: f64 = highs[start..=i]
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let lowest: f64 = lows[start..=i]
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);

        let range = highest - lowest;
        if range > f64::EPSILON {
            result[i] = (closes[i] - lowest) / range * 100.0;
        } else {
            result[i] = 50.0; // midpoint when range is zero
        }
    }

    result
}

/// CCI: Commodity Channel Index.
fn rolling_cci(highs: &[f64], lows: &[f64], closes: &[f64], period: usize) -> Vec<f64> {
    let n = closes.len();
    let mut result = vec![f64::NAN; n];

    // Typical price = (H + L + C) / 3
    let tp: Vec<f64> = (0..n)
        .map(|i| (highs[i] + lows[i] + closes[i]) / 3.0)
        .collect();

    for i in (period - 1)..n {
        let start = i + 1 - period;
        let slice = &tp[start..=i];
        let mean = slice.iter().sum::<f64>() / period as f64;
        let mean_dev = slice.iter().map(|&x| (x - mean).abs()).sum::<f64>() / period as f64;

        if mean_dev > f64::EPSILON {
            result[i] = (tp[i] - mean) / (0.015 * mean_dev);
        } else {
            result[i] = 0.0;
        }
    }

    result
}

/// OBV: On-Balance Volume (cumulative, no period).
fn rolling_obv(closes: &[f64], volumes: &[f64]) -> Vec<f64> {
    let n = closes.len();
    if n == 0 {
        return vec![];
    }
    let mut result = vec![0.0; n];
    result[0] = volumes.first().copied().unwrap_or(0.0);

    for i in 1..n {
        let vol = volumes.get(i).copied().unwrap_or(0.0);
        if closes[i] > closes[i - 1] {
            result[i] = result[i - 1] + vol;
        } else if closes[i] < closes[i - 1] {
            result[i] = result[i - 1] - vol;
        } else {
            result[i] = result[i - 1];
        }
    }

    result
}
