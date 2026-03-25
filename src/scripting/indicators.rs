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
fn parse_indicator_declaration(decl: &str) -> Result<(String, Vec<usize>)> {
    let parts: Vec<&str> = decl.split(':').collect();
    let name = parts[0].to_lowercase();
    let params: Vec<usize> = parts[1..].iter().filter_map(|s| s.parse().ok()).collect();
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
    let n = closes.len();
    let period = params.first().copied().unwrap_or(14);

    match name {
        "sma" => Ok(rolling_sma(closes, period)),
        "ema" => Ok(rolling_ema(closes, period)),
        "rsi" => Ok(rolling_rsi(closes, period)),
        "atr" => Ok(rolling_atr(highs, lows, closes, period)),
        _ => bail!(
            "Indicator '{name}' not recognized. Supported: sma, ema, rsi, atr, \
             macd_line, macd_signal, macd_hist, bbands_upper, bbands_mid, bbands_lower, \
             stochastic, adx, cci, obv"
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
