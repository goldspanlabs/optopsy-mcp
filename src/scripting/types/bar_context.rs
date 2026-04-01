//! BarContext — the `ctx` object exposed to Rhai scripts.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use rhai::Dynamic;

use super::config::{CrossSymbolBar, OhlcvBar, ScriptConfig};
use super::position::ScriptPosition;

use crate::scripting::indicators::IndicatorStore;

// ---------------------------------------------------------------------------
// Custom series store — collects script-emitted plot data during simulation
// ---------------------------------------------------------------------------

/// Shared store for custom indicator series emitted by scripts via `ctx.plot()`.
///
/// Thread-safe via `Mutex` (no contention since Rhai is single-threaded).
/// Pre-allocates each series to `num_bars` length on first write so indexing
/// by `bar_idx` is always valid.
pub struct CustomSeriesStore {
    /// series_name → values indexed by bar_idx (`None` = not plotted on that bar).
    pub series: HashMap<String, Vec<Option<f64>>>,
    /// series_name → display type hint ("overlay" or "subchart").
    pub display_types: HashMap<String, String>,
    /// Total number of bars (used to pre-allocate series vectors).
    pub num_bars: usize,
}

/// Maximum number of distinct custom series a script may emit.
pub const MAX_CUSTOM_SERIES: usize = 50;

// ---------------------------------------------------------------------------
// BarContext — the `ctx` object exposed to Rhai scripts
// ---------------------------------------------------------------------------

/// Per-bar context object exposed to Rhai scripts as `ctx`.
///
/// Rebuilt each bar (or between Phase A and Phase B within a bar).
/// Contains immutable data references and a snapshot of portfolio state.
/// Positions are wrapped in `Arc` so context construction is just an Arc
/// increment rather than a full `Vec<ScriptPosition>` clone.
#[derive(Clone)]
pub struct BarContext {
    // Current bar data
    pub datetime: NaiveDateTime,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub bar_idx: usize,

    // Portfolio snapshot
    pub cash: f64,
    pub equity: f64,
    pub positions: Arc<Vec<ScriptPosition>>,

    // Shared data (Arc for cheap cloning into Rhai)
    pub indicator_store: Arc<IndicatorStore>,
    pub price_history: Arc<Vec<OhlcvBar>>,
    pub cross_symbol_data: Arc<HashMap<String, Vec<CrossSymbolBar>>>,

    // Options data, pre-partitioned by date (None for pure stock backtests)
    pub options_by_date: Option<Arc<crate::scripting::options_cache::DatePartitionedOptions>>,

    // Config reference for ctx.config.defaults access
    pub config: Arc<ScriptConfig>,

    // Closed trade P&L history (for Kelly sizing)
    pub pnl_history: Arc<Vec<f64>>,

    // Custom series emitted by scripts via ctx.plot()
    pub custom_series: Arc<Mutex<CustomSeriesStore>>,

    // Adjusted close price (accounts for splits + dividends)
    pub adjusted_close: f64,
}

// ---------------------------------------------------------------------------
// BarContext — Rhai getter and method implementations
// ---------------------------------------------------------------------------

impl BarContext {
    // --- Data getters ---
    pub fn get_date(&mut self) -> String {
        self.datetime.date().to_string()
    }
    pub fn get_datetime(&mut self) -> String {
        self.datetime.to_string()
    }
    pub fn get_open(&mut self) -> f64 {
        self.open
    }
    pub fn get_high(&mut self) -> f64 {
        self.high
    }
    pub fn get_low(&mut self) -> f64 {
        self.low
    }
    pub fn get_close(&mut self) -> f64 {
        self.close
    }
    pub fn get_volume(&mut self) -> f64 {
        self.volume
    }
    pub fn get_adjusted_close(&mut self) -> f64 {
        self.adjusted_close
    }
    pub fn get_bar_idx(&mut self) -> i64 {
        self.bar_idx as i64
    }

    // --- Portfolio getters ---
    pub fn get_cash(&mut self) -> f64 {
        self.cash
    }
    pub fn get_equity(&mut self) -> f64 {
        self.equity
    }

    // --- Methods ---
    pub fn price(&mut self, n: i64) -> Dynamic {
        if n < 0 {
            return Dynamic::UNIT;
        }
        let n = n as usize;
        if n > self.bar_idx {
            return Dynamic::UNIT;
        }
        let idx = self.bar_idx - n;
        self.price_history
            .get(idx)
            .map(|b| Dynamic::from(b.close))
            .unwrap_or(Dynamic::UNIT)
    }

    pub fn get_positions(&mut self) -> rhai::Array {
        self.positions.iter().cloned().map(Dynamic::from).collect()
    }

    pub fn position_count(&mut self) -> i64 {
        self.positions.iter().filter(|p| !p.implicit).count() as i64
    }

    pub fn has_positions(&mut self) -> bool {
        self.positions.iter().any(|p| !p.implicit)
    }

    // --- Indicators (current bar) ---
    pub(in crate::scripting) fn indicator_value(&self, name: &str, period: i64) -> Dynamic {
        use crate::scripting::indicators::{IndicatorKey, IndicatorParam};
        let key = IndicatorKey {
            name: name.to_string(),
            params: vec![IndicatorParam::Int(period)],
        };
        match self.indicator_store.get(&key, self.bar_idx) {
            Some(v) if v.is_nan() => Dynamic::UNIT,
            Some(v) => Dynamic::from(v),
            None => Dynamic::UNIT, // indicator not pre-computed
        }
    }

    fn indicator_value_at(&self, name: &str, period: i64, bars_ago: i64) -> Dynamic {
        use crate::scripting::indicators::{IndicatorKey, IndicatorParam};
        if bars_ago < 0 {
            return Dynamic::UNIT;
        }
        let key = IndicatorKey {
            name: name.to_string(),
            params: vec![IndicatorParam::Int(period)],
        };
        match self
            .indicator_store
            .get_at(&key, self.bar_idx, bars_ago as usize)
        {
            Some(v) if v.is_nan() => Dynamic::UNIT,
            Some(v) => Dynamic::from(v),
            None => Dynamic::UNIT,
        }
    }

    pub fn sma(&mut self, period: i64) -> Dynamic {
        self.indicator_value("sma", period)
    }
    pub fn ema(&mut self, period: i64) -> Dynamic {
        self.indicator_value("ema", period)
    }
    pub fn rsi(&mut self, period: i64) -> Dynamic {
        self.indicator_value("rsi", period)
    }
    pub fn atr(&mut self, period: i64) -> Dynamic {
        self.indicator_value("atr", period)
    }
    pub fn macd_line(&mut self) -> Dynamic {
        self.indicator_value_multi("macd_line", &[12, 26, 9])
    }
    pub fn macd_signal(&mut self) -> Dynamic {
        self.indicator_value_multi("macd_signal", &[12, 26, 9])
    }
    pub fn macd_hist(&mut self) -> Dynamic {
        self.indicator_value_multi("macd_hist", &[12, 26, 9])
    }
    pub fn bbands_upper(&mut self, period: i64) -> Dynamic {
        // Default std_dev=2.0, stored as 20 (multiplied by 10 for hashing)
        self.indicator_value_multi("bbands_upper", &[period, 20])
    }
    pub fn bbands_mid(&mut self, period: i64) -> Dynamic {
        self.indicator_value("bbands_mid", period)
    }
    pub fn bbands_lower(&mut self, period: i64) -> Dynamic {
        // Default std_dev=2.0, stored as 20 (multiplied by 10 for hashing)
        self.indicator_value_multi("bbands_lower", &[period, 20])
    }
    pub fn stochastic(&mut self, period: i64) -> Dynamic {
        self.indicator_value("stochastic", period)
    }
    pub fn adx(&mut self, period: i64) -> Dynamic {
        self.indicator_value("adx", period)
    }
    pub fn cci(&mut self, period: i64) -> Dynamic {
        self.indicator_value("cci", period)
    }
    pub fn obv(&mut self) -> Dynamic {
        self.indicator_value_multi("obv", &[])
    }

    // --- New Tier 1 named accessors ---
    pub fn plus_di(&mut self, period: i64) -> Dynamic {
        self.indicator_value("plus_di", period)
    }
    pub fn minus_di(&mut self, period: i64) -> Dynamic {
        self.indicator_value("minus_di", period)
    }
    pub fn keltner_upper(&mut self, period: i64) -> Dynamic {
        self.indicator_value("keltner_upper", period)
    }
    pub fn keltner_lower(&mut self, period: i64) -> Dynamic {
        self.indicator_value("keltner_lower", period)
    }
    pub fn psar(&mut self) -> Dynamic {
        self.indicator_value_multi("psar", &[2, 20]) // 0.02 accel, 0.20 max
    }
    pub fn supertrend(&mut self, period: i64) -> Dynamic {
        self.indicator_value("supertrend", period)
    }
    pub fn donchian_upper(&mut self, period: i64) -> Dynamic {
        self.indicator_value("donchian_upper", period)
    }
    pub fn donchian_mid(&mut self, period: i64) -> Dynamic {
        self.indicator_value("donchian_mid", period)
    }
    pub fn donchian_lower(&mut self, period: i64) -> Dynamic {
        self.indicator_value("donchian_lower", period)
    }
    pub fn williams_r(&mut self, period: i64) -> Dynamic {
        self.indicator_value("williams_r", period)
    }
    pub fn mfi(&mut self, period: i64) -> Dynamic {
        self.indicator_value("mfi", period)
    }
    pub fn rank(&mut self, period: i64) -> Dynamic {
        self.indicator_value("rank", period)
    }
    pub fn iv_rank(&mut self, period: i64) -> Dynamic {
        self.indicator_value("iv_rank", period)
    }
    pub fn tr(&mut self) -> Dynamic {
        self.indicator_value_multi("tr", &[])
    }

    // --- Date/time methods (computed from bar datetime, no IndicatorStore) ---
    pub fn day_of_week(&mut self) -> i64 {
        self.datetime.date().weekday().num_days_from_monday() as i64 + 1 // 1=Mon..7=Sun
    }
    pub fn month(&mut self) -> i64 {
        self.datetime.date().month() as i64
    }
    pub fn day_of_month(&mut self) -> i64 {
        self.datetime.date().day() as i64
    }
    pub fn hour(&mut self) -> i64 {
        self.datetime.time().hour() as i64
    }
    pub fn minute(&mut self) -> i64 {
        self.datetime.time().minute() as i64
    }
    pub fn week_of_year(&mut self) -> i64 {
        self.datetime.date().iso_week().week() as i64
    }

    // --- Generic indicator accessor ---
    pub fn indicator(&mut self, name: String, period: i64) -> Dynamic {
        self.indicator_value(&name, period)
    }
    /// Multi-param indicator lookup via Rhai Map.
    /// Example: `ctx.indicator_with("keltner_upper", #{ period: 20, mult: 15 })`
    /// Params are converted to the IndicatorKey param vector.
    pub fn indicator_with(&mut self, name: String, params: rhai::Map) -> Dynamic {
        use crate::scripting::indicators::{IndicatorKey, IndicatorParam};

        // Extract params as integers (matching IndicatorStore convention)
        let mut param_vec: Vec<IndicatorParam> = Vec::new();
        // Try known param names in order
        for key in &[
            "period",
            "fast",
            "slow",
            "signal",
            "mult",
            "accel",
            "max_accel",
        ] {
            if let Some(val) = params.get(*key) {
                if let Ok(i) = val.as_int() {
                    param_vec.push(IndicatorParam::Int(i));
                } else if let Ok(f) = val.as_float() {
                    // Scale to integer: accel params use *100 (0.02->2), others use *10 (2.0->20)
                    let scaled = match *key {
                        "accel" | "max_accel" => (f * 100.0) as i64,
                        _ => (f * 10.0) as i64,
                    };
                    param_vec.push(IndicatorParam::Int(scaled));
                }
            }
        }

        let key = IndicatorKey {
            name,
            params: param_vec,
        };
        match self.indicator_store.get(&key, self.bar_idx) {
            Some(v) if v.is_nan() => Dynamic::UNIT,
            Some(v) => Dynamic::from(v),
            None => Dynamic::UNIT,
        }
    }

    // --- Indicator lookback ---
    pub fn sma_at(&mut self, period: i64, bars_ago: i64) -> Dynamic {
        self.indicator_value_at("sma", period, bars_ago)
    }
    pub fn ema_at(&mut self, period: i64, bars_ago: i64) -> Dynamic {
        self.indicator_value_at("ema", period, bars_ago)
    }
    pub fn rsi_at(&mut self, period: i64, bars_ago: i64) -> Dynamic {
        self.indicator_value_at("rsi", period, bars_ago)
    }
    pub fn indicator_at(&mut self, name: String, period: i64, bars_ago: i64) -> Dynamic {
        self.indicator_value_at(&name, period, bars_ago)
    }

    // --- Crossover helpers ---
    pub fn crossed_above(&mut self, ind_a: String, ind_b: String) -> bool {
        let (name_a, period_a) = parse_indicator_ref(&ind_a);
        let (name_b, period_b) = parse_indicator_ref(&ind_b);
        let a_now = self.indicator_value(&name_a, period_a);
        let b_now = self.indicator_value(&name_b, period_b);
        let a_prev = self.indicator_value_at(&name_a, period_a, 1);
        let b_prev = self.indicator_value_at(&name_b, period_b, 1);

        match (
            a_now.as_float(),
            b_now.as_float(),
            a_prev.as_float(),
            b_prev.as_float(),
        ) {
            (Ok(a), Ok(b), Ok(ap), Ok(bp)) => a > b && ap <= bp,
            _ => false,
        }
    }

    pub fn crossed_below(&mut self, ind_a: String, ind_b: String) -> bool {
        let (name_a, period_a) = parse_indicator_ref(&ind_a);
        let (name_b, period_b) = parse_indicator_ref(&ind_b);
        let a_now = self.indicator_value(&name_a, period_a);
        let b_now = self.indicator_value(&name_b, period_b);
        let a_prev = self.indicator_value_at(&name_a, period_a, 1);
        let b_prev = self.indicator_value_at(&name_b, period_b, 1);

        match (
            a_now.as_float(),
            b_now.as_float(),
            a_prev.as_float(),
            b_prev.as_float(),
        ) {
            (Ok(a), Ok(b), Ok(ap), Ok(bp)) => a < b && ap >= bp,
            _ => false,
        }
    }

    // --- Multi-param indicator overloads ---

    fn indicator_value_multi(&self, name: &str, params: &[i64]) -> Dynamic {
        use crate::scripting::indicators::{IndicatorKey, IndicatorParam};
        let key = IndicatorKey {
            name: name.to_string(),
            params: params.iter().map(|&p| IndicatorParam::Int(p)).collect(),
        };
        match self.indicator_store.get(&key, self.bar_idx) {
            Some(v) if v.is_nan() => Dynamic::UNIT,
            Some(v) => Dynamic::from(v),
            None => Dynamic::UNIT,
        }
    }

    pub fn macd_line_custom(&mut self, fast: i64, slow: i64, signal: i64) -> Dynamic {
        self.indicator_value_multi("macd_line", &[fast, slow, signal])
    }
    pub fn macd_signal_custom(&mut self, fast: i64, slow: i64, signal: i64) -> Dynamic {
        self.indicator_value_multi("macd_signal", &[fast, slow, signal])
    }
    pub fn macd_hist_custom(&mut self, fast: i64, slow: i64, signal: i64) -> Dynamic {
        self.indicator_value_multi("macd_hist", &[fast, slow, signal])
    }
    pub fn bbands_upper_custom(&mut self, period: i64, std_dev: f64) -> Dynamic {
        // Store std_dev * 10 as integer for hashing
        self.indicator_value_multi("bbands_upper", &[period, (std_dev * 10.0) as i64])
    }
    pub fn bbands_mid_custom(&mut self, period: i64, _std_dev: f64) -> Dynamic {
        self.indicator_value_multi("bbands_mid", &[period])
    }
    pub fn bbands_lower_custom(&mut self, period: i64, std_dev: f64) -> Dynamic {
        self.indicator_value_multi("bbands_lower", &[period, (std_dev * 10.0) as i64])
    }
    pub fn stochastic_custom(&mut self, k_period: i64, d_smoothing: i64) -> Dynamic {
        self.indicator_value_multi("stochastic", &[k_period, d_smoothing])
    }

    // --- Options chain ---

    /// Build an options strategy from an array of leg specifications.
    /// Each leg: `#{ side: "short", option_type: "put", delta: 0.30, dte: 45 }`
    /// Returns `#{ legs: [...], net_premium }` or `()` if any leg can't be filled.
    ///
    /// Works for any structure — single legs, spreads, condors, butterflies, etc.
    pub fn build_strategy(&mut self, legs: rhai::Array) -> Dynamic {
        let mut resolved_legs = Vec::new();
        let mut net_premium = 0.0;

        for leg_dyn in legs {
            let Some(leg) = leg_dyn.try_cast::<rhai::Map>() else {
                return Dynamic::UNIT;
            };

            let opt_type = leg
                .get("option_type")
                .and_then(|v| v.clone().into_immutable_string().ok())
                .unwrap_or_default()
                .to_string();
            let delta = leg
                .get("delta")
                .and_then(|v| v.as_float().ok())
                .unwrap_or(0.30);
            let dte = leg.get("dte").and_then(|v| v.as_int().ok()).unwrap_or(45);
            let side = leg
                .get("side")
                .and_then(|v| v.clone().into_immutable_string().ok())
                .unwrap_or_default()
                .to_string();

            // Resolve this leg to a specific contract
            let target = crate::engine::types::TargetRange {
                target: delta,
                min: (delta - 0.10).max(0.01),
                max: (delta + 0.10).min(1.0),
            };
            let found = self.resolve_leg(
                &opt_type,
                &target,
                dte as i32,
                (dte - 15).max(1) as i32,
                (dte + 15) as i32,
            );
            if found.is_unit() {
                return Dynamic::UNIT; // any failed leg -> entire strategy fails
            }

            let found_map = found.clone().cast::<rhai::Map>();

            // Compute premium contribution
            let bid = found_map
                .get("bid")
                .and_then(|v| v.as_float().ok())
                .unwrap_or(0.0);
            let ask = found_map
                .get("ask")
                .and_then(|v| v.as_float().ok())
                .unwrap_or(0.0);
            let mid = f64::midpoint(bid, ask);

            match side.as_str() {
                "short" => net_premium -= mid,
                "long" => net_premium += mid,
                _ => {}
            }

            // Build resolved leg with side and type
            let mut leg_map = found_map;
            leg_map.insert("side".into(), Dynamic::from(side));
            leg_map.insert("option_type".into(), Dynamic::from(opt_type));
            resolved_legs.push(Dynamic::from(leg_map));
        }

        let mut result = rhai::Map::new();
        result.insert("legs".into(), Dynamic::from(resolved_legs));
        result.insert("net_premium".into(), Dynamic::from(net_premium));
        Dynamic::from(result)
    }

    /// Resolve a single leg to a specific contract via the filter pipeline.
    fn resolve_leg(
        &self,
        option_type: &str,
        target: &crate::engine::types::TargetRange,
        _dte_target: i32,
        dte_min: i32,
        dte_max: i32,
    ) -> Dynamic {
        use crate::engine::filters;

        let today = self.datetime.date();

        // O(1) lookup: get today's options slice (~5K-10K rows)
        let today_df = match &self.options_by_date {
            Some(opts) => match opts.get(today) {
                Some(df) => df,
                None => return Dynamic::UNIT,
            },
            None => return Dynamic::UNIT,
        };

        let min_bid_ask = 0.05;

        let opt_type_code = match option_type.to_lowercase().as_str() {
            "call" | "c" => "c",
            "put" | "p" => "p",
            _ => return Dynamic::UNIT,
        };

        // Filter the small daily slice by type, DTE, quotes (clone since we borrow from cache)
        let filtered = match filters::filter_leg_candidates(
            today_df.clone(),
            opt_type_code,
            dte_max,
            dte_min,
            min_bid_ask,
        ) {
            Ok(f) if f.height() > 0 => f,
            _ => return Dynamic::UNIT,
        };

        // Select closest delta
        let selected = match filters::select_closest_delta(filtered, target) {
            Ok(s) if s.height() > 0 => s,
            _ => return Dynamic::UNIT,
        };

        row_to_option_map(&selected, 0, today)
    }
    // --- Cross-symbol ---
    pub fn price_of(&mut self, symbol: String) -> Dynamic {
        self.cross_symbol_data
            .get(&symbol)
            .and_then(|bars| bars.get(self.bar_idx))
            .map(|b| Dynamic::from(b.close))
            .unwrap_or(Dynamic::UNIT)
    }

    pub fn price_of_col(&mut self, symbol: String, col: String) -> Dynamic {
        self.cross_symbol_data
            .get(&symbol)
            .and_then(|bars| bars.get(self.bar_idx))
            .map(|b| match col.as_str() {
                "open" => Dynamic::from(b.open),
                "high" => Dynamic::from(b.high),
                "low" => Dynamic::from(b.low),
                "close" => Dynamic::from(b.close),
                "volume" => Dynamic::from(b.volume),
                _ => Dynamic::UNIT,
            })
            .unwrap_or(Dynamic::UNIT)
    }

    // --- Position sizing (see helpers.rs for implementations) ---

    // --- Historical bar lookback (MQL4-inspired) ---

    /// High price N bars ago (0 = current bar). Returns `()` if out of range.
    pub fn high_at(&mut self, n: i64) -> Dynamic {
        if n < 0 {
            return Dynamic::UNIT;
        }
        let n = n as usize;
        if n > self.bar_idx {
            return Dynamic::UNIT;
        }
        self.price_history
            .get(self.bar_idx - n)
            .map(|b| Dynamic::from(b.high))
            .unwrap_or(Dynamic::UNIT)
    }

    /// Low price N bars ago (0 = current bar). Returns `()` if out of range.
    pub fn low_at(&mut self, n: i64) -> Dynamic {
        if n < 0 {
            return Dynamic::UNIT;
        }
        let n = n as usize;
        if n > self.bar_idx {
            return Dynamic::UNIT;
        }
        self.price_history
            .get(self.bar_idx - n)
            .map(|b| Dynamic::from(b.low))
            .unwrap_or(Dynamic::UNIT)
    }

    /// Open price N bars ago (0 = current bar). Returns `()` if out of range.
    pub fn open_at(&mut self, n: i64) -> Dynamic {
        if n < 0 {
            return Dynamic::UNIT;
        }
        let n = n as usize;
        if n > self.bar_idx {
            return Dynamic::UNIT;
        }
        self.price_history
            .get(self.bar_idx - n)
            .map(|b| Dynamic::from(b.open))
            .unwrap_or(Dynamic::UNIT)
    }

    /// Close price N bars ago (0 = current bar). Returns `()` if out of range.
    pub fn close_at(&mut self, n: i64) -> Dynamic {
        if n < 0 {
            return Dynamic::UNIT;
        }
        let n = n as usize;
        if n > self.bar_idx {
            return Dynamic::UNIT;
        }
        self.price_history
            .get(self.bar_idx - n)
            .map(|b| Dynamic::from(b.close))
            .unwrap_or(Dynamic::UNIT)
    }

    /// Volume N bars ago (0 = current bar). Returns `()` if out of range.
    pub fn volume_at(&mut self, n: i64) -> Dynamic {
        if n < 0 {
            return Dynamic::UNIT;
        }
        let n = n as usize;
        if n > self.bar_idx {
            return Dynamic::UNIT;
        }
        self.price_history
            .get(self.bar_idx - n)
            .map(|b| Dynamic::from(b.volume))
            .unwrap_or(Dynamic::UNIT)
    }

    // --- Range queries (MQL4-inspired iHighest/iLowest) ---

    /// Maximum high over the last `period` bars (including current). Returns 0.0 if period <= 0.
    pub fn highest_high(&mut self, period: i64) -> f64 {
        if period <= 0 {
            return 0.0;
        }
        let period = period as usize;
        let start = self.bar_idx.saturating_sub(period - 1);
        self.price_history[start..=self.bar_idx]
            .iter()
            .map(|b| b.high)
            .fold(f64::NEG_INFINITY, f64::max)
    }

    /// Minimum low over the last `period` bars (including current). Returns 0.0 if period <= 0.
    pub fn lowest_low(&mut self, period: i64) -> f64 {
        if period <= 0 {
            return 0.0;
        }
        let period = period as usize;
        let start = self.bar_idx.saturating_sub(period - 1);
        self.price_history[start..=self.bar_idx]
            .iter()
            .map(|b| b.low)
            .fold(f64::INFINITY, f64::min)
    }

    /// Maximum close over the last `period` bars (including current). Returns 0.0 if period <= 0.
    pub fn highest_close(&mut self, period: i64) -> f64 {
        if period <= 0 {
            return 0.0;
        }
        let period = period as usize;
        let start = self.bar_idx.saturating_sub(period - 1);
        self.price_history[start..=self.bar_idx]
            .iter()
            .map(|b| b.close)
            .fold(f64::NEG_INFINITY, f64::max)
    }

    /// Minimum close over the last `period` bars (including current). Returns 0.0 if period <= 0.
    pub fn lowest_close(&mut self, period: i64) -> f64 {
        if period <= 0 {
            return 0.0;
        }
        let period = period as usize;
        let start = self.bar_idx.saturating_sub(period - 1);
        self.price_history[start..=self.bar_idx]
            .iter()
            .map(|b| b.close)
            .fold(f64::INFINITY, f64::min)
    }

    // --- Portfolio methods ---

    /// Sum of unrealized P&L across all open positions.
    pub fn get_unrealized_pnl(&mut self) -> f64 {
        self.positions.iter().map(|p| p.unrealized_pnl).sum()
    }

    /// Realized P&L = equity - starting capital.
    /// `equity` in the engine is realized equity (cash + realized gains).
    pub fn get_realized_pnl(&mut self) -> f64 {
        self.equity - self.config.capital
    }

    /// Total exposure = sum of abs(entry_cost) across all open positions.
    pub fn get_total_exposure(&mut self) -> f64 {
        self.positions.iter().map(|p| p.entry_cost.abs()).sum()
    }

    // --- Custom series plotting ---

    /// Emit a custom value for charting. Defaults to overlay display.
    ///
    /// Called from Rhai as `ctx.plot("entry_threshold", sma * 1.04)`.
    pub fn plot(&mut self, name: String, value: f64) {
        let mut store = self.custom_series.lock().unwrap_or_else(|e| e.into_inner());
        // Reject new series beyond the cap to prevent memory DoS
        if !store.series.contains_key(&name) && store.series.len() >= MAX_CUSTOM_SERIES {
            return;
        }
        let num_bars = store.num_bars;
        let series = store
            .series
            .entry(name)
            .or_insert_with(|| vec![None; num_bars]);
        if self.bar_idx < series.len() {
            series[self.bar_idx] = if value.is_finite() { Some(value) } else { None };
        }
    }

    /// Emit a custom value with an explicit display type ("overlay" or "subchart").
    ///
    /// Called from Rhai as `ctx.plot_with("my_rsi", value, "subchart")`.
    pub fn plot_with(&mut self, name: String, value: f64, display: String) {
        let mut store = self.custom_series.lock().unwrap_or_else(|e| e.into_inner());
        // Reject new series beyond the cap to prevent memory DoS
        if !store.series.contains_key(&name) && store.series.len() >= MAX_CUSTOM_SERIES {
            return;
        }
        store.display_types.entry(name.clone()).or_insert(display);
        let num_bars = store.num_bars;
        let series = store
            .series
            .entry(name)
            .or_insert_with(|| vec![None; num_bars]);
        if self.bar_idx < series.len() {
            series[self.bar_idx] = if value.is_finite() { Some(value) } else { None };
        }
    }
}

/// Parse "sma:20" into ("sma", 20).
fn parse_indicator_ref(s: &str) -> (String, i64) {
    let parts: Vec<&str> = s.split(':').collect();
    let name = parts[0].to_string();
    let period = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
    (name, period)
}

// ---------------------------------------------------------------------------
// Options chain helpers
// ---------------------------------------------------------------------------

/// Convert a DataFrame row to a Rhai Map for find_option results.
/// Returns `#{ strike, bid, ask, delta, expiration, dte }` or `()`.
fn row_to_option_map(df: &polars::prelude::DataFrame, row: usize, today: NaiveDate) -> Dynamic {
    use polars::prelude::*;

    let get_f64 =
        |col_name: &str| -> Option<f64> { df.column(col_name).ok()?.f64().ok()?.get(row) };

    let Some(strike) = get_f64("strike") else {
        return Dynamic::UNIT;
    };
    let bid = get_f64("bid").unwrap_or(0.0);
    let ask = get_f64("ask").unwrap_or(0.0);
    let delta = get_f64("delta").unwrap_or(0.0);

    // Get expiration date — handle both Date and Datetime column types
    let expiration: Option<NaiveDate> = df.column("expiration").ok().and_then(|c| {
        // Try as Date first (physical i32 = days since epoch)
        if let Ok(date_ca) = c.date() {
            let phys = date_ca
                .clone()
                .into_series()
                .to_physical_repr()
                .into_owned();
            let physical = phys.i32().ok()?;
            let epoch_days = physical.get(row)?;
            return NaiveDate::from_num_days_from_ce_opt(
                epoch_days + crate::engine::types::EPOCH_DAYS_CE_OFFSET,
            );
        }
        // Try as Datetime (physical i64 = microseconds since epoch)
        if let Ok(dt_ca) = c.datetime() {
            let phys = dt_ca.clone().into_series().to_physical_repr().into_owned();
            let physical = phys.i64().ok()?;
            let us = physical.get(row)?;
            let secs = us / 1_000_000;
            let nsecs = ((us % 1_000_000) * 1000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nsecs)?;
            return Some(dt.date_naive());
        }
        None
    });

    let Some(expiration) = expiration else {
        return Dynamic::UNIT;
    };

    let dte = (expiration - today).num_days();

    let mut map = rhai::Map::new();
    map.insert("strike".into(), Dynamic::from(strike));
    map.insert("bid".into(), Dynamic::from(bid));
    map.insert("ask".into(), Dynamic::from(ask));
    map.insert("delta".into(), Dynamic::from(delta));
    map.insert("expiration".into(), Dynamic::from(expiration.to_string()));
    map.insert("dte".into(), Dynamic::from(dte));
    Dynamic::from(map)
}

/// Extract the expiration date from a DataFrame row.
pub(in crate::scripting) fn row_to_expiration_date(
    df: &polars::prelude::DataFrame,
    row: usize,
) -> Option<NaiveDate> {
    use polars::prelude::*;

    let col = df.column("expiration").ok()?;
    // Try Date (physical i32)
    if let Ok(date_ca) = col.date() {
        let phys = date_ca
            .clone()
            .into_series()
            .to_physical_repr()
            .into_owned();
        let physical = phys.i32().ok()?;
        let epoch_days = physical.get(row)?;
        return NaiveDate::from_num_days_from_ce_opt(
            epoch_days + crate::engine::types::EPOCH_DAYS_CE_OFFSET,
        );
    }
    // Try Datetime (physical i64)
    if let Ok(dt_ca) = col.datetime() {
        let series = dt_ca.clone().into_series();
        let physical = series.i64().ok()?;
        let us = physical.get(row)?;
        let secs = us / 1_000_000;
        let nsecs = ((us % 1_000_000) * 1000) as u32;
        let dt = chrono::DateTime::from_timestamp(secs, nsecs)?;
        return Some(dt.date_naive());
    }
    None
}
