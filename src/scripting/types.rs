//! Types for the Rhai scripting engine.
//!
//! Defines `BarContext` (exposed to scripts as `ctx`), `ScriptPosition` (exposed as `pos`),
//! `ScriptConfig` (parsed from `config()` return), and action enums for processing
//! script commands.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{NaiveDate, NaiveDateTime};

use crate::engine::types::{
    Commission, ExpirationFilter, OptionType, Side, Slippage, TradeSelector,
};

use super::indicators::IndicatorStore;

// ---------------------------------------------------------------------------
// ScriptConfig — parsed from the Rhai config() callback return value
// ---------------------------------------------------------------------------

/// Configuration extracted from a script's `config()` callback.
#[derive(Debug, Clone)]
pub struct ScriptConfig {
    pub symbol: String,
    pub capital: f64,
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
    pub interval: Interval,
    pub multiplier: i32,
    pub timeout_secs: u64,
    pub auto_close_on_end: bool,

    // Data requirements
    pub needs_ohlcv: bool,
    pub needs_options: bool,
    pub cross_symbols: Vec<String>,
    pub declared_indicators: Vec<String>,

    // Engine-enforced settings
    pub slippage: Slippage,
    pub commission: Option<Commission>,
    pub min_days_between_entries: Option<i32>,
    pub expiration_filter: ExpirationFilter,
    pub trade_selector: TradeSelector,

    // Script-readable defaults (NOT engine-enforced)
    pub defaults: HashMap<String, ScriptValue>,
}

/// Interval for bar iteration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Interval {
    Daily,
    Intraday(IntradayInterval),
}

/// Intraday bar sizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntradayInterval {
    Min1,
    Min5,
    Min10,
    Min15,
    Min30,
    Hour1,
    Hour2,
    Hour4,
}

impl Interval {
    /// Trading bars per year, used for annualized metrics (Sharpe, CAGR, etc.).
    #[must_use]
    pub fn bars_per_year(self) -> f64 {
        match self {
            Self::Daily => 252.0,
            Self::Intraday(intra) => match intra {
                IntradayInterval::Min1 => 252.0 * 390.0,
                IntradayInterval::Min5 => 252.0 * 78.0,
                IntradayInterval::Min10 => 252.0 * 39.0,
                IntradayInterval::Min15 => 252.0 * 26.0,
                IntradayInterval::Min30 => 252.0 * 13.0,
                IntradayInterval::Hour1 => 252.0 * 6.5,
                IntradayInterval::Hour2 => 252.0 * 3.25,
                IntradayInterval::Hour4 => 252.0 * 1.625,
            },
        }
    }

    /// Parse from a string like "daily", "1m", "5m", "1h", etc.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "daily" | "1d" => Some(Self::Daily),
            "1m" | "1min" => Some(Self::Intraday(IntradayInterval::Min1)),
            "5m" | "5min" => Some(Self::Intraday(IntradayInterval::Min5)),
            "10m" | "10min" => Some(Self::Intraday(IntradayInterval::Min10)),
            "15m" | "15min" => Some(Self::Intraday(IntradayInterval::Min15)),
            "30m" | "30min" => Some(Self::Intraday(IntradayInterval::Min30)),
            "1h" | "60m" => Some(Self::Intraday(IntradayInterval::Hour1)),
            "2h" => Some(Self::Intraday(IntradayInterval::Hour2)),
            "4h" => Some(Self::Intraday(IntradayInterval::Hour4)),
            _ => None,
        }
    }
}

/// A loosely-typed value for script-readable defaults.
#[derive(Debug, Clone)]
pub enum ScriptValue {
    Float(f64),
    Int(i64),
    String(String),
    Bool(bool),
    None,
}

// ---------------------------------------------------------------------------
// BarContext — the `ctx` object exposed to Rhai scripts
// ---------------------------------------------------------------------------

/// Per-bar context object exposed to Rhai scripts as `ctx`.
///
/// Rebuilt each bar (or between Phase A and Phase B within a bar).
/// Contains immutable data references and a snapshot of portfolio state.
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
    pub positions: Vec<ScriptPosition>,

    // Shared data (Arc for cheap cloning into Rhai)
    pub indicator_store: Arc<IndicatorStore>,
    pub price_history: Arc<Vec<OhlcvBar>>,
    pub cross_symbol_data: Arc<HashMap<String, Vec<CrossSymbolBar>>>,

    // Options data (None for pure stock backtests)
    pub options_df: Option<Arc<polars::prelude::DataFrame>>,

    // Config reference for ctx.config.defaults access
    pub config: Arc<ScriptConfig>,
}

/// A single OHLCV bar.
#[derive(Debug, Clone)]
pub struct OhlcvBar {
    pub datetime: NaiveDateTime,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

/// Cross-symbol bar data (forward-filled to primary timeline).
#[derive(Debug, Clone)]
pub struct CrossSymbolBar {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

// ---------------------------------------------------------------------------
// ScriptPosition — exposed to Rhai scripts as `pos`
// ---------------------------------------------------------------------------

/// Position object exposed to Rhai scripts.
#[derive(Debug, Clone)]
pub struct ScriptPosition {
    pub id: usize,
    pub entry_date: NaiveDate,
    pub inner: ScriptPositionInner,
    pub entry_cost: f64,
    pub unrealized_pnl: f64,
    pub days_held: i64,
    /// Current simulation date — used by `get_dte()` to compute days to expiration.
    pub current_date: NaiveDate,
    /// `"script"` for positions opened by the script, `"assignment"` for
    /// positions auto-created by the engine on ITM put expiration.
    pub source: String,
    /// Whether this is an implicit position (from assignment) that does NOT
    /// count toward `max_positions`.
    pub implicit: bool,
}

/// The inner variant: options (multi-leg) or stock (single holding).
#[derive(Debug, Clone)]
pub enum ScriptPositionInner {
    Options {
        legs: Vec<ScriptPositionLeg>,
        expiration: NaiveDate,
        secondary_expiration: Option<NaiveDate>,
        multiplier: i32,
    },
    Stock {
        side: Side,
        qty: i32,
        entry_price: f64,
    },
}

/// A single leg of an options position, exposed to scripts.
#[derive(Debug, Clone)]
pub struct ScriptPositionLeg {
    pub strike: f64,
    pub option_type: OptionType,
    pub side: Side,
    pub expiration: NaiveDate,
    pub entry_price: f64,
    pub current_price: f64,
    pub delta: f64,
    pub qty: i32,
}

impl ScriptPosition {
    /// Days to expiration for options positions; `None` for stock.
    #[must_use]
    pub fn dte(&self, today: NaiveDate) -> Option<i64> {
        match &self.inner {
            ScriptPositionInner::Options { expiration, .. } => {
                Some((*expiration - today).num_days())
            }
            ScriptPositionInner::Stock { .. } => None,
        }
    }

    /// P&L as a fraction of absolute entry cost.
    #[must_use]
    pub fn pnl_pct(&self) -> f64 {
        let abs_cost = self.entry_cost.abs();
        if abs_cost < f64::EPSILON {
            0.0
        } else {
            self.unrealized_pnl / abs_cost
        }
    }

    #[must_use]
    pub fn is_options(&self) -> bool {
        matches!(self.inner, ScriptPositionInner::Options { .. })
    }

    #[must_use]
    pub fn is_stock(&self) -> bool {
        matches!(self.inner, ScriptPositionInner::Stock { .. })
    }
}

// ---------------------------------------------------------------------------
// ScriptPosition — Rhai getter methods
// ---------------------------------------------------------------------------

impl ScriptPosition {
    pub fn get_id(&mut self) -> i64 {
        self.id as i64
    }
    pub fn get_entry_date(&mut self) -> String {
        self.entry_date.to_string()
    }
    pub fn get_expiration(&mut self) -> Dynamic {
        match &self.inner {
            ScriptPositionInner::Options { expiration, .. } => {
                Dynamic::from(expiration.to_string())
            }
            ScriptPositionInner::Stock { .. } => Dynamic::UNIT,
        }
    }
    pub fn get_dte(&mut self) -> Dynamic {
        match self.dte(self.current_date) {
            Some(days) => Dynamic::from(days),
            None => Dynamic::UNIT,
        }
    }
    pub fn get_entry_cost(&mut self) -> f64 {
        self.entry_cost
    }
    pub fn get_unrealized_pnl(&mut self) -> f64 {
        self.unrealized_pnl
    }
    pub fn get_pnl_pct(&mut self) -> f64 {
        self.pnl_pct()
    }
    pub fn get_days_held(&mut self) -> i64 {
        self.days_held
    }
    pub fn get_legs(&mut self) -> Dynamic {
        match &self.inner {
            ScriptPositionInner::Options { legs, .. } => {
                let arr: rhai::Array = legs
                    .iter()
                    .map(|leg| {
                        let mut map = rhai::Map::new();
                        map.insert("strike".into(), Dynamic::from(leg.strike));
                        map.insert(
                            "option_type".into(),
                            Dynamic::from(format!("{:?}", leg.option_type).to_lowercase()),
                        );
                        map.insert(
                            "side".into(),
                            Dynamic::from(match leg.side {
                                Side::Long => "long",
                                Side::Short => "short",
                            }),
                        );
                        map.insert(
                            "expiration".into(),
                            Dynamic::from(leg.expiration.to_string()),
                        );
                        map.insert("entry_price".into(), Dynamic::from(leg.entry_price));
                        map.insert("current_price".into(), Dynamic::from(leg.current_price));
                        map.insert("delta".into(), Dynamic::from(leg.delta));
                        map.insert("qty".into(), Dynamic::from(leg.qty as i64));
                        Dynamic::from(map)
                    })
                    .collect();
                Dynamic::from(arr)
            }
            ScriptPositionInner::Stock { .. } => Dynamic::UNIT,
        }
    }
    pub fn get_side(&mut self) -> Dynamic {
        match &self.inner {
            ScriptPositionInner::Stock { side, .. } => Dynamic::from(match side {
                Side::Long => "long",
                Side::Short => "short",
            }),
            ScriptPositionInner::Options { .. } => Dynamic::UNIT,
        }
    }
    pub fn get_is_options(&mut self) -> bool {
        self.is_options()
    }
    pub fn get_is_stock(&mut self) -> bool {
        self.is_stock()
    }
    pub fn get_source(&mut self) -> String {
        self.source.clone()
    }
}

// ---------------------------------------------------------------------------
// BarContext — Rhai getter and method implementations
// ---------------------------------------------------------------------------

use rhai::Dynamic;

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
    fn indicator_value(&self, name: &str, period: i64) -> Dynamic {
        use super::indicators::{IndicatorKey, IndicatorParam};
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
        use super::indicators::{IndicatorKey, IndicatorParam};
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
        self.indicator_value("macd_line", 0)
    }
    pub fn macd_signal(&mut self) -> Dynamic {
        self.indicator_value("macd_signal", 0)
    }
    pub fn macd_hist(&mut self) -> Dynamic {
        self.indicator_value("macd_hist", 0)
    }
    pub fn bbands_upper(&mut self, period: i64) -> Dynamic {
        self.indicator_value("bbands_upper", period)
    }
    pub fn bbands_mid(&mut self, period: i64) -> Dynamic {
        self.indicator_value("bbands_mid", period)
    }
    pub fn bbands_lower(&mut self, period: i64) -> Dynamic {
        self.indicator_value("bbands_lower", period)
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
        self.indicator_value("obv", 0)
    }

    // --- Generic indicator accessor ---
    pub fn indicator(&mut self, name: String, period: i64) -> Dynamic {
        self.indicator_value(&name, period)
    }
    pub fn indicator_with(&mut self, _name: String, _params: rhai::Map) -> Dynamic {
        // TODO: multi-param indicator lookup
        Dynamic::UNIT
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
        use super::indicators::{IndicatorKey, IndicatorParam};
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

    /// Find a single option contract matching the given criteria.
    /// Simple form: scalar delta target with sensible defaults (±0.10 delta range, ±15 DTE range).
    /// Returns a Map with { strike, bid, ask, delta, expiration, dte } or () if not found.
    pub fn find_option(&mut self, option_type: String, delta: f64, dte: i64) -> Dynamic {
        use crate::engine::types::TargetRange;
        let target = TargetRange {
            target: delta,
            min: (delta - 0.10).max(0.01),
            max: (delta + 0.10).min(1.0),
        };
        self.find_option_internal(
            &option_type,
            &target,
            dte as i32,
            (dte - 15).max(1) as i32,
            (dte + 15) as i32,
        )
    }

    /// Find a single option contract with full TargetRange/DteRange control.
    /// delta_range: #{ target: 0.30, min: 0.20, max: 0.40 }
    /// dte_range: #{ target: 45, min: 30, max: 60 }
    pub fn find_option_with(
        &mut self,
        option_type: String,
        delta_range: rhai::Map,
        dte_range: rhai::Map,
    ) -> Dynamic {
        use crate::engine::types::TargetRange;
        let target = TargetRange {
            target: delta_range
                .get("target")
                .and_then(|v| v.as_float().ok())
                .unwrap_or(0.30),
            min: delta_range
                .get("min")
                .and_then(|v| v.as_float().ok())
                .unwrap_or(0.20),
            max: delta_range
                .get("max")
                .and_then(|v| v.as_float().ok())
                .unwrap_or(0.40),
        };
        let dte_target = dte_range
            .get("target")
            .and_then(|v| v.as_int().ok())
            .unwrap_or(45) as i32;
        let dte_min = dte_range
            .get("min")
            .and_then(|v| v.as_int().ok())
            .unwrap_or(30) as i32;
        let dte_max = dte_range
            .get("max")
            .and_then(|v| v.as_int().ok())
            .unwrap_or(60) as i32;
        self.find_option_internal(&option_type, &target, dte_target, dte_min, dte_max)
    }

    /// Internal: run the filter pipeline and return the best match for the current date.
    fn find_option_internal(
        &self,
        option_type: &str,
        target: &crate::engine::types::TargetRange,
        _dte_target: i32,
        dte_min: i32,
        dte_max: i32,
    ) -> Dynamic {
        use crate::engine::filters;

        let df = match &self.options_df {
            Some(df) => df.as_ref(),
            None => return Dynamic::UNIT, // no options data loaded
        };

        let today = self.datetime.date();
        let min_bid_ask = 0.05;

        // Map option_type string to the code used in data ("c" or "p")
        let opt_type_code = match option_type.to_lowercase().as_str() {
            "call" | "c" => "c",
            "put" | "p" => "p",
            _ => return Dynamic::UNIT,
        };

        // 1. Filter by option type, DTE range, valid quotes
        let filtered = match filters::filter_leg_candidates(
            df,
            opt_type_code,
            dte_max,
            dte_min,
            min_bid_ask,
        ) {
            Ok(f) if f.height() > 0 => f,
            _ => return Dynamic::UNIT,
        };

        // 2. Filter to current date only
        let today_filtered = match filter_to_date(&filtered, today) {
            Some(f) if f.height() > 0 => f,
            _ => return Dynamic::UNIT,
        };

        // 3. Select closest delta
        let selected = match filters::select_closest_delta(today_filtered, target) {
            Ok(s) if s.height() > 0 => s,
            _ => return Dynamic::UNIT,
        };

        // 4. Pick the first (closest) match
        row_to_option_map(&selected, 0, today)
    }
    /// Find a multi-leg spread by resolving each leg independently.
    /// Input: array of leg maps with { side, option_type, delta, dte }.
    /// Returns a Map with { legs: [...], net_premium, expiration } or () if any leg fails.
    pub fn find_spread(&mut self, legs: rhai::Array) -> Dynamic {
        self.find_spread_internal(legs, None)
    }

    /// Find a multi-leg spread with additional filters (min/max net premium/delta).
    pub fn find_spread_with(&mut self, legs: rhai::Array, filters: rhai::Map) -> Dynamic {
        self.find_spread_internal(legs, Some(filters))
    }

    fn find_spread_internal(&mut self, legs: rhai::Array, filters: Option<rhai::Map>) -> Dynamic {
        let _today = self.datetime.date();
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

            // Find this leg
            let found = self.find_option(opt_type.clone(), delta, dte);
            if found.is_unit() {
                return Dynamic::UNIT; // any failed leg → entire spread fails
            }

            let found_map = found.clone().cast::<rhai::Map>();

            // Get side from the leg spec
            let side = leg
                .get("side")
                .and_then(|v| v.clone().into_immutable_string().ok())
                .unwrap_or_default()
                .to_string();

            // Compute premium contribution
            let bid = found_map
                .get("bid")
                .and_then(|v| v.as_float().ok())
                .unwrap_or(0.0);
            let ask = found_map
                .get("ask")
                .and_then(|v| v.as_float().ok())
                .unwrap_or(0.0);
            let mid = (bid + ask) / 2.0;

            match side.as_str() {
                "short" => net_premium -= mid, // credit received
                "long" => net_premium += mid,  // debit paid
                _ => {}
            }

            // Build leg with side added
            let mut leg_map = found_map;
            leg_map.insert("side".into(), Dynamic::from(side));
            leg_map.insert("option_type".into(), Dynamic::from(opt_type));
            resolved_legs.push(Dynamic::from(leg_map));
        }

        // Apply filters
        if let Some(filters) = filters {
            if let Some(min) = filters
                .get("min_net_premium")
                .and_then(|v| v.as_float().ok())
            {
                if net_premium.abs() < min {
                    return Dynamic::UNIT;
                }
            }
            if let Some(max) = filters
                .get("max_net_premium")
                .and_then(|v| v.as_float().ok())
            {
                if net_premium.abs() > max {
                    return Dynamic::UNIT;
                }
            }
        }

        let mut result = rhai::Map::new();
        result.insert("legs".into(), Dynamic::from(resolved_legs));
        result.insert("net_premium".into(), Dynamic::from(net_premium));
        Dynamic::from(result)
    }

    // --- Strategy builders ---
    // Each resolves to a find_spread call with pre-configured legs.

    /// Iron condor with explicit per-leg deltas.
    /// `ctx.iron_condor(#{ long_put: 0.10, short_put: 0.25, short_call: 0.35, long_call: 0.10, dte: 45 })`
    pub fn iron_condor(&mut self, params: rhai::Map) -> Dynamic {
        let short_put = params
            .get("short_put")
            .and_then(|v| v.as_float().ok())
            .unwrap_or(0.30);
        let long_put = params
            .get("long_put")
            .and_then(|v| v.as_float().ok())
            .unwrap_or(0.10);
        let short_call = params
            .get("short_call")
            .and_then(|v| v.as_float().ok())
            .unwrap_or(0.30);
        let long_call = params
            .get("long_call")
            .and_then(|v| v.as_float().ok())
            .unwrap_or(0.10);
        let dte = params
            .get("dte")
            .and_then(|v| v.as_int().ok())
            .unwrap_or(45);

        let legs: rhai::Array = vec![
            leg_map("long", "put", long_put, dte),
            leg_map("short", "put", short_put, dte),
            leg_map("short", "call", short_call, dte),
            leg_map("long", "call", long_call, dte),
        ];
        self.find_spread(legs)
    }

    /// Vertical spread (bull put, bear call, etc.)
    /// `ctx.vertical_spread(#{ type: "put", short: 0.30, long: 0.15, dte: 45 })`
    pub fn vertical_spread(&mut self, params: rhai::Map) -> Dynamic {
        let opt_type = params
            .get("type")
            .and_then(|v| v.clone().into_immutable_string().ok())
            .unwrap_or_default();
        let short_delta = params
            .get("short")
            .and_then(|v| v.as_float().ok())
            .unwrap_or(0.30);
        let long_delta = params
            .get("long")
            .and_then(|v| v.as_float().ok())
            .unwrap_or(0.15);
        let dte = params
            .get("dte")
            .and_then(|v| v.as_int().ok())
            .unwrap_or(45);

        let ot = opt_type.as_str();
        let legs: rhai::Array = vec![
            leg_map("short", ot, short_delta, dte),
            leg_map("long", ot, long_delta, dte),
        ];
        self.find_spread(legs)
    }

    /// Butterfly spread.
    /// `ctx.butterfly(#{ type: "call", short: 0.50, long: 0.25, dte: 45 })`
    /// Short legs are the body (ATM), long legs are the wings (OTM).
    pub fn butterfly(&mut self, params: rhai::Map) -> Dynamic {
        let opt_type = params
            .get("type")
            .and_then(|v| v.clone().into_immutable_string().ok())
            .unwrap_or_default();
        let short_delta = params
            .get("short")
            .and_then(|v| v.as_float().ok())
            .unwrap_or(0.50);
        let long_delta = params
            .get("long")
            .and_then(|v| v.as_float().ok())
            .unwrap_or(0.25);
        let dte = params
            .get("dte")
            .and_then(|v| v.as_int().ok())
            .unwrap_or(45);

        let ot = opt_type.as_str();
        let legs: rhai::Array = vec![
            leg_map("long", ot, long_delta, dte),
            leg_map("short", ot, short_delta, dte),
            leg_map("short", ot, short_delta, dte),
            leg_map("long", ot, long_delta, dte),
        ];
        self.find_spread(legs)
    }

    /// Straddle (short or long).
    /// `ctx.straddle(#{ side: "short", delta: 0.50, dte: 45 })`
    pub fn straddle(&mut self, params: rhai::Map) -> Dynamic {
        let side = params
            .get("side")
            .and_then(|v| v.clone().into_immutable_string().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "short".to_string());
        let delta = params
            .get("delta")
            .and_then(|v| v.as_float().ok())
            .unwrap_or(0.50);
        let dte = params
            .get("dte")
            .and_then(|v| v.as_int().ok())
            .unwrap_or(45);

        let legs: rhai::Array = vec![
            leg_map(&side, "put", delta, dte),
            leg_map(&side, "call", delta, dte),
        ];
        self.find_spread(legs)
    }

    /// Strangle (short or long).
    /// `ctx.strangle(#{ side: "short", put: 0.25, call: 0.25, dte: 45 })`
    pub fn strangle(&mut self, params: rhai::Map) -> Dynamic {
        let side = params
            .get("side")
            .and_then(|v| v.clone().into_immutable_string().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "short".to_string());
        let put_delta = params
            .get("put")
            .and_then(|v| v.as_float().ok())
            .unwrap_or(0.25);
        let call_delta = params
            .get("call")
            .and_then(|v| v.as_float().ok())
            .unwrap_or(0.25);
        let dte = params
            .get("dte")
            .and_then(|v| v.as_int().ok())
            .unwrap_or(45);

        let legs: rhai::Array = vec![
            leg_map(&side, "put", put_delta, dte),
            leg_map(&side, "call", call_delta, dte),
        ];
        self.find_spread(legs)
    }

    /// Calendar spread.
    /// `ctx.calendar(#{ type: "put", delta: 0.30, near_dte: 30, far_dte: 60 })`
    pub fn calendar(&mut self, params: rhai::Map) -> Dynamic {
        let opt_type = params
            .get("type")
            .and_then(|v| v.clone().into_immutable_string().ok())
            .unwrap_or_default();
        let delta = params
            .get("delta")
            .and_then(|v| v.as_float().ok())
            .unwrap_or(0.30);
        let near_dte = params
            .get("near_dte")
            .and_then(|v| v.as_int().ok())
            .unwrap_or(30);
        let far_dte = params
            .get("far_dte")
            .and_then(|v| v.as_int().ok())
            .unwrap_or(60);

        let ot = opt_type.as_str();
        let legs: rhai::Array = vec![
            leg_map("short", ot, delta, near_dte),
            leg_map("long", ot, delta, far_dte),
        ];
        self.find_spread(legs)
    }

    /// Covered call — short call leg only (stock opened separately).
    /// `ctx.covered_call(#{ delta: 0.30, dte: 30 })`
    pub fn covered_call(&mut self, params: rhai::Map) -> Dynamic {
        let delta = params
            .get("delta")
            .and_then(|v| v.as_float().ok())
            .unwrap_or(0.30);
        let dte = params
            .get("dte")
            .and_then(|v| v.as_int().ok())
            .unwrap_or(30);

        let legs: rhai::Array = vec![leg_map("short", "call", delta, dte)];
        self.find_spread(legs)
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

    // --- Position sizing ---
    pub fn compute_quantity(&mut self, _method: String, _params: rhai::Map) -> i64 {
        // TODO: wrap sizing.rs::compute_quantity()
        1
    }
}

/// Parse "sma:20" into ("sma", 20).
fn parse_indicator_ref(s: &str) -> (String, i64) {
    let parts: Vec<&str> = s.split(':').collect();
    let name = parts[0].to_string();
    let period = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
    (name, period)
}

/// Build a leg map for strategy builder calls.
fn leg_map(side: &str, option_type: &str, delta: f64, dte: i64) -> Dynamic {
    let mut map = rhai::Map::new();
    map.insert("side".into(), Dynamic::from(side.to_string()));
    map.insert("option_type".into(), Dynamic::from(option_type.to_string()));
    map.insert("delta".into(), Dynamic::from(delta));
    map.insert("dte".into(), Dynamic::from(dte));
    Dynamic::from(map)
}

// ---------------------------------------------------------------------------
// Options chain helpers
// ---------------------------------------------------------------------------

/// Filter a DataFrame to rows matching a specific quote date.
pub(super) fn filter_to_date(
    df: &polars::prelude::DataFrame,
    date: NaiveDate,
) -> Option<polars::prelude::DataFrame> {
    use polars::prelude::*;

    // The datetime column may be NaiveDateTime — we need to compare just the date part
    let _datetime_col = df.column("datetime").ok()?;

    // Build a boolean mask: date part of datetime == target date
    let target_start = date.and_hms_opt(0, 0, 0)?;
    let target_end = date.succ_opt()?.and_hms_opt(0, 0, 0)?;

    let result = df
        .clone()
        .lazy()
        .filter(
            col("datetime")
                .gt_eq(lit(target_start))
                .and(col("datetime").lt(lit(target_end))),
        )
        .collect()
        .ok()?;

    Some(result)
}

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
            let series = date_ca.clone().into_series();
            let physical = series.i32().ok()?;
            let epoch_days = physical.get(row)?;
            return NaiveDate::from_num_days_from_ce_opt(
                epoch_days + crate::engine::types::EPOCH_DAYS_CE_OFFSET,
            );
        }
        // Try as Datetime (physical i64 = microseconds since epoch)
        if let Ok(dt_ca) = c.datetime() {
            let series = dt_ca.clone().into_series();
            let physical = series.i64().ok()?;
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
pub(super) fn row_to_expiration_date(
    df: &polars::prelude::DataFrame,
    row: usize,
) -> Option<NaiveDate> {
    use polars::prelude::*;

    let col = df.column("expiration").ok()?;
    // Try Date (physical i32)
    if let Ok(date_ca) = col.date() {
        let series = date_ca.clone().into_series();
        let physical = series.i32().ok()?;
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

// ---------------------------------------------------------------------------
// Action types — returned by script callbacks, processed by the engine
// ---------------------------------------------------------------------------

/// An action returned by `on_bar` or `on_exit_check` callbacks.
#[derive(Debug, Clone)]
pub enum ScriptAction {
    /// Open an options position with the given legs.
    OpenOptions {
        legs: Vec<LegSpec>,
        qty: Option<i32>,
    },
    /// Open a stock position.
    OpenStock { side: Side, qty: i32 },
    /// Close a specific position.
    Close {
        position_id: Option<usize>,
        reason: String,
    },
    /// Do nothing (from `on_exit_check`).
    Hold,
    /// Stop the backtest loop early.
    Stop { reason: String },
}

/// A leg specification in an `open_options` action.
/// Can be "unresolved" (delta/DTE targets) or "resolved" (specific contract).
#[derive(Debug, Clone)]
pub enum LegSpec {
    /// Engine resolves to a specific contract via `filters.rs`.
    Unresolved {
        side: Side,
        option_type: OptionType,
        delta: f64,
        dte: i32,
    },
    /// Pre-resolved contract from `find_option`.
    Resolved {
        side: Side,
        option_type: OptionType,
        strike: f64,
        expiration: NaiveDate,
        bid: f64,
        ask: f64,
    },
}

// ---------------------------------------------------------------------------
// ScriptSimContext — internal engine state (not exposed to scripts)
// ---------------------------------------------------------------------------

use crate::engine::sim_types::{DateIndex, LastKnown, PriceTable};

/// Internal state maintained by the unified scripting engine.
/// Scripts never see this — it bridges the clean Rhai API to the native engine's
/// `SimContext` / `BacktestParams` dependency chain.
pub struct ScriptSimContext {
    pub price_table: PriceTable,
    pub date_index: DateIndex,
    pub last_known: LastKnown,
    pub slippage: Slippage,
    pub commission: Option<Commission>,
    pub multiplier: i32,
    pub bars_per_year: f64,
    pub min_days_between_entries: Option<i32>,
    pub expiration_filter: ExpirationFilter,
    pub trade_selector: TradeSelector,
    pub next_position_id: usize,
    pub last_entry_date: Option<NaiveDate>,
}
