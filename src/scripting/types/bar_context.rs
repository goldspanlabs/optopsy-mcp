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

    // --- Position awareness (next-bar execution model) ---
    /// Current market position: 1 = long, -1 = short, 0 = flat.
    pub market_position: i64,
    /// Entry price of the current position (0.0 if flat).
    pub entry_price: f64,
    /// Bars since entry of the current position (0 if flat).
    pub bars_since_entry: i64,
    /// Number of shares/contracts in the current position (0 if flat).
    pub current_shares: i64,
    /// Unrealized P&L of the current stock position (0.0 if flat).
    pub open_profit: f64,
    /// Maximum unrealized profit seen since entry (0.0 if flat).
    pub max_profit: f64,
    /// Maximum unrealized loss seen since entry (0.0 if flat).
    pub max_loss: f64,
    /// Number of pending orders in the order queue.
    pub pending_orders_count: i64,
    /// Portfolio-level aggregate state, exposed as `ctx.portfolio`.
    pub portfolio: PortfolioState,
}

// ---------------------------------------------------------------------------
// PortfolioState — the `portfolio` namespace exposed to Rhai scripts
// ---------------------------------------------------------------------------

/// Portfolio-level state computed once per bar.
/// Exposed to scripts as `ctx.portfolio` with property getters.
///
/// Note: `position_count` excludes implicit positions (e.g., auto-hedged stock
/// from assignment), so `long_count + short_count` may not equal `position_count`.
#[derive(Clone, Debug, Default)]
pub struct PortfolioState {
    pub cash: f64,
    pub equity: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
    pub total_exposure: f64,
    pub exposure_pct: f64,
    pub net_delta: f64,
    pub long_delta: f64,
    pub short_delta: f64,
    pub position_count: i64,
    pub long_count: i64,
    pub short_count: i64,
    pub max_position_pnl: f64,
    pub min_position_pnl: f64,
    pub drawdown: f64,
    pub peak_equity: f64,
}

impl PortfolioState {
    pub fn get_cash(&mut self) -> f64 {
        self.cash
    }
    pub fn get_equity(&mut self) -> f64 {
        self.equity
    }
    pub fn get_unrealized_pnl(&mut self) -> f64 {
        self.unrealized_pnl
    }
    pub fn get_realized_pnl(&mut self) -> f64 {
        self.realized_pnl
    }
    pub fn get_total_exposure(&mut self) -> f64 {
        self.total_exposure
    }
    pub fn get_exposure_pct(&mut self) -> f64 {
        self.exposure_pct
    }
    pub fn get_net_delta(&mut self) -> f64 {
        self.net_delta
    }
    pub fn get_long_delta(&mut self) -> f64 {
        self.long_delta
    }
    pub fn get_short_delta(&mut self) -> f64 {
        self.short_delta
    }
    pub fn get_position_count(&mut self) -> i64 {
        self.position_count
    }
    pub fn get_long_count(&mut self) -> i64 {
        self.long_count
    }
    pub fn get_short_count(&mut self) -> i64 {
        self.short_count
    }
    pub fn get_max_position_pnl(&mut self) -> f64 {
        self.max_position_pnl
    }
    pub fn get_min_position_pnl(&mut self) -> f64 {
        self.min_position_pnl
    }
    pub fn get_drawdown(&mut self) -> f64 {
        self.drawdown
    }
    pub fn get_peak_equity(&mut self) -> f64 {
        self.peak_equity
    }
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

    // --- Position awareness getters ---
    pub fn get_market_position(&mut self) -> i64 {
        self.market_position
    }
    pub fn get_entry_price(&mut self) -> f64 {
        self.entry_price
    }
    pub fn get_bars_since_entry(&mut self) -> i64 {
        self.bars_since_entry
    }
    pub fn get_current_shares(&mut self) -> i64 {
        self.current_shares
    }
    pub fn get_open_profit(&mut self) -> f64 {
        self.open_profit
    }
    pub fn get_max_profit(&mut self) -> f64 {
        self.max_profit
    }
    pub fn get_max_loss(&mut self) -> f64 {
        self.max_loss
    }
    pub fn get_pending_orders_count(&mut self) -> i64 {
        self.pending_orders_count
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

    /// Returns the time of day as "HH:MM" string (lexicographic comparison friendly).
    pub fn time(&mut self) -> String {
        format!(
            "{:02}:{:02}",
            self.datetime.time().hour(),
            self.datetime.time().minute()
        )
    }

    /// True if this is the first bar in the dataset (or first bar of the trading day for intraday).
    pub fn is_first_bar(&mut self) -> bool {
        if self.bar_idx == 0 {
            return true;
        }
        // For intraday: check if previous bar was a different date
        let prev = &self.price_history[self.bar_idx - 1];
        prev.datetime.date() != self.datetime.date()
    }

    /// True if this is the last bar in the dataset (or last bar of the trading day for intraday).
    pub fn is_last_bar(&mut self) -> bool {
        let total = self.price_history.len();
        if total == 0 || self.bar_idx >= total.saturating_sub(1) {
            return true;
        }
        // For intraday: check if next bar is a different date
        self.price_history
            .get(self.bar_idx + 1)
            .is_none_or(|next| next.datetime.date() != self.datetime.date())
    }

    /// True if the current bar falls in options expiration week (week of 3rd Friday).
    pub fn is_expiry_week(&mut self) -> bool {
        let date = self.datetime.date();
        // Find the 3rd Friday of this month
        let first_of_month = NaiveDate::from_ymd_opt(date.year(), date.month(), 1).unwrap_or(date);
        // Days until first Friday: (Friday=4 - weekday) mod 7
        let first_day_wd = first_of_month.weekday().num_days_from_monday(); // 0=Mon
        let days_to_first_fri = (4 + 7 - first_day_wd) % 7;
        let third_friday_day = 1 + days_to_first_fri + 14; // 1-indexed day of month
        if let Some(third_friday) =
            NaiveDate::from_ymd_opt(date.year(), date.month(), third_friday_day)
        {
            let expiry_week = third_friday.iso_week().week();
            date.iso_week().week() == expiry_week
        } else {
            false
        }
    }

    /// True if the current bar is the last trading day of a calendar quarter.
    /// Inferred from bar data: checks if the next bar crosses a quarter boundary.
    pub fn is_quarter_end(&mut self) -> bool {
        let month = self.datetime.date().month();
        // Quarter-end months: 3, 6, 9, 12
        if month != 3 && month != 6 && month != 9 && month != 12 {
            return false;
        }
        let curr_q = (month - 1) / 3;
        // Check if next bar is in a different quarter (or no next bar)
        match self.price_history.get(self.bar_idx + 1) {
            Some(next) => {
                let next_q = (next.datetime.date().month() - 1) / 3;
                next_q != curr_q
            }
            None => true, // Last bar of dataset in a quarter-end month
        }
    }

    /// Trading days remaining in the current month, counted from bar data.
    /// For intraday datasets, multiple bars on the same date count as one trading day.
    pub fn trading_days_left(&mut self) -> i64 {
        let target_month = self.datetime.date().month();
        let mut last_date = None;
        self.price_history
            .get(self.bar_idx + 1..)
            .unwrap_or_default()
            .iter()
            .take_while(|b| b.datetime.date().month() == target_month)
            .filter(|b| {
                let date = b.datetime.date();
                if last_date == Some(date) {
                    false
                } else {
                    last_date = Some(date);
                    true
                }
            })
            .count() as i64
    }

    /// Minutes elapsed since market open (assumes 09:30 ET open).
    pub fn minutes_since_open(&mut self) -> i64 {
        let h = self.datetime.time().hour() as i64;
        let m = self.datetime.time().minute() as i64;
        let total_mins = h * 60 + m;
        let open_mins = 9 * 60 + 30; // 09:30
        (total_mins - open_mins).max(0)
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

    /// Get portfolio aggregate state.
    pub fn get_portfolio(&mut self) -> PortfolioState {
        self.portfolio.clone()
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scripting::indicators::IndicatorStore;
    use crate::scripting::types::config::{Interval, OhlcvBar, ScriptConfig};
    use chrono::{NaiveDate, NaiveTime};

    /// Build a minimal BarContext for testing time methods.
    fn make_ctx(dt: NaiveDateTime, bar_idx: usize, bars: Vec<NaiveDateTime>) -> BarContext {
        let price_history: Vec<OhlcvBar> = bars
            .into_iter()
            .map(|d| OhlcvBar {
                datetime: d,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.0,
                volume: 1000.0,
            })
            .collect();

        let config = ScriptConfig {
            symbol: "TEST".to_string(),
            capital: 100_000.0,
            start_date: None,
            end_date: None,
            interval: Interval::Daily,
            multiplier: 100,
            timeout_secs: 30,
            auto_close_on_end: false,
            needs_ohlcv: true,
            needs_options: false,
            cross_symbols: vec![],
            declared_indicators: vec![],
            slippage: Default::default(),
            commission: None,
            min_days_between_entries: None,
            expiration_filter: Default::default(),
            trade_selector: Default::default(),
            defaults: HashMap::new(),
            procedural: false,
        };

        BarContext {
            datetime: dt,
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
            volume: 1000.0,
            bar_idx,
            cash: 100_000.0,
            equity: 100_000.0,
            positions: Arc::new(vec![]),
            indicator_store: Arc::new(IndicatorStore::new()),
            price_history: Arc::new(price_history),
            cross_symbol_data: Arc::new(HashMap::new()),
            options_by_date: None,
            config: Arc::new(config),
            pnl_history: Arc::new(vec![]),
            custom_series: Arc::new(Mutex::new(CustomSeriesStore {
                series: HashMap::new(),
                display_types: HashMap::new(),
                num_bars: 1,
            })),
            adjusted_close: 100.0,
            market_position: 0,
            entry_price: 0.0,
            bars_since_entry: 0,
            current_shares: 0,
            open_profit: 0.0,
            max_profit: 0.0,
            max_loss: 0.0,
            pending_orders_count: 0,
            portfolio: PortfolioState::default(),
        }
    }

    fn dt(y: i32, m: u32, d: u32, h: u32, min: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_time(NaiveTime::from_hms_opt(h, min, 0).unwrap())
    }

    fn daily(y: i32, m: u32, d: u32) -> NaiveDateTime {
        dt(y, m, d, 15, 59)
    }

    // -----------------------------------------------------------------------
    // day_of_week
    // -----------------------------------------------------------------------

    #[test]
    fn test_day_of_week_monday_through_friday() {
        // 2024-01-01 is Monday
        let mut ctx = make_ctx(daily(2024, 1, 1), 0, vec![daily(2024, 1, 1)]);
        assert_eq!(ctx.day_of_week(), 1); // Monday

        ctx.datetime = daily(2024, 1, 2);
        assert_eq!(ctx.day_of_week(), 2); // Tuesday

        ctx.datetime = daily(2024, 1, 3);
        assert_eq!(ctx.day_of_week(), 3); // Wednesday

        ctx.datetime = daily(2024, 1, 4);
        assert_eq!(ctx.day_of_week(), 4); // Thursday

        ctx.datetime = daily(2024, 1, 5);
        assert_eq!(ctx.day_of_week(), 5); // Friday
    }

    #[test]
    fn test_day_of_week_weekend() {
        let mut ctx = make_ctx(daily(2024, 1, 6), 0, vec![daily(2024, 1, 6)]);
        assert_eq!(ctx.day_of_week(), 6); // Saturday

        ctx.datetime = daily(2024, 1, 7);
        assert_eq!(ctx.day_of_week(), 7); // Sunday
    }

    // -----------------------------------------------------------------------
    // month
    // -----------------------------------------------------------------------

    #[test]
    fn test_month_all_twelve() {
        let mut ctx = make_ctx(daily(2024, 1, 15), 0, vec![daily(2024, 1, 15)]);
        for m in 1..=12 {
            ctx.datetime = daily(2024, m, 15);
            assert_eq!(ctx.month(), m as i64);
        }
    }

    // -----------------------------------------------------------------------
    // day_of_month edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_day_of_month_boundaries() {
        let mut ctx = make_ctx(daily(2024, 1, 1), 0, vec![daily(2024, 1, 1)]);
        assert_eq!(ctx.day_of_month(), 1);

        ctx.datetime = daily(2024, 1, 31);
        assert_eq!(ctx.day_of_month(), 31);

        // Feb 29 leap year
        ctx.datetime = daily(2024, 2, 29);
        assert_eq!(ctx.day_of_month(), 29);

        // Feb 28 non-leap year
        ctx.datetime = daily(2023, 2, 28);
        assert_eq!(ctx.day_of_month(), 28);
    }

    // -----------------------------------------------------------------------
    // time()
    // -----------------------------------------------------------------------

    #[test]
    fn test_time_formatting() {
        let mut ctx = make_ctx(dt(2024, 1, 2, 9, 30), 0, vec![dt(2024, 1, 2, 9, 30)]);
        assert_eq!(ctx.time(), "09:30");

        ctx.datetime = dt(2024, 1, 2, 15, 59);
        assert_eq!(ctx.time(), "15:59");

        ctx.datetime = dt(2024, 1, 2, 0, 0);
        assert_eq!(ctx.time(), "00:00");

        ctx.datetime = dt(2024, 1, 2, 23, 59);
        assert_eq!(ctx.time(), "23:59");
    }

    #[test]
    fn test_time_lexicographic_comparison() {
        // Verify string ordering matches chronological ordering
        let t0930 = "09:30";
        let t1000 = "10:00";
        let t1530 = "15:30";
        let t1559 = "15:59";
        assert!(t0930 < t1000);
        assert!(t1000 < t1530);
        assert!(t1530 < t1559);
    }

    // -----------------------------------------------------------------------
    // minutes_since_open
    // -----------------------------------------------------------------------

    #[test]
    fn test_minutes_since_open() {
        let mut ctx = make_ctx(dt(2024, 1, 2, 9, 30), 0, vec![dt(2024, 1, 2, 9, 30)]);
        assert_eq!(ctx.minutes_since_open(), 0); // Market open

        ctx.datetime = dt(2024, 1, 2, 10, 0);
        assert_eq!(ctx.minutes_since_open(), 30);

        ctx.datetime = dt(2024, 1, 2, 16, 0);
        assert_eq!(ctx.minutes_since_open(), 390); // 6.5 hours

        // Before market open — clamped to 0
        ctx.datetime = dt(2024, 1, 2, 8, 0);
        assert_eq!(ctx.minutes_since_open(), 0);
    }

    // -----------------------------------------------------------------------
    // is_first_bar / is_last_bar
    // -----------------------------------------------------------------------

    #[test]
    fn test_is_first_bar_daily() {
        // For daily bars, every bar is the first (and last) bar of its trading day
        // because each bar is on a different date.
        let bars = vec![daily(2024, 1, 2), daily(2024, 1, 3), daily(2024, 1, 4)];
        let mut ctx = make_ctx(daily(2024, 1, 2), 0, bars.clone());
        assert!(ctx.is_first_bar()); // First bar of dataset

        ctx.bar_idx = 1;
        ctx.datetime = daily(2024, 1, 3);
        assert!(ctx.is_first_bar()); // Different date than prev → first bar of this day
    }

    #[test]
    fn test_is_first_bar_intraday_day_transition() {
        let bars = vec![
            dt(2024, 1, 2, 15, 55),
            dt(2024, 1, 2, 16, 0),
            dt(2024, 1, 3, 9, 30), // New day
            dt(2024, 1, 3, 9, 35),
        ];
        let mut ctx = make_ctx(dt(2024, 1, 3, 9, 30), 2, bars.clone());
        assert!(ctx.is_first_bar()); // First bar of new trading day

        ctx.bar_idx = 3;
        ctx.datetime = dt(2024, 1, 3, 9, 35);
        assert!(!ctx.is_first_bar());
    }

    #[test]
    fn test_is_last_bar_daily() {
        // For daily bars, every bar is the last bar of its trading day
        let bars = vec![daily(2024, 1, 2), daily(2024, 1, 3), daily(2024, 1, 4)];
        let mut ctx = make_ctx(daily(2024, 1, 4), 2, bars.clone());
        assert!(ctx.is_last_bar()); // Last bar of dataset

        ctx.bar_idx = 0;
        ctx.datetime = daily(2024, 1, 2);
        assert!(ctx.is_last_bar()); // Next bar is different date → last bar of this day
    }

    #[test]
    fn test_is_last_bar_intraday_day_transition() {
        let bars = vec![
            dt(2024, 1, 2, 15, 55),
            dt(2024, 1, 2, 16, 0), // Last bar of Jan 2
            dt(2024, 1, 3, 9, 30), // First bar of Jan 3
            dt(2024, 1, 3, 9, 35),
        ];
        let mut ctx = make_ctx(dt(2024, 1, 2, 16, 0), 1, bars.clone());
        assert!(ctx.is_last_bar()); // Last bar before day transition

        ctx.bar_idx = 2;
        ctx.datetime = dt(2024, 1, 3, 9, 30);
        assert!(!ctx.is_last_bar());
    }

    // -----------------------------------------------------------------------
    // is_expiry_week
    // -----------------------------------------------------------------------

    #[test]
    fn test_is_expiry_week_jan_2024() {
        // January 2024: 3rd Friday = Jan 19
        // ISO week of Jan 19, 2024 = week 3
        let mut ctx = make_ctx(daily(2024, 1, 15), 0, vec![daily(2024, 1, 15)]);
        assert!(ctx.is_expiry_week()); // Mon of expiry week

        ctx.datetime = daily(2024, 1, 19);
        assert!(ctx.is_expiry_week()); // The 3rd Friday itself

        ctx.datetime = daily(2024, 1, 8);
        assert!(!ctx.is_expiry_week()); // Week before
    }

    #[test]
    fn test_is_expiry_week_feb_2024() {
        // February 2024: 1st is Thursday
        // 1st Friday = Feb 2, 2nd = Feb 9, 3rd Friday = Feb 16
        let mut ctx = make_ctx(daily(2024, 2, 16), 0, vec![daily(2024, 2, 16)]);
        assert!(ctx.is_expiry_week()); // 3rd Friday

        ctx.datetime = daily(2024, 2, 12);
        assert!(ctx.is_expiry_week()); // Monday of same week

        ctx.datetime = daily(2024, 2, 23);
        assert!(!ctx.is_expiry_week()); // Following week
    }

    #[test]
    fn test_is_expiry_week_month_starting_saturday() {
        // June 2024: June 1 is Saturday
        // 1st Friday = June 7, 2nd = June 14, 3rd Friday = June 21
        let mut ctx = make_ctx(daily(2024, 6, 21), 0, vec![daily(2024, 6, 21)]);
        assert!(ctx.is_expiry_week());

        ctx.datetime = daily(2024, 6, 14);
        assert!(!ctx.is_expiry_week()); // 2nd Friday, not 3rd
    }

    #[test]
    fn test_is_expiry_week_month_starting_friday() {
        // November 2024: Nov 1 is Friday
        // 1st Friday = Nov 1, 2nd = Nov 8, 3rd Friday = Nov 15
        let mut ctx = make_ctx(daily(2024, 11, 15), 0, vec![daily(2024, 11, 15)]);
        assert!(ctx.is_expiry_week());

        ctx.datetime = daily(2024, 11, 22);
        assert!(!ctx.is_expiry_week()); // 4th Friday
    }

    // -----------------------------------------------------------------------
    // is_quarter_end
    // -----------------------------------------------------------------------

    #[test]
    fn test_is_quarter_end_next_bar_crosses_quarter() {
        // Last bar of March, next bar is in April → quarter end
        let bars = vec![daily(2024, 3, 28), daily(2024, 4, 1)];
        let mut ctx = make_ctx(daily(2024, 3, 28), 0, bars);
        assert!(ctx.is_quarter_end());
    }

    #[test]
    fn test_is_quarter_end_next_bar_same_quarter() {
        // Still in March, next bar also in March → not quarter end
        let bars = vec![daily(2024, 3, 27), daily(2024, 3, 28)];
        let mut ctx = make_ctx(daily(2024, 3, 27), 0, bars);
        assert!(!ctx.is_quarter_end());
    }

    #[test]
    fn test_is_quarter_end_not_quarter_month() {
        let bars = vec![daily(2024, 2, 28), daily(2024, 3, 1)];
        let mut ctx = make_ctx(daily(2024, 2, 28), 0, bars);
        assert!(!ctx.is_quarter_end()); // February is not a quarter-end month
    }

    #[test]
    fn test_is_quarter_end_last_bar_of_dataset() {
        // No next bar, in a quarter-end month → true
        let bars = vec![daily(2024, 12, 31)];
        let mut ctx = make_ctx(daily(2024, 12, 31), 0, bars);
        assert!(ctx.is_quarter_end());
    }

    #[test]
    fn test_is_quarter_end_all_quarters() {
        // Each quarter transition: last bar in Q, first bar in next Q
        let transitions = vec![
            (daily(2024, 3, 28), daily(2024, 4, 1)),  // Q1→Q2
            (daily(2024, 6, 28), daily(2024, 7, 1)),  // Q2→Q3
            (daily(2024, 9, 30), daily(2024, 10, 1)), // Q3→Q4
            (daily(2024, 12, 31), daily(2025, 1, 2)), // Q4→Q1
        ];
        for (last_bar, next_bar) in transitions {
            let bars = vec![last_bar, next_bar];
            let mut ctx = make_ctx(last_bar, 0, bars);
            assert!(ctx.is_quarter_end(), "Expected quarter end at {last_bar}");
        }
    }

    // -----------------------------------------------------------------------
    // trading_days_left (bar-counting)
    // -----------------------------------------------------------------------

    #[test]
    fn test_trading_days_left_counts_remaining_bars() {
        // 3 bars remaining in January after the current bar
        let bars = vec![
            daily(2024, 1, 29),
            daily(2024, 1, 30),
            daily(2024, 1, 31),
            daily(2024, 2, 1), // Next month — not counted
        ];
        let mut ctx = make_ctx(daily(2024, 1, 29), 0, bars);
        assert_eq!(ctx.trading_days_left(), 2); // Jan 30, Jan 31
    }

    #[test]
    fn test_trading_days_left_last_day_of_month() {
        // Current bar is the last bar in the month
        let bars = vec![daily(2024, 1, 31), daily(2024, 2, 1)];
        let mut ctx = make_ctx(daily(2024, 1, 31), 0, bars);
        assert_eq!(ctx.trading_days_left(), 0);
    }

    #[test]
    fn test_trading_days_left_last_bar_of_dataset() {
        // No more bars at all
        let bars = vec![daily(2024, 1, 15)];
        let mut ctx = make_ctx(daily(2024, 1, 15), 0, bars);
        assert_eq!(ctx.trading_days_left(), 0);
    }

    #[test]
    fn test_trading_days_left_many_bars() {
        // 5 trading days left in the month
        let bars = vec![
            daily(2024, 4, 22), // Monday (current)
            daily(2024, 4, 23),
            daily(2024, 4, 24),
            daily(2024, 4, 25),
            daily(2024, 4, 26),
            daily(2024, 4, 29),
            daily(2024, 4, 30),
            daily(2024, 5, 1), // Next month
        ];
        let mut ctx = make_ctx(daily(2024, 4, 22), 0, bars);
        assert_eq!(ctx.trading_days_left(), 6); // 23,24,25,26,29,30
    }

    // -----------------------------------------------------------------------
    #[test]
    fn test_trading_days_left_intraday_counts_unique_dates() {
        // Intraday: 3 bars on Jan 15, then 2 bars on Jan 16, then Feb 1
        // From bar 0 (Jan 15 09:30), remaining unique dates in Jan: Jan 15 (2 more bars) + Jan 16 = 2 days
        let bars = vec![
            dt(2024, 1, 15, 9, 30),
            dt(2024, 1, 15, 10, 0),
            dt(2024, 1, 15, 10, 30),
            dt(2024, 1, 16, 9, 30),
            dt(2024, 1, 16, 10, 0),
            dt(2024, 2, 1, 9, 30),
        ];
        let mut ctx = make_ctx(dt(2024, 1, 15, 9, 30), 0, bars);
        assert_eq!(ctx.trading_days_left(), 2); // Jan 15 + Jan 16 (unique dates)
    }

    #[test]
    fn test_trading_days_left_mid_dataset() {
        // Current bar is in the middle, not at index 0
        let bars = vec![
            daily(2024, 4, 22),
            daily(2024, 4, 23),
            daily(2024, 4, 24), // ← current
            daily(2024, 4, 25),
            daily(2024, 4, 26),
            daily(2024, 5, 1),
        ];
        let mut ctx = make_ctx(daily(2024, 4, 24), 2, bars);
        assert_eq!(ctx.trading_days_left(), 2); // Apr 25, Apr 26
    }

    #[test]
    fn test_is_quarter_end_intraday_last_bar_of_day() {
        // Intraday: last 5m bar of March, next bar is April
        let bars = vec![dt(2024, 3, 28, 15, 55), dt(2024, 4, 1, 9, 30)];
        let mut ctx = make_ctx(dt(2024, 3, 28, 15, 55), 0, bars);
        assert!(ctx.is_quarter_end());
    }

    #[test]
    fn test_is_quarter_end_intraday_not_last_bar_of_day() {
        // Intraday: mid-day bar in March, next bar still in March
        let bars = vec![dt(2024, 3, 28, 10, 0), dt(2024, 3, 28, 10, 5)];
        let mut ctx = make_ctx(dt(2024, 3, 28, 10, 0), 0, bars);
        assert!(!ctx.is_quarter_end()); // Next bar is same month
    }

    #[test]
    fn test_is_quarter_end_mid_dataset() {
        // bar_idx is not 0 — test that we look at the right next bar
        let bars = vec![
            daily(2024, 6, 27),
            daily(2024, 6, 28), // ← current
            daily(2024, 7, 1),
        ];
        let mut ctx = make_ctx(daily(2024, 6, 28), 1, bars);
        assert!(ctx.is_quarter_end());
    }

    #[test]
    fn test_is_first_bar_single_bar_dataset() {
        let bars = vec![daily(2024, 1, 2)];
        let mut ctx = make_ctx(daily(2024, 1, 2), 0, bars);
        assert!(ctx.is_first_bar());
    }

    #[test]
    fn test_is_last_bar_single_bar_dataset() {
        let bars = vec![daily(2024, 1, 2)];
        let mut ctx = make_ctx(daily(2024, 1, 2), 0, bars);
        assert!(ctx.is_last_bar());
    }

    #[test]
    fn test_is_last_bar_empty_dataset() {
        let bars: Vec<NaiveDateTime> = vec![];
        let mut ctx = make_ctx(daily(2024, 1, 2), 0, bars);
        assert!(ctx.is_last_bar()); // Empty → true (graceful)
    }

    #[test]
    fn test_trading_days_left_empty_dataset() {
        let bars: Vec<NaiveDateTime> = vec![];
        let mut ctx = make_ctx(daily(2024, 1, 15), 0, bars);
        assert_eq!(ctx.trading_days_left(), 0); // No bars → 0
    }

    #[test]
    fn test_is_quarter_end_empty_dataset() {
        let bars: Vec<NaiveDateTime> = vec![];
        let mut ctx = make_ctx(daily(2024, 3, 28), 0, bars);
        // Empty dataset, no next bar → true (in a quarter-end month)
        assert!(ctx.is_quarter_end());
    }

    // -----------------------------------------------------------------------
    // week_of_year
    // -----------------------------------------------------------------------

    #[test]
    fn test_week_of_year() {
        let mut ctx = make_ctx(daily(2024, 1, 1), 0, vec![daily(2024, 1, 1)]);
        assert_eq!(ctx.week_of_year(), 1);

        ctx.datetime = daily(2024, 12, 31);
        assert_eq!(ctx.week_of_year(), 1); // Dec 31, 2024 is in ISO week 1 of 2025
    }
}
