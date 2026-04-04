//! SymbolContext — per-symbol accessor returned by `ctx.sym("SYMBOL")` in multi-symbol scripts.
//!
//! Provides OHLCV getters, indicators, and strategy helpers scoped to a specific
//! symbol's data. Lighter than `BarContext` — no portfolio state, no cross-symbol
//! data, no custom series. Just the data needed to read prices, check indicators,
//! and build options trades for one symbol.

use std::sync::Arc;

use chrono::NaiveDateTime;
use rhai::Dynamic;

use super::config::OhlcvBar;
use crate::scripting::helpers::{
    build_strategy_from_legs, indicator_lookup, indicator_lookup_multi, indicators_all_ready, leg,
};
use crate::scripting::indicators::IndicatorStore;
use crate::scripting::options_cache::DatePartitionedOptions;

/// Per-symbol context returned by `ctx.sym("SYMBOL")`.
///
/// Contains the symbol's OHLCV bar at the current date, indicator store,
/// and options data — everything needed to read data and build trades.
#[derive(Clone)]
pub struct SymbolContext {
    pub symbol: String,
    pub datetime: NaiveDateTime,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub bar_idx: usize,
    pub indicator_store: Arc<IndicatorStore>,
    pub price_history: Arc<Vec<OhlcvBar>>,
    pub options_by_date: Option<Arc<DatePartitionedOptions>>,
}

// ---------------------------------------------------------------------------
// Rhai property getters
// ---------------------------------------------------------------------------

impl SymbolContext {
    pub fn get_symbol(&mut self) -> String {
        self.symbol.clone()
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
    pub fn get_date(&mut self) -> String {
        self.datetime.date().to_string()
    }
    pub fn get_datetime(&mut self) -> String {
        self.datetime.to_string()
    }
}

// ---------------------------------------------------------------------------
// Indicator methods — delegates to shared helpers in helpers.rs
// ---------------------------------------------------------------------------

impl SymbolContext {
    fn iv(&self, name: &str, period: i64) -> Dynamic {
        indicator_lookup(&self.indicator_store, self.bar_idx, name, period)
    }
    fn ivm(&self, name: &str, params: &[i64]) -> Dynamic {
        indicator_lookup_multi(&self.indicator_store, self.bar_idx, name, params)
    }

    pub fn sma(&mut self, period: i64) -> Dynamic {
        self.iv("sma", period)
    }
    pub fn ema(&mut self, period: i64) -> Dynamic {
        self.iv("ema", period)
    }
    pub fn rsi(&mut self, period: i64) -> Dynamic {
        self.iv("rsi", period)
    }
    pub fn atr(&mut self, period: i64) -> Dynamic {
        self.iv("atr", period)
    }
    pub fn macd_line(&mut self) -> Dynamic {
        self.ivm("macd_line", &[12, 26, 9])
    }
    pub fn macd_signal(&mut self) -> Dynamic {
        self.ivm("macd_signal", &[12, 26, 9])
    }
    pub fn macd_hist(&mut self) -> Dynamic {
        self.ivm("macd_hist", &[12, 26, 9])
    }
    pub fn bbands_upper(&mut self, period: i64) -> Dynamic {
        self.ivm("bbands_upper", &[period, 20])
    }
    pub fn bbands_mid(&mut self, period: i64) -> Dynamic {
        self.iv("bbands_mid", period)
    }
    pub fn bbands_lower(&mut self, period: i64) -> Dynamic {
        self.ivm("bbands_lower", &[period, 20])
    }
    pub fn stochastic(&mut self, period: i64) -> Dynamic {
        self.iv("stochastic", period)
    }
    pub fn cci(&mut self, period: i64) -> Dynamic {
        self.iv("cci", period)
    }
    pub fn obv(&mut self) -> Dynamic {
        self.ivm("obv", &[])
    }
    pub fn adx(&mut self, period: i64) -> Dynamic {
        self.iv("adx", period)
    }
    pub fn plus_di(&mut self, period: i64) -> Dynamic {
        self.iv("plus_di", period)
    }
    pub fn minus_di(&mut self, period: i64) -> Dynamic {
        self.iv("minus_di", period)
    }
    pub fn psar(&mut self) -> Dynamic {
        self.ivm("psar", &[2, 20])
    }
    pub fn supertrend(&mut self, period: i64) -> Dynamic {
        self.iv("supertrend", period)
    }
    pub fn indicator(&mut self, name: String, period: i64) -> Dynamic {
        self.iv(&name, period)
    }

    pub fn indicator_with(&mut self, name: &str, params: rhai::Array) -> Dynamic {
        let int_params: Vec<i64> = params.iter().filter_map(|v| v.as_int().ok()).collect();
        self.ivm(name, &int_params)
    }

    pub fn indicators_ready(&mut self, indicators: rhai::Array) -> bool {
        indicators_all_ready(&self.indicator_store, self.bar_idx, indicators)
    }
}

// ---------------------------------------------------------------------------
// Historical bar lookback
// ---------------------------------------------------------------------------

impl SymbolContext {
    pub fn high_at(&self, n: i64) -> Dynamic {
        self.bar_at_offset(n)
            .map_or(Dynamic::UNIT, |b| Dynamic::from(b.high))
    }
    pub fn low_at(&self, n: i64) -> Dynamic {
        self.bar_at_offset(n)
            .map_or(Dynamic::UNIT, |b| Dynamic::from(b.low))
    }
    pub fn open_at(&self, n: i64) -> Dynamic {
        self.bar_at_offset(n)
            .map_or(Dynamic::UNIT, |b| Dynamic::from(b.open))
    }
    pub fn close_at(&self, n: i64) -> Dynamic {
        self.bar_at_offset(n)
            .map_or(Dynamic::UNIT, |b| Dynamic::from(b.close))
    }

    fn bar_at_offset(&self, n: i64) -> Option<&OhlcvBar> {
        if n < 0 {
            return None;
        }
        if n == 0 {
            return self.price_history.get(self.bar_idx);
        }
        let idx = self.bar_idx.checked_sub(n as usize)?;
        self.price_history.get(idx)
    }
}

// ---------------------------------------------------------------------------
// Options strategy helpers — delegates to shared build_strategy_from_legs()
// ---------------------------------------------------------------------------

/// Wrap a resolved spread into an action map.
fn wrap_spread_action(resolved: Dynamic) -> Dynamic {
    if resolved.is_unit() {
        return Dynamic::UNIT;
    }
    let mut map = resolved.cast::<rhai::Map>();
    map.insert("action".into(), Dynamic::from("open_options".to_string()));
    Dynamic::from(map)
}

impl SymbolContext {
    pub fn build_strategy(&mut self, legs: rhai::Array) -> Dynamic {
        build_strategy_from_legs(
            legs,
            &self.options_by_date,
            self.datetime,
            Some(&self.symbol),
        )
    }

    // Singles
    pub fn long_call(&mut self, delta: f64, dte: i64) -> Dynamic {
        wrap_spread_action(self.build_strategy(vec![leg("long", "call", delta, dte)]))
    }
    pub fn short_call(&mut self, delta: f64, dte: i64) -> Dynamic {
        wrap_spread_action(self.build_strategy(vec![leg("short", "call", delta, dte)]))
    }
    pub fn long_put(&mut self, delta: f64, dte: i64) -> Dynamic {
        wrap_spread_action(self.build_strategy(vec![leg("long", "put", delta, dte)]))
    }
    pub fn short_put(&mut self, delta: f64, dte: i64) -> Dynamic {
        wrap_spread_action(self.build_strategy(vec![leg("short", "put", delta, dte)]))
    }
    pub fn covered_call(&mut self, delta: f64, dte: i64) -> Dynamic {
        wrap_spread_action(self.build_strategy(vec![leg("short", "call", delta, dte)]))
    }

    // Vertical spreads
    pub fn bull_call_spread(&mut self, long_d: f64, short_d: f64, dte: i64) -> Dynamic {
        let legs = vec![
            leg("long", "call", long_d, dte),
            leg("short", "call", short_d, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }
    pub fn bear_call_spread(&mut self, short_d: f64, long_d: f64, dte: i64) -> Dynamic {
        let legs = vec![
            leg("short", "call", short_d, dte),
            leg("long", "call", long_d, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }
    pub fn bull_put_spread(&mut self, long_d: f64, short_d: f64, dte: i64) -> Dynamic {
        let legs = vec![
            leg("long", "put", long_d, dte),
            leg("short", "put", short_d, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }
    pub fn bear_put_spread(&mut self, short_d: f64, long_d: f64, dte: i64) -> Dynamic {
        let legs = vec![
            leg("short", "put", short_d, dte),
            leg("long", "put", long_d, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    // Straddles & strangles
    pub fn long_straddle(&mut self, delta: f64, dte: i64) -> Dynamic {
        let legs = vec![
            leg("long", "call", delta, dte),
            leg("long", "put", delta, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }
    pub fn short_straddle(&mut self, delta: f64, dte: i64) -> Dynamic {
        let legs = vec![
            leg("short", "call", delta, dte),
            leg("short", "put", delta, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }
    pub fn long_strangle(&mut self, call_d: f64, put_d: f64, dte: i64) -> Dynamic {
        let legs = vec![
            leg("long", "call", call_d, dte),
            leg("long", "put", put_d, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }
    pub fn short_strangle(&mut self, call_d: f64, put_d: f64, dte: i64) -> Dynamic {
        let legs = vec![
            leg("short", "call", call_d, dte),
            leg("short", "put", put_d, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    // Iron strategies
    pub fn iron_condor(
        &mut self,
        short_put_d: f64,
        long_put_d: f64,
        short_call_d: f64,
        long_call_d: f64,
        dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("short", "put", short_put_d, dte),
            leg("long", "put", long_put_d, dte),
            leg("short", "call", short_call_d, dte),
            leg("long", "call", long_call_d, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }
    pub fn iron_butterfly(&mut self, put_d: f64, center_d: f64, call_d: f64, dte: i64) -> Dynamic {
        let legs = vec![
            leg("long", "put", put_d, dte),
            leg("short", "put", center_d, dte),
            leg("short", "call", center_d, dte),
            leg("long", "call", call_d, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }
}
