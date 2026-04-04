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
use crate::scripting::indicators::IndicatorStore;
use crate::scripting::options_cache::DatePartitionedOptions;

/// Per-symbol context returned by `ctx.sym("SYMBOL")`.
///
/// Contains the symbol's OHLCV bar at the current date, indicator store,
/// and options data — everything needed to read data and build trades.
#[derive(Clone)]
pub struct SymbolContext {
    /// Symbol name (uppercase).
    pub symbol: String,

    // Current bar data (at master timeline bar_idx)
    pub datetime: NaiveDateTime,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub bar_idx: usize,

    // Shared per-symbol data (Arc for cheap cloning)
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
// Indicator methods (same API as BarContext)
// ---------------------------------------------------------------------------

impl SymbolContext {
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
        self.indicator_value_multi("bbands_upper", &[period, 20])
    }
    pub fn bbands_mid(&mut self, period: i64) -> Dynamic {
        self.indicator_value("bbands_mid", period)
    }
    pub fn bbands_lower(&mut self, period: i64) -> Dynamic {
        self.indicator_value_multi("bbands_lower", &[period, 20])
    }
    pub fn stochastic(&mut self) -> Dynamic {
        self.indicator_value_multi("stochastic", &[14, 3, 3])
    }
    pub fn cci(&mut self, period: i64) -> Dynamic {
        self.indicator_value("cci", period)
    }
    pub fn obv(&mut self) -> Dynamic {
        self.indicator_value_multi("obv", &[])
    }
    pub fn adx(&mut self, period: i64) -> Dynamic {
        self.indicator_value("adx", period)
    }
    pub fn plus_di(&mut self, period: i64) -> Dynamic {
        self.indicator_value("plus_di", period)
    }
    pub fn minus_di(&mut self, period: i64) -> Dynamic {
        self.indicator_value("minus_di", period)
    }
    pub fn psar(&mut self) -> Dynamic {
        self.indicator_value_multi("psar", &[])
    }
    pub fn supertrend(&mut self) -> Dynamic {
        self.indicator_value_multi("supertrend", &[])
    }

    /// Generic indicator lookup by name and params (matches `BarContext::indicator`).
    pub fn indicator(&mut self, name: &str, params: rhai::Array) -> Dynamic {
        let int_params: Vec<i64> = params.iter().filter_map(|v| v.as_int().ok()).collect();
        self.indicator_value_multi(name, &int_params)
    }

    /// Check if a list of indicators have warmed up at the current bar.
    pub fn indicators_ready(&mut self, indicators: rhai::Array) -> bool {
        use crate::scripting::indicators::{
            parse_indicator_declaration, IndicatorKey, IndicatorParam,
        };

        for item in indicators {
            let Ok(s) = item.into_immutable_string() else {
                return false;
            };
            let Ok((name, params)) = parse_indicator_declaration(&s) else {
                return false;
            };
            let key = IndicatorKey {
                name,
                params: params
                    .iter()
                    .map(|&p| IndicatorParam::Int(p as i64))
                    .collect(),
            };
            match self.indicator_store.get(&key, self.bar_idx) {
                Some(v) if !v.is_nan() => {}
                _ => return false,
            }
        }
        true
    }

    // Internal helpers matching BarContext's pattern
    fn indicator_value(&self, name: &str, period: i64) -> Dynamic {
        use crate::scripting::indicators::{IndicatorKey, IndicatorParam};
        let key = IndicatorKey {
            name: name.to_string(),
            params: vec![IndicatorParam::Int(period)],
        };
        match self.indicator_store.get(&key, self.bar_idx) {
            Some(v) if v.is_nan() => Dynamic::UNIT,
            Some(v) => Dynamic::from(v),
            None => Dynamic::UNIT,
        }
    }

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
        if n <= 0 {
            return self.price_history.get(self.bar_idx);
        }
        let idx = self.bar_idx.checked_sub(n as usize)?;
        self.price_history.get(idx)
    }
}

// ---------------------------------------------------------------------------
// Options strategy helpers (same API as BarContext)
// ---------------------------------------------------------------------------

impl SymbolContext {
    /// Resolve a single options leg via the filter pipeline.
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

        let selected = match filters::select_closest_delta(filtered, target) {
            Ok(s) if s.height() > 0 => s,
            _ => return Dynamic::UNIT,
        };

        super::bar_context::row_to_option_map(&selected, 0, today)
    }

    /// Build a multi-leg options strategy, resolving each leg to a specific contract.
    /// Returns a map with `legs` array and `net_premium`, or `()` if any leg fails.
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
                return Dynamic::UNIT;
            }

            let found_map = found.clone().cast::<rhai::Map>();
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

            let mut leg_map = found_map;
            leg_map.insert("side".into(), Dynamic::from(side));
            leg_map.insert("option_type".into(), Dynamic::from(opt_type));
            resolved_legs.push(Dynamic::from(leg_map));
        }

        let mut result = rhai::Map::new();
        result.insert("legs".into(), Dynamic::from(resolved_legs));
        result.insert("net_premium".into(), Dynamic::from(net_premium));
        // Tag with symbol so the engine knows which symbol this trade targets
        result.insert("symbol".into(), Dynamic::from(self.symbol.clone()));
        Dynamic::from(result)
    }
}

// ---------------------------------------------------------------------------
// Named strategy helpers (delegate to build_strategy)
// ---------------------------------------------------------------------------

/// Build a leg map for passing to `build_strategy()`.
fn leg(side: &str, option_type: &str, delta: f64, dte: i64) -> Dynamic {
    let mut m = rhai::Map::new();
    m.insert("side".into(), Dynamic::from(side.to_string()));
    m.insert("option_type".into(), Dynamic::from(option_type.to_string()));
    m.insert("delta".into(), Dynamic::from(delta));
    m.insert("dte".into(), Dynamic::from(dte));
    Dynamic::from(m)
}

/// Wrap a resolved spread into an action map tagged with the symbol.
fn wrap_spread_action(resolved: Dynamic) -> Dynamic {
    if resolved.is_unit() {
        return Dynamic::UNIT;
    }
    let mut map = resolved.cast::<rhai::Map>();
    map.insert("action".into(), Dynamic::from("open_options".to_string()));
    Dynamic::from(map)
}

impl SymbolContext {
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
