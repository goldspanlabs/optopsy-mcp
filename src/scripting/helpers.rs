//! High-level Rhai helper functions for strategy construction and action building.
//!
//! These helpers reduce boilerplate in scripts by providing named strategy
//! constructors (e.g., `ctx.bull_put_spread(...)`) and action builders
//! (e.g., `hold_position()`, `close_position(...)`).
//!
//! All strategy helpers delegate to `BarContext::build_strategy()` internally
//! and return ready-to-use action maps or `()` if leg resolution fails.

use std::sync::Arc;

use chrono::NaiveDateTime;
use rhai::Dynamic;

use super::indicators::IndicatorStore;
use super::options_cache::DatePartitionedOptions;
use super::types::BarContext;

// ---------------------------------------------------------------------------
// Shared indicator helpers — used by both BarContext and SymbolContext
// ---------------------------------------------------------------------------

/// Look up a single-param indicator value at a given bar index.
pub(super) fn indicator_lookup(
    store: &IndicatorStore,
    bar_idx: usize,
    name: &str,
    period: i64,
) -> Dynamic {
    use super::indicators::{IndicatorKey, IndicatorParam};
    let key = IndicatorKey {
        name: name.to_string(),
        params: vec![IndicatorParam::Int(period)],
    };
    match store.get(&key, bar_idx) {
        Some(v) if v.is_nan() => Dynamic::UNIT,
        Some(v) => Dynamic::from(v),
        None => Dynamic::UNIT,
    }
}

/// Look up a multi-param indicator value at a given bar index.
pub(super) fn indicator_lookup_multi(
    store: &IndicatorStore,
    bar_idx: usize,
    name: &str,
    params: &[i64],
) -> Dynamic {
    use super::indicators::{IndicatorKey, IndicatorParam};
    let key = IndicatorKey {
        name: name.to_string(),
        params: params.iter().map(|&p| IndicatorParam::Int(p)).collect(),
    };
    match store.get(&key, bar_idx) {
        Some(v) if v.is_nan() => Dynamic::UNIT,
        Some(v) => Dynamic::from(v),
        None => Dynamic::UNIT,
    }
}

/// Look up a multi-param indicator using a named parameter map (matching `BarContext::indicator_with`).
///
/// Extracts known param keys in a fixed order, converting floats to scaled integers
/// following the `IndicatorStore` convention.
pub(super) fn indicator_lookup_map(
    store: &IndicatorStore,
    bar_idx: usize,
    name: String,
    params: rhai::Map,
) -> Dynamic {
    use super::indicators::{IndicatorKey, IndicatorParam};

    let mut param_vec: Vec<IndicatorParam> = Vec::new();
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
    match store.get(&key, bar_idx) {
        Some(v) if v.is_nan() => Dynamic::UNIT,
        Some(v) => Dynamic::from(v),
        None => Dynamic::UNIT,
    }
}

/// Check if all declared indicators have warmed up at a given bar index.
pub(super) fn indicators_all_ready(
    store: &IndicatorStore,
    bar_idx: usize,
    indicators: rhai::Array,
) -> bool {
    use super::indicators::{parse_indicator_declaration, IndicatorKey, IndicatorParam};

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
        match store.get(&key, bar_idx) {
            Some(v) if !v.is_nan() => {}
            _ => return false,
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Shared options leg resolution — used by both BarContext and SymbolContext
// ---------------------------------------------------------------------------

/// Resolve a single options leg to a specific contract via the filter pipeline.
pub(super) fn resolve_option_leg(
    option_type: &str,
    target: &crate::engine::types::TargetRange,
    dte_min: i32,
    dte_max: i32,
    options_by_date: &Option<Arc<DatePartitionedOptions>>,
    datetime: NaiveDateTime,
) -> Dynamic {
    use crate::engine::filters;

    let today = datetime.date();

    let today_df = match options_by_date {
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

    super::types::row_to_option_map(&selected, 0, today)
}

/// Build a multi-leg options strategy from a legs array, resolving each leg.
/// Returns `#{ legs, net_premium, symbol? }` or `()` if any leg fails.
pub(super) fn build_strategy_from_legs(
    legs: rhai::Array,
    options_by_date: &Option<Arc<DatePartitionedOptions>>,
    datetime: NaiveDateTime,
    symbol: Option<&str>,
) -> Dynamic {
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
        let found = resolve_option_leg(
            &opt_type,
            &target,
            (dte - 15).max(1) as i32,
            (dte + 15) as i32,
            options_by_date,
            datetime,
        );
        if found.is_unit() {
            return Dynamic::UNIT;
        }

        let found_map = found.cast::<rhai::Map>();
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
    if let Some(sym) = symbol {
        result.insert("symbol".into(), Dynamic::from(sym.to_string()));
    }
    Dynamic::from(result)
}

// ---------------------------------------------------------------------------
// Internal: build a leg map for passing to build_strategy()
// ---------------------------------------------------------------------------

pub(super) fn leg(side: &str, option_type: &str, delta: f64, dte: i64) -> Dynamic {
    let mut map = rhai::Map::new();
    map.insert("side".into(), side.into());
    map.insert("option_type".into(), option_type.into());
    map.insert("delta".into(), delta.into());
    map.insert("dte".into(), dte.into());
    map.into()
}

/// Wrap a resolved spread (from `build_strategy`) into a ready action map.
fn wrap_spread_action(spread: Dynamic) -> Dynamic {
    if spread.is_unit() {
        return Dynamic::UNIT;
    }
    let mut action = rhai::Map::new();
    action.insert("action".into(), "open_spread".into());
    action.insert("spread".into(), spread);
    action.into()
}

// ---------------------------------------------------------------------------
// Global action helpers (registered as standalone Rhai functions)
// ---------------------------------------------------------------------------

/// `hold_position()` → `#{ action: "hold" }`
pub fn hold_position() -> Dynamic {
    let mut map = rhai::Map::new();
    map.insert("action".into(), "hold".into());
    map.into()
}

/// `close_position(reason)` → `#{ action: "close", reason }`
pub fn close_position(reason: String) -> Dynamic {
    let mut map = rhai::Map::new();
    map.insert("action".into(), "close".into());
    map.insert("reason".into(), reason.into());
    map.into()
}

/// `close_position_id(position_id, reason)` → `#{ action: "close", position_id, reason }`
pub fn close_position_id(position_id: i64, reason: String) -> Dynamic {
    let mut map = rhai::Map::new();
    map.insert("action".into(), "close".into());
    map.insert("position_id".into(), position_id.into());
    map.insert("reason".into(), reason.into());
    map.into()
}

/// `stop_backtest(reason)` → `#{ action: "stop", reason }`
pub fn stop_backtest(reason: String) -> Dynamic {
    let mut map = rhai::Map::new();
    map.insert("action".into(), "stop".into());
    map.insert("reason".into(), reason.into());
    map.into()
}

/// `buy_stock(qty)` → `#{ action: "open_stock", side: "long", qty }`
pub fn buy_stock(qty: i64) -> Dynamic {
    let mut map = rhai::Map::new();
    map.insert("action".into(), "open_stock".into());
    map.insert("side".into(), "long".into());
    map.insert("qty".into(), qty.into());
    map.into()
}

/// `sell_stock(qty)` → `#{ action: "open_stock", side: "short", qty }`
pub fn sell_stock(qty: i64) -> Dynamic {
    let mut map = rhai::Map::new();
    map.insert("action".into(), "open_stock".into());
    map.insert("side".into(), "short".into());
    map.insert("qty".into(), qty.into());
    map.into()
}

// ---------------------------------------------------------------------------
// Order-type action helpers (next-bar execution model)
// ---------------------------------------------------------------------------

/// `buy_limit(qty, price)` → `#{ action: "open_stock", side: "long", qty, order_type: "limit", limit_price }`
pub fn buy_limit(qty: i64, price: f64) -> Dynamic {
    let mut map = rhai::Map::new();
    map.insert("action".into(), "open_stock".into());
    map.insert("side".into(), "long".into());
    map.insert("qty".into(), qty.into());
    map.insert("order_type".into(), "limit".into());
    map.insert("limit_price".into(), price.into());
    map.into()
}

/// `buy_stop(qty, price)` → `#{ action: "open_stock", side: "long", qty, order_type: "stop", stop_price }`
pub fn buy_stop(qty: i64, price: f64) -> Dynamic {
    let mut map = rhai::Map::new();
    map.insert("action".into(), "open_stock".into());
    map.insert("side".into(), "long".into());
    map.insert("qty".into(), qty.into());
    map.insert("order_type".into(), "stop".into());
    map.insert("stop_price".into(), price.into());
    map.into()
}

/// `buy_stop_limit(qty, stop, limit)` → `#{ action: "open_stock", side: "long", qty, order_type: "stop_limit", stop_price, limit_price }`
pub fn buy_stop_limit(qty: i64, stop: f64, limit: f64) -> Dynamic {
    let mut map = rhai::Map::new();
    map.insert("action".into(), "open_stock".into());
    map.insert("side".into(), "long".into());
    map.insert("qty".into(), qty.into());
    map.insert("order_type".into(), "stop_limit".into());
    map.insert("stop_price".into(), stop.into());
    map.insert("limit_price".into(), limit.into());
    map.into()
}

/// `sell_limit(qty, price)` → `#{ action: "open_stock", side: "short", qty, order_type: "limit", limit_price }`
pub fn sell_limit(qty: i64, price: f64) -> Dynamic {
    let mut map = rhai::Map::new();
    map.insert("action".into(), "open_stock".into());
    map.insert("side".into(), "short".into());
    map.insert("qty".into(), qty.into());
    map.insert("order_type".into(), "limit".into());
    map.insert("limit_price".into(), price.into());
    map.into()
}

/// `sell_stop(qty, price)` → `#{ action: "open_stock", side: "short", qty, order_type: "stop", stop_price }`
pub fn sell_stop(qty: i64, price: f64) -> Dynamic {
    let mut map = rhai::Map::new();
    map.insert("action".into(), "open_stock".into());
    map.insert("side".into(), "short".into());
    map.insert("qty".into(), qty.into());
    map.insert("order_type".into(), "stop".into());
    map.insert("stop_price".into(), price.into());
    map.into()
}

/// `sell_stop_limit(qty, stop, limit)` → `#{ action: "open_stock", side: "short", qty, order_type: "stop_limit", stop_price, limit_price }`
pub fn sell_stop_limit(qty: i64, stop: f64, limit: f64) -> Dynamic {
    let mut map = rhai::Map::new();
    map.insert("action".into(), "open_stock".into());
    map.insert("side".into(), "short".into());
    map.insert("qty".into(), qty.into());
    map.insert("order_type".into(), "stop_limit".into());
    map.insert("stop_price".into(), stop.into());
    map.insert("limit_price".into(), limit.into());
    map.into()
}

/// `cancel_orders()` → `#{ action: "cancel_orders" }`
pub fn cancel_orders() -> Dynamic {
    let mut map = rhai::Map::new();
    map.insert("action".into(), "cancel_orders".into());
    map.into()
}

/// `cancel_orders_by_signal(signal)` → `#{ action: "cancel_orders", signal }`
pub fn cancel_orders_by_signal(signal: String) -> Dynamic {
    let mut map = rhai::Map::new();
    map.insert("action".into(), "cancel_orders".into());
    map.insert("signal".into(), signal.into());
    map.into()
}

// ---------------------------------------------------------------------------
// Indicator utility (on BarContext)
// ---------------------------------------------------------------------------

impl BarContext {
    /// Check if all listed indicators have valid (non-NaN) values at the current bar.
    ///
    /// Usage: `ctx.indicators_ready(["sma:50", "rsi:14", "atr:14", "obv"])`
    pub fn indicators_ready(&mut self, indicators: rhai::Array) -> bool {
        use super::indicators::{parse_indicator_declaration, IndicatorKey, IndicatorParam};

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

    // -----------------------------------------------------------------------
    // Position sizing helpers
    // -----------------------------------------------------------------------

    /// Size by equity fraction: invest `fraction` of equity at current price.
    ///
    /// `fraction` > 1.0 allows leveraged sizing (e.g., 1.5 = 150% exposure).
    ///
    /// Usage: `ctx.size_by_equity(1.0)` → full equity, `ctx.size_by_equity(0.5)` → half
    pub fn size_by_equity(&mut self, fraction: f64) -> i64 {
        if self.close <= 0.0 || fraction <= 0.0 {
            return 0;
        }
        ((self.equity * fraction) / self.close).floor() as i64
    }

    /// Size by risk percentage: risk `risk_pct` of equity per trade with a defined stop.
    ///
    /// `risk_pct`: fraction of equity to risk (e.g., 0.02 = 2%)
    /// `stop_price`: price at which you'd exit (must be below current price).
    ///   e.g., `ctx.close - 2.0 * ctx.atr(14)`
    ///   Returns 0 if `stop_price >= close` (long-only: stop must be below entry).
    ///
    /// Usage: `ctx.size_by_risk(0.02, stop_price)` → shares where loss at stop = 2% of equity
    pub fn size_by_risk(&mut self, risk_pct: f64, stop_price: f64) -> i64 {
        let risk_per_share = self.close - stop_price;
        if risk_per_share <= 0.0 || risk_pct <= 0.0 {
            return 0;
        }
        let risk_amount = self.equity * risk_pct.min(1.0);
        (risk_amount / risk_per_share).floor() as i64
    }

    /// Size by target volatility: normalize position size by ATR so each trade
    /// has approximately equal dollar risk.
    ///
    /// `target_risk`: dollar risk per trade (e.g., 1000.0 = $1000 risk per position)
    /// `atr_period`: ATR lookback period (must be declared in config indicators)
    ///
    /// Usage: `ctx.size_by_volatility(1000.0, 14)` → shares where 1 ATR move = $1000
    pub fn size_by_volatility(&mut self, target_risk: f64, atr_period: i64) -> i64 {
        if target_risk <= 0.0 {
            return 0;
        }
        let atr_val = self.indicator_value("atr", atr_period);
        if atr_val.is_unit() {
            return 0;
        }
        let atr = match atr_val.as_float() {
            Ok(v) if v > 0.0 => v,
            _ => return 0,
        };
        let qty = (target_risk / atr).floor() as i64;
        // Cap at full equity worth of shares
        if self.close <= 0.0 {
            return 0;
        }
        let max_shares = (self.equity / self.close).floor() as i64;
        qty.min(max_shares).max(0)
    }

    /// Size by Kelly criterion: use historical win rate and avg win/loss to compute
    /// optimal fraction of equity to bet.
    ///
    /// `fraction`: Kelly fraction (0.0–1.0). Use 0.5 for half-Kelly (recommended).
    /// `lookback`: number of recent trades to consider (0 = all trades).
    /// Returns 0 if fewer than 20 closed trades (cold start).
    ///
    /// Usage: `ctx.size_by_kelly(0.5, 0)` → half-Kelly using all trade history
    pub fn size_by_kelly(&mut self, fraction: f64, lookback: i64) -> i64 {
        const MIN_TRADES: usize = 20;

        let trades = &*self.pnl_history;
        if trades.len() < MIN_TRADES {
            return 0;
        }

        // Slice to lookback window
        let window = if lookback > 0 && (lookback as usize) < trades.len() {
            &trades[trades.len() - lookback as usize..]
        } else {
            trades
        };

        if window.len() < MIN_TRADES {
            return 0;
        }

        let wins: Vec<f64> = window.iter().filter(|&&p| p > 0.0).copied().collect();
        let losses: Vec<f64> = window.iter().filter(|&&p| p < 0.0).copied().collect();

        if wins.is_empty() || losses.is_empty() {
            return 0;
        }

        let win_rate = wins.len() as f64 / window.len() as f64;
        let avg_win = wins.iter().sum::<f64>() / wins.len() as f64;
        let avg_loss = losses.iter().sum::<f64>().abs() / losses.len() as f64;

        if avg_loss <= 0.0 {
            return 0;
        }

        let win_loss_ratio = avg_win / avg_loss;

        // Kelly formula: f* = W - (1 - W) / R
        // where W = win rate, R = win/loss ratio
        let kelly_pct = win_rate - (1.0 - win_rate) / win_loss_ratio;

        if kelly_pct <= 0.0 {
            return 0;
        }

        // Apply fractional Kelly and compute shares
        if self.close <= 0.0 {
            return 0;
        }
        let bet_size = self.equity * kelly_pct * fraction.clamp(0.0, 1.0);
        let qty = (bet_size / self.close).floor() as i64;
        qty.max(0)
    }

    // -----------------------------------------------------------------------
    // Singles
    // -----------------------------------------------------------------------

    /// Long call: buy one call at the given delta and DTE.
    pub fn long_call(&mut self, call_delta: f64, dte: i64) -> Dynamic {
        let legs = vec![leg("long", "call", call_delta, dte)];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Short call: sell one call at the given delta and DTE.
    pub fn short_call(&mut self, call_delta: f64, dte: i64) -> Dynamic {
        let legs = vec![leg("short", "call", call_delta, dte)];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Long put: buy one put at the given delta and DTE.
    pub fn long_put(&mut self, put_delta: f64, dte: i64) -> Dynamic {
        let legs = vec![leg("long", "put", put_delta, dte)];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Short put: sell one put at the given delta and DTE.
    pub fn short_put(&mut self, put_delta: f64, dte: i64) -> Dynamic {
        let legs = vec![leg("short", "put", put_delta, dte)];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Covered call: sell one call (assumes stock already held).
    pub fn covered_call(&mut self, call_delta: f64, dte: i64) -> Dynamic {
        let legs = vec![leg("short", "call", call_delta, dte)];
        wrap_spread_action(self.build_strategy(legs))
    }

    // -----------------------------------------------------------------------
    // Vertical Spreads
    // -----------------------------------------------------------------------

    /// Bull call spread: buy higher-delta (lower-strike) call, sell lower-delta (higher-strike) call.
    pub fn bull_call_spread(
        &mut self,
        long_call_delta: f64,
        short_call_delta: f64,
        dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("long", "call", long_call_delta, dte),
            leg("short", "call", short_call_delta, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Bear call spread: sell higher-delta (lower-strike) call, buy lower-delta (higher-strike) call.
    pub fn bear_call_spread(
        &mut self,
        short_call_delta: f64,
        long_call_delta: f64,
        dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("short", "call", short_call_delta, dte),
            leg("long", "call", long_call_delta, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Bull put spread: sell higher-delta put, buy lower-delta put.
    pub fn bull_put_spread(
        &mut self,
        short_put_delta: f64,
        long_put_delta: f64,
        dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("short", "put", short_put_delta, dte),
            leg("long", "put", long_put_delta, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Bear put spread: buy higher-delta put, sell lower-delta put.
    pub fn bear_put_spread(
        &mut self,
        long_put_delta: f64,
        short_put_delta: f64,
        dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("long", "put", long_put_delta, dte),
            leg("short", "put", short_put_delta, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    // -----------------------------------------------------------------------
    // Straddles & Strangles
    // -----------------------------------------------------------------------

    /// Long straddle: buy call and put at specified deltas.
    pub fn long_straddle(&mut self, call_delta: f64, put_delta: f64, dte: i64) -> Dynamic {
        let legs = vec![
            leg("long", "call", call_delta, dte),
            leg("long", "put", put_delta, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Short straddle: sell call and put at specified deltas.
    pub fn short_straddle(&mut self, call_delta: f64, put_delta: f64, dte: i64) -> Dynamic {
        let legs = vec![
            leg("short", "call", call_delta, dte),
            leg("short", "put", put_delta, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Long strangle: buy OTM put and OTM call.
    pub fn long_strangle(&mut self, put_delta: f64, call_delta: f64, dte: i64) -> Dynamic {
        let legs = vec![
            leg("long", "put", put_delta, dte),
            leg("long", "call", call_delta, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Short strangle: sell OTM put and OTM call.
    pub fn short_strangle(&mut self, put_delta: f64, call_delta: f64, dte: i64) -> Dynamic {
        let legs = vec![
            leg("short", "put", put_delta, dte),
            leg("short", "call", call_delta, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    // -----------------------------------------------------------------------
    // Butterflies
    // -----------------------------------------------------------------------

    /// Long call butterfly: long lower wing, 2x short center, long upper wing.
    pub fn long_call_butterfly(
        &mut self,
        lower_call_delta: f64,
        center_call_delta: f64,
        upper_call_delta: f64,
        dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("long", "call", lower_call_delta, dte),
            leg("short", "call", center_call_delta, dte),
            leg("short", "call", center_call_delta, dte),
            leg("long", "call", upper_call_delta, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Short call butterfly: short lower wing, 2x long center, short upper wing.
    pub fn short_call_butterfly(
        &mut self,
        lower_call_delta: f64,
        center_call_delta: f64,
        upper_call_delta: f64,
        dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("short", "call", lower_call_delta, dte),
            leg("long", "call", center_call_delta, dte),
            leg("long", "call", center_call_delta, dte),
            leg("short", "call", upper_call_delta, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Long put butterfly: long lower wing, 2x short center, long upper wing.
    pub fn long_put_butterfly(
        &mut self,
        lower_put_delta: f64,
        center_put_delta: f64,
        upper_put_delta: f64,
        dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("long", "put", lower_put_delta, dte),
            leg("short", "put", center_put_delta, dte),
            leg("short", "put", center_put_delta, dte),
            leg("long", "put", upper_put_delta, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Short put butterfly: short lower wing, 2x long center, short upper wing.
    pub fn short_put_butterfly(
        &mut self,
        lower_put_delta: f64,
        center_put_delta: f64,
        upper_put_delta: f64,
        dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("short", "put", lower_put_delta, dte),
            leg("long", "put", center_put_delta, dte),
            leg("long", "put", center_put_delta, dte),
            leg("short", "put", upper_put_delta, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    // -----------------------------------------------------------------------
    // Condors (all same option type)
    // -----------------------------------------------------------------------

    /// Long call condor: long outer wings, short inner wings (all calls).
    pub fn long_call_condor(
        &mut self,
        outer_lower_call_delta: f64,
        inner_lower_call_delta: f64,
        inner_upper_call_delta: f64,
        outer_upper_call_delta: f64,
        dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("long", "call", outer_lower_call_delta, dte),
            leg("short", "call", inner_lower_call_delta, dte),
            leg("short", "call", inner_upper_call_delta, dte),
            leg("long", "call", outer_upper_call_delta, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Short call condor: short outer wings, long inner wings (all calls).
    pub fn short_call_condor(
        &mut self,
        outer_lower_call_delta: f64,
        inner_lower_call_delta: f64,
        inner_upper_call_delta: f64,
        outer_upper_call_delta: f64,
        dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("short", "call", outer_lower_call_delta, dte),
            leg("long", "call", inner_lower_call_delta, dte),
            leg("long", "call", inner_upper_call_delta, dte),
            leg("short", "call", outer_upper_call_delta, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Long put condor: long outer wings, short inner wings (all puts).
    pub fn long_put_condor(
        &mut self,
        outer_lower_put_delta: f64,
        inner_lower_put_delta: f64,
        inner_upper_put_delta: f64,
        outer_upper_put_delta: f64,
        dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("long", "put", outer_lower_put_delta, dte),
            leg("short", "put", inner_lower_put_delta, dte),
            leg("short", "put", inner_upper_put_delta, dte),
            leg("long", "put", outer_upper_put_delta, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Short put condor: short outer wings, long inner wings (all puts).
    pub fn short_put_condor(
        &mut self,
        outer_lower_put_delta: f64,
        inner_lower_put_delta: f64,
        inner_upper_put_delta: f64,
        outer_upper_put_delta: f64,
        dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("short", "put", outer_lower_put_delta, dte),
            leg("long", "put", inner_lower_put_delta, dte),
            leg("long", "put", inner_upper_put_delta, dte),
            leg("short", "put", outer_upper_put_delta, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    // -----------------------------------------------------------------------
    // Iron Strategies (mixed put + call)
    // -----------------------------------------------------------------------

    /// Iron condor: short put + long put wing + short call + long call wing.
    pub fn iron_condor(
        &mut self,
        short_put_delta: f64,
        long_put_delta: f64,
        short_call_delta: f64,
        long_call_delta: f64,
        dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("long", "put", long_put_delta, dte),
            leg("short", "put", short_put_delta, dte),
            leg("short", "call", short_call_delta, dte),
            leg("long", "call", long_call_delta, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Reverse iron condor: long put + short put wing + long call + short call wing.
    pub fn reverse_iron_condor(
        &mut self,
        long_put_delta: f64,
        short_put_delta: f64,
        long_call_delta: f64,
        short_call_delta: f64,
        dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("short", "put", short_put_delta, dte),
            leg("long", "put", long_put_delta, dte),
            leg("long", "call", long_call_delta, dte),
            leg("short", "call", short_call_delta, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Iron butterfly: short ATM put + long OTM put wing + short ATM call + long OTM call wing.
    pub fn iron_butterfly(
        &mut self,
        short_put_delta: f64,
        long_put_delta: f64,
        short_call_delta: f64,
        long_call_delta: f64,
        dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("long", "put", long_put_delta, dte),
            leg("short", "put", short_put_delta, dte),
            leg("short", "call", short_call_delta, dte),
            leg("long", "call", long_call_delta, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Reverse iron butterfly: long ATM put + short OTM put wing + long ATM call + short OTM call wing.
    pub fn reverse_iron_butterfly(
        &mut self,
        long_put_delta: f64,
        short_put_delta: f64,
        long_call_delta: f64,
        short_call_delta: f64,
        dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("short", "put", short_put_delta, dte),
            leg("long", "put", long_put_delta, dte),
            leg("long", "call", long_call_delta, dte),
            leg("short", "call", short_call_delta, dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    // -----------------------------------------------------------------------
    // Calendar & Diagonal (multi-expiration)
    // -----------------------------------------------------------------------

    /// Call calendar: short near-term call, long far-term call.
    pub fn call_calendar(
        &mut self,
        near_call_delta: f64,
        far_call_delta: f64,
        near_dte: i64,
        far_dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("short", "call", near_call_delta, near_dte),
            leg("long", "call", far_call_delta, far_dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Put calendar: short near-term put, long far-term put.
    pub fn put_calendar(
        &mut self,
        near_put_delta: f64,
        far_put_delta: f64,
        near_dte: i64,
        far_dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("short", "put", near_put_delta, near_dte),
            leg("long", "put", far_put_delta, far_dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Call diagonal: short near-term OTM call, long far-term ATM/ITM call.
    pub fn call_diagonal(
        &mut self,
        short_call_delta: f64,
        long_call_delta: f64,
        near_dte: i64,
        far_dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("short", "call", short_call_delta, near_dte),
            leg("long", "call", long_call_delta, far_dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Put diagonal: short near-term OTM put, long far-term ATM/ITM put.
    pub fn put_diagonal(
        &mut self,
        short_put_delta: f64,
        long_put_delta: f64,
        near_dte: i64,
        far_dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("short", "put", short_put_delta, near_dte),
            leg("long", "put", long_put_delta, far_dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Double calendar: put calendar + call calendar.
    pub fn double_calendar(
        &mut self,
        near_put_delta: f64,
        far_put_delta: f64,
        near_call_delta: f64,
        far_call_delta: f64,
        near_dte: i64,
        far_dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("short", "put", near_put_delta, near_dte),
            leg("long", "put", far_put_delta, far_dte),
            leg("short", "call", near_call_delta, near_dte),
            leg("long", "call", far_call_delta, far_dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }

    /// Double diagonal: put diagonal + call diagonal.
    pub fn double_diagonal(
        &mut self,
        short_put_delta: f64,
        long_put_delta: f64,
        short_call_delta: f64,
        long_call_delta: f64,
        near_dte: i64,
        far_dte: i64,
    ) -> Dynamic {
        let legs = vec![
            leg("short", "put", short_put_delta, near_dte),
            leg("long", "put", long_put_delta, far_dte),
            leg("short", "call", short_call_delta, near_dte),
            leg("long", "call", long_call_delta, far_dte),
        ];
        wrap_spread_action(self.build_strategy(legs))
    }
}
