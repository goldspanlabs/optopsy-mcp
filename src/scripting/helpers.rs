//! High-level Rhai helper functions for strategy construction and action building.
//!
//! These helpers reduce boilerplate in scripts by providing named strategy
//! constructors (e.g., `ctx.bull_put_spread(...)`) and action builders
//! (e.g., `hold_position()`, `close_position(...)`).
//!
//! All strategy helpers delegate to `BarContext::build_strategy()` internally
//! and return ready-to-use action maps or `()` if leg resolution fails.

use rhai::Dynamic;

use super::types::BarContext;

// ---------------------------------------------------------------------------
// Internal: build a leg map for passing to build_strategy()
// ---------------------------------------------------------------------------

fn leg(side: &str, option_type: &str, delta: f64, dte: i64) -> Dynamic {
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
