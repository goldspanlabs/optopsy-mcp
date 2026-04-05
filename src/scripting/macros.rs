//! Declarative macros that generate indicator and options-strategy method impls
//! plus their Rhai registrations for any context type.
//!
//! Used by both `BarContext` and `SymbolContext` to eliminate duplication and
//! prevent feature drift (missing methods on one type but not the other).

/// Generate indicator accessor methods on a context type.
///
/// The type must have:
/// - `indicator_store: Arc<IndicatorStore>` field
/// - `bar_idx: usize` field
///
/// Produces: `sma`, `ema`, `rsi`, `atr`, `macd_line`, `macd_signal`, `macd_hist`,
/// `bbands_upper/mid/lower`, `stochastic`, `cci`, `obv`, `adx`, `plus_di`, `minus_di`,
/// `keltner_upper/lower`, `psar`, `supertrend`, `donchian_upper/mid/lower`,
/// `williams_r`, `mfi`, `rank`, `iv_rank`, `tr`, `indicator`, `indicator_with`,
/// `indicators_ready`.
macro_rules! impl_indicators {
    ($ty:ty) => {
        impl $ty {
            // --- Single-param indicators ---
            pub fn sma(&mut self, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup(
                    &self.indicator_store,
                    self.bar_idx,
                    "sma",
                    period,
                )
            }
            pub fn ema(&mut self, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup(
                    &self.indicator_store,
                    self.bar_idx,
                    "ema",
                    period,
                )
            }
            pub fn rsi(&mut self, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup(
                    &self.indicator_store,
                    self.bar_idx,
                    "rsi",
                    period,
                )
            }
            pub fn atr(&mut self, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup(
                    &self.indicator_store,
                    self.bar_idx,
                    "atr",
                    period,
                )
            }
            pub fn stochastic(&mut self, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup(
                    &self.indicator_store,
                    self.bar_idx,
                    "stochastic",
                    period,
                )
            }
            pub fn adx(&mut self, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup(
                    &self.indicator_store,
                    self.bar_idx,
                    "adx",
                    period,
                )
            }
            pub fn cci(&mut self, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup(
                    &self.indicator_store,
                    self.bar_idx,
                    "cci",
                    period,
                )
            }
            pub fn plus_di(&mut self, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup(
                    &self.indicator_store,
                    self.bar_idx,
                    "plus_di",
                    period,
                )
            }
            pub fn minus_di(&mut self, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup(
                    &self.indicator_store,
                    self.bar_idx,
                    "minus_di",
                    period,
                )
            }
            pub fn keltner_upper(&mut self, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup(
                    &self.indicator_store,
                    self.bar_idx,
                    "keltner_upper",
                    period,
                )
            }
            pub fn keltner_lower(&mut self, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup(
                    &self.indicator_store,
                    self.bar_idx,
                    "keltner_lower",
                    period,
                )
            }
            pub fn supertrend(&mut self, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup(
                    &self.indicator_store,
                    self.bar_idx,
                    "supertrend",
                    period,
                )
            }
            pub fn bbands_mid(&mut self, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup(
                    &self.indicator_store,
                    self.bar_idx,
                    "bbands_mid",
                    period,
                )
            }
            pub fn donchian_upper(&mut self, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup(
                    &self.indicator_store,
                    self.bar_idx,
                    "donchian_upper",
                    period,
                )
            }
            pub fn donchian_mid(&mut self, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup(
                    &self.indicator_store,
                    self.bar_idx,
                    "donchian_mid",
                    period,
                )
            }
            pub fn donchian_lower(&mut self, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup(
                    &self.indicator_store,
                    self.bar_idx,
                    "donchian_lower",
                    period,
                )
            }
            pub fn williams_r(&mut self, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup(
                    &self.indicator_store,
                    self.bar_idx,
                    "williams_r",
                    period,
                )
            }
            pub fn mfi(&mut self, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup(
                    &self.indicator_store,
                    self.bar_idx,
                    "mfi",
                    period,
                )
            }
            pub fn rank(&mut self, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup(
                    &self.indicator_store,
                    self.bar_idx,
                    "rank",
                    period,
                )
            }
            pub fn iv_rank(&mut self, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup(
                    &self.indicator_store,
                    self.bar_idx,
                    "iv_rank",
                    period,
                )
            }

            // --- Multi-param indicators ---
            pub fn macd_line(&mut self) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup_multi(
                    &self.indicator_store,
                    self.bar_idx,
                    "macd_line",
                    &[12, 26, 9],
                )
            }
            pub fn macd_signal(&mut self) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup_multi(
                    &self.indicator_store,
                    self.bar_idx,
                    "macd_signal",
                    &[12, 26, 9],
                )
            }
            pub fn macd_hist(&mut self) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup_multi(
                    &self.indicator_store,
                    self.bar_idx,
                    "macd_hist",
                    &[12, 26, 9],
                )
            }
            pub fn bbands_upper(&mut self, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup_multi(
                    &self.indicator_store,
                    self.bar_idx,
                    "bbands_upper",
                    &[period, 20],
                )
            }
            pub fn bbands_lower(&mut self, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup_multi(
                    &self.indicator_store,
                    self.bar_idx,
                    "bbands_lower",
                    &[period, 20],
                )
            }
            pub fn obv(&mut self) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup_multi(
                    &self.indicator_store,
                    self.bar_idx,
                    "obv",
                    &[],
                )
            }
            pub fn psar(&mut self) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup_multi(
                    &self.indicator_store,
                    self.bar_idx,
                    "psar",
                    &[2, 20],
                )
            }
            pub fn tr(&mut self) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup_multi(
                    &self.indicator_store,
                    self.bar_idx,
                    "tr",
                    &[],
                )
            }

            // --- Generic accessors ---
            pub fn indicator(&mut self, name: String, period: i64) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup(
                    &self.indicator_store,
                    self.bar_idx,
                    &name,
                    period,
                )
            }
            pub fn indicator_with(&mut self, name: String, params: rhai::Map) -> rhai::Dynamic {
                $crate::scripting::helpers::indicator_lookup_map(
                    &self.indicator_store,
                    self.bar_idx,
                    name,
                    params,
                )
            }
            pub fn indicators_ready(&mut self, indicators: rhai::Array) -> bool {
                $crate::scripting::helpers::indicators_all_ready(
                    &self.indicator_store,
                    self.bar_idx,
                    indicators,
                )
            }
        }
    };
}

/// Register indicator methods for a context type with a Rhai engine.
macro_rules! register_indicators {
    ($engine:expr, $ty:ty) => {
        // Single-param
        $engine.register_fn("sma", <$ty>::sma);
        $engine.register_fn("ema", <$ty>::ema);
        $engine.register_fn("rsi", <$ty>::rsi);
        $engine.register_fn("atr", <$ty>::atr);
        $engine.register_fn("stochastic", <$ty>::stochastic);
        $engine.register_fn("cci", <$ty>::cci);
        $engine.register_fn("adx", <$ty>::adx);
        $engine.register_fn("plus_di", <$ty>::plus_di);
        $engine.register_fn("minus_di", <$ty>::minus_di);
        $engine.register_fn("keltner_upper", <$ty>::keltner_upper);
        $engine.register_fn("keltner_lower", <$ty>::keltner_lower);
        $engine.register_fn("supertrend", <$ty>::supertrend);
        $engine.register_fn("bbands_mid", <$ty>::bbands_mid);
        $engine.register_fn("donchian_upper", <$ty>::donchian_upper);
        $engine.register_fn("donchian_mid", <$ty>::donchian_mid);
        $engine.register_fn("donchian_lower", <$ty>::donchian_lower);
        $engine.register_fn("williams_r", <$ty>::williams_r);
        $engine.register_fn("mfi", <$ty>::mfi);
        $engine.register_fn("rank", <$ty>::rank);
        $engine.register_fn("iv_rank", <$ty>::iv_rank);

        // Multi-param / no-param
        $engine.register_fn("macd_line", <$ty>::macd_line);
        $engine.register_fn("macd_signal", <$ty>::macd_signal);
        $engine.register_fn("macd_hist", <$ty>::macd_hist);
        $engine.register_fn("bbands_upper", <$ty>::bbands_upper);
        $engine.register_fn("bbands_lower", <$ty>::bbands_lower);
        $engine.register_fn("obv", <$ty>::obv);
        $engine.register_fn("psar", <$ty>::psar);
        $engine.register_fn("tr", <$ty>::tr);

        // Generic + multi-param
        $engine.register_fn("indicator", <$ty>::indicator);
        $engine.register_fn("indicator_with", <$ty>::indicator_with);
        $engine.register_fn("indicators_ready", <$ty>::indicators_ready);
    };
}

/// Generate options strategy methods on a context type.
///
/// The type must have:
/// - `fn build_strategy(&mut self, legs: rhai::Array) -> rhai::Dynamic`
/// - `fn wrap_strategy_action(spread: rhai::Dynamic) -> rhai::Dynamic` (associated fn)
///
/// Produces all 23 named strategy builders (singles through calendar/diagonal).
macro_rules! impl_options_strategies {
    ($ty:ty) => {
        impl $ty {
            // Singles
            pub fn long_call(&mut self, delta: f64, dte: i64) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("long", "call", delta, dte),
                ]))
            }
            pub fn short_call(&mut self, delta: f64, dte: i64) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("short", "call", delta, dte),
                ]))
            }
            pub fn long_put(&mut self, delta: f64, dte: i64) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("long", "put", delta, dte),
                ]))
            }
            pub fn short_put(&mut self, delta: f64, dte: i64) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("short", "put", delta, dte),
                ]))
            }
            pub fn covered_call(&mut self, delta: f64, dte: i64) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("short", "call", delta, dte),
                ]))
            }

            // Vertical spreads
            pub fn bull_call_spread(
                &mut self,
                long_d: f64,
                short_d: f64,
                dte: i64,
            ) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("long", "call", long_d, dte),
                    $crate::scripting::helpers::leg("short", "call", short_d, dte),
                ]))
            }
            pub fn bear_call_spread(
                &mut self,
                short_d: f64,
                long_d: f64,
                dte: i64,
            ) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("short", "call", short_d, dte),
                    $crate::scripting::helpers::leg("long", "call", long_d, dte),
                ]))
            }
            pub fn bull_put_spread(
                &mut self,
                short_d: f64,
                long_d: f64,
                dte: i64,
            ) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("short", "put", short_d, dte),
                    $crate::scripting::helpers::leg("long", "put", long_d, dte),
                ]))
            }
            pub fn bear_put_spread(
                &mut self,
                long_d: f64,
                short_d: f64,
                dte: i64,
            ) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("long", "put", long_d, dte),
                    $crate::scripting::helpers::leg("short", "put", short_d, dte),
                ]))
            }

            // Straddles & strangles
            pub fn long_straddle(&mut self, call_d: f64, put_d: f64, dte: i64) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("long", "call", call_d, dte),
                    $crate::scripting::helpers::leg("long", "put", put_d, dte),
                ]))
            }
            pub fn short_straddle(&mut self, call_d: f64, put_d: f64, dte: i64) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("short", "call", call_d, dte),
                    $crate::scripting::helpers::leg("short", "put", put_d, dte),
                ]))
            }
            pub fn long_strangle(&mut self, put_d: f64, call_d: f64, dte: i64) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("long", "put", put_d, dte),
                    $crate::scripting::helpers::leg("long", "call", call_d, dte),
                ]))
            }
            pub fn short_strangle(&mut self, put_d: f64, call_d: f64, dte: i64) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("short", "put", put_d, dte),
                    $crate::scripting::helpers::leg("short", "call", call_d, dte),
                ]))
            }

            // Butterflies
            pub fn long_call_butterfly(
                &mut self,
                lower: f64,
                center: f64,
                upper: f64,
                dte: i64,
            ) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("long", "call", lower, dte),
                    $crate::scripting::helpers::leg("short", "call", center, dte),
                    $crate::scripting::helpers::leg("short", "call", center, dte),
                    $crate::scripting::helpers::leg("long", "call", upper, dte),
                ]))
            }
            pub fn short_call_butterfly(
                &mut self,
                lower: f64,
                center: f64,
                upper: f64,
                dte: i64,
            ) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("short", "call", lower, dte),
                    $crate::scripting::helpers::leg("long", "call", center, dte),
                    $crate::scripting::helpers::leg("long", "call", center, dte),
                    $crate::scripting::helpers::leg("short", "call", upper, dte),
                ]))
            }
            pub fn long_put_butterfly(
                &mut self,
                lower: f64,
                center: f64,
                upper: f64,
                dte: i64,
            ) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("long", "put", lower, dte),
                    $crate::scripting::helpers::leg("short", "put", center, dte),
                    $crate::scripting::helpers::leg("short", "put", center, dte),
                    $crate::scripting::helpers::leg("long", "put", upper, dte),
                ]))
            }
            pub fn short_put_butterfly(
                &mut self,
                lower: f64,
                center: f64,
                upper: f64,
                dte: i64,
            ) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("short", "put", lower, dte),
                    $crate::scripting::helpers::leg("long", "put", center, dte),
                    $crate::scripting::helpers::leg("long", "put", center, dte),
                    $crate::scripting::helpers::leg("short", "put", upper, dte),
                ]))
            }

            // Condors (all same option type)
            pub fn long_call_condor(
                &mut self,
                ol: f64,
                il: f64,
                iu: f64,
                ou: f64,
                dte: i64,
            ) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("long", "call", ol, dte),
                    $crate::scripting::helpers::leg("short", "call", il, dte),
                    $crate::scripting::helpers::leg("short", "call", iu, dte),
                    $crate::scripting::helpers::leg("long", "call", ou, dte),
                ]))
            }
            pub fn short_call_condor(
                &mut self,
                ol: f64,
                il: f64,
                iu: f64,
                ou: f64,
                dte: i64,
            ) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("short", "call", ol, dte),
                    $crate::scripting::helpers::leg("long", "call", il, dte),
                    $crate::scripting::helpers::leg("long", "call", iu, dte),
                    $crate::scripting::helpers::leg("short", "call", ou, dte),
                ]))
            }
            pub fn long_put_condor(
                &mut self,
                ol: f64,
                il: f64,
                iu: f64,
                ou: f64,
                dte: i64,
            ) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("long", "put", ol, dte),
                    $crate::scripting::helpers::leg("short", "put", il, dte),
                    $crate::scripting::helpers::leg("short", "put", iu, dte),
                    $crate::scripting::helpers::leg("long", "put", ou, dte),
                ]))
            }
            pub fn short_put_condor(
                &mut self,
                ol: f64,
                il: f64,
                iu: f64,
                ou: f64,
                dte: i64,
            ) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("short", "put", ol, dte),
                    $crate::scripting::helpers::leg("long", "put", il, dte),
                    $crate::scripting::helpers::leg("long", "put", iu, dte),
                    $crate::scripting::helpers::leg("short", "put", ou, dte),
                ]))
            }

            // Iron strategies
            pub fn iron_condor(
                &mut self,
                sp: f64,
                lp: f64,
                sc: f64,
                lc: f64,
                dte: i64,
            ) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("long", "put", lp, dte),
                    $crate::scripting::helpers::leg("short", "put", sp, dte),
                    $crate::scripting::helpers::leg("short", "call", sc, dte),
                    $crate::scripting::helpers::leg("long", "call", lc, dte),
                ]))
            }
            pub fn reverse_iron_condor(
                &mut self,
                lp: f64,
                sp: f64,
                lc: f64,
                sc: f64,
                dte: i64,
            ) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("short", "put", sp, dte),
                    $crate::scripting::helpers::leg("long", "put", lp, dte),
                    $crate::scripting::helpers::leg("long", "call", lc, dte),
                    $crate::scripting::helpers::leg("short", "call", sc, dte),
                ]))
            }
            pub fn iron_butterfly(
                &mut self,
                sp: f64,
                lp: f64,
                sc: f64,
                lc: f64,
                dte: i64,
            ) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("long", "put", lp, dte),
                    $crate::scripting::helpers::leg("short", "put", sp, dte),
                    $crate::scripting::helpers::leg("short", "call", sc, dte),
                    $crate::scripting::helpers::leg("long", "call", lc, dte),
                ]))
            }
            pub fn reverse_iron_butterfly(
                &mut self,
                lp: f64,
                sp: f64,
                lc: f64,
                sc: f64,
                dte: i64,
            ) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("short", "put", sp, dte),
                    $crate::scripting::helpers::leg("long", "put", lp, dte),
                    $crate::scripting::helpers::leg("long", "call", lc, dte),
                    $crate::scripting::helpers::leg("short", "call", sc, dte),
                ]))
            }

            // Calendar & diagonal
            pub fn call_calendar(
                &mut self,
                near_d: f64,
                far_d: f64,
                near_dte: i64,
                far_dte: i64,
            ) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("short", "call", near_d, near_dte),
                    $crate::scripting::helpers::leg("long", "call", far_d, far_dte),
                ]))
            }
            pub fn put_calendar(
                &mut self,
                near_d: f64,
                far_d: f64,
                near_dte: i64,
                far_dte: i64,
            ) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("short", "put", near_d, near_dte),
                    $crate::scripting::helpers::leg("long", "put", far_d, far_dte),
                ]))
            }
            pub fn call_diagonal(
                &mut self,
                short_d: f64,
                long_d: f64,
                near_dte: i64,
                far_dte: i64,
            ) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("short", "call", short_d, near_dte),
                    $crate::scripting::helpers::leg("long", "call", long_d, far_dte),
                ]))
            }
            pub fn put_diagonal(
                &mut self,
                short_d: f64,
                long_d: f64,
                near_dte: i64,
                far_dte: i64,
            ) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("short", "put", short_d, near_dte),
                    $crate::scripting::helpers::leg("long", "put", long_d, far_dte),
                ]))
            }
            pub fn double_calendar(
                &mut self,
                np: f64,
                fp: f64,
                nc: f64,
                fc: f64,
                near_dte: i64,
                far_dte: i64,
            ) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("short", "put", np, near_dte),
                    $crate::scripting::helpers::leg("long", "put", fp, far_dte),
                    $crate::scripting::helpers::leg("short", "call", nc, near_dte),
                    $crate::scripting::helpers::leg("long", "call", fc, far_dte),
                ]))
            }
            pub fn double_diagonal(
                &mut self,
                sp: f64,
                lp: f64,
                sc: f64,
                lc: f64,
                near_dte: i64,
                far_dte: i64,
            ) -> rhai::Dynamic {
                <$ty>::wrap_strategy_action(self.build_strategy(vec![
                    $crate::scripting::helpers::leg("short", "put", sp, near_dte),
                    $crate::scripting::helpers::leg("long", "put", lp, far_dte),
                    $crate::scripting::helpers::leg("short", "call", sc, near_dte),
                    $crate::scripting::helpers::leg("long", "call", lc, far_dte),
                ]))
            }
        }
    };
}

/// Register options strategy methods for a context type with a Rhai engine.
macro_rules! register_options_strategies {
    ($engine:expr, $ty:ty) => {
        $engine.register_fn("build_strategy", <$ty>::build_strategy);

        // Singles
        $engine.register_fn("long_call", <$ty>::long_call);
        $engine.register_fn("short_call", <$ty>::short_call);
        $engine.register_fn("long_put", <$ty>::long_put);
        $engine.register_fn("short_put", <$ty>::short_put);
        $engine.register_fn("covered_call", <$ty>::covered_call);

        // Vertical spreads
        $engine.register_fn("bull_call_spread", <$ty>::bull_call_spread);
        $engine.register_fn("bear_call_spread", <$ty>::bear_call_spread);
        $engine.register_fn("bull_put_spread", <$ty>::bull_put_spread);
        $engine.register_fn("bear_put_spread", <$ty>::bear_put_spread);

        // Straddles & strangles
        $engine.register_fn("long_straddle", <$ty>::long_straddle);
        $engine.register_fn("short_straddle", <$ty>::short_straddle);
        $engine.register_fn("long_strangle", <$ty>::long_strangle);
        $engine.register_fn("short_strangle", <$ty>::short_strangle);

        // Butterflies
        $engine.register_fn("long_call_butterfly", <$ty>::long_call_butterfly);
        $engine.register_fn("short_call_butterfly", <$ty>::short_call_butterfly);
        $engine.register_fn("long_put_butterfly", <$ty>::long_put_butterfly);
        $engine.register_fn("short_put_butterfly", <$ty>::short_put_butterfly);

        // Condors
        $engine.register_fn("long_call_condor", <$ty>::long_call_condor);
        $engine.register_fn("short_call_condor", <$ty>::short_call_condor);
        $engine.register_fn("long_put_condor", <$ty>::long_put_condor);
        $engine.register_fn("short_put_condor", <$ty>::short_put_condor);

        // Iron strategies
        $engine.register_fn("iron_condor", <$ty>::iron_condor);
        $engine.register_fn("reverse_iron_condor", <$ty>::reverse_iron_condor);
        $engine.register_fn("iron_butterfly", <$ty>::iron_butterfly);
        $engine.register_fn("reverse_iron_butterfly", <$ty>::reverse_iron_butterfly);

        // Calendar & diagonal
        $engine.register_fn("call_calendar", <$ty>::call_calendar);
        $engine.register_fn("put_calendar", <$ty>::put_calendar);
        $engine.register_fn("call_diagonal", <$ty>::call_diagonal);
        $engine.register_fn("put_diagonal", <$ty>::put_diagonal);
        $engine.register_fn("double_calendar", <$ty>::double_calendar);
        $engine.register_fn("double_diagonal", <$ty>::double_diagonal);
    };
}
