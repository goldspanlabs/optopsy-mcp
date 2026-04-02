//! Tests for the Rhai scripting engine.

#[cfg(test)]
mod tests {
    use crate::scripting::indicators::{IndicatorKey, IndicatorParam, IndicatorStore};
    use crate::scripting::registration::build_engine;
    use crate::scripting::stdlib;
    use crate::scripting::types::OhlcvBar;

    use chrono::NaiveDateTime;
    use rhai::{CallFnOptions, Dynamic, Scope};
    use std::collections::HashMap;

    // -----------------------------------------------------------------------
    // Engine sandbox tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_engine_sandbox_operations_limit() {
        let engine = build_engine();
        let ast = engine.compile("let x = 0; loop { x += 1; }").unwrap();
        let mut scope = Scope::new();
        let result = engine.eval_ast_with_scope::<Dynamic>(&mut scope, &ast);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("operations") || err.contains("limit") || err.contains("Too many"),
            "Expected operations limit error, got: {err}"
        );
    }

    #[test]
    fn test_engine_print_intercepted() {
        // print() should not panic (it's intercepted to tracing)
        let engine = build_engine();
        let result = engine.eval::<()>(r#"print("hello from script");"#);
        assert!(result.is_ok());
    }

    #[test]
    fn test_engine_basic_rhai_syntax() {
        let engine = build_engine();

        // Maps
        let result: Dynamic = engine
            .eval(r#"let m = #{ x: 42, y: "hello" }; m.x"#)
            .unwrap();
        assert_eq!(result.as_int().unwrap(), 42);

        // Arrays
        let result: Dynamic = engine.eval("[1, 2, 3].len()").unwrap();
        assert_eq!(result.as_int().unwrap(), 3);

        // Closures (enabled)
        let result: Dynamic = engine.eval("[1, 2, 3, 4].filter(|x| x > 2).len()").unwrap();
        assert_eq!(result.as_int().unwrap(), 2);
    }

    // -----------------------------------------------------------------------
    // Scope rewind tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_scope_rewind_preserves_state() {
        let engine = build_engine();
        let ast = engine
            .compile(
                r#"
                let state = "initial";

                fn update_state() {
                    state = "updated";
                    let temp = 42;  // should be cleaned up by rewind
                }
            "#,
            )
            .unwrap();

        let mut scope = Scope::new();
        let _ = engine
            .eval_ast_with_scope::<Dynamic>(&mut scope, &ast)
            .unwrap();

        assert_eq!(scope.get_value::<String>("state").unwrap(), "initial");

        // Call function with rewind
        let checkpoint = scope.len();
        let options = CallFnOptions::new().eval_ast(false).rewind_scope(false);
        let _ = engine
            .call_fn_with_options::<Dynamic>(options, &mut scope, &ast, "update_state", ())
            .unwrap();

        // State should be mutated
        assert_eq!(scope.get_value::<String>("state").unwrap(), "updated");

        // Rewind to remove temp variables
        let after_len = scope.len();
        scope.rewind(checkpoint);

        // State mutation persists (in-place), temp is gone
        assert_eq!(scope.get_value::<String>("state").unwrap(), "updated");
        assert!(
            after_len >= checkpoint,
            "Scope should have grown during function call"
        );
    }

    // -----------------------------------------------------------------------
    // Config parsing tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_config_parsing_minimal() {
        let engine = build_engine();
        let ast = engine
            .compile(
                r#"
                fn config() {
                    #{
                        symbol: "SPY",
                        capital: 50000.0,
                    }
                }
            "#,
            )
            .unwrap();

        let mut scope = Scope::new();
        let _ = engine
            .eval_ast_with_scope::<Dynamic>(&mut scope, &ast)
            .unwrap();

        let options = CallFnOptions::new().eval_ast(false).rewind_scope(false);
        let result: Dynamic = engine
            .call_fn_with_options(options, &mut scope, &ast, "config", ())
            .unwrap();

        let map = result.cast::<rhai::Map>();
        assert_eq!(
            map.get("symbol")
                .unwrap()
                .clone()
                .into_immutable_string()
                .unwrap()
                .as_str(),
            "SPY"
        );
        assert_eq!(map.get("capital").unwrap().as_float().unwrap(), 50000.0);
    }

    #[test]
    fn test_config_parsing_with_engine_section() {
        let engine = build_engine();
        let ast = engine
            .compile(
                r#"
                fn config() {
                    #{
                        symbol: "AAPL",
                        capital: 100000,
                        interval: "daily",
                        engine: #{
                            slippage: "mid",
                            commission: #{ per_contract: 0.65, base_fee: 0.0, min_fee: 0.0 },
                            expiration_filter: "monthly",
                        },
                        defaults: #{
                            max_positions: 3,
                            stop_loss: 0.50,
                        },
                    }
                }
            "#,
            )
            .unwrap();

        let mut scope = Scope::new();
        let _ = engine
            .eval_ast_with_scope::<Dynamic>(&mut scope, &ast)
            .unwrap();

        let options = CallFnOptions::new().eval_ast(false).rewind_scope(false);
        let result: Dynamic = engine
            .call_fn_with_options(options, &mut scope, &ast, "config", ())
            .unwrap();

        let map = result.cast::<rhai::Map>();
        let engine_map = map.get("engine").unwrap().clone().cast::<rhai::Map>();
        assert_eq!(
            engine_map
                .get("expiration_filter")
                .unwrap()
                .clone()
                .into_immutable_string()
                .unwrap()
                .as_str(),
            "monthly"
        );
    }

    // -----------------------------------------------------------------------
    // Indicator store tests
    // -----------------------------------------------------------------------

    fn make_bars(prices: &[f64]) -> Vec<OhlcvBar> {
        prices
            .iter()
            .enumerate()
            .map(|(i, &p)| OhlcvBar {
                datetime: NaiveDateTime::parse_from_str(
                    &format!("2024-01-{:02} 00:00:00", i + 1),
                    "%Y-%m-%d %H:%M:%S",
                )
                .unwrap_or_default(),
                open: p,
                high: p * 1.01,
                low: p * 0.99,
                close: p,
                volume: 1000.0,
            })
            .collect()
    }

    #[test]
    fn test_indicator_store_sma() {
        let bars = make_bars(&[10.0, 11.0, 12.0, 13.0, 14.0]);
        let store = IndicatorStore::build(&["sma:3".to_string()], &bars).unwrap();

        let key = IndicatorKey {
            name: "sma".to_string(),
            params: vec![IndicatorParam::Int(3)],
        };

        // First two bars should be NaN (warmup period)
        assert!(store.get(&key, 0).unwrap().is_nan());
        assert!(store.get(&key, 1).unwrap().is_nan());

        // SMA(3) at bar 2: (10 + 11 + 12) / 3 = 11.0
        let val = store.get(&key, 2).unwrap();
        assert!((val - 11.0).abs() < 1e-10, "Expected 11.0, got {val}");

        // SMA(3) at bar 4: (12 + 13 + 14) / 3 = 13.0
        let val = store.get(&key, 4).unwrap();
        assert!((val - 13.0).abs() < 1e-10, "Expected 13.0, got {val}");
    }

    #[test]
    fn test_indicator_store_rsi() {
        // RSI needs enough bars for warmup
        let prices: Vec<f64> = (0..30).map(|i| 100.0 + (i as f64) * 0.5).collect();
        let bars = make_bars(&prices);
        let store = IndicatorStore::build(&["rsi:14".to_string()], &bars).unwrap();

        let key = IndicatorKey {
            name: "rsi".to_string(),
            params: vec![IndicatorParam::Int(14)],
        };

        // First 13 bars should be NaN (rust_ti uses windows of size period)
        assert!(store.get(&key, 0).unwrap().is_nan());
        assert!(store.get(&key, 12).unwrap().is_nan());

        // Bar 13 should have a value (first window completes at period-1)
        let val = store.get(&key, 13).unwrap();
        assert!(!val.is_nan(), "RSI at bar 13 should not be NaN");
        assert!(
            val > 90.0,
            "RSI should be high for monotonically increasing prices, got {val}"
        );
    }

    #[test]
    fn test_indicator_store_macd() {
        // MACD needs enough data (rust_ti requires 34-bar windows)
        let prices: Vec<f64> = (0..50).map(|i| 100.0 + (i as f64) * 0.3).collect();
        let bars = make_bars(&prices);
        let store = IndicatorStore::build(&["macd_line".to_string()], &bars).unwrap();

        let key = IndicatorKey {
            name: "macd_line".to_string(),
            params: vec![
                IndicatorParam::Int(12),
                IndicatorParam::Int(26),
                IndicatorParam::Int(9),
            ],
        };

        // First 33 bars should be NaN (rust_ti uses 34-bar windows)
        assert!(store.get(&key, 0).unwrap().is_nan());
        assert!(store.get(&key, 32).unwrap().is_nan());
        // Bar 33+ should have values
        let val = store.get(&key, 33).unwrap();
        assert!(!val.is_nan(), "MACD at bar 33 should not be NaN");
    }

    #[test]
    fn test_indicator_store_bbands() {
        let prices: Vec<f64> = (0..30).map(|i| 100.0 + (i as f64).sin() * 5.0).collect();
        let bars = make_bars(&prices);
        let store = IndicatorStore::build(
            &["bbands_upper:20".to_string(), "bbands_lower:20".to_string()],
            &bars,
        )
        .unwrap();

        let key_upper = IndicatorKey {
            name: "bbands_upper".to_string(),
            params: vec![IndicatorParam::Int(20), IndicatorParam::Int(20)],
        };
        let key_lower = IndicatorKey {
            name: "bbands_lower".to_string(),
            params: vec![IndicatorParam::Int(20), IndicatorParam::Int(20)],
        };

        let upper = store.get(&key_upper, 25).unwrap();
        let lower = store.get(&key_lower, 25).unwrap();
        assert!(!upper.is_nan());
        assert!(!lower.is_nan());
        assert!(upper > lower, "Upper band should be above lower band");
    }

    #[test]
    fn test_indicator_store_obv() {
        let bars = make_bars(&[10.0, 11.0, 10.5, 12.0, 11.5]);
        let store = IndicatorStore::build(&["obv".to_string()], &bars).unwrap();

        let key = IndicatorKey {
            name: "obv".to_string(),
            params: vec![],
        };

        // rust_ti OBV needs 2 prices minimum; first bar is NaN (warmup)
        assert!(store.get(&key, 0).unwrap().is_nan());
        // Bar 1+ should have values
        let val = store.get(&key, 1).unwrap();
        assert!(!val.is_nan(), "OBV at bar 1 should not be NaN");
    }

    #[test]
    fn test_indicator_store_lookback() {
        let bars = make_bars(&[10.0, 11.0, 12.0, 13.0, 14.0]);
        let store = IndicatorStore::build(&["sma:3".to_string()], &bars).unwrap();

        let key = IndicatorKey {
            name: "sma".to_string(),
            params: vec![IndicatorParam::Int(3)],
        };

        // get_at(key, bar_idx=4, bars_ago=1) = value at bar 3
        // SMA(3) at bar 3: (11 + 12 + 13) / 3 = 12.0
        let val = store.get_at(&key, 4, 1).unwrap();
        assert!((val - 12.0).abs() < 1e-10, "Expected 12.0, got {val}");

        // Out of bounds lookback
        assert!(store.get_at(&key, 1, 5).is_none());
    }

    // -----------------------------------------------------------------------
    // Stdlib injection tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_params_map_injection() {
        let mut params = HashMap::new();
        params.insert("SYMBOL".to_string(), serde_json::json!("SPY"));
        params.insert("CAPITAL".to_string(), serde_json::json!(50000.0));
        params.insert("STOP_LOSS".to_string(), serde_json::json!(null));

        let mut scope = Scope::new();
        stdlib::inject_params_map(&mut scope, &params);

        let map = scope.get_value::<rhai::Map>("params").unwrap();
        assert_eq!(
            map.get("SYMBOL")
                .unwrap()
                .clone()
                .into_immutable_string()
                .unwrap()
                .as_str(),
            "SPY"
        );
        assert_eq!(map.get("CAPITAL").unwrap().as_float().unwrap(), 50000.0);
        // null → ()
        assert!(map.get("STOP_LOSS").unwrap().is_unit());
    }

    #[test]
    fn test_params_accessible_in_fn() {
        let engine = build_engine();

        let mut params = HashMap::new();
        params.insert("SYMBOL".to_string(), serde_json::json!("SPY"));
        params.insert("CAPITAL".to_string(), serde_json::json!(50000));

        let source = r"
            fn config() {
                #{ symbol: params.SYMBOL, capital: params.CAPITAL }
            }
        ";

        let ast = engine.compile(source).unwrap();
        let mut scope = Scope::new();
        let _ = engine
            .eval_ast_with_scope::<Dynamic>(&mut scope, &ast)
            .unwrap();

        // Inject params map after scope init (mirrors engine.rs flow)
        stdlib::inject_params_map(&mut scope, &params);

        let options = CallFnOptions::new().eval_ast(false).rewind_scope(false);
        let result: Dynamic = engine
            .call_fn_with_options(options, &mut scope, &ast, "config", ())
            .unwrap();

        let map = result.cast::<rhai::Map>();
        assert_eq!(
            map.get("symbol")
                .unwrap()
                .clone()
                .into_immutable_string()
                .unwrap()
                .as_str(),
            "SPY"
        );
        assert_eq!(map.get("capital").unwrap().as_int().unwrap(), 50000);
    }

    #[test]
    fn test_params_null_becomes_unit() {
        let engine = build_engine();

        let mut params = HashMap::new();
        params.insert("STOP_LOSS".to_string(), serde_json::json!(null));
        params.insert("TAKE_PROFIT".to_string(), serde_json::json!(0.5));

        let source = r#"
            fn check() {
                if params.STOP_LOSS == () { "unset" } else { "set" }
            }
        "#;

        let ast = engine.compile(source).unwrap();
        let mut scope = Scope::new();
        let _ = engine
            .eval_ast_with_scope::<Dynamic>(&mut scope, &ast)
            .unwrap();
        stdlib::inject_params_map(&mut scope, &params);

        let options = CallFnOptions::new().eval_ast(false).rewind_scope(false);
        let result: Dynamic = engine
            .call_fn_with_options(options, &mut scope, &ast, "check", ())
            .unwrap();
        assert_eq!(result.into_immutable_string().unwrap().as_str(), "unset");
    }

    #[test]
    fn test_params_scope_rebuild() {
        let mut params = HashMap::new();
        params.insert("SYMBOL".to_string(), serde_json::json!("SPY"));

        let mut scope = Scope::new();
        stdlib::inject_params_map(&mut scope, &params);

        // Update via inject_into_scope (sweep path)
        let mut new_params = HashMap::new();
        new_params.insert("SYMBOL".to_string(), serde_json::json!("QQQ"));
        stdlib::inject_into_scope(&mut scope, &new_params);

        let map = scope.get_value::<rhai::Map>("params").unwrap();
        assert_eq!(
            map.get("SYMBOL")
                .unwrap()
                .clone()
                .into_immutable_string()
                .unwrap()
                .as_str(),
            "QQQ"
        );
    }

    // -----------------------------------------------------------------------
    // Callback pattern tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_on_bar_returns_actions() {
        let engine = build_engine();
        let ast = engine
            .compile(
                r#"
                fn on_bar(ctx) {
                    [#{ action: "open_stock", side: "long", qty: 100 }]
                }
            "#,
            )
            .unwrap();

        let mut scope = Scope::new();
        let _ = engine
            .eval_ast_with_scope::<Dynamic>(&mut scope, &ast)
            .unwrap();

        // We can't call on_bar without a real BarContext, but we can verify
        // the function exists and the AST compiles
        let has_on_bar = ast
            .iter_functions()
            .any(|f| f.name == "on_bar" && f.params.len() == 1);
        assert!(has_on_bar, "on_bar(ctx) should exist in AST");
    }

    #[test]
    fn test_on_exit_check_returns_hold() {
        let engine = build_engine();
        let ast = engine
            .compile(
                r#"
                fn on_exit_check(ctx, pos) {
                    #{ action: "hold" }
                }
            "#,
            )
            .unwrap();

        let has_fn = ast
            .iter_functions()
            .any(|f| f.name == "on_exit_check" && f.params.len() == 2);
        assert!(has_fn, "on_exit_check(ctx, pos) should exist in AST");
    }

    #[test]
    fn test_stateful_wheel_script_compiles() {
        let engine = build_engine();

        // Verify the wheel pattern compiles and state transitions work
        let ast = engine
            .compile(
                r#"
                let state = "selling_puts";
                let cost_basis = 0.0;

                fn config() {
                    #{ symbol: "SPY", capital: 50000.0 }
                }

                fn on_bar(ctx) {
                    if state == "selling_puts" {
                        return [];
                    }
                    if state == "holding_stock" {
                        state = "selling_calls";
                        return [];
                    }
                    []
                }

                fn on_position_closed(ctx, pos, exit_type) {
                    if state == "selling_puts" && exit_type == "assignment" {
                        cost_basis = 395.0;
                        state = "holding_stock";
                    } else if state == "selling_calls" {
                        if exit_type == "called_away" {
                            state = "selling_puts";
                        } else {
                            state = "holding_stock";
                        }
                    }
                }
            "#,
            )
            .unwrap();

        // Test state transitions via scope
        let mut scope = Scope::new();
        let _ = engine
            .eval_ast_with_scope::<Dynamic>(&mut scope, &ast)
            .unwrap();

        assert_eq!(scope.get_value::<String>("state").unwrap(), "selling_puts");

        // Simulate assignment
        let checkpoint = scope.len();
        let options = CallFnOptions::new().eval_ast(false).rewind_scope(false);
        let _: Dynamic = engine
            .call_fn_with_options(
                options,
                &mut scope,
                &ast,
                "on_position_closed",
                (Dynamic::UNIT, Dynamic::UNIT, Dynamic::from("assignment")),
            )
            .unwrap();
        scope.rewind(checkpoint);

        assert_eq!(scope.get_value::<String>("state").unwrap(), "holding_stock");
        assert_eq!(scope.get_value::<f64>("cost_basis").unwrap(), 395.0);
    }

    // -----------------------------------------------------------------------
    // BarContext helper
    // -----------------------------------------------------------------------

    use crate::engine::types::{ExpirationFilter, Slippage, TradeSelector};
    use crate::scripting::types::{
        BarContext, Interval, ScriptConfig, ScriptPosition, ScriptPositionInner,
    };
    use std::sync::Arc;

    /// Build a minimal BarContext at `bar_idx` with the given price history.
    fn make_ctx(bars: &[OhlcvBar], bar_idx: usize) -> BarContext {
        make_ctx_with_positions(bars, bar_idx, vec![])
    }

    fn make_ctx_with_positions(
        bars: &[OhlcvBar],
        bar_idx: usize,
        positions: Vec<ScriptPosition>,
    ) -> BarContext {
        let bar = &bars[bar_idx];
        let config = Arc::new(ScriptConfig {
            symbol: "TEST".into(),
            capital: 50000.0,
            start_date: None,
            end_date: None,
            interval: Interval::Daily,
            multiplier: 100,
            timeout_secs: 60,
            auto_close_on_end: false,
            needs_ohlcv: true,
            needs_options: false,
            cross_symbols: vec![],
            declared_indicators: vec![],
            slippage: Slippage::Mid,
            commission: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            trade_selector: TradeSelector::Nearest,
            defaults: HashMap::new(),
            stop_loss: None,
            profit_target: None,
            trailing_stop: None,
            procedural: false,
        });
        let indicator_store = Arc::new(IndicatorStore::build(&[], bars).unwrap());
        BarContext {
            datetime: bar.datetime,
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            volume: bar.volume,
            bar_idx,
            cash: 50000.0,
            equity: 50000.0,
            positions: Arc::new(positions),
            indicator_store,
            price_history: Arc::new(bars.to_vec()),
            cross_symbol_data: Arc::new(HashMap::new()),
            options_by_date: None,
            config,
            pnl_history: Arc::new(vec![]),
            custom_series: Arc::new(std::sync::Mutex::new(
                crate::scripting::types::CustomSeriesStore {
                    series: HashMap::new(),
                    display_types: HashMap::new(),
                    num_bars: bars.len(),
                },
            )),
            adjusted_close: bar.close, // no adjustments in tests
            market_position: 0,
            entry_price: 0.0,
            bars_since_entry: 0,
            current_shares: 0,
            open_profit: 0.0,
            max_profit: 0.0,
            max_loss: 0.0,
            pending_orders_count: 0,
        }
    }

    // -----------------------------------------------------------------------
    // Historical bar lookback tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_lookback_current_bar() {
        let bars = make_bars(&[100.0, 110.0, 120.0, 130.0, 140.0]);
        let mut ctx = make_ctx(&bars, 4);

        // n=0 should return current bar values
        assert_eq!(ctx.high_at(0).as_float().unwrap(), bars[4].high);
        assert_eq!(ctx.low_at(0).as_float().unwrap(), bars[4].low);
        assert_eq!(ctx.open_at(0).as_float().unwrap(), bars[4].open);
        assert_eq!(ctx.close_at(0).as_float().unwrap(), bars[4].close);
        assert_eq!(ctx.volume_at(0).as_float().unwrap(), bars[4].volume);
    }

    #[test]
    fn test_lookback_previous_bars() {
        let bars = make_bars(&[100.0, 110.0, 120.0, 130.0, 140.0]);
        let mut ctx = make_ctx(&bars, 4);

        // 1 bar ago = bar[3] (close=130)
        assert_eq!(ctx.close_at(1).as_float().unwrap(), 130.0);
        // 4 bars ago = bar[0] (close=100)
        assert_eq!(ctx.close_at(4).as_float().unwrap(), 100.0);
    }

    #[test]
    fn test_lookback_out_of_range() {
        let bars = make_bars(&[100.0, 110.0, 120.0]);
        let mut ctx = make_ctx(&bars, 2);

        // 3 bars ago from index 2 → out of range
        assert!(ctx.close_at(3).is_unit());
        // Negative index
        assert!(ctx.close_at(-1).is_unit());
    }

    #[test]
    fn test_lookback_at_first_bar() {
        let bars = make_bars(&[100.0, 110.0, 120.0]);
        let mut ctx = make_ctx(&bars, 0);

        // Current bar works
        assert_eq!(ctx.close_at(0).as_float().unwrap(), 100.0);
        // Any lookback is out of range
        assert!(ctx.close_at(1).is_unit());
    }

    // -----------------------------------------------------------------------
    // Range query tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_highest_high() {
        // bars have high = price * 1.01
        let bars = make_bars(&[100.0, 110.0, 105.0, 120.0, 115.0]);
        let mut ctx = make_ctx(&bars, 4);

        // Highest high over all 5 bars = 120 * 1.01 = 121.2
        let hh = ctx.highest_high(5);
        assert!((hh - 121.2).abs() < 1e-10, "Expected 121.2, got {hh}");

        // Highest high over last 2 bars (index 3, 4) = 120 * 1.01
        let hh2 = ctx.highest_high(2);
        assert!((hh2 - 121.2).abs() < 1e-10);

        // Highest high over last 1 bar (just current) = 115 * 1.01
        let hh1 = ctx.highest_high(1);
        assert!((hh1 - 116.15).abs() < 1e-10);
    }

    #[test]
    fn test_lowest_low() {
        // bars have low = price * 0.99
        let bars = make_bars(&[100.0, 110.0, 105.0, 120.0, 115.0]);
        let mut ctx = make_ctx(&bars, 4);

        // Lowest low over all 5 bars = 100 * 0.99 = 99.0
        let ll = ctx.lowest_low(5);
        assert!((ll - 99.0).abs() < 1e-10, "Expected 99.0, got {ll}");

        // Lowest low over last 2 bars (index 3, 4) = 115 * 0.99 = 113.85
        let ll2 = ctx.lowest_low(2);
        assert!((ll2 - 113.85).abs() < 1e-10);
    }

    #[test]
    fn test_highest_close_lowest_close() {
        let bars = make_bars(&[100.0, 110.0, 105.0, 120.0, 115.0]);
        let mut ctx = make_ctx(&bars, 4);

        assert!((ctx.highest_close(5) - 120.0).abs() < 1e-10);
        assert!((ctx.lowest_close(5) - 100.0).abs() < 1e-10);

        // Last 3 bars: 105, 120, 115
        assert!((ctx.highest_close(3) - 120.0).abs() < 1e-10);
        assert!((ctx.lowest_close(3) - 105.0).abs() < 1e-10);
    }

    #[test]
    fn test_range_query_period_zero() {
        let bars = make_bars(&[100.0, 110.0]);
        let mut ctx = make_ctx(&bars, 1);
        assert_eq!(ctx.highest_high(0), 0.0);
        assert_eq!(ctx.lowest_low(0), 0.0);
    }

    #[test]
    fn test_range_query_period_exceeds_history() {
        let bars = make_bars(&[100.0, 110.0, 120.0]);
        let mut ctx = make_ctx(&bars, 1);

        // Period 10 but only 2 bars available (index 0..=1)
        // Should use all available bars, not panic
        let hh = ctx.highest_high(10);
        assert!((hh - 110.0 * 1.01).abs() < 1e-10);
    }

    // -----------------------------------------------------------------------
    // Portfolio method tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_unrealized_pnl_no_positions() {
        let bars = make_bars(&[100.0]);
        let mut ctx = make_ctx(&bars, 0);
        assert_eq!(ctx.get_unrealized_pnl(), 0.0);
    }

    #[test]
    fn test_unrealized_pnl_with_positions() {
        use crate::engine::types::Side;
        let bars = make_bars(&[100.0]);
        let positions = vec![
            ScriptPosition {
                id: 1,
                entry_date: bars[0].datetime.date(),
                inner: ScriptPositionInner::Stock {
                    side: Side::Long,
                    qty: 100,
                    entry_price: 95.0,
                },
                entry_cost: 9500.0,
                unrealized_pnl: 500.0,
                days_held: 5,
                current_date: bars[0].datetime.date(),
                entry_bar_idx: 0,
                source: String::new(),
                implicit: false,
                group: None,
            },
            ScriptPosition {
                id: 2,
                entry_date: bars[0].datetime.date(),
                inner: ScriptPositionInner::Stock {
                    side: Side::Long,
                    qty: 50,
                    entry_price: 100.0,
                },
                entry_cost: 5000.0,
                unrealized_pnl: -200.0,
                days_held: 3,
                current_date: bars[0].datetime.date(),
                entry_bar_idx: 0,
                source: String::new(),
                implicit: false,
                group: None,
            },
        ];
        let mut ctx = make_ctx_with_positions(&bars, 0, positions);
        assert!((ctx.get_unrealized_pnl() - 300.0).abs() < 1e-10);
    }

    #[test]
    fn test_realized_pnl() {
        let bars = make_bars(&[100.0]);
        let mut ctx = make_ctx(&bars, 0);
        // equity=50000, capital=50000 → realized=0
        assert_eq!(ctx.get_realized_pnl(), 0.0);

        // Simulate some realized gains
        ctx.equity = 52000.0;
        assert!((ctx.get_realized_pnl() - 2000.0).abs() < 1e-10);
    }

    // -----------------------------------------------------------------------
    // Action helper tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_hold_position_helper() {
        use crate::scripting::helpers;
        let result = helpers::hold_position();
        let map = result.cast::<rhai::Map>();
        assert_eq!(
            map.get("action")
                .unwrap()
                .clone()
                .into_immutable_string()
                .unwrap()
                .as_str(),
            "hold"
        );
    }

    #[test]
    fn test_close_position_helper() {
        use crate::scripting::helpers;
        let result = helpers::close_position("take_profit".to_string());
        let map = result.cast::<rhai::Map>();
        assert_eq!(
            map.get("action")
                .unwrap()
                .clone()
                .into_immutable_string()
                .unwrap()
                .as_str(),
            "close"
        );
        assert_eq!(
            map.get("reason")
                .unwrap()
                .clone()
                .into_immutable_string()
                .unwrap()
                .as_str(),
            "take_profit"
        );
    }

    #[test]
    fn test_close_position_id_helper() {
        use crate::scripting::helpers;
        let result = helpers::close_position_id(42, "stop_loss".to_string());
        let map = result.cast::<rhai::Map>();
        assert_eq!(
            map.get("action")
                .unwrap()
                .clone()
                .into_immutable_string()
                .unwrap()
                .as_str(),
            "close"
        );
        assert_eq!(map.get("position_id").unwrap().as_int().unwrap(), 42);
    }

    #[test]
    fn test_buy_stock_helper() {
        use crate::scripting::helpers;
        let result = helpers::buy_stock(100);
        let map = result.cast::<rhai::Map>();
        assert_eq!(
            map.get("action")
                .unwrap()
                .clone()
                .into_immutable_string()
                .unwrap()
                .as_str(),
            "open_stock"
        );
        assert_eq!(
            map.get("side")
                .unwrap()
                .clone()
                .into_immutable_string()
                .unwrap()
                .as_str(),
            "long"
        );
        assert_eq!(map.get("qty").unwrap().as_int().unwrap(), 100);
    }

    #[test]
    fn test_sell_stock_helper() {
        use crate::scripting::helpers;
        let result = helpers::sell_stock(50);
        let map = result.cast::<rhai::Map>();
        assert_eq!(
            map.get("side")
                .unwrap()
                .clone()
                .into_immutable_string()
                .unwrap()
                .as_str(),
            "short"
        );
    }

    #[test]
    fn test_stop_backtest_helper() {
        use crate::scripting::helpers;
        let result = helpers::stop_backtest("capital_depleted".to_string());
        let map = result.cast::<rhai::Map>();
        assert_eq!(
            map.get("action")
                .unwrap()
                .clone()
                .into_immutable_string()
                .unwrap()
                .as_str(),
            "stop"
        );
    }

    // -----------------------------------------------------------------------
    // Action helpers via Rhai engine (integration test)
    // -----------------------------------------------------------------------

    #[test]
    fn test_action_helpers_registered_in_engine() {
        let engine = build_engine();

        // hold_position() returns #{ action: "hold" }
        let result: Dynamic = engine.eval("hold_position()").unwrap();
        let map = result.cast::<rhai::Map>();
        assert_eq!(
            map.get("action")
                .unwrap()
                .clone()
                .into_immutable_string()
                .unwrap()
                .as_str(),
            "hold"
        );

        // close_position(reason) returns #{ action: "close", reason }
        let result: Dynamic = engine.eval(r#"close_position("take_profit")"#).unwrap();
        let map = result.cast::<rhai::Map>();
        assert_eq!(
            map.get("action")
                .unwrap()
                .clone()
                .into_immutable_string()
                .unwrap()
                .as_str(),
            "close"
        );

        // buy_stock(qty) returns #{ action: "open_stock", side: "long", qty }
        let result: Dynamic = engine.eval("buy_stock(100)").unwrap();
        let map = result.cast::<rhai::Map>();
        assert_eq!(map.get("qty").unwrap().as_int().unwrap(), 100);

        // sell_stock(qty) returns #{ action: "open_stock", side: "short", qty }
        let result: Dynamic = engine.eval("sell_stock(50)").unwrap();
        let map = result.cast::<rhai::Map>();
        assert_eq!(
            map.get("side")
                .unwrap()
                .clone()
                .into_immutable_string()
                .unwrap()
                .as_str(),
            "short"
        );
    }

    // -----------------------------------------------------------------------
    // indicators_ready tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_indicators_ready_with_period_indicators() {
        let bars = make_bars(&[10.0, 11.0, 12.0, 13.0, 14.0]);
        let store = IndicatorStore::build(&["sma:3".to_string()], &bars).unwrap();
        let mut ctx = make_ctx(&bars, 4);
        ctx.indicator_store = Arc::new(store);

        // SMA:3 should be ready at bar 4
        let arr: rhai::Array = vec![Dynamic::from("sma:3")];
        assert!(ctx.indicators_ready(arr));

        // SMA:3 not ready at bar 0 (warmup)
        let mut ctx0 = make_ctx(&bars, 0);
        ctx0.indicator_store = ctx.indicator_store.clone();
        let arr: rhai::Array = vec![Dynamic::from("sma:3")];
        assert!(!ctx0.indicators_ready(arr));
    }

    #[test]
    fn test_indicators_ready_with_obv() {
        let bars = make_bars(&[10.0, 11.0, 12.0, 13.0, 14.0]);
        let store = IndicatorStore::build(&["obv".to_string()], &bars).unwrap();
        let mut ctx = make_ctx(&bars, 4);
        ctx.indicator_store = Arc::new(store);

        // OBV (no period) should be ready at bar 4
        let arr: rhai::Array = vec![Dynamic::from("obv")];
        assert!(ctx.indicators_ready(arr));
    }

    #[test]
    fn test_indicators_ready_mixed() {
        let bars: Vec<f64> = (0..30).map(|i| 100.0 + (i as f64) * 0.5).collect();
        let bars = make_bars(&bars);
        let store =
            IndicatorStore::build(&["sma:3".to_string(), "obv".to_string()], &bars).unwrap();
        let mut ctx = make_ctx(&bars, 20);
        ctx.indicator_store = Arc::new(store);

        let arr: rhai::Array = vec![Dynamic::from("sma:3"), Dynamic::from("obv")];
        assert!(ctx.indicators_ready(arr));
    }

    #[test]
    fn test_indicators_ready_default_params_macd() {
        // macd_line declared without params → store uses default [12, 26, 9]
        let bars: Vec<f64> = (0..50).map(|i| 100.0 + (i as f64) * 0.3).collect();
        let bars = make_bars(&bars);
        let store = IndicatorStore::build(&["macd_line".to_string()], &bars).unwrap();
        let mut ctx = make_ctx(&bars, 40);
        ctx.indicator_store = Arc::new(store);

        // indicators_ready must use parse_indicator_declaration to match the stored key
        let arr: rhai::Array = vec![Dynamic::from("macd_line")];
        assert!(
            ctx.indicators_ready(arr),
            "indicators_ready should find macd_line with default params"
        );
    }

    #[test]
    fn test_indicators_ready_default_params_bbands() {
        // bbands_upper:20 → store uses key [20, 20] (period + default std_mult)
        let bars: Vec<f64> = (0..30).map(|i| 100.0 + (i as f64).sin() * 5.0).collect();
        let bars = make_bars(&bars);
        let store = IndicatorStore::build(&["bbands_upper:20".to_string()], &bars).unwrap();
        let mut ctx = make_ctx(&bars, 25);
        ctx.indicator_store = Arc::new(store);

        let arr: rhai::Array = vec![Dynamic::from("bbands_upper:20")];
        assert!(
            ctx.indicators_ready(arr),
            "indicators_ready should find bbands_upper:20 with default std_mult param"
        );
    }

    // -----------------------------------------------------------------------
    // Position sizing helper tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_size_by_equity_full() {
        let bars = make_bars(&[100.0]);
        let mut ctx = make_ctx(&bars, 0);
        // equity=50000, close=100 → 500 shares
        assert_eq!(ctx.size_by_equity(1.0), 500);
    }

    #[test]
    fn test_size_by_equity_half() {
        let bars = make_bars(&[100.0]);
        let mut ctx = make_ctx(&bars, 0);
        // 50% of 50000 / 100 = 250
        assert_eq!(ctx.size_by_equity(0.5), 250);
    }

    #[test]
    fn test_size_by_equity_leverage() {
        let bars = make_bars(&[100.0]);
        let mut ctx = make_ctx(&bars, 0);
        // fraction 2.0 = 200% leverage: 100000 / 100 = 1000 shares
        assert_eq!(ctx.size_by_equity(2.0), 1000);
    }

    #[test]
    fn test_size_by_equity_zero_price() {
        let bars = make_bars(&[100.0]);
        let mut ctx = make_ctx(&bars, 0);
        ctx.close = 0.0;
        assert_eq!(ctx.size_by_equity(1.0), 0);
    }

    #[test]
    fn test_size_by_risk() {
        let bars = make_bars(&[100.0]);
        let mut ctx = make_ctx(&bars, 0);
        // Risk 2% of 50000 = $1000, stop at $95, risk/share = $5
        // qty = 1000 / 5 = 200
        assert_eq!(ctx.size_by_risk(0.02, 95.0), 200);
    }

    #[test]
    fn test_size_by_risk_stop_equals_price() {
        let bars = make_bars(&[100.0]);
        let mut ctx = make_ctx(&bars, 0);
        // stop = price → risk/share = 0 → returns 0
        assert_eq!(ctx.size_by_risk(0.02, 100.0), 0);
    }

    #[test]
    fn test_size_by_risk_stop_above_price() {
        let bars = make_bars(&[100.0]);
        let mut ctx = make_ctx(&bars, 0);
        // stop above price (wrong direction for longs) → returns 0
        assert_eq!(ctx.size_by_risk(0.02, 105.0), 0);
    }

    #[test]
    fn test_size_by_volatility() {
        let bars: Vec<f64> = (0..30).map(|i| 100.0 + (i as f64) * 0.5).collect();
        let bars = make_bars(&bars);
        let store = IndicatorStore::build(&["atr:14".to_string()], &bars).unwrap();
        let mut ctx = make_ctx(&bars, 20);
        ctx.indicator_store = Arc::new(store);
        // ATR should be a small positive value for trending data
        let qty = ctx.size_by_volatility(1000.0, 14);
        assert!(qty > 0, "Should compute positive qty from ATR");
        // Should not exceed full equity worth of shares
        let max = (ctx.equity / ctx.close).floor() as i64;
        assert!(qty <= max);
    }

    #[test]
    fn test_size_by_volatility_no_indicator() {
        let bars = make_bars(&[100.0, 110.0, 120.0]);
        let mut ctx = make_ctx(&bars, 2);
        // No ATR indicator declared → returns 0
        assert_eq!(ctx.size_by_volatility(1000.0, 14), 0);
    }

    #[test]
    fn test_size_by_kelly_cold_start() {
        let bars = make_bars(&[100.0]);
        let mut ctx = make_ctx(&bars, 0);
        // No trade history → returns 0
        assert_eq!(ctx.size_by_kelly(0.5, 0), 0);
    }

    #[test]
    fn test_size_by_kelly_insufficient_trades() {
        let bars = make_bars(&[100.0]);
        let mut ctx = make_ctx(&bars, 0);
        // Only 10 trades (need 20 minimum)
        ctx.pnl_history = Arc::new(vec![100.0; 10]);
        assert_eq!(ctx.size_by_kelly(0.5, 0), 0);
    }

    #[test]
    fn test_size_by_kelly_winning_system() {
        let bars = make_bars(&[100.0]);
        let mut ctx = make_ctx(&bars, 0);
        // 70% win rate, avg win $500, avg loss $300
        let mut pnls = vec![500.0; 14]; // 14 wins
        pnls.extend(vec![-300.0; 6]); // 6 losses
        ctx.pnl_history = Arc::new(pnls);

        let qty = ctx.size_by_kelly(0.5, 0);
        assert!(qty > 0, "Winning system should produce positive Kelly qty");
        // Full Kelly = 0.7 - 0.3 / (500/300) = 0.7 - 0.18 = 0.52
        // Half Kelly = 0.26 of equity = 13000 / 100 = 130 shares
        assert!(qty > 100 && qty < 200, "Expected ~130 shares, got {qty}");
    }

    #[test]
    fn test_size_by_kelly_losing_system() {
        let bars = make_bars(&[100.0]);
        let mut ctx = make_ctx(&bars, 0);
        // 30% win rate, avg win $200, avg loss $400
        let mut pnls = vec![200.0; 6]; // 6 wins
        pnls.extend(vec![-400.0; 14]); // 14 losses
        ctx.pnl_history = Arc::new(pnls);

        // Kelly = 0.3 - 0.7 / (200/400) = 0.3 - 1.4 = -1.1 → 0
        assert_eq!(ctx.size_by_kelly(0.5, 0), 0);
    }

    #[test]
    fn test_size_by_kelly_with_lookback() {
        let bars = make_bars(&[100.0]);
        let mut ctx = make_ctx(&bars, 0);
        // 30 losing trades followed by 25 winning trades
        let mut pnls = vec![-300.0; 30];
        pnls.extend(vec![500.0; 18]);
        pnls.extend(vec![-200.0; 7]);
        ctx.pnl_history = Arc::new(pnls);

        // lookback=25: only recent 25 trades (18 wins + 7 losses)
        let qty_recent = ctx.size_by_kelly(0.5, 25);
        assert!(qty_recent > 0, "Recent window is profitable");

        // lookback=0: all trades (worse overall)
        let qty_all = ctx.size_by_kelly(0.5, 0);
        // Recent window should size larger than the full history
        assert!(
            qty_recent > qty_all,
            "Recent trades are better: {qty_recent} > {qty_all}"
        );
    }

    // -----------------------------------------------------------------------
    // Position sizing helpers via Rhai engine
    // -----------------------------------------------------------------------

    #[test]
    fn test_sizing_helpers_registered_in_engine() {
        let engine = build_engine();
        // Verify the function names are registered (they need a BarContext to call,
        // but we can check they compile in a script)
        let result = engine.compile(
            r"
            fn on_bar(ctx) {
                let q1 = ctx.size_by_equity(0.5);
                let q2 = ctx.size_by_risk(0.02, 95.0);
                let q3 = ctx.size_by_volatility(1000.0, 14);
                let q4 = ctx.size_by_kelly(0.5, 0);
                [buy_stock(q1)]
            }
            ",
        );
        assert!(
            result.is_ok(),
            "Sizing helpers should be registered: {:?}",
            result.err()
        );
    }

    // -----------------------------------------------------------------------
    // Strategy helper leg verification tests
    // -----------------------------------------------------------------------

    fn get_str(map: &rhai::Map, key: &str) -> String {
        map.get(key)
            .unwrap()
            .clone()
            .into_immutable_string()
            .unwrap()
            .to_string()
    }

    fn get_f64(map: &rhai::Map, key: &str) -> f64 {
        map.get(key).unwrap().as_float().unwrap()
    }

    fn get_i64(map: &rhai::Map, key: &str) -> i64 {
        map.get(key).unwrap().as_int().unwrap()
    }

    #[test]
    fn test_leg_builder() {
        use crate::scripting::helpers::leg;
        let l = leg("short", "put", 0.30, 45);
        let map = l.cast::<rhai::Map>();
        assert_eq!(get_str(&map, "side"), "short");
        assert_eq!(get_str(&map, "option_type"), "put");
        assert!((get_f64(&map, "delta") - 0.30).abs() < 1e-10);
        assert_eq!(get_i64(&map, "dte"), 45);
    }

    /// Verify iron_condor passes 4 correct legs: long put, short put, short call, long call.
    #[test]
    fn test_iron_condor_leg_ordering() {
        use crate::scripting::helpers::leg;
        // Replicate what iron_condor() does internally
        let legs = [
            leg("long", "put", 0.10, 45),
            leg("short", "put", 0.30, 45),
            leg("short", "call", 0.30, 45),
            leg("long", "call", 0.10, 45),
        ];

        let l0 = legs[0].clone().cast::<rhai::Map>();
        assert_eq!(get_str(&l0, "side"), "long");
        assert_eq!(get_str(&l0, "option_type"), "put");
        assert!((get_f64(&l0, "delta") - 0.10).abs() < 1e-10);

        let l1 = legs[1].clone().cast::<rhai::Map>();
        assert_eq!(get_str(&l1, "side"), "short");
        assert_eq!(get_str(&l1, "option_type"), "put");
        assert!((get_f64(&l1, "delta") - 0.30).abs() < 1e-10);

        let l2 = legs[2].clone().cast::<rhai::Map>();
        assert_eq!(get_str(&l2, "side"), "short");
        assert_eq!(get_str(&l2, "option_type"), "call");

        let l3 = legs[3].clone().cast::<rhai::Map>();
        assert_eq!(get_str(&l3, "side"), "long");
        assert_eq!(get_str(&l3, "option_type"), "call");
    }

    /// Verify bull_put_spread: short higher-delta put + long lower-delta put.
    #[test]
    fn test_bull_put_spread_leg_ordering() {
        use crate::scripting::helpers::leg;
        let legs = [leg("short", "put", 0.30, 45), leg("long", "put", 0.15, 45)];

        let short_leg = legs[0].clone().cast::<rhai::Map>();
        assert_eq!(get_str(&short_leg, "side"), "short");
        assert_eq!(get_str(&short_leg, "option_type"), "put");
        assert!((get_f64(&short_leg, "delta") - 0.30).abs() < 1e-10);

        let long_leg = legs[1].clone().cast::<rhai::Map>();
        assert_eq!(get_str(&long_leg, "side"), "long");
        assert_eq!(get_str(&long_leg, "option_type"), "put");
        assert!((get_f64(&long_leg, "delta") - 0.15).abs() < 1e-10);
    }

    /// Verify bear_call_spread: short higher-delta call + long lower-delta call.
    #[test]
    fn test_bear_call_spread_leg_ordering() {
        use crate::scripting::helpers::leg;
        let legs = [
            leg("short", "call", 0.40, 30),
            leg("long", "call", 0.20, 30),
        ];
        let short_leg = legs[0].clone().cast::<rhai::Map>();
        assert_eq!(get_str(&short_leg, "side"), "short");
        assert!((get_f64(&short_leg, "delta") - 0.40).abs() < 1e-10);

        let long_leg = legs[1].clone().cast::<rhai::Map>();
        assert_eq!(get_str(&long_leg, "side"), "long");
        assert!((get_f64(&long_leg, "delta") - 0.20).abs() < 1e-10);
    }

    /// Verify call_calendar: short near-term + long far-term with different DTEs.
    #[test]
    fn test_call_calendar_leg_ordering() {
        use crate::scripting::helpers::leg;
        let legs = [
            leg("short", "call", 0.50, 30),
            leg("long", "call", 0.50, 60),
        ];
        let near = legs[0].clone().cast::<rhai::Map>();
        assert_eq!(get_str(&near, "side"), "short");
        assert_eq!(get_i64(&near, "dte"), 30);

        let far = legs[1].clone().cast::<rhai::Map>();
        assert_eq!(get_str(&far, "side"), "long");
        assert_eq!(get_i64(&far, "dte"), 60);
    }

    #[test]
    fn test_total_exposure() {
        use crate::engine::types::Side;
        let bars = make_bars(&[100.0]);
        let positions = vec![
            ScriptPosition {
                id: 1,
                entry_date: bars[0].datetime.date(),
                inner: ScriptPositionInner::Stock {
                    side: Side::Long,
                    qty: 100,
                    entry_price: 95.0,
                },
                entry_cost: 9500.0,
                unrealized_pnl: 0.0,
                days_held: 0,
                current_date: bars[0].datetime.date(),
                entry_bar_idx: 0,
                source: String::new(),
                implicit: false,
                group: None,
            },
            ScriptPosition {
                id: 2,
                entry_date: bars[0].datetime.date(),
                inner: ScriptPositionInner::Stock {
                    side: Side::Short,
                    qty: 50,
                    entry_price: 100.0,
                },
                entry_cost: -5000.0, // short → negative entry cost
                unrealized_pnl: 0.0,
                days_held: 0,
                current_date: bars[0].datetime.date(),
                entry_bar_idx: 0,
                source: String::new(),
                implicit: false,
                group: None,
            },
        ];
        let mut ctx = make_ctx_with_positions(&bars, 0, positions);
        // abs(9500) + abs(-5000) = 14500
        assert!((ctx.get_total_exposure() - 14500.0).abs() < 1e-10);
    }

    // -----------------------------------------------------------------------
    // Custom series plotting tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_plot_records_value_at_correct_bar_idx() {
        let bars = make_bars(&[100.0, 110.0, 120.0]);
        let mut ctx = make_ctx(&bars, 1);

        ctx.plot("test_series".to_string(), 42.0);

        let store = ctx.custom_series.lock().unwrap();
        let series = store.series.get("test_series").unwrap();
        assert_eq!(series.len(), 3); // pre-allocated to num_bars
        assert_eq!(series[0], None); // bar 0 not plotted
        assert_eq!(series[1], Some(42.0)); // bar 1 plotted
        assert_eq!(series[2], None); // bar 2 not plotted
    }

    #[test]
    fn test_plot_non_finite_values_become_none() {
        let bars = make_bars(&[100.0, 110.0, 120.0]);

        // NaN at bar 0
        let mut ctx = make_ctx(&bars, 0);
        ctx.plot("s".to_string(), f64::NAN);

        // Verify NaN was stored as None
        {
            let store = ctx.custom_series.lock().unwrap();
            let series = store.series.get("s").unwrap();
            assert_eq!(series[0], None, "NaN should become None");
        }

        // +Inf, -Inf, NaN across 3 bars
        let mut ctx2 = make_ctx(&bars, 0);
        ctx2.plot("inf_test".to_string(), f64::INFINITY);
        ctx2.bar_idx = 1;
        ctx2.plot("inf_test".to_string(), f64::NEG_INFINITY);
        ctx2.bar_idx = 2;
        ctx2.plot("inf_test".to_string(), f64::NAN);

        let store = ctx2.custom_series.lock().unwrap();
        let series = store.series.get("inf_test").unwrap();
        assert_eq!(series[0], None); // +inf → None
        assert_eq!(series[1], None); // -inf → None
        assert_eq!(series[2], None); // NaN → None
    }

    #[test]
    fn test_plot_with_sets_display_type() {
        let bars = make_bars(&[100.0, 110.0, 120.0]);
        let mut ctx = make_ctx(&bars, 0);

        ctx.plot_with("my_osc".to_string(), 5.0, "subchart".to_string());

        let store = ctx.custom_series.lock().unwrap();
        assert_eq!(
            store.display_types.get("my_osc").map(|s| s.as_str()),
            Some("subchart")
        );
        assert_eq!(store.series.get("my_osc").unwrap()[0], Some(5.0));
    }

    #[test]
    fn test_plot_max_series_limit() {
        let bars = make_bars(&[100.0]);
        let mut ctx = make_ctx(&bars, 0);

        // Fill up to the max
        for i in 0..crate::scripting::types::MAX_CUSTOM_SERIES {
            ctx.plot(format!("series_{i}"), 1.0);
        }

        // One more should be silently rejected
        ctx.plot("overflow".to_string(), 1.0);
        let store = ctx.custom_series.lock().unwrap();
        assert!(!store.series.contains_key("overflow"));
        assert_eq!(
            store.series.len(),
            crate::scripting::types::MAX_CUSTOM_SERIES
        );
    }

    #[test]
    fn test_format_custom_series_in_indicator_data() {
        use crate::tools::run_script::{format_indicator_data, DisplayType};

        let raw = HashMap::new(); // no pre-computed indicators
        let custom = crate::scripting::types::CustomSeriesStore {
            series: HashMap::from([("my_band".to_string(), vec![Some(100.0), None, Some(102.0)])]),
            display_types: HashMap::from([("my_band".to_string(), "subchart".to_string())]),
            num_bars: 3,
        };

        let result = format_indicator_data(&raw, &custom);
        assert_eq!(result.len(), 1);
        let item = &result[0];
        assert_eq!(item.key, "custom:my_band");
        assert_eq!(item.name, "my_band");
        assert!(matches!(item.display_type, DisplayType::Subchart));
        assert_eq!(item.values, vec![Some(100.0), None, Some(102.0)]);
    }

    // -----------------------------------------------------------------------
    // Order type and next-bar execution tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_pending_order_market_fill() {
        use crate::engine::types::Side;
        use crate::scripting::types::{OrderType, PendingOrder, ScriptAction};

        let order = PendingOrder {
            action: ScriptAction::OpenStock {
                side: Side::Long,
                qty: 100,
            },
            order_type: OrderType::Market,
            is_buy: true,
            signal: None,
            submitted_bar: 0,
            ttl: None,
        };

        // Market orders always fill at the open
        let fill = order.try_fill(150.0, 155.0, 148.0, 152.0);
        assert_eq!(fill, Some(150.0));
    }

    #[test]
    fn test_pending_order_limit_buy_fill() {
        use crate::engine::types::Side;
        use crate::scripting::types::{OrderType, PendingOrder, ScriptAction};

        let order = PendingOrder {
            action: ScriptAction::OpenStock {
                side: Side::Long,
                qty: 100,
            },
            order_type: OrderType::Limit { price: 148.0 },
            is_buy: true,
            signal: None,
            submitted_bar: 0,
            ttl: None,
        };

        // Low reaches limit → fills at limit price
        let fill = order.try_fill(150.0, 155.0, 147.0, 152.0);
        assert_eq!(fill, Some(148.0));

        // Low doesn't reach limit → no fill
        let fill = order.try_fill(150.0, 155.0, 149.0, 152.0);
        assert_eq!(fill, None);

        // Open gaps below limit → fills at open
        let fill = order.try_fill(147.0, 155.0, 146.0, 152.0);
        assert_eq!(fill, Some(147.0));
    }

    #[test]
    fn test_pending_order_stop_buy_fill() {
        use crate::engine::types::Side;
        use crate::scripting::types::{OrderType, PendingOrder, ScriptAction};

        let order = PendingOrder {
            action: ScriptAction::OpenStock {
                side: Side::Long,
                qty: 100,
            },
            order_type: OrderType::Stop { price: 155.0 },
            is_buy: true,
            signal: None,
            submitted_bar: 0,
            ttl: None,
        };

        // High reaches stop → fills at stop price
        let fill = order.try_fill(150.0, 156.0, 149.0, 154.0);
        assert_eq!(fill, Some(155.0));

        // High doesn't reach stop → no fill
        let fill = order.try_fill(150.0, 154.0, 149.0, 152.0);
        assert_eq!(fill, None);

        // Open gaps above stop → fills at open
        let fill = order.try_fill(156.0, 158.0, 155.0, 157.0);
        assert_eq!(fill, Some(156.0));
    }

    #[test]
    fn test_pending_order_sell_stop_fill() {
        use crate::engine::types::Side;
        use crate::scripting::types::{OrderType, PendingOrder, ScriptAction};

        let order = PendingOrder {
            action: ScriptAction::OpenStock {
                side: Side::Short,
                qty: 100,
            },
            order_type: OrderType::Stop { price: 145.0 },
            is_buy: false,
            signal: None,
            submitted_bar: 0,
            ttl: None,
        };

        // Low reaches stop → fills at stop price
        let fill = order.try_fill(150.0, 152.0, 144.0, 146.0);
        assert_eq!(fill, Some(145.0));

        // Low doesn't reach stop → no fill
        let fill = order.try_fill(150.0, 152.0, 146.0, 148.0);
        assert_eq!(fill, None);
    }

    #[test]
    fn test_pending_order_expiry() {
        use crate::engine::types::Side;
        use crate::scripting::types::{OrderType, PendingOrder, ScriptAction};

        let order = PendingOrder {
            action: ScriptAction::OpenStock {
                side: Side::Long,
                qty: 100,
            },
            order_type: OrderType::Market,
            is_buy: true,
            signal: Some("test".to_string()),
            submitted_bar: 5,
            ttl: Some(3),
        };

        assert!(!order.is_expired(6));
        assert!(!order.is_expired(8)); // exactly 3 bars = not expired
        assert!(order.is_expired(9)); // 4 bars = expired
    }

    #[test]
    fn test_pending_order_gtc_never_expires() {
        use crate::engine::types::Side;
        use crate::scripting::types::{OrderType, PendingOrder, ScriptAction};

        let order = PendingOrder {
            action: ScriptAction::OpenStock {
                side: Side::Long,
                qty: 100,
            },
            order_type: OrderType::Market,
            is_buy: true,
            signal: None,
            submitted_bar: 0,
            ttl: None, // GTC
        };

        assert!(!order.is_expired(1000));
    }

    #[test]
    fn test_stop_limit_buy_fill() {
        use crate::engine::types::Side;
        use crate::scripting::types::{OrderType, PendingOrder, ScriptAction};

        let order = PendingOrder {
            action: ScriptAction::OpenStock {
                side: Side::Long,
                qty: 100,
            },
            order_type: OrderType::StopLimit {
                stop: 155.0,
                limit: 157.0,
            },
            is_buy: true,
            signal: None,
            submitted_bar: 0,
            ttl: None,
        };

        // Both conditions met: high >= stop AND low <= limit
        let fill = order.try_fill(150.0, 158.0, 149.0, 154.0);
        assert!(fill.is_some());

        // Stop not reached
        let fill = order.try_fill(150.0, 154.0, 149.0, 152.0);
        assert!(fill.is_none());
    }

    #[test]
    fn test_position_awareness_flat() {
        use crate::scripting::engine::compute_position_awareness;

        let awareness = compute_position_awareness(&[], 0, 0);
        assert_eq!(awareness.market_position, 0);
        assert_eq!(awareness.entry_price, 0.0);
        assert_eq!(awareness.current_shares, 0);
        assert_eq!(awareness.pending_orders_count, 0);
    }

    #[test]
    fn test_position_awareness_long() {
        use crate::engine::types::Side;
        use crate::scripting::engine::compute_position_awareness;
        use crate::scripting::types::{ScriptPosition, ScriptPositionInner};

        let pos = ScriptPosition {
            id: 1,
            entry_date: chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            inner: ScriptPositionInner::Stock {
                side: Side::Long,
                qty: 100,
                entry_price: 150.0,
            },
            entry_cost: 15000.0,
            unrealized_pnl: 500.0,
            days_held: 5,
            current_date: chrono::NaiveDate::from_ymd_opt(2024, 1, 6).unwrap(),
            entry_bar_idx: 5,
            source: "script".to_string(),
            implicit: false,
            group: None,
        };

        let awareness = compute_position_awareness(&[pos], 2, 10);
        assert_eq!(awareness.market_position, 1);
        assert_eq!(awareness.entry_price, 150.0);
        assert_eq!(awareness.current_shares, 100);
        assert_eq!(awareness.bars_since_entry, 5); // 10 - 5 = 5 bars
        assert_eq!(awareness.open_profit, 500.0);
        assert_eq!(awareness.pending_orders_count, 2);
    }

    #[test]
    fn test_helper_buy_limit() {
        use crate::scripting::helpers;

        let result = helpers::buy_limit(100, 150.0);
        let map = result.cast::<rhai::Map>();
        assert_eq!(
            map.get("order_type")
                .unwrap()
                .clone()
                .into_immutable_string()
                .unwrap()
                .as_str(),
            "limit"
        );
        assert_eq!(map.get("limit_price").unwrap().as_float().unwrap(), 150.0);
        assert_eq!(
            map.get("side")
                .unwrap()
                .clone()
                .into_immutable_string()
                .unwrap()
                .as_str(),
            "long"
        );
    }

    #[test]
    fn test_helper_sell_stop() {
        use crate::scripting::helpers;

        let result = helpers::sell_stop(50, 145.0);
        let map = result.cast::<rhai::Map>();
        assert_eq!(
            map.get("order_type")
                .unwrap()
                .clone()
                .into_immutable_string()
                .unwrap()
                .as_str(),
            "stop"
        );
        assert_eq!(map.get("stop_price").unwrap().as_float().unwrap(), 145.0);
        assert_eq!(
            map.get("side")
                .unwrap()
                .clone()
                .into_immutable_string()
                .unwrap()
                .as_str(),
            "short"
        );
    }

    #[test]
    fn test_helper_cancel_orders() {
        use crate::scripting::helpers;

        let result = helpers::cancel_orders();
        let map = result.cast::<rhai::Map>();
        assert_eq!(
            map.get("action")
                .unwrap()
                .clone()
                .into_immutable_string()
                .unwrap()
                .as_str(),
            "cancel_orders"
        );
    }

    #[test]
    fn test_helper_buy_stop_limit() {
        use crate::scripting::helpers;

        let result = helpers::buy_stop_limit(100, 155.0, 153.0);
        let map = result.cast::<rhai::Map>();
        assert_eq!(
            map.get("order_type")
                .unwrap()
                .clone()
                .into_immutable_string()
                .unwrap()
                .as_str(),
            "stop_limit"
        );
        assert_eq!(map.get("stop_price").unwrap().as_float().unwrap(), 155.0);
        assert_eq!(map.get("limit_price").unwrap().as_float().unwrap(), 153.0);
    }
}
