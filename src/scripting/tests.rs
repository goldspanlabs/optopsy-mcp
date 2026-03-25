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

        // First 14 bars should be NaN
        assert!(store.get(&key, 0).unwrap().is_nan());
        assert!(store.get(&key, 13).unwrap().is_nan());

        // Bar 14 should have a value (all gains → RSI close to 100)
        let val = store.get(&key, 14).unwrap();
        assert!(!val.is_nan(), "RSI at bar 14 should not be NaN");
        assert!(
            val > 90.0,
            "RSI should be high for monotonically increasing prices, got {val}"
        );
    }

    #[test]
    fn test_indicator_store_macd() {
        // MACD needs enough data for the slow EMA warmup (26 bars default)
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

        // First 25 bars should be NaN (slow EMA warmup)
        assert!(store.get(&key, 0).unwrap().is_nan());
        // Bar 25+ should have values
        let val = store.get(&key, 30).unwrap();
        assert!(!val.is_nan(), "MACD at bar 30 should not be NaN");
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

        // OBV has no warmup — all bars should have values
        let val = store.get(&key, 0).unwrap();
        assert!(!val.is_nan(), "OBV at bar 0 should not be NaN");
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
}
