//! Custom Rhai syntax registrations for inline DSL patterns.
//!
//! These supplement the preprocessor/transpiler by adding readable syntactic
//! sugar that can be used *within* generated or hand-written Rhai scripts.
//! They are not the core DSL mechanism — the transpiler is. These patterns
//! are useful for people who mix DSL-like keywords into standard Rhai.
//!
//! # Registered Patterns
//!
//! | DSL Syntax                        | Equivalent Rhai            |
//! |-----------------------------------|---------------------------|
//! | `buy EXPR shares`                 | `buy_stock(symbol, EXPR)` |
//! | `sell EXPR shares`                | `sell_stock(symbol, EXPR)`|
//! | `sell validated EXPR shares`      | Quantity-validated sell    |
//! | `exit_position REASON`            | `close_position(REASON)`  |
//! | `hold`                            | `hold_position()`         |

use rhai::{Dynamic, Engine, Position};

/// Register all custom syntax patterns on the given engine.
///
/// Call this from `build_engine()` after registering types and functions.
pub fn register_dsl_syntax(engine: &mut Engine) {
    register_buy_shares(engine);
    // NOTE: `sell validated` must be registered BEFORE `sell` because the raw
    // `sell` parser yields to `sell validated` by returning Ok(None) on the
    // "validated" lookahead, allowing Rhai to fall back to fixed-token matches.
    register_sell_validated(engine);
    register_sell_shares(engine);
    // NOTE: "close position" syntax is NOT registered because "close" conflicts
    // with ctx.close (a BarContext property). The transpiler handles this by
    // generating close_position("reason") calls directly in the Rhai output.
    register_exit_position(engine);
    register_hold(engine);
}

/// `buy EXPR shares` → `buy_stock(symbol, EXPR)`
/// `buy EXPR shares of IDENT` → `buy_stock(IDENT, EXPR)`
///
/// Reads `symbol` from the calling scope when no `of` target is specified.
fn register_buy_shares(engine: &mut Engine) {
    engine.register_custom_syntax_with_state_raw(
        "buy",
        |symbols, look_ahead, state| match symbols.len() {
            1 => Ok(Some("$expr$".into())),
            2 => {
                if look_ahead == "shares" {
                    Ok(Some("shares".into()))
                } else {
                    Err(rhai::ParseError(
                        Box::new(rhai::ParseErrorType::BadInput(
                            rhai::LexError::ImproperSymbol(
                                "shares".into(),
                                "expected 'shares' after quantity".into(),
                            ),
                        )),
                        Position::NONE,
                    ))
                }
            }
            3 => {
                if look_ahead == "of" {
                    Ok(Some("of".into()))
                } else {
                    // No "of" — done
                    Ok(None)
                }
            }
            4 => {
                // After "of", expect an identifier (variable name)
                *state = Dynamic::from(look_ahead.to_string());
                Ok(Some(look_ahead.into()))
            }
            5 => Ok(None),
            _ => unreachable!(),
        },
        true,
        |context, inputs, state| {
            let qty = context.eval_expression_tree(&inputs[0])?;
            let qty_int = qty.as_int().map_err(|_| {
                Box::new(rhai::EvalAltResult::ErrorMismatchDataType(
                    "i64".to_string(),
                    qty.type_name().to_string(),
                    Position::NONE,
                ))
            })?;

            let mut map = rhai::Map::new();
            map.insert("action".into(), "open_stock".into());
            map.insert("side".into(), "long".into());
            map.insert("qty".into(), Dynamic::from(qty_int));

            if !state.is_unit() {
                // "of IDENT" — read the named variable from scope
                let var_name = state.clone().into_string().unwrap_or_default();
                if let Some(sym) = context
                    .scope()
                    .get_value::<rhai::ImmutableString>(&var_name)
                {
                    map.insert("symbol".into(), Dynamic::from(sym));
                }
            } else if let Some(sym) = context.scope().get_value::<rhai::ImmutableString>("symbol") {
                map.insert("symbol".into(), Dynamic::from(sym));
            }
            Ok(Dynamic::from_map(map))
        },
    );
}

/// Unified `sell` syntax handler supporting three forms:
/// - `sell EXPR shares` → `sell_stock(symbol, EXPR)`
/// - `sell EXPR shares of IDENT` → `sell_stock(IDENT, EXPR)`
/// - `sell validated EXPR shares [of IDENT]` → Quantity-validated sell
///
/// Uses state to track: `is_validated` (bool) and `target_symbol` (string).
/// State encoding: unit = plain sell, "validated" = validated mode,
/// "validated:sym" or ":sym" = with target symbol.
fn register_sell_shares(engine: &mut Engine) {
    engine.register_custom_syntax_with_state_raw(
        "sell",
        |symbols, look_ahead, state| match symbols.len() {
            // sell
            1 => {
                if look_ahead == "validated" {
                    *state = Dynamic::from("validated");
                    Ok(Some("validated".into()))
                } else {
                    Ok(Some("$expr$".into()))
                }
            }
            // sell EXPR  -or-  sell validated
            2 => {
                if state.clone().into_string().unwrap_or_default() == "validated" {
                    // sell validated → expect expression
                    Ok(Some("$expr$".into()))
                } else if look_ahead == "shares" {
                    Ok(Some("shares".into()))
                } else {
                    Err(rhai::ParseError(
                        Box::new(rhai::ParseErrorType::BadInput(
                            rhai::LexError::ImproperSymbol(
                                "shares".into(),
                                "expected 'shares' after quantity".into(),
                            ),
                        )),
                        Position::NONE,
                    ))
                }
            }
            // sell EXPR shares  -or-  sell validated EXPR
            3 => {
                let st = state.clone().into_string().unwrap_or_default();
                if st == "validated" {
                    // sell validated EXPR → expect "shares"
                    if look_ahead == "shares" {
                        Ok(Some("shares".into()))
                    } else {
                        Err(rhai::ParseError(
                            Box::new(rhai::ParseErrorType::BadInput(
                                rhai::LexError::ImproperSymbol(
                                    "shares".into(),
                                    "expected 'shares' after quantity".into(),
                                ),
                            )),
                            Position::NONE,
                        ))
                    }
                } else if look_ahead == "of" {
                    // sell EXPR shares of
                    Ok(Some("of".into()))
                } else {
                    Ok(None)
                }
            }
            // sell EXPR shares of  -or-  sell validated EXPR shares
            4 => {
                let st = state.clone().into_string().unwrap_or_default();
                if st == "validated" {
                    // sell validated EXPR shares → optional "of"
                    if look_ahead == "of" {
                        Ok(Some("of".into()))
                    } else {
                        Ok(None)
                    }
                } else {
                    // sell EXPR shares of → expect IDENT
                    let sym_state = format!(":{look_ahead}");
                    *state = Dynamic::from(sym_state);
                    Ok(Some(look_ahead.into()))
                }
            }
            // sell EXPR shares of IDENT  -or-  sell validated EXPR shares of
            5 => {
                let st = state.clone().into_string().unwrap_or_default();
                if st == "validated" {
                    // sell validated EXPR shares of → expect IDENT
                    let sym_state = format!("validated:{look_ahead}");
                    *state = Dynamic::from(sym_state);
                    Ok(Some(look_ahead.into()))
                } else {
                    Ok(None)
                }
            }
            // sell validated EXPR shares of IDENT
            6 => Ok(None),
            _ => unreachable!(),
        },
        true,
        |context, inputs, state| {
            let st = state.clone().into_string().unwrap_or_default();
            let is_validated = st.starts_with("validated");
            let target_sym = st
                .split_once(':')
                .map(|(_, s)| s.to_string())
                .filter(|s| !s.is_empty());

            let qty = context.eval_expression_tree(&inputs[0])?;
            let qty_int = qty.as_int().map_err(|_| {
                Box::new(rhai::EvalAltResult::ErrorMismatchDataType(
                    "i64".to_string(),
                    qty.type_name().to_string(),
                    Position::NONE,
                ))
            })?;

            // Validated mode: refuse zero or negative quantities
            if is_validated && qty_int <= 0 {
                return Ok(Dynamic::UNIT);
            }

            let mut map = rhai::Map::new();
            map.insert("action".into(), "open_stock".into());
            map.insert("side".into(), "short".into());
            map.insert("qty".into(), Dynamic::from(qty_int));

            if let Some(var_name) = target_sym {
                if let Some(sym) = context
                    .scope()
                    .get_value::<rhai::ImmutableString>(&var_name)
                {
                    map.insert("symbol".into(), Dynamic::from(sym));
                }
            } else if let Some(sym) = context.scope().get_value::<rhai::ImmutableString>("symbol") {
                map.insert("symbol".into(), Dynamic::from(sym));
            }
            Ok(Dynamic::from_map(map))
        },
    );
}

/// `sell validated` is now handled by the unified `register_sell_shares`.
/// This function is kept as a no-op so the registration call site doesn't change.
fn register_sell_validated(_engine: &mut Engine) {
    // Merged into register_sell_shares
}

/// `exit_position REASON` → `close_position(REASON)`
///
/// Uses `exit_position` instead of `close position` because `close` conflicts
/// with `ctx.close` (the BarContext property for current bar close price).
fn register_exit_position(engine: &mut Engine) {
    engine
        .register_custom_syntax(["exit_position", "$expr$"], false, |context, inputs| {
            let reason = context.eval_expression_tree(&inputs[0])?;
            let reason_str = reason.into_string().map_err(|_| {
                Box::new(rhai::EvalAltResult::ErrorMismatchDataType(
                    "String".to_string(),
                    "non-string".to_string(),
                    Position::NONE,
                ))
            })?;

            let mut map = rhai::Map::new();
            map.insert("action".into(), "close".into());
            map.insert("reason".into(), Dynamic::from(reason_str));
            Ok(Dynamic::from_map(map))
        })
        .expect("failed to register 'exit_position' syntax");
}

/// `hold` → `hold_position()`
fn register_hold(engine: &mut Engine) {
    engine
        .register_custom_syntax(["hold"], false, |_context, _inputs| {
            let mut map = rhai::Map::new();
            map.insert("action".into(), "hold".into());
            Ok(Dynamic::from_map(map))
        })
        .expect("failed to register 'hold' syntax");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buy_shares_syntax() {
        let mut engine = Engine::new();
        register_buy_shares(&mut engine);

        let result: Dynamic = engine.eval("buy 100 shares").unwrap();
        let map = result.cast::<rhai::Map>();
        assert_eq!(
            map.get("action").unwrap().clone().into_string().unwrap(),
            "open_stock"
        );
        assert_eq!(
            map.get("side").unwrap().clone().into_string().unwrap(),
            "long"
        );
        assert_eq!(map.get("qty").unwrap().as_int().unwrap(), 100);
    }

    #[test]
    fn test_exit_position_syntax() {
        let mut engine = Engine::new();
        register_exit_position(&mut engine);

        let result: Dynamic = engine.eval(r#"exit_position "stop_loss""#).unwrap();
        let map = result.cast::<rhai::Map>();
        assert_eq!(
            map.get("action").unwrap().clone().into_string().unwrap(),
            "close"
        );
        assert_eq!(
            map.get("reason").unwrap().clone().into_string().unwrap(),
            "stop_loss"
        );
    }

    #[test]
    fn test_hold_syntax() {
        let mut engine = Engine::new();
        register_hold(&mut engine);

        let result: Dynamic = engine.eval("hold").unwrap();
        let map = result.cast::<rhai::Map>();
        assert_eq!(
            map.get("action").unwrap().clone().into_string().unwrap(),
            "hold"
        );
    }

    #[test]
    fn test_sell_validated_zero_returns_unit() {
        let mut engine = Engine::new();
        register_sell_shares(&mut engine);

        let result: Dynamic = engine.eval("sell validated 0 shares").unwrap();
        assert!(result.is_unit());
    }

    #[test]
    fn test_sell_validated_negative_returns_unit() {
        let mut engine = Engine::new();
        register_sell_shares(&mut engine);

        let result: Dynamic = engine.eval("sell validated -5 shares").unwrap();
        assert!(result.is_unit());
    }

    #[test]
    fn test_sell_validated_positive() {
        let mut engine = Engine::new();
        register_sell_shares(&mut engine);

        let result: Dynamic = engine.eval("sell validated 50 shares").unwrap();
        let map = result.cast::<rhai::Map>();
        assert_eq!(map.get("qty").unwrap().as_int().unwrap(), 50);
    }

    #[test]
    fn test_buy_shares_of_symbol() {
        let mut engine = Engine::new();
        register_buy_shares(&mut engine);

        let mut scope = rhai::Scope::new();
        scope.push("spy", "SPY".to_string());

        let result: Dynamic = engine
            .eval_with_scope::<Dynamic>(&mut scope, "buy 10 shares of spy")
            .unwrap();
        let map = result.cast::<rhai::Map>();
        assert_eq!(map.get("qty").unwrap().as_int().unwrap(), 10);
        assert_eq!(
            map.get("symbol").unwrap().clone().into_string().unwrap(),
            "SPY"
        );
    }

    #[test]
    fn test_sell_shares_of_symbol() {
        let mut engine = Engine::new();
        register_sell_shares(&mut engine);

        let mut scope = rhai::Scope::new();
        scope.push("qqq", "QQQ".to_string());

        let result: Dynamic = engine
            .eval_with_scope::<Dynamic>(&mut scope, "sell 5 shares of qqq")
            .unwrap();
        let map = result.cast::<rhai::Map>();
        assert_eq!(map.get("qty").unwrap().as_int().unwrap(), 5);
        assert_eq!(
            map.get("symbol").unwrap().clone().into_string().unwrap(),
            "QQQ"
        );
    }

    #[test]
    fn test_sell_validated_of_symbol() {
        let mut engine = Engine::new();
        register_sell_shares(&mut engine);

        let mut scope = rhai::Scope::new();
        scope.push("qqq", "QQQ".to_string());

        let result: Dynamic = engine
            .eval_with_scope::<Dynamic>(&mut scope, "sell validated 20 shares of qqq")
            .unwrap();
        let map = result.cast::<rhai::Map>();
        assert_eq!(map.get("qty").unwrap().as_int().unwrap(), 20);
        assert_eq!(
            map.get("symbol").unwrap().clone().into_string().unwrap(),
            "QQQ"
        );
    }
}
