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
///
/// Reads `symbol` from the calling scope to populate the action map.
fn register_buy_shares(engine: &mut Engine) {
    engine.register_custom_syntax_with_state_raw(
        "buy",
        |symbols, look_ahead, _state| match symbols.len() {
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
            3 => Ok(None),
            _ => unreachable!(),
        },
        true,
        |context, inputs, _state| {
            let qty = context.eval_expression_tree(&inputs[0])?;
            let qty_int = qty.as_int().map_err(|_| {
                Box::new(rhai::EvalAltResult::ErrorMismatchDataType(
                    "i64".to_string(),
                    qty.type_name().to_string(),
                    Position::NONE,
                ))
            })?;

            // Read `symbol` from scope (set by extern_symbol or let binding)
            let mut map = rhai::Map::new();
            map.insert("action".into(), "open_stock".into());
            map.insert("side".into(), "long".into());
            map.insert("qty".into(), Dynamic::from(qty_int));
            // Only insert symbol if it exists in scope; omitting lets the engine
            // fall back to config.symbol instead of matching on empty string.
            if let Some(sym) = context.scope().get_value::<rhai::ImmutableString>("symbol") {
                map.insert("symbol".into(), Dynamic::from(sym));
            }
            Ok(Dynamic::from_map(map))
        },
    );
}

/// `sell EXPR shares` → `sell_stock(symbol, EXPR)`
///
/// Reads `symbol` from the calling scope to populate the action map.
fn register_sell_shares(engine: &mut Engine) {
    engine.register_custom_syntax_with_state_raw(
        "sell",
        |symbols, look_ahead, _state| match symbols.len() {
            1 => {
                if look_ahead == "validated" {
                    // Don't claim this token — let the `sell validated` fixed
                    // syntax handle it by signaling no match here.
                    return Ok(None);
                }
                Ok(Some("$expr$".into()))
            }
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
            3 => Ok(None),
            _ => unreachable!(),
        },
        true,
        |context, inputs, _state| {
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
            map.insert("side".into(), "short".into());
            map.insert("qty".into(), Dynamic::from(qty_int));
            if let Some(sym) = context.scope().get_value::<rhai::ImmutableString>("symbol") {
                map.insert("symbol".into(), Dynamic::from(sym));
            }
            Ok(Dynamic::from_map(map))
        },
    );
}

/// `sell validated EXPR shares` → Quantity-sign validated sell.
///
/// Guards against zero/negative quantities produced by the expression.
/// Returns `()` (no action) if the quantity is zero or negative.
///
/// Note: this does *not* validate against current portfolio holdings —
/// that responsibility lives in the engine's execution layer which
/// validates all actions against portfolio state before processing.
fn register_sell_validated(engine: &mut Engine) {
    engine
        .register_custom_syntax(
            ["sell", "validated", "$expr$", "shares"],
            true,
            |context, inputs| {
                let requested_qty = context.eval_expression_tree(&inputs[0])?;
                let requested = requested_qty.as_int().map_err(|_| {
                    Box::new(rhai::EvalAltResult::ErrorMismatchDataType(
                        "i64".to_string(),
                        requested_qty.type_name().to_string(),
                        Position::NONE,
                    ))
                })?;

                // Clamp: refuse to sell zero or negative quantities
                if requested <= 0 {
                    return Ok(Dynamic::UNIT);
                }

                let mut map = rhai::Map::new();
                map.insert("action".into(), "open_stock".into());
                map.insert("side".into(), "short".into());
                map.insert("qty".into(), Dynamic::from(requested));
                if let Some(sym) = context.scope().get_value::<rhai::ImmutableString>("symbol") {
                    map.insert("symbol".into(), Dynamic::from(sym));
                }
                Ok(Dynamic::from_map(map))
            },
        )
        .expect("failed to register 'sell validated' syntax");
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
        register_sell_validated(&mut engine);

        let result: Dynamic = engine.eval("sell validated 0 shares").unwrap();
        assert!(result.is_unit());
    }

    #[test]
    fn test_sell_validated_negative_returns_unit() {
        let mut engine = Engine::new();
        register_sell_validated(&mut engine);

        let result: Dynamic = engine.eval("sell validated -5 shares").unwrap();
        assert!(result.is_unit());
    }

    #[test]
    fn test_sell_validated_positive() {
        let mut engine = Engine::new();
        register_sell_validated(&mut engine);

        let result: Dynamic = engine.eval("sell validated 50 shares").unwrap();
        let map = result.cast::<rhai::Map>();
        assert_eq!(map.get("qty").unwrap().as_int().unwrap(), 50);
    }
}
