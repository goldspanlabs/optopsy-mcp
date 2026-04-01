//! DSL parser: converts trading DSL strings into Rhai action map source code.
//!
//! The parser handles simple order syntax and emits Rhai code that produces
//! the same action maps as the helper functions.

use anyhow::{bail, Result};

/// Compile a DSL string into Rhai source code that returns an action map.
///
/// # Examples
///
/// ```ignore
/// compile_dsl("buy 100 shares")
/// // => r#"[#{ action: "open_stock", side: "long", qty: 100 }]"#
///
/// compile_dsl("buy 100 shares at 150.00 limit")
/// // => r#"[#{ action: "open_stock", side: "long", qty: 100, order_type: "limit", limit_price: 150.0 }]"#
///
/// compile_dsl("cancel all orders")
/// // => r#"[#{ action: "cancel_orders" }]"#
/// ```
pub fn compile_dsl(input: &str) -> Result<String> {
    let input = input.trim();
    if input.is_empty() {
        bail!("Empty DSL input");
    }

    let tokens: Vec<&str> = input.split_whitespace().collect();
    if tokens.is_empty() {
        bail!("Empty DSL input");
    }

    match tokens[0].to_lowercase().as_str() {
        "buy" | "sell" => parse_order(&tokens),
        "cancel" => parse_cancel(&tokens),
        _ => bail!(
            "Unknown DSL command: '{}'. Expected 'buy', 'sell', or 'cancel'",
            tokens[0]
        ),
    }
}

/// Parse `buy/sell <qty> shares [at <price> limit|stop] [at <price> stop <price> limit]`
fn parse_order(tokens: &[&str]) -> Result<String> {
    if tokens.len() < 3 {
        bail!("Order syntax: buy/sell <qty> shares [at <price> limit|stop]");
    }

    let side = match tokens[0].to_lowercase().as_str() {
        "buy" => "long",
        "sell" => "short",
        _ => unreachable!(),
    };

    let qty: i64 = tokens[1]
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid quantity: '{}'", tokens[1]))?;

    if qty <= 0 {
        bail!("Quantity must be positive, got {qty}");
    }

    // tokens[2] should be "shares" (optional, but expected)
    let rest_start = if tokens[2].to_lowercase() == "shares" {
        3
    } else {
        2
    };

    let rest = &tokens[rest_start..];

    if rest.is_empty() {
        // Market order
        return Ok(format!(
            r#"[#{{ action: "open_stock", side: "{side}", qty: {qty} }}]"#
        ));
    }

    // Parse order type modifiers
    if rest[0].to_lowercase() != "at" {
        bail!("Expected 'at' after 'shares', got '{}'", rest[0]);
    }

    if rest.len() < 3 {
        bail!("Order syntax: ... at <price> limit|stop");
    }

    let price1: f64 = rest[1]
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid price: '{}'", rest[1]))?;

    let order_type_str = rest[2].to_lowercase();

    match order_type_str.as_str() {
        "limit" => Ok(format!(
            r#"[#{{ action: "open_stock", side: "{side}", qty: {qty}, order_type: "limit", limit_price: {price1} }}]"#
        )),
        "stop" => {
            // Check for stop-limit: ... at <stop> stop <limit> limit
            if rest.len() >= 5 && rest[4].to_lowercase() == "limit" {
                let price2: f64 = rest[3]
                    .parse()
                    .map_err(|_| anyhow::anyhow!("Invalid limit price: '{}'", rest[3]))?;
                Ok(format!(
                    r#"[#{{ action: "open_stock", side: "{side}", qty: {qty}, order_type: "stop_limit", stop_price: {price1}, limit_price: {price2} }}]"#
                ))
            } else {
                Ok(format!(
                    r#"[#{{ action: "open_stock", side: "{side}", qty: {qty}, order_type: "stop", stop_price: {price1} }}]"#
                ))
            }
        }
        _ => bail!("Unknown order type: '{order_type_str}'. Expected 'limit' or 'stop'"),
    }
}

/// Parse `cancel all orders`
fn parse_cancel(tokens: &[&str]) -> Result<String> {
    if tokens.len() >= 3
        && tokens[1].to_lowercase() == "all"
        && tokens[2].to_lowercase() == "orders"
    {
        Ok(r#"[#{ action: "cancel_orders" }]"#.to_string())
    } else {
        bail!("Cancel syntax: 'cancel all orders'");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_market_buy() {
        let result = compile_dsl("buy 100 shares").unwrap();
        assert!(result.contains(r#"action: "open_stock""#));
        assert!(result.contains(r#"side: "long""#));
        assert!(result.contains("qty: 100"));
    }

    #[test]
    fn test_market_sell() {
        let result = compile_dsl("sell 50 shares").unwrap();
        assert!(result.contains(r#"side: "short""#));
        assert!(result.contains("qty: 50"));
    }

    #[test]
    fn test_limit_buy() {
        let result = compile_dsl("buy 100 shares at 150.0 limit").unwrap();
        assert!(result.contains(r#"order_type: "limit""#));
        assert!(result.contains("limit_price: 150"));
    }

    #[test]
    fn test_stop_sell() {
        let result = compile_dsl("sell 100 shares at 145.0 stop").unwrap();
        assert!(result.contains(r#"order_type: "stop""#));
        assert!(result.contains("stop_price: 145"));
    }

    #[test]
    fn test_stop_limit_buy() {
        let result = compile_dsl("buy 100 shares at 155.0 stop 153.0 limit").unwrap();
        assert!(result.contains(r#"order_type: "stop_limit""#));
        assert!(result.contains("stop_price: 155"));
        assert!(result.contains("limit_price: 153"));
    }

    #[test]
    fn test_cancel_all() {
        let result = compile_dsl("cancel all orders").unwrap();
        assert!(result.contains(r#"action: "cancel_orders""#));
    }

    #[test]
    fn test_empty_input() {
        assert!(compile_dsl("").is_err());
    }

    #[test]
    fn test_unknown_command() {
        assert!(compile_dsl("wait 100 shares").is_err());
    }
}
