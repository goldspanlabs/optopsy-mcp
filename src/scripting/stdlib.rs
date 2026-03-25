//! Standard library of built-in strategy scripts and parameter injection.

use std::collections::HashMap;
use std::fmt::Write;

use anyhow::{bail, Result};

/// Inject parameters as `const` declarations prepended to script source.
///
/// Used for one-shot `run_script` calls. For sweep iterations, use
/// `inject_into_scope` instead (avoids recompilation).
///
/// Optional parameters not provided by the caller are injected as `const X = ();`
/// so scripts can use `if X != () { ... }` without runtime errors.
pub fn inject_as_const(source: &str, params: &HashMap<String, serde_json::Value>) -> String {
    let mut preamble = String::new();
    for (key, value) in params {
        let rhai_val = json_to_rhai_literal(value);
        let _ = writeln!(preamble, "const {key} = {rhai_val};");
    }
    format!("{preamble}\n{source}")
}

/// Inject parameters into an existing Rhai `Scope` as variables.
///
/// Used for `parameter_sweep` iterations where the AST is compiled once
/// and only scope values change per iteration.
pub fn inject_into_scope(scope: &mut rhai::Scope, params: &HashMap<String, serde_json::Value>) {
    for (key, value) in params {
        match value {
            serde_json::Value::Number(n) => {
                if let Some(f) = n.as_f64() {
                    scope.set_or_push(key.as_str(), f);
                }
            }
            serde_json::Value::String(s) => {
                scope.set_or_push(key.as_str(), s.clone());
            }
            serde_json::Value::Bool(b) => {
                scope.set_or_push(key.as_str(), *b);
            }
            serde_json::Value::Null => {
                scope.set_or_push(key.as_str(), ());
            }
            _ => {
                // Arrays and objects: convert to string representation
                scope.set_or_push(key.as_str(), value.to_string());
            }
        }
    }
}

/// Convert a JSON value to a Rhai literal string for `const` injection.
fn json_to_rhai_literal(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "()".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => {
            let s = n.to_string();
            // Ensure floats have a decimal point for Rhai
            if s.contains('.') {
                s
            } else {
                format!("{s}.0")
            }
        }
        serde_json::Value::String(s) => format!("\"{s}\""),
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(json_to_rhai_literal).collect();
            format!("[{}]", items.join(", "))
        }
        serde_json::Value::Object(map) => {
            let items: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{k}: {}", json_to_rhai_literal(v)))
                .collect();
            format!("#{{{}}}", items.join(", "))
        }
    }
}

/// List available built-in strategy script names.
#[must_use]
pub fn list_strategies() -> Vec<&'static str> {
    vec!["short_put", "iron_condor", "wheel"]
}

/// Load a built-in strategy script source by name.
pub fn load_strategy(name: &str) -> Result<&'static str> {
    match name {
        "short_put" => Ok(include_str!("../../scripts/strategies/short_put.rhai")),
        "iron_condor" => Ok(include_str!("../../scripts/strategies/iron_condor.rhai")),
        "wheel" => Ok(include_str!("../../scripts/strategies/wheel.rhai")),
        _ => bail!(
            "Strategy script '{name}' not found. Available: {:?}",
            list_strategies()
        ),
    }
}
