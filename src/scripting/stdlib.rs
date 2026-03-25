//! Standard library of built-in strategy scripts and parameter injection.

use std::collections::HashMap;
use std::fmt::Write;

/// Inject parameters as `const` declarations prepended to script source.
///
/// Used for one-shot `run_script` calls. For sweep iterations, use
/// `inject_into_scope` instead (avoids recompilation).
///
/// Callers must pass `null` for optional parameters they want to leave unset —
/// this injects `const X = ();` so scripts can use `if X != () { ... }`.
pub fn inject_as_const(source: &str, params: &HashMap<String, serde_json::Value>) -> String {
    let mut preamble = String::new();
    for (key, value) in params {
        // Validate key is a valid Rhai identifier (prevents code injection)
        if !is_valid_identifier(key) {
            continue;
        }
        let rhai_val = json_to_rhai_literal(value);
        let _ = writeln!(preamble, "const {key} = {rhai_val};");
    }
    format!("{preamble}\n{source}")
}

/// Check if a string is a valid Rhai identifier (ASCII alphanumeric + underscore, not starting with digit).
fn is_valid_identifier(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
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
        serde_json::Value::String(s) => {
            // Escape special characters for Rhai string literals
            let escaped = s
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n")
                .replace('\r', "\\r")
                .replace('\t', "\\t");
            format!("\"{escaped}\"")
        }
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

/// List `.rhai` strategy files in `scripts/strategies/`.
#[must_use]
pub fn list_strategies() -> Vec<String> {
    let dir = std::path::Path::new("scripts/strategies");
    let Ok(entries) = std::fs::read_dir(dir) else {
        return vec![];
    };
    entries
        .filter_map(|e| {
            let e = e.ok()?;
            let name = e.file_name().to_string_lossy().to_string();
            name.strip_suffix(".rhai").map(String::from)
        })
        .collect()
}
