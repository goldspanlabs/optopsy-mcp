//! Standard library of built-in strategy scripts and parameter injection.

use std::collections::HashMap;

use rhai::Dynamic;

/// Build a Rhai `Map` from JSON parameters and push it into scope as an
/// immutable `params` constant.  Scripts access values via `params.SYMBOL`,
/// `params.CAPITAL`, etc.
///
/// Callers must pass `null` for optional parameters they want to leave unset —
/// this inserts `()` so scripts can use `if params.X != () { ... }`.
pub fn inject_params_map(scope: &mut rhai::Scope, params: &HashMap<String, serde_json::Value>) {
    let mut map = rhai::Map::new();
    for (key, value) in params {
        if !is_valid_identifier(key) {
            continue;
        }
        map.insert(key.as_str().into(), json_to_dynamic(value));
    }
    scope.push_constant("params", map);
}

/// Check if a string is a valid Rhai identifier (ASCII alphanumeric + underscore, not starting with digit).
fn is_valid_identifier(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Rebuild the `params` map in an existing scope with new values.
///
/// Used for `parameter_sweep` iterations where the AST is compiled once
/// and only scope values change per iteration.
pub fn inject_into_scope(scope: &mut rhai::Scope, params: &HashMap<String, serde_json::Value>) {
    let mut map = rhai::Map::new();
    for (key, value) in params {
        if !is_valid_identifier(key) {
            continue;
        }
        map.insert(key.as_str().into(), json_to_dynamic(value));
    }
    scope.set_or_push("params", map);
}

/// Convert a JSON value to a Rhai `Dynamic`.
fn json_to_dynamic(value: &serde_json::Value) -> Dynamic {
    match value {
        serde_json::Value::Null => Dynamic::UNIT,
        serde_json::Value::Bool(b) => Dynamic::from(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Dynamic::from(i)
            } else if let Some(f) = n.as_f64() {
                Dynamic::from(f)
            } else {
                Dynamic::UNIT
            }
        }
        serde_json::Value::String(s) => Dynamic::from(s.clone()),
        serde_json::Value::Array(arr) => {
            Dynamic::from(arr.iter().map(json_to_dynamic).collect::<Vec<Dynamic>>())
        }
        serde_json::Value::Object(obj) => {
            let mut m = rhai::Map::new();
            for (k, v) in obj {
                m.insert(k.as_str().into(), json_to_dynamic(v));
            }
            Dynamic::from(m)
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
