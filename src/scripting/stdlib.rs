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

/// Metadata extracted from a script's `//!` doc-comment header.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct ScriptMeta {
    /// Filename stem (e.g., `"ibs_mean_reversion"`)
    pub id: String,
    /// Human-readable display name (from `//! name:` or derived from filename)
    pub name: String,
    /// One-line description (from `//! description:`)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Strategy category for UI grouping (from `//! category:`)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
}

/// Parse `//!` doc-comment header from script source for metadata fields.
///
/// Recognized keys: `name`, `description`, `category`.
/// Lines must start with `//!` followed by `key: value`.
pub fn parse_script_meta(id: &str, source: &str) -> ScriptMeta {
    let mut name = None;
    let mut description = None;
    let mut category = None;

    for line in source.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("//!") {
            let rest = rest.trim();
            if let Some(val) = rest.strip_prefix("name:") {
                name = Some(val.trim().to_string());
            } else if let Some(val) = rest.strip_prefix("description:") {
                description = Some(val.trim().to_string());
            } else if let Some(val) = rest.strip_prefix("category:") {
                category = Some(val.trim().to_string());
            }
        } else if !trimmed.is_empty() && !trimmed.starts_with("//") {
            break; // Stop at first non-comment line
        }
    }

    ScriptMeta {
        id: id.to_string(),
        name: name.unwrap_or_else(|| id.replace('_', " ")),
        description,
        category,
    }
}

/// List `.rhai` strategy scripts with parsed metadata.
#[must_use]
pub fn list_scripts() -> Vec<ScriptMeta> {
    let dir = std::path::Path::new("scripts/strategies");
    let Ok(entries) = std::fs::read_dir(dir) else {
        return vec![];
    };
    let mut scripts: Vec<ScriptMeta> = entries
        .filter_map(|e| {
            let e = e.ok()?;
            let filename = e.file_name().to_string_lossy().to_string();
            let id = filename.strip_suffix(".rhai")?;
            let source = std::fs::read_to_string(e.path()).ok()?;
            Some(parse_script_meta(id, &source))
        })
        .collect();
    scripts.sort_by(|a, b| a.name.cmp(&b.name));
    scripts
}

/// List `.rhai` strategy file stems (legacy — use `list_scripts()` for metadata).
#[must_use]
pub fn list_strategies() -> Vec<String> {
    list_scripts().into_iter().map(|s| s.id).collect()
}
