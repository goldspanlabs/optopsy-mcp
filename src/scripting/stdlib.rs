//! Standard library of built-in strategy scripts and parameter injection.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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
pub fn json_to_dynamic(value: &serde_json::Value) -> Dynamic {
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

/// A script parameter declared via `extern("NAME", default, "description")` or
/// `extern("NAME", default, "description", ["opt1", "opt2"])` for enum params.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct ExternParam {
    /// Parameter name (e.g., "PUT_DELTA")
    pub name: String,
    /// Default value — None means required
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
    /// Human-readable description
    pub description: String,
    /// Type: "string", "number", or "bool"
    pub param_type: String,
    /// Allowed values for enum-style params (renders as dropdown in UI)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<serde_json::Value>,
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
    /// Parameters declared via `extern()` in the script
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<ExternParam>,
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
                let val = val.trim();
                if !val.is_empty() {
                    name = Some(val.to_string());
                }
            } else if let Some(val) = rest.strip_prefix("description:") {
                let val = val.trim();
                if !val.is_empty() {
                    description = Some(val.to_string());
                }
            } else if let Some(val) = rest.strip_prefix("category:") {
                let val = val.trim();
                if !val.is_empty() {
                    category = Some(val.to_string());
                }
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
        params: Vec::new(),
    }
}

/// Convert a Rhai Dynamic to a serde_json::Value for storage in ExternParam.
fn dynamic_to_json(val: &Dynamic) -> (Option<serde_json::Value>, String) {
    if val.is_unit() {
        (None, "string".to_string()) // unit = required, type unknown — default to string
    } else if val.is_int() {
        let i = val.as_int().unwrap_or(0);
        (Some(serde_json::json!(i)), "number".to_string())
    } else if val.is_float() {
        let f = val.as_float().unwrap_or(0.0);
        (Some(serde_json::json!(f)), "number".to_string())
    } else if val.is_bool() {
        let b = val.as_bool().unwrap_or(false);
        (Some(serde_json::json!(b)), "bool".to_string())
    } else if val.is_string() {
        let s = val.clone().into_string().unwrap_or_default();
        (Some(serde_json::json!(s)), "string".to_string())
    } else {
        (None, "string".to_string())
    }
}

/// Extract `extern()` parameter declarations from a script without running it.
///
/// Compiles the script with an `extern()` function that captures declarations
/// into a shared vec, evaluates top-level statements, then returns the collected params.
pub fn extract_extern_params(script_source: &str) -> Vec<ExternParam> {
    // Use the full engine so scripts that reference registered functions
    // (hold_position, close_position, buy_stock, etc.) can compile and eval.
    let mut engine = super::registration::build_engine();

    let collected: Arc<Mutex<Vec<ExternParam>>> = Arc::new(Mutex::new(Vec::new()));

    // Register extern(name, default, description) — 3-arg form
    let collector = collected.clone();
    engine.register_fn(
        "extern",
        move |name: &str, default: Dynamic, desc: &str| -> Dynamic {
            let (default_val, param_type) = dynamic_to_json(&default);
            if let Ok(mut params) = collector.lock() {
                params.push(ExternParam {
                    name: name.to_string(),
                    default: default_val,
                    description: desc.to_string(),
                    param_type,
                    options: Vec::new(),
                });
            }
            if default.is_unit() {
                Dynamic::from("")
            } else {
                default
            }
        },
    );

    // Register extern(name, default, description, options) — 4-arg form for enums
    let collector4 = collected.clone();
    engine.register_fn(
        "extern",
        move |name: &str, default: Dynamic, desc: &str, opts: rhai::Array| -> Dynamic {
            let (default_val, param_type) = dynamic_to_json(&default);
            let options: Vec<serde_json::Value> = opts
                .iter()
                .filter_map(|v| {
                    if v.is_string() {
                        Some(serde_json::Value::String(
                            v.clone().into_string().unwrap_or_default(),
                        ))
                    } else if v.is_int() {
                        Some(serde_json::json!(v.as_int().unwrap_or(0)))
                    } else if v.is_float() {
                        Some(serde_json::json!(v.as_float().unwrap_or(0.0)))
                    } else {
                        None
                    }
                })
                .collect();
            if let Ok(mut params) = collector4.lock() {
                params.push(ExternParam {
                    name: name.to_string(),
                    default: default_val,
                    description: desc.to_string(),
                    param_type,
                    options,
                });
            }
            if default.is_unit() {
                Dynamic::from("")
            } else {
                default
            }
        },
    );

    // Compile and evaluate top-level statements to trigger extern() calls
    let Ok(ast) = engine.compile(script_source) else {
        return Vec::new();
    };

    // Inject an empty params map so scripts referencing params.SYMBOL etc.
    // don't crash during extraction (they'll just get unit values).
    let mut scope = rhai::Scope::new();
    scope.push_constant("params", rhai::Map::new());
    let _ = engine.eval_ast_with_scope::<Dynamic>(&mut scope, &ast);

    // Return collected params
    Arc::try_unwrap(collected)
        .unwrap_or_else(|arc| (*arc).lock().unwrap().clone().into())
        .into_inner()
        .unwrap_or_default()
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
            let mut meta = parse_script_meta(id, &source);
            meta.params = extract_extern_params(&source);
            Some(meta)
        })
        .collect();
    scripts.sort_by(|a, b| a.name.cmp(&b.name));
    scripts
}
