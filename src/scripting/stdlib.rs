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
    /// Research hypothesis this strategy is testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hypothesis: Option<String>,
    /// Searchable tags for categorization (parsed from comma-separated `//! tags:` header).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    /// Expected market regime(s) (parsed from comma-separated `//! regime:` header).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regime: Option<Vec<String>>,
    /// Asset-class parameter profiles (parsed from `//! profile.<name>:` headers).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profiles: Option<HashMap<String, HashMap<String, serde_json::Value>>>,
}

/// Parse `//!` doc-comment header from script source for metadata fields.
///
/// Recognized keys: `name`, `description`, `category`, `hypothesis`, `tags`, `regime`,
/// and `profile.<name>:` for asset-class parameter profiles.
/// Lines must start with `//!` followed by `key: value`.
/// `tags` and `regime` accept comma-separated values.
/// `profile.<name>:` accepts comma-separated `key=value` pairs.
pub fn parse_script_meta(id: &str, source: &str) -> ScriptMeta {
    let mut name = None;
    let mut description = None;
    let mut category = None;
    let mut hypothesis = None;
    let mut tags = None;
    let mut regime = None;
    let mut profiles: HashMap<String, HashMap<String, serde_json::Value>> = HashMap::new();

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
            } else if let Some(val) = rest.strip_prefix("hypothesis:") {
                let val = val.trim();
                if !val.is_empty() {
                    hypothesis = Some(val.to_string());
                }
            } else if let Some(val) = rest.strip_prefix("tags:") {
                let items: Vec<String> = val
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                if !items.is_empty() {
                    tags = Some(items);
                }
            } else if let Some(val) = rest.strip_prefix("regime:") {
                let items: Vec<String> = val
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                if !items.is_empty() {
                    regime = Some(items);
                }
            } else if let Some(rest_profile) = rest.strip_prefix("profile.") {
                if let Some((profile_name, kv_str)) = rest_profile.split_once(':') {
                    let profile_name = profile_name.trim();
                    let kv_str = kv_str.trim();
                    if !profile_name.is_empty() && !kv_str.is_empty() {
                        let entry = profiles.entry(profile_name.to_string()).or_default();
                        for pair in kv_str.split(',') {
                            let pair = pair.trim();
                            if let Some((k, v)) = pair.split_once('=') {
                                let k = k.trim();
                                let v = v.trim();
                                if !k.is_empty() {
                                    entry.insert(k.to_string(), parse_profile_value(v));
                                }
                            }
                        }
                    }
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
        hypothesis,
        tags,
        regime,
        profiles: if profiles.is_empty() { None } else { Some(profiles) },
    }
}

/// Parse a scalar value string into a `serde_json::Value`.
fn parse_profile_value(s: &str) -> serde_json::Value {
    if let Ok(i) = s.parse::<i64>() {
        serde_json::json!(i)
    } else if let Ok(f) = s.parse::<f64>() {
        serde_json::json!(f)
    } else if s == "true" {
        serde_json::json!(true)
    } else if s == "false" {
        serde_json::json!(false)
    } else {
        serde_json::json!(s)
    }
}

/// Convert a `toml::Value` to a `serde_json::Value` (scalar types only).
fn toml_to_json(val: &toml::Value) -> serde_json::Value {
    match val {
        toml::Value::String(s) => serde_json::Value::String(s.clone()),
        toml::Value::Integer(i) => serde_json::json!(i),
        toml::Value::Float(f) => serde_json::json!(f),
        toml::Value::Boolean(b) => serde_json::json!(b),
        _ => serde_json::Value::Null,
    }
}

/// Parse a TOML profiles string into a map of profile name → param name → JSON value.
pub fn parse_profiles_toml(content: &str) -> HashMap<String, HashMap<String, serde_json::Value>> {
    let table: toml::Table = content.parse().unwrap_or_default();
    let mut profiles = HashMap::new();
    for (profile_name, value) in &table {
        if let Some(params_table) = value.as_table() {
            let mut params = HashMap::new();
            for (key, val) in params_table {
                params.insert(key.clone(), toml_to_json(val));
            }
            profiles.insert(profile_name.clone(), params);
        }
    }
    profiles
}

/// Load the central profiles registry from `scripts/profiles.toml`.
/// Returns an empty map if the file doesn't exist or can't be parsed.
pub fn load_profiles_registry() -> HashMap<String, HashMap<String, serde_json::Value>> {
    let path = std::path::Path::new("scripts/profiles.toml");
    match std::fs::read_to_string(path) {
        Ok(content) => parse_profiles_toml(&content),
        Err(_) => HashMap::new(),
    }
}

/// Merge parameter values from three layers: registry → script profile → caller params.
/// Each layer overrides the previous.
pub fn merge_profile_params(
    profile_name: &str,
    registry: &HashMap<String, HashMap<String, serde_json::Value>>,
    script_profiles: Option<&HashMap<String, HashMap<String, serde_json::Value>>>,
    caller_params: &HashMap<String, serde_json::Value>,
) -> HashMap<String, serde_json::Value> {
    let mut merged = HashMap::new();

    // Layer 1: central registry
    if let Some(reg_profile) = registry.get(profile_name) {
        merged.extend(reg_profile.clone());
    }

    // Layer 2: script-level profile
    if let Some(script_profiles) = script_profiles {
        if let Some(script_profile) = script_profiles.get(profile_name) {
            merged.extend(script_profile.clone());
        }
    }

    // Layer 3: caller params (highest precedence)
    merged.extend(caller_params.clone());

    merged
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_provenance_fields() {
        let source = "//! name: Test Strategy\n//! description: A test\n//! category: test\n//! hypothesis: Low IBS reverts in uptrends\n//! tags: mean_reversion, options, short_put\n//! regime: uptrend, low_volatility\n";
        let meta = parse_script_meta("test", source);
        assert_eq!(
            meta.hypothesis.as_deref(),
            Some("Low IBS reverts in uptrends")
        );
        assert_eq!(
            meta.tags,
            Some(vec![
                "mean_reversion".to_string(),
                "options".to_string(),
                "short_put".to_string()
            ])
        );
        assert_eq!(
            meta.regime,
            Some(vec!["uptrend".to_string(), "low_volatility".to_string()])
        );
    }

    #[test]
    fn test_parse_no_provenance_fields() {
        let source = "//! name: Basic Strategy\n//! description: No provenance\n";
        let meta = parse_script_meta("basic", source);
        assert!(meta.hypothesis.is_none());
        assert!(meta.tags.is_none());
        assert!(meta.regime.is_none());
    }

    #[test]
    fn test_load_profiles_from_toml() {
        let toml_content = r#"
[equities]
delta = 0.30
dte = 45
stop_pct = 0.05

[crypto]
delta = 0.25
dte = 14
stop_pct = 0.15
"#;
        let profiles = parse_profiles_toml(toml_content);
        assert_eq!(profiles.len(), 2);
        let equities = &profiles["equities"];
        assert_eq!(equities["delta"], serde_json::json!(0.3));
        assert_eq!(equities["dte"], serde_json::json!(45));
        let crypto = &profiles["crypto"];
        assert_eq!(crypto["delta"], serde_json::json!(0.25));
        assert_eq!(crypto["dte"], serde_json::json!(14));
    }

    #[test]
    fn test_parse_script_profiles() {
        let source = r#"//! name: Test Strategy
//! profile.equities: delta=0.30, dte=45, ibs_threshold=0.2
//! profile.crypto: delta=0.20, dte=14
"#;
        let meta = parse_script_meta("test", source);
        let profiles = meta.profiles.as_ref().expect("profiles should be present");
        assert_eq!(profiles.len(), 2);
        let eq = &profiles["equities"];
        assert_eq!(eq["delta"], serde_json::json!(0.3));
        assert_eq!(eq["dte"], serde_json::json!(45));
        assert_eq!(eq["ibs_threshold"], serde_json::json!(0.2));
        let cr = &profiles["crypto"];
        assert_eq!(cr["delta"], serde_json::json!(0.2));
        assert_eq!(cr["dte"], serde_json::json!(14));
    }

    #[test]
    fn test_parse_no_profiles() {
        let source = "//! name: Basic\n";
        let meta = parse_script_meta("basic", source);
        assert!(meta.profiles.is_none());
    }

    #[test]
    fn test_merge_profiles() {
        let registry = parse_profiles_toml(
            "[equities]\ndelta = 0.30\ndte = 45\nlookback = 20\n",
        );

        let mut script_profiles = HashMap::new();
        let mut eq_overrides = HashMap::new();
        eq_overrides.insert("delta".to_string(), serde_json::json!(0.35));
        eq_overrides.insert("ibs_threshold".to_string(), serde_json::json!(0.2));
        script_profiles.insert("equities".to_string(), eq_overrides);

        let caller_params: HashMap<String, serde_json::Value> =
            vec![("dte".to_string(), serde_json::json!(30))]
                .into_iter()
                .collect();

        let merged = merge_profile_params("equities", &registry, Some(&script_profiles), &caller_params);

        assert_eq!(merged["delta"], serde_json::json!(0.35));
        assert_eq!(merged["dte"], serde_json::json!(30));
        assert_eq!(merged["lookback"], serde_json::json!(20));
        assert_eq!(merged["ibs_threshold"], serde_json::json!(0.2));
    }

    #[test]
    fn test_merge_unknown_profile() {
        let registry = parse_profiles_toml("[equities]\ndelta = 0.30\n");
        let caller: HashMap<String, serde_json::Value> =
            vec![("dte".to_string(), serde_json::json!(45))]
                .into_iter()
                .collect();

        let merged = merge_profile_params("unknown", &registry, None, &caller);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged["dte"], serde_json::json!(45));
    }
}
