//! Signal specification enum defining all supported signal types.
//!
//! All indicator logic is expressed through the formula DSL.
//! A plain string is the primary way to pass a formula signal:
//! ```json
//! "rsi(close, 14) < 30"
//! ```
//! Tagged objects are used for structural variants (`Saved`, `And`, `Or`).

use schemars::JsonSchema;
use serde::Serialize;

/// Serializable signal specification.
///
/// A plain formula string is the primary input format:
/// `"rsi(close, 14) < 30"` deserialized as `Formula { formula: "rsi(close, 14) < 30".to_string() }`.
///
/// Tagged objects are used for `Saved`, `And`, and `Or`.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum SignalSpec {
    /// A formula expression that evaluates to a boolean series.
    /// Examples:
    /// - `"close > sma(close, 20)"` — price above 20-day SMA
    /// - `"rsi(close, 14) < 30 and close > bbands_lower(close, 20)"` — oversold + below lower band
    /// - `"iv_rank(iv, 252) > 50"` — IV rank above 50%
    Formula {
        /// Formula expression that evaluates to a boolean series
        formula: String,
    },

    /// Reference to a previously saved signal by name.
    Saved {
        /// Name of a previously saved signal
        name: String,
    },

    /// Logical AND: fire only when both left and right signals are active.
    And {
        left: Box<SignalSpec>,
        right: Box<SignalSpec>,
    },
    /// Logical OR: fire when either left or right signal is active.
    Or {
        left: Box<SignalSpec>,
        right: Box<SignalSpec>,
    },
}

// ---------------------------------------------------------------------------
// Custom Deserialize: accept a plain string as Formula shorthand
// ---------------------------------------------------------------------------

/// Internal tagged representation for object-form deserialization.
/// Accepts both `"type": "Formula"` and legacy `"type": "Custom"`.
#[derive(serde::Deserialize, JsonSchema)]
#[serde(tag = "type")]
enum SignalSpecTagged {
    Formula {
        formula: String,
    },
    /// Legacy: `{"type": "Custom", "formula": "..."}` → Formula
    Custom {
        formula: String,
    },
    Saved {
        name: String,
    },
    And {
        left: Box<SignalSpec>,
        right: Box<SignalSpec>,
    },
    Or {
        left: Box<SignalSpec>,
        right: Box<SignalSpec>,
    },
}

impl<'de> serde::Deserialize<'de> for SignalSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        let value = serde_json::Value::deserialize(deserializer)?;

        // Plain string → Formula
        if let serde_json::Value::String(formula) = &value {
            return Ok(SignalSpec::Formula {
                formula: formula.clone(),
            });
        }

        serde_json::from_value::<SignalSpecTagged>(value)
            .map(|tagged| match tagged {
                SignalSpecTagged::Formula { formula } | SignalSpecTagged::Custom { formula } => {
                    SignalSpec::Formula { formula }
                }
                SignalSpecTagged::Saved { name } => SignalSpec::Saved { name },
                SignalSpecTagged::And { left, right } => SignalSpec::And { left, right },
                SignalSpecTagged::Or { left, right } => SignalSpec::Or { left, right },
            })
            .map_err(D::Error::custom)
    }
}

// ---------------------------------------------------------------------------
// Custom JsonSchema: advertise string shorthand + tagged objects
// ---------------------------------------------------------------------------

impl JsonSchema for SignalSpec {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "SignalSpec".into()
    }

    fn json_schema(gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
        // Generate the tagged-object schema from the internal enum
        let object_schema = gen.subschema_for::<SignalSpecTagged>();

        // Combine: anyOf [string, tagged-object]
        let combined = serde_json::json!({
            "description": "Signal specification. Pass a plain formula string (e.g. \"rsi(close, 14) < 30\"), or an object with \"type\" for Saved, And, Or. Cross-symbol references (e.g. VIX / VIX3M < 0.9) are supported directly in formulas.",
            "anyOf": [
                { "type": "string", "description": "Formula string shorthand — e.g. \"rsi(close, 14) < 30\"" },
                object_schema
            ]
        });
        schemars::Schema::try_from(combined).expect("SignalSpec schema is a valid JSON Schema")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_string_shorthand() {
        let spec: SignalSpec = serde_json::from_str(r#""rsi(close, 14) < 30""#).unwrap();
        assert!(
            matches!(spec, SignalSpec::Formula { formula: ref f } if f == "rsi(close, 14) < 30")
        );
    }

    #[test]
    fn deserialize_legacy_custom_object() {
        let json = r#"{"type": "Custom", "formula": "close > 100"}"#;
        let spec: SignalSpec = serde_json::from_str(json).unwrap();
        assert!(matches!(spec, SignalSpec::Formula { formula: ref f } if f == "close > 100"));
    }

    #[test]
    fn deserialize_formula_object() {
        let json = r#"{"type": "Formula", "formula": "close > 100"}"#;
        let spec: SignalSpec = serde_json::from_str(json).unwrap();
        assert!(matches!(spec, SignalSpec::Formula { formula: ref f } if f == "close > 100"));
    }

    #[test]
    fn deserialize_saved() {
        let json = r#"{"type": "Saved", "name": "my_signal"}"#;
        let spec: SignalSpec = serde_json::from_str(json).unwrap();
        assert!(matches!(spec, SignalSpec::Saved { name } if name == "my_signal"));
    }

    #[test]
    fn deserialize_and_with_string_children() {
        let json =
            r#"{"type": "And", "left": "rsi(close, 14) < 30", "right": "close > sma(close, 50)"}"#;
        let spec: SignalSpec = serde_json::from_str(json).unwrap();
        match spec {
            SignalSpec::And { left, right } => {
                assert!(
                    matches!(*left, SignalSpec::Formula { formula: ref f } if f == "rsi(close, 14) < 30")
                );
                assert!(
                    matches!(*right, SignalSpec::Formula { formula: ref f } if f == "close > sma(close, 50)")
                );
            }
            _ => panic!("expected And variant"),
        }
    }

    #[test]
    fn serialize_roundtrip() {
        let spec = SignalSpec::Formula {
            formula: "close > 100".to_string(),
        };
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains(r#""type":"Formula""#));
        let deserialized: SignalSpec = serde_json::from_str(&json).unwrap();
        assert!(
            matches!(deserialized, SignalSpec::Formula { formula: ref f } if f == "close > 100")
        );
    }

    #[test]
    fn deserialize_or_with_string_children() {
        let json =
            r#"{"type": "Or", "left": "rsi(close, 14) < 30", "right": "close > sma(close, 50)"}"#;
        let spec: SignalSpec = serde_json::from_str(json).unwrap();
        match spec {
            SignalSpec::Or { left, right } => {
                assert!(
                    matches!(*left, SignalSpec::Formula { formula: ref f } if f == "rsi(close, 14) < 30")
                );
                assert!(
                    matches!(*right, SignalSpec::Formula { formula: ref f } if f == "close > sma(close, 50)")
                );
            }
            _ => panic!("expected Or variant"),
        }
    }

    #[test]
    fn deserialize_deeply_nested_combinators() {
        let json = r#"{
            "type": "And",
            "left": {
                "type": "Or",
                "left": "close > 100",
                "right": "close < 50"
            },
            "right": {
                "type": "And",
                "left": "rsi(close, 14) < 30",
                "right": "close > sma(close, 50)"
            }
        }"#;
        let spec: SignalSpec = serde_json::from_str(json).unwrap();
        match spec {
            SignalSpec::And { left, right } => {
                assert!(matches!(*left, SignalSpec::Or { .. }));
                assert!(matches!(*right, SignalSpec::And { .. }));
            }
            _ => panic!("expected And variant"),
        }
    }

    #[test]
    fn deserialize_invalid_type_errors() {
        let json = r#"{"type": "InvalidType", "formula": "close > 100"}"#;
        let result = serde_json::from_str::<SignalSpec>(json);
        assert!(result.is_err());
    }

    #[test]
    fn deserialize_missing_required_field_errors() {
        // And missing "right"
        let json = r#"{"type": "And", "left": "close > 100"}"#;
        let result = serde_json::from_str::<SignalSpec>(json);
        assert!(result.is_err());
    }

    #[test]
    fn deserialize_number_errors() {
        // A bare number is neither a string nor a valid object
        let result = serde_json::from_str::<SignalSpec>("42");
        assert!(result.is_err());
    }
}
