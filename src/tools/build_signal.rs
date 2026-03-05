//! `build_signal` tool — create, validate, save, list, and delete custom formula-based signals.

use serde_json::json;

use super::response_types::BuildSignalResponse;
use crate::signals::custom::validate_formula;
use crate::signals::registry::SignalSpec;
use crate::signals::storage;

/// Actions supported by the `build_signal` tool.
pub enum Action {
    /// Create (and optionally save) a custom signal from a formula
    Create {
        name: String,
        formula: String,
        description: Option<String>,
        save: bool,
    },
    /// List all saved custom signals
    List,
    /// Delete a saved signal by name
    Delete { name: String },
    /// Validate a formula without saving
    Validate { formula: String },
    /// Load a saved signal and return its spec
    Get { name: String },
}

pub fn execute(action: Action) -> BuildSignalResponse {
    match action {
        Action::Create {
            name,
            formula,
            description,
            save,
        } => execute_create(&name, &formula, description.as_deref(), save),
        Action::List => execute_list(),
        Action::Delete { name } => execute_delete(&name),
        Action::Validate { formula } => execute_validate(&formula),
        Action::Get { name } => execute_get(&name),
    }
}

fn execute_create(
    name: &str,
    formula: &str,
    description: Option<&str>,
    save: bool,
) -> BuildSignalResponse {
    // Validate the formula first
    if let Err(e) = validate_formula(formula) {
        return BuildSignalResponse {
            summary: format!("Formula validation failed: {e}"),
            success: false,
            signal_spec: None,
            saved_signals: None,
            formula_help: Some(formula_help()),
            suggested_next_steps: vec![
                "Fix the formula syntax and try again".to_string(),
                "Use the formula_help field for syntax reference".to_string(),
            ],
        };
    }

    let spec = SignalSpec::Custom {
        name: name.to_string(),
        formula: formula.to_string(),
        description: description.map(String::from),
    };

    if save {
        if let Err(e) = storage::save_signal(name, &spec) {
            return BuildSignalResponse {
                summary: format!("Signal validated but save failed: {e}"),
                success: false,
                signal_spec: Some(
                    serde_json::to_value(&spec).unwrap_or(json!({"error": "serialize failed"})),
                ),
                saved_signals: None,
                formula_help: None,
                suggested_next_steps: vec![
                    "Check file permissions for ~/.optopsy/signals/".to_string()
                ],
            };
        }
    }

    let spec_json = serde_json::to_value(&spec).unwrap_or(json!({"error": "serialize failed"}));

    let summary = if save {
        format!("Custom signal '{name}' created and saved. Formula: {formula}")
    } else {
        format!("Custom signal '{name}' created (not saved). Formula: {formula}")
    };

    BuildSignalResponse {
        summary,
        success: true,
        signal_spec: Some(spec_json),
        saved_signals: None,
        formula_help: None,
        suggested_next_steps: vec![
            format!("Use this signal as entry_signal or exit_signal in run_backtest"),
            if save {
                format!(
                    "Reference this signal later with: {{ \"type\": \"Saved\", \"name\": \"{name}\" }}"
                )
            } else {
                "Call build_signal again with save=true to persist this signal".to_string()
            },
            "Combine with other signals using And/Or combinators".to_string(),
        ],
    }
}

fn execute_list() -> BuildSignalResponse {
    match storage::list_saved_signals() {
        Ok(signals) => {
            let saved: Vec<serde_json::Value> = signals
                .iter()
                .map(|s| {
                    json!({
                        "name": s.name,
                        "formula": s.formula,
                        "description": s.description,
                        "usage": {
                            "type": "Saved",
                            "name": s.name,
                        }
                    })
                })
                .collect();

            let count = saved.len();
            BuildSignalResponse {
                summary: format!("{count} saved signal(s) found."),
                success: true,
                signal_spec: None,
                saved_signals: Some(saved),
                formula_help: None,
                suggested_next_steps: if count == 0 {
                    vec![
                        "Create a custom signal with build_signal action='create'".to_string(),
                        "Use construct_signal to find built-in signals".to_string(),
                    ]
                } else {
                    vec![
                        "Use a saved signal via { \"type\": \"Saved\", \"name\": \"signal_name\" }"
                            .to_string(),
                        "Delete signals you no longer need with action='delete'".to_string(),
                    ]
                },
            }
        }
        Err(e) => BuildSignalResponse {
            summary: format!("Failed to list signals: {e}"),
            success: false,
            signal_spec: None,
            saved_signals: None,
            formula_help: None,
            suggested_next_steps: vec!["Check permissions for ~/.optopsy/signals/".to_string()],
        },
    }
}

fn execute_delete(name: &str) -> BuildSignalResponse {
    match storage::delete_signal(name) {
        Ok(()) => BuildSignalResponse {
            summary: format!("Signal '{name}' deleted."),
            success: true,
            signal_spec: None,
            saved_signals: None,
            formula_help: None,
            suggested_next_steps: vec![
                "Use build_signal action='list' to see remaining signals".to_string()
            ],
        },
        Err(e) => BuildSignalResponse {
            summary: format!("Failed to delete signal '{name}': {e}"),
            success: false,
            signal_spec: None,
            saved_signals: None,
            formula_help: None,
            suggested_next_steps: vec![
                "Check that the signal name exists with action='list'".to_string()
            ],
        },
    }
}

fn execute_validate(formula: &str) -> BuildSignalResponse {
    match validate_formula(formula) {
        Ok(()) => BuildSignalResponse {
            summary: format!("Formula is valid: {formula}"),
            success: true,
            signal_spec: None,
            saved_signals: None,
            formula_help: None,
            suggested_next_steps: vec![
                "Call build_signal with action='create' to create and save this signal".to_string(),
                "Use the formula directly in a Custom signal spec for run_backtest".to_string(),
            ],
        },
        Err(e) => BuildSignalResponse {
            summary: format!("Formula validation failed: {e}"),
            success: false,
            signal_spec: None,
            saved_signals: None,
            formula_help: Some(formula_help()),
            suggested_next_steps: vec![
                "Fix the formula syntax and try again".to_string(),
                "Use the formula_help field for syntax reference".to_string(),
            ],
        },
    }
}

fn execute_get(name: &str) -> BuildSignalResponse {
    match storage::load_signal(name) {
        Ok(spec) => {
            let spec_json =
                serde_json::to_value(&spec).unwrap_or(json!({"error": "serialize failed"}));
            BuildSignalResponse {
                summary: format!("Loaded saved signal '{name}'."),
                success: true,
                signal_spec: Some(spec_json),
                saved_signals: None,
                formula_help: None,
                suggested_next_steps: vec![
                    "Use this signal_spec directly as entry_signal or exit_signal in run_backtest"
                        .to_string(),
                    "Combine with other signals using And/Or combinators".to_string(),
                ],
            }
        }
        Err(e) => BuildSignalResponse {
            summary: format!("Failed to load signal '{name}': {e}"),
            success: false,
            signal_spec: None,
            saved_signals: None,
            formula_help: None,
            suggested_next_steps: vec![
                "Check that the signal name exists with action='list'".to_string()
            ],
        },
    }
}

fn formula_help() -> serde_json::Value {
    json!({
        "columns": ["close", "open", "high", "low", "volume", "adjclose"],
        "lookback": "close[1] = previous close, close[5] = 5 bars ago",
        "functions": {
            "sma(col, period)": "Simple Moving Average",
            "ema(col, period)": "Exponential Moving Average",
            "std(col, period)": "Rolling Standard Deviation",
            "max(col, period)": "Rolling Maximum",
            "min(col, period)": "Rolling Minimum",
            "abs(expr)": "Absolute value",
            "change(col, period)": "col - col[period]",
            "pct_change(col, period)": "(col - col[period]) / col[period]",
        },
        "operators": ["+", "-", "*", "/"],
        "comparisons": [">", "<", ">=", "<=", "==", "!="],
        "logical": ["and", "or", "not"],
        "examples": [
            "close > sma(close, 20)",
            "close > close[1] * 1.02",
            "(close - low) / (high - low) < 0.2",
            "volume > sma(volume, 20) * 2.0",
            "close > sma(close, 50) and close > sma(close, 200)",
            "pct_change(close, 1) > 0.03 or pct_change(close, 1) < -0.03",
            "close < sma(close, 20) - 2.0 * std(close, 20)",
        ],
    })
}
