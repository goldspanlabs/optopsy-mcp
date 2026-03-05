//! `build_signal` tool — create, validate, save, list, and delete custom formula-based signals.

use std::collections::HashMap;

use super::response_types::{BuildSignalResponse, FormulaHelp, SavedSignalEntry, SavedSignalUsage};
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
            saved_signals: vec![],
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
                signal_spec: Some(spec),
                saved_signals: vec![],
                formula_help: None,
                suggested_next_steps: vec![
                    "Check file permissions for ~/.optopsy/signals/".to_string()
                ],
            };
        }
    }

    let summary = if save {
        format!("Custom signal '{name}' created and saved. Formula: {formula}")
    } else {
        format!("Custom signal '{name}' created (not saved). Formula: {formula}")
    };

    BuildSignalResponse {
        summary,
        success: true,
        signal_spec: Some(spec),
        saved_signals: vec![],
        formula_help: None,
        suggested_next_steps: vec![
            "Use this signal as entry_signal or exit_signal in run_backtest".to_string(),
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
            let saved: Vec<SavedSignalEntry> = signals
                .into_iter()
                .map(|s| SavedSignalEntry {
                    name: s.name.clone(),
                    formula: s.formula,
                    description: s.description,
                    usage: SavedSignalUsage {
                        kind: "Saved".to_string(),
                        name: s.name,
                    },
                })
                .collect();

            let count = saved.len();
            BuildSignalResponse {
                summary: format!("{count} saved signal(s) found."),
                success: true,
                signal_spec: None,
                saved_signals: saved,
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
            saved_signals: vec![],
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
            saved_signals: vec![],
            formula_help: None,
            suggested_next_steps: vec![
                "Use build_signal action='list' to see remaining signals".to_string()
            ],
        },
        Err(e) => BuildSignalResponse {
            summary: format!("Failed to delete signal '{name}': {e}"),
            success: false,
            signal_spec: None,
            saved_signals: vec![],
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
            saved_signals: vec![],
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
            saved_signals: vec![],
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
        Ok(spec) => BuildSignalResponse {
            summary: format!("Loaded saved signal '{name}'."),
            success: true,
            signal_spec: Some(spec),
            saved_signals: vec![],
            formula_help: None,
            suggested_next_steps: vec![
                "Use this signal_spec directly as entry_signal or exit_signal in run_backtest"
                    .to_string(),
                "Combine with other signals using And/Or combinators".to_string(),
            ],
        },
        Err(e) => BuildSignalResponse {
            summary: format!("Failed to load signal '{name}': {e}"),
            success: false,
            signal_spec: None,
            saved_signals: vec![],
            formula_help: None,
            suggested_next_steps: vec![
                "Check that the signal name exists with action='list'".to_string()
            ],
        },
    }
}

fn formula_help() -> FormulaHelp {
    FormulaHelp {
        columns: vec![
            "close".to_string(),
            "open".to_string(),
            "high".to_string(),
            "low".to_string(),
            "volume".to_string(),
            "adjclose".to_string(),
        ],
        lookback: "close[1] = previous close, close[5] = 5 bars ago".to_string(),
        functions: HashMap::from([
            (
                "sma(col, period)".to_string(),
                "Simple Moving Average".to_string(),
            ),
            (
                "ema(col, period)".to_string(),
                "Exponential Moving Average (true EWM with alpha=2/(period+1))".to_string(),
            ),
            (
                "std(col, period)".to_string(),
                "Rolling Standard Deviation".to_string(),
            ),
            (
                "max(col, period)".to_string(),
                "Rolling Maximum".to_string(),
            ),
            (
                "min(col, period)".to_string(),
                "Rolling Minimum".to_string(),
            ),
            ("abs(expr)".to_string(), "Absolute value".to_string()),
            (
                "change(col, period)".to_string(),
                "col - col[period]".to_string(),
            ),
            (
                "pct_change(col, period)".to_string(),
                "(col - col[period]) / col[period]".to_string(),
            ),
        ]),
        operators: vec![
            "+".to_string(),
            "-".to_string(),
            "*".to_string(),
            "/".to_string(),
        ],
        comparisons: vec![
            ">".to_string(),
            "<".to_string(),
            ">=".to_string(),
            "<=".to_string(),
            "==".to_string(),
            "!=".to_string(),
        ],
        logical: vec!["and".to_string(), "or".to_string(), "not".to_string()],
        examples: vec![
            "close > sma(close, 20)".to_string(),
            "close > close[1] * 1.02".to_string(),
            "(close - low) / (high - low) < 0.2".to_string(),
            "volume > sma(volume, 20) * 2.0".to_string(),
            "close > sma(close, 50) and close > sma(close, 200)".to_string(),
            "pct_change(close, 1) > 0.03 or pct_change(close, 1) < -0.03".to_string(),
            "close < sma(close, 20) - 2.0 * std(close, 20)".to_string(),
        ],
    }
}
