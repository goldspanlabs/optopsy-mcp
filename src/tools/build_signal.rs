//! `build_signal` tool — create, validate, save, list, delete, and search signals.

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
    /// Search the built-in signal catalog using natural language
    Search { prompt: String },
    /// Browse the full built-in signal catalog grouped by category
    Catalog,
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
        Action::Search { prompt } => execute_search(&prompt),
        Action::Catalog => execute_catalog(),
    }
}

/// Helper to build a non-search response (search-specific fields default to empty/None).
fn base_response(
    summary: String,
    success: bool,
    signal_spec: Option<SignalSpec>,
    saved_signals: Vec<SavedSignalEntry>,
    formula_help: Option<FormulaHelp>,
    suggested_next_steps: Vec<String>,
) -> BuildSignalResponse {
    BuildSignalResponse {
        summary,
        success,
        signal_spec,
        saved_signals,
        formula_help,
        candidates: vec![],
        schema: None,
        column_defaults: None,
        combinator_examples: vec![],
        catalog: None,
        suggested_next_steps,
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
        return base_response(
            format!("Formula validation failed: {e}"),
            false,
            None,
            vec![],
            Some(formula_help()),
            vec![
                "[RETRY]Fix the formula syntax and try again".to_string(),
                "[TIP]Use the formula_help field for syntax reference".to_string(),
            ],
        );
    }

    // Check for duplicate formula under a different name
    if save {
        match storage::find_duplicate_formula(formula, name) {
            Ok(Some(existing_name)) => {
                // Load the existing signal and return it instead of creating a duplicate
                let existing_spec = storage::load_signal(&existing_name).ok();
                return base_response(
                    format!(
                        "Duplicate formula detected: this formula already exists as signal '{existing_name}'. Use the existing signal instead of creating a duplicate."
                    ),
                    false,
                    existing_spec,
                    vec![],
                    None,
                    vec![
                        format!(
                            "[NEXT]Use the existing signal: {{ \"type\": \"Saved\", \"name\": \"{existing_name}\" }}"
                        ),
                        format!(
                            "[TIP]Delete '{existing_name}' first with action='delete' if you want to replace it"
                        ),
                    ],
                );
            }
            Ok(None) => {} // No duplicate — proceed
            Err(e) => {
                tracing::warn!("Failed to check for duplicate formulas: {e}");
                // Non-fatal — proceed with save
            }
        }
    }

    let spec = SignalSpec::Custom {
        name: name.to_string(),
        formula: formula.to_string(),
        description: description.map(String::from),
    };

    if save {
        // Check if we're overwriting an existing signal with the same name
        let is_overwrite = storage::load_signal(name).is_ok();

        if let Err(e) = storage::save_signal(name, &spec) {
            return base_response(
                format!("Signal validated but save failed: {e}"),
                false,
                Some(spec),
                vec![],
                None,
                vec!["[RETRY]Check file permissions for ~/.optopsy/signals/".to_string()],
            );
        }

        let summary = if is_overwrite {
            format!("Custom signal '{name}' updated (overwritten). Formula: {formula}")
        } else {
            format!("Custom signal '{name}' created and saved. Formula: {formula}")
        };

        base_response(
            summary,
            true,
            Some(spec),
            vec![],
            None,
            vec![
                "[INFO] OHLCV data is auto-fetched when signals are used in run_options_backtest".to_string(),
                "[NEXT]Use this signal as entry_signal or exit_signal in run_options_backtest — you MUST also provide a strategy (e.g. short_put, iron_condor). Signals filter WHEN to trade, not WHAT to trade.".to_string(),
                format!(
                    "[TIP]Reference this signal later with: {{ \"type\": \"Saved\", \"name\": \"{name}\" }}"
                ),
            ],
        )
    } else {
        base_response(
            format!("Custom signal '{name}' created (not saved). Formula: {formula}"),
            true,
            Some(spec),
            vec![],
            None,
            vec![
                "[INFO] OHLCV data is auto-fetched when signals are used in run_options_backtest".to_string(),
                "[NEXT]Use this signal as entry_signal or exit_signal in run_options_backtest — you MUST also provide a strategy (e.g. short_put, iron_condor). Signals filter WHEN to trade, not WHAT to trade.".to_string(),
                "[TIP]Call build_signal again with save=true to persist this signal".to_string(),
            ],
        )
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
            base_response(
                format!("{count} saved signal(s) found."),
                true,
                None,
                saved,
                None,
                if count == 0 {
                    vec![
                        "[NEXT]Create a custom signal with build_signal action='create'"
                            .to_string(),
                        "[ALT]Use build_signal action='search' to find built-in signals"
                            .to_string(),
                    ]
                } else {
                    vec![
                        "[NEXT]Use a saved signal via { \"type\": \"Saved\", \"name\": \"signal_name\" } as entry_signal/exit_signal in run_options_backtest — you MUST also provide a strategy (e.g. short_put, iron_condor)".to_string(),
                        "[TIP]Delete signals you no longer need with action='delete'".to_string(),
                    ]
                },
            )
        }
        Err(e) => base_response(
            format!("Failed to list signals: {e}"),
            false,
            None,
            vec![],
            None,
            vec!["[RETRY]Check permissions for ~/.optopsy/signals/".to_string()],
        ),
    }
}

fn execute_delete(name: &str) -> BuildSignalResponse {
    match storage::delete_signal(name) {
        Ok(()) => base_response(
            format!("Signal '{name}' deleted."),
            true,
            None,
            vec![],
            None,
            vec!["[NEXT]Use build_signal action='list' to see remaining signals".to_string()],
        ),
        Err(e) => base_response(
            format!("Failed to delete signal '{name}': {e}"),
            false,
            None,
            vec![],
            None,
            vec!["[RETRY]Check that the signal name exists with action='list'".to_string()],
        ),
    }
}

fn execute_validate(formula: &str) -> BuildSignalResponse {
    match validate_formula(formula) {
        Ok(()) => base_response(
            format!("Formula is valid: {formula}"),
            true,
            None,
            vec![],
            None,
            vec![
                "[NEXT]Call build_signal with action='create' to save this signal".to_string(),
                "[THEN]Use the formula directly in a Custom signal spec for run_options_backtest"
                    .to_string(),
            ],
        ),
        Err(e) => base_response(
            format!("Formula validation failed: {e}"),
            false,
            None,
            vec![],
            Some(formula_help()),
            vec![
                "[RETRY]Fix the formula syntax and try again".to_string(),
                "[TIP]Use the formula_help field for syntax reference".to_string(),
            ],
        ),
    }
}

fn execute_get(name: &str) -> BuildSignalResponse {
    match storage::load_signal(name) {
        Ok(spec) => base_response(
            format!("Loaded saved signal '{name}'."),
            true,
            Some(spec),
            vec![],
            None,
            vec![
                "[INFO] OHLCV data is auto-fetched when signals are used in run_options_backtest".to_string(),
                "[NEXT]Use this signal_spec as entry_signal or exit_signal in run_options_backtest — you MUST also provide a strategy (e.g. short_put, iron_condor). Signals filter WHEN to trade, not WHAT to trade.".to_string(),
            ],
        ),
        Err(e) => base_response(
            format!("Failed to load signal '{name}': {e}"),
            false,
            None,
            vec![],
            None,
            vec![
                "[RETRY]Check that the signal name exists with action='list'"
                    .to_string(),
            ],
        ),
    }
}

fn execute_search(prompt: &str) -> BuildSignalResponse {
    let result = super::construct_signal::execute(prompt);
    BuildSignalResponse {
        summary: result.summary,
        success: result.had_real_matches,
        signal_spec: None,
        saved_signals: vec![],
        formula_help: None,
        candidates: result.candidates,
        schema: Some(result.schema),
        column_defaults: Some(result.column_defaults),
        combinator_examples: result.combinator_examples,
        catalog: None,
        suggested_next_steps: result.suggested_next_steps,
    }
}

fn execute_catalog() -> BuildSignalResponse {
    let catalog = super::signals::execute();
    let num_categories = catalog.categories.len();
    let total = catalog.total;
    BuildSignalResponse {
        summary: catalog.summary.clone(),
        success: true,
        signal_spec: None,
        saved_signals: vec![],
        formula_help: None,
        candidates: vec![],
        schema: None,
        column_defaults: None,
        combinator_examples: vec![],
        catalog: Some(catalog),
        suggested_next_steps: vec![
            "[NEXT] Call build_signal({ action: \"search\", prompt: \"<signal_name>\" }) to get the JSON spec for a signal".to_string(),
            "[THEN] Pass the signal JSON as entry_signal or exit_signal in run_options_backtest — OHLCV data is auto-fetched".to_string(),
            format!("[INFO] {total} signals across {num_categories} categories: momentum, trend, volatility, overlap, price, volume"),
        ],
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
