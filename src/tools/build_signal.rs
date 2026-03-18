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
        } => {
            // `description` is accepted for backward compatibility but not persisted
            let _ = description;
            execute_create(&name, &formula, save)
        }
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

fn execute_create(name: &str, formula: &str, save: bool) -> BuildSignalResponse {
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

    let spec = SignalSpec::Formula {
        formula: formula.to_string(),
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
                "[INFO] OHLCV data is loaded from cache when signals are used in run_options_backtest".to_string(),
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
                "[INFO] OHLCV data is loaded from cache when signals are used in run_options_backtest".to_string(),
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
                "[INFO] OHLCV data is loaded from cache when signals are used in run_options_backtest".to_string(),
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
    // Search saved custom signals only — built-in signals are listed in the
    // tool description so the agent can construct them directly without searching.
    let prompt_lower = prompt.to_lowercase();
    let tokens: Vec<&str> = prompt_lower.split_whitespace().collect();

    let matching_saved: Vec<SavedSignalEntry> = storage::list_saved_signals()
        .unwrap_or_default()
        .into_iter()
        .filter(|s| {
            let name_lower = s.name.to_lowercase();
            let desc_lower = s.description.as_deref().unwrap_or_default().to_lowercase();
            let formula_lower = s.formula.as_deref().unwrap_or_default().to_lowercase();

            // Match if all tokens appear in name, description, or formula
            tokens.iter().all(|tok| {
                name_lower.contains(tok) || desc_lower.contains(tok) || formula_lower.contains(tok)
            })
        })
        .map(|s| SavedSignalEntry {
            usage: SavedSignalUsage {
                kind: "Saved".to_string(),
                name: s.name.clone(),
            },
            name: s.name,
            formula: s.formula,
            description: s.description,
        })
        .collect();

    let has_saved = !matching_saved.is_empty();

    let summary = if has_saved {
        format!(
            "Found {} saved custom signal(s) matching '{prompt}'. \
             Use {{ \"type\": \"Saved\", \"name\": \"<name>\" }} to reference them.",
            matching_saved.len(),
        )
    } else {
        format!(
            "No saved custom signals match '{prompt}'. \
             For built-in signals (RsiBelow, RsiAbove, MacdBullish, etc.), \
             construct the JSON directly — see the tool description for examples."
        )
    };

    BuildSignalResponse {
        summary,
        success: has_saved,
        signal_spec: None,
        saved_signals: matching_saved,
        formula_help: None,
        candidates: vec![],
        schema: None,
        column_defaults: None,
        combinator_examples: vec![],
        catalog: None,
        suggested_next_steps: if has_saved {
            vec![
                "Use { \"type\": \"Saved\", \"name\": \"<name>\" } as entry_signal or exit_signal in run_options_backtest or run_stock_backtest".to_string(),
            ]
        } else {
            vec![
                "Construct built-in signals directly as JSON (e.g. { \"type\": \"RsiBelow\", \"column\": \"adjclose\", \"threshold\": 30.0 })".to_string(),
                "Use action='catalog' to browse all built-in signals if unsure of the type name".to_string(),
                "Use action='create' to define a custom signal with a formula".to_string(),
            ]
        },
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
            "[THEN] Pass the signal JSON as entry_signal or exit_signal in run_options_backtest — OHLCV data is loaded from cache".to_string(),
            format!("[INFO] {total} signals across {num_categories} categories: momentum, overlap, trend, volatility, volume, price, iv, datetime, utility, cross-symbol"),
        ],
    }
}

#[allow(clippy::too_many_lines)]
fn formula_help() -> FormulaHelp {
    FormulaHelp {
        columns: vec![
            "close".to_string(),
            "open".to_string(),
            "high".to_string(),
            "low".to_string(),
            "volume".to_string(),
            "adjclose".to_string(),
            "iv".to_string(),
        ],
        lookback: "close[1] = previous close, close[5] = 5 bars ago".to_string(),
        functions: HashMap::from([
            // Basic
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
            // TA indicators
            (
                "rsi(col, period)".to_string(),
                "Relative Strength Index (Wilder smoothing, variable period)".to_string(),
            ),
            (
                "macd_hist(col)".to_string(),
                "MACD histogram (12/26/9 default)".to_string(),
            ),
            (
                "macd_signal(col)".to_string(),
                "MACD signal line (12/26/9 default)".to_string(),
            ),
            (
                "macd_line(col)".to_string(),
                "MACD line (12/26/9 default)".to_string(),
            ),
            (
                "roc(col, period)".to_string(),
                "Rate of change: (col - col[period]) / col[period] * 100".to_string(),
            ),
            (
                "bbands_upper(col, period)".to_string(),
                "Bollinger upper band: SMA + 2 * std".to_string(),
            ),
            (
                "bbands_lower(col, period)".to_string(),
                "Bollinger lower band: SMA - 2 * std".to_string(),
            ),
            (
                "bbands_mid(col, period)".to_string(),
                "Bollinger middle band (= SMA)".to_string(),
            ),
            (
                "atr(close, high, low, period)".to_string(),
                "Average True Range (multi-column)".to_string(),
            ),
            (
                "stochastic(close, high, low, period)".to_string(),
                "Stochastic %K oscillator (multi-column)".to_string(),
            ),
            (
                "keltner_upper(close, high, low, period, mult)".to_string(),
                "Upper Keltner Channel (multi-column)".to_string(),
            ),
            (
                "keltner_lower(close, high, low, period, mult)".to_string(),
                "Lower Keltner Channel (multi-column)".to_string(),
            ),
            (
                "obv(close, volume)".to_string(),
                "On-Balance Volume (multi-column)".to_string(),
            ),
            (
                "mfi(close, high, low, volume, period)".to_string(),
                "Money Flow Index (multi-column)".to_string(),
            ),
            // Derived features
            (
                "tr(close, high, low)".to_string(),
                "True Range: max(H-L, |H-prevC|, |L-prevC|)".to_string(),
            ),
            (
                "rel_volume(vol, period)".to_string(),
                "Relative volume: vol / SMA(vol, period)".to_string(),
            ),
            (
                "range_pct(close, high, low)".to_string(),
                "Position within bar range: (close-low)/(high-low)".to_string(),
            ),
            (
                "zscore(col, period)".to_string(),
                "Z-score: (col - rolling_mean) / rolling_std".to_string(),
            ),
            (
                "rank(col, period)".to_string(),
                "Percentile rank within rolling window (0-100). Use rank(iv, 252) for IV Percentile".to_string(),
            ),
            (
                "iv_rank(col, period)".to_string(),
                "Min-max rank: (current - min) / (max - min) × 100. Use iv_rank(iv, 252) for IV Rank".to_string(),
            ),
            // Trend
            (
                "aroon_up(high, low, period)".to_string(),
                "Aroon Up indicator (0-100)".to_string(),
            ),
            (
                "aroon_down(high, low, period)".to_string(),
                "Aroon Down indicator (0-100)".to_string(),
            ),
            (
                "aroon_osc(high, low, period)".to_string(),
                "Aroon Oscillator (Up - Down, range -100 to 100)".to_string(),
            ),
            (
                "supertrend(close, high, low, period, mult)".to_string(),
                "Supertrend line value".to_string(),
            ),
            // Volume
            (
                "cmf(close, high, low, volume, period)".to_string(),
                "Chaikin Money Flow (-1 to 1)".to_string(),
            ),
            // Counting
            (
                "consecutive_up(col)".to_string(),
                "Count of consecutive rises (resets on non-rise)".to_string(),
            ),
            (
                "consecutive_down(col)".to_string(),
                "Count of consecutive falls (resets on non-fall)".to_string(),
            ),
            // Control flow
            (
                "if(cond, then, else)".to_string(),
                "Conditional: when(cond).then(then).otherwise(else)".to_string(),
            ),
            // Date/time (zero-argument)
            (
                "day_of_week()".to_string(),
                "Day of week: 1=Mon..7=Sun (ISO 8601)".to_string(),
            ),
            (
                "month()".to_string(),
                "Month: 1-12".to_string(),
            ),
            (
                "day_of_month()".to_string(),
                "Day of month: 1-31".to_string(),
            ),
            (
                "hour()".to_string(),
                "Hour: 0-23 (0 for daily bars)".to_string(),
            ),
            (
                "minute()".to_string(),
                "Minute: 0-59 (0 for daily bars)".to_string(),
            ),
            (
                "week_of_year()".to_string(),
                "ISO week number: 1-53".to_string(),
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
            "rsi(close, 14) < 30 and close > bbands_lower(close, 20)".to_string(),
            "atr(close, high, low, 14) > 2.0 and stochastic(close, high, low, 14) < 20".to_string(),
            "if(atr(close, high, low, 14) > 3.0, rsi(close, 14) < 25, rsi(close, 14) < 35)"
                .to_string(),
            "macd_hist(close) > 0 and rel_volume(volume, 20) > 2.0".to_string(),
            "zscore(close, 20) < -2 and range_pct(close, high, low) < 0.2".to_string(),
            "aroon_osc(high, low, 25) > 0 and close > supertrend(close, high, low, 10, 3.0)"
                .to_string(),
            "cmf(close, high, low, volume, 20) > 0 and consecutive_up(close) >= 3".to_string(),
            "sma(close, 5)[1] > sma(close, 5)[2]".to_string(),
            "iv_rank(iv, 252) > 50".to_string(),
            "rank(iv, 252) < 10 and rsi(close, 14) < 30".to_string(),
            "day_of_week() == 1 and close > sma(close, 20)".to_string(),
            "month() >= 11 or month() <= 4".to_string(),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Use the shared cross-module lock to prevent TEST_SIGNALS_DIR races.
    use crate::signals::storage::TEST_FS_LOCK as FS_LOCK;

    #[test]
    fn search_finds_saved_custom_signals() {
        let _lock = FS_LOCK.lock().unwrap();
        let _guard = storage::TempSignalsGuard::new();

        // Save a custom signal
        let spec = SignalSpec::Formula {
            formula: "close < sma(close, 20)".to_string(),
        };
        storage::save_signal("ibs_mean_reversion_entry", &spec).unwrap();

        // Search should find it by name
        let resp = execute(Action::Search {
            prompt: "ibs_mean_reversion".to_string(),
        });
        assert!(
            resp.success,
            "search should succeed when saved signal matches"
        );
        assert!(
            !resp.saved_signals.is_empty(),
            "saved_signals should contain the matching custom signal"
        );
        assert_eq!(resp.saved_signals[0].name, "ibs_mean_reversion_entry");
        assert_eq!(resp.saved_signals[0].usage.kind, "Saved");
        assert_eq!(resp.saved_signals[0].usage.name, "ibs_mean_reversion_entry");
    }

    #[test]
    fn search_finds_saved_signal_by_name() {
        let _lock = FS_LOCK.lock().unwrap();
        let _guard = storage::TempSignalsGuard::new();

        let spec = SignalSpec::Formula {
            formula: "close > high[1]".to_string(),
        };
        storage::save_signal("my_exit", &spec).unwrap();

        // Search by saved signal name
        let resp = execute(Action::Search {
            prompt: "my_exit".to_string(),
        });
        assert!(
            !resp.saved_signals.is_empty(),
            "should find saved signal matching name"
        );
        assert_eq!(resp.saved_signals[0].name, "my_exit");
    }

    #[test]
    fn search_returns_empty_when_no_saved_match() {
        let _lock = FS_LOCK.lock().unwrap();
        let _guard = storage::TempSignalsGuard::new();

        let spec = SignalSpec::Formula {
            formula: "close > open".to_string(),
        };
        storage::save_signal("unrelated_signal", &spec).unwrap();

        let resp = execute(Action::Search {
            prompt: "RSI oversold".to_string(),
        });
        // Search only covers saved signals now — no built-in candidates
        assert!(
            resp.saved_signals.is_empty(),
            "unrelated saved signal should not appear in search results"
        );
        assert!(
            resp.candidates.is_empty(),
            "search should not return built-in candidates"
        );
        assert!(!resp.success);
    }

    #[test]
    fn search_summary_guides_to_builtins_when_no_match() {
        let _lock = FS_LOCK.lock().unwrap();
        let _guard = storage::TempSignalsGuard::new();

        let resp = execute(Action::Search {
            prompt: "rsi".to_string(),
        });
        // No saved signals match "rsi", summary should guide to built-ins
        assert!(resp.summary.contains("No saved custom signals"));
        assert!(resp.summary.contains("construct the JSON directly"));
    }

    #[test]
    fn search_summary_reflects_saved_matches() {
        let _lock = FS_LOCK.lock().unwrap();
        let _guard = storage::TempSignalsGuard::new();

        let spec = SignalSpec::Formula {
            formula: "close < sma(close, 14)".to_string(),
        };
        storage::save_signal("rsi_custom_entry", &spec).unwrap();

        let resp = execute(Action::Search {
            prompt: "rsi".to_string(),
        });
        assert!(resp.summary.contains("saved custom signal"));
        assert!(resp.success);
    }
}
