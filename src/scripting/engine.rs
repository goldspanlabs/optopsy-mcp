//! Unified scripting engine: compiles and executes Rhai backtest scripts.
//!
//! Drives the main simulation loop, calling Rhai callbacks at each bar.
//! Handles data loading, position management, and metrics calculation
//! while scripts define trading logic via `config()`, `on_bar()`, etc.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use chrono::{NaiveDate, NaiveDateTime};
use rhai::{CallFnOptions, Dynamic, Engine, Scope, AST};

use crate::engine::metrics::calculate_metrics;
use crate::engine::types::{
    BacktestResult, Commission, EquityPoint, ExpirationFilter, Side, Slippage, TradeRecord,
    TradeSelector,
};

use super::indicators::IndicatorStore;
use super::options_cache::DatePartitionedOptions;
use super::registration::build_engine;
use super::types::*;

/// Run a Rhai script backtest.
///
/// This is the main entry point. It compiles the script, extracts config,
/// loads data, pre-computes indicators, and runs the unified simulation loop.
/// Optional progress callback: receives (current_bar, total_bars).
pub type ProgressCallback = Box<dyn Fn(usize, usize) + Send + Sync>;
pub type CancelCallback = Box<dyn Fn() -> bool + Send + Sync>;

/// Pre-computed options data that can be shared across sweep iterations.
///
/// Building the `PriceTable` and `DatePartitionedOptions` from a large options
/// DataFrame (e.g. SPY with millions of rows) is expensive. When running a
/// parameter sweep, the symbol, date range, and expiration filter are identical
/// across combos — only script params (delta, DTE, etc.) change. This struct
/// allows building once and reusing across all combos.
#[derive(Clone)]
pub struct PrecomputedOptionsData {
    pub options_by_date: Arc<DatePartitionedOptions>,
    pub price_table: Arc<crate::engine::sim_types::PriceTable>,
    pub date_index: Arc<crate::engine::sim_types::DateIndex>,
}

// ---------------------------------------------------------------------------
// Script validation (compile + config check, no data loading or execution)
// ---------------------------------------------------------------------------

/// Diagnostic message from script validation.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ValidationDiagnostic {
    pub level: DiagnosticLevel,
    pub message: String,
}

/// Severity level for a validation diagnostic.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DiagnosticLevel {
    Error,
    Warning,
    Info,
}

/// Result of validating a Rhai script without executing a backtest.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ValidationResult {
    /// Whether the script passed all checks (no errors).
    pub valid: bool,
    /// Diagnostics: errors, warnings, and info messages.
    pub diagnostics: Vec<ValidationDiagnostic>,
    /// Callbacks found in the script.
    pub callbacks: Vec<String>,
    /// Config extracted from `config()` (None if compilation/init failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<ValidatedConfig>,
    /// Extern parameters declared in the script.
    pub params: Vec<super::stdlib::ExternParam>,
}

/// Subset of `ScriptConfig` safe to expose in validation response.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ValidatedConfig {
    pub symbol: String,
    /// All tradeable symbols. For single-symbol scripts this is `[symbol]`.
    pub symbols: Vec<String>,
    pub capital: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_date: Option<String>,
    pub interval: String,
    pub needs_ohlcv: bool,
    pub needs_options: bool,
    pub cross_symbols: Vec<String>,
    pub indicators: Vec<String>,
}

/// Known indicator names accepted by the scripting engine.
const KNOWN_INDICATORS: &[&str] = &[
    "sma",
    "ema",
    "rsi",
    "atr",
    "macd_line",
    "macd_signal",
    "macd_hist",
    "bbands_upper",
    "bbands_mid",
    "bbands_lower",
    "stochastic",
    "cci",
    "obv",
    "adx",
    "plus_di",
    "minus_di",
    "psar",
    "supertrend",
    "keltner_upper",
    "keltner_lower",
    "donchian_upper",
    "donchian_mid",
    "donchian_lower",
    "tr",
    "williams_r",
    "ppo",
    "cmo",
    "mfi",
    "vpt",
    "roc",
    "rank",
    "iv_rank",
    "cmf",
    "change",
    "pct_change",
    "std",
    "max",
    "min",
    "consecutive_up",
    "consecutive_down",
];

/// Validate a Rhai script without running a backtest.
///
/// Performs: syntax check (compile), top-level init, `config()` extraction,
/// callback detection, indicator name validation, and extern param extraction.
pub fn validate_script(
    script_source: &str,
    params: &HashMap<String, serde_json::Value>,
) -> ValidationResult {
    let mut diagnostics = Vec::new();
    let mut callbacks = Vec::new();
    // Extract extern params (uses its own engine instance)
    let extern_params = super::stdlib::extract_extern_params(script_source);

    // Check for //! name: header in source
    if !script_source
        .lines()
        .any(|l| l.trim_start().starts_with("//! name:"))
    {
        diagnostics.push(ValidationDiagnostic {
            level: DiagnosticLevel::Warning,
            message: "No `//! name:` header found; consider adding one for display in the UI"
                .into(),
        });
    }

    // 1. Check for missing required extern params before compilation/config
    let mut missing_required = false;
    for ep in &extern_params {
        if ep.default.is_none() && !params.contains_key(&ep.name) {
            diagnostics.push(ValidationDiagnostic {
                level: DiagnosticLevel::Error,
                message: format!("Required parameter '{}' is not provided", ep.name),
            });
            missing_required = true;
        }
    }

    if missing_required {
        return ValidationResult {
            valid: false,
            diagnostics,
            callbacks,
            config: None,
            params: extern_params,
        };
    }

    // 2. Build engine and compile
    let mut engine = build_engine();

    let params_clone = params.clone();
    engine.register_fn(
        "extern",
        move |name: &str, default: rhai::Dynamic, _desc: &str| -> rhai::Dynamic {
            if let Some(value) = params_clone.get(name) {
                super::stdlib::json_to_dynamic(value)
            } else if default.is_unit() {
                rhai::Dynamic::from(format!("ERROR: Required parameter '{name}' not provided"))
            } else {
                default
            }
        },
    );

    let params_clone4 = params.clone();
    engine.register_fn(
        "extern",
        move |name: &str,
              default: rhai::Dynamic,
              _desc: &str,
              _opts: rhai::Array|
              -> rhai::Dynamic {
            if let Some(value) = params_clone4.get(name) {
                super::stdlib::json_to_dynamic(value)
            } else if default.is_unit() {
                rhai::Dynamic::from(format!("ERROR: Required parameter '{name}' not provided"))
            } else {
                default
            }
        },
    );

    // extern_symbol behaves identically to extern at validation time
    // Uses case-insensitive lookup for backward compat (SYMBOL vs symbol)
    let params_clone_sym = params.clone();
    engine.register_fn(
        "extern_symbol",
        move |name: &str, default: rhai::Dynamic, _desc: &str| -> rhai::Dynamic {
            if let Some(value) = params_clone_sym.get(name).or_else(|| {
                params_clone_sym
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(name))
                    .map(|(_, v)| v)
            }) {
                super::stdlib::json_to_dynamic(value)
            } else if default.is_unit() {
                rhai::Dynamic::from(format!("ERROR: Required parameter '{name}' not provided"))
            } else {
                default
            }
        },
    );

    let ast = match engine.compile(script_source) {
        Ok(ast) => ast,
        Err(e) => {
            diagnostics.push(ValidationDiagnostic {
                level: DiagnosticLevel::Error,
                message: format!("Compile error: {e}"),
            });
            return ValidationResult {
                valid: false,
                diagnostics,
                callbacks,
                config: None,
                params: extern_params,
            };
        }
    };

    diagnostics.push(ValidationDiagnostic {
        level: DiagnosticLevel::Info,
        message: "Script compiled successfully".to_string(),
    });

    // 2. Detect callbacks
    let expected = [
        ("config", 0),
        ("on_bar", 1),
        ("on_exit_check", 2),
        ("on_position_opened", 2),
        ("on_position_closed", 3),
        ("on_end", 1),
    ];

    for (name, arity) in &expected {
        if has_fn(&ast, name, *arity) {
            callbacks.push((*name).to_string());
        }
    }

    if !callbacks.contains(&"config".to_string()) {
        diagnostics.push(ValidationDiagnostic {
            level: DiagnosticLevel::Error,
            message: "Missing required callback: config()".to_string(),
        });
    }
    if !callbacks.contains(&"on_bar".to_string()) {
        diagnostics.push(ValidationDiagnostic {
            level: DiagnosticLevel::Error,
            message: "Missing required callback: on_bar(ctx)".to_string(),
        });
    }
    if !callbacks.contains(&"on_exit_check".to_string()) {
        diagnostics.push(ValidationDiagnostic {
            level: DiagnosticLevel::Warning,
            message: "Missing on_exit_check(ctx, pos) — positions will only exit at expiration"
                .to_string(),
        });
    }

    // If config() is missing, we can't proceed further
    if !callbacks.contains(&"config".to_string()) {
        return ValidationResult {
            valid: false,
            diagnostics,
            callbacks,
            config: None,
            params: extern_params,
        };
    }

    // 3. Initialize scope and call config()
    // Merge defaults into params so validation works without caller-provided values.
    // Universal params (SYMBOL, CAPITAL) are always needed but not declared via extern().
    // Extern params with defaults are also merged.
    let mut merged_params = params.clone();
    merged_params
        .entry("SYMBOL".to_string())
        .or_insert_with(|| serde_json::json!("SPY"));
    merged_params
        .entry("CAPITAL".to_string())
        .or_insert_with(|| serde_json::json!(100_000.0));
    for ep in &extern_params {
        if !merged_params.contains_key(&ep.name) {
            if let Some(default) = &ep.default {
                merged_params.insert(ep.name.clone(), default.clone());
            }
        }
    }
    let mut scope = Scope::new();
    super::stdlib::inject_params_map(&mut scope, &merged_params);

    if let Err(e) = engine.eval_ast_with_scope::<Dynamic>(&mut scope, &ast) {
        diagnostics.push(ValidationDiagnostic {
            level: DiagnosticLevel::Error,
            message: format!("Initialization error: {e}"),
        });
        return ValidationResult {
            valid: false,
            diagnostics,
            callbacks,
            config: None,
            params: extern_params,
        };
    }

    let config_dynamic = match call_fn_persistent(&engine, &mut scope, &ast, "config", ()) {
        Ok(d) => d,
        Err(e) => {
            diagnostics.push(ValidationDiagnostic {
                level: DiagnosticLevel::Error,
                message: format!("config() call failed: {e}"),
            });
            return ValidationResult {
                valid: false,
                diagnostics,
                callbacks,
                config: None,
                params: extern_params,
            };
        }
    };

    let mut config = match parse_config(config_dynamic) {
        Ok(c) => c,
        Err(e) => {
            diagnostics.push(ValidationDiagnostic {
                level: DiagnosticLevel::Error,
                message: format!("Invalid config: {e}"),
            });
            return ValidationResult {
                valid: false,
                diagnostics,
                callbacks,
                config: None,
                params: extern_params,
            };
        }
    };

    // Auto-detect symbols from extern_symbol params if not specified in config
    if config.symbols.is_empty() {
        let mut symbol_values: Vec<String> = Vec::new();
        for ep in &extern_params {
            if ep.role == "symbol" {
                let value = params
                    .get(&ep.name)
                    .or_else(|| {
                        params
                            .iter()
                            .find(|(k, _)| k.eq_ignore_ascii_case(&ep.name))
                            .map(|(_, v)| v)
                    })
                    .and_then(|v| v.as_str().map(String::from))
                    .or_else(|| {
                        ep.default
                            .as_ref()
                            .and_then(|d| d.as_str().map(String::from))
                    });
                if let Some(sym) = value {
                    let sym = sym.trim().to_uppercase();
                    if !sym.is_empty() && !symbol_values.contains(&sym) {
                        symbol_values.push(sym);
                    }
                }
            }
        }
        if symbol_values.is_empty() {
            diagnostics.push(ValidationDiagnostic {
                level: DiagnosticLevel::Error,
                message:
                    "No symbols: declare extern_symbol() params or set symbol/symbols in config()"
                        .to_string(),
            });
            return ValidationResult {
                valid: false,
                diagnostics,
                callbacks,
                config: None,
                params: extern_params,
            };
        }
        config.symbol.clone_from(&symbol_values[0]);
        config.symbols = symbol_values;
    }

    diagnostics.push(ValidationDiagnostic {
        level: DiagnosticLevel::Info,
        message: "config() returned valid configuration".to_string(),
    });

    // 4. Validate declared indicators
    for decl in &config.declared_indicators {
        let name = decl.split(':').next().unwrap_or(decl).to_lowercase();
        if !KNOWN_INDICATORS.contains(&name.as_str()) {
            diagnostics.push(ValidationDiagnostic {
                level: DiagnosticLevel::Error,
                message: format!("Unknown indicator '{name}' in data.indicators"),
            });
        }
    }

    // 5. Check for common issues
    if config.needs_options && !callbacks.contains(&"on_exit_check".to_string()) {
        diagnostics.push(ValidationDiagnostic {
            level: DiagnosticLevel::Warning,
            message: "Options strategy without on_exit_check — no early exit logic".to_string(),
        });
    }

    let config_out = Some(ValidatedConfig {
        symbol: config.symbol,
        symbols: config.symbols,
        capital: config.capital,
        start_date: config.start_date.map(|d| d.to_string()),
        end_date: config.end_date.map(|d| d.to_string()),
        interval: config.interval.to_string(),
        needs_ohlcv: config.needs_ohlcv,
        needs_options: config.needs_options,
        cross_symbols: config.cross_symbols,
        indicators: config.declared_indicators,
    });

    let has_errors = diagnostics
        .iter()
        .any(|d| matches!(d.level, DiagnosticLevel::Error));

    ValidationResult {
        valid: !has_errors,
        diagnostics,
        callbacks,
        config: config_out,
        params: extern_params,
    }
}

pub async fn run_script_backtest(
    script_source: &str,
    params: &HashMap<String, serde_json::Value>,
    data_loader: &dyn DataLoader,
    progress: Option<ProgressCallback>,
    precomputed_options: Option<&PrecomputedOptionsData>,
    is_cancelled: Option<&CancelCallback>,
) -> Result<ScriptBacktestResult> {
    let backtest_start = std::time::Instant::now();

    // 1. Compile
    let mut engine = build_engine();

    // Register extern() with captured params for runtime resolution.
    // Arc-shared to avoid cloning the full HashMap for each overload.
    let params_arc = Arc::new(params.clone());

    // 3-arg overload
    let p = Arc::clone(&params_arc);
    engine.register_fn(
        "extern",
        move |name: &str, default: Dynamic, _desc: &str| -> Dynamic {
            if let Some(value) = p.get(name) {
                super::stdlib::json_to_dynamic(value)
            } else if default.is_unit() {
                Dynamic::from(format!("ERROR: Required parameter '{name}' not provided"))
            } else {
                default
            }
        },
    );

    // 4-arg overload (with options array — ignored at runtime)
    let p = Arc::clone(&params_arc);
    engine.register_fn(
        "extern",
        move |name: &str, default: Dynamic, _desc: &str, _opts: rhai::Array| -> Dynamic {
            if let Some(value) = p.get(name) {
                super::stdlib::json_to_dynamic(value)
            } else if default.is_unit() {
                Dynamic::from(format!("ERROR: Required parameter '{name}' not provided"))
            } else {
                default
            }
        },
    );

    // extern_symbol — identical to extern at runtime (role tag is only for extraction).
    // Uses case-insensitive lookup so SYMBOL (legacy) resolves for extern_symbol("symbol", ...).
    let p = Arc::clone(&params_arc);
    engine.register_fn(
        "extern_symbol",
        move |name: &str, default: Dynamic, _desc: &str| -> Dynamic {
            if let Some(value) = p.get(name).or_else(|| {
                p.iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(name))
                    .map(|(_, v)| v)
            }) {
                super::stdlib::json_to_dynamic(value)
            } else if default.is_unit() {
                Dynamic::from(format!("ERROR: Required parameter '{name}' not provided"))
            } else {
                default
            }
        },
    );

    let ast = engine
        .compile(script_source)
        .map_err(|e| anyhow::anyhow!("Script compile error: {e}"))?;

    // 2. Inject params map into scope FIRST so extern() calls during
    //    top-level initialization can resolve parameter values.
    let mut scope = Scope::new();
    super::stdlib::inject_params_map(&mut scope, params);

    // 3. Initialize scope (evaluate top-level let/const statements)
    //    This is where extern() calls execute and resolve from the params map.
    let _ = engine
        .eval_ast_with_scope::<Dynamic>(&mut scope, &ast)
        .map_err(|e| anyhow::anyhow!("Script initialization error: {e}"))?;

    // 3. Call config()
    let config_map: Dynamic = call_fn_persistent(&engine, &mut scope, &ast, "config", ())?;
    let mut config = parse_config(config_map).context("Failed to parse config() return value")?;

    // 3a. Auto-detect symbols from extern_symbol params if not specified in config()
    if config.symbols.is_empty() {
        let extern_params = super::stdlib::extract_extern_params(script_source);
        let mut symbol_values: Vec<String> = Vec::new();
        for ep in &extern_params {
            if ep.role == "symbol" {
                // Resolve value from params map (case-insensitive) or fall back to default.
                // Supports both "symbol" and "SYMBOL" param keys for backward compat.
                let value = params
                    .get(&ep.name)
                    .or_else(|| {
                        params
                            .iter()
                            .find(|(k, _)| k.eq_ignore_ascii_case(&ep.name))
                            .map(|(_, v)| v)
                    })
                    .and_then(|v| v.as_str().map(String::from))
                    .or_else(|| {
                        ep.default
                            .as_ref()
                            .and_then(|d| d.as_str().map(String::from))
                    });
                if let Some(sym) = value {
                    let sym = sym.trim().to_uppercase();
                    if !sym.is_empty() && !symbol_values.contains(&sym) {
                        symbol_values.push(sym);
                    }
                }
            }
        }
        if symbol_values.is_empty() {
            bail!(
                "No symbols specified: declare extern_symbol() params or set symbol/symbols in config()"
            );
        }
        config.symbol.clone_from(&symbol_values[0]);
        config.symbols = symbol_values;
    }

    // 3b. Override date bounds from params (used by walk-forward to set window dates)
    if let Some(s) = params.get("START_DATE").and_then(|v| v.as_str()) {
        if let Ok(d) = s.parse::<chrono::NaiveDate>() {
            config.start_date = Some(d);
        }
    }
    if let Some(s) = params.get("END_DATE").and_then(|v| v.as_str()) {
        if let Ok(d) = s.parse::<chrono::NaiveDate>() {
            config.end_date = Some(d);
        }
    }

    // 4. Load data
    let mut early_warnings: Vec<String> = Vec::new();

    // These variables are set by either the single-symbol or multi-symbol path below.
    let price_history: Arc<Vec<OhlcvBar>>;
    let indicator_store: Arc<IndicatorStore>;
    let adjustment_timeline: Arc<crate::engine::adjustments::AdjustmentTimeline>;
    let split_timeline: Arc<crate::engine::adjustments::AdjustmentTimeline>;
    let options_by_date: Option<Arc<DatePartitionedOptions>>;
    let price_table: Option<Arc<crate::engine::sim_types::PriceTable>>;
    let date_index: Option<Arc<crate::engine::sim_types::DateIndex>>;
    let per_symbol_data: Option<HashMap<String, PerSymbolData>>;
    let mut last_known = crate::engine::sim_types::LastKnown::new();

    if config.symbols.len() > 1 {
        // ----- Multi-symbol path -----

        // Coerce interval to daily if options are needed (mirrors single-symbol path)
        if config.needs_options && config.interval != Interval::Daily {
            early_warnings.push(format!(
                "Options require daily data; coercing interval from {:?} to Daily",
                config.interval
            ));
            config.interval = Interval::Daily;
        }

        let (psd, _master_dates) =
            load_multi_symbol_data(&config, data_loader, &mut early_warnings).await?;

        // Use first symbol's data as the primary loop driver.
        // All symbols share the same dates after intersection.
        let first = &config.symbols[0];
        let first_data = psd.get(first).with_context(|| {
            format!("multi-symbol data load succeeded but '{first}' missing from per_symbol_data")
        })?;
        price_history = Arc::clone(&first_data.bars);
        indicator_store = Arc::clone(&first_data.indicator_store);
        split_timeline = Arc::clone(&first_data.split_timeline);
        adjustment_timeline = Arc::clone(&first_data.adjustment_timeline);

        // Primary symbol's options data (used for single-symbol compat paths)
        options_by_date = first_data.options_by_date.as_ref().map(Arc::clone);
        price_table = first_data.price_table.as_ref().map(Arc::clone);
        date_index = first_data.date_index.as_ref().map(Arc::clone);

        per_symbol_data = Some(psd);
    } else {
        // ----- Single-symbol path (existing logic) -----
        per_symbol_data = None;

        let ohlcv_df = data_loader
            .load_ohlcv(&config.symbol, config.start_date, config.end_date)
            .await?;

        if ohlcv_df.height() == 0 {
            bail!("No OHLCV data found for symbol '{}'", config.symbol);
        }

        // 4a. Resample to daily if needed.
        let data_is_intraday = is_intraday_data(&ohlcv_df);
        let needs_daily =
            (config.interval == Interval::Daily && data_is_intraday) || config.needs_options;

        let ohlcv_df = if needs_daily && data_is_intraday {
            let original_rows = ohlcv_df.height();
            let resampled = crate::engine::ohlcv::resample_ohlcv(
                &ohlcv_df,
                crate::engine::types::Interval::Daily,
            )?;
            if config.needs_options && config.interval != Interval::Daily {
                early_warnings.push(format!(
                    "Options require daily data; resampled {} intraday ({:?}) bars to {} daily bars",
                    original_rows, config.interval, resampled.height()
                ));
                config.interval = Interval::Daily;
            } else {
                early_warnings.push(format!(
                    "Resampled {} intraday bars to {} daily bars",
                    original_rows,
                    resampled.height()
                ));
            }
            resampled
        } else {
            ohlcv_df
        };

        // 4b. Convert DataFrame → Vec<OhlcvBar> for the simulation loop
        let bars = ohlcv_bars_from_df(&ohlcv_df)?;

        // Load adjustment factors (splits + dividends)
        let splits = data_loader.load_splits(&config.symbol)?;
        let dividends = data_loader.load_dividends(&config.symbol)?;
        let closes: Vec<(NaiveDate, f64)> =
            bars.iter().map(|b| (b.datetime.date(), b.close)).collect();

        split_timeline = Arc::new(crate::engine::adjustments::AdjustmentTimeline::build(
            &splits,
            &[],
            &[],
        ));

        adjustment_timeline = Arc::new(crate::engine::adjustments::AdjustmentTimeline::build(
            &splits, &dividends, &closes,
        ));

        // Apply split-only adjustment to simulation bars
        let bars = if split_timeline.is_empty() {
            bars
        } else {
            bars.iter()
                .map(|b| {
                    let factor = split_timeline.factor_at(b.datetime.date());
                    OhlcvBar {
                        datetime: b.datetime,
                        open: b.open * factor,
                        high: b.high * factor,
                        low: b.low * factor,
                        close: b.close * factor,
                        volume: b.volume,
                    }
                })
                .collect()
        };

        // 5. Pre-compute indicators
        price_history = Arc::new(bars);
        let indicator_bars: Arc<Vec<OhlcvBar>> = if adjustment_timeline.is_empty() {
            Arc::clone(&price_history)
        } else {
            Arc::new(
                price_history
                    .iter()
                    .map(|b| {
                        let split_factor = split_timeline.factor_at(b.datetime.date());
                        let full_factor = adjustment_timeline.factor_at(b.datetime.date());
                        let div_factor = if split_factor.abs() > f64::EPSILON {
                            full_factor / split_factor
                        } else {
                            1.0
                        };
                        OhlcvBar {
                            datetime: b.datetime,
                            open: b.open * div_factor,
                            high: b.high * div_factor,
                            low: b.low * div_factor,
                            close: b.close * div_factor,
                            volume: b.volume,
                        }
                    })
                    .collect(),
            )
        };

        indicator_store = Arc::new(IndicatorStore::build(
            &config.declared_indicators,
            &indicator_bars,
        )?);

        // Load options data if needed + build PriceTable for MTM
        if config.needs_options {
            if let Some(pre) = precomputed_options {
                options_by_date = Some(Arc::clone(&pre.options_by_date));
                price_table = Some(Arc::clone(&pre.price_table));
                date_index = Some(Arc::clone(&pre.date_index));
            } else {
                let df = data_loader
                    .load_options(&config.symbol, config.start_date, config.end_date)
                    .await?;
                let (pt, _trading_days, di) = crate::engine::price_table::build_price_table(&df)?;
                price_table = Some(Arc::new(pt));
                date_index = Some(Arc::new(di));
                options_by_date = Some(Arc::new(DatePartitionedOptions::from_df(
                    &df,
                    &config.expiration_filter,
                )?));
            }
        } else {
            options_by_date = None;
            price_table = None;
            date_index = None;
        }
    }

    let config = Arc::new(config);

    // 6. Run main simulation loop
    // Load cross-symbol data (forward-filled to primary timeline)
    let cross_symbol_data = if config.cross_symbols.is_empty() {
        Arc::new(HashMap::new())
    } else {
        let mut cross_map: HashMap<String, Vec<CrossSymbolBar>> = HashMap::new();
        let primary_dates: Vec<NaiveDate> =
            price_history.iter().map(|b| b.datetime.date()).collect();

        for cross_sym in &config.cross_symbols {
            let cross_df = match data_loader
                .load_ohlcv(cross_sym, config.start_date, config.end_date)
                .await
            {
                Ok(df) => df,
                Err(e) => {
                    tracing::warn!(
                        symbol = cross_sym,
                        error = %e,
                        "Failed to load cross-symbol data — ctx.price_of() will return ()"
                    );
                    continue;
                }
            };

            match ohlcv_bars_from_df(&cross_df) {
                Ok(cross_bars) => {
                    let filled = forward_fill_cross_symbol(&primary_dates, &cross_bars);
                    cross_map.insert(cross_sym.to_uppercase(), filled);
                }
                Err(e) => {
                    tracing::warn!(
                        symbol = cross_sym,
                        error = %e,
                        "Failed to parse cross-symbol data — ctx.price_of() will return ()"
                    );
                }
            }
        }

        Arc::new(cross_map)
    };

    let has_on_exit_check = has_fn(&ast, "on_exit_check", 2);
    let has_on_position_opened = has_fn(&ast, "on_position_opened", 2);
    let has_on_position_closed = has_fn(&ast, "on_position_closed", 3);
    let has_on_end = has_fn(&ast, "on_end", 1);

    let mut positions: Vec<ScriptPosition> = Vec::new();
    let mut trade_log: Vec<TradeRecord> = Vec::new();
    let mut pnl_history: Vec<f64> = Vec::new();
    let mut equity_curve: Vec<EquityPoint> = Vec::new();
    let mut warnings: Vec<String> = early_warnings;
    let mut realized_equity = config.capital;
    let mut peak_equity = config.capital;
    let mut next_id = 1usize;
    let mut last_entry_date: Option<NaiveDate> = None;
    let mut stop_requested = false;
    let loop_start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(config.timeout_secs);
    let mut pnl_history_arc = Arc::new(Vec::<f64>::new());
    let mut pnl_dirty = false;

    let custom_series = Arc::new(Mutex::new(CustomSeriesStore {
        series: HashMap::new(),
        display_types: HashMap::new(),
        num_bars: price_history.len(),
    }));

    let ctx_factory = BarContextFactory {
        indicator_store: Arc::clone(&indicator_store),
        price_history: Arc::clone(&price_history),
        cross_symbol_data: Arc::clone(&cross_symbol_data),
        config: Arc::clone(&config),
        options_by_date: options_by_date.clone(),
        per_symbol_data: per_symbol_data.map(Arc::new),
        custom_series: Arc::clone(&custom_series),
        adjustment_timeline: Arc::clone(&adjustment_timeline),
        split_timeline: Arc::clone(&split_timeline),
    };

    // Pending order queue for next-bar execution model
    let mut pending_orders: Vec<PendingOrder> = Vec::new();
    // Max profit/loss tracking per position id
    let mut max_profit_tracker: HashMap<usize, f64> = HashMap::new();
    let mut max_loss_tracker: HashMap<usize, f64> = HashMap::new();

    for (bar_idx, bar) in price_history.iter().enumerate() {
        if stop_requested {
            break;
        }

        // Wall-clock timeout check (every 100 bars to minimize overhead)
        if bar_idx % 100 == 0 {
            if let Some(cancel_fn) = is_cancelled {
                if cancel_fn() {
                    warnings.push("Backtest cancelled by user".to_string());
                    break;
                }
            }
            if loop_start.elapsed() > timeout {
                warnings.push(format!(
                    "Backtest exceeded {}s timeout at bar {bar_idx}",
                    config.timeout_secs
                ));
                break;
            }
            if let Some(ref cb) = progress {
                cb(bar_idx, price_history.len());
            }
        }

        if pnl_dirty {
            pnl_history_arc = Arc::new(pnl_history.clone());
            pnl_dirty = false;
        }

        let today = bar.datetime.date();

        // Note: stock position quantities are NOT adjusted at split dates because
        // OHLCV bars are already split-adjusted before entering the simulation loop.
        // With split-adjusted prices there is no discontinuity, so positions remain
        // correct without runtime adjustment. The "100 shares at $100" in split-adjusted
        // terms is economically equivalent to "200 shares at $100" in unadjusted terms.

        // --- Phase A: Fill pending orders from previous bar ---
        // Orders submitted on bar N are filled on bar N+1.
        // Remove expired orders first, then attempt to fill remaining orders.
        pending_orders.retain(|order| !order.is_expired(bar_idx));

        let orders_to_process = std::mem::take(&mut pending_orders);
        // Snapshot the pending count before processing for accurate callback contexts
        let phase_a_pending_count = orders_to_process.len();
        let mut unfilled_orders: Vec<PendingOrder> = Vec::new();
        let mut auto_exit_orders: Vec<PendingOrder> = Vec::new();
        for order in orders_to_process {
            // Resolve the target symbol's bar for fill checks (multi-symbol support).
            // For close orders, derive the symbol from the referenced position so
            // auto stop-loss/take-profit orders fill against the correct market.
            let position_sym: Option<String> = match &order.action {
                ScriptAction::Close {
                    position_id: Some(pid),
                    ..
                } => positions
                    .iter()
                    .find(|p| p.id == *pid)
                    .map(|p| p.symbol.clone()),
                _ => None,
            };
            // Normalize: treat empty/whitespace-only symbols as None so the
            // fallback to config.symbol works reliably.
            let target_sym = order
                .symbol
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .or(position_sym.as_deref())
                .unwrap_or(&config.symbol);
            let fill_bar = if let Some(psd) = &ctx_factory.per_symbol_data {
                psd.get(target_sym).and_then(|d| d.bars.get(bar_idx))
            } else {
                Some(bar)
            };
            let Some(fill_bar) = fill_bar else {
                unfilled_orders.push(order);
                continue;
            };
            if let Some(fill_price) =
                order.try_fill(fill_bar.open, fill_bar.high, fill_bar.low, fill_bar.close)
            {
                match &order.action {
                    ScriptAction::OpenStock { side, qty, .. } => {
                        // Stagger check
                        if let Some(min_days) = config.min_days_between_entries {
                            if let Some(last) = last_entry_date {
                                if (today - last).num_days() < i64::from(min_days) {
                                    unfilled_orders.push(order);
                                    continue;
                                }
                            }
                        }

                        let adjusted_fill =
                            apply_stock_slippage(fill_price, *side, &config.slippage);
                        let pos = ScriptPosition {
                            id: next_id,
                            symbol: target_sym.to_string(),
                            entry_date: today,
                            inner: ScriptPositionInner::Stock {
                                side: *side,
                                qty: *qty,
                                entry_price: adjusted_fill,
                            },
                            entry_cost: adjusted_fill * *qty as f64 * side.multiplier(),
                            unrealized_pnl: 0.0,
                            days_held: 0,
                            current_date: today,
                            entry_bar_idx: bar_idx,
                            source: "script".to_string(),
                            implicit: false,
                            group: read_group(&scope),
                            trailing_stop: order.trailing_stop.clone(),
                        };
                        realized_equity -= compute_commission(&config.commission, &pos);
                        next_id += 1;
                        last_entry_date = Some(today);

                        if has_on_position_opened {
                            let positions_arc = Arc::new(positions.clone());
                            let awareness = compute_position_awareness(
                                &positions,
                                phase_a_pending_count,
                                bar_idx,
                            );
                            let ctx = ctx_factory.build(
                                bar,
                                bar_idx,
                                &positions_arc,
                                realized_equity,
                                &pnl_history_arc,
                                &awareness,
                                peak_equity,
                            );
                            let pos_dyn = Dynamic::from(pos.clone());
                            let _ = call_fn_persistent(
                                &engine,
                                &mut scope,
                                &ast,
                                "on_position_opened",
                                (ctx, pos_dyn),
                            );
                        }

                        positions.push(pos);

                        // Create per-order exit orders (stop loss / profit target)
                        // Direction-aware: long positions exit with sells, short with buys
                        // Dollar amounts are total P&L (e.g., $500 = exit when loss exceeds $500),
                        // converted to per-share offset: amt / qty
                        let pos_id = next_id - 1;
                        let is_long = *side == Side::Long;
                        let shares = *qty as f64;
                        if let Some(ref sl) = order.stop_loss {
                            let stop_price = if is_long {
                                match sl {
                                    ExitModifier::Percent(pct) => adjusted_fill * (1.0 - pct),
                                    ExitModifier::Dollar(amt) => {
                                        adjusted_fill - amt / shares.max(1.0)
                                    }
                                }
                            } else {
                                match sl {
                                    ExitModifier::Percent(pct) => adjusted_fill * (1.0 + pct),
                                    ExitModifier::Dollar(amt) => {
                                        adjusted_fill + amt / shares.max(1.0)
                                    }
                                }
                            };
                            auto_exit_orders.push(PendingOrder {
                                action: ScriptAction::Close {
                                    position_id: Some(pos_id),
                                    reason: "stop_loss".to_string(),
                                },
                                symbol: None,
                                order_type: OrderType::Stop { price: stop_price },
                                is_buy: !is_long,
                                signal: Some("__auto_stop".to_string()),
                                submitted_bar: bar_idx,
                                ttl: None,
                                stop_loss: None,
                                profit_target: None,
                                trailing_stop: None,
                            });
                        }
                        if let Some(ref pt) = order.profit_target {
                            let limit_price = if is_long {
                                match pt {
                                    ExitModifier::Percent(pct) => adjusted_fill * (1.0 + pct),
                                    ExitModifier::Dollar(amt) => {
                                        adjusted_fill + amt / shares.max(1.0)
                                    }
                                }
                            } else {
                                match pt {
                                    ExitModifier::Percent(pct) => adjusted_fill * (1.0 - pct),
                                    ExitModifier::Dollar(amt) => {
                                        adjusted_fill - amt / shares.max(1.0)
                                    }
                                }
                            };
                            auto_exit_orders.push(PendingOrder {
                                action: ScriptAction::Close {
                                    position_id: Some(pos_id),
                                    reason: "take_profit".to_string(),
                                },
                                symbol: None,
                                order_type: OrderType::Limit { price: limit_price },
                                is_buy: !is_long,
                                signal: Some("__auto_target".to_string()),
                                submitted_bar: bar_idx,
                                ttl: None,
                                stop_loss: None,
                                profit_target: None,
                                trailing_stop: None,
                            });
                        }
                    }
                    ScriptAction::Close {
                        position_id: Some(pid),
                        reason,
                    } => {
                        if let Some(idx) = positions.iter().position(|p| p.id == *pid) {
                            let closed_pos = positions[idx].clone();
                            // Apply slippage: closing a position is the opposite direction
                            // Closing is the opposite direction: long→sell, short→buy
                            let exit_side = match &closed_pos.inner {
                                ScriptPositionInner::Stock {
                                    side: Side::Long, ..
                                } => Side::Short,
                                ScriptPositionInner::Stock {
                                    side: Side::Short, ..
                                } => Side::Long,
                                _ => Side::Short, // default: sell
                            };
                            let exit_fill =
                                apply_stock_slippage(fill_price, exit_side, &config.slippage);
                            let pnl = compute_close_pnl_at_price(&closed_pos, exit_fill);
                            let exit_comm = compute_commission(&config.commission, &closed_pos);
                            realized_equity += pnl - exit_comm;

                            if has_on_position_closed {
                                let positions_arc = Arc::new(positions.clone());
                                let awareness = compute_position_awareness(
                                    &positions,
                                    unfilled_orders.len(),
                                    bar_idx,
                                );
                                let ctx = ctx_factory.build(
                                    bar,
                                    bar_idx,
                                    &positions_arc,
                                    realized_equity,
                                    &pnl_history_arc,
                                    &awareness,
                                    peak_equity,
                                );
                                let pos_dyn = Dynamic::from(closed_pos.clone());
                                let exit_dyn = Dynamic::from(reason.clone());
                                let _ = call_fn_persistent(
                                    &engine,
                                    &mut scope,
                                    &ast,
                                    "on_position_closed",
                                    (ctx, pos_dyn, exit_dyn),
                                );
                            }

                            trade_log.push(build_script_trade_record(
                                &closed_pos,
                                bar.datetime,
                                pnl,
                                reason,
                            ));
                            pnl_history.push(pnl);
                            pnl_dirty = true;
                            max_profit_tracker.remove(pid);
                            max_loss_tracker.remove(pid);
                            positions.swap_remove(idx);

                            // Cancel auto-generated exit orders for this position
                            let cancel_pid = |o: &PendingOrder| {
                                if let ScriptAction::Close {
                                    position_id: Some(opid),
                                    ..
                                } = &o.action
                                {
                                    *opid != *pid
                                } else {
                                    true
                                }
                            };
                            unfilled_orders.retain(cancel_pid);
                            auto_exit_orders.retain(cancel_pid);
                        } else {
                            // Silently skip — position already closed (e.g., sibling auto-exit)
                        }
                    }
                    ScriptAction::Close {
                        position_id: None,
                        reason,
                    } => {
                        // Close without position_id: close the first non-implicit position
                        if let Some(idx) = positions.iter().position(|p| !p.implicit) {
                            let closed_pos = positions[idx].clone();
                            let exit_side = match &closed_pos.inner {
                                ScriptPositionInner::Stock {
                                    side: Side::Long, ..
                                } => Side::Short,
                                ScriptPositionInner::Stock {
                                    side: Side::Short, ..
                                } => Side::Long,
                                _ => Side::Short,
                            };
                            let exit_fill =
                                apply_stock_slippage(fill_price, exit_side, &config.slippage);
                            let pnl = compute_close_pnl_at_price(&closed_pos, exit_fill);
                            let exit_comm = compute_commission(&config.commission, &closed_pos);
                            realized_equity += pnl - exit_comm;

                            if has_on_position_closed {
                                let positions_arc = Arc::new(positions.clone());
                                let awareness = compute_position_awareness(
                                    &positions,
                                    unfilled_orders.len(),
                                    bar_idx,
                                );
                                let ctx = ctx_factory.build(
                                    bar,
                                    bar_idx,
                                    &positions_arc,
                                    realized_equity,
                                    &pnl_history_arc,
                                    &awareness,
                                    peak_equity,
                                );
                                let pos_dyn = Dynamic::from(closed_pos.clone());
                                let exit_dyn = Dynamic::from(reason.clone());
                                let _ = call_fn_persistent(
                                    &engine,
                                    &mut scope,
                                    &ast,
                                    "on_position_closed",
                                    (ctx, pos_dyn, exit_dyn),
                                );
                            }

                            trade_log.push(build_script_trade_record(
                                &closed_pos,
                                bar.datetime,
                                pnl,
                                reason,
                            ));
                            pnl_history.push(pnl);
                            pnl_dirty = true;
                            max_profit_tracker.remove(&closed_pos.id);
                            max_loss_tracker.remove(&closed_pos.id);

                            // Cancel auto-generated exit orders for this position
                            let cid = closed_pos.id;
                            unfilled_orders.retain(|o| {
                                if let ScriptAction::Close {
                                    position_id: Some(opid),
                                    ..
                                } = &o.action
                                {
                                    *opid != cid
                                } else {
                                    true
                                }
                            });

                            positions.swap_remove(idx);
                        } else {
                            warnings.push(
                                "Pending close order (no position_id): no open positions to close"
                                    .to_string(),
                            );
                        }
                    }
                    ScriptAction::OpenOptions { legs, qty, .. } => {
                        // In multi-symbol mode, use the target symbol's options chain
                        let target_obd = if let Some(psd) = &ctx_factory.per_symbol_data {
                            psd.get(target_sym)
                                .and_then(|d| d.options_by_date.as_ref().map(Arc::clone))
                        } else {
                            options_by_date.as_ref().map(Arc::clone)
                        };
                        let resolved = resolve_option_legs(legs, &target_obd, today, &config);
                        if resolved.is_empty() {
                            warnings.push(format!(
                                "OpenOptions pending order skipped on {today}: no option contracts \
                                 could be resolved (check data.options coverage for this date)"
                            ));
                            continue;
                        }
                        let effective_qty = qty.unwrap_or(1);
                        let (entry_cost, script_legs, expiration) =
                            compute_options_entry(&resolved, &config, effective_qty);
                        let pos = ScriptPosition {
                            id: next_id,
                            symbol: target_sym.to_string(),
                            entry_date: today,
                            inner: ScriptPositionInner::Options {
                                legs: script_legs,
                                expiration,
                                secondary_expiration: None,
                                multiplier: config.multiplier,
                            },
                            entry_cost: entry_cost
                                * effective_qty as f64
                                * config.multiplier as f64,
                            unrealized_pnl: 0.0,
                            days_held: 0,
                            current_date: today,
                            entry_bar_idx: bar_idx,
                            source: "script".to_string(),
                            implicit: false,
                            group: read_group(&scope),
                            trailing_stop: None,
                        };

                        // Per-order exit modifiers are not yet supported for options entries
                        if order.stop_loss.is_some()
                            || order.profit_target.is_some()
                            || order.trailing_stop.is_some()
                        {
                            warnings.push(
                                "Per-order stop_loss/profit_target/trailing_stop modifiers \
                                 are not supported for options entries and will be ignored"
                                    .to_string(),
                            );
                        }

                        realized_equity -= compute_commission(&config.commission, &pos);
                        next_id += 1;
                        last_entry_date = Some(today);

                        if has_on_position_opened {
                            let positions_arc = Arc::new(positions.clone());
                            let awareness = compute_position_awareness(
                                &positions,
                                phase_a_pending_count,
                                bar_idx,
                            );
                            let ctx = ctx_factory.build(
                                bar,
                                bar_idx,
                                &positions_arc,
                                realized_equity,
                                &pnl_history_arc,
                                &awareness,
                                peak_equity,
                            );
                            let pos_dyn = Dynamic::from(pos.clone());
                            let _ = call_fn_persistent(
                                &engine,
                                &mut scope,
                                &ast,
                                "on_position_opened",
                                (ctx, pos_dyn),
                            );
                        }

                        positions.push(pos);
                    }
                    _ => {} // Hold, Stop, CancelOrders handled elsewhere
                }
            } else {
                unfilled_orders.push(order);
            }
        }
        pending_orders = unfilled_orders;
        pending_orders.extend(auto_exit_orders);

        // --- Phase B: Exits (immediate processing) ---

        // Compute position awareness for this bar
        let mut awareness = compute_position_awareness(&positions, pending_orders.len(), bar_idx);
        // Apply tracked max profit/loss only for the chosen awareness position
        if let Some(pid) = awareness.chosen_position_id {
            if let Some(&mp) = max_profit_tracker.get(&pid) {
                awareness.max_profit = mp;
            }
            if let Some(&ml) = max_loss_tracker.get(&pid) {
                awareness.max_loss = ml;
            }
        }

        // --- Phase B: Exits (immediate processing) ---

        // Build an Arc snapshot of positions for exit checks.
        // This is cheap (one Arc::new) and shared across all on_exit_check calls
        // until positions are actually modified (close/assignment).
        let mut positions_arc = Arc::new(positions.clone());
        let mut positions_dirty = false; // track if positions_arc needs rebuild

        // Check built-in exits + script exit checks
        let mut i = 0;
        while i < positions.len() {
            let pos = &positions[i];
            let mut should_close = false;
            let mut exit_reason = String::new();

            // Built-in: option expiration with ITM detection
            if let ScriptPositionInner::Options {
                expiration, legs, ..
            } = &pos.inner
            {
                if today >= *expiration {
                    should_close = true;
                    // Determine if any leg is ITM to classify as assignment/called_away
                    let exp_close = if let Some(psd) = &ctx_factory.per_symbol_data {
                        psd.get(&pos.symbol)
                            .and_then(|d| d.bars.get(bar_idx).map(|b| b.close))
                            .unwrap_or(bar.close)
                    } else {
                        bar.close
                    };
                    exit_reason = classify_expiration(legs, exp_close);
                }
            }

            // Per-order trailing stop check
            if !should_close && pos.days_held > 0 {
                if let Some(ref ts) = pos.trailing_stop {
                    if let Some(&max_p) = max_profit_tracker.get(&pos.id) {
                        if max_p > 0.0 {
                            let drawdown = max_p - pos.unrealized_pnl;
                            let triggered = match ts {
                                ExitModifier::Percent(pct) => {
                                    let entry_cost = pos.entry_cost.abs();
                                    entry_cost > 0.0 && drawdown / entry_cost >= *pct
                                }
                                ExitModifier::Dollar(amt) => drawdown >= *amt,
                            };
                            if triggered {
                                should_close = true;
                                exit_reason = "trailing_stop".to_string();
                            }
                        }
                    }
                }
            }

            // Script exit check (only for positions NOT opened this bar)
            if !should_close && has_on_exit_check && pos.days_held > 0 {
                // Rebuild Arc snapshot only if positions were modified since last snapshot
                if positions_dirty {
                    positions_arc = Arc::new(positions.clone());
                    positions_dirty = false;
                }
                let ctx = ctx_factory.build(
                    bar,
                    bar_idx,
                    &positions_arc,
                    realized_equity,
                    &pnl_history_arc,
                    &awareness,
                    peak_equity,
                );
                let pos_dyn = Dynamic::from(positions[i].clone());

                match call_fn_persistent(&engine, &mut scope, &ast, "on_exit_check", (ctx, pos_dyn))
                {
                    Ok(result) => {
                        if let Some(action) = parse_exit_action(&result) {
                            match action {
                                ScriptAction::Close { reason, .. } => {
                                    should_close = true;
                                    exit_reason = reason;
                                }
                                ScriptAction::Stop { reason } => {
                                    stop_requested = true;
                                    warnings.push(format!("Script requested stop: {reason}"));
                                    break;
                                }
                                _ => {} // Hold or other
                            }
                        }
                    }
                    Err(e) => {
                        warnings.push(format!("on_exit_check error on bar {bar_idx}: {e}"));
                    }
                }
            }

            if should_close {
                // Clone before removal to reference position data after it's removed
                let closed_pos = positions[i].clone();

                // Close position immediately (deduct exit commission)
                // In multi-symbol mode, use position's symbol's close for stock P&L
                let pnl = if closed_pos.is_stock() {
                    if let Some(psd) = &ctx_factory.per_symbol_data {
                        let sym_close = psd
                            .get(&closed_pos.symbol)
                            .and_then(|d| d.bars.get(bar_idx).map(|b| b.close))
                            .unwrap_or(bar.close);
                        compute_close_pnl_at_price(&closed_pos, sym_close)
                    } else {
                        compute_close_pnl(&closed_pos, bar)
                    }
                } else {
                    compute_close_pnl(&closed_pos, bar)
                };
                let exit_comm = compute_commission(&config.commission, &closed_pos);
                realized_equity += pnl - exit_comm;

                // Fire on_position_closed synchronously
                if has_on_position_closed {
                    // Rebuild Arc snapshot for the callback (positions changed)
                    positions_arc = Arc::new(positions.clone());
                    let ctx = ctx_factory.build(
                        bar,
                        bar_idx,
                        &positions_arc,
                        realized_equity,
                        &pnl_history_arc,
                        &awareness,
                        peak_equity,
                    );
                    let pos_dyn = Dynamic::from(closed_pos.clone());
                    let exit_type_dyn = Dynamic::from(exit_reason.clone());
                    let _ = call_fn_persistent(
                        &engine,
                        &mut scope,
                        &ast,
                        "on_position_closed",
                        (ctx, pos_dyn, exit_type_dyn),
                    );
                }

                trade_log.push(build_script_trade_record(
                    &closed_pos,
                    bar.datetime,
                    pnl,
                    &exit_reason,
                ));
                pnl_history.push(pnl);
                pnl_dirty = true;

                max_profit_tracker.remove(&closed_pos.id);
                max_loss_tracker.remove(&closed_pos.id);
                positions.swap_remove(i);
                positions_dirty = true; // positions changed, Arc needs rebuild

                // Cancel auto-generated stop/target orders for this position
                let closed_id = closed_pos.id;
                pending_orders.retain(|o| {
                    if let ScriptAction::Close {
                        position_id: Some(pid),
                        ..
                    } = &o.action
                    {
                        *pid != closed_id
                    } else {
                        true
                    }
                });

                // Handle implicit stock transitions for wheel-like strategies.
                // "assignment": short put expired ITM → open an implicit long stock at the strike.
                // "called_away": short call expired ITM → close any implicit long stock at the strike.
                match exit_reason.as_str() {
                    "assignment" => {
                        if let ScriptPositionInner::Options {
                            legs, multiplier, ..
                        } = &closed_pos.inner
                        {
                            for leg in legs {
                                if leg.side == Side::Short
                                    && leg.option_type == crate::engine::types::OptionType::Put
                                {
                                    // Use saturating_mul to avoid silent i32 overflow for
                                    // unusually large position sizes (e.g. qty=1, multiplier=100
                                    // is the typical case — always safely within i32 range).
                                    let shares = leg.qty.saturating_mul(*multiplier);
                                    let implicit = ScriptPosition {
                                        id: next_id,
                                        symbol: closed_pos.symbol.clone(),
                                        entry_date: today,
                                        inner: ScriptPositionInner::Stock {
                                            side: Side::Long,
                                            qty: shares,
                                            entry_price: leg.strike,
                                        },
                                        // Cost basis is strike × shares. The put premium already
                                        // received offsets this in realized_equity; we intentionally
                                        // do not re-deduct it here to avoid double-counting.
                                        entry_cost: leg.strike * f64::from(shares),
                                        unrealized_pnl: {
                                            let assign_close =
                                                if let Some(psd) = &ctx_factory.per_symbol_data {
                                                    psd.get(&closed_pos.symbol)
                                                        .and_then(|d| {
                                                            d.bars.get(bar_idx).map(|b| b.close)
                                                        })
                                                        .unwrap_or(bar.close)
                                                } else {
                                                    bar.close
                                                };
                                            (assign_close - leg.strike) * f64::from(shares)
                                        },
                                        days_held: 0,
                                        current_date: today,
                                        entry_bar_idx: bar_idx,
                                        source: "assignment".to_string(),
                                        implicit: true,
                                        group: closed_pos.group.clone(),
                                        trailing_stop: None,
                                    };
                                    next_id += 1;
                                    positions.push(implicit);
                                }
                            }
                        }
                    }
                    "called_away" => {
                        if let ScriptPositionInner::Options { legs, .. } = &closed_pos.inner {
                            for leg in legs {
                                if leg.side == Side::Short
                                    && leg.option_type == crate::engine::types::OptionType::Call
                                {
                                    let call_strike = leg.strike;
                                    // Close all implicit long stock positions (source="assignment").
                                    // In a typical single-contract wheel each short call corresponds
                                    // to exactly one prior assignment, so this is best-effort
                                    // matching by source tag rather than by exact quantity.
                                    let mut j = 0;
                                    while j < positions.len() {
                                        let is_target = positions[j].implicit
                                            && positions[j].source == "assignment"
                                            && positions[j].symbol == closed_pos.symbol
                                            && matches!(
                                                &positions[j].inner,
                                                ScriptPositionInner::Stock {
                                                    side: Side::Long,
                                                    ..
                                                }
                                            );
                                        if is_target {
                                            let stock_pnl = compute_stock_pnl_at_price(
                                                &positions[j],
                                                call_strike,
                                            );
                                            let stock_exit_comm = compute_commission(
                                                &config.commission,
                                                &positions[j],
                                            );
                                            realized_equity += stock_pnl - stock_exit_comm;
                                            trade_log.push(build_script_trade_record(
                                                &positions[j],
                                                bar.datetime,
                                                stock_pnl,
                                                "called_away",
                                            ));
                                            pnl_history.push(stock_pnl);
                                            pnl_dirty = true;
                                            positions.swap_remove(j);
                                            // Don't increment j
                                        } else {
                                            j += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
                // Don't increment i — the next position is now at index i
            } else {
                i += 1;
            }
        }

        if stop_requested {
            break;
        }

        // --- Phase C: Entries (queue orders for next-bar execution) ---
        //
        // Actions from on_bar() are queued as pending orders. They will be
        // filled on the next bar (bar N+1). This eliminates look-ahead bias.

        // Update awareness after exits
        awareness = compute_position_awareness(&positions, pending_orders.len(), bar_idx);
        if let Some(chosen_id) = awareness.chosen_position_id {
            if let Some(&mp) = max_profit_tracker.get(&chosen_id) {
                awareness.max_profit = mp;
            }
            if let Some(&ml) = max_loss_tracker.get(&chosen_id) {
                awareness.max_loss = ml;
            }
        }

        let phase_c_positions = Arc::new(positions.clone());
        let ctx = ctx_factory.build(
            bar,
            bar_idx,
            &phase_c_positions,
            realized_equity,
            &pnl_history_arc,
            &awareness,
            peak_equity,
        );

        // Call on_bar(ctx) — actions are queued, not immediately executed
        match call_fn_persistent(&engine, &mut scope, &ast, "on_bar", (ctx,)) {
            Ok(result) => {
                let parsed = parse_bar_actions(&result);
                let is_last_bar = bar_idx == price_history.len() - 1;
                for pa in parsed {
                    match &pa.action {
                        ScriptAction::Stop { reason } => {
                            stop_requested = true;
                            warnings.push(format!("Script requested stop: {reason}"));
                            break;
                        }
                        ScriptAction::Hold => {}
                        ScriptAction::CancelOrders { signal } => {
                            if let Some(sig) = signal {
                                pending_orders
                                    .retain(|o| o.signal.as_deref() != Some(sig.as_str()));
                            } else {
                                pending_orders.clear();
                            }
                        }
                        _ => {
                            // Don't queue orders on the final bar — there's no N+1 to fill them
                            if is_last_bar {
                                continue;
                            }

                            pending_orders.push(PendingOrder {
                                action: pa.action,
                                symbol: pa.symbol,
                                order_type: pa.order_type,
                                is_buy: pa.is_buy,
                                signal: pa.signal,
                                submitted_bar: bar_idx,
                                ttl: pa.ttl,
                                stop_loss: pa.stop_loss,
                                profit_target: pa.profit_target,
                                trailing_stop: pa.trailing_stop,
                            });
                        }
                    }
                }
            }
            Err(e) => {
                warnings.push(format!("on_bar error on bar {bar_idx}: {e}"));
            }
        }

        // --- Phase D: Bookkeeping ---

        // Update days_held and current_date for all open positions
        for pos in &mut positions {
            pos.days_held = (today - pos.entry_date).num_days();
            pos.current_date = today;
        }

        // Update last_known prices for data-gap fill pricing (options only)
        if config.needs_options {
            if let Some(psd) = &ctx_factory.per_symbol_data {
                for (_sym, data) in psd.iter() {
                    if let (Some(pt), Some(di)) = (&data.price_table, &data.date_index) {
                        let mut lk = data.last_known.lock().unwrap_or_else(|e| e.into_inner());
                        crate::engine::positions::update_last_known(pt, di, today, &mut lk);
                    }
                }
            }
        }
        if let (Some(pt), Some(di)) = (&price_table, &date_index) {
            crate::engine::positions::update_last_known(pt, di, today, &mut last_known);
        }

        // Mark-to-market all open positions
        let mut unrealized = 0.0;
        for pos in &mut positions {
            match &mut pos.inner {
                ScriptPositionInner::Stock {
                    side,
                    qty,
                    entry_price,
                } => {
                    // In multi-symbol mode, use the position's symbol's close price
                    let close_price = if let Some(psd) = &ctx_factory.per_symbol_data {
                        psd.get(&pos.symbol)
                            .and_then(|d| d.bars.get(bar_idx).map(|b| b.close))
                            .unwrap_or(*entry_price)
                    } else {
                        bar.close
                    };
                    let pnl = (close_price - *entry_price) * *qty as f64 * side.multiplier();
                    pos.unrealized_pnl = pnl;
                    unrealized += pnl;
                }
                ScriptPositionInner::Options {
                    legs, multiplier, ..
                } => {
                    // MTM each leg using PriceTable / last_known.
                    // Lock once per position (not per leg) to reduce mutex overhead.
                    let sym_lk_guard = ctx_factory
                        .per_symbol_data
                        .as_ref()
                        .and_then(|psd| psd.get(&pos.symbol))
                        .map(|d| d.last_known.lock().unwrap_or_else(|e| e.into_inner()));

                    let mut pos_pnl = 0.0;
                    for leg in legs.iter_mut() {
                        let current = if let Some(psd) = &ctx_factory.per_symbol_data {
                            if let Some(data) = psd.get(&pos.symbol) {
                                let lk_ref = sym_lk_guard.as_deref().unwrap();
                                lookup_option_price(
                                    &data.price_table,
                                    lk_ref,
                                    today,
                                    leg.expiration,
                                    leg.strike,
                                    leg.option_type,
                                    leg.side,
                                    &config.slippage,
                                )
                            } else {
                                None
                            }
                        } else {
                            lookup_option_price(
                                &price_table,
                                &last_known,
                                today,
                                leg.expiration,
                                leg.strike,
                                leg.option_type,
                                leg.side,
                                &config.slippage,
                            )
                        };
                        if let Some(price) = current {
                            leg.current_price = price;
                            let leg_pnl = (price - leg.entry_price)
                                * leg.side.multiplier()
                                * leg.qty as f64
                                * *multiplier as f64;
                            pos_pnl += leg_pnl;
                        }
                    }
                    pos.unrealized_pnl = pos_pnl;
                    unrealized += pos_pnl;
                }
            }

            // Track max profit and max loss for position awareness
            let current_pnl = pos.unrealized_pnl;
            let mp = max_profit_tracker.entry(pos.id).or_insert(0.0);
            if current_pnl > *mp {
                *mp = current_pnl;
            }
            let ml = max_loss_tracker.entry(pos.id).or_insert(0.0);
            if current_pnl < *ml {
                *ml = current_pnl;
            }
        }

        let current_equity = realized_equity + unrealized;
        peak_equity = peak_equity.max(current_equity);
        equity_curve.push(EquityPoint {
            datetime: bar.datetime,
            equity: current_equity,
            unrealized: Some(unrealized),
        });
    }

    // 7. End-of-simulation
    if config.auto_close_on_end {
        // Auto-close remaining positions
        if let Some(last_bar) = price_history.last() {
            let last_bar_idx = price_history.len().saturating_sub(1);
            for pos in &positions {
                // In multi-symbol mode, use position's symbol's last bar close
                let pnl = if pos.is_stock() {
                    if let Some(psd) = &ctx_factory.per_symbol_data {
                        let sym_close = psd
                            .get(&pos.symbol)
                            .and_then(|d| d.bars.get(last_bar_idx).map(|b| b.close))
                            .unwrap_or(last_bar.close);
                        compute_close_pnl_at_price(pos, sym_close)
                    } else {
                        compute_close_pnl(pos, last_bar)
                    }
                } else {
                    compute_close_pnl(pos, last_bar)
                };
                trade_log.push(build_script_trade_record(
                    pos,
                    last_bar.datetime,
                    pnl,
                    "end_of_data",
                ));
                pnl_history.push(pnl);
            }
        }
    }

    // Call on_end(ctx) — may return metadata
    let metadata = if has_on_end {
        let end_positions_arc = Arc::new(positions.clone());
        let pnl_history_arc = Arc::new(pnl_history.clone());
        let ctx = if let Some(last_bar) = price_history.last() {
            let end_awareness = compute_position_awareness(
                &positions,
                pending_orders.len(),
                price_history.len().saturating_sub(1),
            );
            ctx_factory.build(
                last_bar,
                price_history.len() - 1,
                &end_positions_arc,
                realized_equity,
                &pnl_history_arc,
                &end_awareness,
                peak_equity,
            )
        } else {
            // Empty bars — shouldn't reach here due to early bail
            unreachable!()
        };
        call_fn_persistent(&engine, &mut scope, &ast, "on_end", (ctx,))
            .ok()
            .and_then(|r| r.try_cast::<rhai::Map>())
    } else {
        None
    };

    // 10. Calculate metrics
    let metrics = if !trade_log.is_empty() {
        calculate_metrics(
            &equity_curve,
            &trade_log,
            config.capital,
            config.interval.bars_per_year(),
        )?
    } else {
        // No trades — return zeroed metrics
        calculate_metrics(
            &[EquityPoint {
                datetime: chrono::NaiveDateTime::default(),
                equity: config.capital,
                unrealized: None,
            }],
            &[],
            config.capital,
            config.interval.bars_per_year(),
        )?
    };

    Ok(ScriptBacktestResult {
        result: BacktestResult {
            symbol: Some(config.symbol.clone()),
            trade_count: trade_log.len(),
            total_pnl: trade_log.iter().map(|t| t.pnl).sum(),
            metrics,
            equity_curve,
            trade_log,
            quality: Default::default(),
            warnings,
        },
        metadata,
        execution_time_ms: backtest_start.elapsed().as_millis() as u64,
        indicator_data: indicator_store.to_series_map(),
        custom_series: {
            // Drop the factory so its Arc reference is released
            drop(ctx_factory);
            match Arc::try_unwrap(custom_series) {
                Ok(mutex) => mutex.into_inner().unwrap_or_else(|e| e.into_inner()),
                Err(arc) => {
                    let store = arc.lock().unwrap_or_else(|e| e.into_inner());
                    CustomSeriesStore {
                        series: store.series.clone(),
                        display_types: store.display_types.clone(),
                        num_bars: store.num_bars,
                    }
                }
            }
        },
        precomputed_options: if let (Some(obd), Some(pt), Some(di)) =
            (options_by_date, price_table, date_index)
        {
            Some(PrecomputedOptionsData {
                options_by_date: obd,
                price_table: pt,
                date_index: di,
            })
        } else {
            None
        },
    })
}

/// Call a Rhai function with persistent scope (rewind_scope = false).
/// Automatically rewinds scope after the call to prevent pollution.
fn call_fn_persistent<A: rhai::FuncArgs>(
    engine: &Engine,
    scope: &mut Scope,
    ast: &AST,
    fn_name: &str,
    args: A,
) -> Result<Dynamic> {
    let checkpoint = scope.len();
    let options = CallFnOptions::new().eval_ast(false).rewind_scope(false);
    let result = engine
        .call_fn_with_options(options, scope, ast, fn_name, args)
        .map_err(|e| anyhow::anyhow!("Error calling {fn_name}(): {e}"))?;
    scope.rewind(checkpoint);
    Ok(result)
}

/// Read the `_group` scope variable if it exists and is a non-empty string.
fn read_group(scope: &Scope) -> Option<String> {
    scope
        .get_value::<rhai::ImmutableString>("_group")
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            // Also try Dynamic in case it's set as a regular variable
            scope
                .get_value::<Dynamic>("_group")
                .and_then(|d| d.into_immutable_string().ok())
                .map(|s| s.to_string())
                .filter(|s| !s.is_empty())
        })
}

/// Check if a function exists in the AST.
fn has_fn(ast: &AST, name: &str, arity: usize) -> bool {
    ast.iter_functions()
        .any(|f| f.name == name && f.params.len() == arity)
}

// ---------------------------------------------------------------------------
// Config parsing
// ---------------------------------------------------------------------------

/// Parse the Rhai Map returned by `config()` into `ScriptConfig`.
fn parse_config(map: Dynamic) -> Result<ScriptConfig> {
    let map = map
        .try_cast::<rhai::Map>()
        .ok_or_else(|| anyhow::anyhow!("config() must return a Map (#{{}}))"))?;

    // Parse symbols: support `symbols: [...]`, `symbol: "..."`, or auto-detect
    // from extern_symbol params (populated after parse_config returns).
    // If both symbol and symbols are present, `symbols` takes precedence.
    let symbols: Vec<String> = if let Some(arr_val) = map.get("symbols") {
        let arr = arr_val
            .clone()
            .try_cast::<rhai::Array>()
            .ok_or_else(|| anyhow::anyhow!("config().symbols must be an array"))?;
        let syms: Vec<String> = arr
            .into_iter()
            .enumerate()
            .map(|(idx, v)| {
                let s = v
                    .into_immutable_string()
                    .map_err(|_| anyhow::anyhow!("config().symbols[{idx}] must be a string"))?;
                let trimmed = s.trim().to_uppercase();
                if trimmed.is_empty() {
                    bail!("config().symbols[{idx}] must not be empty");
                }
                Ok(trimmed)
            })
            .collect::<Result<Vec<_>>>()?;
        if syms.is_empty() {
            bail!("config().symbols must contain at least one symbol");
        }
        // Reject duplicates
        {
            let mut seen = std::collections::HashSet::new();
            for s in &syms {
                if !seen.insert(s.as_str()) {
                    bail!("config().symbols contains duplicate '{s}'");
                }
            }
        }
        syms
    } else if map.contains_key("symbol") {
        // Legacy single-symbol mode
        let sym = get_string(&map, "symbol")?;
        vec![sym.to_uppercase()]
    } else {
        // No symbol in config — will be resolved from extern_symbol params after parse
        Vec::new()
    };

    // Primary symbol = first in the list (for backward compat: ctx.close, etc.)
    // Empty symbols means auto-detect from extern_symbol params (resolved by caller).
    let symbol = symbols.first().cloned().unwrap_or_default();

    let capital = get_float(&map, "capital")?;

    let interval_str = get_string_or(&map, "interval", "daily".to_string());
    let interval = Interval::parse(&interval_str)
        .ok_or_else(|| anyhow::anyhow!("Unknown interval: '{interval_str}'"))?;

    let multiplier = get_int_or(&map, "multiplier", 100) as i32;
    let timeout_secs = get_int_or(&map, "timeout_secs", 60) as u64;
    let auto_close_on_end = get_bool_or(&map, "auto_close_on_end", false);

    // Data requirements
    let (needs_ohlcv, needs_options, cross_symbols, declared_indicators) =
        parse_data_section(&map)?;

    // Engine-enforced settings
    let (slippage, commission, min_days_between, exp_filter, trade_selector) =
        parse_engine_section(&map)?;

    // Script-readable defaults
    let defaults = parse_defaults_section(&map);

    let procedural = get_bool_or(&map, "procedural", false);

    Ok(ScriptConfig {
        symbol,
        symbols,
        capital,
        start_date: get_date_opt(&map, "start_date"),
        end_date: get_date_opt(&map, "end_date"),
        interval,
        multiplier,
        timeout_secs,
        auto_close_on_end,
        needs_ohlcv,
        needs_options,
        cross_symbols,
        declared_indicators,
        slippage,
        commission,
        min_days_between_entries: min_days_between,
        expiration_filter: exp_filter,
        trade_selector,
        defaults,
        procedural,
    })
}

fn parse_data_section(map: &rhai::Map) -> Result<(bool, bool, Vec<String>, Vec<String>)> {
    let data = match map.get("data") {
        Some(d) => d.clone().try_cast::<rhai::Map>().unwrap_or_default(),
        None => return Ok((true, false, vec![], vec![])),
    };

    let needs_ohlcv = data
        .get("ohlcv")
        .and_then(|v| v.as_bool().ok())
        .unwrap_or(true);
    let needs_options = data
        .get("options")
        .and_then(|v| v.as_bool().ok())
        .unwrap_or(false);

    let cross_symbols = data
        .get("cross_symbols")
        .and_then(|v| v.clone().try_cast::<rhai::Array>())
        .map(|arr| {
            arr.into_iter()
                .filter_map(|v| v.into_immutable_string().ok().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    let indicators = data
        .get("indicators")
        .and_then(|v| v.clone().try_cast::<rhai::Array>())
        .map(|arr| {
            arr.into_iter()
                .filter_map(|v| v.into_immutable_string().ok().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    Ok((needs_ohlcv, needs_options, cross_symbols, indicators))
}

fn parse_engine_section(
    map: &rhai::Map,
) -> Result<(
    Slippage,
    Option<Commission>,
    Option<i32>,
    ExpirationFilter,
    TradeSelector,
)> {
    let engine_map = match map.get("engine") {
        Some(d) => d.clone().try_cast::<rhai::Map>().unwrap_or_default(),
        None => {
            return Ok((
                Slippage::Mid,
                None,
                None,
                ExpirationFilter::default(),
                TradeSelector::default(),
            ))
        }
    };

    // Slippage
    let slippage = match engine_map.get("slippage") {
        Some(v) if !v.is_unit() => parse_slippage(v)?,
        _ => Slippage::Mid,
    };

    // Commission
    let commission = engine_map.get("commission").and_then(|v| {
        let m = v.clone().try_cast::<rhai::Map>()?;
        Some(Commission {
            per_contract: m.get("per_contract")?.as_float().ok()?,
            base_fee: m
                .get("base_fee")
                .and_then(|v| v.as_float().ok())
                .unwrap_or(0.0),
            min_fee: m
                .get("min_fee")
                .and_then(|v| v.as_float().ok())
                .unwrap_or(0.0),
        })
    });

    let min_days = engine_map
        .get("min_days_between_entries")
        .and_then(|v| v.as_int().ok())
        .map(|v| v as i32);

    let exp_filter = engine_map
        .get("expiration_filter")
        .and_then(|v| v.clone().into_immutable_string().ok())
        .map(|s| match s.as_str() {
            "weekly" => ExpirationFilter::Weekly,
            "monthly" => ExpirationFilter::Monthly,
            _ => ExpirationFilter::Any,
        })
        .unwrap_or_default();

    let trade_selector = engine_map
        .get("trade_selector")
        .and_then(|v| v.clone().into_immutable_string().ok())
        .map(|s| match s.as_str() {
            "highest_premium" => TradeSelector::HighestPremium,
            "lowest_premium" => TradeSelector::LowestPremium,
            "first" => TradeSelector::First,
            _ => TradeSelector::Nearest,
        })
        .unwrap_or_default();

    Ok((slippage, commission, min_days, exp_filter, trade_selector))
}

fn parse_slippage(value: &Dynamic) -> Result<Slippage> {
    // String form: "mid", "spread"
    if let Ok(s) = value.clone().into_immutable_string() {
        return match s.as_str() {
            "mid" => Ok(Slippage::Mid),
            "spread" => Ok(Slippage::Spread),
            other => bail!("Unknown slippage model: '{other}'"),
        };
    }
    // Map form: #{ type: "per_leg", per_leg: 0.05 }
    if let Some(m) = value.clone().try_cast::<rhai::Map>() {
        let typ = m
            .get("type")
            .and_then(|v| v.clone().into_immutable_string().ok())
            .unwrap_or_default();
        return match typ.as_str() {
            "per_leg" => {
                let per_leg = m
                    .get("per_leg")
                    .and_then(|v| v.as_float().ok())
                    .unwrap_or(0.0);
                Ok(Slippage::PerLeg { per_leg })
            }
            "liquidity" => {
                let fill_ratio = m
                    .get("fill_ratio")
                    .and_then(|v| v.as_float().ok())
                    .unwrap_or(0.5);
                let ref_volume = m
                    .get("ref_volume")
                    .and_then(|v| v.as_int().ok())
                    .unwrap_or(1000) as u64;
                Ok(Slippage::Liquidity {
                    fill_ratio,
                    ref_volume,
                })
            }
            "bid_ask_travel" => {
                let pct = m.get("pct").and_then(|v| v.as_float().ok()).unwrap_or(0.25);
                Ok(Slippage::BidAskTravel { pct })
            }
            other => bail!("Unknown slippage type: '{other}'"),
        };
    }
    bail!("slippage must be a string or map")
}

fn parse_defaults_section(map: &rhai::Map) -> HashMap<String, ScriptValue> {
    let mut defaults = HashMap::new();
    let section = match map.get("defaults") {
        Some(d) => match d.clone().try_cast::<rhai::Map>() {
            Some(m) => m,
            None => return defaults,
        },
        None => return defaults,
    };

    for (key, value) in &section {
        let sv = if value.is_float() {
            ScriptValue::Float(value.as_float().unwrap_or(0.0))
        } else if value.is_int() {
            ScriptValue::Int(value.as_int().unwrap_or(0))
        } else if value.is_string() {
            ScriptValue::String(
                value
                    .clone()
                    .into_immutable_string()
                    .unwrap_or_default()
                    .to_string(),
            )
        } else if value.is_bool() {
            ScriptValue::Bool(value.as_bool().unwrap_or(false))
        } else if value.is_unit() {
            ScriptValue::None
        } else {
            ScriptValue::None
        };
        defaults.insert(key.to_string(), sv);
    }

    defaults
}

// ---------------------------------------------------------------------------
// Map helper functions
// ---------------------------------------------------------------------------

fn get_string(map: &rhai::Map, key: &str) -> Result<String> {
    map.get(key)
        .ok_or_else(|| anyhow::anyhow!("config() missing required field: '{key}'"))?
        .clone()
        .into_immutable_string()
        .map(|s| s.to_string())
        .map_err(|_| anyhow::anyhow!("config().{key} must be a string"))
}

fn get_string_or(map: &rhai::Map, key: &str, default: String) -> String {
    map.get(key)
        .and_then(|v| v.clone().into_immutable_string().ok())
        .map(|s| s.to_string())
        .unwrap_or(default)
}

fn get_float(map: &rhai::Map, key: &str) -> Result<f64> {
    let val = map
        .get(key)
        .ok_or_else(|| anyhow::anyhow!("config() missing required field: '{key}'"))?;
    // Handle both int and float
    val.as_float()
        .or_else(|_| val.as_int().map(|i| i as f64))
        .map_err(|_| anyhow::anyhow!("config().{key} must be a number"))
}

fn get_int_or(map: &rhai::Map, key: &str, default: i64) -> i64 {
    map.get(key)
        .and_then(|v| v.as_int().ok())
        .unwrap_or(default)
}

fn get_bool_or(map: &rhai::Map, key: &str, default: bool) -> bool {
    map.get(key)
        .and_then(|v| v.as_bool().ok())
        .unwrap_or(default)
}

fn get_date_opt(map: &rhai::Map, key: &str) -> Option<NaiveDate> {
    map.get(key)
        .and_then(|v| v.clone().into_immutable_string().ok())
        .and_then(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok())
}

// ---------------------------------------------------------------------------
// Simulation helpers
// ---------------------------------------------------------------------------

/// Holds immutable references shared across all `build()` calls in the simulation loop.
///
/// Reduces 10-argument `build_bar_context` calls to 4-argument `build()` calls.
struct BarContextFactory {
    indicator_store: Arc<IndicatorStore>,
    price_history: Arc<Vec<OhlcvBar>>,
    cross_symbol_data: Arc<HashMap<String, Vec<CrossSymbolBar>>>,
    config: Arc<ScriptConfig>,
    options_by_date: Option<Arc<DatePartitionedOptions>>,
    per_symbol_data: Option<Arc<HashMap<String, PerSymbolData>>>,
    custom_series: Arc<Mutex<CustomSeriesStore>>,
    adjustment_timeline: Arc<crate::engine::adjustments::AdjustmentTimeline>,
    split_timeline: Arc<crate::engine::adjustments::AdjustmentTimeline>,
}

/// Position awareness snapshot for the `BarContext`.
#[derive(Clone, Default)]
pub struct PositionAwareness {
    pub market_position: i64,
    pub entry_price: f64,
    pub bars_since_entry: i64,
    pub current_shares: i64,
    pub open_profit: f64,
    pub max_profit: f64,
    pub max_loss: f64,
    pub pending_orders_count: i64,
    /// The position id chosen for awareness (used to apply tracked max values).
    pub chosen_position_id: Option<usize>,
}

/// Compute position awareness from the current positions vector (stock-only for now).
pub fn compute_position_awareness(
    positions: &[ScriptPosition],
    pending_orders_count: usize,
    current_bar_idx: usize,
) -> PositionAwareness {
    // Find the first non-implicit stock position for awareness
    // Prefer non-implicit stock; fall back to implicit (e.g., assignment-created)
    let stock_pos = positions
        .iter()
        .find(|p| !p.implicit && p.is_stock())
        .or_else(|| positions.iter().find(|p| p.implicit && p.is_stock()));

    if let Some(pos) = stock_pos {
        if let ScriptPositionInner::Stock {
            side,
            qty,
            entry_price,
            ..
        } = &pos.inner
        {
            let mp = match side {
                Side::Long => 1i64,
                Side::Short => -1i64,
            };
            return PositionAwareness {
                market_position: mp,
                entry_price: *entry_price,
                bars_since_entry: current_bar_idx.saturating_sub(pos.entry_bar_idx) as i64,
                current_shares: *qty as i64,
                open_profit: pos.unrealized_pnl,
                max_profit: 0.0, // tracked externally
                max_loss: 0.0,   // tracked externally
                pending_orders_count: pending_orders_count as i64,
                chosen_position_id: Some(pos.id),
            };
        }
    }

    PositionAwareness {
        pending_orders_count: pending_orders_count as i64,
        ..Default::default()
    }
}

/// Compute portfolio-level aggregate state from current positions and equity.
fn compute_portfolio_state(
    positions: &[ScriptPosition],
    equity: f64,
    capital: f64,
    peak_equity: f64,
) -> PortfolioState {
    let mut unrealized_pnl = 0.0_f64;
    let mut total_exposure = 0.0_f64;
    let mut net_delta = 0.0_f64;
    let mut long_delta = 0.0_f64;
    let mut short_delta = 0.0_f64;
    let mut long_count: i64 = 0;
    let mut short_count: i64 = 0;
    let mut position_count: i64 = 0;
    let mut max_position_pnl = f64::NEG_INFINITY;
    let mut min_position_pnl = f64::INFINITY;

    for pos in positions {
        unrealized_pnl += pos.unrealized_pnl;
        total_exposure += pos.entry_cost.abs();
        if !pos.implicit {
            position_count += 1;
        }
        if pos.unrealized_pnl > max_position_pnl {
            max_position_pnl = pos.unrealized_pnl;
        }
        if pos.unrealized_pnl < min_position_pnl {
            min_position_pnl = pos.unrealized_pnl;
        }

        match &pos.inner {
            ScriptPositionInner::Options {
                legs, multiplier, ..
            } => {
                let mut pos_delta = 0.0;
                for leg in legs {
                    let leg_delta = leg.delta * leg.qty as f64 * *multiplier as f64;
                    pos_delta += leg_delta;
                    if leg_delta > 0.0 {
                        long_delta += leg_delta;
                    } else {
                        short_delta += leg_delta;
                    }
                }
                if pos_delta > 0.0 {
                    long_count += 1;
                } else if pos_delta < 0.0 {
                    short_count += 1;
                }
                net_delta += pos_delta;
            }
            ScriptPositionInner::Stock { side, qty, .. } => {
                let d = match side {
                    Side::Long => *qty as f64,
                    Side::Short => -(*qty as f64),
                };
                net_delta += d;
                if d > 0.0 {
                    long_delta += d;
                    long_count += 1;
                } else {
                    short_delta += d;
                    short_count += 1;
                }
            }
        }
    }

    if positions.is_empty() {
        max_position_pnl = 0.0;
        min_position_pnl = 0.0;
    }

    let cash = equity - unrealized_pnl;
    let realized_pnl = equity - capital;
    let exposure_pct = if equity.abs() > f64::EPSILON {
        total_exposure / equity.abs()
    } else {
        0.0
    };

    let effective_peak = peak_equity.max(equity);
    let drawdown = if effective_peak.abs() > f64::EPSILON {
        (equity - effective_peak) / effective_peak
    } else {
        0.0
    };

    PortfolioState {
        cash,
        equity,
        unrealized_pnl,
        realized_pnl,
        total_exposure,
        exposure_pct,
        net_delta,
        long_delta,
        short_delta,
        position_count,
        long_count,
        short_count,
        max_position_pnl,
        min_position_pnl,
        drawdown,
        peak_equity: effective_peak,
    }
}

impl BarContextFactory {
    fn build(
        &self,
        bar: &OhlcvBar,
        bar_idx: usize,
        positions_arc: &Arc<Vec<ScriptPosition>>,
        equity: f64,
        pnl_history: &Arc<Vec<f64>>,
        awareness: &PositionAwareness,
        peak_equity: f64,
    ) -> BarContext {
        let portfolio =
            compute_portfolio_state(positions_arc, equity, self.config.capital, peak_equity);
        let cash = portfolio.cash;
        BarContext {
            datetime: bar.datetime,
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            volume: bar.volume,
            bar_idx,
            cash,
            equity,
            positions: Arc::clone(positions_arc),
            indicator_store: Arc::clone(&self.indicator_store),
            price_history: Arc::clone(&self.price_history),
            cross_symbol_data: Arc::clone(&self.cross_symbol_data),
            options_by_date: self.options_by_date.clone(),
            per_symbol_data: self.per_symbol_data.clone(),
            config: Arc::clone(&self.config),
            pnl_history: Arc::clone(pnl_history),
            custom_series: Arc::clone(&self.custom_series),
            // bar.close is already split-adjusted; apply dividend-only factor
            // for the fully-adjusted close (dividend factor = full / split)
            adjusted_close: {
                let split_f = self.split_timeline.factor_at(bar.datetime.date());
                let full_f = self.adjustment_timeline.factor_at(bar.datetime.date());
                let div_f = if split_f.abs() > f64::EPSILON {
                    full_f / split_f
                } else {
                    1.0
                };
                bar.close * div_f
            },
            market_position: awareness.market_position,
            entry_price: awareness.entry_price,
            bars_since_entry: awareness.bars_since_entry,
            current_shares: awareness.current_shares,
            open_profit: awareness.open_profit,
            max_profit: awareness.max_profit,
            max_loss: awareness.max_loss,
            pending_orders_count: awareness.pending_orders_count,
            portfolio,
        }
    }
}

/// A resolved option leg — result of resolving unresolved legs via filter pipeline.
struct ResolvedLeg {
    side: Side,
    option_type: crate::engine::types::OptionType,
    strike: f64,
    expiration: NaiveDate,
    bid: f64,
    ask: f64,
    delta: f64,
}

/// Resolve unresolved option legs via the filter pipeline.
/// Returns a Vec of resolved legs. Unresolved legs are queried via find_option.
fn resolve_option_legs(
    legs: &[LegSpec],
    options_by_date: &Option<Arc<DatePartitionedOptions>>,
    today: NaiveDate,
    _config: &ScriptConfig,
) -> Vec<ResolvedLeg> {
    use crate::engine::filters;
    use polars::prelude::*;

    let today_df = match options_by_date {
        Some(opts) => match opts.get(today) {
            Some(df) => df,
            None => return vec![],
        },
        None => return vec![],
    };

    legs.iter()
        .filter_map(|leg| match leg {
            LegSpec::Resolved {
                side,
                option_type,
                strike,
                expiration,
                bid,
                ask,
            } => {
                // Look up delta from the options chain for this strike/expiration
                let opt_code = match option_type {
                    crate::engine::types::OptionType::Call => "c",
                    crate::engine::types::OptionType::Put => "p",
                };
                let delta = today_df
                    .clone()
                    .lazy()
                    .filter(
                        col("option_type")
                            .eq(lit(opt_code))
                            .and(col("strike").eq(lit(*strike)))
                            .and(col("expiration").cast(DataType::Date).eq(lit(*expiration))),
                    )
                    .select([col("delta")])
                    .collect()
                    .ok()
                    .and_then(|df| df.column("delta").ok()?.f64().ok()?.get(0))
                    .unwrap_or(0.0);
                Some(ResolvedLeg {
                    side: *side,
                    option_type: *option_type,
                    strike: *strike,
                    expiration: *expiration,
                    bid: *bid,
                    ask: *ask,
                    delta,
                })
            }
            LegSpec::Unresolved {
                side,
                option_type,
                delta,
                dte,
            } => {
                let opt_code = match option_type {
                    crate::engine::types::OptionType::Call => "c",
                    crate::engine::types::OptionType::Put => "p",
                };
                let target = crate::engine::types::TargetRange {
                    target: *delta,
                    min: (*delta - 0.10).max(0.01),
                    max: (*delta + 0.10).min(1.0),
                };
                let dte_min = (*dte - 15).max(1);
                let dte_max = *dte + 15;

                // Already date-filtered — clone the daily slice and filter by type/DTE/quotes
                let filtered = filters::filter_leg_candidates(
                    today_df.clone(),
                    opt_code,
                    dte_max,
                    dte_min,
                    0.05,
                )
                .ok()?;
                if filtered.height() == 0 {
                    return None;
                }
                let selected = filters::select_closest_delta(filtered, &target).ok()?;
                if selected.height() == 0 {
                    return None;
                }

                let get_f64 = |col: &str| -> f64 {
                    selected
                        .column(col)
                        .ok()
                        .and_then(|c| c.f64().ok())
                        .and_then(|ca| ca.get(0))
                        .unwrap_or(0.0)
                };

                let strike = get_f64("strike");
                let bid = get_f64("bid");
                let ask = get_f64("ask");
                let found_delta = get_f64("delta");
                let expiration = super::types::row_to_expiration_date(&selected, 0)?;

                Some(ResolvedLeg {
                    side: *side,
                    option_type: *option_type,
                    strike,
                    expiration,
                    bid,
                    ask,
                    delta: found_delta,
                })
            }
        })
        .collect()
}

/// Compute entry cost and build ScriptPositionLeg vec from resolved legs.
/// Returns (net_entry_cost_per_contract, legs, primary_expiration).
fn compute_options_entry(
    resolved: &[ResolvedLeg],
    config: &ScriptConfig,
    effective_qty: i32,
) -> (f64, Vec<ScriptPositionLeg>, NaiveDate) {
    use crate::engine::pricing::fill_price;

    let mut net_cost = 0.0;
    let mut legs = Vec::new();
    let mut primary_exp = NaiveDate::from_ymd_opt(2099, 1, 1).unwrap();

    for leg in resolved {
        let entry_price = fill_price(leg.bid, leg.ask, leg.side, &config.slippage);
        // Long side pays (debit), short side receives (credit)
        net_cost += entry_price * leg.side.multiplier();

        if leg.expiration < primary_exp {
            primary_exp = leg.expiration;
        }

        legs.push(ScriptPositionLeg {
            strike: leg.strike,
            option_type: leg.option_type,
            side: leg.side,
            expiration: leg.expiration,
            entry_price,
            current_price: entry_price, // starts at entry
            delta: leg.delta,
            qty: effective_qty,
        });
    }

    (net_cost, legs, primary_exp)
}

/// Compute P&L for closing a position at the current bar's prices.
///
/// For stocks, uses the current bar's close price.
/// For options, recomputes P&L from each leg's cached `current_price`
/// (updated in Phase D bookkeeping) and `entry_price`, including the contract
/// multiplier. Both `leg.current_price` and `leg.entry_price` are per-contract
/// premiums (e.g., $2.50 for a $2.50 premium option); the contract multiplier
/// (typically 100) converts them to per-position dollar P&L.
/// Note: Phase B closes happen before Phase D MTM update, so `current_price`
/// reflects the previous bar — this matches the native engine behavior where
/// exit prices are determined at the close trigger bar.
fn compute_close_pnl(pos: &ScriptPosition, bar: &OhlcvBar) -> f64 {
    match &pos.inner {
        ScriptPositionInner::Stock {
            side,
            qty,
            entry_price,
        } => (bar.close - entry_price) * *qty as f64 * side.multiplier(),
        ScriptPositionInner::Options {
            legs, multiplier, ..
        } => legs
            .iter()
            .map(|leg| {
                (leg.current_price - leg.entry_price)
                    * leg.side.multiplier()
                    * f64::from(leg.qty)
                    * f64::from(*multiplier)
            })
            .sum(),
    }
}

/// Apply slippage to a stock fill price.
///
/// For `PerLeg`, adds a fixed cost per share (adverse direction).
/// For `Mid`, no adjustment (fill at computed price).
/// Other models are options-specific and don't apply to stock fills.
fn apply_stock_slippage(fill_price: f64, side: Side, slippage: &Slippage) -> f64 {
    match slippage {
        Slippage::PerLeg { per_leg } => {
            // Buy: price worsens upward, Sell: price worsens downward
            match side {
                Side::Long => fill_price + per_leg,
                Side::Short => fill_price - per_leg,
            }
        }
        // Mid / Spread / Liquidity / BidAskTravel are options-specific (need bid/ask)
        _ => fill_price,
    }
}

/// Compute P&L for closing a position at a specific fill price.
///
/// Used by the next-bar execution model where orders fill at computed prices
/// (open, limit, stop) rather than bar close.
fn compute_close_pnl_at_price(pos: &ScriptPosition, exit_price: f64) -> f64 {
    match &pos.inner {
        ScriptPositionInner::Stock {
            side,
            qty,
            entry_price,
        } => (exit_price - entry_price) * f64::from(*qty) * side.multiplier(),
        // For options, use the current_price from MTM (same as compute_close_pnl)
        ScriptPositionInner::Options {
            legs, multiplier, ..
        } => legs
            .iter()
            .map(|leg| {
                (leg.current_price - leg.entry_price)
                    * leg.side.multiplier()
                    * f64::from(leg.qty)
                    * f64::from(*multiplier)
            })
            .sum(),
    }
}

/// Compute P&L for closing a stock position at an explicit price.
///
/// Used for "called_away" exits where the stock must be priced at the
/// short call's strike rather than the bar's close.
fn compute_stock_pnl_at_price(pos: &ScriptPosition, exit_price: f64) -> f64 {
    match &pos.inner {
        ScriptPositionInner::Stock {
            side,
            qty,
            entry_price,
        } => (exit_price - entry_price) * f64::from(*qty) * side.multiplier(),
        ScriptPositionInner::Options { .. } => 0.0,
    }
}

/// Look up current option price from PriceTable or LastKnown fallback.
/// Returns the fill price under the configured slippage model, or None.
/// Compute commission for a position (entry or exit).
fn compute_commission(commission: &Option<Commission>, pos: &ScriptPosition) -> f64 {
    let Some(comm) = commission else {
        return 0.0;
    };
    // Commission is per-contract (options only). Stock positions don't use per-contract fees.
    let contracts = match &pos.inner {
        ScriptPositionInner::Options { legs, .. } => legs.iter().map(|l| l.qty).sum::<i32>(),
        ScriptPositionInner::Stock { .. } => return 0.0,
    };
    comm.calculate(contracts)
}

/// Classify an options expiration as "expiration" (OTM), "assignment" (short put ITM),
/// or "called_away" (short call ITM) based on strike vs underlying close.
fn classify_expiration(legs: &[ScriptPositionLeg], underlying_close: f64) -> String {
    for leg in legs {
        match (leg.side, leg.option_type) {
            // Short put is ITM when strike >= close → assignment
            (Side::Short, crate::engine::types::OptionType::Put) => {
                if leg.strike >= underlying_close {
                    return "assignment".to_string();
                }
            }
            // Short call is ITM when strike <= close → called away
            (Side::Short, crate::engine::types::OptionType::Call) => {
                if leg.strike <= underlying_close {
                    return "called_away".to_string();
                }
            }
            _ => {}
        }
    }
    // All legs OTM → standard expiration
    "expiration".to_string()
}

fn lookup_option_price(
    price_table: &Option<Arc<crate::engine::sim_types::PriceTable>>,
    last_known: &crate::engine::sim_types::LastKnown,
    today: NaiveDate,
    expiration: NaiveDate,
    strike: f64,
    option_type: crate::engine::types::OptionType,
    side: Side,
    slippage: &Slippage,
) -> Option<f64> {
    use crate::engine::pricing::fill_price;
    use ordered_float::OrderedFloat;

    let key = (today, expiration, OrderedFloat(strike), option_type);

    // Try PriceTable first
    if let Some(pt) = price_table {
        if let Some(quote) = pt.get(&key) {
            // For MTM, use the exit side (flipped from entry side)
            let exit_side = side.flip();
            return Some(fill_price(quote.bid, quote.ask, exit_side, slippage));
        }
    }

    // Fallback: last known price
    let lk_key = (expiration, OrderedFloat(strike), option_type);
    if let Some(quote) = last_known.get(&lk_key) {
        let exit_side = side.flip();
        return Some(fill_price(quote.bid, quote.ask, exit_side, slippage));
    }

    None
}

/// Build a `TradeRecord` from a script position close.
fn build_script_trade_record(
    pos: &ScriptPosition,
    exit_datetime: NaiveDateTime,
    pnl: f64,
    exit_reason: &str,
) -> TradeRecord {
    use crate::engine::types::{CashflowLabel, ExitType, LegDetail};

    let entry_datetime = pos
        .entry_date
        .and_hms_opt(0, 0, 0)
        .expect("and_hms_opt should not fail");

    let exit_type = match exit_reason {
        "expiration" => ExitType::Expiration,
        "stop_loss" => ExitType::StopLoss,
        "take_profit" => ExitType::TakeProfit,
        "dte_exit" => ExitType::DteExit,
        "max_hold" => ExitType::MaxHold,
        "signal" => ExitType::Signal,
        "assignment" => ExitType::Assignment,
        "called_away" => ExitType::CalledAway,
        "delta_exit" => ExitType::DeltaExit,
        "end_of_data" => ExitType::Expiration, // no dedicated variant; closest match
        _ => ExitType::Signal,                 // script-defined exit reasons
    };

    let entry_cost = pos.entry_cost;
    let exit_proceeds = entry_cost + pnl;
    let (entry_label, entry_amount) = if entry_cost >= 0.0 {
        (CashflowLabel::DR, entry_cost)
    } else {
        (CashflowLabel::CR, -entry_cost)
    };
    let (exit_label, exit_amount) = if exit_proceeds >= 0.0 {
        (CashflowLabel::CR, exit_proceeds)
    } else {
        (CashflowLabel::DR, -exit_proceeds)
    };

    let legs = match &pos.inner {
        ScriptPositionInner::Options { legs, .. } => legs
            .iter()
            .map(|l| LegDetail {
                side: l.side,
                option_type: l.option_type,
                strike: l.strike,
                expiration: l.expiration.to_string(),
                entry_price: l.entry_price,
                exit_price: Some(l.current_price),
                qty: l.qty,
                entry_delta: Some(l.delta),
                is_stock: false,
            })
            .collect(),
        ScriptPositionInner::Stock {
            side,
            qty,
            entry_price,
        } => vec![LegDetail {
            side: *side,
            option_type: crate::engine::types::OptionType::Call, // unused for stock
            strike: 0.0,
            expiration: String::new(),
            entry_price: *entry_price,
            exit_price: None,
            qty: *qty,
            entry_delta: None,
            is_stock: true,
        }],
    };

    TradeRecord {
        trade_id: pos.id,
        symbol: Some(pos.symbol.clone()),
        entry_datetime,
        exit_datetime,
        entry_cost,
        exit_proceeds,
        entry_amount,
        entry_label,
        exit_amount,
        exit_label,
        pnl,
        days_held: pos.days_held,
        exit_type,
        legs,
        computed_quantity: None,
        entry_equity: None,
        stock_entry_price: match &pos.inner {
            ScriptPositionInner::Stock { entry_price, .. } => Some(*entry_price),
            ScriptPositionInner::Options { .. } => None,
        },
        stock_exit_price: match &pos.inner {
            ScriptPositionInner::Stock { .. } => {
                // Approximate exit price from entry + pnl/qty
                let ep = match &pos.inner {
                    ScriptPositionInner::Stock {
                        entry_price,
                        qty,
                        side,
                    } => {
                        if *qty != 0 {
                            entry_price + pnl / (*qty as f64 * side.multiplier())
                        } else {
                            0.0
                        }
                    }
                    ScriptPositionInner::Options { .. } => 0.0,
                };
                Some(ep)
            }
            ScriptPositionInner::Options { .. } => None,
        },
        stock_pnl: match &pos.inner {
            ScriptPositionInner::Stock { .. } => Some(pnl),
            ScriptPositionInner::Options { .. } => None,
        },
        group: pos.group.clone(),
    }
}

/// Parse the result of `on_exit_check` into a `ScriptAction`.
fn parse_exit_action(result: &Dynamic) -> Option<ScriptAction> {
    let map = result.clone().try_cast::<rhai::Map>()?;
    let action = map.get("action")?.clone().into_immutable_string().ok()?;

    match action.as_str() {
        "close" => {
            let reason = map
                .get("reason")
                .and_then(|v| v.clone().into_immutable_string().ok())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "script_exit".to_string());
            Some(ScriptAction::Close {
                position_id: None,
                reason,
            })
        }
        "hold" => Some(ScriptAction::Hold),
        "stop" => {
            let reason = map
                .get("reason")
                .and_then(|v| v.clone().into_immutable_string().ok())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "stop".to_string());
            Some(ScriptAction::Stop { reason })
        }
        _ => None,
    }
}

/// Parse the result of `on_bar` into a list of `ScriptAction`s.
/// A parsed action from `on_bar()` with associated order metadata.
struct ParsedAction {
    action: ScriptAction,
    /// Target symbol for this action. `None` = primary symbol.
    symbol: Option<String>,
    order_type: OrderType,
    /// `true` = buy direction, `false` = sell direction for fill logic.
    is_buy: bool,
    signal: Option<String>,
    ttl: Option<usize>,
    stop_loss: Option<ExitModifier>,
    profit_target: Option<ExitModifier>,
    trailing_stop: Option<ExitModifier>,
}

/// Parse the result of `on_bar` into a list of `ParsedAction`s.
///
/// Each action is paired with its order metadata (order type, signal, TTL)
/// extracted from the same map, ensuring metadata is tied to the correct item.
fn parse_bar_actions(result: &Dynamic) -> Vec<ParsedAction> {
    let Some(arr) = result.clone().try_cast::<rhai::Array>() else {
        return vec![];
    };

    arr.into_iter()
        .filter_map(|item| {
            let map = item.try_cast::<rhai::Map>()?;
            let action_str = map.get("action")?.clone().into_immutable_string().ok()?;

            let (action, is_buy) = match action_str.as_str() {
                "open_stock" => {
                    let side_str = map.get("side")?.clone().into_immutable_string().ok()?;
                    let side = match side_str.as_str() {
                        "long" => Side::Long,
                        "short" => Side::Short,
                        _ => return None,
                    };
                    let qty = map.get("qty")?.as_int().ok()? as i32;
                    let is_buy = side == Side::Long;
                    let symbol = map
                        .get("symbol")
                        .and_then(|v| v.clone().into_immutable_string().ok())
                        .and_then(|s| {
                            let t = s.trim();
                            (!t.is_empty()).then(|| t.to_uppercase())
                        });
                    (ScriptAction::OpenStock { side, qty, symbol }, is_buy)
                }
                "close" => {
                    let position_id = map
                        .get("position_id")
                        .and_then(|v| v.as_int().ok())
                        .map(|v| v as usize);
                    let reason = map
                        .get("reason")
                        .and_then(|v| v.clone().into_immutable_string().ok())
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "script_close".to_string());
                    // Close a long position = sell (is_buy=false),
                    // close a short position = buy-to-cover (is_buy=true).
                    // Default to sell (most common: closing a long).
                    let side_str = map
                        .get("side")
                        .and_then(|v| v.clone().into_immutable_string().ok());
                    let is_buy = side_str.as_deref() == Some("short");
                    (
                        ScriptAction::Close {
                            position_id,
                            reason,
                        },
                        is_buy,
                    )
                }
                "stop" => {
                    let reason = map
                        .get("reason")
                        .and_then(|v| v.clone().into_immutable_string().ok())
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "stop".to_string());
                    (ScriptAction::Stop { reason }, false)
                }
                "cancel_orders" => {
                    let signal = map
                        .get("signal")
                        .and_then(|v| v.clone().into_immutable_string().ok())
                        .map(|s| s.to_string());
                    (ScriptAction::CancelOrders { signal }, false)
                }
                "open_options" | "open_spread" => {
                    let legs_arr = if let Some(legs) = map.get("legs") {
                        legs.clone().try_cast::<rhai::Array>()?
                    } else if let Some(spread) = map.get("spread") {
                        let spread_map = spread.clone().try_cast::<rhai::Map>()?;
                        spread_map.get("legs")?.clone().try_cast::<rhai::Array>()?
                    } else {
                        return None;
                    };
                    let qty = map
                        .get("qty")
                        .and_then(|v| v.as_int().ok())
                        .map(|v| v as i32);

                    let legs: Vec<LegSpec> = legs_arr
                        .into_iter()
                        .filter_map(|leg_dyn| {
                            let leg = leg_dyn.try_cast::<rhai::Map>()?;
                            let side_str = leg.get("side")?.clone().into_immutable_string().ok()?;
                            let side = match side_str.as_str() {
                                "long" => Side::Long,
                                "short" => Side::Short,
                                _ => return None,
                            };
                            let opt_type_str = leg
                                .get("option_type")
                                .and_then(|v| v.clone().into_immutable_string().ok())?;
                            let option_type = match opt_type_str.as_str() {
                                "call" | "c" => crate::engine::types::OptionType::Call,
                                "put" | "p" => crate::engine::types::OptionType::Put,
                                _ => return None,
                            };

                            if let Some(strike_val) = leg.get("strike") {
                                let strike = strike_val.as_float().ok()?;
                                let exp_str = leg
                                    .get("expiration")?
                                    .clone()
                                    .into_immutable_string()
                                    .ok()?;
                                let expiration =
                                    NaiveDate::parse_from_str(&exp_str, "%Y-%m-%d").ok()?;
                                let bid = leg
                                    .get("bid")
                                    .and_then(|v| v.as_float().ok())
                                    .unwrap_or(0.0);
                                let ask = leg
                                    .get("ask")
                                    .and_then(|v| v.as_float().ok())
                                    .unwrap_or(0.0);
                                Some(LegSpec::Resolved {
                                    side,
                                    option_type,
                                    strike,
                                    expiration,
                                    bid,
                                    ask,
                                })
                            } else {
                                let delta = leg
                                    .get("delta")
                                    .and_then(|v| v.as_float().ok())
                                    .unwrap_or(0.30);
                                let dte = leg.get("dte").and_then(|v| v.as_int().ok()).unwrap_or(45)
                                    as i32;
                                Some(LegSpec::Unresolved {
                                    side,
                                    option_type,
                                    delta,
                                    dte,
                                })
                            }
                        })
                        .collect();

                    if legs.is_empty() {
                        return None;
                    }
                    // Options: buy = long first leg (net debit), sell = short first leg
                    let is_buy =
                        legs.first()
                            .map(|l| match l {
                                LegSpec::Unresolved { side, .. }
                                | LegSpec::Resolved { side, .. } => *side == Side::Long,
                            })
                            .unwrap_or(true);
                    // Symbol may be on the outer action map or nested inside the
                    // "spread" sub-map (set by SymbolContext.build_strategy).
                    let symbol = map
                        .get("symbol")
                        .cloned()
                        .or_else(|| {
                            map.get("spread")
                                .and_then(|s| s.clone().try_cast::<rhai::Map>())
                                .and_then(|m| m.get("symbol").cloned())
                        })
                        .and_then(|v| v.into_immutable_string().ok())
                        .and_then(|s| {
                            let t = s.trim();
                            (!t.is_empty()).then(|| t.to_uppercase())
                        });
                    (ScriptAction::OpenOptions { legs, qty, symbol }, is_buy)
                }
                _ => return None,
            };

            // Extract order metadata from the same map (tied to this exact action)
            let signal = map
                .get("signal")
                .and_then(|v| v.clone().into_immutable_string().ok())
                .map(|s| s.to_string());

            let ttl = map
                .get("ttl")
                .and_then(|v| v.as_int().ok())
                .map(|v| v as usize);

            let order_type = extract_order_type(&map);

            let stop_loss = extract_exit_modifier(&map, "stop_loss_pct", "stop_loss_dollar");
            let profit_target =
                extract_exit_modifier(&map, "profit_target_pct", "profit_target_dollar");
            let trailing_stop =
                extract_exit_modifier(&map, "trailing_stop_pct", "trailing_stop_dollar");

            // Extract symbol from the action (set by SymbolContext.build_strategy)
            let symbol = match &action {
                ScriptAction::OpenStock { symbol, .. }
                | ScriptAction::OpenOptions { symbol, .. } => symbol.clone(),
                _ => None,
            };

            Some(ParsedAction {
                action,
                symbol,
                order_type,
                is_buy,
                signal,
                ttl,
                stop_loss,
                profit_target,
                trailing_stop,
            })
        })
        .collect()
}

/// Extract `OrderType` from an action map. Falls back to `Market` with a warning
/// if required price fields are missing.
fn extract_order_type(map: &rhai::Map) -> OrderType {
    let order_type_str = map
        .get("order_type")
        .and_then(|v| v.clone().into_immutable_string().ok());

    match order_type_str.as_deref() {
        Some("limit") => {
            if let Some(price) = map.get("limit_price").and_then(|v| v.as_float().ok()) {
                OrderType::Limit { price }
            } else {
                tracing::warn!("Missing `limit_price` for limit order; falling back to market");
                OrderType::Market
            }
        }
        Some("stop") => {
            if let Some(price) = map.get("stop_price").and_then(|v| v.as_float().ok()) {
                OrderType::Stop { price }
            } else {
                tracing::warn!("Missing `stop_price` for stop order; falling back to market");
                OrderType::Market
            }
        }
        Some("stop_limit") => {
            if let (Some(stop), Some(limit)) = (
                map.get("stop_price").and_then(|v| v.as_float().ok()),
                map.get("limit_price").and_then(|v| v.as_float().ok()),
            ) {
                OrderType::StopLimit { stop, limit }
            } else {
                tracing::warn!(
                    "Missing `stop_price`/`limit_price` for stop_limit order; falling back to market"
                );
                OrderType::Market
            }
        }
        _ => OrderType::Market,
    }
}

fn extract_exit_modifier(map: &rhai::Map, pct_key: &str, dollar_key: &str) -> Option<ExitModifier> {
    if let Some(v) = map.get(pct_key) {
        let pct = v
            .as_float()
            .ok()
            .or_else(|| v.as_int().ok().map(|i| i as f64))?;
        if pct > 0.0 {
            return Some(ExitModifier::Percent(pct));
        }
    }
    if let Some(v) = map.get(dollar_key) {
        let amt = v
            .as_float()
            .ok()
            .or_else(|| v.as_int().ok().map(|i| i as f64))?;
        if amt > 0.0 {
            return Some(ExitModifier::Dollar(amt));
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Data loader trait (for testability)
// ---------------------------------------------------------------------------

/// Trait for loading OHLCV and options data.
/// Abstracted for testability — real implementation uses `CachedStore`.
#[async_trait::async_trait]
pub trait DataLoader: Send + Sync {
    async fn load_ohlcv(
        &self,
        symbol: &str,
        start: Option<NaiveDate>,
        end: Option<NaiveDate>,
    ) -> Result<polars::prelude::DataFrame>;

    async fn load_options(
        &self,
        symbol: &str,
        start: Option<NaiveDate>,
        end: Option<NaiveDate>,
    ) -> Result<polars::prelude::DataFrame>;

    /// Load splits for a symbol. Returns empty vec if no adjustment store available.
    fn load_splits(&self, symbol: &str) -> Result<Vec<crate::data::adjustment_store::SplitRow>>;

    /// Load dividends for a symbol. Returns empty vec if no adjustment store available.
    fn load_dividends(
        &self,
        symbol: &str,
    ) -> Result<Vec<crate::data::adjustment_store::DividendRow>>;
}

/// `DataLoader` backed by `CachedStore` — the production implementation.
///
/// Resolves symbol → Parquet path via `CachedStore::find_ohlcv`, loads the
/// DataFrame via `stock_sim::load_ohlcv_df`.
pub struct CachedDataLoader {
    pub cache: Arc<crate::data::cache::CachedStore>,
    pub adjustment_store: Option<Arc<crate::data::adjustment_store::SqliteAdjustmentStore>>,
}

#[async_trait::async_trait]
impl DataLoader for CachedDataLoader {
    async fn load_ohlcv(
        &self,
        symbol: &str,
        start: Option<NaiveDate>,
        end: Option<NaiveDate>,
    ) -> Result<polars::prelude::DataFrame> {
        let cache = Arc::clone(&self.cache);
        let symbol = symbol.to_uppercase();

        tokio::task::spawn_blocking(move || {
            let path = cache.find_ohlcv(&symbol).ok_or_else(|| {
                anyhow::anyhow!(
                    "No OHLCV data found for '{symbol}'. \
                         Searched: etf/, stocks/, futures/, indices/ in {}",
                    cache.cache_dir().display()
                )
            })?;

            let path_str = path.to_string_lossy().to_string();
            crate::engine::ohlcv::load_ohlcv_df(&path_str, start, end)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Task join error: {e}"))?
    }

    async fn load_options(
        &self,
        symbol: &str,
        start: Option<NaiveDate>,
        end: Option<NaiveDate>,
    ) -> Result<polars::prelude::DataFrame> {
        use crate::data::DataStore;
        self.cache
            .load_options(&symbol.to_uppercase(), start, end)
            .await
    }

    fn load_splits(&self, symbol: &str) -> Result<Vec<crate::data::adjustment_store::SplitRow>> {
        match &self.adjustment_store {
            Some(store) => store.splits(symbol),
            None => Ok(Vec::new()),
        }
    }

    fn load_dividends(
        &self,
        symbol: &str,
    ) -> Result<Vec<crate::data::adjustment_store::DividendRow>> {
        match &self.adjustment_store {
            Some(store) => store.dividends(symbol),
            None => Ok(Vec::new()),
        }
    }
}

/// `DataLoader` wrapper that caches full DataFrames in memory by symbol.
///
/// Loads the full (unfiltered) parquet once per symbol, then applies date-range
/// filters in-memory on subsequent calls. This eliminates repeated disk I/O during
/// walk-forward sweeps (50+ backtests hitting the same files).
pub struct CachingDataLoader {
    inner: CachedDataLoader,
    ohlcv_cache: tokio::sync::Mutex<HashMap<String, Arc<polars::prelude::DataFrame>>>,
    options_cache: tokio::sync::Mutex<HashMap<String, Arc<polars::prelude::DataFrame>>>,
}

impl CachingDataLoader {
    pub fn new(
        cache: Arc<crate::data::cache::CachedStore>,
        adjustment_store: Option<Arc<crate::data::adjustment_store::SqliteAdjustmentStore>>,
    ) -> Self {
        Self {
            inner: CachedDataLoader {
                cache,
                adjustment_store,
            },
            ohlcv_cache: tokio::sync::Mutex::new(HashMap::new()),
            options_cache: tokio::sync::Mutex::new(HashMap::new()),
        }
    }
}

/// Apply date-range filter to a cached (full) DataFrame.
fn filter_df_by_date(
    df: &polars::prelude::DataFrame,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
) -> Result<polars::prelude::DataFrame> {
    use polars::prelude::*;

    if start.is_none() && end.is_none() {
        return Ok(df.clone());
    }

    let date_col_name = crate::engine::ohlcv::detect_date_col(df);
    let is_datetime = date_col_name == "datetime";
    let mut lazy = df.clone().lazy();

    if let Some(s) = start {
        if is_datetime {
            let sdt = s.and_hms_opt(0, 0, 0).unwrap();
            lazy = lazy.filter(col(date_col_name).gt_eq(lit(sdt)));
        } else {
            lazy = lazy.filter(col(date_col_name).gt_eq(lit(s)));
        }
    }
    if let Some(e) = end {
        if is_datetime {
            let edt = e.succ_opt().unwrap_or(e).and_hms_opt(0, 0, 0).unwrap();
            lazy = lazy.filter(col(date_col_name).lt(lit(edt)));
        } else {
            lazy = lazy.filter(col(date_col_name).lt_eq(lit(e)));
        }
    }

    Ok(lazy.collect()?)
}

#[async_trait::async_trait]
impl DataLoader for CachingDataLoader {
    async fn load_ohlcv(
        &self,
        symbol: &str,
        start: Option<NaiveDate>,
        end: Option<NaiveDate>,
    ) -> Result<polars::prelude::DataFrame> {
        let key = symbol.to_uppercase();

        // Check cache
        {
            let cache = self.ohlcv_cache.lock().await;
            if let Some(df) = cache.get(&key) {
                return filter_df_by_date(df, start, end);
            }
        }

        // Cache miss — load full file (no date filter) and store
        let full_df = self.inner.load_ohlcv(symbol, None, None).await?;
        let arc_df = Arc::new(full_df);
        {
            let mut cache = self.ohlcv_cache.lock().await;
            cache.insert(key, Arc::clone(&arc_df));
        }

        filter_df_by_date(&arc_df, start, end)
    }

    async fn load_options(
        &self,
        symbol: &str,
        start: Option<NaiveDate>,
        end: Option<NaiveDate>,
    ) -> Result<polars::prelude::DataFrame> {
        let key = symbol.to_uppercase();

        // Check cache
        {
            let cache = self.options_cache.lock().await;
            if let Some(df) = cache.get(&key) {
                return filter_df_by_date(df, start, end);
            }
        }

        // Cache miss — load full file and store
        let full_df = self.inner.load_options(symbol, None, None).await?;
        let arc_df = Arc::new(full_df);
        {
            let mut cache = self.options_cache.lock().await;
            cache.insert(key, Arc::clone(&arc_df));
        }

        filter_df_by_date(&arc_df, start, end)
    }

    fn load_splits(&self, symbol: &str) -> Result<Vec<crate::data::adjustment_store::SplitRow>> {
        self.inner.load_splits(symbol)
    }

    fn load_dividends(
        &self,
        symbol: &str,
    ) -> Result<Vec<crate::data::adjustment_store::DividendRow>> {
        self.inner.load_dividends(symbol)
    }
}

/// Forward-fill cross-symbol data to align with primary timeline dates.
///
/// For each primary date, uses the last available cross-symbol bar on or before that date.
/// If no data exists before the first primary date, backfills with the first available bar.
fn forward_fill_cross_symbol(
    primary_dates: &[NaiveDate],
    cross_bars: &[OhlcvBar],
) -> Vec<CrossSymbolBar> {
    if cross_bars.is_empty() || primary_dates.is_empty() {
        return vec![
            CrossSymbolBar {
                open: 0.0,
                high: 0.0,
                low: 0.0,
                close: 0.0,
                volume: 0.0,
            };
            primary_dates.len()
        ];
    }

    // Build date → bar index map for cross data
    let mut cross_by_date: std::collections::BTreeMap<NaiveDate, usize> =
        std::collections::BTreeMap::new();
    for (i, bar) in cross_bars.iter().enumerate() {
        cross_by_date.insert(bar.datetime.date(), i);
    }

    let mut result = Vec::with_capacity(primary_dates.len());
    let mut last_bar_idx: Option<usize> = None;

    for &date in primary_dates {
        // Find the last cross bar on or before this date
        if let Some((&_d, &idx)) = cross_by_date.range(..=date).next_back() {
            last_bar_idx = Some(idx);
        }

        let bar = last_bar_idx
            .map(|idx| {
                let b = &cross_bars[idx];
                CrossSymbolBar {
                    open: b.open,
                    high: b.high,
                    low: b.low,
                    close: b.close,
                    volume: b.volume,
                }
            })
            .unwrap_or_else(|| {
                // Backfill with first available bar
                let b = &cross_bars[0];
                CrossSymbolBar {
                    open: b.open,
                    high: b.high,
                    low: b.low,
                    close: b.close,
                    volume: b.volume,
                }
            });

        result.push(bar);
    }

    result
}

// ---------------------------------------------------------------------------
// Multi-symbol data loading helpers
// ---------------------------------------------------------------------------

/// Load all per-symbol data for a multi-symbol portfolio backtest.
///
/// For each symbol: loads OHLCV, adjustments, indicators, and tries to load options.
/// Returns the per-symbol data map and the master timeline (date intersection).
///
/// The master timeline is the intersection of all symbols' daily OHLCV date ranges,
/// ensuring every bar has real data for every symbol. Multi-symbol backtests
/// require daily data; intraday intervals must be coerced to daily before calling.
#[allow(clippy::too_many_lines, clippy::single_match_else)]
async fn load_multi_symbol_data(
    config: &ScriptConfig,
    data_loader: &dyn DataLoader,
    warnings: &mut Vec<String>,
) -> Result<(HashMap<String, PerSymbolData>, Vec<NaiveDate>)> {
    use std::collections::BTreeSet;

    // Multi-symbol backtests require daily bars because the master timeline is
    // built by intersecting NaiveDate sets. Intraday intervals would produce
    // misaligned bar indices across symbols.
    if config.interval != Interval::Daily {
        bail!(
            "Multi-symbol backtests require daily interval (got {:?}). \
             Coerce to daily or use single-symbol mode for intraday data.",
            config.interval
        );
    }

    // 1. Load OHLCV for all symbols and collect their date sets
    let mut raw_bars_by_symbol: HashMap<String, Vec<OhlcvBar>> = HashMap::new();
    let mut date_sets: Vec<BTreeSet<NaiveDate>> = Vec::new();

    for sym in &config.symbols {
        let df = data_loader
            .load_ohlcv(sym, config.start_date, config.end_date)
            .await
            .with_context(|| format!("Failed to load OHLCV for '{sym}'"))?;

        if df.height() == 0 {
            bail!("No OHLCV data found for symbol '{sym}'");
        }

        // Resample intraday → daily only when needed (matching single-symbol logic)
        let data_is_intraday = is_intraday_data(&df);
        let needs_daily = config.interval == Interval::Daily && data_is_intraday;
        let df = if needs_daily {
            crate::engine::ohlcv::resample_ohlcv(&df, crate::engine::types::Interval::Daily)?
        } else {
            df
        };

        let bars = ohlcv_bars_from_df(&df)?;
        let dates: BTreeSet<NaiveDate> = bars.iter().map(|b| b.datetime.date()).collect();
        date_sets.push(dates);
        raw_bars_by_symbol.insert(sym.clone(), bars);
    }

    // 2. Compute date intersection (dates where ALL symbols have data)
    let master_dates: BTreeSet<NaiveDate> = if date_sets.len() == 1 {
        date_sets.into_iter().next().unwrap()
    } else {
        let mut intersection = date_sets[0].clone();
        for ds in &date_sets[1..] {
            intersection = intersection.intersection(ds).copied().collect();
        }
        intersection
    };

    if master_dates.is_empty() {
        bail!(
            "No overlapping dates across symbols: {}",
            config.symbols.join(", ")
        );
    }

    let master_dates_vec: Vec<NaiveDate> = master_dates.iter().copied().collect();

    // Report date range trimming
    for sym in &config.symbols {
        let bars = &raw_bars_by_symbol[sym];
        let sym_start = bars.first().map(|b| b.datetime.date());
        let sym_end = bars.last().map(|b| b.datetime.date());
        let master_start = master_dates_vec.first().copied();
        let master_end = master_dates_vec.last().copied();
        if sym_start != master_start || sym_end != master_end {
            warnings.push(format!(
                "{sym}: OHLCV range {}-{} trimmed to intersection {}-{}",
                sym_start.map_or("?".into(), |d| d.to_string()),
                sym_end.map_or("?".into(), |d| d.to_string()),
                master_start.map_or("?".into(), |d| d.to_string()),
                master_end.map_or("?".into(), |d| d.to_string()),
            ));
        }
    }

    // 3. For each symbol: filter bars to intersection, load adjustments, indicators, options
    let mut per_symbol: HashMap<String, PerSymbolData> = HashMap::new();

    for sym in &config.symbols {
        let raw_bars = raw_bars_by_symbol.remove(sym).unwrap();

        // Filter bars to master timeline dates
        let bars: Vec<OhlcvBar> = raw_bars
            .into_iter()
            .filter(|b| master_dates.contains(&b.datetime.date()))
            .collect();

        // Load adjustments (propagate real errors, don't silently default to empty)
        let splits = data_loader
            .load_splits(sym)
            .with_context(|| format!("Failed to load splits for '{sym}'"))?;
        let dividends = data_loader
            .load_dividends(sym)
            .with_context(|| format!("Failed to load dividends for '{sym}'"))?;
        let closes: Vec<(NaiveDate, f64)> =
            bars.iter().map(|b| (b.datetime.date(), b.close)).collect();

        let split_timeline = Arc::new(crate::engine::adjustments::AdjustmentTimeline::build(
            &splits,
            &[],
            &[],
        ));
        let adjustment_timeline = Arc::new(crate::engine::adjustments::AdjustmentTimeline::build(
            &splits, &dividends, &closes,
        ));

        // Apply split adjustment to simulation bars
        let bars: Vec<OhlcvBar> = if split_timeline.is_empty() {
            bars
        } else {
            bars.iter()
                .map(|b| {
                    let factor = split_timeline.factor_at(b.datetime.date());
                    OhlcvBar {
                        datetime: b.datetime,
                        open: b.open * factor,
                        high: b.high * factor,
                        low: b.low * factor,
                        close: b.close * factor,
                        volume: b.volume,
                    }
                })
                .collect()
        };

        // Build indicator bars (dividend-adjusted)
        let indicator_bars: Vec<OhlcvBar> = if adjustment_timeline.is_empty() {
            bars.clone()
        } else {
            bars.iter()
                .map(|b| {
                    let split_factor = split_timeline.factor_at(b.datetime.date());
                    let full_factor = adjustment_timeline.factor_at(b.datetime.date());
                    let div_factor = if split_factor.abs() > f64::EPSILON {
                        full_factor / split_factor
                    } else {
                        1.0
                    };
                    OhlcvBar {
                        datetime: b.datetime,
                        open: b.open * div_factor,
                        high: b.high * div_factor,
                        low: b.low * div_factor,
                        close: b.close * div_factor,
                        volume: b.volume,
                    }
                })
                .collect()
        };

        let indicator_store = Arc::new(IndicatorStore::build(
            &config.declared_indicators,
            &indicator_bars,
        )?);

        // Only load options when the script needs them (avoids I/O and warnings
        // for stock-only multi-symbol scripts).
        let (options_by_date, price_table, date_index) = if config.needs_options {
            match data_loader
                .load_options(sym, config.start_date, config.end_date)
                .await
            {
                Ok(df) if df.height() > 0 => {
                    let (pt, _days, di) = crate::engine::price_table::build_price_table(&df)?;
                    let obd = DatePartitionedOptions::from_df(&df, &config.expiration_filter)?;
                    (Some(Arc::new(obd)), Some(Arc::new(pt)), Some(Arc::new(di)))
                }
                Ok(_) => {
                    tracing::info!(symbol = sym, "Options data empty — OHLCV only");
                    (None, None, None)
                }
                Err(e) => {
                    tracing::info!(symbol = sym, error = %e, "No options data available — OHLCV only");
                    warnings.push(format!("{sym}: no options data ({e:#})"));
                    (None, None, None)
                }
            }
        } else {
            (None, None, None)
        };

        per_symbol.insert(
            sym.clone(),
            PerSymbolData {
                bars: Arc::new(bars),
                indicator_store,
                split_timeline,
                adjustment_timeline,
                options_by_date,
                price_table,
                date_index,
                last_known: std::sync::Mutex::new(crate::engine::sim_types::LastKnown::new()),
            },
        );
    }

    Ok((per_symbol, master_dates_vec))
}

/// Convert a Polars DataFrame (with OHLCV columns) to `Vec<OhlcvBar>`.
///
/// Reuses `stock_sim::bars_from_df` for datetime handling, then converts
/// `Bar` → `OhlcvBar` with volume (which the stock sim `Bar` struct lacks).
fn ohlcv_bars_from_df(df: &polars::prelude::DataFrame) -> Result<Vec<OhlcvBar>> {
    // Use the existing bars_from_df for datetime parsing (handles date vs datetime columns)
    let stock_bars = crate::engine::ohlcv::bars_from_df(df)?;

    // Extract volume column if present
    let volumes: Option<Vec<f64>> = df
        .column("volume")
        .ok()
        .and_then(|c| c.f64().ok())
        .map(|ca| ca.into_no_null_iter().collect());

    Ok(stock_bars
        .into_iter()
        .enumerate()
        .map(|(i, bar)| OhlcvBar {
            datetime: bar.datetime,
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            volume: volumes
                .as_ref()
                .and_then(|v| v.get(i).copied())
                .unwrap_or(0.0),
        })
        .collect())
}

/// Detect whether a DataFrame contains intraday data by checking the time gap
/// between the first two rows. A gap of less than one day indicates sub-daily bars.
/// Returns `false` if the DataFrame has fewer than two rows or lacks a `datetime` column.
fn is_intraday_data(df: &polars::prelude::DataFrame) -> bool {
    use crate::engine::types::timestamp_to_naive_datetime;

    if df.height() < 2 {
        return false;
    }

    let Ok(col) = df.column("datetime") else {
        return false;
    };
    let Ok(dt_ca) = col.datetime() else {
        return false;
    };
    let tu = dt_ca.time_unit();

    let (Some(raw0), Some(raw1)) = (dt_ca.phys.get(0), dt_ca.phys.get(1)) else {
        return false;
    };
    let (Some(t0), Some(t1)) = (
        timestamp_to_naive_datetime(raw0, tu),
        timestamp_to_naive_datetime(raw1, tu),
    ) else {
        return false;
    };

    let gap = t1.signed_duration_since(t0);
    gap < chrono::Duration::days(1)
}

// ---------------------------------------------------------------------------
// Result type
// ---------------------------------------------------------------------------

/// Extended backtest result with script metadata.
#[derive(Clone)]
pub struct ScriptBacktestResult {
    pub result: BacktestResult,
    pub metadata: Option<rhai::Map>,
    pub execution_time_ms: u64,
    /// Pre-computed indicator series used by the script, keyed by declaration
    /// (e.g., "sma:20", "rsi:14"). Each value is aligned to the bar index.
    /// The FE can plot these on the chart to show what the script used.
    pub indicator_data: HashMap<String, Vec<f64>>,
    /// Script-emitted custom series via `ctx.plot()` / `ctx.plot_with()`.
    pub custom_series: CustomSeriesStore,
    /// Precomputed options data from this run, available for reuse by subsequent
    /// sweep iterations. `None` for stock-only strategies.
    pub precomputed_options: Option<PrecomputedOptionsData>,
}

/// A single indicator series for JSON serialization in the response.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct IndicatorSeries {
    pub name: String,
    pub values: Vec<f64>,
}
