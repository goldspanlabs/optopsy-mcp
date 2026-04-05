//! MCP tool handler for `run_script` — execute a Rhai backtest script.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use garde::Validate;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::data::traits::StrategyStore;
use crate::engine::types::BacktestResult;
use crate::scripting::stdlib::ScriptMeta;
use crate::scripting::types::CustomSeriesStore;

/// Base directory for strategy scripts (relative to project root).
const STRATEGIES_DIR: &str = "scripts/strategies";

/// Parameters for the `run_script` MCP tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct RunScriptParams {
    /// Strategy script filename (without extension).
    #[garde(skip)]
    pub strategy: Option<String>,

    /// Inline Rhai script source code. Use for quick one-off tests only.
    #[garde(skip)]
    pub script: Option<String>,

    /// Parameters injected as an immutable `params` map in the script scope.
    #[serde(default)]
    #[garde(skip)]
    pub params: HashMap<String, serde_json::Value>,

    /// Asset-class profile name (e.g., "equities", "crypto"). Loads defaults from
    /// `scripts/profiles.toml` and script-level `//! profile.<name>:` headers.
    #[serde(default)]
    #[garde(skip)]
    pub profile: Option<String>,
}

/// Response from a script backtest — passes through the full `BacktestResult`
/// so the FE can render trade markers, equity curves, and indicator overlays.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct RunScriptResponse {
    /// Script metadata (name, description, category) parsed from `//!` header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub script_meta: Option<ScriptMeta>,

    /// Full backtest result: `trade_log`, `equity_curve`, `metrics`, `warnings`.
    #[serde(flatten)]
    pub result: BacktestResult,

    /// Indicator series for chart rendering, matching the FE `IndicatorData` shape.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub indicator_data: Vec<IndicatorData>,

    /// Wall-clock execution time in milliseconds.
    pub execution_time_ms: u64,
}

// ---------------------------------------------------------------------------
// Indicator chart types — compact format for intraday efficiency
// ---------------------------------------------------------------------------

/// How an indicator should be displayed on a chart.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DisplayType {
    /// Overlay on the price chart (SMA, Bollinger Bands, Keltner, etc.)
    Overlay,
    /// Separate subchart below price (RSI, MACD, Stochastic, etc.)
    Subchart,
}

/// Compact indicator data for charting.
///
/// `values` is aligned to `equity_curve` by bar index — the FE zips
/// `equity_curve[i].datetime` with `values[i]` to get date+value pairs.
/// NaN warmup bars are serialized as `null` in JSON.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct IndicatorData {
    /// Declaration key (e.g., `"rsi:14"`, `"sma:20"`).
    pub key: String,
    /// Human-readable name (e.g., `"RSI"`, `"SMA"`).
    pub name: String,
    pub display_type: DisplayType,
    /// Raw values aligned to `equity_curve` bar indices. `NaN` = `null` in JSON.
    pub values: Vec<Option<f64>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub thresholds: Vec<f64>,
}

/// Convert raw indicator store data and custom series into compact `IndicatorData` format.
#[allow(clippy::implicit_hasher)]
pub fn format_indicator_data(
    raw: &std::collections::HashMap<String, Vec<f64>>,
    custom: &CustomSeriesStore,
) -> Vec<IndicatorData> {
    let mut result: Vec<IndicatorData> = raw
        .iter()
        .map(|(decl, values)| IndicatorData {
            key: decl.clone(),
            name: indicator_display_name(decl),
            display_type: indicator_display_type(decl),
            values: values
                .iter()
                .map(|&v| if v.is_finite() { Some(v) } else { None })
                .collect(),
            thresholds: indicator_thresholds(decl),
        })
        .collect();

    // Append script-emitted custom series
    for (name, values) in &custom.series {
        let display = custom
            .display_types
            .get(name)
            .map_or(DisplayType::Overlay, |d| match d.as_str() {
                "subchart" => DisplayType::Subchart,
                _ => DisplayType::Overlay,
            });

        result.push(IndicatorData {
            key: format!("custom:{name}"),
            name: name.clone(),
            display_type: display,
            values: values.clone(),
            thresholds: vec![],
        });
    }

    result
}

/// Human-readable display name from declaration (e.g., "sma:20" → "SMA")
fn indicator_display_name(decl: &str) -> String {
    let name = decl.split(':').next().unwrap_or(decl);
    match name {
        "sma" => "SMA",
        "ema" => "EMA",
        "rsi" => "RSI",
        "atr" => "ATR",
        "macd_line" => "MACD Line",
        "macd_signal" => "MACD Signal",
        "macd_hist" => "MACD Histogram",
        "bbands_upper" => "Bollinger Upper",
        "bbands_mid" => "Bollinger Mid",
        "bbands_lower" => "Bollinger Lower",
        "stochastic" => "Stochastic %K",
        "cci" => "CCI",
        "obv" => "OBV",
        "adx" => "ADX",
        "plus_di" => "+DI",
        "minus_di" => "-DI",
        "psar" => "Parabolic SAR",
        "supertrend" => "Supertrend",
        "keltner_upper" => "Keltner Upper",
        "keltner_lower" => "Keltner Lower",
        "donchian_upper" => "Donchian Upper",
        "donchian_mid" => "Donchian Mid",
        "donchian_lower" => "Donchian Lower",
        "williams_r" => "Williams %R",
        "mfi" => "MFI",
        "rank" => "Rank",
        "iv_rank" => "IV Rank",
        "tr" => "True Range",
        "ppo" => "PPO",
        "cmo" => "CMO",
        "roc" => "ROC",
        "vpt" => "VPT",
        "cmf" => "CMF",
        "std" => "Std Dev",
        "consecutive_up" => "Consecutive Up",
        "consecutive_down" => "Consecutive Down",
        _ => name,
    }
    .to_string()
}

/// Determine if indicator overlays on price or goes in a subchart.
fn indicator_display_type(decl: &str) -> DisplayType {
    let name = decl.split(':').next().unwrap_or(decl);
    match name {
        // Overlay on price chart
        "sma" | "ema" | "bbands_upper" | "bbands_mid" | "bbands_lower" | "psar" | "supertrend"
        | "keltner_upper" | "keltner_lower" | "donchian_upper" | "donchian_mid"
        | "donchian_lower" => DisplayType::Overlay,
        // Everything else in subchart
        _ => DisplayType::Subchart,
    }
}

/// Common threshold lines for oscillator indicators.
fn indicator_thresholds(decl: &str) -> Vec<f64> {
    let name = decl.split(':').next().unwrap_or(decl);
    match name {
        "rsi" => vec![30.0, 70.0],
        "stochastic" | "mfi" => vec![20.0, 80.0],
        "williams_r" => vec![-80.0, -20.0],
        "cci" => vec![-100.0, 100.0],
        _ => vec![],
    }
}

// ---------------------------------------------------------------------------
// Script resolution
// ---------------------------------------------------------------------------

/// Transpile `.trading` DSL to Rhai if the source looks like DSL, otherwise
/// return it unchanged.  This is the **single** place where DSL→Rhai
/// conversion is applied before execution.
pub fn maybe_transpile(source: String) -> Result<String> {
    if crate::scripting::dsl::is_trading_dsl(&source) {
        crate::scripting::dsl::transpile(&source)
            .map_err(|e| anyhow::anyhow!("DSL transpilation failed: {e}"))
    } else {
        Ok(source)
    }
}

/// Resolve the script source code from the strategy store or inline source.
///
/// Returns `(Option<strategy_id>, source)` — the resolved strategy UUID (None
/// for inline scripts) and the Rhai source code (transpiled from DSL if needed).
pub fn resolve_script_source(
    params: &RunScriptParams,
    strategy_store: Option<&dyn StrategyStore>,
) -> Result<(Option<String>, String)> {
    let (id, source) = match (&params.strategy, &params.script) {
        (Some(name_or_id), _) => {
            let (id, source) = load_strategy(name_or_id, strategy_store)?;
            (Some(id), source)
        }
        (None, Some(script)) => (None, script.clone()),
        (None, None) => {
            anyhow::bail!(
                "Either 'strategy' (script filename) or 'script' (inline source) is required"
            )
        }
    };
    Ok((id, maybe_transpile(source)?))
}

/// Load a strategy by ID or display name from the database, falling back to
/// filesystem only when no store is available (e.g. tests without a DB).
///
/// Returns `(resolved_id, source)`.
fn load_strategy(
    name_or_id: &str,
    strategy_store: Option<&dyn StrategyStore>,
) -> Result<(String, String)> {
    if let Some(store) = strategy_store {
        // Try exact ID match first
        if let Some(source) = store.get_source(name_or_id)? {
            return Ok((name_or_id.to_string(), source));
        }
        // Fall back to case-insensitive name match (so Claude can reference by name)
        if let Some((id, source)) = store.get_source_by_name(name_or_id)? {
            return Ok((id, source));
        }
        anyhow::bail!("Strategy '{name_or_id}' not found");
    }

    // Filesystem fallback for contexts without a store (e.g. tests)
    let source = load_strategy_file(name_or_id)?;
    Ok((name_or_id.to_string(), source))
}

/// Load a strategy script from `scripts/strategies/`. Tries `.trading` first, then `.rhai`.
fn load_strategy_file(name: &str) -> Result<String> {
    let trading_path = PathBuf::from(STRATEGIES_DIR).join(format!("{name}.trading"));
    if trading_path.exists() {
        return std::fs::read_to_string(&trading_path).map_err(|e| {
            anyhow::anyhow!(
                "Strategy '{name}' not found at '{}': {e}",
                trading_path.display()
            )
        });
    }
    let rhai_path = PathBuf::from(STRATEGIES_DIR).join(format!("{name}.rhai"));
    std::fs::read_to_string(&rhai_path).map_err(|e| {
        anyhow::anyhow!(
            "Strategy '{name}' not found at '{}': {e}",
            rhai_path.display()
        )
    })
}
