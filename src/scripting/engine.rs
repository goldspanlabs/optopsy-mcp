//! Unified scripting engine: compiles and executes Rhai backtest scripts.
//!
//! Drives the main simulation loop, calling Rhai callbacks at each bar.
//! Handles data loading, position management, and metrics calculation
//! while scripts define trading logic via `config()`, `on_bar()`, etc.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use chrono::{NaiveDate, NaiveDateTime};
use rhai::{CallFnOptions, Dynamic, Engine, Scope, AST};

use crate::engine::metrics::calculate_metrics;
use crate::engine::types::{
    BacktestResult, Commission, EquityPoint, ExitType, ExpirationFilter, PerformanceMetrics,
    Slippage, TradeRecord, TradeSelector,
};

use super::indicators::IndicatorStore;
use super::registration::build_engine;
use super::types::*;

/// Run a Rhai script backtest.
///
/// This is the main entry point. It compiles the script, extracts config,
/// loads data, pre-computes indicators, and runs the unified simulation loop.
pub async fn run_script_backtest(
    script_source: &str,
    data_loader: &dyn DataLoader,
) -> Result<ScriptBacktestResult> {
    // 1. Compile
    let engine = build_engine();
    let ast = engine
        .compile(script_source)
        .map_err(|e| anyhow::anyhow!("Script compile error: {e}"))?;

    // 2. Initialize scope (evaluate top-level let/const statements)
    let mut scope = Scope::new();
    engine
        .eval_ast_with_scope::<Dynamic>(&mut scope, &ast)
        .map_err(|e| anyhow::anyhow!("Script initialization error: {e}"))?;

    // 3. Call config()
    let config_map: Dynamic = call_fn_persistent(&engine, &mut scope, &ast, "config", ())?;
    let config = parse_config(config_map).context("Failed to parse config() return value")?;
    let config = Arc::new(config);

    // 4. Load data
    let bars = data_loader
        .load_ohlcv(
            &config.symbol,
            config.start_date,
            config.end_date,
            config.interval,
        )
        .await?;

    if bars.is_empty() {
        bail!("No OHLCV data found for symbol '{}'", config.symbol);
    }

    // 5. Pre-compute indicators
    let indicator_store = Arc::new(IndicatorStore::build(&config.declared_indicators, &bars)?);

    // 6-9. Run main loop (placeholder — will implement in Step 6)
    let equity_curve = vec![];
    let trade_log = vec![];
    let warnings = vec![];

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
            }],
            &[],
            config.capital,
            config.interval.bars_per_year(),
        )?
    };

    Ok(ScriptBacktestResult {
        result: BacktestResult {
            trade_count: trade_log.len(),
            total_pnl: trade_log.iter().map(|t| t.pnl).sum(),
            metrics,
            equity_curve,
            trade_log,
            quality: Default::default(),
            warnings,
        },
        metadata: None,
        execution_time_ms: 0,
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

    let symbol = get_string(&map, "symbol")?;
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

    Ok(ScriptConfig {
        symbol,
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
        Some(v) => parse_slippage(v)?,
        None => Slippage::Mid,
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
        interval: Interval,
    ) -> Result<Vec<OhlcvBar>>;
}

// ---------------------------------------------------------------------------
// Result type
// ---------------------------------------------------------------------------

/// Extended backtest result with script metadata.
pub struct ScriptBacktestResult {
    pub result: BacktestResult,
    pub metadata: Option<rhai::Map>,
    pub execution_time_ms: u64,
}
