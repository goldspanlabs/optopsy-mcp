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
    BacktestResult, CashflowLabel, Commission, EquityPoint, ExitType, ExpirationFilter, LegDetail,
    PerformanceMetrics, Side, Slippage, TradeRecord, TradeSelector,
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

    // 6. Run main simulation loop
    let price_history = Arc::new(bars);
    let cross_symbol_data = Arc::new(HashMap::new()); // TODO: load cross-symbol data

    let has_on_exit_check = has_fn(&ast, "on_exit_check", 2);
    let has_on_position_opened = has_fn(&ast, "on_position_opened", 2);
    let has_on_position_closed = has_fn(&ast, "on_position_closed", 3);
    let has_on_end = has_fn(&ast, "on_end", 1);

    let mut positions: Vec<ScriptPosition> = Vec::new();
    let mut trade_log: Vec<TradeRecord> = Vec::new();
    let mut equity_curve: Vec<EquityPoint> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();
    let mut realized_equity = config.capital;
    let mut next_id = 1usize;
    let mut last_entry_date: Option<NaiveDate> = None;
    let mut stop_requested = false;

    for (bar_idx, bar) in price_history.iter().enumerate() {
        if stop_requested {
            break;
        }

        let today = bar.datetime.date();

        // --- Phase A: Exits (immediate processing) ---

        // Check built-in exits + script exit checks
        let mut i = 0;
        while i < positions.len() {
            let pos = &positions[i];
            let mut should_close = false;
            let mut exit_reason = String::new();

            // Built-in: option expiration
            if let ScriptPositionInner::Options { expiration, .. } = &pos.inner {
                if today >= *expiration {
                    should_close = true;
                    exit_reason = "expiration".to_string();
                }
            }

            // Script exit check (only for positions NOT opened this bar)
            if !should_close && has_on_exit_check && pos.days_held > 0 {
                let ctx = build_bar_context(
                    bar,
                    bar_idx,
                    &positions,
                    realized_equity,
                    &indicator_store,
                    &price_history,
                    &cross_symbol_data,
                    &config,
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
                // Close position immediately
                let pnl = compute_close_pnl(&positions[i], bar);
                realized_equity += pnl;

                // Fire on_position_closed synchronously
                if has_on_position_closed {
                    let ctx = build_bar_context(
                        bar,
                        bar_idx,
                        &positions,
                        realized_equity,
                        &indicator_store,
                        &price_history,
                        &cross_symbol_data,
                        &config,
                    );
                    let pos_dyn = Dynamic::from(positions[i].clone());
                    let exit_type_dyn = Dynamic::from(exit_reason.clone());
                    let _ = call_fn_persistent(
                        &engine,
                        &mut scope,
                        &ast,
                        "on_position_closed",
                        (ctx, pos_dyn, exit_type_dyn),
                    );
                }

                // Add to trade log
                trade_log.push(build_script_trade_record(
                    &positions[i],
                    bar.datetime,
                    pnl,
                    &exit_reason,
                ));

                positions.remove(i);
                // Don't increment i — next position is now at index i
            } else {
                i += 1;
            }
        }

        if stop_requested {
            break;
        }

        // --- Phase B: Entries (sees post-exit state) ---

        // Rebuild ctx with updated position state
        let ctx = build_bar_context(
            bar,
            bar_idx,
            &positions,
            realized_equity,
            &indicator_store,
            &price_history,
            &cross_symbol_data,
            &config,
        );

        // Call on_bar(ctx)
        match call_fn_persistent(&engine, &mut scope, &ast, "on_bar", (ctx,)) {
            Ok(result) => {
                let actions = parse_bar_actions(&result);
                for action in actions {
                    match action {
                        ScriptAction::Stop { reason } => {
                            stop_requested = true;
                            warnings.push(format!("Script requested stop: {reason}"));
                            break;
                        }
                        ScriptAction::OpenStock { side, qty } => {
                            // Stagger check
                            if let Some(min_days) = config.min_days_between_entries {
                                if let Some(last) = last_entry_date {
                                    if (today - last).num_days() < i64::from(min_days) {
                                        continue;
                                    }
                                }
                            }

                            let pos = ScriptPosition {
                                id: next_id,
                                entry_date: today,
                                inner: ScriptPositionInner::Stock {
                                    side,
                                    qty,
                                    entry_price: bar.close,
                                },
                                entry_cost: bar.close * qty as f64 * side.multiplier(),
                                unrealized_pnl: 0.0,
                                days_held: 0,
                                source: "script".to_string(),
                                implicit: false,
                            };
                            next_id += 1;
                            last_entry_date = Some(today);

                            if has_on_position_opened {
                                let ctx = build_bar_context(
                                    bar,
                                    bar_idx,
                                    &positions,
                                    realized_equity,
                                    &indicator_store,
                                    &price_history,
                                    &cross_symbol_data,
                                    &config,
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
                        ScriptAction::Close {
                            position_id,
                            reason,
                        } => {
                            if let Some(pid) = position_id {
                                if let Some(idx) = positions.iter().position(|p| p.id == pid) {
                                    let pnl = compute_close_pnl(&positions[idx], bar);
                                    realized_equity += pnl;

                                    if has_on_position_closed {
                                        let ctx = build_bar_context(
                                            bar,
                                            bar_idx,
                                            &positions,
                                            realized_equity,
                                            &indicator_store,
                                            &price_history,
                                            &cross_symbol_data,
                                            &config,
                                        );
                                        let pos_dyn = Dynamic::from(positions[idx].clone());
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
                                        &positions[idx],
                                        bar.datetime,
                                        pnl,
                                        &reason,
                                    ));
                                    positions.remove(idx);
                                } else {
                                    warnings
                                        .push(format!("Close action: position_id {pid} not found"));
                                }
                            }
                        }
                        // OpenOptions handled later (needs options data integration)
                        _ => {}
                    }
                }
            }
            Err(e) => {
                warnings.push(format!("on_bar error on bar {bar_idx}: {e}"));
            }
        }

        // --- Phase C: Bookkeeping ---

        // Update days_held for all open positions
        for pos in &mut positions {
            pos.days_held = (today - pos.entry_date).num_days();
        }

        // Mark-to-market: compute unrealized P&L for stock positions
        let mut unrealized = 0.0;
        for pos in &mut positions {
            match &pos.inner {
                ScriptPositionInner::Stock {
                    side,
                    qty,
                    entry_price,
                } => {
                    let pnl = (bar.close - entry_price) * *qty as f64 * side.multiplier();
                    pos.unrealized_pnl = pnl;
                    unrealized += pnl;
                }
                ScriptPositionInner::Options { .. } => {
                    // TODO: Options MTM requires PriceTable lookup
                    unrealized += pos.unrealized_pnl;
                }
            }
        }

        equity_curve.push(EquityPoint {
            datetime: bar.datetime,
            equity: realized_equity + unrealized,
        });
    }

    // 7. End-of-simulation
    if config.auto_close_on_end {
        // Auto-close remaining positions
        if let Some(last_bar) = price_history.last() {
            for pos in &positions {
                let pnl = compute_close_pnl(pos, last_bar);
                trade_log.push(build_script_trade_record(
                    pos,
                    last_bar.datetime,
                    pnl,
                    "end_of_data",
                ));
            }
        }
    }

    // Call on_end(ctx) — may return metadata
    let metadata = if has_on_end {
        let ctx = if let Some(last_bar) = price_history.last() {
            build_bar_context(
                last_bar,
                price_history.len() - 1,
                &positions,
                realized_equity,
                &indicator_store,
                &price_history,
                &cross_symbol_data,
                &config,
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
        metadata,
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
// Simulation helpers
// ---------------------------------------------------------------------------

/// Build a `BarContext` for the given bar.
fn build_bar_context(
    bar: &OhlcvBar,
    bar_idx: usize,
    positions: &[ScriptPosition],
    equity: f64,
    indicator_store: &Arc<IndicatorStore>,
    price_history: &Arc<Vec<OhlcvBar>>,
    cross_symbol_data: &Arc<HashMap<String, Vec<CrossSymbolBar>>>,
    config: &Arc<ScriptConfig>,
) -> BarContext {
    let cash = equity - positions.iter().map(|p| p.unrealized_pnl).sum::<f64>();
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
        positions: positions.to_vec(),
        indicator_store: Arc::clone(indicator_store),
        price_history: Arc::clone(price_history),
        cross_symbol_data: Arc::clone(cross_symbol_data),
        config: Arc::clone(config),
    }
}

/// Compute P&L for closing a position at the current bar's prices.
fn compute_close_pnl(pos: &ScriptPosition, bar: &OhlcvBar) -> f64 {
    match &pos.inner {
        ScriptPositionInner::Stock {
            side,
            qty,
            entry_price,
        } => (bar.close - entry_price) * *qty as f64 * side.multiplier(),
        ScriptPositionInner::Options { .. } => {
            // TODO: options close P&L requires PriceTable lookup
            // For now use unrealized_pnl as approximation
            pos.unrealized_pnl
        }
    }
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
        _ => ExitType::Signal, // generic fallback
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
            })
            .collect(),
        ScriptPositionInner::Stock { side, qty, .. } => vec![LegDetail {
            side: *side,
            option_type: crate::engine::types::OptionType::Call, // placeholder for stock
            strike: 0.0,
            expiration: String::new(),
            entry_price: match &pos.inner {
                ScriptPositionInner::Stock { entry_price, .. } => *entry_price,
                _ => 0.0,
            },
            exit_price: None,
            qty: *qty,
        }],
    };

    TradeRecord {
        trade_id: pos.id,
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
            _ => None,
        },
        stock_exit_price: None,
        stock_pnl: None,
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
fn parse_bar_actions(result: &Dynamic) -> Vec<ScriptAction> {
    let arr = match result.clone().try_cast::<rhai::Array>() {
        Some(a) => a,
        None => return vec![],
    };

    arr.into_iter()
        .filter_map(|item| {
            let map = item.try_cast::<rhai::Map>()?;
            let action = map.get("action")?.clone().into_immutable_string().ok()?;

            match action.as_str() {
                "open_stock" => {
                    let side_str = map.get("side")?.clone().into_immutable_string().ok()?;
                    let side = match side_str.as_str() {
                        "long" => Side::Long,
                        "short" => Side::Short,
                        _ => return None,
                    };
                    let qty = map.get("qty")?.as_int().ok()? as i32;
                    Some(ScriptAction::OpenStock { side, qty })
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
                    Some(ScriptAction::Close {
                        position_id,
                        reason,
                    })
                }
                "stop" => {
                    let reason = map
                        .get("reason")
                        .and_then(|v| v.clone().into_immutable_string().ok())
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "stop".to_string());
                    Some(ScriptAction::Stop { reason })
                }
                // "open_options" and "open_spread" — TODO: requires options data integration
                _ => None,
            }
        })
        .collect()
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
