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

    // Load options data if needed + build PriceTable for MTM
    let options_df: Option<Arc<polars::prelude::DataFrame>>;
    let price_table: Option<Arc<crate::engine::sim_types::PriceTable>>;
    let date_index: Option<Arc<crate::engine::sim_types::DateIndex>>;

    if config.needs_options {
        let df = data_loader
            .load_options(&config.symbol, config.start_date, config.end_date)
            .await?;
        let (pt, _trading_days, di) = crate::engine::price_table::build_price_table(&df)?;
        price_table = Some(Arc::new(pt));
        date_index = Some(Arc::new(di));
        options_df = Some(Arc::new(df));
    } else {
        options_df = None;
        price_table = None;
        date_index = None;
    }

    // Last-known prices for data-gap fill pricing (options MTM)
    let mut last_known = crate::engine::sim_types::LastKnown::new();

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
                    &options_df,
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
                        &options_df,
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
            &options_df,
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
                                    &options_df,
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
                                            &options_df,
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
                        ScriptAction::OpenOptions { legs, qty } => {
                            // Resolve unresolved legs via find_option pipeline
                            let resolved = resolve_option_legs(&legs, &options_df, today, &config);

                            if resolved.is_empty() {
                                continue; // no valid legs found
                            }

                            // Compute entry cost from resolved legs
                            let (entry_cost, script_legs, expiration) =
                                compute_options_entry(&resolved, &config);

                            let effective_qty = qty.unwrap_or(1);

                            let pos = ScriptPosition {
                                id: next_id,
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
                                    &options_df,
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

        // Update last_known prices for data-gap fill pricing
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
                    let pnl = (bar.close - *entry_price) * *qty as f64 * side.multiplier();
                    pos.unrealized_pnl = pnl;
                    unrealized += pnl;
                }
                ScriptPositionInner::Options {
                    legs, multiplier, ..
                } => {
                    // MTM each leg using PriceTable / last_known
                    let mut pos_pnl = 0.0;
                    for leg in legs.iter_mut() {
                        let current = lookup_option_price(
                            &price_table,
                            &last_known,
                            today,
                            leg.expiration,
                            leg.strike,
                            leg.option_type,
                            leg.side,
                            &config.slippage,
                        );
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
                &options_df,
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
    options_df: &Option<Arc<polars::prelude::DataFrame>>,
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
        options_df: options_df.clone(),
        config: Arc::clone(config),
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
    options_df: &Option<Arc<polars::prelude::DataFrame>>,
    today: NaiveDate,
    config: &ScriptConfig,
) -> Vec<ResolvedLeg> {
    use crate::engine::filters;

    let df = match options_df {
        Some(df) => df.as_ref(),
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
            } => Some(ResolvedLeg {
                side: *side,
                option_type: *option_type,
                strike: *strike,
                expiration: *expiration,
                bid: *bid,
                ask: *ask,
                delta: 0.0, // not available for pre-resolved legs
            }),
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

                let filtered =
                    filters::filter_leg_candidates(df, opt_code, dte_max, dte_min, 0.05).ok()?;
                let today_filtered = super::types::filter_to_date(&filtered, today)?;
                if today_filtered.height() == 0 {
                    return None;
                }
                let selected = filters::select_closest_delta(today_filtered, &target).ok()?;
                if selected.height() == 0 {
                    return None;
                }

                // Extract first row
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

                // Get expiration
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
            qty: 1,
        });
    }

    (net_cost, legs, primary_exp)
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
            // Use the latest MTM (updated each bar in Phase C)
            pos.unrealized_pnl
        }
    }
}

/// Look up current option price from PriceTable or LastKnown fallback.
/// Returns the fill price under the configured slippage model, or None.
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
                "open_options" | "open_spread" => {
                    let legs_arr = map.get("legs")?.clone().try_cast::<rhai::Array>()?;
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

                            // Check if resolved (has strike/expiration) or unresolved (has delta/dte)
                            if let Some(strike_val) = leg.get("strike") {
                                // Resolved leg
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
                                // Unresolved leg — needs delta/dte
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
                    Some(ScriptAction::OpenOptions { legs, qty })
                }
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

    async fn load_options(
        &self,
        symbol: &str,
        start: Option<NaiveDate>,
        end: Option<NaiveDate>,
    ) -> Result<polars::prelude::DataFrame>;
}

/// `DataLoader` backed by `CachedStore` — the production implementation.
///
/// Resolves symbol → Parquet path via `CachedStore::find_ohlcv`, loads the
/// DataFrame via `stock_sim::load_ohlcv_df`, and converts to `Vec<OhlcvBar>`.
pub struct CachedDataLoader {
    pub cache: Arc<crate::data::cache::CachedStore>,
}

#[async_trait::async_trait]
impl DataLoader for CachedDataLoader {
    async fn load_ohlcv(
        &self,
        symbol: &str,
        start: Option<NaiveDate>,
        end: Option<NaiveDate>,
        _interval: Interval,
    ) -> Result<Vec<OhlcvBar>> {
        let cache = Arc::clone(&self.cache);
        let symbol = symbol.to_uppercase();
        let start = start;
        let end = end;

        // Parquet I/O is blocking — run on a blocking thread
        tokio::task::spawn_blocking(move || {
            let path = cache.find_ohlcv(&symbol).ok_or_else(|| {
                anyhow::anyhow!(
                    "No OHLCV data found for '{symbol}'. \
                         Searched: etf/, stocks/, futures/, indices/ in {}",
                    cache.cache_dir().display()
                )
            })?;

            let path_str = path.to_string_lossy().to_string();
            let df = crate::engine::stock_sim::load_ohlcv_df(&path_str, start, end)?;

            ohlcv_bars_from_df(&df)
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
        let cache = Arc::clone(&self.cache);
        let symbol = symbol.to_uppercase();

        tokio::task::spawn_blocking(move || {
            // Use the DataStore trait's load_options (synchronous path within spawn_blocking)
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                use crate::data::DataStore;
                cache.load_options(&symbol, start, end).await
            })
        })
        .await
        .map_err(|e| anyhow::anyhow!("Task join error: {e}"))?
    }
}

/// Convert a Polars DataFrame (with OHLCV columns) to `Vec<OhlcvBar>`.
///
/// Reuses `stock_sim::bars_from_df` for datetime handling, then converts
/// `Bar` → `OhlcvBar` with volume (which the stock sim Bar struct lacks).
fn ohlcv_bars_from_df(df: &polars::prelude::DataFrame) -> Result<Vec<OhlcvBar>> {
    // Use the existing bars_from_df for datetime parsing (handles date vs datetime columns)
    let stock_bars = crate::engine::stock_sim::bars_from_df(df)?;

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

// ---------------------------------------------------------------------------
// Result type
// ---------------------------------------------------------------------------

/// Extended backtest result with script metadata.
pub struct ScriptBacktestResult {
    pub result: BacktestResult,
    pub metadata: Option<rhai::Map>,
    pub execution_time_ms: u64,
}
