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
pub async fn run_script_backtest(
    script_source: &str,
    params: &HashMap<String, serde_json::Value>,
    data_loader: &dyn DataLoader,
) -> Result<ScriptBacktestResult> {
    let backtest_start = std::time::Instant::now();

    // 1. Compile
    let mut engine = build_engine();

    // Register extern() with captured params for runtime resolution (3-arg)
    let params_clone = params.clone();
    engine.register_fn(
        "extern",
        move |name: &str, default: Dynamic, _desc: &str| -> Dynamic {
            if let Some(value) = params_clone.get(name) {
                super::stdlib::json_to_dynamic(value)
            } else if default.is_unit() {
                Dynamic::from(format!("ERROR: Required parameter '{name}' not provided"))
            } else {
                default
            }
        },
    );

    // Register extern() 4-arg overload (with options array — ignored at runtime)
    let params_clone4 = params.clone();
    engine.register_fn(
        "extern",
        move |name: &str, default: Dynamic, _desc: &str, _opts: rhai::Array| -> Dynamic {
            if let Some(value) = params_clone4.get(name) {
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

    // 4. Load data
    let mut early_warnings: Vec<String> = Vec::new();

    let ohlcv_df = data_loader
        .load_ohlcv(&config.symbol, config.start_date, config.end_date)
        .await?;

    if ohlcv_df.height() == 0 {
        bail!("No OHLCV data found for symbol '{}'", config.symbol);
    }

    // 4a. Resample to daily if needed.
    // Detect intraday data by checking the time gap between the first two bars:
    // a gap < 1 day means the data is sub-daily.
    let data_is_intraday = is_intraday_data(&ohlcv_df);
    let needs_daily =
        (config.interval == Interval::Daily && data_is_intraday) || config.needs_options;

    let ohlcv_df = if needs_daily && data_is_intraday {
        let original_rows = ohlcv_df.height();
        let resampled =
            crate::engine::ohlcv::resample_ohlcv(&ohlcv_df, crate::engine::types::Interval::Daily)?;
        if config.needs_options && config.interval != Interval::Daily {
            early_warnings.push(format!(
                "Options require daily data; resampled {} intraday ({:?}) bars to {} daily bars",
                original_rows,
                config.interval,
                resampled.height()
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

    let config = Arc::new(config);

    // 5. Pre-compute indicators
    let indicator_store = Arc::new(IndicatorStore::build(&config.declared_indicators, &bars)?);

    // 6. Run main simulation loop
    let price_history = Arc::new(bars);
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

    // Load options data if needed + build PriceTable for MTM
    let options_by_date: Option<Arc<DatePartitionedOptions>>;
    let price_table: Option<Arc<crate::engine::sim_types::PriceTable>>;
    let date_index: Option<Arc<crate::engine::sim_types::DateIndex>>;

    if config.needs_options {
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
    } else {
        options_by_date = None;
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
    let mut pnl_history: Vec<f64> = Vec::new();
    let mut equity_curve: Vec<EquityPoint> = Vec::new();
    let mut warnings: Vec<String> = early_warnings;
    let mut realized_equity = config.capital;
    let mut next_id = 1usize;
    let mut last_entry_date: Option<NaiveDate> = None;
    let mut stop_requested = false;
    let loop_start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(config.timeout_secs);
    let mut pnl_history_arc = Arc::new(Vec::<f64>::new());
    let mut pnl_dirty = false;

    let ctx_factory = BarContextFactory {
        indicator_store: Arc::clone(&indicator_store),
        price_history: Arc::clone(&price_history),
        cross_symbol_data: Arc::clone(&cross_symbol_data),
        config: Arc::clone(&config),
        options_by_date: options_by_date.clone(),
    };

    for (bar_idx, bar) in price_history.iter().enumerate() {
        if stop_requested {
            break;
        }

        // Wall-clock timeout check (every 100 bars to minimize overhead)
        if bar_idx % 100 == 0 && loop_start.elapsed() > timeout {
            warnings.push(format!(
                "Backtest exceeded {}s timeout at bar {bar_idx}",
                config.timeout_secs
            ));
            break;
        }

        if pnl_dirty {
            pnl_history_arc = Arc::new(pnl_history.clone());
            pnl_dirty = false;
        }

        let today = bar.datetime.date();

        // --- Phase A: Exits (immediate processing) ---

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
                    exit_reason = classify_expiration(legs, bar.close);
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
                let pnl = compute_close_pnl(&closed_pos, bar);
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

                // Add to trade log
                trade_log.push(build_script_trade_record(
                    &closed_pos,
                    bar.datetime,
                    pnl,
                    &exit_reason,
                ));
                pnl_history.push(pnl);
                pnl_dirty = true;

                positions.swap_remove(i);
                positions_dirty = true; // positions changed, Arc needs rebuild

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
                                        unrealized_pnl: (bar.close - leg.strike)
                                            * f64::from(shares),
                                        days_held: 0,
                                        current_date: today,
                                        source: "assignment".to_string(),
                                        implicit: true,
                                        group: closed_pos.group.clone(),
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

        // --- Phase B: Entries (sees post-exit state) ---

        // Build ONE context for Phase B (reused by on_bar and callbacks)
        let phase_b_positions = Arc::new(positions.clone());
        let ctx = ctx_factory.build(
            bar,
            bar_idx,
            &phase_b_positions,
            realized_equity,
            &pnl_history_arc,
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
                                current_date: today,
                                source: "script".to_string(),
                                implicit: false,
                                group: read_group(&scope),
                            };
                            // Deduct entry commission for stock
                            realized_equity -= compute_commission(&config.commission, &pos);
                            next_id += 1;
                            last_entry_date = Some(today);

                            if has_on_position_opened {
                                // Reuse Phase B positions Arc for callback
                                let ctx = ctx_factory.build(
                                    bar,
                                    bar_idx,
                                    &phase_b_positions,
                                    realized_equity,
                                    &pnl_history_arc,
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
                                    let exit_comm =
                                        compute_commission(&config.commission, &positions[idx]);
                                    realized_equity += pnl - exit_comm;

                                    if has_on_position_closed {
                                        // Reuse Phase B positions Arc for callback
                                        let ctx = ctx_factory.build(
                                            bar,
                                            bar_idx,
                                            &phase_b_positions,
                                            realized_equity,
                                            &pnl_history_arc,
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
                                    pnl_history.push(pnl);
                                    pnl_dirty = true;
                                    positions.swap_remove(idx);
                                } else {
                                    warnings
                                        .push(format!("Close action: position_id {pid} not found"));
                                }
                            }
                        }
                        ScriptAction::OpenOptions { legs, qty } => {
                            // Resolve unresolved legs via find_option pipeline
                            let resolved =
                                resolve_option_legs(&legs, &options_by_date, today, &config);

                            if resolved.is_empty() {
                                continue; // no valid legs found
                            }

                            let effective_qty = qty.unwrap_or(1);

                            // Compute entry cost from resolved legs
                            let (entry_cost, script_legs, expiration) =
                                compute_options_entry(&resolved, &config, effective_qty);

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
                                current_date: today,
                                source: "script".to_string(),
                                implicit: false,
                                group: read_group(&scope),
                            };
                            // Deduct entry commission for options
                            realized_equity -= compute_commission(&config.commission, &pos);
                            next_id += 1;
                            last_entry_date = Some(today);

                            if has_on_position_opened {
                                // Reuse Phase B positions Arc for callback
                                let ctx = ctx_factory.build(
                                    bar,
                                    bar_idx,
                                    &phase_b_positions,
                                    realized_equity,
                                    &pnl_history_arc,
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
                        ScriptAction::Hold => {}
                    }
                }
            }
            Err(e) => {
                warnings.push(format!("on_bar error on bar {bar_idx}: {e}"));
            }
        }

        // --- Phase C: Bookkeeping ---

        // Update days_held and current_date for all open positions
        for pos in &mut positions {
            pos.days_held = (today - pos.entry_date).num_days();
            pos.current_date = today;
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
            unrealized: Some(unrealized),
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
                pnl_history.push(pnl);
            }
        }
    }

    // Call on_end(ctx) — may return metadata
    let metadata = if has_on_end {
        let end_positions_arc = Arc::new(positions.clone());
        let pnl_history_arc = Arc::new(pnl_history.clone());
        let ctx = if let Some(last_bar) = price_history.last() {
            ctx_factory.build(
                last_bar,
                price_history.len() - 1,
                &end_positions_arc,
                realized_equity,
                &pnl_history_arc,
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
}

impl BarContextFactory {
    fn build(
        &self,
        bar: &OhlcvBar,
        bar_idx: usize,
        positions_arc: &Arc<Vec<ScriptPosition>>,
        equity: f64,
        pnl_history: &Arc<Vec<f64>>,
    ) -> BarContext {
        let cash = equity - positions_arc.iter().map(|p| p.unrealized_pnl).sum::<f64>();
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
            config: Arc::clone(&self.config),
            pnl_history: Arc::clone(pnl_history),
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
            } => Some(ResolvedLeg {
                side: *side,
                option_type: *option_type,
                strike: *strike,
                expiration: *expiration,
                bid: *bid,
                ask: *ask,
                delta: 0.0,
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
/// (updated in Phase C) and `entry_price`, including the contract multiplier.
/// Both `leg.current_price` and `leg.entry_price` are per-contract premiums
/// (e.g., $2.50 for a $2.50 premium option); the contract multiplier (typically
/// 100) converts them to per-position dollar P&L.
/// Note: Phase A closes happen before Phase C MTM update, so `current_price`
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
            is_stock: true,
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
fn parse_bar_actions(result: &Dynamic) -> Vec<ScriptAction> {
    let Some(arr) = result.clone().try_cast::<rhai::Array>() else {
        return vec![];
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
                    // Support two shapes:
                    // 1. #{ action: "open_options", legs: [...] }
                    // 2. #{ action: "open_spread", spread: #{ legs: [...], ... } }
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
    ) -> Result<polars::prelude::DataFrame>;

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
/// DataFrame via `stock_sim::load_ohlcv_df`.
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
    pub fn new(cache: Arc<crate::data::cache::CachedStore>) -> Self {
        Self {
            inner: CachedDataLoader { cache },
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
pub struct ScriptBacktestResult {
    pub result: BacktestResult,
    pub metadata: Option<rhai::Map>,
    pub execution_time_ms: u64,
    /// Pre-computed indicator series used by the script, keyed by declaration
    /// (e.g., "sma:20", "rsi:14"). Each value is aligned to the bar index.
    /// The FE can plot these on the chart to show what the script used.
    pub indicator_data: HashMap<String, Vec<f64>>,
}

/// A single indicator series for JSON serialization in the response.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct IndicatorSeries {
    pub name: String,
    pub values: Vec<f64>,
}
