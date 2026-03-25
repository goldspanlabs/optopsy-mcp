//! Handler body for the `compare_strategies` tool.

use std::collections::HashMap;

use garde::Validate;

use crate::data::cache::validate_path_segment;
use crate::engine::types::{CompareEntry, CompareParams};
use crate::signals::registry::collect_cross_symbols;
use crate::tools;
use crate::tools::response_types::CompareResponse;

use super::super::params::{resolve_leg_deltas, tool_err, CompareStrategiesParams};
use super::super::OptopsyServer;

/// Execute the `compare_strategies` tool logic (stock mode).
async fn compare_stock(
    server: &OptopsyServer,
    params: CompareStrategiesParams,
) -> Result<CompareResponse, String> {
    let stock_entries = params
        .stock_entries
        .ok_or_else(|| "stock_entries is required when mode is \"stock\" (min 2)".to_string())?;
    if stock_entries.len() < 2 {
        return Err("stock_entries requires at least 2 entries for comparison".to_string());
    }

    let symbol = params
        .symbol
        .as_deref()
        .ok_or("symbol is required for stock mode")?
        .to_uppercase();
    validate_path_segment(&symbol).map_err(|e| format!("Invalid symbol: {e}"))?;
    let ohlcv_path = server.ensure_ohlcv(&symbol)?;

    // Collect all signals for cross-symbol resolution
    let all_sigs: Vec<&crate::signals::registry::SignalSpec> = stock_entries
        .iter()
        .map(|e| &e.entry_signal)
        .chain(stock_entries.iter().filter_map(|e| e.exit_signal.as_ref()))
        .collect();
    // Resolve cross symbols from the stock entry signals
    let mut cross_paths = HashMap::new();
    for sig in &all_sigs {
        for sym in collect_cross_symbols(sig) {
            if let std::collections::hash_map::Entry::Vacant(e) = cross_paths.entry(sym) {
                validate_path_segment(e.key())
                    .map_err(|err| format!("Invalid cross-symbol \"{}\": {err}", e.key()))?;
                let path = server.ensure_ohlcv(e.key())?;
                e.insert(path);
            }
        }
    }

    // Build StockCompareEntry for each entry
    let entries: Vec<crate::engine::core::StockCompareEntry> = stock_entries
        .into_iter()
        .enumerate()
        .map(|(idx, e)| {
            let label = e.label.unwrap_or_else(|| {
                let sig_label = crate::engine::sweep_analysis::signal_spec_label(&e.entry_signal);
                format!("Entry {}: {sig_label}", idx + 1)
            });
            let side = e.side.unwrap_or(crate::engine::types::Side::Long);
            let interval = e.interval.unwrap_or_default();
            crate::engine::core::StockCompareEntry {
                label,
                params: crate::engine::stock_sim::StockBacktestParams {
                    symbol: symbol.clone(),
                    side,
                    capital: params.sim_params.capital,
                    quantity: params.sim_params.quantity,
                    sizing: params.sim_params.sizing.clone(),
                    max_positions: params.sim_params.max_positions,
                    slippage: e.slippage,
                    commission: e.commission,
                    stop_loss: e.stop_loss.or(params.sim_params.stop_loss),
                    take_profit: e.take_profit.or(params.sim_params.take_profit),
                    max_hold_days: params.sim_params.max_hold_days,
                    max_hold_bars: params.sim_params.max_hold_bars,
                    min_days_between_entries: params.sim_params.min_days_between_entries,
                    min_bars_between_entries: params.sim_params.min_bars_between_entries,
                    conflict_resolution: params.sim_params.conflict_resolution.unwrap_or_default(),
                    entry_signal: Some(e.entry_signal),
                    exit_signal: e.exit_signal,
                    ohlcv_path: Some(ohlcv_path.clone()),
                    cross_ohlcv_paths: cross_paths.clone(),
                    start_date: None,
                    end_date: None,
                    interval,
                    session_filter: e.session_filter,
                },
            }
        })
        .collect();

    tokio::task::spawn_blocking(move || tools::compare::execute_stock(&entries))
        .await
        .map_err(|e| format!("Stock compare task panicked: {e}"))?
        .map_err(tool_err)
}

/// Execute the `compare_strategies` tool logic (options mode).
async fn compare_options(
    server: &OptopsyServer,
    params: CompareStrategiesParams,
) -> Result<CompareResponse, String> {
    let strategies = params.strategies.ok_or_else(|| {
        "strategies is required when mode is \"options\" (or omitted)".to_string()
    })?;

    let (symbol, df) = server.ensure_data_loaded(params.symbol.as_deref()).await?;

    let any_stock_leg = strategies
        .iter()
        .any(|s| crate::strategies::find_strategy(&s.name).is_some_and(|def| def.has_stock_leg));
    let ohlcv_path =
        if params.entry_signal.is_some() || params.exit_signal.is_some() || any_stock_leg {
            Some(server.ensure_ohlcv(&symbol)?)
        } else {
            None
        };

    let cross_ohlcv_paths = server.resolve_cross_ohlcv_paths(
        params.entry_signal.as_ref(),
        params.exit_signal.as_ref(),
        &[],
        &[],
        &[],
    )?;

    let mut sim_params = params.sim_params;
    sim_params.entry_signal = params.entry_signal;
    sim_params.exit_signal = params.exit_signal;
    sim_params.ohlcv_path = ohlcv_path;
    sim_params.cross_ohlcv_paths = cross_ohlcv_paths;

    let compare_params = CompareParams {
        strategies: strategies
            .into_iter()
            .map(|s| {
                let leg_deltas = resolve_leg_deltas(s.leg_deltas, &s.name)?;
                Ok(CompareEntry {
                    name: s.name,
                    leg_deltas,
                    entry_dte: s.entry_dte,
                    exit_dte: s.exit_dte,
                    slippage: s.slippage,
                    commission: s.commission,
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
        sim_params,
    };

    compare_params
        .validate()
        .map_err(|e| super::super::params::validation_err("compare_strategies", e))?;

    tokio::task::spawn_blocking(move || tools::compare::execute(&df, &compare_params))
        .await
        .map_err(|e| format!("Compare task panicked: {e}"))?
        .map_err(tool_err)
}

/// Execute the `compare_strategies` tool logic, dispatching to stock or options mode.
pub async fn execute(
    server: &OptopsyServer,
    params: CompareStrategiesParams,
) -> Result<CompareResponse, String> {
    let is_stock = params.mode.as_deref() == Some("stock");

    tracing::info!(
        mode = if is_stock { "stock" } else { "options" },
        symbol = params.symbol.as_deref().unwrap_or("auto"),
        "Compare strategies request received"
    );

    if is_stock {
        compare_stock(server, params).await
    } else {
        compare_options(server, params).await
    }
}
