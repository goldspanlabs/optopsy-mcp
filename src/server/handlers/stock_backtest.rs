//! Handler body for the `run_stock_backtest` tool.

use crate::data::cache::validate_path_segment;
use crate::tools;
use crate::tools::response_types::StockBacktestResponse;

use super::super::params::{tool_err, RunStockBacktestParams};
use super::super::OptopsyServer;

/// Execute the `run_stock_backtest` tool logic.
///
/// Resolves parameters, runs the stock backtest, and loads chart prices.
#[allow(clippy::too_many_lines)]
pub async fn execute(
    server: &OptopsyServer,
    params: RunStockBacktestParams,
) -> Result<StockBacktestResponse, String> {
    let symbol = params.symbol.to_uppercase();
    validate_path_segment(&symbol).map_err(|e| format!("Invalid symbol: {e}"))?;

    tracing::info!(
        symbol = %symbol,
        side = ?params.side.unwrap_or(crate::engine::types::Side::Long),
        "Stock backtest request received"
    );

    // Ensure OHLCV data is available (also used for chart overlay)
    let ohlcv_path = server.ensure_ohlcv(&symbol)?;

    // Extract chart indicator formulas for cross-symbol resolution
    let chart_indicators_entry =
        crate::signals::helpers::extract_chart_indicators(Some(&params.entry_signal));
    let mut chart_formulas: Vec<String> = chart_indicators_entry
        .iter()
        .map(|(f, _)| f.clone())
        .collect();
    let chart_indicators_exit =
        crate::signals::helpers::extract_chart_indicators(params.exit_signal.as_ref());
    chart_formulas.extend(chart_indicators_exit.iter().map(|(f, _)| f.clone()));

    // Resolve cross-symbol OHLCV paths for signals
    let cross_ohlcv_paths = server.resolve_cross_ohlcv_paths(
        Some(&params.entry_signal),
        params.exit_signal.as_ref(),
        &[],
        &[],
        &chart_formulas,
    )?;

    let start_date = params
        .start_date
        .as_deref()
        .map(|s| {
            chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|_| format!("Invalid start_date \"{s}\": expected YYYY-MM-DD"))
        })
        .transpose()?;
    let end_date = params
        .end_date
        .as_deref()
        .map(|s| {
            chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|_| format!("Invalid end_date \"{s}\": expected YYYY-MM-DD"))
        })
        .transpose()?;

    let interval = params.interval.unwrap_or_default();
    // Save date strings before params fields are moved into stock_params
    let fallback_start = params.start_date.clone();
    let fallback_end = params.end_date.clone();

    let stock_params = crate::engine::stock_sim::StockBacktestParams {
        symbol: symbol.clone(),
        side: params.side.unwrap_or(crate::engine::types::Side::Long),
        capital: params.capital,
        quantity: params.quantity,
        sizing: params.sizing,
        max_positions: params.max_positions,
        slippage: params.slippage,
        commission: params.commission,
        stop_loss: params.stop_loss,
        take_profit: params.take_profit,
        max_hold_days: params.max_hold_days,
        max_hold_bars: params.max_hold_bars,
        min_days_between_entries: params.min_days_between_entries,
        min_bars_between_entries: params.min_bars_between_entries,
        conflict_resolution: params.conflict_resolution.unwrap_or_default(),
        entry_signal: Some(params.entry_signal),
        exit_signal: params.exit_signal,
        ohlcv_path: Some(ohlcv_path),
        cross_ohlcv_paths,
        start_date,
        end_date,
        interval,
        session_filter: params.session_filter,
    };

    // Run the backtest first
    let backtest_result =
        tokio::task::spawn_blocking(move || tools::stock_backtest::execute(&stock_params, vec![]))
            .await
            .map_err(|e| format!("Stock backtest task panicked: {e}"))?
            .map_err(tool_err)?;

    // Load chart prices via the same path as get_raw_prices,
    // bounded to the trade date range (first entry → last exit).
    let trade_log = &backtest_result.trade_log;
    let (chart_start, chart_end) = if trade_log.is_empty() {
        // No trades — fall back to the backtest date range
        (fallback_start, fallback_end)
    } else {
        let first_entry = trade_log
            .iter()
            .map(|t| t.entry_datetime)
            .min()
            .map(|dt| dt.format("%Y-%m-%d").to_string());
        let last_exit = trade_log
            .iter()
            .map(|t| t.exit_datetime)
            .max()
            .map(|dt| dt.format("%Y-%m-%d").to_string());
        (first_entry, last_exit)
    };

    let cache = server.cache.clone();
    let chart_symbol = symbol.clone();
    let underlying_prices = tokio::task::spawn_blocking(move || {
        // Use a runtime handle to call the async load_and_execute
        let rt = tokio::runtime::Handle::current();
        rt.block_on(tools::raw_prices::load_and_execute(
            &cache,
            &chart_symbol,
            chart_start.as_deref(),
            chart_end.as_deref(),
            Some(2000), // cap to keep response size manageable
            interval,
            Some(true), // tail: most recent bars covering trades
        ))
    })
    .await
    .map_err(|e| format!("Price load task panicked: {e}"))?
    .map(|resp| {
        // Convert PriceBar → UnderlyingPrice
        resp.prices
            .into_iter()
            .map(|b| tools::response_types::UnderlyingPrice {
                date: b.date,
                open: b.open,
                high: b.high,
                low: b.low,
                close: b.close,
                volume: Some(b.volume),
            })
            .collect::<Vec<_>>()
    })
    .unwrap_or_default();

    // Attach prices to the backtest result
    let mut result = backtest_result;
    result.underlying_prices = underlying_prices;
    Ok(result)
}
