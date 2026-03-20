//! Handler body for the `run_options_backtest` tool.

use crate::data::parquet::DATETIME_COL;
use crate::tools;
use crate::tools::response_types::BacktestResponse;

use super::super::{load_underlying_prices, OptopsyServer};

/// Execute the `run_options_backtest` tool logic.
///
/// Resolves parameters, loads underlying prices for chart overlay,
/// and runs the backtest on a blocking thread.
pub async fn execute(
    server: &OptopsyServer,
    base: super::super::params::BacktestBaseParams,
) -> Result<BacktestResponse, String> {
    let (symbol, df, backtest_params) = server.resolve_backtest_params(base).await?;

    // Strategies with undefined max loss + sizing require stop_loss
    if backtest_params.sizing.is_some() && backtest_params.stop_loss.is_none() {
        if let Some(strategy_def) = crate::strategies::find_strategy(&backtest_params.strategy) {
            let all_short = strategy_def
                .legs
                .iter()
                .all(|l| l.side == crate::engine::types::Side::Short);
            if all_short {
                return Err(
                    "Dynamic sizing with an all-short strategy (naked, straddle, strangle) \
                     requires `stop_loss` to compute max loss per contract. Add a stop_loss value."
                        .to_string(),
                );
            }
        }
    }

    // Load underlying OHLCV close prices for chart overlay, auto-fetching if needed.
    // Filter to only timestamps present in the options data so we return ~600
    // bars instead of millions of intraday bars.
    let ohlcv_path = server.ensure_ohlcv(&symbol).ok();
    let dt_filter = df.column(DATETIME_COL).ok().cloned();
    let underlying_prices = match ohlcv_path {
        Some(path_str) => {
            let path = std::path::PathBuf::from(path_str);
            tokio::task::spawn_blocking(move || -> Vec<tools::response_types::UnderlyingPrice> {
                load_underlying_prices(&path, dt_filter.as_ref(), None, None)
            })
            .await
            .unwrap_or_default()
        }
        None => vec![],
    };

    // Run backtest on a blocking thread — the engine performs synchronous
    // Polars I/O (scan_parquet) which conflicts with the tokio runtime.
    tokio::task::spawn_blocking(move || {
        // Load OHLCV data for indicator charting (if signals need it)
        let ohlcv_df = backtest_params
            .ohlcv_path
            .as_deref()
            .and_then(|path| crate::engine::stock_sim::load_ohlcv_df(path, None, None).ok());
        tools::backtest::execute(&df, &backtest_params, underlying_prices, ohlcv_df.as_ref())
    })
    .await
    .map_err(|e| format!("Backtest task panicked: {e}"))?
    .map_err(super::super::params::tool_err)
}
