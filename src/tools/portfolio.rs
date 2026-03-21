//! Portfolio backtest orchestrator.
//!
//! Runs multiple stock backtests, combines their equity curves, computes
//! cross-strategy correlation, and delegates formatting to `ai_format::portfolio`.

use anyhow::Result;
use std::collections::HashMap;

use crate::engine::metrics::calculate_metrics;
use crate::engine::portfolio::{
    align_return_streams, combine_equity_curves, compute_contributions, compute_correlation_matrix,
    extract_daily_returns,
};
use crate::engine::stock_sim::{self, StockBacktestParams};
use crate::engine::types::{BacktestResult, Interval, Side, Slippage};
use crate::signals::registry::SignalSpec;
use crate::tools::ai_helpers::compute_trade_summary;
use crate::tools::response_types::{PortfolioBacktestResponse, PortfolioStrategyResult};

use super::ai_format;

/// Resolved data for a single strategy ready for execution.
pub struct ResolvedStrategy {
    pub symbol: String,
    pub side: Option<Side>,
    pub entry_signal: SignalSpec,
    pub exit_signal: Option<SignalSpec>,
    pub allocation_pct: f64,
    pub quantity: i32,
    pub stop_loss: Option<f64>,
    pub take_profit: Option<f64>,
    pub max_hold_days: Option<i32>,
    pub slippage: Option<Slippage>,
    pub ohlcv_path: String,
    pub cross_ohlcv_paths: HashMap<String, String>,
    pub start_date: Option<chrono::NaiveDate>,
    pub end_date: Option<chrono::NaiveDate>,
}

/// Execute the portfolio backtest: run each strategy, combine results, format response.
#[allow(clippy::too_many_lines, clippy::needless_pass_by_value)]
pub fn execute(
    strategies: Vec<ResolvedStrategy>,
    capital: f64,
    default_slippage: &Slippage,
) -> Result<PortfolioBacktestResponse> {
    let start = std::time::Instant::now();

    let mut strategy_results: Vec<PortfolioStrategyResult> = Vec::new();
    let mut equity_curves: Vec<(Vec<crate::engine::types::EquityPoint>, f64)> = Vec::new();
    let mut daily_return_streams: Vec<Vec<(chrono::NaiveDate, f64)>> = Vec::new();
    let mut labels: Vec<String> = Vec::new();
    let mut pnl_values: Vec<f64> = Vec::new();
    let mut backtest_results: Vec<BacktestResult> = Vec::new();

    for resolved in &strategies {
        let side = resolved.side.unwrap_or(Side::Long);
        let slippage = resolved
            .slippage
            .clone()
            .unwrap_or_else(|| default_slippage.clone());
        let allocation_frac = resolved.allocation_pct / 100.0;
        let strategy_capital = capital * allocation_frac;

        let label = format!(
            "{} {} {}",
            resolved.symbol,
            match side {
                Side::Long => "Long",
                Side::Short => "Short",
            },
            format_signal_short(&resolved.entry_signal),
        );

        let stock_params = StockBacktestParams {
            symbol: resolved.symbol.clone(),
            side,
            capital: strategy_capital,
            quantity: resolved.quantity,
            sizing: None,
            max_positions: 1,
            slippage,
            commission: None,
            stop_loss: resolved.stop_loss,
            take_profit: resolved.take_profit,
            max_hold_days: resolved.max_hold_days,
            max_hold_bars: None,
            min_days_between_entries: None,
            min_bars_between_entries: None,
            conflict_resolution: crate::engine::types::ConflictResolution::default(),
            entry_signal: Some(resolved.entry_signal.clone()),
            exit_signal: resolved.exit_signal.clone(),
            ohlcv_path: Some(resolved.ohlcv_path.clone()),
            cross_ohlcv_paths: resolved.cross_ohlcv_paths.clone(),
            start_date: resolved.start_date,
            end_date: resolved.end_date,
            interval: Interval::Daily,
            session_filter: None,
        };

        // Load OHLCV and run the backtest
        let ohlcv_df =
            stock_sim::load_ohlcv_df(&resolved.ohlcv_path, resolved.start_date, resolved.end_date)?;
        let ohlcv_df = stock_sim::resample_ohlcv(&ohlcv_df, Interval::Daily)?;
        let bars = stock_sim::bars_from_df(&ohlcv_df)?;
        // Derive cache_dir from ohlcv_path ({cache_dir}/{category}/{SYMBOL}.parquet)
        let cache_dir = std::path::Path::new(&resolved.ohlcv_path)
            .parent()
            .and_then(|p| p.parent());
        let (entry_dates, exit_dates) =
            stock_sim::build_stock_signal_filters(&stock_params, &ohlcv_df, cache_dir)?;

        let result = stock_sim::run_stock_backtest(
            &bars,
            &stock_params,
            entry_dates.as_ref(),
            exit_dates.as_ref(),
        )?;

        let m = &result.metrics;
        let trade_summary = compute_trade_summary(&result.trade_log, m);
        let side_label = match side {
            Side::Long => "Long",
            Side::Short => "Short",
        };

        let daily_returns = extract_daily_returns(&result.equity_curve);
        daily_return_streams.push(daily_returns);
        equity_curves.push((result.equity_curve.clone(), allocation_frac));
        labels.push(label.clone());
        pnl_values.push(result.total_pnl);

        strategy_results.push(PortfolioStrategyResult {
            label,
            symbol: resolved.symbol.clone(),
            side: side_label.to_string(),
            allocation_pct: resolved.allocation_pct,
            metrics: result.metrics.clone(),
            trade_summary,
            contribution_pct: 0.0, // filled after all strategies run
            total_pnl: result.total_pnl,
        });

        backtest_results.push(result);
    }

    // Compute contributions
    let contributions = compute_contributions(&pnl_values, &labels);
    for (i, (_, pct)) in contributions.iter().enumerate() {
        strategy_results[i].contribution_pct = *pct;
    }

    // Compute correlation matrix
    let aligned = align_return_streams(&daily_return_streams);
    let correlation_matrix = compute_correlation_matrix(&aligned, &labels);

    // Combine equity curves
    let combined_curve = combine_equity_curves(&equity_curves, capital);

    // Compute portfolio-level metrics on the combined curve
    // Build a synthetic trade log by merging all individual trade logs
    let mut all_trades = Vec::new();
    for result in &backtest_results {
        all_trades.extend(result.trade_log.clone());
    }
    all_trades.sort_by_key(|t| t.entry_datetime);

    let portfolio_metrics = calculate_metrics(&combined_curve, &all_trades, capital, 252.0)?;

    super::macros::log_elapsed!(
        start,
        "Portfolio backtest finished",
        strategies = strategies.len()
    );

    Ok(ai_format::format_portfolio(
        capital,
        portfolio_metrics,
        strategy_results,
        correlation_matrix,
        combined_curve,
    ))
}

/// Generate a short display string for a signal spec.
fn format_signal_short(signal: &SignalSpec) -> String {
    let s = format!("{signal:?}");
    if s.len() > 40 {
        format!("{}...", &s[..37])
    } else {
        s
    }
}
