use std::collections::HashSet;

use anyhow::{bail, Result};
use chrono::NaiveDate;
use polars::prelude::*;

use super::evaluation;
use super::event_sim;
use super::filters;
use super::metrics;
use super::output;
use super::pricing;
use super::rules;
#[allow(clippy::wildcard_imports)]
use super::types::*;
use crate::signals;
use crate::strategies;

/// Evaluate strategy statistically — returns grouped stats by DTE/delta buckets
#[allow(clippy::too_many_lines)]
pub fn evaluate_strategy(df: &DataFrame, params: &EvaluateParams) -> Result<Vec<GroupStats>> {
    let strategy_def = strategies::find_strategy(&params.strategy)
        .ok_or_else(|| anyhow::anyhow!("Unknown strategy: {}", params.strategy))?;

    if params.leg_deltas.len() != strategy_def.legs.len() {
        bail!(
            "Strategy '{}' has {} legs but {} delta targets provided",
            params.strategy,
            strategy_def.legs.len(),
            params.leg_deltas.len()
        );
    }

    let is_multi_exp = strategy_def.is_multi_expiration();
    let base_cols: &[&str] = &["strike", "bid", "ask", "delta", "exit_bid", "exit_ask"];

    // Process each leg
    let mut leg_dfs = Vec::new();
    for (i, (leg, delta_target)) in strategy_def
        .legs
        .iter()
        .zip(params.leg_deltas.iter())
        .enumerate()
    {
        let filtered = filters::filter_option_type(df, leg.option_type.as_str())?;

        // Compute DTE
        let with_dte = filters::compute_dte(&filtered)?;

        // Filter DTE range — secondary legs get wider range to find far-term expirations
        let max_dte = if leg.expiration_cycle == ExpirationCycle::Secondary {
            params.max_entry_dte * 2
        } else {
            params.max_entry_dte
        };
        let dte_filtered = filters::filter_dte_range(&with_dte, max_dte, params.exit_dte)?;

        // Filter valid quotes
        let valid = filters::filter_valid_quotes(&dte_filtered)?;

        // Select closest delta
        let selected = filters::select_closest_delta(&valid, delta_target)?;

        // Match entry/exit
        let matched = evaluation::match_entry_exit(&selected, &with_dte, params.exit_dte)?;

        if matched.height() == 0 {
            bail!(
                "No trades found for leg {} of strategy '{}'",
                i,
                params.strategy
            );
        }

        // Select only needed columns and rename with leg index
        let prepared = if is_multi_exp {
            filters::prepare_leg_for_join_multi_exp(&matched, i, base_cols, leg.expiration_cycle)?
        } else {
            filters::prepare_leg_for_join(&matched, i, base_cols)?
        };

        leg_dfs.push((prepared, leg.expiration_cycle));
    }

    // Join all legs
    let combined = filters::join_legs(&leg_dfs, is_multi_exp)?;

    if combined.height() == 0 {
        return Ok(vec![]);
    }

    // Apply strike ordering rules
    let num_legs = strategy_def.legs.len();
    let combined = rules::filter_strike_order(
        &combined,
        num_legs,
        strategy_def.strict_strike_order,
        if is_multi_exp {
            Some(&strategy_def)
        } else {
            None
        },
    )?;

    // Calculate P&L for each trade
    let mut pnl_values = Vec::with_capacity(combined.height());
    let commission = params.commission.clone().unwrap_or_default();

    for row_idx in 0..combined.height() {
        let mut trade_pnl = 0.0;
        let mut total_contracts = 0i32;

        for (i, leg) in strategy_def.legs.iter().enumerate() {
            let bid = combined
                .column(&format!("bid_{i}"))?
                .f64()?
                .get(row_idx)
                .unwrap_or(0.0);
            let ask = combined
                .column(&format!("ask_{i}"))?
                .f64()?
                .get(row_idx)
                .unwrap_or(0.0);
            let exit_bid = combined
                .column(&format!("exit_bid_{i}"))?
                .f64()?
                .get(row_idx)
                .unwrap_or(0.0);
            let exit_ask = combined
                .column(&format!("exit_ask_{i}"))?
                .f64()?
                .get(row_idx)
                .unwrap_or(0.0);

            let leg_pl = pricing::leg_pnl(
                bid,
                ask,
                exit_bid,
                exit_ask,
                leg.side,
                &params.slippage,
                leg.qty,
                100, // default multiplier for evaluation
            );

            trade_pnl += leg_pl;
            total_contracts += leg.qty.abs();
        }

        // Apply commission
        trade_pnl -= commission.calculate(total_contracts) * 2.0; // entry + exit

        pnl_values.push(trade_pnl);
    }

    // Add P&L column and DTE/delta columns for binning
    let pnl_series = Column::new("pnl".into(), &pnl_values);
    let combined = combined.hstack(&[pnl_series])?;

    // Use leg 0's delta for binning (primary leg)
    let combined = combined
        .lazy()
        .with_column(col("delta_0").abs().alias("abs_delta"))
        .collect()?;

    // Compute DTE for binning (entry DTE)
    // For multi-expiration strategies, use primary expiration for DTE binning
    let combined = if combined.schema().contains("dte") {
        combined
    } else if is_multi_exp {
        // Add a temporary "expiration" column from "expiration_primary" for compute_dte
        let with_exp = combined
            .clone()
            .lazy()
            .with_column(col("expiration_primary").alias("expiration"))
            .collect()?;
        let with_dte = filters::compute_dte(&with_exp)?;
        with_dte.drop("expiration")?
    } else {
        filters::compute_dte(&combined)?
    };

    // Bin and aggregate
    output::bin_and_aggregate(&combined, params.dte_interval, params.delta_interval)
}

type DateFilter = Option<HashSet<NaiveDate>>;

/// Load OHLCV parquet into a `DataFrame`.
fn load_ohlcv(ohlcv_path: &str) -> Result<DataFrame> {
    let args = ScanArgsParquet::default();
    Ok(LazyFrame::scan_parquet(ohlcv_path.into(), args)?.collect()?)
}

/// Build entry/exit date filters from signal specs, loading OHLCV at most once.
fn build_signal_filters(params: &BacktestParams) -> Result<(DateFilter, DateFilter)> {
    let has_entry = params.entry_signal.is_some();
    let has_exit = params.exit_signal.is_some();

    if !has_entry && !has_exit {
        return Ok((None, None));
    }

    let ohlcv_path = params.ohlcv_path.as_deref().ok_or_else(|| {
        anyhow::anyhow!("ohlcv_path is required when entry_signal or exit_signal is set")
    })?;

    let ohlcv_df = load_ohlcv(ohlcv_path)?;

    let entry_dates = params
        .entry_signal
        .as_ref()
        .map(|spec| signals::active_dates(spec, &ohlcv_df, "date"))
        .transpose()?;
    let exit_dates = params
        .exit_signal
        .as_ref()
        .map(|spec| signals::active_dates(spec, &ohlcv_df, "date"))
        .transpose()?;

    Ok((entry_dates, exit_dates))
}

/// Run a full backtest simulation using event-driven simulation
pub fn run_backtest(df: &DataFrame, params: &BacktestParams) -> Result<BacktestResult> {
    let strategy_def = strategies::find_strategy(&params.strategy)
        .ok_or_else(|| anyhow::anyhow!("Unknown strategy: {}", params.strategy))?;

    if params.leg_deltas.len() != strategy_def.legs.len() {
        bail!(
            "Strategy '{}' has {} legs but {} delta targets provided",
            params.strategy,
            strategy_def.legs.len(),
            params.leg_deltas.len()
        );
    }

    // Build signal date filters if specified (loads OHLCV at most once)
    let (entry_dates, exit_dates) = build_signal_filters(params)?;

    let (price_table, trading_days) = event_sim::build_price_table(df)?;
    let mut candidates = event_sim::find_entry_candidates(df, &strategy_def, params)?;

    // Filter entry candidates to only dates where the entry signal is active
    if let Some(ref allowed_dates) = entry_dates {
        candidates.retain(|date, _| allowed_dates.contains(date));
    }

    let (trade_log, equity_curve, quality) = event_sim::run_event_loop(
        &price_table,
        &candidates,
        &trading_days,
        params,
        &strategy_def,
        exit_dates.as_ref(),
    );

    let perf_metrics = metrics::calculate_metrics(&equity_curve, &trade_log, params.capital)?;

    Ok(BacktestResult {
        trade_count: trade_log.len(),
        total_pnl: trade_log.iter().map(|t| t.pnl).sum(),
        metrics: perf_metrics,
        equity_curve,
        trade_log,
        quality,
    })
}

/// Compare multiple strategies
#[allow(clippy::unnecessary_wraps)]
pub fn compare_strategies(df: &DataFrame, params: &CompareParams) -> Result<Vec<CompareResult>> {
    let mut results = Vec::new();

    for entry in &params.strategies {
        let backtest_params = BacktestParams {
            strategy: entry.name.clone(),
            leg_deltas: entry.leg_deltas.clone(),
            max_entry_dte: entry.max_entry_dte,
            exit_dte: entry.exit_dte,
            slippage: entry.slippage.clone(),
            commission: entry.commission.clone(),
            stop_loss: params.sim_params.stop_loss,
            take_profit: params.sim_params.take_profit,
            max_hold_days: params.sim_params.max_hold_days,
            capital: params.sim_params.capital,
            quantity: params.sim_params.quantity,
            multiplier: params.sim_params.multiplier,
            max_positions: params.sim_params.max_positions,
            selector: params.sim_params.selector.clone(),
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
        };

        match run_backtest(df, &backtest_params) {
            Ok(bt) => {
                results.push(CompareResult {
                    strategy: entry.name.clone(),
                    trades: bt.trade_count,
                    pnl: bt.total_pnl,
                    sharpe: bt.metrics.sharpe,
                    sortino: bt.metrics.sortino,
                    max_dd: bt.metrics.max_drawdown,
                    win_rate: bt.metrics.win_rate,
                    profit_factor: bt.metrics.profit_factor,
                    calmar: bt.metrics.calmar,
                    total_return_pct: bt.metrics.total_return_pct,
                });
            }
            Err(e) => {
                tracing::warn!("Strategy '{}' failed: {}", entry.name, e);
                results.push(CompareResult {
                    strategy: entry.name.clone(),
                    trades: 0,
                    pnl: 0.0,
                    sharpe: 0.0,
                    sortino: 0.0,
                    max_dd: 0.0,
                    win_rate: 0.0,
                    profit_factor: 0.0,
                    calmar: 0.0,
                    total_return_pct: 0.0,
                });
            }
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::parquet::QUOTE_DATETIME_COL;
    use chrono::NaiveDate;

    /// Build a synthetic options `DataFrame` with 2 rows for `evaluate_strategy` tests
    /// (which still use the old `match_entry_exit` pipeline).
    fn make_synthetic_options_df() -> DataFrame {
        let quote_dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 11)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ];
        let expirations = [
            NaiveDate::from_ymd_opt(2024, 2, 16).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 16).unwrap(),
        ];

        let mut df = df! {
            QUOTE_DATETIME_COL => &quote_dates,
            "option_type" => &["call", "call"],
            "strike" => &[100.0f64, 100.0],
            "bid" => &[5.0f64, 2.0],
            "ask" => &[5.50f64, 2.50],
            "delta" => &[0.50f64, 0.30],
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations).into_column(),
        )
        .unwrap();
        df
    }

    /// Build a daily options `DataFrame` with intermediate dates for event-driven backtest.
    /// Shows price decay from entry to exit for a long call.
    fn make_daily_options_df() -> DataFrame {
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        // 6 days of data: entry at Jan 15 (DTE=32), decay through to Feb 11 (DTE=5)
        let dates: Vec<_> = vec![
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), // DTE=32
            NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(), // DTE=25
            NaiveDate::from_ymd_opt(2024, 1, 29).unwrap(), // DTE=18
            NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),  // DTE=15
            NaiveDate::from_ymd_opt(2024, 2, 5).unwrap(),  // DTE=11
            NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(), // DTE=5
        ];

        let quote_dates: Vec<_> = dates
            .iter()
            .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
            .collect();
        let expirations: Vec<_> = dates.iter().map(|_| exp).collect();

        // Simulate time decay: option losing value over time
        let bids = vec![5.00, 4.50, 3.80, 3.20, 2.60, 2.00f64];
        let asks = vec![5.50, 5.00, 4.30, 3.70, 3.10, 2.50f64];
        let deltas = vec![0.50, 0.47, 0.42, 0.38, 0.33, 0.25f64];

        let n = dates.len();
        let mut df = df! {
            QUOTE_DATETIME_COL => &quote_dates,
            "option_type" => vec!["call"; n],
            "strike" => vec![100.0f64; n],
            "bid" => &bids,
            "ask" => &asks,
            "delta" => &deltas,
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations).into_column(),
        )
        .unwrap();
        df
    }

    /// Build daily data where price drops sharply to trigger stop loss.
    fn make_stop_loss_df() -> DataFrame {
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let dates: Vec<_> = vec![
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), // DTE=32, entry
            NaiveDate::from_ymd_opt(2024, 1, 16).unwrap(), // DTE=31
            NaiveDate::from_ymd_opt(2024, 1, 17).unwrap(), // DTE=30, big drop → SL
            NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(), // DTE=25
            NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(), // DTE=5
        ];

        let quote_dates: Vec<_> = dates
            .iter()
            .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
            .collect();
        let expirations: Vec<_> = dates.iter().map(|_| exp).collect();

        // Sharp drop on day 3: entry mid=5.25, day 3 mid=1.25 → loss = 400 > 50% of 525 = 262.5
        let bids = vec![5.00, 4.00, 1.00, 0.80, 0.50f64];
        let asks = vec![5.50, 4.50, 1.50, 1.30, 1.00f64];
        let deltas = vec![0.50, 0.45, 0.15, 0.12, 0.08f64];

        let n = dates.len();
        let mut df = df! {
            QUOTE_DATETIME_COL => &quote_dates,
            "option_type" => vec!["call"; n],
            "strike" => vec![100.0f64; n],
            "bid" => &bids,
            "ask" => &asks,
            "delta" => &deltas,
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations).into_column(),
        )
        .unwrap();
        df
    }

    fn default_backtest_params() -> BacktestParams {
        BacktestParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.50,
                min: 0.20,
                max: 0.80,
            }],
            max_entry_dte: 45,
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
        }
    }

    #[test]
    fn evaluate_strategy_e2e_long_call() {
        let df = make_synthetic_options_df();
        let params = EvaluateParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.50,
                min: 0.20,
                max: 0.80,
            }],
            max_entry_dte: 45,
            exit_dte: 5,
            dte_interval: 10,
            delta_interval: 0.10,
            slippage: Slippage::Mid,
            commission: None,
        };

        let result = evaluate_strategy(&df, &params);
        assert!(
            result.is_ok(),
            "evaluate_strategy failed: {:?}",
            result.err()
        );
        let stats = result.unwrap();
        assert!(!stats.is_empty(), "Expected at least one group stat");
    }

    #[test]
    fn run_backtest_e2e_long_call() {
        let df = make_daily_options_df();
        let params = default_backtest_params();

        let result = run_backtest(&df, &params);
        assert!(result.is_ok(), "run_backtest failed: {:?}", result.err());
        let bt = result.unwrap();

        assert_eq!(bt.trade_count, 1);
        // Long call: buy at mid 5.25 on Jan 15, DTE exit triggers on Feb 11 (DTE=5)
        // Sell at mid 2.25 → loss = (2.25 - 5.25) * 100 = -300
        assert!(
            (bt.total_pnl - (-300.0)).abs() < 1.0,
            "Expected ~-300 PnL, got {}",
            bt.total_pnl
        );
        assert!(!bt.equity_curve.is_empty());
        assert_eq!(bt.trade_log.len(), 1);
        // Entry Jan 15, exit Feb 11 = 27 days
        assert_eq!(bt.trade_log[0].days_held, 27);
    }

    #[test]
    fn run_backtest_daily_equity_curve_has_all_days() {
        let df = make_daily_options_df();
        let params = default_backtest_params();

        let result = run_backtest(&df, &params).unwrap();

        // Should have one equity point per trading day (6 days)
        assert_eq!(
            result.equity_curve.len(),
            6,
            "Expected 6 equity points (one per trading day), got {}",
            result.equity_curve.len()
        );

        // First day equity should include unrealized (entry at mid 5.25, current mid 5.25 → 0 unrealized)
        assert!(
            (result.equity_curve[0].equity - 10000.0).abs() < 1.0,
            "Day 1 equity should be ~10000, got {}",
            result.equity_curve[0].equity
        );
    }

    #[test]
    fn run_backtest_e2e_with_stop_loss() {
        let df = make_stop_loss_df();
        let mut params = default_backtest_params();
        params.stop_loss = Some(0.50); // 50% stop loss

        let result = run_backtest(&df, &params);
        assert!(
            result.is_ok(),
            "run_backtest with stop loss failed: {:?}",
            result.err()
        );
        let bt = result.unwrap();
        assert_eq!(bt.trade_count, 1);
        // Stop loss fires on day 3 (Jan 17) at real market prices
        assert!(
            matches!(bt.trade_log[0].exit_type, ExitType::StopLoss),
            "Expected StopLoss exit, got {:?}",
            bt.trade_log[0].exit_type
        );
        // Exit on Jan 17 = 2 days held
        assert_eq!(bt.trade_log[0].days_held, 2);
    }

    #[test]
    fn run_backtest_unknown_strategy_errors() {
        let df = make_daily_options_df();
        let mut params = default_backtest_params();
        params.strategy = "nonexistent".to_string();

        let result = run_backtest(&df, &params);
        assert!(result.is_err());
    }

    #[test]
    fn run_backtest_wrong_leg_count_errors() {
        let df = make_daily_options_df();
        let mut params = default_backtest_params();
        params.leg_deltas = vec![]; // long_call needs 1 delta, providing 0

        let result = run_backtest(&df, &params);
        assert!(result.is_err());
    }

    #[test]
    fn run_backtest_signal_without_ohlcv_path_errors() {
        let df = make_daily_options_df();
        let mut params = default_backtest_params();
        params.entry_signal = Some(signals::registry::SignalSpec::ConsecutiveUp {
            column: "close".into(),
            count: 2,
        });
        // ohlcv_path intentionally left None
        let result = run_backtest(&df, &params);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("ohlcv_path is required"),);
    }

    /// Write a minimal OHLCV parquet to a temp file for signal tests.
    /// Returns a `TempDir` that keeps the file alive until dropped.
    fn write_ohlcv_parquet(dates: &[NaiveDate], closes: &[f64]) -> (tempfile::TempDir, String) {
        let n = dates.len();
        let mut df = df! {
            "open" => vec![100.0f64; n],
            "high" => vec![105.0f64; n],
            "low" => vec![95.0f64; n],
            "close" => closes,
            "adjclose" => closes,
            "volume" => vec![1_000_000i64; n],
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("date"), dates.to_vec()).into_column(),
        )
        .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ohlcv.parquet");
        let file = std::fs::File::create(&path).unwrap();
        polars::prelude::ParquetWriter::new(file)
            .finish(&mut df)
            .unwrap();
        let path_str = path.to_string_lossy().to_string();
        (dir, path_str)
    }

    #[test]
    fn run_backtest_entry_signal_filters_candidates() {
        let df = make_daily_options_df();
        // Options dates: Jan 15, 22, 29, Feb 1, 5, 11 — all are entry candidates (DTE > exit_dte=5)
        //
        // OHLCV: closes decline throughout, so ConsecutiveUp(2) never fires.
        // All entry candidates should be blocked → 0 trades.
        let ohlcv_dates: Vec<NaiveDate> = vec![
            NaiveDate::from_ymd_opt(2024, 1, 11).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 29).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 5).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(),
        ];
        // Monotonically decreasing → ConsecutiveUp(2) never fires
        let closes = vec![107.0, 106.0, 105.0, 104.0, 103.0, 102.0, 101.0];
        let (_dir, path) = write_ohlcv_parquet(&ohlcv_dates, &closes);

        let mut params = default_backtest_params();
        params.entry_signal = Some(signals::registry::SignalSpec::ConsecutiveUp {
            column: "close".into(),
            count: 2,
        });
        params.ohlcv_path = Some(path);

        let result = run_backtest(&df, &params).unwrap();
        // All entry dates blocked since close never goes up twice in a row
        assert_eq!(result.trade_count, 0);

        // Verify baseline without signal would have produced a trade
        let mut baseline = default_backtest_params();
        baseline.entry_signal = None;
        let baseline_result = run_backtest(&df, &baseline).unwrap();
        assert!(
            baseline_result.trade_count > 0,
            "Baseline without signal should produce trades"
        );
    }

    #[test]
    fn run_backtest_exit_signal_triggers_early_close() {
        let df = make_daily_options_df();
        // Options dates: Jan 15, 22, 29, Feb 1, 5, 11
        // Without exit signal, trade closes on Feb 11 (DTE=5 exit).
        // With exit signal on Jan 29, trade should close there instead.
        let ohlcv_dates: Vec<NaiveDate> = vec![
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 29).unwrap(), // exit signal fires here
            NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 5).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(),
        ];
        // ConsecutiveUp(2): fires when 2 consecutive up closes
        // Make close go up on Jan 22 and Jan 29 → signal fires on Jan 29
        let closes = vec![100.0, 101.0, 102.0, 99.0, 98.0, 97.0];
        let (_dir, path) = write_ohlcv_parquet(&ohlcv_dates, &closes);

        let mut params = default_backtest_params();
        params.max_positions = 1; // prevent re-entry after signal exit
        params.exit_signal = Some(signals::registry::SignalSpec::ConsecutiveUp {
            column: "close".into(),
            count: 2,
        });
        params.ohlcv_path = Some(path);

        let result = run_backtest(&df, &params).unwrap();
        // First trade: entry Jan 15, signal exit Jan 29 (ConsecutiveUp fires)
        // With max_positions=1, a second trade may open after exit.
        // Verify the first trade was closed by signal.
        assert!(
            result.trade_count >= 1,
            "Expected at least 1 trade, got {}",
            result.trade_count
        );
        assert!(
            matches!(result.trade_log[0].exit_type, ExitType::Signal),
            "Expected Signal exit on first trade, got {:?}",
            result.trade_log[0].exit_type
        );
        // Entry Jan 15, exit Jan 29 = 14 days
        assert_eq!(result.trade_log[0].days_held, 14);
    }

    #[test]
    fn run_backtest_spread_strategy() {
        // Build data for a bull call spread: long call at lower strike, short call at higher
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let dates = [
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(),
        ];

        // Two strikes per date: 100 and 105
        let mut quote_dates = Vec::new();
        let mut expirations_vec = Vec::new();
        let mut option_types = Vec::new();
        let mut strikes = Vec::new();
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        let mut deltas = Vec::new();

        // Strike 100 data
        let bids_100 = [5.00, 4.00, 2.00f64];
        let asks_100 = [5.50, 4.50, 2.50f64];
        let deltas_100 = [0.50, 0.42, 0.25f64];

        // Strike 105 data
        let bids_105 = [3.00, 2.20, 1.00f64];
        let asks_105 = [3.50, 2.70, 1.50f64];
        let deltas_105 = [0.35, 0.28, 0.15f64];

        for (i, date) in dates.iter().enumerate() {
            // Strike 100
            quote_dates.push(date.and_hms_opt(0, 0, 0).unwrap());
            expirations_vec.push(exp);
            option_types.push("call");
            strikes.push(100.0f64);
            bids.push(bids_100[i]);
            asks.push(asks_100[i]);
            deltas.push(deltas_100[i]);

            // Strike 105
            quote_dates.push(date.and_hms_opt(0, 0, 0).unwrap());
            expirations_vec.push(exp);
            option_types.push("call");
            strikes.push(105.0f64);
            bids.push(bids_105[i]);
            asks.push(asks_105[i]);
            deltas.push(deltas_105[i]);
        }

        let mut df = df! {
            QUOTE_DATETIME_COL => &quote_dates,
            "option_type" => &option_types,
            "strike" => &strikes,
            "bid" => &bids,
            "ask" => &asks,
            "delta" => &deltas,
        }
        .unwrap();
        df.with_column(
            DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations_vec)
                .into_column(),
        )
        .unwrap();

        let params = BacktestParams {
            strategy: "bull_call_spread".to_string(),
            leg_deltas: vec![
                TargetRange {
                    target: 0.50,
                    min: 0.20,
                    max: 0.80,
                },
                TargetRange {
                    target: 0.35,
                    min: 0.10,
                    max: 0.60,
                },
            ],
            max_entry_dte: 45,
            exit_dte: 5,
            slippage: Slippage::Mid,
            commission: None,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10000.0,
            quantity: 1,
            multiplier: 100,
            max_positions: 5,
            selector: TradeSelector::First,
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
        };

        let result = run_backtest(&df, &params);
        assert!(
            result.is_ok(),
            "run_backtest for spread failed: {:?}",
            result.err()
        );
        let bt = result.unwrap();
        assert_eq!(bt.trade_count, 1);
        // Both legs should be present in the trade
        assert_eq!(bt.trade_log.len(), 1);
    }
}
