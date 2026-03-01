use anyhow::{bail, Result};
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
use crate::data::parquet::QUOTE_DATETIME_COL;
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
        let option_type_str = match leg.option_type {
            OptionType::Call => "call",
            OptionType::Put => "put",
        };

        // Filter by option type
        let filtered = filters::filter_option_type(df, option_type_str)?;

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
            filters::prepare_leg_for_join_multi_exp(
                &matched,
                i,
                base_cols,
                leg.expiration_cycle,
            )?
        } else {
            filters::prepare_leg_for_join(&matched, i, base_cols)?
        };

        leg_dfs.push((prepared, leg.expiration_cycle));
    }

    // Join all legs
    let combined = if is_multi_exp {
        join_multi_expiration_legs(&leg_dfs)?
    } else {
        let mut combined = leg_dfs[0].0.clone();
        for (leg_df, _) in leg_dfs.iter().skip(1) {
            let join_cols: Vec<&str> = vec![QUOTE_DATETIME_COL, "expiration"];
            combined = combined
                .lazy()
                .join(
                    leg_df.clone().lazy(),
                    join_cols.iter().map(|c| col(*c)).collect::<Vec<_>>(),
                    join_cols.iter().map(|c| col(*c)).collect::<Vec<_>>(),
                    JoinArgs::new(JoinType::Inner),
                )
                .collect()?;
        }
        combined
    };

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

    let (price_table, trading_days) = event_sim::build_price_table(df)?;
    let candidates = event_sim::find_entry_candidates(df, &strategy_def, params)?;
    let (trade_log, equity_curve) = event_sim::run_event_loop(
        &price_table,
        &candidates,
        &trading_days,
        params,
        &strategy_def,
    );

    let perf_metrics = metrics::calculate_metrics(&equity_curve, params.capital)?;

    Ok(BacktestResult {
        trade_count: trade_log.len(),
        total_pnl: trade_log.iter().map(|t| t.pnl).sum(),
        metrics: perf_metrics,
        equity_curve,
        trade_log,
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
        };

        match run_backtest(df, &backtest_params) {
            Ok(bt) => {
                results.push(CompareResult {
                    strategy: entry.name.clone(),
                    trades: bt.trade_count,
                    pnl: bt.total_pnl,
                    sharpe: bt.metrics.sharpe,
                    max_dd: bt.metrics.max_drawdown,
                    win_rate: bt.metrics.win_rate,
                });
            }
            Err(e) => {
                tracing::warn!("Strategy '{}' failed: {}", entry.name, e);
                results.push(CompareResult {
                    strategy: entry.name.clone(),
                    trades: 0,
                    pnl: 0.0,
                    sharpe: 0.0,
                    max_dd: 0.0,
                    win_rate: 0.0,
                });
            }
        }
    }

    Ok(results)
}

/// Join legs for multi-expiration strategies (calendar/diagonal).
///
/// Primary and secondary legs are joined separately within their groups on
/// `(quote_datetime, expiration_<cycle>)`, then cross-joined on `quote_datetime`
/// with a filter ensuring `expiration_secondary > expiration_primary`.
fn join_multi_expiration_legs(
    leg_dfs: &[(DataFrame, ExpirationCycle)],
) -> Result<DataFrame> {
    let mut primary_dfs: Vec<&DataFrame> = Vec::new();
    let mut secondary_dfs: Vec<&DataFrame> = Vec::new();

    for (df, cycle) in leg_dfs {
        match cycle {
            ExpirationCycle::Primary => primary_dfs.push(df),
            ExpirationCycle::Secondary => secondary_dfs.push(df),
        }
    }

    // Join within primary group
    let mut primary = primary_dfs[0].clone();
    for df in primary_dfs.iter().skip(1) {
        let join_cols: Vec<&str> = vec![QUOTE_DATETIME_COL, "expiration_primary"];
        primary = primary
            .lazy()
            .join(
                (*df).clone().lazy(),
                join_cols.iter().map(|c| col(*c)).collect::<Vec<_>>(),
                join_cols.iter().map(|c| col(*c)).collect::<Vec<_>>(),
                JoinArgs::new(JoinType::Inner),
            )
            .collect()?;
    }

    // Join within secondary group
    let mut secondary = secondary_dfs[0].clone();
    for df in secondary_dfs.iter().skip(1) {
        let join_cols: Vec<&str> = vec![QUOTE_DATETIME_COL, "expiration_secondary"];
        secondary = secondary
            .lazy()
            .join(
                (*df).clone().lazy(),
                join_cols.iter().map(|c| col(*c)).collect::<Vec<_>>(),
                join_cols.iter().map(|c| col(*c)).collect::<Vec<_>>(),
                JoinArgs::new(JoinType::Inner),
            )
            .collect()?;
    }

    // Cross-join on quote_datetime, then filter expiration_secondary > expiration_primary
    let combined = primary
        .lazy()
        .join(
            secondary.lazy(),
            vec![col(QUOTE_DATETIME_COL)],
            vec![col(QUOTE_DATETIME_COL)],
            JoinArgs::new(JoinType::Inner),
        )
        .filter(col("expiration_secondary").gt(col("expiration_primary")))
        .collect()?;

    Ok(combined)
}

/// Intermediate trade representation used by legacy exits/simulator modules
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct RawTrade {
    pub entry_datetime: chrono::NaiveDateTime,
    pub exit_datetime: chrono::NaiveDateTime,
    pub entry_cost: f64,
    pub exit_proceeds: f64,
    pub pnl: f64,
    pub days_held: i64,
    pub exit_type: ExitType,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    /// Build a synthetic options DataFrame with 2 rows for evaluate_strategy tests
    /// (which still use the old match_entry_exit pipeline).
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

    /// Build a daily options DataFrame with intermediate dates for event-driven backtest.
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
    fn run_backtest_spread_strategy() {
        // Build data for a bull call spread: long call at lower strike, short call at higher
        let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();
        let dates = vec![
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
