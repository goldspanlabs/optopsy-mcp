use anyhow::{bail, Result};
use chrono::{Days, NaiveDate};
use polars::prelude::*;

use crate::data::parquet::QUOTE_DATETIME_COL;
use crate::engine::core::run_backtest;
use crate::engine::types::BacktestParams;

/// Result of a single walk-forward window (train + test).
#[derive(Debug, Clone)]
pub struct WindowResult {
    pub window_number: usize,
    pub train_start: NaiveDate,
    pub train_end: NaiveDate,
    pub test_start: NaiveDate,
    pub test_end: NaiveDate,
    pub train_sharpe: f64,
    pub test_sharpe: f64,
    pub train_pnl: f64,
    pub test_pnl: f64,
    pub train_trades: usize,
    pub test_trades: usize,
    pub train_win_rate: f64,
    pub test_win_rate: f64,
}

/// Aggregate statistics across all walk-forward windows.
#[derive(Debug, Clone)]
pub struct WalkForwardAggregate {
    pub total_windows: usize,
    pub failed_windows: usize,
    pub avg_test_sharpe: f64,
    pub std_test_sharpe: f64,
    pub avg_test_pnl: f64,
    pub pct_profitable_windows: f64,
    pub avg_train_test_sharpe_decay: f64,
    pub total_test_pnl: f64,
}

/// Full walk-forward analysis result.
#[derive(Debug, Clone)]
pub struct WalkForwardResult {
    pub windows: Vec<WindowResult>,
    pub aggregate: WalkForwardAggregate,
}

/// Filter a `DataFrame` to rows within `[start, end)` by calendar date.
fn slice_by_date_range(df: &DataFrame, start: NaiveDate, end: NaiveDate) -> Result<DataFrame> {
    let date_expr = col(QUOTE_DATETIME_COL).cast(DataType::Date);
    Ok(df
        .clone()
        .lazy()
        .filter(
            date_expr
                .clone()
                .gt_eq(lit(start))
                .and(date_expr.lt(lit(end))),
        )
        .collect()?)
}

// Polars `Date` is stored as i32 days since 1970-01-01.
// This is the number of days from 0001-01-01 to 1970-01-01.
const CE_OFFSET: i64 = 719_163;

/// Get the min and max dates from the `DataFrame`.
fn date_range(df: &DataFrame) -> Result<(NaiveDate, NaiveDate)> {
    let stats = df
        .clone()
        .lazy()
        .select([
            col(QUOTE_DATETIME_COL)
                .cast(DataType::Date)
                .min()
                .alias("min_date"),
            col(QUOTE_DATETIME_COL)
                .cast(DataType::Date)
                .max()
                .alias("max_date"),
        ])
        .collect()?;

    let min_val = stats
        .column("min_date")?
        .date()?
        .phys
        .get(0)
        .ok_or_else(|| anyhow::anyhow!("empty date column"))?;
    let max_val = stats
        .column("max_date")?
        .date()?
        .phys
        .get(0)
        .ok_or_else(|| anyhow::anyhow!("empty date column"))?;

    // Convert Polars signed days-since-epoch to Chrono CE days and validate range.
    let min_ce_days = i64::from(min_val) + CE_OFFSET;
    if min_ce_days < 1 || min_ce_days > i64::from(i32::MAX) {
        return Err(anyhow::anyhow!(
            "min_date out of range for NaiveDate: {min_val} (CE days: {min_ce_days})"
        ));
    }
    let min_date = NaiveDate::from_num_days_from_ce_opt(min_ce_days as i32)
        .ok_or_else(|| anyhow::anyhow!("invalid min_date value: {min_val}"))?;

    let max_ce_days = i64::from(max_val) + CE_OFFSET;
    if max_ce_days < 1 || max_ce_days > i64::from(i32::MAX) {
        return Err(anyhow::anyhow!(
            "max_date out of range for NaiveDate: {max_val} (CE days: {max_ce_days})"
        ));
    }
    let max_date = NaiveDate::from_num_days_from_ce_opt(max_ce_days as i32)
        .ok_or_else(|| anyhow::anyhow!("invalid max_date value: {max_val}"))?;

    Ok((min_date, max_date))
}

/// Run walk-forward analysis: rolling train/test windows across the data.
pub fn run_walk_forward(
    df: &DataFrame,
    params: &BacktestParams,
    train_days: i32,
    test_days: i32,
    step_days: Option<i32>,
) -> Result<WalkForwardResult> {
    if train_days < 1 {
        bail!("train_days must be >= 1");
    }
    if test_days < 1 {
        bail!("test_days must be >= 1");
    }
    let step = step_days.unwrap_or(test_days);
    if step < 1 {
        bail!("step_days must be >= 1");
    }

    let (min_date, max_date) = date_range(df)?;
    // Use the inclusive day span (both endpoints counted) when validating data length.
    let total_days_inclusive = (max_date - min_date).num_days() + 1;
    if total_days_inclusive < i64::from(train_days + test_days) {
        bail!(
            "Data spans {} days but walk-forward requires at least {} (train_days={} + test_days={})",
            total_days_inclusive,
            train_days + test_days,
            train_days,
            test_days
        );
    }

    let mut windows = Vec::new();
    let mut failed_count = 0usize;
    let mut cursor = min_date + Days::new(train_days as u64);
    let mut window_num = 1usize;

    while cursor + Days::new(test_days as u64) <= max_date + Days::new(1) {
        let train_start = cursor - Days::new(train_days as u64);
        let train_end = cursor;
        let test_start = cursor;
        let test_end = cursor + Days::new(test_days as u64);

        let train_df = slice_by_date_range(df, train_start, train_end)?;
        let test_df = slice_by_date_range(df, test_start, test_end)?;

        if train_df.height() == 0 || test_df.height() == 0 {
            cursor = cursor + Days::new(step as u64);
            continue;
        }

        let train_result = run_backtest(&train_df, params);
        let test_result = run_backtest(&test_df, params);

        match (train_result, test_result) {
            (Ok(train_r), Ok(test_r)) => {
                windows.push(WindowResult {
                    window_number: window_num,
                    train_start,
                    train_end,
                    test_start,
                    test_end,
                    train_sharpe: train_r.metrics.sharpe,
                    test_sharpe: test_r.metrics.sharpe,
                    train_pnl: train_r.total_pnl,
                    test_pnl: test_r.total_pnl,
                    train_trades: train_r.trade_count,
                    test_trades: test_r.trade_count,
                    train_win_rate: train_r.metrics.win_rate,
                    test_win_rate: test_r.metrics.win_rate,
                });
                window_num += 1;
            }
            _ => {
                failed_count += 1;
            }
        }

        cursor = cursor + Days::new(step as u64);
    }

    if windows.is_empty() {
        bail!("No valid walk-forward windows could be generated from the data");
    }

    let aggregate = compute_aggregate(&windows, failed_count);
    Ok(WalkForwardResult { windows, aggregate })
}

fn compute_aggregate(windows: &[WindowResult], failed_windows: usize) -> WalkForwardAggregate {
    let n = windows.len() as f64;
    let test_sharpes: Vec<f64> = windows.iter().map(|w| w.test_sharpe).collect();
    let test_pnls: Vec<f64> = windows.iter().map(|w| w.test_pnl).collect();

    let avg_test_sharpe = test_sharpes.iter().sum::<f64>() / n;
    let std_test_sharpe = {
        let variance = test_sharpes
            .iter()
            .map(|s| (s - avg_test_sharpe).powi(2))
            .sum::<f64>()
            / n;
        variance.sqrt()
    };
    let avg_test_pnl = test_pnls.iter().sum::<f64>() / n;
    let profitable = windows.iter().filter(|w| w.test_pnl > 0.0).count();
    let pct_profitable_windows = (profitable as f64 / n) * 100.0;

    let decays: Vec<f64> = windows
        .iter()
        .map(|w| w.train_sharpe - w.test_sharpe)
        .collect();
    let avg_train_test_sharpe_decay = decays.iter().sum::<f64>() / n;

    let total_test_pnl = test_pnls.iter().sum::<f64>();

    WalkForwardAggregate {
        total_windows: windows.len(),
        failed_windows,
        avg_test_sharpe,
        std_test_sharpe,
        avg_test_pnl,
        pct_profitable_windows,
        avg_train_test_sharpe_decay,
        total_test_pnl,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use polars::prelude::{DatetimeChunked, PlSmallStr};
    use std::collections::HashMap;

    /// Build a minimal `DataFrame` with a `quote_datetime` Datetime column
    /// covering the specified inclusive date range (one row per day).
    fn make_df_for_dates(start: NaiveDate, end: NaiveDate) -> DataFrame {
        let mut dates = Vec::new();
        let mut d = start;
        while d <= end {
            dates.push(d.and_hms_opt(0, 0, 0).unwrap());
            d += Duration::days(1);
        }
        let n = dates.len();
        let dt_chunked = DatetimeChunked::new(PlSmallStr::from(QUOTE_DATETIME_COL), &dates);
        DataFrame::new(n, vec![dt_chunked.into_series().into()])
            .expect("failed to build test DataFrame")
    }

    fn make_params() -> crate::engine::types::BacktestParams {
        use crate::engine::types::{
            BacktestParams, DteRange, ExpirationFilter, Slippage, TradeSelector,
        };
        BacktestParams {
            strategy: "short_put".to_string(),
            leg_deltas: vec![],
            entry_dte: DteRange {
                target: 45,
                min: 30,
                max: 60,
            },
            exit_dte: 0,
            slippage: Slippage::default(),
            commission: None,
            min_bid_ask: 0.05,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10_000.0,
            quantity: 1,
            multiplier: 100,
            max_positions: 1,
            selector: TradeSelector::default(),
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::default(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::default(),
            exit_net_delta: None,
        }
    }

    #[test]
    fn get_date_range_returns_correct_bounds() {
        let start = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2020, 12, 31).unwrap();
        let df = make_df_for_dates(start, end);
        let (mn, mx) = date_range(&df).unwrap();
        assert_eq!(mn, start);
        assert_eq!(mx, end);
    }

    #[test]
    fn get_date_range_handles_pre_epoch_dates() {
        // Dates before 1970-01-01 produce negative Polars Date values (signed i32).
        let start = NaiveDate::from_ymd_opt(1960, 6, 15).unwrap();
        let end = NaiveDate::from_ymd_opt(1960, 12, 31).unwrap();
        let df = make_df_for_dates(start, end);
        let (mn, mx) = date_range(&df).unwrap();
        assert_eq!(mn, start);
        assert_eq!(mx, end);
    }

    #[test]
    fn slice_by_date_range_respects_half_open_interval() {
        let start = NaiveDate::from_ymd_opt(2021, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2021, 1, 10).unwrap();
        let df = make_df_for_dates(start, end);

        let from = NaiveDate::from_ymd_opt(2021, 1, 3).unwrap();
        let to = NaiveDate::from_ymd_opt(2021, 1, 7).unwrap(); // exclusive

        let sliced = slice_by_date_range(&df, from, to).unwrap();
        // Should include Jan 3, 4, 5, 6 (not Jan 7)
        assert_eq!(sliced.height(), 4);
    }

    #[test]
    fn insufficient_data_returns_error() {
        // 10 days of data, but train_days + test_days = 25
        let start = NaiveDate::from_ymd_opt(2021, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2021, 1, 10).unwrap();
        let df = make_df_for_dates(start, end);

        let params = make_params();
        let result = run_walk_forward(&df, &params, 15, 10, None);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("walk-forward requires at least"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn exact_data_span_is_accepted() {
        // total_days_inclusive == train_days + test_days should pass span validation.
        // 5 days inclusive (Jan 1..Jan 5), train=3, test=2 => needs exactly 5.
        let start = NaiveDate::from_ymd_opt(2021, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2021, 1, 5).unwrap();
        let df = make_df_for_dates(start, end);

        let params = make_params();
        // Span check should pass — the result may fail with "No valid walk-forward windows"
        // (not enough options data), but NOT with the span-check error.
        let result = run_walk_forward(&df, &params, 3, 2, None);
        if let Err(e) = &result {
            assert!(
                !e.to_string().contains("walk-forward requires at least"),
                "span check should pass for exact size, but got: {e}"
            );
        }
    }
}
