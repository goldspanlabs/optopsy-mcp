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
    Ok(df
        .clone()
        .lazy()
        .filter(
            col(QUOTE_DATETIME_COL)
                .cast(DataType::Date)
                .gt_eq(lit(start))
                .and(col(QUOTE_DATETIME_COL).cast(DataType::Date).lt(lit(end))),
        )
        .collect()?)
}

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

    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let min_date = epoch + Days::new(min_val as u64);
    let max_date = epoch + Days::new(max_val as u64);
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
    let total_days = (max_date - min_date).num_days();
    if total_days < i64::from(train_days + test_days) {
        bail!(
            "Data spans {} days but walk-forward requires at least {} (train_days={} + test_days={})",
            total_days,
            train_days + test_days,
            train_days,
            test_days
        );
    }

    let mut windows = Vec::new();
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

        let (train_sharpe, train_pnl, train_trades, train_win_rate) = match train_result {
            Ok(r) => (
                r.metrics.sharpe,
                r.total_pnl,
                r.trade_count,
                r.metrics.win_rate,
            ),
            Err(_) => (0.0, 0.0, 0, 0.0),
        };

        let (test_sharpe, test_pnl, test_trades, test_win_rate) = match test_result {
            Ok(r) => (
                r.metrics.sharpe,
                r.total_pnl,
                r.trade_count,
                r.metrics.win_rate,
            ),
            Err(_) => (0.0, 0.0, 0, 0.0),
        };

        windows.push(WindowResult {
            window_number: window_num,
            train_start,
            train_end,
            test_start,
            test_end,
            train_sharpe,
            test_sharpe,
            train_pnl,
            test_pnl,
            train_trades,
            test_trades,
            train_win_rate,
            test_win_rate,
        });

        window_num += 1;
        cursor = cursor + Days::new(step as u64);
    }

    if windows.is_empty() {
        bail!("No valid walk-forward windows could be generated from the data");
    }

    let aggregate = compute_aggregate(&windows);
    Ok(WalkForwardResult { windows, aggregate })
}

fn compute_aggregate(windows: &[WindowResult]) -> WalkForwardAggregate {
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
        avg_test_sharpe,
        std_test_sharpe,
        avg_test_pnl,
        pct_profitable_windows,
        avg_train_test_sharpe_decay,
        total_test_pnl,
    }
}
