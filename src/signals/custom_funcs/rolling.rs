//! Rolling window functions: `sma`, `ema`, `std`, `max`, `min`, `bbands_mid`, `bbands_upper`, `bbands_lower`.

use polars::prelude::*;

use super::helpers::{extract_col_period, FuncArg};

pub fn build(name: &str, args: &[FuncArg]) -> Result<Expr, String> {
    match name {
        "sma" => {
            let (col_expr, period) = extract_col_period(args, "sma")?;
            Ok(col_expr.rolling_mean(RollingOptionsFixedWindow {
                window_size: period,
                min_periods: period,
                ..Default::default()
            }))
        }
        "ema" => {
            let (col_expr, period) = extract_col_period(args, "ema")?;
            let alpha = 2.0f64 / (period as f64 + 1.0);
            Ok(col_expr.ewm_mean(EWMOptions {
                alpha,
                adjust: true,
                bias: false,
                min_periods: period,
                ignore_nulls: true,
            }))
        }
        "std" => {
            let (col_expr, period) = extract_col_period(args, "std")?;
            Ok(col_expr.rolling_std(RollingOptionsFixedWindow {
                window_size: period,
                min_periods: period,
                ..Default::default()
            }))
        }
        "max" => {
            let (col_expr, period) = extract_col_period(args, "max")?;
            Ok(col_expr.rolling_max(RollingOptionsFixedWindow {
                window_size: period,
                min_periods: period,
                ..Default::default()
            }))
        }
        "min" => {
            let (col_expr, period) = extract_col_period(args, "min")?;
            Ok(col_expr.rolling_min(RollingOptionsFixedWindow {
                window_size: period,
                min_periods: period,
                ..Default::default()
            }))
        }
        "bbands_mid" => {
            let (col_expr, period) = extract_col_period(args, "bbands_mid")?;
            Ok(col_expr.rolling_mean(RollingOptionsFixedWindow {
                window_size: period,
                min_periods: period,
                ..Default::default()
            }))
        }
        "bbands_upper" => {
            let (col_expr, period) = extract_col_period(args, "bbands_upper")?;
            let sma = col_expr.clone().rolling_mean(RollingOptionsFixedWindow {
                window_size: period,
                min_periods: period,
                ..Default::default()
            });
            let std_dev = col_expr.rolling_std(RollingOptionsFixedWindow {
                window_size: period,
                min_periods: period,
                ..Default::default()
            });
            Ok(sma + lit(2.0) * std_dev)
        }
        "bbands_lower" => {
            let (col_expr, period) = extract_col_period(args, "bbands_lower")?;
            let sma = col_expr.clone().rolling_mean(RollingOptionsFixedWindow {
                window_size: period,
                min_periods: period,
                ..Default::default()
            });
            let std_dev = col_expr.rolling_std(RollingOptionsFixedWindow {
                window_size: period,
                min_periods: period,
                ..Default::default()
            });
            Ok(sma - lit(2.0) * std_dev)
        }
        _ => Err(format!("rolling: unknown function '{name}'")),
    }
}
