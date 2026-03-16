//! Math and derived-feature functions: abs, change, pct_change, roc, rel_volume, zscore,
//! range_pct, if.

use polars::prelude::*;

use super::helpers::{extract_col_period, extract_three_cols, FuncArg};

pub fn build(name: &str, args: Vec<FuncArg>) -> Result<Expr, String> {
    match name {
        "abs" => {
            if args.len() != 1 {
                return Err("abs() takes exactly 1 argument".to_string());
            }
            let expr = args.into_iter().next().unwrap().into_expr();
            Ok(expr.abs())
        }
        "change" => {
            let (col_expr, period) = extract_col_period(&args, "change")?;
            let shifted = col_expr.clone().shift(lit(period as i64));
            Ok(col_expr - shifted)
        }
        "pct_change" => {
            let (col_expr, period) = extract_col_period(&args, "pct_change")?;
            let shifted = col_expr.clone().shift(lit(period as i64));
            Ok((col_expr - shifted.clone()) / shifted)
        }
        "roc" => {
            let (col_expr, period) = extract_col_period(&args, "roc")?;
            let shifted = col_expr.clone().shift(lit(period as i64));
            Ok((col_expr - shifted.clone()) / shifted * lit(100.0))
        }
        "rel_volume" => {
            let (col_expr, period) = extract_col_period(&args, "rel_volume")?;
            let sma_expr = col_expr.clone().rolling_mean(RollingOptionsFixedWindow {
                window_size: period,
                min_periods: period,
                ..Default::default()
            });
            Ok(col_expr / sma_expr)
        }
        "zscore" => {
            let (col_expr, period) = extract_col_period(&args, "zscore")?;
            let mean = col_expr.clone().rolling_mean(RollingOptionsFixedWindow {
                window_size: period,
                min_periods: period,
                ..Default::default()
            });
            let std_dev = col_expr.clone().rolling_std(RollingOptionsFixedWindow {
                window_size: period,
                min_periods: period,
                ..Default::default()
            });
            Ok((col_expr - mean) / std_dev)
        }
        "range_pct" => {
            let (close_e, high_e, low_e) = extract_three_cols(&args, "range_pct")?;
            let range = high_e - low_e.clone();
            let pct = (close_e - low_e) / range.clone();
            Ok(when(range.neq(lit(0.0))).then(pct).otherwise(lit(NULL)))
        }
        "if" => {
            if args.len() != 3 {
                return Err(
                    "if() takes exactly 3 arguments: (condition, then_value, else_value)"
                        .to_string(),
                );
            }
            let cond = args[0].clone().into_expr();
            let then_val = args[1].clone().into_expr();
            let else_val = args[2].clone().into_expr();
            Ok(when(cond).then(then_val).otherwise(else_val))
        }
        _ => Err(format!("math: unknown function '{name}'")),
    }
}
