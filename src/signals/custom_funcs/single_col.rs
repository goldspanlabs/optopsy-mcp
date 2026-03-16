//! Single-column map functions: `rsi`, `macd_hist`, `macd_signal`, `macd_line`, `rank`, `iv_rank`,
//! `cci`, `ppo`, `cmo`.

use polars::prelude::*;
use rust_ti::momentum_indicators::bulk as mti;
use rust_ti::standard_indicators::bulk as sti;

use super::helpers::{
    compute_iv_rank, compute_rolling_rank, extract_col_period, extract_col_two_periods,
    extract_single_col, FuncArg,
};
use crate::signals::helpers::pad_series;

#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
pub fn build(name: &str, args: Vec<FuncArg>) -> Result<Expr, String> {
    match name {
        "rsi" => {
            let (col_expr, period) = extract_col_period(&args, "rsi")?;
            Ok(col_expr.map(
                move |col: Column| {
                    let ca = col.as_materialized_series().f64()?;
                    let n = ca.len();
                    let vals: Vec<f64> = ca
                        .into_iter()
                        .map(|opt_v| opt_v.unwrap_or(f64::NAN))
                        .collect();
                    if n <= period {
                        return Ok(Series::new("rsi".into(), vec![f64::NAN; n]).into());
                    }
                    let rsi_vals = mti::relative_strength_index(
                        &vals,
                        rust_ti::ConstantModelType::SmoothedMovingAverage,
                        period,
                    );
                    let padded = pad_series(&rsi_vals, n);
                    Ok(Series::new("rsi".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        "macd_hist" => {
            let col_expr = extract_single_col(&args, "macd_hist")?;
            Ok(col_expr.map(
                move |col: Column| {
                    let ca = col.as_materialized_series().f64()?;
                    let n = ca.len();
                    let vals: Vec<f64> = ca
                        .into_iter()
                        .map(|opt_v| opt_v.unwrap_or(f64::NAN))
                        .collect();
                    if n < 34 {
                        return Ok(Series::new("macd_hist".into(), vec![f64::NAN; n]).into());
                    }
                    let macd_values = sti::macd(&vals);
                    let histograms: Vec<f64> = macd_values.iter().map(|t| t.2).collect();
                    let padded = pad_series(&histograms, n);
                    Ok(Series::new("macd_hist".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        "macd_signal" => {
            let col_expr = extract_single_col(&args, "macd_signal")?;
            Ok(col_expr.map(
                move |col: Column| {
                    let ca = col.as_materialized_series().f64()?;
                    let n = ca.len();
                    let vals: Vec<f64> = ca
                        .into_iter()
                        .map(|opt_v| opt_v.unwrap_or(f64::NAN))
                        .collect();
                    if n < 34 {
                        return Ok(Series::new("macd_signal".into(), vec![f64::NAN; n]).into());
                    }
                    let macd_values = sti::macd(&vals);
                    let signals: Vec<f64> = macd_values.iter().map(|t| t.1).collect();
                    let padded = pad_series(&signals, n);
                    Ok(Series::new("macd_signal".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        "macd_line" => {
            let col_expr = extract_single_col(&args, "macd_line")?;
            Ok(col_expr.map(
                move |col: Column| {
                    let ca = col.as_materialized_series().f64()?;
                    let n = ca.len();
                    let vals: Vec<f64> = ca
                        .into_iter()
                        .map(|opt_v| opt_v.unwrap_or(f64::NAN))
                        .collect();
                    if n < 34 {
                        return Ok(Series::new("macd_line".into(), vec![f64::NAN; n]).into());
                    }
                    let macd_values = sti::macd(&vals);
                    let lines: Vec<f64> = macd_values.iter().map(|t| t.0).collect();
                    let padded = pad_series(&lines, n);
                    Ok(Series::new("macd_line".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        "rank" => {
            let (col_expr, period) = extract_col_period(&args, "rank")?;
            Ok(col_expr.map(
                move |col: Column| {
                    let ca = col.as_materialized_series().f64()?;
                    let n = ca.len();
                    let vals: Vec<f64> = ca
                        .into_iter()
                        .map(|opt_v| opt_v.unwrap_or(f64::NAN))
                        .collect();
                    if n < period {
                        return Ok(Series::new("rank".into(), vec![f64::NAN; n]).into());
                    }
                    let rank_vals = compute_rolling_rank(&vals, period);
                    let padded = pad_series(&rank_vals, n);
                    Ok(Series::new("rank".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        "iv_rank" => {
            let (col_expr, period) = extract_col_period(&args, "iv_rank")?;
            Ok(col_expr.map(
                move |col: Column| {
                    let ca = col.as_materialized_series().f64()?;
                    let vals: Vec<f64> = ca
                        .into_iter()
                        .map(|opt_v| opt_v.unwrap_or(f64::NAN))
                        .collect();
                    let n = vals.len();
                    if n < period {
                        return Ok(Series::new("iv_rank".into(), vec![f64::NAN; n]).into());
                    }
                    let rank_vals = compute_iv_rank(&vals, period);
                    let padded = pad_series(&rank_vals, n);
                    Ok(Series::new("iv_rank".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        "cci" => {
            let (col_expr, period) = extract_col_period(&args, "cci")?;
            Ok(col_expr.map(
                move |col: Column| {
                    let ca = col.as_materialized_series().f64()?;
                    let n = ca.len();
                    let vals: Vec<f64> = ca.into_iter().map(|v| v.unwrap_or(f64::NAN)).collect();
                    if n <= period {
                        return Ok(Series::new("cci".into(), vec![f64::NAN; n]).into());
                    }
                    let cci_vals = mti::commodity_channel_index(
                        &vals,
                        rust_ti::ConstantModelType::SimpleMovingAverage,
                        rust_ti::DeviationModel::MeanAbsoluteDeviation,
                        0.015,
                        period,
                    );
                    let padded = pad_series(&cci_vals, n);
                    Ok(Series::new("cci".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        "ppo" => {
            let (col_expr, short_period, long_period) = extract_col_two_periods(&args, "ppo")?;
            Ok(col_expr.map(
                move |col: Column| {
                    let ca = col.as_materialized_series().f64()?;
                    let n = ca.len();
                    let vals: Vec<f64> = ca.into_iter().map(|v| v.unwrap_or(f64::NAN)).collect();
                    if n <= long_period {
                        return Ok(Series::new("ppo".into(), vec![f64::NAN; n]).into());
                    }
                    let ppo_vals = mti::percentage_price_oscillator(
                        &vals,
                        short_period,
                        long_period,
                        rust_ti::ConstantModelType::ExponentialMovingAverage,
                    );
                    let padded = pad_series(&ppo_vals, n);
                    Ok(Series::new("ppo".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        "cmo" => {
            let (col_expr, period) = extract_col_period(&args, "cmo")?;
            Ok(col_expr.map(
                move |col: Column| {
                    let ca = col.as_materialized_series().f64()?;
                    let n = ca.len();
                    let vals: Vec<f64> = ca.into_iter().map(|v| v.unwrap_or(f64::NAN)).collect();
                    if n <= period {
                        return Ok(Series::new("cmo".into(), vec![f64::NAN; n]).into());
                    }
                    let cmo_vals = mti::chande_momentum_oscillator(&vals, period);
                    let padded = pad_series(&cmo_vals, n);
                    Ok(Series::new("cmo".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        _ => Err(format!("single_col: unknown function '{name}'")),
    }
}
