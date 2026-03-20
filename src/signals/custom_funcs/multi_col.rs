//! Multi-column functions: `atr`, `stochastic`, `keltner_upper`, `keltner_lower`, `obv`, `mfi`, `tr`, `cmf`.

// Multi-column map closures use conventional short names (s, c, h, l, v, n)
#![allow(clippy::many_single_char_names)]

use polars::prelude::*;

use super::helpers::{
    extract_four_cols_period, extract_three_cols, extract_three_cols_period,
    extract_three_cols_period_mult, extract_two_cols, FuncArg,
};
use crate::signals::helpers::pad_series;
use crate::signals::volatility::{compute_atr, compute_keltner_channel};
use crate::signals::volume::{compute_cmf, compute_typical_price};

#[allow(clippy::too_many_lines)]
pub fn build(name: &str, args: &[FuncArg]) -> Result<Expr, String> {
    match name {
        "atr" => {
            let (close_expr, high_expr, low_expr, period) = extract_three_cols_period(args, "atr")?;
            Ok(as_struct(vec![
                close_expr.alias("__c"),
                high_expr.alias("__h"),
                low_expr.alias("__l"),
            ])
            .map(
                move |col: Column| {
                    let s = col.as_materialized_series();
                    let ca = s.struct_()?;
                    let close_s = ca.field_by_name("__c")?;
                    let high_s = ca.field_by_name("__h")?;
                    let low_s = ca.field_by_name("__l")?;
                    let c: Vec<f64> = close_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let h: Vec<f64> = high_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let l: Vec<f64> = low_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let atr_vals = compute_atr(&c, &h, &l, period);
                    let padded = pad_series(&atr_vals, s.len());
                    Ok(Series::new("atr".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        "stochastic" => {
            let (close_expr, high_expr, low_expr, period) =
                extract_three_cols_period(args, "stochastic")?;
            Ok(as_struct(vec![
                close_expr.alias("__c"),
                high_expr.alias("__h"),
                low_expr.alias("__l"),
            ])
            .map(
                move |col: Column| {
                    let s = col.as_materialized_series();
                    let ca = s.struct_()?;
                    let close_s = ca.field_by_name("__c")?;
                    let high_s = ca.field_by_name("__h")?;
                    let low_s = ca.field_by_name("__l")?;
                    let c: Vec<f64> = close_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let h: Vec<f64> = high_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let l: Vec<f64> = low_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let stoch_vals =
                        crate::signals::momentum::compute_stochastic(&c, &h, &l, period);
                    let padded = pad_series(&stoch_vals, s.len());
                    Ok(Series::new("stochastic".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        "keltner_upper" => {
            let (close_expr, high_expr, low_expr, period, mult) =
                extract_three_cols_period_mult(args, "keltner_upper")?;
            Ok(as_struct(vec![
                close_expr.alias("__c"),
                high_expr.alias("__h"),
                low_expr.alias("__l"),
            ])
            .map(
                move |col: Column| {
                    let s = col.as_materialized_series();
                    let ca = s.struct_()?;
                    let close_s = ca.field_by_name("__c")?;
                    let high_s = ca.field_by_name("__h")?;
                    let low_s = ca.field_by_name("__l")?;
                    let c: Vec<f64> = close_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let h: Vec<f64> = high_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let l: Vec<f64> = low_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let (_, upper) = compute_keltner_channel(&c, &h, &l, period, mult);
                    let padded = pad_series(&upper, s.len());
                    Ok(Series::new("keltner_upper".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        "keltner_lower" => {
            let (close_expr, high_expr, low_expr, period, mult) =
                extract_three_cols_period_mult(args, "keltner_lower")?;
            Ok(as_struct(vec![
                close_expr.alias("__c"),
                high_expr.alias("__h"),
                low_expr.alias("__l"),
            ])
            .map(
                move |col: Column| {
                    let s = col.as_materialized_series();
                    let ca = s.struct_()?;
                    let close_s = ca.field_by_name("__c")?;
                    let high_s = ca.field_by_name("__h")?;
                    let low_s = ca.field_by_name("__l")?;
                    let c: Vec<f64> = close_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let h: Vec<f64> = high_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let l: Vec<f64> = low_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let (lower, _) = compute_keltner_channel(&c, &h, &l, period, mult);
                    let padded = pad_series(&lower, s.len());
                    Ok(Series::new("keltner_lower".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        "obv" => {
            let (close_expr, vol_expr) = extract_two_cols(args, "obv")?;
            Ok(
                as_struct(vec![close_expr.alias("__c"), vol_expr.alias("__v")]).map(
                    move |col: Column| {
                        let s = col.as_materialized_series();
                        let ca = s.struct_()?;
                        let close_s = ca.field_by_name("__c")?;
                        let vol_s = ca.field_by_name("__v")?;
                        let c: Vec<f64> = close_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let v: Vec<f64> = vol_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        if c.len() < 2 {
                            return Ok(Series::new("obv".into(), vec![f64::NAN; c.len()]).into());
                        }
                        let obv_vals =
                            rust_ti::momentum_indicators::bulk::on_balance_volume(&c, &v, 0.0);
                        let padded = pad_series(&obv_vals, s.len());
                        Ok(Series::new("obv".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ),
            )
        }
        "mfi" => {
            let (close_expr, high_expr, low_expr, vol_expr, period) =
                extract_four_cols_period(args, "mfi")?;
            Ok(as_struct(vec![
                close_expr.alias("__c"),
                high_expr.alias("__h"),
                low_expr.alias("__l"),
                vol_expr.alias("__v"),
            ])
            .map(
                move |col: Column| {
                    let s = col.as_materialized_series();
                    let ca = s.struct_()?;
                    let close_s = ca.field_by_name("__c")?;
                    let high_s = ca.field_by_name("__h")?;
                    let low_s = ca.field_by_name("__l")?;
                    let vol_s = ca.field_by_name("__v")?;
                    let c: Vec<f64> = close_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let h: Vec<f64> = high_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let l: Vec<f64> = low_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let v: Vec<f64> = vol_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let typical = compute_typical_price(&h, &l, &c);
                    let n = typical.len();
                    let mfi_vals = if period > 0 && n >= period {
                        rust_ti::momentum_indicators::bulk::money_flow_index(&typical, &v, period)
                    } else {
                        vec![]
                    };
                    let padded = pad_series(&mfi_vals, s.len());
                    Ok(Series::new("mfi".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        "tr" => {
            let (close_expr, high_expr, low_expr) = extract_three_cols(args, "tr")?;
            Ok(as_struct(vec![
                close_expr.alias("__c"),
                high_expr.alias("__h"),
                low_expr.alias("__l"),
            ])
            .map(
                move |col: Column| {
                    let s = col.as_materialized_series();
                    let ca = s.struct_()?;
                    let close_s = ca.field_by_name("__c")?;
                    let high_s = ca.field_by_name("__h")?;
                    let low_s = ca.field_by_name("__l")?;
                    let c: Vec<f64> = close_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let h: Vec<f64> = high_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let l: Vec<f64> = low_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let n = c.len();
                    let mut tr_vals = Vec::with_capacity(n);
                    // First bar: high - low (no previous close)
                    if n > 0 {
                        tr_vals.push(h[0] - l[0]);
                    }
                    for i in 1..n {
                        let hl = h[i] - l[i];
                        let hc = (h[i] - c[i - 1]).abs();
                        let lc = (l[i] - c[i - 1]).abs();
                        tr_vals.push(hl.max(hc).max(lc));
                    }
                    Ok(Series::new("tr".into(), tr_vals).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        "cmf" => {
            let (close_expr, high_expr, low_expr, vol_expr, period) =
                extract_four_cols_period(args, "cmf")?;
            Ok(as_struct(vec![
                close_expr.alias("__c"),
                high_expr.alias("__h"),
                low_expr.alias("__l"),
                vol_expr.alias("__v"),
            ])
            .map(
                move |col: Column| {
                    let s = col.as_materialized_series();
                    let ca = s.struct_()?;
                    let close_s = ca.field_by_name("__c")?;
                    let high_s = ca.field_by_name("__h")?;
                    let low_s = ca.field_by_name("__l")?;
                    let vol_s = ca.field_by_name("__v")?;
                    let c: Vec<f64> = close_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let h: Vec<f64> = high_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let l: Vec<f64> = low_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let v: Vec<f64> = vol_s
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let cmf_vals = compute_cmf(&c, &h, &l, &v, period);
                    let padded = pad_series(&cmf_vals, s.len());
                    Ok(Series::new("cmf".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        _ => Err(format!("multi_col: unknown function '{name}'")),
    }
}
