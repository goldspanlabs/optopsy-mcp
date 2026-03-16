//! Momentum and trend functions: williams_r, adx, plus_di, minus_di, psar, tsi, vpt.

// Multi-column map closures use conventional short names (s, c, h, l, v, n)
#![allow(clippy::many_single_char_names)]

use polars::prelude::*;
use rust_ti::momentum_indicators::bulk as mti;
use rust_ti::trend_indicators::bulk as tti;

use super::helpers::{
    extract_col_two_periods, extract_three_cols_period, extract_two_cols,
    extract_two_cols_two_floats, FuncArg,
};
use crate::signals::helpers::pad_series;

pub fn build(name: &str, args: Vec<FuncArg>) -> Result<Expr, String> {
    match name {
        "williams_r" => {
            let (high_expr, low_expr, close_expr, period) =
                extract_three_cols_period(&args, "williams_r")?;
            Ok(as_struct(vec![
                high_expr.alias("__h"),
                low_expr.alias("__l"),
                close_expr.alias("__c"),
            ])
            .map(
                move |col: Column| {
                    let s = col.as_materialized_series();
                    let ca = s.struct_()?;
                    let h: Vec<f64> = ca
                        .field_by_name("__h")?
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let l: Vec<f64> = ca
                        .field_by_name("__l")?
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let c: Vec<f64> = ca
                        .field_by_name("__c")?
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let n = c.len();
                    if n < period {
                        return Ok(Series::new("williams_r".into(), vec![f64::NAN; n]).into());
                    }
                    let vals = mti::williams_percent_r(&h, &l, &c, period);
                    let padded = pad_series(&vals, n);
                    Ok(Series::new("williams_r".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        "adx" | "plus_di" | "minus_di" => {
            let func = name.to_string();
            let (high_expr, low_expr, close_expr, period) =
                extract_three_cols_period(&args, &func)?;
            Ok(as_struct(vec![
                high_expr.alias("__h"),
                low_expr.alias("__l"),
                close_expr.alias("__c"),
            ])
            .map(
                move |col: Column| {
                    let s = col.as_materialized_series();
                    let ca = s.struct_()?;
                    let h: Vec<f64> = ca
                        .field_by_name("__h")?
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let l: Vec<f64> = ca
                        .field_by_name("__l")?
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let c: Vec<f64> = ca
                        .field_by_name("__c")?
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let n = c.len();
                    if n < period + 1 {
                        return Ok(Series::new("dms".into(), vec![f64::NAN; n]).into());
                    }
                    let dms = tti::directional_movement_system(
                        &h,
                        &l,
                        &c,
                        period,
                        rust_ti::ConstantModelType::SmoothedMovingAverage,
                    );
                    let extracted: Vec<f64> = dms
                        .iter()
                        .map(|t| match func.as_str() {
                            "plus_di" => t.0,
                            "minus_di" => t.1,
                            _ => t.2, // adx
                        })
                        .collect();
                    let padded = pad_series(&extracted, n);
                    Ok(Series::new("dms".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        "psar" => {
            let (high_expr, low_expr, accel, max_accel) =
                extract_two_cols_two_floats(&args, "psar")?;
            Ok(
                as_struct(vec![high_expr.alias("__h"), low_expr.alias("__l")]).map(
                    move |col: Column| {
                        let s = col.as_materialized_series();
                        let ca = s.struct_()?;
                        let h: Vec<f64> = ca
                            .field_by_name("__h")?
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let l: Vec<f64> = ca
                            .field_by_name("__l")?
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let n = h.len();
                        if n < 2 {
                            return Ok(Series::new("psar".into(), vec![f64::NAN; n]).into());
                        }
                        let sar_vals = tti::parabolic_time_price_system(
                            &h,
                            &l,
                            accel,
                            max_accel,
                            accel,
                            rust_ti::Position::Long,
                            l[0],
                        );
                        let padded = pad_series(&sar_vals, n);
                        Ok(Series::new("psar".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ),
            )
        }
        "tsi" => {
            let (col_expr, fast, slow) = extract_col_two_periods(&args, "tsi")?;
            Ok(col_expr.map(
                move |col: Column| {
                    let ca = col.as_materialized_series().f64()?;
                    let n = ca.len();
                    let vals: Vec<f64> = ca.into_iter().map(|v| v.unwrap_or(f64::NAN)).collect();
                    if n <= slow {
                        return Ok(Series::new("tsi".into(), vec![f64::NAN; n]).into());
                    }
                    let tsi_vals = tti::true_strength_index(
                        &vals,
                        rust_ti::ConstantModelType::ExponentialMovingAverage,
                        fast,
                        rust_ti::ConstantModelType::ExponentialMovingAverage,
                        slow,
                    );
                    let padded = pad_series(&tsi_vals, n);
                    Ok(Series::new("tsi".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        "vpt" => {
            let (close_expr, vol_expr) = extract_two_cols(&args, "vpt")?;
            Ok(
                as_struct(vec![close_expr.alias("__c"), vol_expr.alias("__v")]).map(
                    move |col: Column| {
                        let s = col.as_materialized_series();
                        let ca = s.struct_()?;
                        let c: Vec<f64> = ca
                            .field_by_name("__c")?
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let v: Vec<f64> = ca
                            .field_by_name("__v")?
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let n = c.len();
                        if n < 2 {
                            return Ok(Series::new("vpt".into(), vec![f64::NAN; n]).into());
                        }
                        let vpt_vals = tti::volume_price_trend(&c, &v[1..], 0.0);
                        let padded = pad_series(&vpt_vals, n);
                        Ok(Series::new("vpt".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ),
            )
        }
        _ => Err(format!("momentum_trend: unknown function '{name}'")),
    }
}
