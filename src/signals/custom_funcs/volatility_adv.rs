//! Volatility and advanced functions: donchian, ichimoku, envelope, supertrend, aroon, ad,
//! pvi, nvi, ulcer.

// Multi-column map closures use conventional short names (s, c, h, l, v, n)
#![allow(clippy::many_single_char_names)]

use polars::prelude::*;
use rust_ti::candle_indicators::bulk as cti;
use rust_ti::strength_indicators::bulk as sti_strength;
use rust_ti::volatility_indicators::bulk as vti;

use super::helpers::{
    extract_col_period, extract_col_period_float, extract_four_cols, extract_three_cols,
    extract_three_cols_period_as_two_cols, extract_three_cols_period_mult, extract_two_cols,
    FuncArg,
};
use crate::signals::helpers::pad_series;

pub fn build(name: &str, args: Vec<FuncArg>) -> Result<Expr, String> {
    match name {
        "aroon_up" => build_aroon(name, args, AroonComponent::Up),
        "aroon_down" => build_aroon(name, args, AroonComponent::Down),
        "aroon_osc" => build_aroon(name, args, AroonComponent::Osc),
        "supertrend" => {
            let (close_expr, high_expr, low_expr, period, mult) =
                extract_three_cols_period_mult(&args, "supertrend")?;
            Ok(as_struct(vec![
                close_expr.alias("__c"),
                high_expr.alias("__h"),
                low_expr.alias("__l"),
            ])
            .map(
                move |col: Column| {
                    let s = col.as_materialized_series();
                    let ca = s.struct_()?;
                    let c: Vec<f64> = ca
                        .field_by_name("__c")?
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
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
                    let n = c.len();
                    if n < period {
                        return Ok(Series::new("supertrend".into(), vec![f64::NAN; n]).into());
                    }
                    let st = cti::supertrend(
                        &h,
                        &l,
                        &c,
                        rust_ti::ConstantModelType::SimpleMovingAverage,
                        mult,
                        period,
                    );
                    let padded = pad_series(&st, n);
                    Ok(Series::new("supertrend".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        "donchian_upper" | "donchian_mid" | "donchian_lower" => {
            let func = name.to_string();
            let (high_expr, low_expr, period) =
                extract_three_cols_period_as_two_cols(&args, &func)?;
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
                        if n < period {
                            return Ok(Series::new("donchian".into(), vec![f64::NAN; n]).into());
                        }
                        let dc = cti::donchian_channels(&h, &l, period);
                        let extracted: Vec<f64> = dc
                            .iter()
                            .map(|t| match func.as_str() {
                                "donchian_upper" => t.0,
                                "donchian_lower" => t.2,
                                _ => t.1, // mid
                            })
                            .collect();
                        let padded = pad_series(&extracted, n);
                        Ok(Series::new("donchian".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ),
            )
        }
        "ichimoku_tenkan" | "ichimoku_kijun" | "ichimoku_senkou_a" | "ichimoku_senkou_b" => {
            let func = name.to_string();
            let (high_expr, low_expr, close_expr) = extract_three_cols(&args, &func)?;
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
                    if n < 52 {
                        return Ok(Series::new("ichimoku".into(), vec![f64::NAN; n]).into());
                    }
                    let ich = cti::ichimoku_cloud(&h, &l, &c, 9, 26, 52);
                    let extracted: Vec<f64> = ich
                        .iter()
                        .map(|t| match func.as_str() {
                            "ichimoku_tenkan" => t.0,
                            "ichimoku_kijun" => t.1,
                            "ichimoku_senkou_a" => t.2,
                            _ => t.3, // senkou_b
                        })
                        .collect();
                    let padded = pad_series(&extracted, n);
                    Ok(Series::new("ichimoku".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        "envelope_upper" | "envelope_lower" => {
            let func = name.to_string();
            let (col_expr, period, pct) = extract_col_period_float(&args, &func)?;
            Ok(col_expr.map(
                move |col: Column| {
                    let ca = col.as_materialized_series().f64()?;
                    let n = ca.len();
                    let vals: Vec<f64> = ca.into_iter().map(|v| v.unwrap_or(f64::NAN)).collect();
                    if n < period {
                        return Ok(Series::new("envelope".into(), vec![f64::NAN; n]).into());
                    }
                    let env = cti::moving_constant_envelopes(
                        &vals,
                        rust_ti::ConstantModelType::SimpleMovingAverage,
                        pct,
                        period,
                    );
                    let extracted: Vec<f64> = env
                        .iter()
                        .map(|t| if func == "envelope_upper" { t.2 } else { t.0 })
                        .collect();
                    let padded = pad_series(&extracted, n);
                    Ok(Series::new("envelope".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        "ad" => {
            let (high_expr, low_expr, close_expr, vol_expr) = extract_four_cols(&args, "ad")?;
            Ok(as_struct(vec![
                high_expr.alias("__h"),
                low_expr.alias("__l"),
                close_expr.alias("__c"),
                vol_expr.alias("__v"),
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
                    let v: Vec<f64> = ca
                        .field_by_name("__v")?
                        .f64()?
                        .into_iter()
                        .map(|v| v.unwrap_or(f64::NAN))
                        .collect();
                    let n = c.len();
                    if n < 2 {
                        return Ok(Series::new("ad".into(), vec![f64::NAN; n]).into());
                    }
                    let ad_vals = sti_strength::accumulation_distribution(&h, &l, &c, &v, 0.0);
                    let padded = pad_series(&ad_vals, n);
                    Ok(Series::new("ad".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        "pvi" => {
            let (close_expr, vol_expr) = extract_two_cols(&args, "pvi")?;
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
                            return Ok(Series::new("pvi".into(), vec![f64::NAN; n]).into());
                        }
                        let pvi_vals = sti_strength::positive_volume_index(&c, &v, 1000.0);
                        let padded = pad_series(&pvi_vals, n);
                        Ok(Series::new("pvi".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ),
            )
        }
        "nvi" => {
            let (close_expr, vol_expr) = extract_two_cols(&args, "nvi")?;
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
                            return Ok(Series::new("nvi".into(), vec![f64::NAN; n]).into());
                        }
                        let nvi_vals = sti_strength::negative_volume_index(&c, &v, 1000.0);
                        let padded = pad_series(&nvi_vals, n);
                        Ok(Series::new("nvi".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ),
            )
        }
        "ulcer" => {
            let (col_expr, period) = extract_col_period(&args, "ulcer")?;
            Ok(col_expr.map(
                move |col: Column| {
                    let ca = col.as_materialized_series().f64()?;
                    let n = ca.len();
                    let vals: Vec<f64> = ca.into_iter().map(|v| v.unwrap_or(f64::NAN)).collect();
                    if n <= period {
                        return Ok(Series::new("ulcer".into(), vec![f64::NAN; n]).into());
                    }
                    let ulcer_vals = vti::ulcer_index(&vals, period);
                    let padded = pad_series(&ulcer_vals, n);
                    Ok(Series::new("ulcer".into(), padded).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        _ => Err(format!("volatility_adv: unknown function '{name}'")),
    }
}

// --- Aroon helper ---

enum AroonComponent {
    Up,
    Down,
    Osc,
}

fn build_aroon(name: &str, args: Vec<FuncArg>, component: AroonComponent) -> Result<Expr, String> {
    let (high_expr, low_expr, period) = extract_three_cols_period_as_two_cols(&args, name)?;
    let series_name = name.to_string();
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
                if n < period + 1 {
                    return Ok(Series::new(series_name.clone().into(), vec![f64::NAN; n]).into());
                }
                let vals: Vec<f64> = (0..(n - period))
                    .map(|i| {
                        let end = i + period + 1;
                        let (up, down, osc) = rust_ti::trend_indicators::single::aroon_indicator(
                            &h[i..end],
                            &l[i..end],
                        );
                        match component {
                            AroonComponent::Up => up,
                            AroonComponent::Down => down,
                            AroonComponent::Osc => osc,
                        }
                    })
                    .collect();
                let padded = pad_series(&vals, n);
                Ok(Series::new(series_name.clone().into(), padded).into())
            },
            |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
        ),
    )
}
