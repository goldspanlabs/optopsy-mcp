//! Stateful counting functions: `consecutive_up`, `consecutive_down`.

use polars::prelude::*;

use super::helpers::{extract_single_col, FuncArg};

pub fn build(name: &str, args: &[FuncArg]) -> Result<Expr, String> {
    match name {
        "consecutive_up" => {
            let col_expr = extract_single_col(args, "consecutive_up")?;
            Ok(col_expr.map(
                move |col: Column| {
                    let ca = col.as_materialized_series().f64()?;
                    let vals: Vec<f64> = ca
                        .into_iter()
                        .map(|opt_v| opt_v.unwrap_or(f64::NAN))
                        .collect();
                    let n = vals.len();
                    let mut counts = vec![0.0_f64; n];
                    for i in 1..n {
                        if !vals[i].is_nan() && !vals[i - 1].is_nan() && vals[i] > vals[i - 1] {
                            counts[i] = counts[i - 1] + 1.0;
                        }
                    }
                    Ok(Series::new("consecutive_up".into(), counts).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        "consecutive_down" => {
            let col_expr = extract_single_col(args, "consecutive_down")?;
            Ok(col_expr.map(
                move |col: Column| {
                    let ca = col.as_materialized_series().f64()?;
                    let vals: Vec<f64> = ca
                        .into_iter()
                        .map(|opt_v| opt_v.unwrap_or(f64::NAN))
                        .collect();
                    let n = vals.len();
                    let mut counts = vec![0.0_f64; n];
                    for i in 1..n {
                        if !vals[i].is_nan() && !vals[i - 1].is_nan() && vals[i] < vals[i - 1] {
                            counts[i] = counts[i - 1] + 1.0;
                        }
                    }
                    Ok(Series::new("consecutive_down".into(), counts).into())
                },
                |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
            ))
        }
        _ => Err(format!("stateful: unknown function '{name}'")),
    }
}
