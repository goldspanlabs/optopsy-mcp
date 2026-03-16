//! Shared helpers for custom formula function dispatch.
//!
//! Contains `FuncArg`, argument extraction utilities, and rolling compute helpers.

use polars::prelude::*;

#[derive(Debug, Clone)]
pub enum FuncArg {
    Number(f64),
    Expression(Expr),
}

impl FuncArg {
    pub fn into_expr(self) -> Expr {
        match self {
            FuncArg::Number(n) => lit(n),
            FuncArg::Expression(e) => e,
        }
    }

    pub fn as_usize(&self) -> Result<usize, String> {
        match self {
            FuncArg::Number(n) => {
                if *n > 0.0 && n.fract() == 0.0 {
                    Ok(*n as usize)
                } else {
                    Err(format!("Expected positive integer, got {n}"))
                }
            }
            FuncArg::Expression(_) => Err("Expected a number, got an expression".to_string()),
        }
    }
}

pub fn extract_col_period(args: &[FuncArg], func_name: &str) -> Result<(Expr, usize), String> {
    if args.len() != 2 {
        return Err(format!(
            "{func_name}() takes exactly 2 arguments: (column, period)"
        ));
    }
    let col_expr = match &args[0] {
        FuncArg::Expression(e) => e.clone(),
        FuncArg::Number(n) => lit(*n),
    };
    let period = args[1].as_usize()?;
    Ok((col_expr, period))
}

pub fn extract_single_col(args: &[FuncArg], func_name: &str) -> Result<Expr, String> {
    if args.len() != 1 {
        return Err(format!("{func_name}() takes exactly 1 argument: (column)"));
    }
    Ok(args[0].clone().into_expr())
}

pub fn extract_two_cols(args: &[FuncArg], func_name: &str) -> Result<(Expr, Expr), String> {
    if args.len() != 2 {
        return Err(format!(
            "{func_name}() takes exactly 2 arguments: (col1, col2)"
        ));
    }
    Ok((args[0].clone().into_expr(), args[1].clone().into_expr()))
}

pub fn extract_three_cols(args: &[FuncArg], func_name: &str) -> Result<(Expr, Expr, Expr), String> {
    if args.len() != 3 {
        return Err(format!(
            "{func_name}() takes exactly 3 arguments: (col1, col2, col3)"
        ));
    }
    Ok((
        args[0].clone().into_expr(),
        args[1].clone().into_expr(),
        args[2].clone().into_expr(),
    ))
}

pub fn extract_three_cols_period(
    args: &[FuncArg],
    func_name: &str,
) -> Result<(Expr, Expr, Expr, usize), String> {
    if args.len() != 4 {
        return Err(format!(
            "{func_name}() takes exactly 4 arguments: (col1, col2, col3, period)"
        ));
    }
    Ok((
        args[0].clone().into_expr(),
        args[1].clone().into_expr(),
        args[2].clone().into_expr(),
        args[3].as_usize()?,
    ))
}

pub fn extract_three_cols_period_mult(
    args: &[FuncArg],
    func_name: &str,
) -> Result<(Expr, Expr, Expr, usize, f64), String> {
    if args.len() != 5 {
        return Err(format!(
            "{func_name}() takes exactly 5 arguments: (col1, col2, col3, period, multiplier)"
        ));
    }
    let mult = match &args[4] {
        FuncArg::Number(n) => *n,
        FuncArg::Expression(_) => {
            return Err(format!(
                "{func_name}(): multiplier (5th arg) must be a number"
            ))
        }
    };
    Ok((
        args[0].clone().into_expr(),
        args[1].clone().into_expr(),
        args[2].clone().into_expr(),
        args[3].as_usize()?,
        mult,
    ))
}

pub fn extract_four_cols_period(
    args: &[FuncArg],
    func_name: &str,
) -> Result<(Expr, Expr, Expr, Expr, usize), String> {
    if args.len() != 5 {
        return Err(format!(
            "{func_name}() takes exactly 5 arguments: (col1, col2, col3, col4, period)"
        ));
    }
    Ok((
        args[0].clone().into_expr(),
        args[1].clone().into_expr(),
        args[2].clone().into_expr(),
        args[3].clone().into_expr(),
        args[4].as_usize()?,
    ))
}

pub fn extract_three_cols_period_as_two_cols(
    args: &[FuncArg],
    func_name: &str,
) -> Result<(Expr, Expr, usize), String> {
    if args.len() != 3 {
        return Err(format!(
            "{func_name}() takes exactly 3 arguments: (col1, col2, period)"
        ));
    }
    Ok((
        args[0].clone().into_expr(),
        args[1].clone().into_expr(),
        args[2].as_usize()?,
    ))
}

pub fn extract_col_two_periods(
    args: &[FuncArg],
    func_name: &str,
) -> Result<(Expr, usize, usize), String> {
    if args.len() != 3 {
        return Err(format!(
            "{func_name}() takes exactly 3 arguments: (column, period1, period2)"
        ));
    }
    Ok((
        args[0].clone().into_expr(),
        args[1].as_usize()?,
        args[2].as_usize()?,
    ))
}

pub fn extract_two_cols_two_floats(
    args: &[FuncArg],
    func_name: &str,
) -> Result<(Expr, Expr, f64, f64), String> {
    if args.len() != 4 {
        return Err(format!(
            "{func_name}() takes exactly 4 arguments: (col1, col2, float1, float2)"
        ));
    }
    let f1 = match &args[2] {
        FuncArg::Number(n) => *n,
        FuncArg::Expression(_) => {
            return Err(format!("{func_name}(): 3rd argument must be a number"))
        }
    };
    let f2 = match &args[3] {
        FuncArg::Number(n) => *n,
        FuncArg::Expression(_) => {
            return Err(format!("{func_name}(): 4th argument must be a number"))
        }
    };
    Ok((
        args[0].clone().into_expr(),
        args[1].clone().into_expr(),
        f1,
        f2,
    ))
}

pub fn extract_col_period_float(
    args: &[FuncArg],
    func_name: &str,
) -> Result<(Expr, usize, f64), String> {
    if args.len() != 3 {
        return Err(format!(
            "{func_name}() takes exactly 3 arguments: (column, period, float)"
        ));
    }
    let period = args[1].as_usize()?;
    let f = match &args[2] {
        FuncArg::Number(n) => *n,
        FuncArg::Expression(_) => {
            return Err(format!("{func_name}(): 3rd argument must be a number"))
        }
    };
    Ok((args[0].clone().into_expr(), period, f))
}

pub fn extract_four_cols(
    args: &[FuncArg],
    func_name: &str,
) -> Result<(Expr, Expr, Expr, Expr), String> {
    if args.len() != 4 {
        return Err(format!(
            "{func_name}() takes exactly 4 arguments: (col1, col2, col3, col4)"
        ));
    }
    Ok((
        args[0].clone().into_expr(),
        args[1].clone().into_expr(),
        args[2].clone().into_expr(),
        args[3].clone().into_expr(),
    ))
}

/// Compute percentile rank within a rolling window.
pub fn compute_rolling_rank(vals: &[f64], period: usize) -> Vec<f64> {
    let n = vals.len();
    if period == 0 || n < period {
        return vec![];
    }
    (0..=n - period)
        .map(|i| {
            let window = &vals[i..i + period];
            let current = vals[i + period - 1];
            if current.is_nan() {
                return f64::NAN;
            }
            let below = window
                .iter()
                .filter(|&&v| !v.is_nan() && v < current)
                .count();
            let valid = window.iter().filter(|&&v| !v.is_nan()).count();
            if valid == 0 {
                f64::NAN
            } else {
                below as f64 / valid as f64 * 100.0
            }
        })
        .collect()
}

/// Compute IV Rank (min-max normalization) within a rolling window.
/// `IV Rank = (current - window_min) / (window_max - window_min) * 100`
pub fn compute_iv_rank(vals: &[f64], period: usize) -> Vec<f64> {
    let n = vals.len();
    if period == 0 || n < period {
        return vec![];
    }
    (0..=n - period)
        .map(|i| {
            let window = &vals[i..i + period];
            let current = vals[i + period - 1];
            if current.is_nan() {
                return f64::NAN;
            }
            let mut min = f64::INFINITY;
            let mut max = f64::NEG_INFINITY;
            let mut valid = 0usize;
            for &v in window {
                if !v.is_nan() {
                    valid += 1;
                    if v < min {
                        min = v;
                    }
                    if v > max {
                        max = v;
                    }
                }
            }
            // Require at least half the lookback to have valid data
            if valid < period / 2 + 1 {
                return f64::NAN;
            }
            let range = max - min;
            if range <= 0.0 {
                return f64::NAN;
            }
            (current - min) / range * 100.0
        })
        .collect()
}
