//! Date/time functions: day_of_week, month, day_of_month, hour, minute, week_of_year.
//!
//! These are zero-argument functions that operate on the injected `__dt` column.

use polars::prelude::*;

use super::helpers::FuncArg;

pub fn build(name: &str, args: Vec<FuncArg>) -> Result<Expr, String> {
    match name {
        "day_of_week" => {
            if !args.is_empty() {
                return Err("day_of_week() takes no arguments".to_string());
            }
            Ok(col("__dt").dt().weekday().cast(DataType::Float64))
        }
        "month" => {
            if !args.is_empty() {
                return Err("month() takes no arguments".to_string());
            }
            Ok(col("__dt").dt().month().cast(DataType::Float64))
        }
        "day_of_month" => {
            if !args.is_empty() {
                return Err("day_of_month() takes no arguments".to_string());
            }
            Ok(col("__dt").dt().day().cast(DataType::Float64))
        }
        "hour" => {
            if !args.is_empty() {
                return Err("hour() takes no arguments".to_string());
            }
            Ok(col("__dt").dt().hour().cast(DataType::Float64))
        }
        "minute" => {
            if !args.is_empty() {
                return Err("minute() takes no arguments".to_string());
            }
            Ok(col("__dt").dt().minute().cast(DataType::Float64))
        }
        "week_of_year" => {
            if !args.is_empty() {
                return Err("week_of_year() takes no arguments".to_string());
            }
            Ok(col("__dt").dt().week().cast(DataType::Float64))
        }
        _ => Err(format!("datetime: unknown function '{name}'")),
    }
}
