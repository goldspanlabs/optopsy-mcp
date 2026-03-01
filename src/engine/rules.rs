use anyhow::{bail, Result};
use polars::prelude::*;

/// Validate strike ordering for multi-leg strategies.
/// For spreads: strikes must be ordered correctly.
/// For butterflies: lower < middle < upper.
/// For condors/iron strategies: s1 < s2 < s3 < s4.
pub fn validate_strike_order(strikes: &[f64]) -> Result<()> {
    for i in 1..strikes.len() {
        if strikes[i] <= strikes[i - 1] {
            bail!(
                "Strike ordering violated: strike[{}]={} must be > strike[{}]={}",
                i,
                strikes[i],
                i - 1,
                strikes[i - 1]
            );
        }
    }
    Ok(())
}

/// Filter a multi-leg `DataFrame` to ensure strike ordering constraints
/// Assumes legs are joined and strikes are in columns like `strike_0`, `strike_1`, etc.
pub fn filter_strike_order(df: &DataFrame, num_legs: usize) -> Result<DataFrame> {
    if num_legs <= 1 {
        return Ok(df.clone());
    }

    let mut lazy = df.clone().lazy();

    for i in 1..num_legs {
        let prev_col = format!("strike_{}", i - 1);
        let curr_col = format!("strike_{i}");
        lazy = lazy.filter(col(&curr_col).gt(col(&prev_col)));
    }

    Ok(lazy.collect()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_ascending_strikes() {
        assert!(validate_strike_order(&[100.0, 105.0, 110.0]).is_ok());
    }

    #[test]
    fn validate_single_strike() {
        assert!(validate_strike_order(&[100.0]).is_ok());
    }

    #[test]
    fn validate_empty_strikes() {
        assert!(validate_strike_order(&[]).is_ok());
    }

    #[test]
    fn validate_equal_strikes_fails() {
        assert!(validate_strike_order(&[100.0, 100.0]).is_err());
    }

    #[test]
    fn validate_descending_strikes_fails() {
        assert!(validate_strike_order(&[110.0, 105.0, 100.0]).is_err());
    }

    #[test]
    fn validate_partial_disorder_fails() {
        assert!(validate_strike_order(&[100.0, 110.0, 105.0]).is_err());
    }

    #[test]
    fn filter_strike_order_single_leg_passthrough() {
        let df = df! {
            "strike_0" => &[100.0, 200.0],
            "value" => &[1, 2],
        }
        .unwrap();
        let result = filter_strike_order(&df, 1).unwrap();
        assert_eq!(result.height(), 2);
    }

    #[test]
    fn filter_strike_order_two_legs() {
        let df = df! {
            "strike_0" => &[100.0, 110.0, 100.0],
            "strike_1" => &[110.0, 100.0, 100.0],
        }
        .unwrap();
        let result = filter_strike_order(&df, 2).unwrap();
        // Only first row has strike_0 < strike_1
        assert_eq!(result.height(), 1);
        assert_eq!(result.column("strike_0").unwrap().f64().unwrap().get(0).unwrap(), 100.0);
    }

    #[test]
    fn filter_strike_order_four_legs() {
        let df = df! {
            "strike_0" => &[100.0, 100.0],
            "strike_1" => &[105.0, 110.0],
            "strike_2" => &[110.0, 105.0],
            "strike_3" => &[115.0, 115.0],
        }
        .unwrap();
        let result = filter_strike_order(&df, 4).unwrap();
        // Only first row is strictly ascending
        assert_eq!(result.height(), 1);
    }
}
