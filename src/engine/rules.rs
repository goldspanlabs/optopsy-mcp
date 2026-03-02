use anyhow::{bail, Result};
use polars::prelude::*;

use super::types::{ExpirationCycle, StrategyDef};

/// Validate strike ordering for multi-leg strategies.
/// For spreads: strikes must be ordered correctly.
/// For butterflies: lower < middle < upper.
/// For condors/iron strategies: s1 < s2 < s3 < s4.
#[allow(dead_code)]
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

/// Filter a multi-leg `DataFrame` to ensure strike ordering constraints.
/// Assumes legs are joined and strikes are in columns like `strike_0`, `strike_1`, etc.
///
/// When `strict` is `true`, requires `strike_0 < strike_1 < ...` (for spreads, condors).
/// When `strict` is `false`, requires `strike_0 <= strike_1 <= ...` (for straddles,
/// iron butterflies, and other strategies where adjacent legs may share a strike).
///
/// For multi-expiration strategies, ordering is applied **within each expiration cycle**
/// independently (Primary legs among themselves, Secondary legs among themselves).
pub fn filter_strike_order(
    df: &DataFrame,
    num_legs: usize,
    strict: bool,
    strategy_def: Option<&StrategyDef>,
) -> Result<DataFrame> {
    if num_legs <= 1 {
        return Ok(df.clone());
    }

    // For multi-expiration strategies, apply ordering within each cycle group
    if let Some(sdef) = strategy_def {
        if sdef.is_multi_expiration() {
            return filter_strike_order_by_cycle(df, sdef, strict);
        }
    }

    // Standard ordering: sequential across all legs
    let mut lazy = df.clone().lazy();

    for i in 1..num_legs {
        let prev_col = format!("strike_{}", i - 1);
        let curr_col = format!("strike_{i}");
        if strict {
            lazy = lazy.filter(col(&curr_col).gt(col(&prev_col)));
        } else {
            lazy = lazy.filter(col(&curr_col).gt_eq(col(&prev_col)));
        }
    }

    Ok(lazy.collect()?)
}

/// Apply strike ordering within each expiration cycle group independently.
fn filter_strike_order_by_cycle(
    df: &DataFrame,
    strategy_def: &StrategyDef,
    strict: bool,
) -> Result<DataFrame> {
    // Group leg indices by cycle
    let mut primary_indices: Vec<usize> = Vec::new();
    let mut secondary_indices: Vec<usize> = Vec::new();

    for (i, leg) in strategy_def.legs.iter().enumerate() {
        match leg.expiration_cycle {
            ExpirationCycle::Primary => primary_indices.push(i),
            ExpirationCycle::Secondary => secondary_indices.push(i),
        }
    }

    let lazy = df.clone().lazy();
    let lazy = apply_window_ordering(lazy, &primary_indices, strict);
    let lazy = apply_window_ordering(lazy, &secondary_indices, strict);
    Ok(lazy.collect()?)
}

/// Apply sequential strike-ordering filters for a group of leg indices.
fn apply_window_ordering(mut lazy: LazyFrame, indices: &[usize], strict: bool) -> LazyFrame {
    for w in indices.windows(2) {
        let prev_col = format!("strike_{}", w[0]);
        let curr_col = format!("strike_{}", w[1]);
        lazy = if strict {
            lazy.filter(col(&curr_col).gt(col(&prev_col)))
        } else {
            lazy.filter(col(&curr_col).gt_eq(col(&prev_col)))
        };
    }
    lazy
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
        let result = filter_strike_order(&df, 1, true, None).unwrap();
        assert_eq!(result.height(), 2);
    }

    #[test]
    fn filter_strike_order_two_legs() {
        let df = df! {
            "strike_0" => &[100.0, 110.0, 100.0],
            "strike_1" => &[110.0, 100.0, 100.0],
        }
        .unwrap();
        let result = filter_strike_order(&df, 2, true, None).unwrap();
        // Only first row has strike_0 < strike_1
        assert_eq!(result.height(), 1);
        assert!(
            (result
                .column("strike_0")
                .unwrap()
                .f64()
                .unwrap()
                .get(0)
                .unwrap()
                - 100.0)
                .abs()
                < f64::EPSILON
        );
    }

    #[test]
    fn filter_strike_order_two_legs_relaxed() {
        let df = df! {
            "strike_0" => &[100.0, 110.0, 100.0],
            "strike_1" => &[110.0, 100.0, 100.0],
        }
        .unwrap();
        let result = filter_strike_order(&df, 2, false, None).unwrap();
        // First row (100 < 110) and third row (100 == 100) pass with <=
        assert_eq!(result.height(), 2);
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
        let result = filter_strike_order(&df, 4, true, None).unwrap();
        // Only first row is strictly ascending
        assert_eq!(result.height(), 1);
    }

    /// Helper to build a `StrategyDef` with given cycles per leg.
    fn make_strategy_def(cycles: &[ExpirationCycle], strict: bool) -> StrategyDef {
        use crate::engine::types::{LegDef, OptionType, Side, TargetRange};
        let legs = cycles
            .iter()
            .map(|&c| LegDef {
                side: Side::Long,
                option_type: OptionType::Call,
                delta: TargetRange {
                    target: 0.5,
                    min: 0.2,
                    max: 0.8,
                },
                qty: 1,
                expiration_cycle: c,
            })
            .collect();
        StrategyDef {
            name: "test".to_string(),
            category: "test".to_string(),
            description: "test".to_string(),
            legs,
            strict_strike_order: strict,
        }
    }

    #[test]
    fn filter_strike_order_by_cycle_two_leg_calendar() {
        // Calendar spread: 1 primary leg, 1 secondary leg — no intra-cycle ordering needed
        let df = df! {
            "strike_0" => &[100.0, 105.0],
            "strike_1" => &[100.0, 100.0],
        }
        .unwrap();
        let sdef = make_strategy_def(
            &[ExpirationCycle::Primary, ExpirationCycle::Secondary],
            false,
        );
        // Both rows should pass since each cycle has only 1 leg (no ordering to enforce)
        let result = filter_strike_order(&df, 2, false, Some(&sdef)).unwrap();
        assert_eq!(result.height(), 2);
    }

    #[test]
    fn filter_strike_order_by_cycle_four_leg_double_calendar() {
        // Double calendar: primary legs [0, 2], secondary legs [1, 3]
        // Primary: strike_0=100 (call), strike_2=95 (put) — relaxed, 95 < 100 ✓
        // Secondary: strike_1=100 (call), strike_3=95 (put) — relaxed, 95 < 100 ✓
        let df = df! {
            "strike_0" => &[100.0, 100.0],
            "strike_1" => &[100.0, 100.0],
            "strike_2" => &[95.0,  110.0],
            "strike_3" => &[95.0,  110.0],
        }
        .unwrap();
        let sdef = make_strategy_def(
            &[
                ExpirationCycle::Primary,
                ExpirationCycle::Secondary,
                ExpirationCycle::Primary,
                ExpirationCycle::Secondary,
            ],
            false,
        );
        // Row 1: primary [0]=100, [2]=95 → 95 < 100 ✓ (relaxed >=)? No: 95 < 100 means
        // strike_2 >= strike_0 fails (95 >= 100 is false). So row 1 is filtered.
        // Row 2: primary [0]=100, [2]=110 → 110 >= 100 ✓
        let result = filter_strike_order(&df, 4, false, Some(&sdef)).unwrap();
        assert_eq!(result.height(), 1);
    }

    #[test]
    fn filter_strike_order_by_cycle_strict_diagonal() {
        // Diagonal spread: 1 primary, 1 secondary — single leg per cycle, no filtering
        let df = df! {
            "strike_0" => &[100.0, 100.0],
            "strike_1" => &[100.0, 105.0],
        }
        .unwrap();
        let sdef = make_strategy_def(
            &[ExpirationCycle::Primary, ExpirationCycle::Secondary],
            true,
        );
        // Single leg per cycle means windows(2) is empty — all rows pass
        let result = filter_strike_order(&df, 2, true, Some(&sdef)).unwrap();
        assert_eq!(result.height(), 2);
    }
}
