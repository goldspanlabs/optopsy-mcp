//! Pricing-related types: delta/DTE ranges, commission, slippage, and position sizing.

use garde::Validate;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Target delta with acceptable min/max range for leg filtering.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
pub struct TargetRange {
    /// Preferred delta value to match.
    #[garde(range(min = 0.0, max = 1.0))]
    pub target: f64,
    /// Minimum acceptable delta (absolute).
    #[garde(range(min = 0.0, max = 1.0))]
    pub min: f64,
    /// Maximum acceptable delta (absolute).
    #[garde(range(min = 0.0, max = 1.0), custom(validate_max_gte_min(&self.min)))]
    pub max: f64,
}

fn validate_max_gte_min(min: &f64) -> impl FnOnce(&f64, &()) -> garde::Result + '_ {
    move |max: &f64, (): &()| {
        if min > max {
            return Err(garde::Error::new(format!(
                "min ({min}) must be <= max ({max})"
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
pub struct DteRange {
    /// Preferred entry DTE (must be within `[min, max]`). Default: 45
    #[serde(default = "default_dte_target")]
    #[schemars(default = "default_dte_target")]
    #[garde(range(min = 1), custom(validate_dte_target_in_range(self.min, self.max)))]
    pub target: i32,
    /// Minimum entry DTE (must be > `exit_dte`). Default: 30
    #[serde(default = "default_dte_min")]
    #[schemars(default = "default_dte_min")]
    #[garde(range(min = 1))]
    pub min: i32,
    /// Maximum entry DTE. Default: 60
    #[serde(default = "default_dte_max")]
    #[schemars(default = "default_dte_max")]
    #[garde(range(min = 1), custom(validate_dte_max_gte_min(&self.min)))]
    pub max: i32,
}

fn default_dte_target() -> i32 {
    45
}
fn default_dte_min() -> i32 {
    30
}
fn default_dte_max() -> i32 {
    60
}

fn validate_dte_target_in_range(min: i32, max: i32) -> impl FnOnce(&i32, &()) -> garde::Result {
    move |target: &i32, (): &()| {
        if *target < min || *target > max {
            return Err(garde::Error::new(format!(
                "target ({target}) must be within [min ({min}), max ({max})]"
            )));
        }
        Ok(())
    }
}

fn validate_dte_max_gte_min(min: &i32) -> impl FnOnce(&i32, &()) -> garde::Result + '_ {
    move |max: &i32, (): &()| {
        if min > max {
            return Err(garde::Error::new(format!(
                "min ({min}) must be <= max ({max})"
            )));
        }
        Ok(())
    }
}

/// Commission schedule applied per trade: `max(base_fee + per_contract * qty, min_fee)`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
pub struct Commission {
    /// Fee charged per contract.
    #[garde(range(min = 0.0))]
    pub per_contract: f64,
    /// Flat base fee added to every trade.
    #[garde(range(min = 0.0))]
    pub base_fee: f64,
    /// Minimum total fee floor.
    #[garde(range(min = 0.0))]
    pub min_fee: f64,
}

impl Default for Commission {
    fn default() -> Self {
        Self {
            per_contract: 0.0,
            base_fee: 0.0,
            min_fee: 0.0,
        }
    }
}

impl Commission {
    /// Compute the total commission for the given number of contracts.
    pub fn calculate(&self, num_contracts: i32) -> f64 {
        let fee = self.base_fee + self.per_contract * f64::from(num_contracts.abs());
        fee.max(self.min_fee)
    }
}

/// Position sizing method controlling how many contracts/shares to trade per entry.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(tag = "method")]
pub enum PositionSizing {
    /// Use the fixed `quantity` from params (default behavior).
    #[serde(rename = "fixed")]
    Fixed,
    /// Risk a fixed fraction of current equity per trade.
    #[serde(rename = "fixed_fractional")]
    FixedFractional {
        #[garde(range(min = 0.001, max = 1.0))]
        risk_pct: f64,
    },
    /// Kelly criterion with a fractional multiplier and optional lookback window.
    /// Falls back to fixed `quantity` for the first 20 trades (cold start).
    #[serde(rename = "kelly")]
    Kelly {
        #[garde(range(min = 0.01, max = 1.0))]
        fraction: f64,
        #[garde(skip)]
        lookback: Option<usize>,
    },
    /// Risk a fixed dollar amount per trade.
    #[serde(rename = "risk_per_trade")]
    RiskPerTrade {
        #[garde(range(min = 1.0))]
        risk_amount: f64,
    },
    /// Target a specific portfolio volatility level.
    #[serde(rename = "volatility_target")]
    VolatilityTarget {
        #[garde(range(min = 0.01, max = 2.0))]
        target_vol: f64,
        #[garde(range(min = 5, max = 252))]
        lookback_days: i32,
    },
}

/// Constraints on computed position sizes.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
pub struct SizingConstraints {
    /// Minimum contracts/shares per trade.
    #[serde(default = "default_min_qty")]
    #[garde(range(min = 1))]
    pub min_quantity: i32,
    /// Optional maximum contracts/shares per trade (must be >= `min_quantity`).
    #[garde(custom(validate_max_quantity(&self.min_quantity)))]
    pub max_quantity: Option<i32>,
}

fn default_min_qty() -> i32 {
    1
}

/// Validate that `max_quantity` (when present) is >= `min_quantity` and >= 1.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn validate_max_quantity(
    min_quantity: &i32,
) -> impl FnOnce(&Option<i32>, &()) -> garde::Result + '_ {
    move |max_quantity: &Option<i32>, (): &()| {
        if let Some(max) = max_quantity {
            if *max < 1 {
                return Err(garde::Error::new("max_quantity must be >= 1".to_string()));
            }
            if *max < *min_quantity {
                return Err(garde::Error::new(format!(
                    "max_quantity ({max}) must be >= min_quantity ({min_quantity})"
                )));
            }
        }
        Ok(())
    }
}

impl Default for SizingConstraints {
    fn default() -> Self {
        Self {
            min_quantity: 1,
            max_quantity: None,
        }
    }
}

/// Dynamic position sizing configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
pub struct SizingConfig {
    /// The sizing method to use.
    #[garde(dive)]
    pub method: PositionSizing,
    /// Min/max constraints on computed quantity.
    #[serde(default)]
    #[garde(dive)]
    pub constraints: SizingConstraints,
}

/// Slippage model controlling how fill prices are derived from bid/ask quotes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(tag = "type")]
#[derive(Default)]
pub enum Slippage {
    Mid,
    #[default]
    Spread,
    Liquidity {
        #[garde(range(min = 0.0, max = 1.0))]
        fill_ratio: f64,
        #[garde(skip)]
        ref_volume: u64,
    },
    PerLeg {
        #[garde(range(min = 0.0))]
        per_leg: f64,
    },
    /// Fill at `bid + (ask − bid) × pct` for longs; `ask − (ask − bid) × pct` for shorts.
    /// `pct = 0` → filled at bid/ask (best for longs/shorts), `pct = 0.5` → mid, `pct = 1` → ask/bid.
    BidAskTravel {
        #[garde(range(min = 0.0, max = 1.0))]
        pct: f64,
    },
}

pub(crate) fn validate_exit_dte_lt_entry_min(
    entry_dte: &DteRange,
) -> impl FnOnce(&i32, &()) -> garde::Result + '_ {
    let entry_min = entry_dte.min;
    move |exit_dte: &i32, (): &()| {
        if *exit_dte >= entry_min {
            return Err(garde::Error::new(format!(
                "exit_dte ({exit_dte}) must be less than entry_dte.min ({entry_min})"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commission_per_contract() {
        let c = Commission {
            per_contract: 0.65,
            base_fee: 0.0,
            min_fee: 0.0,
        };
        assert!((c.calculate(10) - 6.50).abs() < 1e-10);
    }

    #[test]
    fn commission_base_fee() {
        let c = Commission {
            per_contract: 0.65,
            base_fee: 1.00,
            min_fee: 0.0,
        };
        assert!((c.calculate(5) - 4.25).abs() < 1e-10);
    }

    #[test]
    fn commission_min_fee() {
        let c = Commission {
            per_contract: 0.10,
            base_fee: 0.0,
            min_fee: 5.00,
        };
        assert!((c.calculate(1) - 5.00).abs() < 1e-10);
    }

    #[test]
    fn commission_min_fee_not_applied_when_above() {
        let c = Commission {
            per_contract: 1.00,
            base_fee: 5.00,
            min_fee: 2.00,
        };
        assert!((c.calculate(3) - 8.00).abs() < 1e-10);
    }

    #[test]
    fn commission_default_zero() {
        let c = Commission::default();
        assert!((c.calculate(10) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn commission_negative_contracts_uses_abs() {
        let c = Commission {
            per_contract: 0.65,
            base_fee: 0.0,
            min_fee: 0.0,
        };
        assert!((c.calculate(-10) - 6.50).abs() < 1e-10);
    }

    #[test]
    fn target_range_valid() {
        let tr = TargetRange {
            target: 0.5,
            min: 0.2,
            max: 0.8,
        };
        assert!(tr.validate().is_ok());
    }

    #[test]
    fn target_range_rejects_negative() {
        let tr = TargetRange {
            target: -0.5,
            min: 0.2,
            max: 0.8,
        };
        assert!(tr.validate().is_err());
    }

    #[test]
    fn target_range_rejects_over_one() {
        let tr = TargetRange {
            target: 0.5,
            min: 0.2,
            max: 1.1,
        };
        assert!(tr.validate().is_err());
    }

    #[test]
    fn target_range_rejects_min_gt_max() {
        let tr = TargetRange {
            target: 0.5,
            min: 0.8,
            max: 0.2,
        };
        assert!(tr.validate().is_err());
    }

    #[test]
    fn commission_rejects_negative_fee() {
        let c = Commission {
            per_contract: -0.65,
            base_fee: 0.0,
            min_fee: 0.0,
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn slippage_liquidity_rejects_fill_ratio_over_one() {
        let s = Slippage::Liquidity {
            fill_ratio: 1.5,
            ref_volume: 1000,
        };
        assert!(s.validate().is_err());
    }
}
