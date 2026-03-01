use chrono::{NaiveDate, NaiveDateTime};
use garde::Validate;
use ordered_float::OrderedFloat;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::signals::registry::SignalSpec;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum Side {
    Long = 1,
    Short = -1,
}

impl Side {
    pub fn multiplier(self) -> f64 {
        match self {
            Side::Long => 1.0,
            Side::Short => -1.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub enum OptionType {
    Call,
    Put,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ExpirationCycle {
    #[default]
    Primary, // Near-term (or same-expiration for non-calendar strategies)
    Secondary, // Far-term (calendar/diagonal only)
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
pub struct TargetRange {
    #[garde(range(min = 0.0, max = 1.0))]
    pub target: f64,
    #[garde(range(min = 0.0, max = 1.0))]
    pub min: f64,
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
pub struct Commission {
    #[garde(range(min = 0.0))]
    pub per_contract: f64,
    #[garde(range(min = 0.0))]
    pub base_fee: f64,
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
    pub fn calculate(&self, num_contracts: i32) -> f64 {
        let fee = self.base_fee + self.per_contract * f64::from(num_contracts.abs());
        fee.max(self.min_fee)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(tag = "type")]
#[derive(Default)]
pub enum Slippage {
    #[default]
    Mid,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
pub enum TradeSelector {
    #[default]
    Nearest,
    HighestPremium,
    LowestPremium,
    First,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub enum ExitType {
    Expiration,
    StopLoss,
    TakeProfit,
    MaxHold,
    DteExit,
    Adjustment,
    Signal,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct LegDef {
    pub side: Side,
    pub option_type: OptionType,
    pub delta: TargetRange,
    pub qty: i32,
    pub expiration_cycle: ExpirationCycle,
}

#[derive(Debug, Clone)]
pub struct StrategyDef {
    pub name: String,
    pub category: String,
    pub description: String,
    pub legs: Vec<LegDef>,
    /// When `false`, adjacent legs may share the same strike (e.g. straddles,
    /// iron butterflies). When `true` (default), strikes must be strictly ascending.
    pub strict_strike_order: bool,
}

impl StrategyDef {
    /// Returns true if this strategy has legs with different expiration cycles.
    pub fn is_multi_expiration(&self) -> bool {
        self.legs
            .iter()
            .any(|l| l.expiration_cycle == ExpirationCycle::Secondary)
    }
}

fn validate_exit_dte_lt_max(max_entry_dte: &i32) -> impl FnOnce(&i32, &()) -> garde::Result + '_ {
    move |exit_dte: &i32, (): &()| {
        if exit_dte >= max_entry_dte {
            return Err(garde::Error::new(format!(
                "exit_dte ({exit_dte}) must be less than max_entry_dte ({max_entry_dte})"
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
pub struct EvaluateParams {
    #[garde(length(min = 1))]
    pub strategy: String,
    #[garde(length(min = 1), dive)]
    pub leg_deltas: Vec<TargetRange>,
    #[garde(range(min = 1))]
    pub max_entry_dte: i32,
    #[garde(range(min = 0), custom(validate_exit_dte_lt_max(&self.max_entry_dte)))]
    pub exit_dte: i32,
    #[garde(range(min = 1))]
    pub dte_interval: i32,
    #[garde(range(min = 0.001, max = 1.0))]
    pub delta_interval: f64,
    #[garde(dive)]
    pub slippage: Slippage,
    #[serde(default)]
    #[garde(dive)]
    pub commission: Option<Commission>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
pub struct BacktestParams {
    #[garde(length(min = 1))]
    pub strategy: String,
    #[garde(length(min = 1), dive)]
    pub leg_deltas: Vec<TargetRange>,
    #[garde(range(min = 1))]
    pub max_entry_dte: i32,
    #[garde(range(min = 0), custom(validate_exit_dte_lt_max(&self.max_entry_dte)))]
    pub exit_dte: i32,
    #[garde(dive)]
    pub slippage: Slippage,
    #[serde(default)]
    #[garde(dive)]
    pub commission: Option<Commission>,
    #[garde(inner(range(min = 0.0)))]
    pub stop_loss: Option<f64>,
    #[garde(inner(range(min = 0.0)))]
    pub take_profit: Option<f64>,
    #[garde(inner(range(min = 1)))]
    pub max_hold_days: Option<i32>,
    #[garde(range(min = 0.01))]
    pub capital: f64,
    #[garde(range(min = 1))]
    pub quantity: i32,
    #[serde(default = "default_multiplier")]
    #[garde(range(min = 1))]
    pub multiplier: i32,
    #[garde(range(min = 1))]
    pub max_positions: i32,
    #[serde(default)]
    #[garde(skip)]
    pub selector: TradeSelector,
    #[serde(default)]
    #[garde(skip)]
    pub adjustment_rules: Vec<AdjustmentRule>,
    /// Optional entry signal — only enter trades on dates where this signal is active
    #[serde(default)]
    #[garde(skip)]
    pub entry_signal: Option<SignalSpec>,
    /// Optional exit signal — close open positions on dates where this signal is active
    #[serde(default)]
    #[garde(skip)]
    pub exit_signal: Option<SignalSpec>,
    /// Path to OHLCV parquet file (auto-resolved by server from cached price data)
    #[serde(default)]
    #[garde(skip)]
    pub ohlcv_path: Option<String>,
}

fn default_multiplier() -> i32 {
    100
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
pub struct CompareParams {
    #[garde(length(min = 2), dive)]
    pub strategies: Vec<CompareEntry>,
    #[garde(dive)]
    pub sim_params: SimParams,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
pub struct CompareEntry {
    #[garde(length(min = 1))]
    pub name: String,
    #[garde(length(min = 1), dive)]
    pub leg_deltas: Vec<TargetRange>,
    #[garde(range(min = 1))]
    pub max_entry_dte: i32,
    #[garde(range(min = 0), custom(validate_exit_dte_lt_max(&self.max_entry_dte)))]
    pub exit_dte: i32,
    #[garde(dive)]
    pub slippage: Slippage,
    #[serde(default)]
    #[garde(dive)]
    pub commission: Option<Commission>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
pub struct SimParams {
    #[garde(range(min = 0.01))]
    pub capital: f64,
    #[garde(range(min = 1))]
    pub quantity: i32,
    #[serde(default = "default_multiplier")]
    #[garde(range(min = 1))]
    pub multiplier: i32,
    #[garde(range(min = 1))]
    pub max_positions: i32,
    #[serde(default)]
    #[garde(skip)]
    pub selector: TradeSelector,
    #[garde(inner(range(min = 0.0)))]
    pub stop_loss: Option<f64>,
    #[garde(inner(range(min = 0.0)))]
    pub take_profit: Option<f64>,
    #[garde(inner(range(min = 1)))]
    pub max_hold_days: Option<i32>,
}

// Output types
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GroupStats {
    pub dte_range: String,
    pub delta_range: String,
    pub count: usize,
    pub mean: f64,
    pub std: f64,
    pub min: f64,
    pub q25: f64,
    pub median: f64,
    pub q75: f64,
    pub max: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BacktestResult {
    pub trade_count: usize,
    pub total_pnl: f64,
    pub metrics: PerformanceMetrics,
    pub equity_curve: Vec<EquityPoint>,
    pub trade_log: Vec<TradeRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PerformanceMetrics {
    pub sharpe: f64,
    pub sortino: f64,
    pub max_drawdown: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub calmar: f64,
    pub var_95: f64,
    pub total_return_pct: f64,
    pub cagr: f64,
    pub avg_trade_pnl: f64,
    pub avg_winner: f64,
    pub avg_loser: f64,
    pub avg_days_held: f64,
    pub max_consecutive_losses: usize,
    pub expectancy: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EquityPoint {
    pub datetime: NaiveDateTime,
    pub equity: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TradeRecord {
    pub trade_id: usize,
    pub entry_datetime: NaiveDateTime,
    pub exit_datetime: NaiveDateTime,
    pub entry_cost: f64,
    pub exit_proceeds: f64,
    pub pnl: f64,
    pub days_held: i64,
    pub exit_type: ExitType,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CompareResult {
    pub strategy: String,
    pub trades: usize,
    pub pnl: f64,
    pub sharpe: f64,
    pub sortino: f64,
    pub max_dd: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub calmar: f64,
    pub total_return_pct: f64,
}

// --- Event-driven simulation types ---

/// Key for looking up option quotes: (`quote_date`, expiration, strike, `option_type`)
pub type PriceKey = (NaiveDate, NaiveDate, OrderedFloat<f64>, OptionType);

/// Lookup table mapping `PriceKey` to quote snapshot
pub type PriceTable = HashMap<PriceKey, QuoteSnapshot>;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct QuoteSnapshot {
    pub bid: f64,
    pub ask: f64,
    pub delta: f64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Position {
    pub id: usize,
    pub entry_date: NaiveDate,
    pub expiration: NaiveDate,
    pub secondary_expiration: Option<NaiveDate>,
    pub legs: Vec<PositionLeg>,
    pub entry_cost: f64,
    pub quantity: i32,
    pub multiplier: i32,
    pub status: PositionStatus,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PositionLeg {
    pub leg_index: usize,
    pub side: Side,
    pub option_type: OptionType,
    pub strike: f64,
    pub expiration: NaiveDate,
    pub entry_price: f64,
    pub qty: i32,
    pub closed: bool,
    pub close_price: Option<f64>,
    pub close_date: Option<NaiveDate>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum PositionStatus {
    Open,
    Closed(ExitType),
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct EntryCandidate {
    pub entry_date: NaiveDate,
    pub expiration: NaiveDate,
    pub secondary_expiration: Option<NaiveDate>,
    pub legs: Vec<CandidateLeg>,
    pub net_premium: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CandidateLeg {
    pub option_type: OptionType,
    pub strike: f64,
    pub expiration: NaiveDate,
    pub bid: f64,
    pub ask: f64,
    pub delta: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub enum AdjustmentAction {
    Close {
        position_id: usize,
        leg_index: usize,
    },
    Roll {
        position_id: usize,
        leg_index: usize,
        new_strike: f64,
        new_expiration: NaiveDate,
    },
    Add {
        position_id: usize,
        leg: CandidateLeg,
        side: Side,
        qty: i32,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub enum AdjustmentTrigger {
    DefensiveRoll { loss_threshold: f64 },
    CalendarRoll { dte_trigger: i32, new_dte: i32 },
    DeltaDrift { leg_index: usize, max_delta: f64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AdjustmentRule {
    pub trigger: AdjustmentTrigger,
    pub action: AdjustmentAction,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn side_multiplier_long() {
        assert!((Side::Long.multiplier() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn side_multiplier_short() {
        assert!((Side::Short.multiplier() - (-1.0)).abs() < f64::EPSILON);
    }

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
        // 1.00 + 0.65 * 5 = 4.25
        assert!((c.calculate(5) - 4.25).abs() < 1e-10);
    }

    #[test]
    fn commission_min_fee() {
        let c = Commission {
            per_contract: 0.10,
            base_fee: 0.0,
            min_fee: 5.00,
        };
        // 0.10 * 1 = 0.10, but min is 5.00
        assert!((c.calculate(1) - 5.00).abs() < 1e-10);
    }

    #[test]
    fn commission_min_fee_not_applied_when_above() {
        let c = Commission {
            per_contract: 1.00,
            base_fee: 5.00,
            min_fee: 2.00,
        };
        // 5.00 + 1.00 * 3 = 8.00 > 2.00, so min not relevant
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

    // --- Validation tests ---

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

    #[test]
    fn backtest_params_rejects_negative_capital() {
        let p = BacktestParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.5,
                min: 0.2,
                max: 0.8,
            }],
            max_entry_dte: 45,
            exit_dte: 0,
            slippage: Slippage::Mid,
            commission: None,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: -1000.0,
            quantity: 1,
            multiplier: 100,
            max_positions: 1,
            selector: TradeSelector::default(),
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
        };
        assert!(p.validate().is_err());
    }

    #[test]
    fn backtest_params_rejects_zero_quantity() {
        let p = BacktestParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.5,
                min: 0.2,
                max: 0.8,
            }],
            max_entry_dte: 45,
            exit_dte: 0,
            slippage: Slippage::Mid,
            commission: None,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10_000.0,
            quantity: 0,
            multiplier: 100,
            max_positions: 1,
            selector: TradeSelector::default(),
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
        };
        assert!(p.validate().is_err());
    }

    #[test]
    fn backtest_params_accepts_stop_loss_above_one() {
        let p = BacktestParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.5,
                min: 0.2,
                max: 0.8,
            }],
            max_entry_dte: 45,
            exit_dte: 0,
            slippage: Slippage::Mid,
            commission: None,
            stop_loss: Some(2.0),
            take_profit: None,
            max_hold_days: None,
            capital: 10_000.0,
            quantity: 1,
            multiplier: 100,
            max_positions: 1,
            selector: TradeSelector::default(),
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
        };
        assert!(p.validate().is_ok());
    }

    #[test]
    fn sim_params_rejects_zero_max_positions() {
        let p = SimParams {
            capital: 10_000.0,
            quantity: 1,
            multiplier: 100,
            max_positions: 0,
            selector: TradeSelector::default(),
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
        };
        assert!(p.validate().is_err());
    }

    #[test]
    fn backtest_params_rejects_empty_strategy() {
        let p = BacktestParams {
            strategy: String::new(),
            leg_deltas: vec![TargetRange {
                target: 0.5,
                min: 0.2,
                max: 0.8,
            }],
            max_entry_dte: 45,
            exit_dte: 0,
            slippage: Slippage::Mid,
            commission: None,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10_000.0,
            quantity: 1,
            multiplier: 100,
            max_positions: 1,
            selector: TradeSelector::default(),
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
        };
        assert!(p.validate().is_err());
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
    fn evaluate_params_rejects_exit_dte_gte_max_entry_dte() {
        let p = EvaluateParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.5,
                min: 0.2,
                max: 0.8,
            }],
            max_entry_dte: 30,
            exit_dte: 45,
            dte_interval: 7,
            delta_interval: 0.05,
            slippage: Slippage::Mid,
            commission: None,
        };
        assert!(p.validate().is_err());
    }
}
