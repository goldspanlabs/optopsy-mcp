use chrono::{NaiveDate, NaiveDateTime};
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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TargetRange {
    pub target: f64,
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Commission {
    pub per_contract: f64,
    pub base_fee: f64,
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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
#[derive(Default)]
pub enum Slippage {
    #[default]
    Mid,
    Spread,
    Liquidity {
        fill_ratio: f64,
        ref_volume: u64,
    },
    PerLeg {
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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EvaluateParams {
    pub strategy: String,
    pub leg_deltas: Vec<TargetRange>,
    pub max_entry_dte: i32,
    pub exit_dte: i32,
    pub dte_interval: i32,
    pub delta_interval: f64,
    pub slippage: Slippage,
    #[serde(default)]
    pub commission: Option<Commission>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BacktestParams {
    pub strategy: String,
    pub leg_deltas: Vec<TargetRange>,
    pub max_entry_dte: i32,
    pub exit_dte: i32,
    pub slippage: Slippage,
    #[serde(default)]
    pub commission: Option<Commission>,
    pub stop_loss: Option<f64>,
    pub take_profit: Option<f64>,
    pub max_hold_days: Option<i32>,
    pub capital: f64,
    pub quantity: i32,
    #[serde(default = "default_multiplier")]
    pub multiplier: i32,
    pub max_positions: i32,
    #[serde(default)]
    pub selector: TradeSelector,
    #[serde(default)]
    pub adjustment_rules: Vec<AdjustmentRule>,
    /// Optional entry signal — only enter trades on dates where this signal is active
    #[serde(default)]
    pub entry_signal: Option<SignalSpec>,
    /// Optional exit signal — close open positions on dates where this signal is active
    #[serde(default)]
    pub exit_signal: Option<SignalSpec>,
    /// Path to OHLCV parquet file (auto-resolved by server from cached price data)
    #[serde(default)]
    pub ohlcv_path: Option<String>,
}

fn default_multiplier() -> i32 {
    100
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CompareParams {
    pub strategies: Vec<CompareEntry>,
    pub sim_params: SimParams,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CompareEntry {
    pub name: String,
    pub leg_deltas: Vec<TargetRange>,
    pub max_entry_dte: i32,
    pub exit_dte: i32,
    pub slippage: Slippage,
    #[serde(default)]
    pub commission: Option<Commission>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SimParams {
    pub capital: f64,
    pub quantity: i32,
    #[serde(default = "default_multiplier")]
    pub multiplier: i32,
    pub max_positions: i32,
    #[serde(default)]
    pub selector: TradeSelector,
    pub stop_loss: Option<f64>,
    pub take_profit: Option<f64>,
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
}
