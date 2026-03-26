//! Parameter structs for backtests, comparisons, and sweeps.

use garde::Validate;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::enums::{ConflictResolution, ExpirationFilter, TradeSelector};
use super::pricing::{
    validate_exit_dte_lt_entry_min, Commission, DteRange, SizingConfig, Slippage, TargetRange,
};
use super::signal_spec::SignalSpec;

use crate::engine::sim_types::AdjustmentRule;

pub(crate) fn default_multiplier() -> i32 {
    100
}

pub(crate) fn default_min_bid_ask() -> f64 {
    0.05
}

/// Full parameter set for running an event-driven backtest simulation.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
pub struct BacktestParams {
    /// Strategy name (must match a registered strategy, e.g. `"iron_condor"`).
    #[garde(length(min = 1))]
    pub strategy: String,
    /// Per-leg delta targets; length must equal the strategy's leg count.
    #[garde(length(min = 1), dive)]
    pub leg_deltas: Vec<TargetRange>,
    /// Entry DTE range (target, min, max).
    #[garde(dive)]
    pub entry_dte: DteRange,
    /// Close positions when DTE falls to this value; must be < `entry_dte.min`.
    #[garde(range(min = 0), custom(validate_exit_dte_lt_entry_min(&self.entry_dte)))]
    pub exit_dte: i32,
    /// Slippage model for fill price calculation.
    #[garde(dive)]
    pub slippage: Slippage,
    /// Optional commission schedule applied at entry and exit.
    #[serde(default)]
    #[garde(dive)]
    pub commission: Option<Commission>,
    /// Minimum bid/ask threshold; quotes below this are filtered out.
    #[serde(default = "default_min_bid_ask")]
    #[garde(range(min = 0.0))]
    pub min_bid_ask: f64,
    /// Stop-loss threshold as a fraction of entry cost (e.g. `0.50` = 50% loss).
    #[garde(inner(range(min = 0.0)))]
    pub stop_loss: Option<f64>,
    /// Take-profit threshold as a fraction of entry cost (e.g. `0.80` = 80% gain).
    #[garde(inner(range(min = 0.0)))]
    pub take_profit: Option<f64>,
    /// Force-close positions after this many calendar days.
    #[garde(inner(range(min = 1)))]
    pub max_hold_days: Option<i32>,
    /// Starting equity for the simulation.
    #[garde(range(min = 0.01))]
    pub capital: f64,
    /// Number of contracts per trade entry.
    #[garde(range(min = 1))]
    pub quantity: i32,
    /// Dynamic position sizing configuration. When set, overrides fixed `quantity`
    /// with a computed value based on equity, risk, or volatility.
    #[serde(default)]
    #[garde(dive)]
    pub sizing: Option<SizingConfig>,
    /// Contract multiplier (typically 100 for equity options).
    #[serde(default = "default_multiplier")]
    #[garde(range(min = 1))]
    pub multiplier: i32,
    /// Maximum number of simultaneously open positions.
    #[garde(range(min = 1))]
    pub max_positions: i32,
    /// Strategy for choosing among multiple candidates on the same date.
    #[serde(default)]
    #[garde(skip)]
    pub selector: TradeSelector,
    /// Adjustment rules evaluated each day on open positions.
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
    /// Paths to OHLCV parquet files for cross-symbol signals (symbol → path).
    /// Auto-resolved by the server when cross-symbol formula references are present.
    #[serde(default)]
    #[garde(skip)]
    pub cross_ohlcv_paths: HashMap<String, String>,

    // ── Entry filters ────────────────────────────────────────────────────────
    /// Minimum absolute net premium at entry (credit or debit, in dollars per share).
    /// Filters out candidates whose `abs(net_premium)` is below this threshold.
    #[serde(default)]
    #[garde(inner(range(min = 0.0)))]
    pub min_net_premium: Option<f64>,
    /// Maximum absolute net premium at entry.
    /// Filters out candidates whose `abs(net_premium)` exceeds this threshold.
    #[serde(default)]
    #[garde(inner(range(min = 0.0)))]
    pub max_net_premium: Option<f64>,
    /// Minimum signed net position delta at entry (sum of per-leg delta × side × qty).
    /// Use to enforce directional or near-neutral entry requirements.
    #[serde(default)]
    #[garde(skip)]
    pub min_net_delta: Option<f64>,
    /// Maximum signed net position delta at entry.
    #[serde(default)]
    #[garde(skip)]
    pub max_net_delta: Option<f64>,
    /// Minimum calendar days that must elapse between consecutive position entries.
    /// Prevents entering a new trade immediately after a prior entry (stagger / cooldown).
    #[serde(default)]
    #[garde(inner(range(min = 1)))]
    pub min_days_between_entries: Option<i32>,
    /// Filter expirations by calendar type: `Any` (default), `Weekly` (Fridays),
    /// or `Monthly` (third Friday of the month).
    #[serde(default)]
    #[garde(skip)]
    pub expiration_filter: ExpirationFilter,

    // ── Exit filters ─────────────────────────────────────────────────────────
    /// Exit the position when the absolute net position delta exceeds this threshold.
    /// Computed as sum of `|delta × side_multiplier × qty|` for all open legs.
    #[serde(default)]
    #[garde(inner(range(min = 0.0)))]
    pub exit_net_delta: Option<f64>,
}

/// Parameters for comparing multiple strategies side by side.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
pub struct CompareParams {
    #[garde(length(min = 2), dive)]
    pub strategies: Vec<CompareEntry>,
    #[garde(dive)]
    pub sim_params: SimParams,
}

/// A single strategy entry within a comparison request.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
pub struct CompareEntry {
    #[garde(length(min = 1))]
    pub name: String,
    #[garde(length(min = 1), dive)]
    pub leg_deltas: Vec<TargetRange>,
    #[garde(dive)]
    pub entry_dte: DteRange,
    #[garde(range(min = 0), custom(validate_exit_dte_lt_entry_min(&self.entry_dte)))]
    pub exit_dte: i32,
    #[garde(dive)]
    pub slippage: Slippage,
    #[serde(default)]
    #[garde(dive)]
    pub commission: Option<Commission>,
}

/// Shared simulation parameters used across strategy comparison and parameter sweeps.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
pub struct SimParams {
    #[garde(range(min = 0.01))]
    pub capital: f64,
    #[garde(range(min = 1))]
    pub quantity: i32,
    /// Dynamic position sizing configuration.
    #[serde(default)]
    #[garde(dive)]
    pub sizing: Option<SizingConfig>,
    /// Contract multiplier (typically 100 for equity options).
    #[serde(default = "default_multiplier")]
    #[garde(range(min = 1))]
    pub multiplier: i32,
    #[garde(range(min = 1))]
    pub max_positions: i32,
    #[serde(default)]
    #[garde(skip)]
    pub selector: TradeSelector,
    /// Stop-loss threshold as a fraction of entry cost (e.g. `0.50` = 50% loss).
    #[garde(inner(range(min = 0.0)))]
    pub stop_loss: Option<f64>,
    /// Take-profit threshold as a fraction of entry cost (e.g. `0.80` = 80% gain).
    #[garde(inner(range(min = 0.0)))]
    pub take_profit: Option<f64>,
    #[garde(inner(range(min = 1)))]
    pub max_hold_days: Option<i32>,
    /// Maximum bars to hold a position before force-closing (intraday alternative to `max_hold_days`).
    #[serde(default)]
    #[garde(inner(range(min = 1)))]
    pub max_hold_bars: Option<i32>,
    /// Entry signal — only open trades on dates where this TA signal fires.
    /// OHLCV data is auto-loaded from the local Parquet cache when signals are present.
    #[serde(default)]
    #[garde(skip)]
    pub entry_signal: Option<SignalSpec>,
    /// Exit signal — close open positions on dates where this TA signal fires.
    /// OHLCV data is auto-loaded from the local Parquet cache when signals are present.
    #[serde(default)]
    #[garde(skip)]
    pub exit_signal: Option<SignalSpec>,
    /// Path to OHLCV parquet file (resolved automatically when signals are present)
    #[serde(default)]
    #[garde(skip)]
    pub ohlcv_path: Option<String>,
    /// Paths to OHLCV parquet files for cross-symbol signals (symbol → path).
    #[serde(default)]
    #[garde(skip)]
    pub cross_ohlcv_paths: HashMap<String, String>,
    /// Minimum calendar days between consecutive position entries (cooldown / stagger).
    #[serde(default)]
    #[garde(inner(range(min = 1)))]
    pub min_days_between_entries: Option<i32>,
    /// Minimum bars between consecutive entries (intraday alternative to `min_days_between_entries`).
    #[serde(default)]
    #[garde(inner(range(min = 1)))]
    pub min_bars_between_entries: Option<i32>,
    /// How to resolve when both stop-loss and take-profit trigger on the same bar.
    #[serde(default)]
    #[garde(skip)]
    pub conflict_resolution: Option<ConflictResolution>,
    /// Exit when the absolute net position delta exceeds this value.
    #[serde(default)]
    #[garde(inner(range(min = 0.0)))]
    pub exit_net_delta: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backtest_params_rejects_negative_capital() {
        let p = BacktestParams {
            strategy: "long_call".to_string(),
            leg_deltas: vec![TargetRange {
                target: 0.5,
                min: 0.2,
                max: 0.8,
            }],
            entry_dte: DteRange {
                target: 45,
                min: 30,
                max: 60,
            },
            exit_dte: 0,
            slippage: Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: -1000.0,
            quantity: 1,
            sizing: None,
            multiplier: 100,
            max_positions: 1,
            selector: TradeSelector::default(),
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: None,
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
            entry_dte: DteRange {
                target: 45,
                min: 30,
                max: 60,
            },
            exit_dte: 0,
            slippage: Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10_000.0,
            quantity: 0,
            sizing: None,
            multiplier: 100,
            max_positions: 1,
            selector: TradeSelector::default(),
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: None,
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
            entry_dte: DteRange {
                target: 45,
                min: 30,
                max: 60,
            },
            exit_dte: 0,
            slippage: Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: Some(2.0),
            take_profit: None,
            max_hold_days: None,
            capital: 10_000.0,
            quantity: 1,
            sizing: None,
            multiplier: 100,
            max_positions: 1,
            selector: TradeSelector::default(),
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: None,
        };
        assert!(p.validate().is_ok());
    }

    #[test]
    fn sim_params_rejects_zero_max_positions() {
        let p = SimParams {
            capital: 10_000.0,
            quantity: 1,
            sizing: None,
            multiplier: 100,
            max_positions: 0,
            selector: TradeSelector::default(),
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            max_hold_bars: None,
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_days_between_entries: None,
            min_bars_between_entries: None,
            conflict_resolution: None,
            exit_net_delta: None,
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
            entry_dte: DteRange {
                target: 45,
                min: 30,
                max: 60,
            },
            exit_dte: 0,
            slippage: Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital: 10_000.0,
            quantity: 1,
            sizing: None,
            multiplier: 100,
            max_positions: 1,
            selector: TradeSelector::default(),
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: ExpirationFilter::Any,
            exit_net_delta: None,
        };
        assert!(p.validate().is_err());
    }
}
