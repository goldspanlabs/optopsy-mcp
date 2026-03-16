//! Core type definitions shared across the backtesting engine.
//!
//! Contains strategy definitions, parameter structs (with `garde` validation),
//! simulation result types, trade records, and enum variants for sides,
//! option types, slippage models, exit types, and trade selectors.

use chrono::{NaiveDateTime, NaiveTime};
use garde::Validate;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::signals::registry::SignalSpec;

/// Days-from-epoch offset: Polars stores `Date` as days since 1970-01-01 (Unix epoch).
/// `chrono::NaiveDate::from_num_days_from_ce` counts from day 1 CE, which is day 719 163
/// relative to the Unix epoch. Add this constant to a Polars date value before passing
/// it to `from_num_days_from_ce_opt`.
pub const EPOCH_DAYS_CE_OFFSET: i32 = 719_163;

/// Bar interval for OHLCV resampling.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Interval {
    #[default]
    Daily,
    Weekly,
    Monthly,
    /// 1-minute bars
    #[serde(rename = "1m")]
    Min1,
    /// 5-minute bars
    #[serde(rename = "5m")]
    Min5,
    /// 30-minute bars
    #[serde(rename = "30m")]
    Min30,
    /// 1-hour bars
    #[serde(rename = "1h")]
    Hour1,
}

impl Interval {
    /// Approximate number of bars per trading year for annualization.
    ///
    /// Intraday counts assume a 6.5-hour regular session (390 minutes) × 252 trading days.
    /// `Hour1` uses 7 bars/day because hour-truncated bucketing produces bars at hours
    /// 9, 10, 11, 12, 13, 14, 15 for a 09:30–16:00 session.
    pub fn bars_per_year(self) -> f64 {
        match self {
            Self::Daily => 252.0,
            Self::Weekly => 52.0,
            Self::Monthly => 12.0,
            Self::Min1 => 252.0 * 390.0,
            Self::Min5 => 252.0 * 78.0,
            Self::Min30 => 252.0 * 13.0,
            Self::Hour1 => 252.0 * 7.0,
        }
    }

    /// Whether this interval represents intraday data.
    pub fn is_intraday(self) -> bool {
        matches!(self, Self::Min1 | Self::Min5 | Self::Min30 | Self::Hour1)
    }
}

impl std::fmt::Display for Interval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Daily => write!(f, "daily"),
            Self::Weekly => write!(f, "weekly"),
            Self::Monthly => write!(f, "monthly"),
            Self::Min1 => write!(f, "1m"),
            Self::Min5 => write!(f, "5m"),
            Self::Min30 => write!(f, "30m"),
            Self::Hour1 => write!(f, "1h"),
        }
    }
}

/// Trading session time-of-day filter for intraday data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub enum SessionFilter {
    /// Pre-market session: 04:00 – 09:30 ET
    Premarket,
    /// Regular trading hours: 09:30 – 16:00 ET
    RegularHours,
    /// After-hours session: 16:00 – 20:00 ET
    AfterHours,
    /// Full extended hours: 04:00 – 20:00 ET
    ExtendedHours,
}

impl SessionFilter {
    /// Return the `(start, end)` time range for this session (half-open: `[start, end)`).
    pub fn time_range(self) -> (NaiveTime, NaiveTime) {
        match self {
            Self::Premarket => (
                NaiveTime::from_hms_opt(4, 0, 0).unwrap(),
                NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
            ),
            Self::RegularHours => (
                NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
                NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
            ),
            Self::AfterHours => (
                NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
                NaiveTime::from_hms_opt(20, 0, 0).unwrap(),
            ),
            Self::ExtendedHours => (
                NaiveTime::from_hms_opt(4, 0, 0).unwrap(),
                NaiveTime::from_hms_opt(20, 0, 0).unwrap(),
            ),
        }
    }
}

/// Market direction bias for a strategy (bullish, bearish, neutral, or volatile).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Bullish,
    Bearish,
    Neutral,
    Volatile,
}

/// Look up the market direction bias for a named strategy, defaulting to `Neutral` if unknown.
pub fn strategy_direction(name: &str) -> Direction {
    if let Some(def) = crate::strategies::find_strategy(name) {
        return def.direction;
    }
    tracing::warn!(
        strategy = name,
        "Unknown strategy — defaulting to Neutral direction"
    );
    Direction::Neutral
}

/// Position direction: long (+1) or short (-1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum Side {
    Long = 1,
    Short = -1,
}

impl Side {
    /// Return the numeric multiplier: `1.0` for Long, `-1.0` for Short.
    pub fn multiplier(self) -> f64 {
        match self {
            Side::Long => 1.0,
            Side::Short => -1.0,
        }
    }

    /// Return the opposite side (Long becomes Short and vice versa).
    #[must_use]
    pub fn flip(self) -> Self {
        match self {
            Side::Long => Side::Short,
            Side::Short => Side::Long,
        }
    }
}

/// Option contract type: call or put.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
pub enum OptionType {
    Call,
    Put,
}

impl OptionType {
    /// Return the lowercase string representation (`"call"` or `"put"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            OptionType::Call => "call",
            OptionType::Put => "put",
        }
    }
}

/// Expiration cycle tag for multi-expiration strategies (calendar/diagonal).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ExpirationCycle {
    #[default]
    Primary, // Near-term (or same-expiration for non-calendar strategies)
    Secondary, // Far-term (calendar/diagonal only)
}

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
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
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

/// Filter entries by expiration calendar type.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default, PartialEq)]
pub enum ExpirationFilter {
    /// Accept any expiration (default).
    #[default]
    Any,
    /// Accept only expirations that fall on a Friday (weekly options).
    Weekly,
    /// Accept only expirations on the third Friday of the month (standard monthly cycle).
    Monthly,
}

/// Strategy for choosing among multiple entry candidates on the same date.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
pub enum TradeSelector {
    #[default]
    Nearest,
    HighestPremium,
    LowestPremium,
    First,
}

/// Reason a position was closed during simulation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub enum ExitType {
    Expiration,
    StopLoss,
    TakeProfit,
    MaxHold,
    DteExit,
    Adjustment,
    Signal,
    /// Exit triggered when the absolute net position delta exceeds `exit_net_delta`.
    DeltaExit,
}

/// Definition of a single leg within a strategy template.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct LegDef {
    pub side: Side,
    pub option_type: OptionType,
    pub delta: TargetRange,
    /// Number of contracts per unit of the strategy (e.g. 2 for butterfly body).
    pub qty: i32,
    /// Which expiration cycle this leg belongs to (`Primary` for near-term,
    /// `Secondary` for far-term in calendar/diagonal strategies).
    pub expiration_cycle: ExpirationCycle,
}

/// Convert a `snake_case` strategy name to Title Case (e.g. `"short_put"` → `"Short Put"`).
pub fn to_display_name(name: &str) -> String {
    name.split('_')
        .map(|w| {
            let mut c = w.chars();
            match c.next() {
                None => String::new(),
                Some(f) => {
                    let upper: String = f.to_uppercase().collect();
                    upper + c.as_str()
                }
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Complete definition of a named options strategy with its leg templates.
#[derive(Debug, Clone)]
pub struct StrategyDef {
    /// Internal `snake_case` identifier (e.g. `"iron_condor"`).
    pub name: String,
    pub category: String,
    pub description: String,
    /// Ordered leg definitions; leg count determines the strategy structure.
    pub legs: Vec<LegDef>,
    /// When `false`, adjacent legs may share the same strike (e.g. straddles,
    /// iron butterflies). When `true` (default), strikes must be strictly ascending.
    pub strict_strike_order: bool,
    pub direction: Direction,
    /// When `true`, the strategy includes a long stock leg (e.g. covered call, protective put).
    /// The engine will track stock entry/exit prices and include stock P&L in the trade.
    pub has_stock_leg: bool,
}

impl StrategyDef {
    /// Returns true if this strategy has legs with different expiration cycles.
    pub fn is_multi_expiration(&self) -> bool {
        self.legs
            .iter()
            .any(|l| l.expiration_cycle == ExpirationCycle::Secondary)
    }

    /// Returns the per-leg default delta targets embedded in the strategy definition.
    pub fn default_deltas(&self) -> Vec<TargetRange> {
        self.legs.iter().map(|l| l.delta.clone()).collect()
    }
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
    /// Auto-resolved by the server when `CrossSymbol` signal variants are present.
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

pub(crate) fn default_multiplier() -> i32 {
    100
}

pub(crate) fn default_min_bid_ask() -> f64 {
    0.05
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
    /// Entry signal — only open trades on dates where this TA signal fires.
    /// Requires OHLCV data (call `fetch_to_parquet` first).
    #[serde(default)]
    #[garde(skip)]
    pub entry_signal: Option<SignalSpec>,
    /// Exit signal — close open positions on dates where this TA signal fires.
    /// Requires OHLCV data (call `fetch_to_parquet` first).
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
    /// Exit when the absolute net position delta exceeds this value.
    #[serde(default)]
    #[garde(inner(range(min = 0.0)))]
    pub exit_net_delta: Option<f64>,
}

/// Internal quality stats collected during backtest simulation for data coverage reporting.
#[derive(Debug, Clone, Default)]
pub struct BacktestQualityStats {
    pub trading_days_total: usize,
    /// Trading days that had at least one price observation in the price table.
    pub trading_days_with_data: usize,
    pub total_candidates: usize,
    pub positions_opened: usize,
    /// Sampled bid-ask spread percentages, used to compute median spread for quality warnings.
    pub entry_spread_pcts: Vec<f64>,
}

/// Output of a single backtest run: trades, metrics, equity curve, and quality stats.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BacktestResult {
    pub trade_count: usize,
    pub total_pnl: f64,
    pub metrics: PerformanceMetrics,
    pub equity_curve: Vec<EquityPoint>,
    pub trade_log: Vec<TradeRecord>,
    /// Internal quality diagnostics collected during simulation (not serialized to clients).
    #[serde(skip)]
    pub quality: BacktestQualityStats,
    /// Diagnostic warnings surfaced to callers (e.g. entries skipped due to insufficient capital).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// Aggregate performance metrics derived from the equity curve and trade log.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PerformanceMetrics {
    pub sharpe: f64,
    pub sortino: f64,
    /// Maximum peak-to-trough drawdown as a fraction of peak equity.
    pub max_drawdown: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub calmar: f64,
    /// Value at Risk at the 95% confidence level (5th percentile of daily returns).
    pub var_95: f64,
    pub total_return_pct: f64,
    pub cagr: f64,
    pub avg_trade_pnl: f64,
    pub avg_winner: f64,
    /// Average P&L of losing trades (negative value).
    pub avg_loser: f64,
    pub avg_days_held: f64,
    pub max_consecutive_losses: usize,
    /// Expected value per trade: `win_rate * avg_winner + (1 - win_rate) * avg_loser`.
    pub expectancy: f64,
}

/// Single point on the equity curve representing portfolio value at a moment in time.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EquityPoint {
    pub datetime: NaiveDateTime,
    pub equity: f64,
}

/// Label indicating whether a cashflow is a credit (received) or debit (paid).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub enum CashflowLabel {
    CR,
    DR,
}

/// Snapshot of a single leg within a completed trade record.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LegDetail {
    pub side: Side,
    pub option_type: OptionType,
    pub strike: f64,
    pub expiration: String,
    pub entry_price: f64,
    /// Fill price at exit (`None` if the option expired worthless or was never closed).
    pub exit_price: Option<f64>,
    pub qty: i32,
}

/// Complete record of a single round-trip trade (entry through exit).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TradeRecord {
    pub trade_id: usize,
    pub entry_datetime: NaiveDateTime,
    pub exit_datetime: NaiveDateTime,
    /// Signed entry cost: negative for credits received, positive for debits paid.
    pub entry_cost: f64,
    /// Signed exit proceeds: positive for credits received, negative for debits paid.
    pub exit_proceeds: f64,
    pub entry_amount: f64,
    pub entry_label: CashflowLabel,
    pub exit_amount: f64,
    pub exit_label: CashflowLabel,
    pub pnl: f64,
    pub days_held: i64,
    pub exit_type: ExitType,
    pub legs: Vec<LegDetail>,
    /// Dynamically computed quantity (set when position sizing is active).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub computed_quantity: Option<i32>,
    /// Portfolio equity at the time of entry (set when position sizing is active).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_equity: Option<f64>,
    /// Per-share stock entry price (set when strategy has a stock leg).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stock_entry_price: Option<f64>,
    /// Per-share stock exit price (set when strategy has a stock leg).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stock_exit_price: Option<f64>,
    /// Stock leg P&L (set when strategy has a stock leg).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stock_pnl: Option<f64>,
}

impl TradeRecord {
    /// Construct a new trade record, automatically deriving cashflow labels from cost signs.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        trade_id: usize,
        entry_datetime: NaiveDateTime,
        exit_datetime: NaiveDateTime,
        entry_cost: f64,
        exit_proceeds: f64,
        pnl: f64,
        days_held: i64,
        exit_type: ExitType,
        legs: Vec<LegDetail>,
    ) -> Self {
        Self {
            trade_id,
            entry_datetime,
            exit_datetime,
            entry_amount: entry_cost.abs(),
            entry_label: if entry_cost < 0.0 {
                CashflowLabel::CR
            } else {
                CashflowLabel::DR
            },
            exit_amount: exit_proceeds.abs(),
            exit_label: if exit_proceeds > 0.0 {
                CashflowLabel::CR
            } else {
                CashflowLabel::DR
            },
            entry_cost,
            exit_proceeds,
            pnl,
            days_held,
            exit_type,
            legs,
            computed_quantity: None,
            entry_equity: None,
            stock_entry_price: None,
            stock_exit_price: None,
            stock_pnl: None,
        }
    }
}

/// Summary metrics for one strategy in a multi-strategy comparison.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CompareResult {
    pub strategy: String,
    pub display_name: String,
    pub trades: usize,
    pub pnl: f64,
    pub sharpe: f64,
    pub sortino: f64,
    pub max_dd: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub calmar: f64,
    pub total_return_pct: f64,
    /// Full trade log, included for equity curve overlays in the frontend.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub trade_log: Vec<TradeRecord>,
    /// If the backtest failed, contains the error message instead of metrics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Result of a single parameter sweep combination.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SweepResult {
    /// Human-readable label encoding strategy, deltas, DTE, and slippage.
    pub label: String,
    pub strategy: String,
    pub display_name: String,
    pub leg_deltas: Vec<TargetRange>,
    pub entry_dte: DteRange,
    pub exit_dte: i32,
    pub slippage: Slippage,
    pub trades: usize,
    pub pnl: f64,
    pub sharpe: f64,
    pub sortino: f64,
    pub max_dd: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub calmar: f64,
    pub total_return_pct: f64,
    /// Count of distinct entry dates, used to assess statistical independence.
    pub independent_entry_periods: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_signal: Option<SignalSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_signal: Option<SignalSpec>,
    /// Dimension key pairs for sensitivity analysis (not serialized to clients).
    #[serde(skip)]
    pub signal_dim_keys: Vec<(String, String)>,
    /// Raw (unadjusted) Sharpe p-value from permutation test, if `num_permutations` was set.
    /// Use the `multiple_comparisons` field in the sweep response for corrected values.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p_value: Option<f64>,
    /// Sizing method name for identification in sweep results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sizing: Option<String>,
}

// --- Event-driven simulation types (re-exported from sim_types) ---
pub use super::sim_types::{
    AdjustmentAction, AdjustmentRule, AdjustmentTrigger, CandidateLeg, DateIndex, EntryCandidate,
    LastKnown, Position, PositionLeg, PositionStatus, PriceKey, PriceTable, QuoteSnapshot,
    SimContext, SimState,
};

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
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: HashMap::new(),
            min_days_between_entries: None,
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
    fn strategy_direction_bullish() {
        assert_eq!(strategy_direction("long_call"), Direction::Bullish);
        assert_eq!(strategy_direction("short_put"), Direction::Bullish);
        assert_eq!(strategy_direction("covered_call"), Direction::Bullish);
        assert_eq!(strategy_direction("bull_call_spread"), Direction::Bullish);
        assert_eq!(strategy_direction("bull_put_spread"), Direction::Bullish);
    }

    #[test]
    fn strategy_direction_bearish() {
        assert_eq!(strategy_direction("short_call"), Direction::Bearish);
        assert_eq!(strategy_direction("long_put"), Direction::Bearish);
        assert_eq!(strategy_direction("bear_call_spread"), Direction::Bearish);
        assert_eq!(strategy_direction("bear_put_spread"), Direction::Bearish);
    }

    #[test]
    fn strategy_direction_volatile() {
        assert_eq!(strategy_direction("long_straddle"), Direction::Volatile);
        assert_eq!(strategy_direction("long_strangle"), Direction::Volatile);
        assert_eq!(
            strategy_direction("reverse_iron_condor"),
            Direction::Volatile
        );
        assert_eq!(
            strategy_direction("reverse_iron_butterfly"),
            Direction::Volatile
        );
    }

    #[test]
    fn strategy_direction_neutral() {
        assert_eq!(strategy_direction("iron_condor"), Direction::Neutral);
        assert_eq!(strategy_direction("short_straddle"), Direction::Neutral);
        assert_eq!(
            strategy_direction("long_call_butterfly"),
            Direction::Neutral
        );
        assert_eq!(strategy_direction("short_put_condor"), Direction::Neutral);
    }

    #[test]
    fn strategy_direction_all_32_covered() {
        let all = crate::strategies::all_strategies();
        for s in all {
            // Just ensure it doesn't panic and returns a valid variant
            let dir = strategy_direction(&s.name);
            assert!(
                matches!(
                    dir,
                    Direction::Bullish
                        | Direction::Bearish
                        | Direction::Neutral
                        | Direction::Volatile
                ),
                "strategy {} returned unexpected direction",
                s.name
            );
        }
    }
}
