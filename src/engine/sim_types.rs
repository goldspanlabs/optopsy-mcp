//! Types for the event-driven simulation layer.
//!
//! Defines the runtime data structures used during day-by-day simulation:
//! `PriceTable` for O(1) quote lookups, `Position`/`PositionLeg` for tracking
//! open trades, `EntryCandidate` for potential entries, and adjustment
//! trigger/action enums for mid-trade position modifications.

use chrono::NaiveDate;
use ordered_float::OrderedFloat;
use rustc_hash::FxBuildHasher;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

use super::types::{
    BacktestParams, DteRange, ExitType, OptionType, Side, StrategyDef, TargetRange, TradeRecord,
};

/// Key for looking up option quotes: (`quote_date`, expiration, strike, `option_type`)
pub type PriceKey = (NaiveDate, NaiveDate, OrderedFloat<f64>, OptionType);

/// Lookup table mapping `PriceKey` to quote snapshot (`FxHash` for speed on fixed-size keys)
pub type PriceTable = HashMap<PriceKey, QuoteSnapshot, FxBuildHasher>;

/// Secondary index: maps each trading date to its price table keys for O(1) daily lookups.
pub type DateIndex = HashMap<NaiveDate, Vec<PriceKey>>;

/// Bid/ask/delta snapshot for a single option contract at a point in time.
/// `Copy` — only 24 bytes (3 × f64), cheaper to copy than ref-count.
#[derive(Debug, Clone, Copy)]
pub struct QuoteSnapshot {
    pub bid: f64,
    pub ask: f64,
    pub delta: f64,
}

/// An open or closed multi-leg options position tracked during simulation.
#[derive(Debug, Clone)]
pub struct Position {
    pub id: usize,
    pub entry_date: NaiveDate,
    pub expiration: NaiveDate,
    /// Secondary expiration, set only for calendar/diagonal strategies.
    pub secondary_expiration: Option<NaiveDate>,
    pub legs: Vec<PositionLeg>,
    /// Net signed cost at entry: negative means a net credit was received.
    pub entry_cost: f64,
    pub quantity: i32,
    /// Points per contract (typically 100 for equity options).
    pub multiplier: i32,
    pub status: PositionStatus,
    /// Per-share stock entry price (set when strategy has `has_stock_leg`).
    pub stock_entry_price: Option<f64>,
}

/// A single leg of an open position, tracking its fill and close state.
#[derive(Debug, Clone)]
pub struct PositionLeg {
    /// Index into the parent strategy's `LegDef` array.
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

/// Current lifecycle state of a position.
#[derive(Debug, Clone)]
pub enum PositionStatus {
    Open,
    Closed(ExitType),
}

/// A potential trade entry assembled from matched option legs on a given date.
#[derive(Debug, Clone)]
pub struct EntryCandidate {
    pub entry_date: NaiveDate,
    pub expiration: NaiveDate,
    pub secondary_expiration: Option<NaiveDate>,
    pub legs: Vec<CandidateLeg>,
    /// Net premium of the position: positive = debit paid, negative = credit received.
    pub net_premium: f64,
    /// Signed net position delta: sum of (delta x `side_multiplier` x qty) for each leg.
    pub net_delta: f64,
}

/// Market data for a single leg of an entry candidate.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CandidateLeg {
    pub option_type: OptionType,
    pub strike: f64,
    pub expiration: NaiveDate,
    pub bid: f64,
    pub ask: f64,
    pub delta: f64,
}

/// Action to execute when an adjustment rule triggers on an open position.
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
    /// Dynamic roll: close the old leg and open a new one at the closest
    /// available strike matching the target delta/DTE on the roll date.
    /// The engine scans the price table for valid contracts at execution time.
    RollToTarget {
        position_id: usize,
        leg_index: usize,
        target_delta: TargetRange,
        target_dte: DteRange,
    },
    Add {
        position_id: usize,
        leg: CandidateLeg,
        side: Side,
        qty: i32,
    },
}

/// Condition that must be met for an adjustment rule to fire.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub enum AdjustmentTrigger {
    DefensiveRoll { loss_threshold: f64 },
    CalendarRoll { dte_trigger: i32, new_dte: i32 },
    DeltaDrift { leg_index: usize, max_delta: f64 },
}

/// Pairing of a trigger condition with the action to take when it fires.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AdjustmentRule {
    /// Condition that activates this rule.
    pub trigger: AdjustmentTrigger,
    /// Action to execute when the trigger fires.
    pub action: AdjustmentAction,
}

/// Last-known price cache, keyed by (`expiration`, `strike`, `option_type`).
pub type LastKnown = HashMap<(NaiveDate, OrderedFloat<f64>, OptionType), QuoteSnapshot>;

/// Immutable simulation context shared across the event loop.
pub struct SimContext<'a> {
    pub price_table: &'a PriceTable,
    pub date_index: &'a DateIndex,
    pub params: &'a BacktestParams,
    pub strategy_def: &'a StrategyDef,
    pub ohlcv_closes: Option<&'a BTreeMap<NaiveDate, f64>>,
}

/// Mutable simulation state accumulated during the event loop.
pub struct SimState {
    pub trade_log: Vec<TradeRecord>,
    pub trade_id: usize,
    pub realized_equity: f64,
}
