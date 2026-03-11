use chrono::NaiveDate;
use ordered_float::OrderedFloat;
use rustc_hash::FxBuildHasher;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::types::{ExitType, OptionType, Side};

/// Key for looking up option quotes: (`quote_date`, expiration, strike, `option_type`)
pub type PriceKey = (NaiveDate, NaiveDate, OrderedFloat<f64>, OptionType);

/// Lookup table mapping `PriceKey` to quote snapshot (`FxHash` for speed on fixed-size keys)
pub type PriceTable = HashMap<PriceKey, QuoteSnapshot, FxBuildHasher>;

/// Secondary index: maps each trading date to its price table keys for O(1) daily lookups.
pub type DateIndex = HashMap<NaiveDate, Vec<PriceKey>>;

#[derive(Debug, Clone)]
pub struct QuoteSnapshot {
    pub bid: f64,
    pub ask: f64,
    pub delta: f64,
}

#[derive(Debug, Clone)]
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
pub enum PositionStatus {
    Open,
    Closed(ExitType),
}

#[derive(Debug, Clone)]
pub struct EntryCandidate {
    pub entry_date: NaiveDate,
    pub expiration: NaiveDate,
    pub secondary_expiration: Option<NaiveDate>,
    pub legs: Vec<CandidateLeg>,
    pub net_premium: f64,
    /// Signed net position delta: sum of (delta × `side_multiplier` × qty) for each leg.
    pub net_delta: f64,
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
