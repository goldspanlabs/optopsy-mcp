//! Core type definitions shared across the backtesting engine.
//!
//! Contains strategy definitions, parameter structs (with `garde` validation),
//! simulation result types, trade records, and enum variants for sides,
//! option types, slippage models, exit types, and trade selectors.

mod enums;
mod hypothesis;
mod interval;
mod params;
mod pricing;
mod results;
mod signal_spec;
mod strategy;

pub use enums::*;
pub use hypothesis::*;
pub use interval::*;
pub use params::*;
pub use pricing::*;
pub use results::*;
pub use signal_spec::*;
pub use strategy::*;

// Re-export sim_types for backwards compatibility.
// These were previously `pub use super::sim_types::*` in the monolithic types.rs.
pub use crate::engine::sim_types::{
    AdjustmentAction, AdjustmentRule, AdjustmentTrigger, CandidateLeg, DateIndex, EntryCandidate,
    LastKnown, Position, PositionLeg, PositionStatus, PriceKey, PriceTable, QuoteSnapshot,
    SimContext, SimState,
};
