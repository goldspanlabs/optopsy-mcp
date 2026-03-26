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

/// Convert a raw Polars timestamp value to a `NaiveDateTime` based on the given `TimeUnit`.
///
/// Returns `None` if the value does not map to a valid timestamp.
#[inline]
pub fn timestamp_to_naive_datetime(
    raw: i64,
    tu: polars::prelude::TimeUnit,
) -> Option<chrono::NaiveDateTime> {
    use polars::prelude::TimeUnit;
    match tu {
        TimeUnit::Milliseconds => {
            chrono::DateTime::from_timestamp_millis(raw).map(|dt| dt.naive_utc())
        }
        TimeUnit::Microseconds => {
            chrono::DateTime::from_timestamp_micros(raw).map(|dt| dt.naive_utc())
        }
        TimeUnit::Nanoseconds => {
            let secs = raw.div_euclid(1_000_000_000);
            let nsecs = raw.rem_euclid(1_000_000_000) as u32;
            chrono::DateTime::from_timestamp(secs, nsecs).map(|dt| dt.naive_utc())
        }
    }
}
