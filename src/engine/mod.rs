//! Backtesting engine core modules.
//!
//! Shared infrastructure used by the Rhai scripting engine: types, metrics,
//! pricing, filters, and data loading.

pub mod filters;
pub mod hmm;
pub mod hypothesis;
pub mod metrics;
pub mod multiple_comparisons;
pub mod ohlcv;
pub mod portfolio;
#[allow(dead_code)]
pub mod positions;
pub mod price_table;
pub mod pricing;
pub mod rules;
pub mod sim_types;
pub mod sizing;
pub mod types;
