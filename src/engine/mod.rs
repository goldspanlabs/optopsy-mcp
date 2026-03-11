//! Options backtesting engine: strategy evaluation, event-driven simulation,
//! pricing, metrics, parameter sweeps, and supporting utilities.

pub mod adjustments;
pub mod core;
pub mod event_sim;
pub mod filters;
pub mod metrics;
pub mod multiple_comparisons;
pub mod permutation;
pub mod positions;
pub mod price_table;
pub mod pricing;
pub mod rules;
pub mod sim_types;
pub mod sweep;
pub mod sweep_analysis;
pub mod types;
pub mod vectorized_sim;
pub mod walk_forward;
