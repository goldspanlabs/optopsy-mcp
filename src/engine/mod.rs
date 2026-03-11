//! Options backtesting engine.
//!
//! Contains two main execution paths: `evaluate_strategy` for fast statistical
//! analysis grouped by DTE x delta buckets, and `run_backtest` for full
//! event-driven day-by-day simulation with position management. Supporting
//! modules handle pricing/slippage, performance metrics, parameter sweeps,
//! walk-forward validation, permutation tests, and strike ordering rules.

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
pub mod sizing;
pub mod stock_sim;
pub mod sweep;
pub mod sweep_analysis;
pub mod types;
pub mod vectorized_sim;
pub mod walk_forward;
