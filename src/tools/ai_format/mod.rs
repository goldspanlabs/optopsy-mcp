//! AI response formatting layer.
//!
//! Transforms raw engine results into enriched response structs with natural-language
//! summaries, key findings, and suggested next steps for LLM consumption.

mod advanced;
mod data;
mod hypothesis;
mod stats;

pub use advanced::{
    format_bayesian, format_permutation_test, format_permutation_test_stock, format_sweep,
    format_walk_forward,
};
pub use data::{format_list_symbols, format_raw_prices, format_strategies};
pub use hypothesis::format_hypotheses;
pub use stats::{
    format_aggregate_prices, format_correlate, format_distribution, format_regime_detect,
    format_rolling_metric,
};
