//! AI response formatting layer.
//!
//! Transforms raw engine results into enriched response structs with natural-language
//! summaries, key findings, and suggested next steps for LLM consumption.

mod advanced;
mod backtest;
mod data;
mod hypothesis;
mod portfolio;
mod stats;
mod wheel;

pub use advanced::{
    format_bayesian, format_permutation_test, format_permutation_test_stock, format_sweep,
    format_walk_forward,
};
pub use backtest::{format_backtest, format_compare, format_stock_backtest, format_stock_compare};
pub use data::{format_list_symbols, format_raw_prices, format_strategies};
pub use hypothesis::format_hypotheses;
pub use portfolio::format_portfolio;
pub use stats::{
    format_aggregate_prices, format_correlate, format_distribution, format_regime_detect,
    format_rolling_metric,
};
pub use wheel::format_wheel_backtest;
