//! AI response formatting layer.
//!
//! Transforms raw engine results into enriched response structs with natural-language
//! summaries, key findings, and suggested next steps for LLM consumption.

mod advanced;
mod backtest;
mod data;

pub use advanced::{
    format_permutation_test, format_permutation_test_stock, format_sweep, format_walk_forward,
};
pub use backtest::{format_backtest, format_compare, format_stock_backtest, format_stock_compare};
pub use data::{format_raw_prices, format_strategies};
