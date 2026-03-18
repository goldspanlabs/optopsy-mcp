//! Tool modules for the MCP server.
//!
//! Each submodule implements a single MCP tool, delegating to the engine layer for
//! computation and returning AI-enriched response types for LLM consumption.

pub mod aggregate_prices;
pub mod ai_format;
pub mod ai_helpers;
pub mod backtest;
pub mod build_signal;
pub mod compare;
pub mod construct_signal;
pub mod correlate;
pub mod distribution;
pub mod list_symbols;
pub mod permutation_test;
pub mod raw_prices;
pub mod regime_detect;
pub mod response_types;
pub mod rolling_metric;
pub mod signals;
pub mod stock_backtest;
pub mod strategies;
pub mod sweep;
pub mod walk_forward;
