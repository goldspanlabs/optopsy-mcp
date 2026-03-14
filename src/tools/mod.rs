//! Tool modules for the MCP server.
//!
//! Each submodule implements a single MCP tool, delegating to the engine layer for
//! computation and returning AI-enriched response types for LLM consumption.

pub mod ai_format;
pub mod ai_helpers;
pub mod backtest;
pub mod build_signal;
pub mod cache_status;
pub mod compare;
pub mod construct_signal;
pub mod fetch;
pub mod permutation_test;
pub mod raw_prices;
pub mod response_types;
pub mod signals;
pub mod status;
pub mod stock_backtest;
pub mod strategies;
pub mod sweep;
pub mod walk_forward;
