//! Options backtesting engine exposed as an MCP (Model Context Protocol) server.
//!
//! Provides 13 tools for loading options chain data, evaluating strategies statistically,
//! running event-driven backtests, comparing strategies, and returning raw price data.

// float_cmp: only in tests where assert_eq! on f64 is intentional.
#![cfg_attr(test, allow(clippy::float_cmp))]

pub mod data;
pub mod engine;
pub mod server;
pub mod signals;
pub mod stats;
pub mod strategies;
pub mod tools;
