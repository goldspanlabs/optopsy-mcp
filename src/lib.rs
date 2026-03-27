//! Options backtesting engine exposed as an MCP (Model Context Protocol) server.
//!
//! Provides 13 tools for loading options chain data, evaluating strategies statistically,
//! running event-driven backtests, comparing strategies, and returning raw price data.

// float_cmp: only in tests where assert_eq! on f64 is intentional.
#![cfg_attr(test, allow(clippy::float_cmp))]

pub mod constants;
pub mod data;
pub mod engine;
// Rhai integration patterns require clippy exceptions:
// - pass-by-value String args (Rhai calling convention)
// - verbose map access patterns for Dynamic → typed extraction
// - too_many_lines/arguments on the simulation loop and bar-context builder
// Only lints actually triggered in this module are suppressed here.
#[allow(
    clippy::too_many_lines,
    clippy::too_many_arguments,
    clippy::doc_markdown,
    clippy::if_not_else,
    clippy::default_trait_access,
    clippy::similar_names,
    clippy::map_unwrap_or,
    clippy::needless_pass_by_value,
    clippy::cast_lossless,
    clippy::match_same_arms,
    clippy::implicit_hasher,
    clippy::wildcard_imports,
    clippy::redundant_closure_for_method_calls,
    clippy::unnecessary_wraps,
    clippy::ref_option,
    clippy::type_complexity,
    clippy::if_same_then_else,
    clippy::needless_range_loop,
    clippy::module_inception
)]
pub mod scripting;
pub mod server;
pub mod stats;
pub mod tools;
