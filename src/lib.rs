//! Options backtesting engine exposed as an MCP (Model Context Protocol) server.
//!
//! Provides 13 tools for loading options chain data, evaluating strategies statistically,
//! running event-driven backtests, comparing strategies, and returning raw price data.

// float_cmp: only in tests where assert_eq! on f64 is intentional.
#![cfg_attr(test, allow(clippy::float_cmp))]

pub mod data;
pub mod engine;
// Rhai integration patterns require many clippy exceptions:
// - pass-by-value String args (Rhai calling convention)
// - unused Dynamic returns from fire-and-forget callbacks
// - verbose map access patterns for Dynamic → typed extraction
// - too_many_lines on the main simulation loop
#[allow(
    clippy::too_many_lines,
    clippy::too_many_arguments,
    clippy::doc_markdown,
    clippy::if_not_else,
    clippy::default_trait_access,
    clippy::similar_names,
    clippy::map_unwrap_or,
    clippy::option_if_let_else,
    clippy::unnested_or_patterns,
    clippy::wildcard_enum_match_arm,
    clippy::needless_pass_by_value,
    clippy::trivially_copy_pass_by_ref,
    clippy::cast_lossless,
    clippy::unused_self,
    clippy::let_underscore_untyped,
    clippy::match_same_arms,
    clippy::redundant_else,
    clippy::option_option,
    clippy::implicit_hasher,
    clippy::wildcard_imports,
    clippy::redundant_closure_for_method_calls,
    clippy::if_same_then_else,
    clippy::unnecessary_wraps,
    clippy::type_complexity,
    clippy::ref_option,
    clippy::module_inception,
    clippy::let_and_return,
    clippy::manual_midpoint
)]
pub mod scripting;
pub mod server;
pub mod signals;
pub mod stats;
pub mod strategies;
pub mod tools;
