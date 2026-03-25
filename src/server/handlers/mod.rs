//! Handler submodules for MCP tool bodies.
//!
//! Each module contains standalone functions that implement the business logic
//! for one or more MCP tool handlers. The `#[tool(...)]` methods in
//! `server/mod.rs` remain thin wrappers that delegate here.

pub mod optimization;
pub mod run_script;
pub mod signals;
