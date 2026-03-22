//! Handler submodules for MCP tool bodies.
//!
//! Each module contains standalone functions that implement the business logic
//! for one or more MCP tool handlers. The `#[tool(...)]` methods in
//! `server/mod.rs` remain thin wrappers that delegate here.

pub mod backtest;
pub mod compare;
pub mod optimization;
pub mod portfolio;
pub mod signals;
pub mod stock_backtest;
pub mod wheel_backtest;
