//! Rhai scripting engine for user-defined backtesting strategies.
//!
//! Provides a unified event-driven simulation loop that executes Rhai scripts
//! with callback functions (`config`, `on_bar`, `on_exit_check`, etc.).
//! The engine handles data loading, position management, and metrics calculation
//! while scripts define trading logic.

pub mod engine;
pub mod indicators;
pub mod registration;
pub mod stdlib;
pub mod types;
