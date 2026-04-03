//! Types for the Rhai scripting engine.
//!
//! Defines `BarContext` (exposed to scripts as `ctx`), `ScriptPosition` (exposed as `pos`),
//! `ScriptConfig` (parsed from `config()` return), action enums for processing
//! script commands, and the US market trading calendar.

mod bar_context;
mod config;
mod position;
pub mod trading_calendar;

pub use bar_context::*;
pub use config::*;
pub use position::*;
