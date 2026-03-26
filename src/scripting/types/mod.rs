//! Types for the Rhai scripting engine.
//!
//! Defines `BarContext` (exposed to scripts as `ctx`), `ScriptPosition` (exposed as `pos`),
//! `ScriptConfig` (parsed from `config()` return), and action enums for processing
//! script commands.

mod bar_context;
mod config;
mod position;

pub use bar_context::*;
pub use config::*;
pub use position::*;
