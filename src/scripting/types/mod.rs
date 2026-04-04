//! Types for the Rhai scripting engine.
//!
//! Defines `BarContext` (exposed to scripts as `ctx`), `SymbolContext` (returned by
//! `ctx.sym("SYMBOL")`), `ScriptPosition` (exposed as `pos`), `ScriptConfig`
//! (parsed from `config()` return), action enums for processing script commands,
//! and the US market trading calendar.

mod bar_context;
mod config;
mod position;
mod symbol_context;

pub use bar_context::*;
pub use config::*;
pub use position::*;
pub use symbol_context::*;
