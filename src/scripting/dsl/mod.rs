//! Trading DSL: compile human-readable order syntax into Rhai action maps.
//!
//! Supports:
//! - `buy 100 shares` → market order
//! - `buy 100 shares at 150.00 limit` → limit order
//! - `buy 100 shares at 155.00 stop` → stop order
//! - `sell 50 shares at 145.00 stop 143.00 limit` → stop-limit order
//! - `cancel all orders` → cancel all pending orders

mod parser;

pub use parser::compile_dsl;
