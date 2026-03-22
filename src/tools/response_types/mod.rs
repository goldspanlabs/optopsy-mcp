//! Response types returned by MCP tool handlers.
//!
//! Most structs here derive `Serialize`, `Deserialize`, and `JsonSchema` so they can be
//! serialized to JSON for the MCP wire format and introspected by schema-aware clients.
//! This module also contains a small number of input/parameter types (e.g.,
//! `DistributionSource`, `CorrelationSeries`) that are shared across the tool and server
//! layers and derive `Deserialize` and `JsonSchema` (but not `Serialize`).

pub mod backtest;
pub mod data;
pub mod hypothesis;
pub mod inputs;
pub mod optimization;
pub mod portfolio;
pub mod risk;
pub mod signals;
pub mod stats;
pub mod wheel;

pub use backtest::*;
pub use data::*;
pub use hypothesis::*;
pub use inputs::*;
pub use optimization::*;
pub use portfolio::*;
pub use risk::*;
pub use signals::*;
pub use stats::*;
pub use wheel::*;
