//! List all built-in options strategies with metadata and default delta targets.
//!
//! The built-in strategy registry (`src/strategies/`) has been removed.
//! This module now returns an empty list. Strategy definitions are handled
//! by the Rhai scripting engine.

use super::ai_format;
use super::response_types::StrategiesResponse;

/// Return an empty strategies response (built-in registry removed).
pub fn execute() -> StrategiesResponse {
    ai_format::format_strategies(vec![])
}
