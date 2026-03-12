//! Signal specification enum defining all supported signal types.
//!
//! All indicator logic is expressed through the Custom formula DSL.
//! The enum retains structural variants for composition, persistence,
//! and cross-symbol evaluation.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Serializable signal specification.
///
/// Indicator signals are expressed as `Custom` formulas (e.g.
/// `"rsi(close, 14) < 30 and close > sma(close, 50)"`).
/// Use `build_signal` to convert a `SignalSpec` into a concrete `Box<dyn SignalFn>`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum SignalSpec {
    /// User-defined formula signal. The `formula` field contains an expression
    /// using price columns (close, open, high, low, volume, iv) with operators and
    /// comparisons. Examples:
    /// - `"close > sma(close, 20)"` — price above 20-day SMA
    /// - `"rsi(close, 14) < 30 and close > bbands_lower(close, 20)"` — oversold + below lower band
    /// - `"aroon_osc(high, low, 25) > 0 and close > supertrend(close, high, low, 10, 3.0)"` — trend
    /// - `"consecutive_up(close) >= 3"` — 3 bars of rising prices
    Custom {
        /// Human-readable name for this signal
        name: String,
        /// Formula expression that evaluates to a boolean series
        formula: String,
        /// Optional description of what this signal detects
        description: Option<String>,
    },

    /// Reference to a previously saved custom signal by name.
    Saved {
        /// Name of a previously saved custom signal
        name: String,
    },

    /// Evaluate the inner signal against a different symbol's OHLCV data.
    /// Example: use VIX close > 20 as an entry filter for SPY strategies.
    CrossSymbol {
        /// Ticker of the secondary symbol (e.g., "^VIX")
        symbol: String,
        /// The signal to evaluate on that symbol's data
        signal: Box<SignalSpec>,
    },

    /// Logical AND: fire only when both left and right signals are active.
    And {
        left: Box<SignalSpec>,
        right: Box<SignalSpec>,
    },
    /// Logical OR: fire when either left or right signal is active.
    Or {
        left: Box<SignalSpec>,
        right: Box<SignalSpec>,
    },
}

impl SignalSpec {
    /// Check if this spec (or any nested child) contains a `CrossSymbol` variant.
    pub fn contains_cross_symbol(&self) -> bool {
        match self {
            SignalSpec::CrossSymbol { .. } => true,
            SignalSpec::And { left, right } | SignalSpec::Or { left, right } => {
                left.contains_cross_symbol() || right.contains_cross_symbol()
            }
            _ => false,
        }
    }
}
