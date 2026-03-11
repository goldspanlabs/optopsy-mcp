//! Signal specification enum defining all supported signal types.
//!
//! Each variant maps 1:1 to a `SignalFn` implementation and is serializable
//! for JSON Schema generation, storage, and MCP transport.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Serializable signal specification. Each variant maps 1:1 to a `SignalFn` struct.
/// Use `build_signal` to convert a `SignalSpec` into a concrete `Box<dyn SignalFn>`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum SignalSpec {
    /// Fire when RSI is below the threshold (oversold).
    RsiBelow { column: String, threshold: f64 },
    /// Fire when RSI is above the threshold (overbought).
    RsiAbove { column: String, threshold: f64 },
    /// Fire when MACD histogram is positive (bullish momentum).
    MacdBullish { column: String },
    /// Fire when MACD histogram is negative (bearish momentum).
    MacdBearish { column: String },
    /// Fire when MACD line crosses above the signal line.
    MacdCrossover { column: String },
    /// Fire when Stochastic %K is below the threshold (oversold).
    StochasticBelow {
        close_col: String,
        high_col: String,
        low_col: String,
        period: usize,
        threshold: f64,
    },
    /// Fire when Stochastic %K is above the threshold (overbought).
    StochasticAbove {
        close_col: String,
        high_col: String,
        low_col: String,
        period: usize,
        threshold: f64,
    },

    /// Fire when price is above the simple moving average.
    PriceAboveSma { column: String, period: usize },
    /// Fire when price is below the simple moving average.
    PriceBelowSma { column: String, period: usize },
    /// Fire when price is above the exponential moving average.
    PriceAboveEma { column: String, period: usize },
    /// Fire when price is below the exponential moving average.
    PriceBelowEma { column: String, period: usize },
    /// Fire when the fast SMA crosses above the slow SMA (bullish crossover).
    SmaCrossover {
        column: String,
        fast_period: usize,
        slow_period: usize,
    },
    /// Fire when the fast SMA crosses below the slow SMA (bearish crossunder).
    SmaCrossunder {
        column: String,
        fast_period: usize,
        slow_period: usize,
    },
    /// Fire when the fast EMA crosses above the slow EMA (bullish crossover).
    EmaCrossover {
        column: String,
        fast_period: usize,
        slow_period: usize,
    },
    /// Fire when the fast EMA crosses below the slow EMA (bearish crossunder).
    EmaCrossunder {
        column: String,
        fast_period: usize,
        slow_period: usize,
    },

    /// Fire when Aroon Up exceeds Aroon Down (uptrend).
    AroonUptrend {
        high_col: String,
        low_col: String,
        period: usize,
    },
    /// Fire when Aroon Down exceeds Aroon Up (downtrend).
    AroonDowntrend {
        high_col: String,
        low_col: String,
        period: usize,
    },
    /// Fire when Aroon Up exceeds the threshold.
    AroonUpAbove {
        high_col: String,
        period: usize,
        threshold: f64,
    },
    /// Fire when price is above the Supertrend indicator (bullish).
    SupertrendBullish {
        close_col: String,
        high_col: String,
        low_col: String,
        period: usize,
        multiplier: f64,
    },
    /// Fire when price is below the Supertrend indicator (bearish).
    SupertrendBearish {
        close_col: String,
        high_col: String,
        low_col: String,
        period: usize,
        multiplier: f64,
    },

    /// Fire when Average True Range exceeds the threshold (high volatility).
    AtrAbove {
        close_col: String,
        high_col: String,
        low_col: String,
        period: usize,
        threshold: f64,
    },
    /// Fire when Average True Range is below the threshold (low volatility).
    AtrBelow {
        close_col: String,
        high_col: String,
        low_col: String,
        period: usize,
        threshold: f64,
    },
    /// Fire when price touches or crosses below the lower Bollinger Band.
    BollingerLowerTouch { column: String, period: usize },
    /// Fire when price touches or crosses above the upper Bollinger Band.
    BollingerUpperTouch { column: String, period: usize },
    /// Fire when price breaks below the lower Keltner Channel.
    KeltnerLowerBreak {
        close_col: String,
        high_col: String,
        low_col: String,
        period: usize,
        multiplier: f64,
    },
    /// Fire when price breaks above the upper Keltner Channel.
    KeltnerUpperBreak {
        close_col: String,
        high_col: String,
        low_col: String,
        period: usize,
        multiplier: f64,
    },

    // -- IV (implied volatility from options chain) --
    /// IV Rank above threshold. `IV Rank = (current - min) / (max - min) × 100`.
    /// Derived from options chain `implied_volatility` column (not OHLCV data).
    IvRankAbove {
        /// Rolling lookback window in trading days (recommended: 252 ≈ 1 year)
        lookback: usize,
        /// Threshold 0–100 (e.g. 50.0 means IV Rank > 50%)
        threshold: f64,
    },
    /// IV Rank below threshold.
    IvRankBelow { lookback: usize, threshold: f64 },
    /// IV Percentile above threshold. IV Percentile = % of lookback days with IV below current × 100.
    IvPercentileAbove { lookback: usize, threshold: f64 },
    /// IV Percentile below threshold.
    IvPercentileBelow { lookback: usize, threshold: f64 },

    /// Fire when today's open gaps up from the previous close by at least the threshold.
    GapUp {
        open_col: String,
        close_col: String,
        threshold: f64,
    },
    /// Fire when today's open gaps down from the previous close by at least the threshold.
    GapDown {
        open_col: String,
        close_col: String,
        threshold: f64,
    },
    /// Fire when drawdown from the rolling high exceeds the threshold.
    DrawdownBelow {
        column: String,
        window: usize,
        threshold: f64,
    },
    /// Fire after N consecutive bars of rising prices.
    ConsecutiveUp { column: String, count: usize },
    /// Fire after N consecutive bars of falling prices.
    ConsecutiveDown { column: String, count: usize },
    /// Fire when the rate of change over the period exceeds the threshold.
    RateOfChange {
        column: String,
        period: usize,
        threshold: f64,
    },

    /// Fire when Money Flow Index is below the threshold (oversold).
    MfiBelow {
        high_col: String,
        low_col: String,
        close_col: String,
        volume_col: String,
        period: usize,
        threshold: f64,
    },
    /// Fire when Money Flow Index is above the threshold (overbought).
    MfiAbove {
        high_col: String,
        low_col: String,
        close_col: String,
        volume_col: String,
        period: usize,
        threshold: f64,
    },
    /// Fire when On-Balance Volume is rising (accumulation).
    ObvRising {
        price_col: String,
        volume_col: String,
    },
    /// Fire when On-Balance Volume is falling (distribution).
    ObvFalling {
        price_col: String,
        volume_col: String,
    },
    /// Fire when Chaikin Money Flow is positive (buying pressure).
    CmfPositive {
        close_col: String,
        high_col: String,
        low_col: String,
        volume_col: String,
        period: usize,
    },
    /// Fire when Chaikin Money Flow is negative (selling pressure).
    CmfNegative {
        close_col: String,
        high_col: String,
        low_col: String,
        volume_col: String,
        period: usize,
    },

    // -- Custom (user-defined formula) --
    /// User-defined formula signal. The `formula` field contains an expression
    /// using price columns (close, open, high, low, volume) with operators and
    /// comparisons. Examples:
    /// - `"close > sma(close, 20)"` — price above 20-day SMA
    /// - `"close > close[1] * 1.02"` — 2% gap up from previous close
    /// - `"(close - low) / (high - low) < 0.2"` — near session lows
    /// - `"volume > sma(volume, 20) * 2.0"` — volume spike
    Custom {
        /// Human-readable name for this signal
        name: String,
        /// Formula expression that evaluates to a boolean series
        formula: String,
        /// Optional description of what this signal detects
        description: Option<String>,
    },

    // -- Saved (reference to a previously saved custom signal by name) --
    Saved {
        /// Name of a previously saved custom signal
        name: String,
    },

    // -- Cross-symbol --
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
