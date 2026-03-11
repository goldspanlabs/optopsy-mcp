use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Serializable signal specification. Each variant maps 1:1 to a `SignalFn` struct.
/// Use `build_signal` to convert a `SignalSpec` into a concrete `Box<dyn SignalFn>`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum SignalSpec {
    // -- Momentum --
    RsiBelow {
        column: String,
        threshold: f64,
    },
    RsiAbove {
        column: String,
        threshold: f64,
    },
    MacdBullish {
        column: String,
    },
    MacdBearish {
        column: String,
    },
    MacdCrossover {
        column: String,
    },
    StochasticBelow {
        close_col: String,
        high_col: String,
        low_col: String,
        period: usize,
        threshold: f64,
    },
    StochasticAbove {
        close_col: String,
        high_col: String,
        low_col: String,
        period: usize,
        threshold: f64,
    },

    // -- Overlap --
    PriceAboveSma {
        column: String,
        period: usize,
    },
    PriceBelowSma {
        column: String,
        period: usize,
    },
    PriceAboveEma {
        column: String,
        period: usize,
    },
    PriceBelowEma {
        column: String,
        period: usize,
    },
    SmaCrossover {
        column: String,
        fast_period: usize,
        slow_period: usize,
    },
    SmaCrossunder {
        column: String,
        fast_period: usize,
        slow_period: usize,
    },
    EmaCrossover {
        column: String,
        fast_period: usize,
        slow_period: usize,
    },
    EmaCrossunder {
        column: String,
        fast_period: usize,
        slow_period: usize,
    },

    // -- Trend --
    AroonUptrend {
        high_col: String,
        low_col: String,
        period: usize,
    },
    AroonDowntrend {
        high_col: String,
        low_col: String,
        period: usize,
    },
    AroonUpAbove {
        high_col: String,
        period: usize,
        threshold: f64,
    },
    SupertrendBullish {
        close_col: String,
        high_col: String,
        low_col: String,
        period: usize,
        multiplier: f64,
    },
    SupertrendBearish {
        close_col: String,
        high_col: String,
        low_col: String,
        period: usize,
        multiplier: f64,
    },

    // -- Volatility --
    AtrAbove {
        close_col: String,
        high_col: String,
        low_col: String,
        period: usize,
        threshold: f64,
    },
    AtrBelow {
        close_col: String,
        high_col: String,
        low_col: String,
        period: usize,
        threshold: f64,
    },
    BollingerLowerTouch {
        column: String,
        period: usize,
    },
    BollingerUpperTouch {
        column: String,
        period: usize,
    },
    KeltnerLowerBreak {
        close_col: String,
        high_col: String,
        low_col: String,
        period: usize,
        multiplier: f64,
    },
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
    IvRankBelow {
        lookback: usize,
        threshold: f64,
    },
    /// IV Percentile above threshold. IV Percentile = % of lookback days with IV below current × 100.
    IvPercentileAbove {
        lookback: usize,
        threshold: f64,
    },
    /// IV Percentile below threshold.
    IvPercentileBelow {
        lookback: usize,
        threshold: f64,
    },

    // -- Price --
    GapUp {
        open_col: String,
        close_col: String,
        threshold: f64,
    },
    GapDown {
        open_col: String,
        close_col: String,
        threshold: f64,
    },
    DrawdownBelow {
        column: String,
        window: usize,
        threshold: f64,
    },
    ConsecutiveUp {
        column: String,
        count: usize,
    },
    ConsecutiveDown {
        column: String,
        count: usize,
    },
    RateOfChange {
        column: String,
        period: usize,
        threshold: f64,
    },

    // -- Volume --
    MfiBelow {
        high_col: String,
        low_col: String,
        close_col: String,
        volume_col: String,
        period: usize,
        threshold: f64,
    },
    MfiAbove {
        high_col: String,
        low_col: String,
        close_col: String,
        volume_col: String,
        period: usize,
        threshold: f64,
    },
    ObvRising {
        price_col: String,
        volume_col: String,
    },
    ObvFalling {
        price_col: String,
        volume_col: String,
    },
    CmfPositive {
        close_col: String,
        high_col: String,
        low_col: String,
        volume_col: String,
        period: usize,
    },
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

    // -- Combinators --
    And {
        left: Box<SignalSpec>,
        right: Box<SignalSpec>,
    },
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
