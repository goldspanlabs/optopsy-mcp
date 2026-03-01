use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::combinators::{AndSignal, OrSignal};
use super::helpers::SignalFn;
use super::momentum::{
    MacdBearish, MacdBullish, MacdCrossover, RsiOverbought, RsiOversold, StochasticOverbought,
    StochasticOversold,
};
use super::overlap::{
    EmaCrossover, EmaCrossunder, PriceAboveEma, PriceAboveSma, PriceBelowEma, PriceBelowSma,
    SmaCrossover, SmaCrossunder,
};
use super::price::{ConsecutiveDown, ConsecutiveUp, DrawdownBelow, GapDown, GapUp, RateOfChange};
use super::trend::{
    AroonDowntrend, AroonUpAbove, AroonUptrend, SupertrendBearish, SupertrendBullish,
};
use super::volatility::{
    AtrAbove, AtrBelow, BollingerLowerTouch, BollingerUpperTouch, KeltnerLowerBreak,
    KeltnerUpperBreak,
};
use super::volume::{CmfNegative, CmfPositive, MfiOverbought, MfiOversold, ObvFalling, ObvRising};

/// Serializable signal specification. Each variant maps 1:1 to a `SignalFn` struct.
/// Use `build_signal` to convert a `SignalSpec` into a concrete `Box<dyn SignalFn>`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum SignalSpec {
    // -- Momentum --
    RsiOversold {
        column: String,
        threshold: f64,
    },
    RsiOverbought {
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
    StochasticOversold {
        close_col: String,
        high_col: String,
        low_col: String,
        period: usize,
        threshold: f64,
    },
    StochasticOverbought {
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
    MfiOversold {
        high_col: String,
        low_col: String,
        close_col: String,
        volume_col: String,
        period: usize,
        threshold: f64,
    },
    MfiOverbought {
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

/// Convert a `SignalSpec` into a concrete `Box<dyn SignalFn>`.
#[allow(clippy::too_many_lines)]
pub fn build_signal(spec: &SignalSpec) -> Box<dyn SignalFn> {
    match spec {
        // Momentum
        SignalSpec::RsiOversold { column, threshold } => Box::new(RsiOversold {
            column: column.clone(),
            threshold: *threshold,
        }),
        SignalSpec::RsiOverbought { column, threshold } => Box::new(RsiOverbought {
            column: column.clone(),
            threshold: *threshold,
        }),
        SignalSpec::MacdBullish { column } => Box::new(MacdBullish {
            column: column.clone(),
        }),
        SignalSpec::MacdBearish { column } => Box::new(MacdBearish {
            column: column.clone(),
        }),
        SignalSpec::MacdCrossover { column } => Box::new(MacdCrossover {
            column: column.clone(),
        }),
        SignalSpec::StochasticOversold {
            close_col,
            high_col,
            low_col,
            period,
            threshold,
        } => Box::new(StochasticOversold {
            close_col: close_col.clone(),
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            period: *period,
            threshold: *threshold,
        }),
        SignalSpec::StochasticOverbought {
            close_col,
            high_col,
            low_col,
            period,
            threshold,
        } => Box::new(StochasticOverbought {
            close_col: close_col.clone(),
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            period: *period,
            threshold: *threshold,
        }),

        // Overlap
        SignalSpec::PriceAboveSma { column, period } => Box::new(PriceAboveSma {
            column: column.clone(),
            period: *period,
        }),
        SignalSpec::PriceBelowSma { column, period } => Box::new(PriceBelowSma {
            column: column.clone(),
            period: *period,
        }),
        SignalSpec::PriceAboveEma { column, period } => Box::new(PriceAboveEma {
            column: column.clone(),
            period: *period,
        }),
        SignalSpec::PriceBelowEma { column, period } => Box::new(PriceBelowEma {
            column: column.clone(),
            period: *period,
        }),
        SignalSpec::SmaCrossover {
            column,
            fast_period,
            slow_period,
        } => Box::new(SmaCrossover {
            column: column.clone(),
            fast_period: *fast_period,
            slow_period: *slow_period,
        }),
        SignalSpec::SmaCrossunder {
            column,
            fast_period,
            slow_period,
        } => Box::new(SmaCrossunder {
            column: column.clone(),
            fast_period: *fast_period,
            slow_period: *slow_period,
        }),
        SignalSpec::EmaCrossover {
            column,
            fast_period,
            slow_period,
        } => Box::new(EmaCrossover {
            column: column.clone(),
            fast_period: *fast_period,
            slow_period: *slow_period,
        }),
        SignalSpec::EmaCrossunder {
            column,
            fast_period,
            slow_period,
        } => Box::new(EmaCrossunder {
            column: column.clone(),
            fast_period: *fast_period,
            slow_period: *slow_period,
        }),

        // Trend
        SignalSpec::AroonUptrend {
            high_col,
            low_col,
            period,
        } => Box::new(AroonUptrend {
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            period: *period,
        }),
        SignalSpec::AroonDowntrend {
            high_col,
            low_col,
            period,
        } => Box::new(AroonDowntrend {
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            period: *period,
        }),
        SignalSpec::AroonUpAbove {
            high_col,
            period,
            threshold,
        } => Box::new(AroonUpAbove {
            high_col: high_col.clone(),
            period: *period,
            threshold: *threshold,
        }),
        SignalSpec::SupertrendBullish {
            close_col,
            high_col,
            low_col,
            period,
            multiplier,
        } => Box::new(SupertrendBullish {
            close_col: close_col.clone(),
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            period: *period,
            multiplier: *multiplier,
        }),
        SignalSpec::SupertrendBearish {
            close_col,
            high_col,
            low_col,
            period,
            multiplier,
        } => Box::new(SupertrendBearish {
            close_col: close_col.clone(),
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            period: *period,
            multiplier: *multiplier,
        }),

        // Volatility
        SignalSpec::AtrAbove {
            close_col,
            high_col,
            low_col,
            period,
            threshold,
        } => Box::new(AtrAbove {
            close_col: close_col.clone(),
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            period: *period,
            threshold: *threshold,
        }),
        SignalSpec::AtrBelow {
            close_col,
            high_col,
            low_col,
            period,
            threshold,
        } => Box::new(AtrBelow {
            close_col: close_col.clone(),
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            period: *period,
            threshold: *threshold,
        }),
        SignalSpec::BollingerLowerTouch { column, period } => Box::new(BollingerLowerTouch {
            column: column.clone(),
            period: *period,
        }),
        SignalSpec::BollingerUpperTouch { column, period } => Box::new(BollingerUpperTouch {
            column: column.clone(),
            period: *period,
        }),
        SignalSpec::KeltnerLowerBreak {
            close_col,
            high_col,
            low_col,
            period,
            multiplier,
        } => Box::new(KeltnerLowerBreak {
            close_col: close_col.clone(),
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            period: *period,
            multiplier: *multiplier,
        }),
        SignalSpec::KeltnerUpperBreak {
            close_col,
            high_col,
            low_col,
            period,
            multiplier,
        } => Box::new(KeltnerUpperBreak {
            close_col: close_col.clone(),
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            period: *period,
            multiplier: *multiplier,
        }),

        // Price
        SignalSpec::GapUp {
            open_col,
            close_col,
            threshold,
        } => Box::new(GapUp {
            open_col: open_col.clone(),
            close_col: close_col.clone(),
            threshold: *threshold,
        }),
        SignalSpec::GapDown {
            open_col,
            close_col,
            threshold,
        } => Box::new(GapDown {
            open_col: open_col.clone(),
            close_col: close_col.clone(),
            threshold: *threshold,
        }),
        SignalSpec::DrawdownBelow {
            column,
            window,
            threshold,
        } => Box::new(DrawdownBelow {
            column: column.clone(),
            window: *window,
            threshold: *threshold,
        }),
        SignalSpec::ConsecutiveUp { column, count } => Box::new(ConsecutiveUp {
            column: column.clone(),
            count: *count,
        }),
        SignalSpec::ConsecutiveDown { column, count } => Box::new(ConsecutiveDown {
            column: column.clone(),
            count: *count,
        }),
        SignalSpec::RateOfChange {
            column,
            period,
            threshold,
        } => Box::new(RateOfChange {
            column: column.clone(),
            period: *period,
            threshold: *threshold,
        }),

        // Volume
        SignalSpec::MfiOversold {
            high_col,
            low_col,
            close_col,
            volume_col,
            period,
            threshold,
        } => Box::new(MfiOversold {
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            close_col: close_col.clone(),
            volume_col: volume_col.clone(),
            period: *period,
            threshold: *threshold,
        }),
        SignalSpec::MfiOverbought {
            high_col,
            low_col,
            close_col,
            volume_col,
            period,
            threshold,
        } => Box::new(MfiOverbought {
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            close_col: close_col.clone(),
            volume_col: volume_col.clone(),
            period: *period,
            threshold: *threshold,
        }),
        SignalSpec::ObvRising {
            price_col,
            volume_col,
        } => Box::new(ObvRising {
            price_col: price_col.clone(),
            volume_col: volume_col.clone(),
        }),
        SignalSpec::ObvFalling {
            price_col,
            volume_col,
        } => Box::new(ObvFalling {
            price_col: price_col.clone(),
            volume_col: volume_col.clone(),
        }),
        SignalSpec::CmfPositive {
            close_col,
            high_col,
            low_col,
            volume_col,
            period,
        } => Box::new(CmfPositive {
            close_col: close_col.clone(),
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            volume_col: volume_col.clone(),
            period: *period,
        }),
        SignalSpec::CmfNegative {
            close_col,
            high_col,
            low_col,
            volume_col,
            period,
        } => Box::new(CmfNegative {
            close_col: close_col.clone(),
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            volume_col: volume_col.clone(),
            period: *period,
        }),

        // Combinators
        SignalSpec::And { left, right } => Box::new(AndSignal {
            left: build_signal(left),
            right: build_signal(right),
        }),
        SignalSpec::Or { left, right } => Box::new(OrSignal {
            left: build_signal(left),
            right: build_signal(right),
        }),
    }
}

/// Metadata about a signal for the `list_signals` catalog.
pub struct SignalInfo {
    pub name: &'static str,
    pub category: &'static str,
    pub description: &'static str,
    pub params: &'static str,
}

pub const SIGNAL_CATALOG: &[SignalInfo] = &[
    // Momentum
    SignalInfo {
        name: "RsiOversold",
        category: "momentum",
        description: "RSI below threshold (oversold). Uses 14-period RSI.",
        params: "column, threshold (e.g. 30.0)",
    },
    SignalInfo {
        name: "RsiOverbought",
        category: "momentum",
        description: "RSI above threshold (overbought). Uses 14-period RSI.",
        params: "column, threshold (e.g. 70.0)",
    },
    SignalInfo {
        name: "MacdBullish",
        category: "momentum",
        description: "MACD histogram > 0 (bullish momentum). Requires 34+ data points.",
        params: "column",
    },
    SignalInfo {
        name: "MacdBearish",
        category: "momentum",
        description: "MACD histogram < 0 (bearish momentum).",
        params: "column",
    },
    SignalInfo {
        name: "MacdCrossover",
        category: "momentum",
        description: "MACD histogram crosses from negative to positive.",
        params: "column",
    },
    SignalInfo {
        name: "StochasticOversold",
        category: "momentum",
        description: "Stochastic oscillator below threshold.",
        params: "close_col, high_col, low_col, period, threshold",
    },
    SignalInfo {
        name: "StochasticOverbought",
        category: "momentum",
        description: "Stochastic oscillator above threshold.",
        params: "close_col, high_col, low_col, period, threshold",
    },
    // Overlap
    SignalInfo {
        name: "PriceAboveSma",
        category: "overlap",
        description: "Price is above its Simple Moving Average.",
        params: "column, period",
    },
    SignalInfo {
        name: "PriceBelowSma",
        category: "overlap",
        description: "Price is below its Simple Moving Average.",
        params: "column, period",
    },
    SignalInfo {
        name: "PriceAboveEma",
        category: "overlap",
        description: "Price is above its Exponential Moving Average.",
        params: "column, period",
    },
    SignalInfo {
        name: "PriceBelowEma",
        category: "overlap",
        description: "Price is below its Exponential Moving Average.",
        params: "column, period",
    },
    SignalInfo {
        name: "SmaCrossover",
        category: "overlap",
        description: "Fast SMA crosses above slow SMA (golden cross).",
        params: "column, fast_period, slow_period",
    },
    SignalInfo {
        name: "SmaCrossunder",
        category: "overlap",
        description: "Fast SMA crosses below slow SMA (death cross).",
        params: "column, fast_period, slow_period",
    },
    SignalInfo {
        name: "EmaCrossover",
        category: "overlap",
        description: "Fast EMA crosses above slow EMA.",
        params: "column, fast_period, slow_period",
    },
    SignalInfo {
        name: "EmaCrossunder",
        category: "overlap",
        description: "Fast EMA crosses below slow EMA.",
        params: "column, fast_period, slow_period",
    },
    // Trend
    SignalInfo {
        name: "AroonUptrend",
        category: "trend",
        description: "Aroon oscillator > 0 (uptrend).",
        params: "high_col, low_col, period",
    },
    SignalInfo {
        name: "AroonDowntrend",
        category: "trend",
        description: "Aroon oscillator < 0 (downtrend).",
        params: "high_col, low_col, period",
    },
    SignalInfo {
        name: "AroonUpAbove",
        category: "trend",
        description: "Aroon Up line above threshold (strong uptrend).",
        params: "high_col, period, threshold",
    },
    SignalInfo {
        name: "SupertrendBullish",
        category: "trend",
        description: "Price above supertrend line (bullish).",
        params: "close_col, high_col, low_col, period, multiplier",
    },
    SignalInfo {
        name: "SupertrendBearish",
        category: "trend",
        description: "Price below supertrend line (bearish).",
        params: "close_col, high_col, low_col, period, multiplier",
    },
    // Volatility
    SignalInfo {
        name: "AtrAbove",
        category: "volatility",
        description: "ATR above threshold (high volatility).",
        params: "close_col, high_col, low_col, period, threshold",
    },
    SignalInfo {
        name: "AtrBelow",
        category: "volatility",
        description: "ATR below threshold (low volatility).",
        params: "close_col, high_col, low_col, period, threshold",
    },
    SignalInfo {
        name: "BollingerLowerTouch",
        category: "volatility",
        description: "Price touches or crosses below lower Bollinger Band (2x std dev).",
        params: "column, period",
    },
    SignalInfo {
        name: "BollingerUpperTouch",
        category: "volatility",
        description: "Price touches or crosses above upper Bollinger Band (2x std dev).",
        params: "column, period",
    },
    SignalInfo {
        name: "KeltnerLowerBreak",
        category: "volatility",
        description: "Price breaks below lower Keltner Channel.",
        params: "close_col, high_col, low_col, period, multiplier",
    },
    SignalInfo {
        name: "KeltnerUpperBreak",
        category: "volatility",
        description: "Price breaks above upper Keltner Channel.",
        params: "close_col, high_col, low_col, period, multiplier",
    },
    // Price
    SignalInfo {
        name: "GapUp",
        category: "price",
        description: "Gap up: open significantly higher than previous close.",
        params: "open_col, close_col, threshold",
    },
    SignalInfo {
        name: "GapDown",
        category: "price",
        description: "Gap down: open significantly lower than previous close.",
        params: "open_col, close_col, threshold",
    },
    SignalInfo {
        name: "DrawdownBelow",
        category: "price",
        description: "Drawdown from rolling max exceeds threshold.",
        params: "column, window, threshold",
    },
    SignalInfo {
        name: "ConsecutiveUp",
        category: "price",
        description: "N consecutive higher closes.",
        params: "column, count",
    },
    SignalInfo {
        name: "ConsecutiveDown",
        category: "price",
        description: "N consecutive lower closes.",
        params: "column, count",
    },
    SignalInfo {
        name: "RateOfChange",
        category: "price",
        description: "Price change from N periods ago exceeds threshold.",
        params: "column, period, threshold",
    },
    // Volume
    SignalInfo {
        name: "MfiOversold",
        category: "volume",
        description: "Money Flow Index below threshold (oversold by volume-weighted momentum).",
        params: "high_col, low_col, close_col, volume_col, period, threshold",
    },
    SignalInfo {
        name: "MfiOverbought",
        category: "volume",
        description: "Money Flow Index above threshold (overbought).",
        params: "high_col, low_col, close_col, volume_col, period, threshold",
    },
    SignalInfo {
        name: "ObvRising",
        category: "volume",
        description: "On-Balance Volume is rising (current > previous).",
        params: "price_col, volume_col",
    },
    SignalInfo {
        name: "ObvFalling",
        category: "volume",
        description: "On-Balance Volume is falling (current < previous).",
        params: "price_col, volume_col",
    },
    SignalInfo {
        name: "CmfPositive",
        category: "volume",
        description: "Chaikin Money Flow > 0 (buying pressure).",
        params: "close_col, high_col, low_col, volume_col, period",
    },
    SignalInfo {
        name: "CmfNegative",
        category: "volume",
        description: "Chaikin Money Flow < 0 (selling pressure).",
        params: "close_col, high_col, low_col, volume_col, period",
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_signal_round_trip_rsi() {
        let spec = SignalSpec::RsiOversold {
            column: "close".into(),
            threshold: 30.0,
        };
        let signal = build_signal(&spec);
        assert_eq!(signal.name(), "rsi_oversold");
    }

    #[test]
    fn build_signal_and_combinator() {
        let spec = SignalSpec::And {
            left: Box::new(SignalSpec::RsiOversold {
                column: "close".into(),
                threshold: 30.0,
            }),
            right: Box::new(SignalSpec::MacdBullish {
                column: "close".into(),
            }),
        };
        let signal = build_signal(&spec);
        assert_eq!(signal.name(), "and");
    }

    #[test]
    fn signal_spec_serde_round_trip() {
        let spec = SignalSpec::RsiOversold {
            column: "close".into(),
            threshold: 30.0,
        };
        let json = serde_json::to_string(&spec).unwrap();
        let parsed: SignalSpec = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, SignalSpec::RsiOversold { .. }));
    }

    #[test]
    fn catalog_has_all_signals() {
        // 38 signals (excluding And/Or combinators)
        assert_eq!(SIGNAL_CATALOG.len(), 38);
    }
}
