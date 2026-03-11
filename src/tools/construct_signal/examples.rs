//! Concrete JSON examples for each signal type, used in `build_signal` search results.
//!
//! Each example provides sensible default parameters so that an LLM client
//! can use them directly or adapt them with minimal changes.

use serde_json::{json, Value};

use super::{DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW, DEFAULT_OPEN, DEFAULT_VOLUME};

/// Build a concrete JSON example for a signal given its name.
/// Note: New signals added to `SIGNAL_CATALOG` must also be added to this function
/// to generate concrete examples. This is a necessary manual step to provide Claude
/// with sensible default parameter values for each signal type.
#[allow(clippy::too_many_lines)]
pub fn build_example(signal_name: &str) -> Value {
    match signal_name {
        // Momentum
        "RsiBelow" => json!({
            "type": "RsiBelow",
            "column": DEFAULT_CLOSE,
            "threshold": 30.0,
        }),
        "RsiAbove" => json!({
            "type": "RsiAbove",
            "column": DEFAULT_CLOSE,
            "threshold": 70.0,
        }),
        "MacdBullish" => json!({
            "type": "MacdBullish",
            "column": DEFAULT_CLOSE,
        }),
        "MacdBearish" => json!({
            "type": "MacdBearish",
            "column": DEFAULT_CLOSE,
        }),
        "MacdCrossover" => json!({
            "type": "MacdCrossover",
            "column": DEFAULT_CLOSE,
        }),
        "StochasticBelow" => json!({
            "type": "StochasticBelow",
            "close_col": DEFAULT_CLOSE,
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "period": 14,
            "threshold": 20.0,
        }),
        "StochasticAbove" => json!({
            "type": "StochasticAbove",
            "close_col": DEFAULT_CLOSE,
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "period": 14,
            "threshold": 80.0,
        }),
        // Overlap
        "PriceAboveSma" => json!({
            "type": "PriceAboveSma",
            "column": DEFAULT_CLOSE,
            "period": 20,
        }),
        "PriceBelowSma" => json!({
            "type": "PriceBelowSma",
            "column": DEFAULT_CLOSE,
            "period": 20,
        }),
        "PriceAboveEma" => json!({
            "type": "PriceAboveEma",
            "column": DEFAULT_CLOSE,
            "period": 20,
        }),
        "PriceBelowEma" => json!({
            "type": "PriceBelowEma",
            "column": DEFAULT_CLOSE,
            "period": 20,
        }),
        "SmaCrossover" => json!({
            "type": "SmaCrossover",
            "column": DEFAULT_CLOSE,
            "fast_period": 50,
            "slow_period": 200,
        }),
        "SmaCrossunder" => json!({
            "type": "SmaCrossunder",
            "column": DEFAULT_CLOSE,
            "fast_period": 50,
            "slow_period": 200,
        }),
        "EmaCrossover" => json!({
            "type": "EmaCrossover",
            "column": DEFAULT_CLOSE,
            "fast_period": 12,
            "slow_period": 26,
        }),
        "EmaCrossunder" => json!({
            "type": "EmaCrossunder",
            "column": DEFAULT_CLOSE,
            "fast_period": 12,
            "slow_period": 26,
        }),
        // Trend
        "AroonUptrend" => json!({
            "type": "AroonUptrend",
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "period": 25,
        }),
        "AroonDowntrend" => json!({
            "type": "AroonDowntrend",
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "period": 25,
        }),
        "AroonUpAbove" => json!({
            "type": "AroonUpAbove",
            "high_col": DEFAULT_HIGH,
            "period": 25,
            "threshold": 70.0,
        }),
        "SupertrendBullish" => json!({
            "type": "SupertrendBullish",
            "close_col": DEFAULT_CLOSE,
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "period": 10,
            "multiplier": 3.0,
        }),
        "SupertrendBearish" => json!({
            "type": "SupertrendBearish",
            "close_col": DEFAULT_CLOSE,
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "period": 10,
            "multiplier": 3.0,
        }),
        // Volatility
        "AtrAbove" => json!({
            "type": "AtrAbove",
            "close_col": DEFAULT_CLOSE,
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "period": 14,
            "threshold": 1.0,
        }),
        "AtrBelow" => json!({
            "type": "AtrBelow",
            "close_col": DEFAULT_CLOSE,
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "period": 14,
            "threshold": 0.5,
        }),
        "BollingerLowerTouch" => json!({
            "type": "BollingerLowerTouch",
            "column": DEFAULT_CLOSE,
            "period": 20,
        }),
        "BollingerUpperTouch" => json!({
            "type": "BollingerUpperTouch",
            "column": DEFAULT_CLOSE,
            "period": 20,
        }),
        "KeltnerLowerBreak" => json!({
            "type": "KeltnerLowerBreak",
            "close_col": DEFAULT_CLOSE,
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "period": 20,
            "multiplier": 2.0,
        }),
        "KeltnerUpperBreak" => json!({
            "type": "KeltnerUpperBreak",
            "close_col": DEFAULT_CLOSE,
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "period": 20,
            "multiplier": 2.0,
        }),
        // IV (implied volatility)
        "IvRankAbove" => json!({
            "type": "IvRankAbove",
            "lookback": 252,
            "threshold": 50.0,
        }),
        "IvRankBelow" => json!({
            "type": "IvRankBelow",
            "lookback": 252,
            "threshold": 30.0,
        }),
        "IvPercentileAbove" => json!({
            "type": "IvPercentileAbove",
            "lookback": 252,
            "threshold": 50.0,
        }),
        "IvPercentileBelow" => json!({
            "type": "IvPercentileBelow",
            "lookback": 252,
            "threshold": 30.0,
        }),
        // Price
        "GapUp" => json!({
            "type": "GapUp",
            "open_col": DEFAULT_OPEN,
            "close_col": DEFAULT_CLOSE,
            "threshold": 0.02,
        }),
        "GapDown" => json!({
            "type": "GapDown",
            "open_col": DEFAULT_OPEN,
            "close_col": DEFAULT_CLOSE,
            "threshold": 0.02,
        }),
        "DrawdownBelow" => json!({
            "type": "DrawdownBelow",
            "column": DEFAULT_CLOSE,
            "window": 20,
            "threshold": 0.1,
        }),
        "ConsecutiveUp" => json!({
            "type": "ConsecutiveUp",
            "column": DEFAULT_CLOSE,
            "count": 3,
        }),
        "ConsecutiveDown" => json!({
            "type": "ConsecutiveDown",
            "column": DEFAULT_CLOSE,
            "count": 3,
        }),
        "RateOfChange" => json!({
            "type": "RateOfChange",
            "column": DEFAULT_CLOSE,
            "period": 14,
            "threshold": 0.02,
        }),
        // Volume
        "MfiBelow" => json!({
            "type": "MfiBelow",
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "close_col": DEFAULT_CLOSE,
            "volume_col": DEFAULT_VOLUME,
            "period": 14,
            "threshold": 20.0,
        }),
        "MfiAbove" => json!({
            "type": "MfiAbove",
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "close_col": DEFAULT_CLOSE,
            "volume_col": DEFAULT_VOLUME,
            "period": 14,
            "threshold": 80.0,
        }),
        "ObvRising" => json!({
            "type": "ObvRising",
            "price_col": DEFAULT_CLOSE,
            "volume_col": DEFAULT_VOLUME,
        }),
        "ObvFalling" => json!({
            "type": "ObvFalling",
            "price_col": DEFAULT_CLOSE,
            "volume_col": DEFAULT_VOLUME,
        }),
        "CmfPositive" => json!({
            "type": "CmfPositive",
            "close_col": DEFAULT_CLOSE,
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "volume_col": DEFAULT_VOLUME,
            "period": 20,
        }),
        "CmfNegative" => json!({
            "type": "CmfNegative",
            "close_col": DEFAULT_CLOSE,
            "high_col": DEFAULT_HIGH,
            "low_col": DEFAULT_LOW,
            "volume_col": DEFAULT_VOLUME,
            "period": 20,
        }),
        // Range (And combinator patterns)
        "RsiRange" => json!({
            "type": "And",
            "left": { "type": "RsiAbove", "column": DEFAULT_CLOSE, "threshold": 30.0 },
            "right": { "type": "RsiBelow", "column": DEFAULT_CLOSE, "threshold": 40.0 },
        }),
        "StochasticRange" => json!({
            "type": "And",
            "left": { "type": "StochasticAbove", "close_col": DEFAULT_CLOSE, "high_col": DEFAULT_HIGH, "low_col": DEFAULT_LOW, "period": 14, "threshold": 20.0 },
            "right": { "type": "StochasticBelow", "close_col": DEFAULT_CLOSE, "high_col": DEFAULT_HIGH, "low_col": DEFAULT_LOW, "period": 14, "threshold": 80.0 },
        }),
        "AtrRange" => json!({
            "type": "And",
            "left": { "type": "AtrAbove", "close_col": DEFAULT_CLOSE, "high_col": DEFAULT_HIGH, "low_col": DEFAULT_LOW, "period": 14, "threshold": 0.5 },
            "right": { "type": "AtrBelow", "close_col": DEFAULT_CLOSE, "high_col": DEFAULT_HIGH, "low_col": DEFAULT_LOW, "period": 14, "threshold": 1.5 },
        }),
        "MfiRange" => json!({
            "type": "And",
            "left": { "type": "MfiAbove", "high_col": DEFAULT_HIGH, "low_col": DEFAULT_LOW, "close_col": DEFAULT_CLOSE, "volume_col": DEFAULT_VOLUME, "period": 14, "threshold": 20.0 },
            "right": { "type": "MfiBelow", "high_col": DEFAULT_HIGH, "low_col": DEFAULT_LOW, "close_col": DEFAULT_CLOSE, "volume_col": DEFAULT_VOLUME, "period": 14, "threshold": 80.0 },
        }),
        // Fallback: return a structured placeholder with explicit error message
        _ => json!({
            "type": "UnknownSignal",
            "error": format!("No example defined for signal '{}'", signal_name),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signals::registry::SignalSpec;

    #[test]
    fn build_example_rsi() {
        let example = build_example("RsiBelow");
        assert_eq!(example["type"], "RsiBelow");
        assert_eq!(example["threshold"], 30.0);
    }

    #[test]
    fn build_example_rsi_range() {
        let example = build_example("RsiRange");
        assert_eq!(example["type"], "And");
        assert_eq!(example["left"]["type"], "RsiAbove");
        assert_eq!(example["left"]["threshold"], 30.0);
        assert_eq!(example["right"]["type"], "RsiBelow");
        assert_eq!(example["right"]["threshold"], 40.0);
    }

    #[test]
    fn rsi_range_roundtrip_deserialize() {
        let example = build_example("RsiRange");
        let spec: SignalSpec = serde_json::from_value(example).unwrap();
        if let SignalSpec::And { left, right } = spec {
            assert!(matches!(*left, SignalSpec::RsiAbove { .. }));
            assert!(matches!(*right, SignalSpec::RsiBelow { .. }));
        } else {
            panic!("expected And combinator");
        }
    }
}
