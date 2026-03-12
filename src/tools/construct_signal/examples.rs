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
        // Momentum (formula-based)
        "RsiBelow" => json!({
            "type": "Formula",
            "formula": format!("rsi({}, 14) < 30", DEFAULT_CLOSE),
        }),
        "RsiAbove" => json!({
            "type": "Formula",
            "formula": format!("rsi({}, 14) > 70", DEFAULT_CLOSE),
        }),
        "MacdBullish" => json!({
            "type": "Formula",
            "formula": format!("macd_hist({}) > 0", DEFAULT_CLOSE),
        }),
        "MacdBearish" => json!({
            "type": "Formula",
            "formula": format!("macd_hist({}) < 0", DEFAULT_CLOSE),
        }),
        "MacdCrossover" => json!({
            "type": "Formula",
            "formula": format!("macd_hist({}) > 0 and macd_hist({})[1] <= 0", DEFAULT_CLOSE, DEFAULT_CLOSE),
        }),
        "StochasticBelow" => json!({
            "type": "Formula",
            "formula": format!("stochastic({}, {}, {}, 14) < 20", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
        }),
        "StochasticAbove" => json!({
            "type": "Formula",
            "formula": format!("stochastic({}, {}, {}, 14) > 80", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
        }),
        // Overlap (formula-based)
        "PriceAboveSma" => json!({
            "type": "Formula",
            "formula": format!("{} > sma({}, 20)", DEFAULT_CLOSE, DEFAULT_CLOSE),
        }),
        "PriceBelowSma" => json!({
            "type": "Formula",
            "formula": format!("{} < sma({}, 20)", DEFAULT_CLOSE, DEFAULT_CLOSE),
        }),
        "PriceAboveEma" => json!({
            "type": "Formula",
            "formula": format!("{} > ema({}, 20)", DEFAULT_CLOSE, DEFAULT_CLOSE),
        }),
        "PriceBelowEma" => json!({
            "type": "Formula",
            "formula": format!("{} < ema({}, 20)", DEFAULT_CLOSE, DEFAULT_CLOSE),
        }),
        "SmaCrossover" => json!({
            "type": "Formula",
            "formula": format!("sma({}, 50) > sma({}, 200) and sma({}, 50)[1] <= sma({}, 200)[1]", DEFAULT_CLOSE, DEFAULT_CLOSE, DEFAULT_CLOSE, DEFAULT_CLOSE),
        }),
        "SmaCrossunder" => json!({
            "type": "Formula",
            "formula": format!("sma({}, 50) < sma({}, 200) and sma({}, 50)[1] >= sma({}, 200)[1]", DEFAULT_CLOSE, DEFAULT_CLOSE, DEFAULT_CLOSE, DEFAULT_CLOSE),
        }),
        "EmaCrossover" => json!({
            "type": "Formula",
            "formula": format!("ema({}, 12) > ema({}, 26) and ema({}, 12)[1] <= ema({}, 26)[1]", DEFAULT_CLOSE, DEFAULT_CLOSE, DEFAULT_CLOSE, DEFAULT_CLOSE),
        }),
        "EmaCrossunder" => json!({
            "type": "Formula",
            "formula": format!("ema({}, 12) < ema({}, 26) and ema({}, 12)[1] >= ema({}, 26)[1]", DEFAULT_CLOSE, DEFAULT_CLOSE, DEFAULT_CLOSE, DEFAULT_CLOSE),
        }),
        // Trend (formula-based)
        "AroonUptrend" => json!({
            "type": "Formula",
            "formula": format!("aroon_osc({}, {}, 25) > 0", DEFAULT_HIGH, DEFAULT_LOW),
        }),
        "AroonDowntrend" => json!({
            "type": "Formula",
            "formula": format!("aroon_osc({}, {}, 25) < 0", DEFAULT_HIGH, DEFAULT_LOW),
        }),
        "AroonUpAbove" => json!({
            "type": "Formula",
            "formula": format!("aroon_up({}, {}, 25) > 70", DEFAULT_HIGH, DEFAULT_LOW),
        }),
        "SupertrendBullish" => json!({
            "type": "Formula",
            "formula": format!("{} > supertrend({}, {}, {}, 10, 3.0)", DEFAULT_CLOSE, DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
        }),
        "SupertrendBearish" => json!({
            "type": "Formula",
            "formula": format!("{} < supertrend({}, {}, {}, 10, 3.0)", DEFAULT_CLOSE, DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
        }),
        // Volatility (formula-based)
        "AtrAbove" => json!({
            "type": "Formula",
            "formula": format!("atr({}, {}, {}, 14) > 1.0", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
        }),
        "AtrBelow" => json!({
            "type": "Formula",
            "formula": format!("atr({}, {}, {}, 14) < 0.5", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
        }),
        "BollingerLowerTouch" => json!({
            "type": "Formula",
            "formula": format!("{} < bbands_lower({}, 20)", DEFAULT_CLOSE, DEFAULT_CLOSE),
        }),
        "BollingerUpperTouch" => json!({
            "type": "Formula",
            "formula": format!("{} > bbands_upper({}, 20)", DEFAULT_CLOSE, DEFAULT_CLOSE),
        }),
        "KeltnerLowerBreak" => json!({
            "type": "Formula",
            "formula": format!("{} < keltner_lower({}, {}, {}, 20, 2.0)", DEFAULT_CLOSE, DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
        }),
        "KeltnerUpperBreak" => json!({
            "type": "Formula",
            "formula": format!("{} > keltner_upper({}, {}, {}, 20, 2.0)", DEFAULT_CLOSE, DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
        }),
        // IV (implied volatility)
        "IvRankAbove" => json!({
            "type": "Formula",
            "formula": "iv_rank(iv, 252) > 50",
        }),
        "IvRankBelow" => json!({
            "type": "Formula",
            "formula": "iv_rank(iv, 252) < 30",
        }),
        "IvPercentileAbove" => json!({
            "type": "Formula",
            "formula": "rank(iv, 252) > 50",
        }),
        "IvPercentileBelow" => json!({
            "type": "Formula",
            "formula": "rank(iv, 252) < 30",
        }),
        // Price (formula-based)
        "GapUp" => json!({
            "type": "Formula",
            "formula": format!("{} / {}[1] - 1 > 0.02", DEFAULT_OPEN, DEFAULT_CLOSE),
        }),
        "GapDown" => json!({
            "type": "Formula",
            "formula": format!("{} / {}[1] - 1 < -0.02", DEFAULT_OPEN, DEFAULT_CLOSE),
        }),
        "DrawdownBelow" => json!({
            "type": "Formula",
            "formula": format!("{} / max({}, 20) - 1 < -0.1", DEFAULT_CLOSE, DEFAULT_CLOSE),
        }),
        "ConsecutiveUp" => json!({
            "type": "Formula",
            "formula": format!("consecutive_up({}) >= 3", DEFAULT_CLOSE),
        }),
        "ConsecutiveDown" => json!({
            "type": "Formula",
            "formula": format!("consecutive_down({}) >= 3", DEFAULT_CLOSE),
        }),
        "RateOfChange" => json!({
            "type": "Formula",
            "formula": format!("roc({}, 14) > 2.0", DEFAULT_CLOSE),
        }),
        // Volume (formula-based)
        "MfiBelow" => json!({
            "type": "Formula",
            "formula": format!("mfi({}, {}, {}, {}, 14) < 20", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW, DEFAULT_VOLUME),
        }),
        "MfiAbove" => json!({
            "type": "Formula",
            "formula": format!("mfi({}, {}, {}, {}, 14) > 80", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW, DEFAULT_VOLUME),
        }),
        "ObvRising" => json!({
            "type": "Formula",
            "formula": format!("obv({}, {}) > obv({}, {})[1]", DEFAULT_CLOSE, DEFAULT_VOLUME, DEFAULT_CLOSE, DEFAULT_VOLUME),
        }),
        "ObvFalling" => json!({
            "type": "Formula",
            "formula": format!("obv({}, {}) < obv({}, {})[1]", DEFAULT_CLOSE, DEFAULT_VOLUME, DEFAULT_CLOSE, DEFAULT_VOLUME),
        }),
        "CmfPositive" => json!({
            "type": "Formula",
            "formula": format!("cmf({}, {}, {}, {}, 20) > 0", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW, DEFAULT_VOLUME),
        }),
        "CmfNegative" => json!({
            "type": "Formula",
            "formula": format!("cmf({}, {}, {}, {}, 20) < 0", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW, DEFAULT_VOLUME),
        }),
        // Range (And combinator patterns)
        "RsiRange" => json!({
            "type": "And",
            "left": format!("rsi({}, 14) > 30", DEFAULT_CLOSE),
            "right": format!("rsi({}, 14) < 40", DEFAULT_CLOSE),
        }),
        "StochasticRange" => json!({
            "type": "And",
            "left": format!("stochastic({}, {}, {}, 14) > 20", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
            "right": format!("stochastic({}, {}, {}, 14) < 80", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
        }),
        "AtrRange" => json!({
            "type": "And",
            "left": format!("atr({}, {}, {}, 14) > 0.5", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
            "right": format!("atr({}, {}, {}, 14) < 1.5", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
        }),
        "MfiRange" => json!({
            "type": "And",
            "left": format!("mfi({}, {}, {}, {}, 14) > 20", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW, DEFAULT_VOLUME),
            "right": format!("mfi({}, {}, {}, {}, 14) < 80", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW, DEFAULT_VOLUME),
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
        assert_eq!(example["type"], "Formula");
        assert!(example["formula"].as_str().unwrap().contains("rsi"));
        assert!(example["formula"].as_str().unwrap().contains("30"));
    }

    #[test]
    fn build_example_rsi_range() {
        let example = build_example("RsiRange");
        assert_eq!(example["type"], "And");
        // Children are string shorthand
        assert!(example["left"].as_str().unwrap().contains("rsi"));
        assert!(example["right"].as_str().unwrap().contains("rsi"));
    }

    #[test]
    fn rsi_range_roundtrip_deserialize() {
        let example = build_example("RsiRange");
        let spec: SignalSpec = serde_json::from_value(example).unwrap();
        if let SignalSpec::And { left, right } = spec {
            assert!(matches!(*left, SignalSpec::Formula { .. }));
            assert!(matches!(*right, SignalSpec::Formula { .. }));
        } else {
            panic!("expected And combinator");
        }
    }
}
