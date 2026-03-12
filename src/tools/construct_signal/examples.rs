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
            "type": "Custom",
            "name": "RSI Below 30",
            "formula": format!("rsi({}, 14) < 30", DEFAULT_CLOSE),
            "description": null,
        }),
        "RsiAbove" => json!({
            "type": "Custom",
            "name": "RSI Above 70",
            "formula": format!("rsi({}, 14) > 70", DEFAULT_CLOSE),
            "description": null,
        }),
        "MacdBullish" => json!({
            "type": "Custom",
            "name": "MACD Bullish",
            "formula": format!("macd_hist({}) > 0", DEFAULT_CLOSE),
            "description": null,
        }),
        "MacdBearish" => json!({
            "type": "Custom",
            "name": "MACD Bearish",
            "formula": format!("macd_hist({}) < 0", DEFAULT_CLOSE),
            "description": null,
        }),
        "MacdCrossover" => json!({
            "type": "Custom",
            "name": "MACD Crossover",
            "formula": format!("macd_hist({}) > 0", DEFAULT_CLOSE),
            "description": null,
        }),
        "StochasticBelow" => json!({
            "type": "Custom",
            "name": "Stochastic Oversold",
            "formula": format!("stochastic({}, {}, {}, 14) < 20", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
            "description": null,
        }),
        "StochasticAbove" => json!({
            "type": "Custom",
            "name": "Stochastic Overbought",
            "formula": format!("stochastic({}, {}, {}, 14) > 80", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
            "description": null,
        }),
        // Overlap (formula-based)
        "PriceAboveSma" => json!({
            "type": "Custom",
            "name": "Price Above SMA 20",
            "formula": format!("{} > sma({}, 20)", DEFAULT_CLOSE, DEFAULT_CLOSE),
            "description": null,
        }),
        "PriceBelowSma" => json!({
            "type": "Custom",
            "name": "Price Below SMA 20",
            "formula": format!("{} < sma({}, 20)", DEFAULT_CLOSE, DEFAULT_CLOSE),
            "description": null,
        }),
        "PriceAboveEma" => json!({
            "type": "Custom",
            "name": "Price Above EMA 20",
            "formula": format!("{} > ema({}, 20)", DEFAULT_CLOSE, DEFAULT_CLOSE),
            "description": null,
        }),
        "PriceBelowEma" => json!({
            "type": "Custom",
            "name": "Price Below EMA 20",
            "formula": format!("{} < ema({}, 20)", DEFAULT_CLOSE, DEFAULT_CLOSE),
            "description": null,
        }),
        "SmaCrossover" => json!({
            "type": "Custom",
            "name": "SMA Golden Cross",
            "formula": format!("sma({}, 50) > sma({}, 200)", DEFAULT_CLOSE, DEFAULT_CLOSE),
            "description": null,
        }),
        "SmaCrossunder" => json!({
            "type": "Custom",
            "name": "SMA Death Cross",
            "formula": format!("sma({}, 50) < sma({}, 200)", DEFAULT_CLOSE, DEFAULT_CLOSE),
            "description": null,
        }),
        "EmaCrossover" => json!({
            "type": "Custom",
            "name": "EMA Crossover",
            "formula": format!("ema({}, 12) > ema({}, 26)", DEFAULT_CLOSE, DEFAULT_CLOSE),
            "description": null,
        }),
        "EmaCrossunder" => json!({
            "type": "Custom",
            "name": "EMA Crossunder",
            "formula": format!("ema({}, 12) < ema({}, 26)", DEFAULT_CLOSE, DEFAULT_CLOSE),
            "description": null,
        }),
        // Trend (formula-based)
        "AroonUptrend" => json!({
            "type": "Custom",
            "name": "Aroon Uptrend",
            "formula": format!("aroon_osc({}, {}, 25) > 0", DEFAULT_HIGH, DEFAULT_LOW),
            "description": null,
        }),
        "AroonDowntrend" => json!({
            "type": "Custom",
            "name": "Aroon Downtrend",
            "formula": format!("aroon_osc({}, {}, 25) < 0", DEFAULT_HIGH, DEFAULT_LOW),
            "description": null,
        }),
        "AroonUpAbove" => json!({
            "type": "Custom",
            "name": "Aroon Up Above 70",
            "formula": format!("aroon_up({}, {}, 25) > 70", DEFAULT_HIGH, DEFAULT_LOW),
            "description": null,
        }),
        "SupertrendBullish" => json!({
            "type": "Custom",
            "name": "Supertrend Bullish",
            "formula": format!("{} > supertrend({}, {}, {}, 10, 3.0)", DEFAULT_CLOSE, DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
            "description": null,
        }),
        "SupertrendBearish" => json!({
            "type": "Custom",
            "name": "Supertrend Bearish",
            "formula": format!("{} < supertrend({}, {}, {}, 10, 3.0)", DEFAULT_CLOSE, DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
            "description": null,
        }),
        // Volatility (formula-based)
        "AtrAbove" => json!({
            "type": "Custom",
            "name": "ATR Above 1.0",
            "formula": format!("atr({}, {}, {}, 14) > 1.0", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
            "description": null,
        }),
        "AtrBelow" => json!({
            "type": "Custom",
            "name": "ATR Below 0.5",
            "formula": format!("atr({}, {}, {}, 14) < 0.5", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
            "description": null,
        }),
        "BollingerLowerTouch" => json!({
            "type": "Custom",
            "name": "Bollinger Lower Touch",
            "formula": format!("{} < bbands_lower({}, 20)", DEFAULT_CLOSE, DEFAULT_CLOSE),
            "description": null,
        }),
        "BollingerUpperTouch" => json!({
            "type": "Custom",
            "name": "Bollinger Upper Touch",
            "formula": format!("{} > bbands_upper({}, 20)", DEFAULT_CLOSE, DEFAULT_CLOSE),
            "description": null,
        }),
        "KeltnerLowerBreak" => json!({
            "type": "Custom",
            "name": "Keltner Lower Break",
            "formula": format!("{} < keltner_lower({}, {}, {}, 20, 2.0)", DEFAULT_CLOSE, DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
            "description": null,
        }),
        "KeltnerUpperBreak" => json!({
            "type": "Custom",
            "name": "Keltner Upper Break",
            "formula": format!("{} > keltner_upper({}, {}, {}, 20, 2.0)", DEFAULT_CLOSE, DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
            "description": null,
        }),
        // IV (implied volatility) — no formula DSL equivalent; use Custom placeholders
        "IvRankAbove" => json!({
            "type": "Custom",
            "name": "IV Rank Above 50",
            "formula": "iv_rank(252) > 50",
            "description": null,
        }),
        "IvRankBelow" => json!({
            "type": "Custom",
            "name": "IV Rank Below 30",
            "formula": "iv_rank(252) < 30",
            "description": null,
        }),
        "IvPercentileAbove" => json!({
            "type": "Custom",
            "name": "IV Percentile Above 50",
            "formula": "iv_percentile(252) > 50",
            "description": null,
        }),
        "IvPercentileBelow" => json!({
            "type": "Custom",
            "name": "IV Percentile Below 30",
            "formula": "iv_percentile(252) < 30",
            "description": null,
        }),
        // Price (formula-based)
        "GapUp" => json!({
            "type": "Custom",
            "name": "Gap Up 2%",
            "formula": format!("{} / close.shift(1) - 1 > 0.02", DEFAULT_OPEN),
            "description": null,
        }),
        "GapDown" => json!({
            "type": "Custom",
            "name": "Gap Down 2%",
            "formula": format!("{} / close.shift(1) - 1 < -0.02", DEFAULT_OPEN),
            "description": null,
        }),
        "DrawdownBelow" => json!({
            "type": "Custom",
            "name": "Drawdown Below 10%",
            "formula": format!("{} / rolling_max({}, 20) - 1 < -0.1", DEFAULT_CLOSE, DEFAULT_CLOSE),
            "description": null,
        }),
        "ConsecutiveUp" => json!({
            "type": "Custom",
            "name": "3 Consecutive Up",
            "formula": format!("consecutive_up({}) >= 3", DEFAULT_CLOSE),
            "description": null,
        }),
        "ConsecutiveDown" => json!({
            "type": "Custom",
            "name": "3 Consecutive Down",
            "formula": format!("consecutive_down({}) >= 3", DEFAULT_CLOSE),
            "description": null,
        }),
        "RateOfChange" => json!({
            "type": "Custom",
            "name": "Rate of Change 2%",
            "formula": format!("roc({}, 14) > 0.02", DEFAULT_CLOSE),
            "description": null,
        }),
        // Volume (formula-based)
        "MfiBelow" => json!({
            "type": "Custom",
            "name": "MFI Oversold",
            "formula": format!("mfi({}, {}, {}, {}, 14) < 20", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW, DEFAULT_VOLUME),
            "description": null,
        }),
        "MfiAbove" => json!({
            "type": "Custom",
            "name": "MFI Overbought",
            "formula": format!("mfi({}, {}, {}, {}, 14) > 80", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW, DEFAULT_VOLUME),
            "description": null,
        }),
        "ObvRising" => json!({
            "type": "Custom",
            "name": "OBV Rising",
            "formula": format!("obv({}, {}) > 0", DEFAULT_CLOSE, DEFAULT_VOLUME),
            "description": null,
        }),
        "ObvFalling" => json!({
            "type": "Custom",
            "name": "OBV Falling",
            "formula": format!("obv({}, {}) < 0", DEFAULT_CLOSE, DEFAULT_VOLUME),
            "description": null,
        }),
        "CmfPositive" => json!({
            "type": "Custom",
            "name": "CMF Positive",
            "formula": format!("cmf({}, {}, {}, {}, 20) > 0", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW, DEFAULT_VOLUME),
            "description": null,
        }),
        "CmfNegative" => json!({
            "type": "Custom",
            "name": "CMF Negative",
            "formula": format!("cmf({}, {}, {}, {}, 20) < 0", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW, DEFAULT_VOLUME),
            "description": null,
        }),
        // Range (And combinator patterns using Custom signals)
        "RsiRange" => json!({
            "type": "And",
            "left": {
                "type": "Custom",
                "name": "RSI Above 30",
                "formula": format!("rsi({}, 14) > 30", DEFAULT_CLOSE),
                "description": null,
            },
            "right": {
                "type": "Custom",
                "name": "RSI Below 40",
                "formula": format!("rsi({}, 14) < 40", DEFAULT_CLOSE),
                "description": null,
            },
        }),
        "StochasticRange" => json!({
            "type": "And",
            "left": {
                "type": "Custom",
                "name": "Stochastic Above 20",
                "formula": format!("stochastic({}, {}, {}, 14) > 20", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
                "description": null,
            },
            "right": {
                "type": "Custom",
                "name": "Stochastic Below 80",
                "formula": format!("stochastic({}, {}, {}, 14) < 80", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
                "description": null,
            },
        }),
        "AtrRange" => json!({
            "type": "And",
            "left": {
                "type": "Custom",
                "name": "ATR Above 0.5",
                "formula": format!("atr({}, {}, {}, 14) > 0.5", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
                "description": null,
            },
            "right": {
                "type": "Custom",
                "name": "ATR Below 1.5",
                "formula": format!("atr({}, {}, {}, 14) < 1.5", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW),
                "description": null,
            },
        }),
        "MfiRange" => json!({
            "type": "And",
            "left": {
                "type": "Custom",
                "name": "MFI Above 20",
                "formula": format!("mfi({}, {}, {}, {}, 14) > 20", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW, DEFAULT_VOLUME),
                "description": null,
            },
            "right": {
                "type": "Custom",
                "name": "MFI Below 80",
                "formula": format!("mfi({}, {}, {}, {}, 14) < 80", DEFAULT_CLOSE, DEFAULT_HIGH, DEFAULT_LOW, DEFAULT_VOLUME),
                "description": null,
            },
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
        assert_eq!(example["type"], "Custom");
        assert!(example["formula"].as_str().unwrap().contains("rsi"));
        assert!(example["formula"].as_str().unwrap().contains("30"));
    }

    #[test]
    fn build_example_rsi_range() {
        let example = build_example("RsiRange");
        assert_eq!(example["type"], "And");
        assert_eq!(example["left"]["type"], "Custom");
        assert!(example["left"]["formula"].as_str().unwrap().contains("rsi"));
        assert_eq!(example["right"]["type"], "Custom");
        assert!(example["right"]["formula"]
            .as_str()
            .unwrap()
            .contains("rsi"));
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
