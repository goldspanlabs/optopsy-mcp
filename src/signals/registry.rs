pub use super::builders::build_signal;
pub use super::spec::SignalSpec;

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
        name: "RsiBelow",
        category: "momentum",
        description: "RSI below threshold (oversold). Uses 14-period RSI.",
        params: "column, threshold (e.g. 30.0)",
    },
    SignalInfo {
        name: "RsiAbove",
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
        name: "StochasticBelow",
        category: "momentum",
        description: "Stochastic oscillator below threshold (oversold). Uses rolling %K.",
        params: "close_col, high_col, low_col, period, threshold",
    },
    SignalInfo {
        name: "StochasticAbove",
        category: "momentum",
        description: "Stochastic oscillator above threshold (overbought). Uses rolling %K.",
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
    SignalInfo {
        name: "IvRankAbove",
        category: "volatility",
        description: "IV Rank above threshold. Derived from options chain implied volatility. High IV Rank means current IV is near 52-week highs — good for premium selling.",
        params: "lookback (recommended 252), threshold (0-100, e.g. 50.0)",
    },
    SignalInfo {
        name: "IvRankBelow",
        category: "volatility",
        description: "IV Rank below threshold. Low IV Rank means current IV is near 52-week lows — good for premium buying.",
        params: "lookback (recommended 252), threshold (0-100, e.g. 30.0)",
    },
    SignalInfo {
        name: "IvPercentileAbove",
        category: "volatility",
        description: "IV Percentile above threshold. Percentage of lookback days with IV below current level. High percentile = elevated IV environment.",
        params: "lookback (recommended 252), threshold (0-100, e.g. 50.0)",
    },
    SignalInfo {
        name: "IvPercentileBelow",
        category: "volatility",
        description: "IV Percentile below threshold. Low percentile = suppressed IV environment.",
        params: "lookback (recommended 252), threshold (0-100, e.g. 30.0)",
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
        name: "MfiBelow",
        category: "volume",
        description: "Money Flow Index below threshold (oversold).",
        params: "high_col, low_col, close_col, volume_col, period, threshold",
    },
    SignalInfo {
        name: "MfiAbove",
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
    // Range (synthetic catalog entries for And combinator patterns)
    SignalInfo {
        name: "RsiRange",
        category: "momentum",
        description:
            "RSI within a range (e.g. 30-40). Uses And combinator with RsiAbove + RsiBelow.",
        params: "And { RsiAbove { threshold: lower }, RsiBelow { threshold: upper } }",
    },
    SignalInfo {
        name: "StochasticRange",
        category: "momentum",
        description:
            "Stochastic within a range. Uses And combinator with StochasticAbove + StochasticBelow.",
        params:
            "And { StochasticAbove { threshold: lower }, StochasticBelow { threshold: upper } }",
    },
    SignalInfo {
        name: "AtrRange",
        category: "volatility",
        description: "ATR within a range. Uses And combinator with AtrAbove + AtrBelow.",
        params: "And { AtrAbove { threshold: lower }, AtrBelow { threshold: upper } }",
    },
    SignalInfo {
        name: "MfiRange",
        category: "volume",
        description: "MFI within a range. Uses And combinator with MfiAbove + MfiBelow.",
        params: "And { MfiAbove { threshold: lower }, MfiBelow { threshold: upper } }",
    },
    // Cross-symbol
    SignalInfo {
        name: "CrossSymbol",
        category: "cross-symbol",
        description: "Evaluate any signal against a different symbol's OHLCV data (e.g., VIX as filter for SPY).",
        params: "symbol, signal (any nested SignalSpec)",
    },
];

/// Collect all secondary symbols referenced by `CrossSymbol` variants in a signal tree.
pub fn collect_cross_symbols(spec: &SignalSpec) -> std::collections::HashSet<String> {
    let mut symbols = std::collections::HashSet::new();
    let mut visited_saved = std::collections::HashSet::new();
    collect_cross_symbols_inner(spec, &mut symbols, &mut visited_saved, 0);
    symbols
}

fn collect_cross_symbols_inner(
    spec: &SignalSpec,
    out: &mut std::collections::HashSet<String>,
    visited_saved: &mut std::collections::HashSet<String>,
    depth: u8,
) {
    const MAX_DEPTH: u8 = 8;
    if depth > MAX_DEPTH {
        return;
    }

    match spec {
        SignalSpec::CrossSymbol { symbol, signal } => {
            out.insert(symbol.to_uppercase());
            collect_cross_symbols_inner(signal, out, visited_saved, depth);
        }
        SignalSpec::And { left, right } | SignalSpec::Or { left, right } => {
            collect_cross_symbols_inner(left, out, visited_saved, depth);
            collect_cross_symbols_inner(right, out, visited_saved, depth);
        }
        SignalSpec::Saved { name } => {
            if !visited_saved.insert(name.clone()) {
                return;
            }
            if let Ok(loaded_spec) = super::storage::load_signal(name) {
                collect_cross_symbols_inner(&loaded_spec, out, visited_saved, depth + 1);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_signal_round_trip_rsi() {
        let spec = SignalSpec::RsiBelow {
            column: "close".into(),
            threshold: 30.0,
        };
        let signal = build_signal(&spec);
        assert_eq!(signal.name(), "rsi_below");
    }

    #[test]
    fn build_signal_and_combinator() {
        let spec = SignalSpec::And {
            left: Box::new(SignalSpec::RsiBelow {
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
        let spec = SignalSpec::RsiBelow {
            column: "close".into(),
            threshold: 30.0,
        };
        let json = serde_json::to_string(&spec).unwrap();
        let parsed: SignalSpec = serde_json::from_str(&json).unwrap();
        if let SignalSpec::RsiBelow { column, threshold } = parsed {
            assert_eq!(column, "close");
            assert_eq!(threshold, 30.0);
        } else {
            panic!("expected RsiBelow");
        }
    }

    #[test]
    fn catalog_has_all_signals() {
        // 47 signals (excluding And/Or combinators; includes 4 range entries + CrossSymbol + 4 IV signals)
        assert_eq!(SIGNAL_CATALOG.len(), 47);
    }

    #[test]
    fn collect_cross_symbols_empty_for_plain() {
        let spec = SignalSpec::ConsecutiveUp {
            column: "close".into(),
            count: 2,
        };
        assert!(collect_cross_symbols(&spec).is_empty());
    }

    #[test]
    fn collect_cross_symbols_handles_saved() {
        // Saved spec that doesn't exist on disk returns empty (best-effort)
        let spec = SignalSpec::Saved {
            name: "nonexistent_saved_signal".into(),
        };
        let symbols = collect_cross_symbols(&spec);
        assert!(symbols.is_empty());
    }

    #[test]
    fn collect_cross_symbols_finds_nested() {
        let spec = SignalSpec::And {
            left: Box::new(SignalSpec::CrossSymbol {
                symbol: "^VIX".into(),
                signal: Box::new(SignalSpec::ConsecutiveUp {
                    column: "close".into(),
                    count: 2,
                }),
            }),
            right: Box::new(SignalSpec::CrossSymbol {
                symbol: "GLD".into(),
                signal: Box::new(SignalSpec::ConsecutiveDown {
                    column: "close".into(),
                    count: 3,
                }),
            }),
        };
        let symbols = collect_cross_symbols(&spec);
        assert_eq!(symbols.len(), 2);
        assert!(symbols.contains("^VIX"));
        assert!(symbols.contains("GLD"));
    }

    #[test]
    fn cross_symbol_serde_round_trip() {
        let spec = SignalSpec::CrossSymbol {
            symbol: "^VIX".into(),
            signal: Box::new(SignalSpec::Custom {
                name: "vix_above_20".into(),
                formula: "close > 20".into(),
                description: None,
            }),
        };
        let json = serde_json::to_string(&spec).unwrap();
        let parsed: SignalSpec = serde_json::from_str(&json).unwrap();
        if let SignalSpec::CrossSymbol { symbol, signal } = parsed {
            assert_eq!(symbol, "^VIX");
            assert!(matches!(*signal, SignalSpec::Custom { .. }));
        } else {
            panic!("expected CrossSymbol");
        }
    }

    #[test]
    fn build_signal_round_trip_rsi_overbought() {
        let spec = SignalSpec::RsiAbove {
            column: "close".into(),
            threshold: 70.0,
        };
        let signal = build_signal(&spec);
        assert_eq!(signal.name(), "rsi_above");
    }

    #[test]
    fn build_signal_macd_bullish() {
        let signal = build_signal(&SignalSpec::MacdBullish {
            column: "close".into(),
        });
        assert_eq!(signal.name(), "macd_bullish");
    }

    #[test]
    fn build_signal_macd_bearish() {
        let signal = build_signal(&SignalSpec::MacdBearish {
            column: "close".into(),
        });
        assert_eq!(signal.name(), "macd_bearish");
    }

    #[test]
    fn build_signal_macd_crossover() {
        let signal = build_signal(&SignalSpec::MacdCrossover {
            column: "close".into(),
        });
        assert_eq!(signal.name(), "macd_crossover");
    }

    #[test]
    fn build_signal_stochastic_oversold() {
        let signal = build_signal(&SignalSpec::StochasticBelow {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 14,
            threshold: 20.0,
        });
        assert_eq!(signal.name(), "stochastic_below");
    }

    #[test]
    fn build_signal_stochastic_overbought() {
        let signal = build_signal(&SignalSpec::StochasticAbove {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 14,
            threshold: 80.0,
        });
        assert_eq!(signal.name(), "stochastic_above");
    }

    #[test]
    fn build_signal_price_above_sma() {
        let signal = build_signal(&SignalSpec::PriceAboveSma {
            column: "close".into(),
            period: 20,
        });
        assert_eq!(signal.name(), "price_above_sma");
    }

    #[test]
    fn build_signal_price_below_sma() {
        let signal = build_signal(&SignalSpec::PriceBelowSma {
            column: "close".into(),
            period: 20,
        });
        assert_eq!(signal.name(), "price_below_sma");
    }

    #[test]
    fn build_signal_price_above_ema() {
        let signal = build_signal(&SignalSpec::PriceAboveEma {
            column: "close".into(),
            period: 20,
        });
        assert_eq!(signal.name(), "price_above_ema");
    }

    #[test]
    fn build_signal_price_below_ema() {
        let signal = build_signal(&SignalSpec::PriceBelowEma {
            column: "close".into(),
            period: 20,
        });
        assert_eq!(signal.name(), "price_below_ema");
    }

    #[test]
    fn build_signal_sma_crossover() {
        let signal = build_signal(&SignalSpec::SmaCrossover {
            column: "close".into(),
            fast_period: 5,
            slow_period: 20,
        });
        assert_eq!(signal.name(), "sma_crossover");
    }

    #[test]
    fn build_signal_sma_crossunder() {
        let signal = build_signal(&SignalSpec::SmaCrossunder {
            column: "close".into(),
            fast_period: 5,
            slow_period: 20,
        });
        assert_eq!(signal.name(), "sma_crossunder");
    }

    #[test]
    fn build_signal_ema_crossover() {
        let signal = build_signal(&SignalSpec::EmaCrossover {
            column: "close".into(),
            fast_period: 5,
            slow_period: 20,
        });
        assert_eq!(signal.name(), "ema_crossover");
    }

    #[test]
    fn build_signal_ema_crossunder() {
        let signal = build_signal(&SignalSpec::EmaCrossunder {
            column: "close".into(),
            fast_period: 5,
            slow_period: 20,
        });
        assert_eq!(signal.name(), "ema_crossunder");
    }

    #[test]
    fn build_signal_aroon_uptrend() {
        let signal = build_signal(&SignalSpec::AroonUptrend {
            high_col: "high".into(),
            low_col: "low".into(),
            period: 14,
        });
        assert_eq!(signal.name(), "aroon_uptrend");
    }

    #[test]
    fn build_signal_aroon_downtrend() {
        let signal = build_signal(&SignalSpec::AroonDowntrend {
            high_col: "high".into(),
            low_col: "low".into(),
            period: 14,
        });
        assert_eq!(signal.name(), "aroon_downtrend");
    }

    #[test]
    fn build_signal_aroon_up_above() {
        let signal = build_signal(&SignalSpec::AroonUpAbove {
            high_col: "high".into(),
            period: 14,
            threshold: 70.0,
        });
        assert_eq!(signal.name(), "aroon_up_above");
    }

    #[test]
    fn build_signal_supertrend_bullish() {
        let signal = build_signal(&SignalSpec::SupertrendBullish {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 10,
            multiplier: 3.0,
        });
        assert_eq!(signal.name(), "supertrend_bullish");
    }

    #[test]
    fn build_signal_supertrend_bearish() {
        let signal = build_signal(&SignalSpec::SupertrendBearish {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 10,
            multiplier: 3.0,
        });
        assert_eq!(signal.name(), "supertrend_bearish");
    }

    #[test]
    fn build_signal_atr_above() {
        let signal = build_signal(&SignalSpec::AtrAbove {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 14,
            threshold: 2.0,
        });
        assert_eq!(signal.name(), "atr_above");
    }

    #[test]
    fn build_signal_atr_below() {
        let signal = build_signal(&SignalSpec::AtrBelow {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 14,
            threshold: 2.0,
        });
        assert_eq!(signal.name(), "atr_below");
    }

    #[test]
    fn build_signal_bollinger_lower() {
        let signal = build_signal(&SignalSpec::BollingerLowerTouch {
            column: "close".into(),
            period: 20,
        });
        assert_eq!(signal.name(), "bollinger_lower_touch");
    }

    #[test]
    fn build_signal_bollinger_upper() {
        let signal = build_signal(&SignalSpec::BollingerUpperTouch {
            column: "close".into(),
            period: 20,
        });
        assert_eq!(signal.name(), "bollinger_upper_touch");
    }

    #[test]
    fn build_signal_keltner_lower() {
        let signal = build_signal(&SignalSpec::KeltnerLowerBreak {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 20,
            multiplier: 2.0,
        });
        assert_eq!(signal.name(), "keltner_lower_break");
    }

    #[test]
    fn build_signal_keltner_upper() {
        let signal = build_signal(&SignalSpec::KeltnerUpperBreak {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 20,
            multiplier: 2.0,
        });
        assert_eq!(signal.name(), "keltner_upper_break");
    }

    #[test]
    fn build_signal_iv_rank_above() {
        let signal = build_signal(&SignalSpec::IvRankAbove {
            lookback: 252,
            threshold: 50.0,
        });
        assert_eq!(signal.name(), "iv_rank_above");
    }

    #[test]
    fn build_signal_iv_rank_below() {
        let signal = build_signal(&SignalSpec::IvRankBelow {
            lookback: 252,
            threshold: 30.0,
        });
        assert_eq!(signal.name(), "iv_rank_below");
    }

    #[test]
    fn build_signal_iv_percentile_above() {
        let signal = build_signal(&SignalSpec::IvPercentileAbove {
            lookback: 252,
            threshold: 50.0,
        });
        assert_eq!(signal.name(), "iv_percentile_above");
    }

    #[test]
    fn build_signal_iv_percentile_below() {
        let signal = build_signal(&SignalSpec::IvPercentileBelow {
            lookback: 252,
            threshold: 30.0,
        });
        assert_eq!(signal.name(), "iv_percentile_below");
    }

    #[test]
    fn signal_spec_serde_round_trip_iv_rank() {
        let spec = SignalSpec::IvRankAbove {
            lookback: 252,
            threshold: 50.0,
        };
        let json = serde_json::to_string(&spec).unwrap();
        let parsed: SignalSpec = serde_json::from_str(&json).unwrap();
        if let SignalSpec::IvRankAbove {
            lookback,
            threshold,
        } = parsed
        {
            assert_eq!(lookback, 252);
            assert_eq!(threshold, 50.0);
        } else {
            panic!("expected IvRankAbove");
        }
    }

    #[test]
    fn build_signal_gap_up() {
        let signal = build_signal(&SignalSpec::GapUp {
            open_col: "open".into(),
            close_col: "close".into(),
            threshold: 0.02,
        });
        assert_eq!(signal.name(), "gap_up");
    }

    #[test]
    fn build_signal_gap_down() {
        let signal = build_signal(&SignalSpec::GapDown {
            open_col: "open".into(),
            close_col: "close".into(),
            threshold: 0.02,
        });
        assert_eq!(signal.name(), "gap_down");
    }

    #[test]
    fn build_signal_drawdown() {
        let signal = build_signal(&SignalSpec::DrawdownBelow {
            column: "close".into(),
            window: 20,
            threshold: 0.05,
        });
        assert_eq!(signal.name(), "drawdown_below");
    }

    #[test]
    fn build_signal_consecutive_up() {
        let signal = build_signal(&SignalSpec::ConsecutiveUp {
            column: "close".into(),
            count: 3,
        });
        assert_eq!(signal.name(), "consecutive_up");
    }

    #[test]
    fn build_signal_consecutive_down() {
        let signal = build_signal(&SignalSpec::ConsecutiveDown {
            column: "close".into(),
            count: 3,
        });
        assert_eq!(signal.name(), "consecutive_down");
    }

    #[test]
    fn build_signal_rate_of_change() {
        let signal = build_signal(&SignalSpec::RateOfChange {
            column: "close".into(),
            period: 10,
            threshold: 0.05,
        });
        assert_eq!(signal.name(), "rate_of_change");
    }

    #[test]
    fn build_signal_mfi_oversold() {
        let signal = build_signal(&SignalSpec::MfiBelow {
            high_col: "high".into(),
            low_col: "low".into(),
            close_col: "close".into(),
            volume_col: "volume".into(),
            period: 14,
            threshold: 20.0,
        });
        assert_eq!(signal.name(), "mfi_below");
    }

    #[test]
    fn build_signal_mfi_overbought() {
        let signal = build_signal(&SignalSpec::MfiAbove {
            high_col: "high".into(),
            low_col: "low".into(),
            close_col: "close".into(),
            volume_col: "volume".into(),
            period: 14,
            threshold: 80.0,
        });
        assert_eq!(signal.name(), "mfi_above");
    }

    #[test]
    fn build_signal_obv_rising() {
        let signal = build_signal(&SignalSpec::ObvRising {
            price_col: "close".into(),
            volume_col: "volume".into(),
        });
        assert_eq!(signal.name(), "obv_rising");
    }

    #[test]
    fn build_signal_obv_falling() {
        let signal = build_signal(&SignalSpec::ObvFalling {
            price_col: "close".into(),
            volume_col: "volume".into(),
        });
        assert_eq!(signal.name(), "obv_falling");
    }

    #[test]
    fn build_signal_cmf_positive() {
        let signal = build_signal(&SignalSpec::CmfPositive {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            volume_col: "volume".into(),
            period: 20,
        });
        assert_eq!(signal.name(), "cmf_positive");
    }

    #[test]
    fn build_signal_cmf_negative() {
        let signal = build_signal(&SignalSpec::CmfNegative {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            volume_col: "volume".into(),
            period: 20,
        });
        assert_eq!(signal.name(), "cmf_negative");
    }

    #[test]
    fn build_signal_or_combinator() {
        let spec = SignalSpec::Or {
            left: Box::new(SignalSpec::RsiBelow {
                column: "close".into(),
                threshold: 30.0,
            }),
            right: Box::new(SignalSpec::MacdBullish {
                column: "close".into(),
            }),
        };
        let signal = build_signal(&spec);
        assert_eq!(signal.name(), "or");
    }

    #[test]
    fn signal_spec_serde_round_trip_macd() {
        let spec = SignalSpec::MacdBullish {
            column: "close".into(),
        };
        let json = serde_json::to_string(&spec).unwrap();
        let parsed: SignalSpec = serde_json::from_str(&json).unwrap();
        if let SignalSpec::MacdBullish { column } = parsed {
            assert_eq!(column, "close");
        } else {
            panic!("expected MacdBullish");
        }
    }

    #[test]
    fn signal_spec_serde_round_trip_and_combinator() {
        let spec = SignalSpec::And {
            left: Box::new(SignalSpec::RsiBelow {
                column: "close".into(),
                threshold: 30.0,
            }),
            right: Box::new(SignalSpec::PriceAboveSma {
                column: "close".into(),
                period: 20,
            }),
        };
        let json = serde_json::to_string(&spec).unwrap();
        let parsed: SignalSpec = serde_json::from_str(&json).unwrap();
        if let SignalSpec::And { left, right } = parsed {
            assert!(matches!(*left, SignalSpec::RsiBelow { .. }));
            assert!(matches!(*right, SignalSpec::PriceAboveSma { .. }));
        } else {
            panic!("expected And");
        }
    }

    #[test]
    fn signal_spec_serde_round_trip_or_combinator() {
        let spec = SignalSpec::Or {
            left: Box::new(SignalSpec::GapUp {
                open_col: "open".into(),
                close_col: "close".into(),
                threshold: 0.02,
            }),
            right: Box::new(SignalSpec::GapDown {
                open_col: "open".into(),
                close_col: "close".into(),
                threshold: 0.02,
            }),
        };
        let json = serde_json::to_string(&spec).unwrap();
        let parsed: SignalSpec = serde_json::from_str(&json).unwrap();
        if let SignalSpec::Or { left, right } = parsed {
            if let SignalSpec::GapUp {
                open_col,
                close_col,
                threshold,
            } = *left
            {
                assert_eq!(open_col, "open");
                assert_eq!(close_col, "close");
                assert_eq!(threshold, 0.02);
            } else {
                panic!("expected GapUp on left");
            }
            assert!(matches!(*right, SignalSpec::GapDown { .. }));
        } else {
            panic!("expected Or");
        }
    }

    #[test]
    fn signal_spec_serde_round_trip_stochastic() {
        let spec = SignalSpec::StochasticBelow {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 14,
            threshold: 20.0,
        };
        let json = serde_json::to_string(&spec).unwrap();
        let parsed: SignalSpec = serde_json::from_str(&json).unwrap();
        if let SignalSpec::StochasticBelow {
            close_col,
            high_col,
            low_col,
            period,
            threshold,
        } = parsed
        {
            assert_eq!(close_col, "close");
            assert_eq!(high_col, "high");
            assert_eq!(low_col, "low");
            assert_eq!(period, 14);
            assert_eq!(threshold, 20.0);
        } else {
            panic!("expected StochasticBelow");
        }
    }

    #[test]
    fn signal_spec_serde_round_trip_keltner() {
        let spec = SignalSpec::KeltnerUpperBreak {
            close_col: "close".into(),
            high_col: "high".into(),
            low_col: "low".into(),
            period: 20,
            multiplier: 2.0,
        };
        let json = serde_json::to_string(&spec).unwrap();
        let parsed: SignalSpec = serde_json::from_str(&json).unwrap();
        if let SignalSpec::KeltnerUpperBreak {
            close_col,
            high_col,
            low_col,
            period,
            multiplier,
        } = parsed
        {
            assert_eq!(close_col, "close");
            assert_eq!(high_col, "high");
            assert_eq!(low_col, "low");
            assert_eq!(period, 20);
            assert_eq!(multiplier, 2.0);
        } else {
            panic!("expected KeltnerUpperBreak");
        }
    }

    #[test]
    fn signal_spec_serde_round_trip_volume() {
        let spec = SignalSpec::ObvRising {
            price_col: "close".into(),
            volume_col: "volume".into(),
        };
        let json = serde_json::to_string(&spec).unwrap();
        let parsed: SignalSpec = serde_json::from_str(&json).unwrap();
        if let SignalSpec::ObvRising {
            price_col,
            volume_col,
        } = parsed
        {
            assert_eq!(price_col, "close");
            assert_eq!(volume_col, "volume");
        } else {
            panic!("expected ObvRising");
        }
    }

    #[test]
    fn catalog_entries_have_non_empty_fields() {
        for info in SIGNAL_CATALOG {
            assert!(!info.name.is_empty());
            assert!(!info.category.is_empty());
            assert!(!info.description.is_empty());
            assert!(!info.params.is_empty());
        }
    }

    #[test]
    fn catalog_categories_are_valid() {
        let valid_categories = [
            "momentum",
            "overlap",
            "trend",
            "volatility",
            "price",
            "volume",
            "cross-symbol",
        ];
        for info in SIGNAL_CATALOG {
            assert!(
                valid_categories.contains(&info.category),
                "unexpected category: {}",
                info.category
            );
        }
    }
}
