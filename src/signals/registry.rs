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
    // Formula functions (available in Custom formula DSL)
    SignalInfo {
        name: "rsi (formula)",
        category: "formula_functions",
        description: "RSI as a formula function with variable period. Uses Wilder smoothing.",
        params: "rsi(column, period) — e.g. rsi(close, 14) < 30",
    },
    SignalInfo {
        name: "macd_hist (formula)",
        category: "formula_functions",
        description: "MACD histogram (12/26/9 default) as a formula function.",
        params: "macd_hist(column) — e.g. macd_hist(close) > 0",
    },
    SignalInfo {
        name: "macd_signal (formula)",
        category: "formula_functions",
        description: "MACD signal line as a formula function.",
        params: "macd_signal(column) — e.g. macd_signal(close)",
    },
    SignalInfo {
        name: "macd_line (formula)",
        category: "formula_functions",
        description: "MACD line as a formula function.",
        params: "macd_line(column) — e.g. macd_line(close)",
    },
    SignalInfo {
        name: "roc (formula)",
        category: "formula_functions",
        description: "Rate of change (%) as a formula function.",
        params: "roc(column, period) — e.g. roc(close, 10) > 5",
    },
    SignalInfo {
        name: "bbands_upper (formula)",
        category: "formula_functions",
        description: "Bollinger upper band (SMA + 2σ) as a formula function.",
        params: "bbands_upper(column, period) — e.g. close > bbands_upper(close, 20)",
    },
    SignalInfo {
        name: "bbands_lower (formula)",
        category: "formula_functions",
        description: "Bollinger lower band (SMA - 2σ) as a formula function.",
        params: "bbands_lower(column, period) — e.g. close < bbands_lower(close, 20)",
    },
    SignalInfo {
        name: "bbands_mid (formula)",
        category: "formula_functions",
        description: "Bollinger middle band (= SMA) as a formula function.",
        params: "bbands_mid(column, period) — e.g. close > bbands_mid(close, 20)",
    },
    SignalInfo {
        name: "atr (formula)",
        category: "formula_functions",
        description: "Average True Range as a formula function. Multi-column.",
        params: "atr(close, high, low, period) — e.g. atr(close, high, low, 14) > 2.0",
    },
    SignalInfo {
        name: "stochastic (formula)",
        category: "formula_functions",
        description: "Stochastic %K as a formula function. Multi-column.",
        params: "stochastic(close, high, low, period) — e.g. stochastic(close, high, low, 14) < 20",
    },
    SignalInfo {
        name: "keltner_upper (formula)",
        category: "formula_functions",
        description: "Upper Keltner Channel as a formula function. Multi-column.",
        params: "keltner_upper(close, high, low, period, mult) — e.g. close > keltner_upper(close, high, low, 20, 2.0)",
    },
    SignalInfo {
        name: "keltner_lower (formula)",
        category: "formula_functions",
        description: "Lower Keltner Channel as a formula function. Multi-column.",
        params: "keltner_lower(close, high, low, period, mult) — e.g. close < keltner_lower(close, high, low, 20, 2.0)",
    },
    SignalInfo {
        name: "obv (formula)",
        category: "formula_functions",
        description: "On-Balance Volume as a formula function.",
        params: "obv(close, volume) — e.g. obv(close, volume) > 0",
    },
    SignalInfo {
        name: "mfi (formula)",
        category: "formula_functions",
        description: "Money Flow Index as a formula function. Multi-column.",
        params: "mfi(close, high, low, volume, period) — e.g. mfi(close, high, low, volume, 14) < 20",
    },
    SignalInfo {
        name: "tr (formula)",
        category: "formula_functions",
        description: "True Range as a formula function.",
        params: "tr(close, high, low) — e.g. tr(close, high, low) > 2.0",
    },
    SignalInfo {
        name: "rel_volume (formula)",
        category: "formula_functions",
        description: "Relative volume (volume / SMA) as a formula function.",
        params: "rel_volume(volume, period) — e.g. rel_volume(volume, 20) > 2.0",
    },
    SignalInfo {
        name: "range_pct (formula)",
        category: "formula_functions",
        description: "Position within bar range as a formula function.",
        params: "range_pct(close, high, low) — e.g. range_pct(close, high, low) < 0.2",
    },
    SignalInfo {
        name: "zscore (formula)",
        category: "formula_functions",
        description: "Z-score (deviations from rolling mean) as a formula function.",
        params: "zscore(column, period) — e.g. zscore(close, 20) < -2",
    },
    SignalInfo {
        name: "rank (formula)",
        category: "formula_functions",
        description: "Percentile rank within rolling window as a formula function.",
        params: "rank(column, period) — e.g. rank(close, 20) > 80",
    },
    SignalInfo {
        name: "if (formula)",
        category: "formula_functions",
        description: "Conditional expression in formula DSL.",
        params: "if(condition, then_value, else_value) — e.g. if(close > 100, 1, 0)",
    },
    SignalInfo {
        name: "aroon_up (formula)",
        category: "formula_functions",
        description: "Aroon Up indicator as a formula function.",
        params: "aroon_up(high, low, period) — e.g. aroon_up(high, low, 25) > 70",
    },
    SignalInfo {
        name: "aroon_down (formula)",
        category: "formula_functions",
        description: "Aroon Down indicator as a formula function.",
        params: "aroon_down(high, low, period) — e.g. aroon_down(high, low, 25) < 30",
    },
    SignalInfo {
        name: "aroon_osc (formula)",
        category: "formula_functions",
        description: "Aroon Oscillator (Up - Down) as a formula function.",
        params: "aroon_osc(high, low, period) — e.g. aroon_osc(high, low, 25) > 0",
    },
    SignalInfo {
        name: "supertrend (formula)",
        category: "formula_functions",
        description: "Supertrend line value as a formula function.",
        params: "supertrend(close, high, low, period, multiplier) — e.g. close > supertrend(close, high, low, 10, 3.0)",
    },
    SignalInfo {
        name: "cmf (formula)",
        category: "formula_functions",
        description: "Chaikin Money Flow as a formula function.",
        params: "cmf(close, high, low, volume, period) — e.g. cmf(close, high, low, volume, 20) > 0",
    },
    SignalInfo {
        name: "consecutive_up (formula)",
        category: "formula_functions",
        description: "Count of consecutive bar rises as a formula function.",
        params: "consecutive_up(column) — e.g. consecutive_up(close) >= 3",
    },
    SignalInfo {
        name: "consecutive_down (formula)",
        category: "formula_functions",
        description: "Count of consecutive bar falls as a formula function.",
        params: "consecutive_down(column) — e.g. consecutive_down(close) >= 3",
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
    fn build_signal_and_combinator() {
        let spec = SignalSpec::And {
            left: Box::new(SignalSpec::Custom {
                name: "rsi_low".into(),
                formula: "rsi(close, 14) < 30".into(),
                description: None,
            }),
            right: Box::new(SignalSpec::Custom {
                name: "macd_bull".into(),
                formula: "macd_hist(close) > 0".into(),
                description: None,
            }),
        };
        let signal = build_signal(&spec);
        assert_eq!(signal.name(), "and");
    }

    #[test]
    fn catalog_has_all_signals() {
        // 28 signals (27 formula_functions entries + 1 CrossSymbol entry)
        assert_eq!(SIGNAL_CATALOG.len(), 28);
    }

    #[test]
    fn collect_cross_symbols_empty_for_plain() {
        let spec = SignalSpec::Custom {
            name: "consecutive_up".into(),
            formula: "consecutive_up(close) >= 2".into(),
            description: None,
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
                signal: Box::new(SignalSpec::Custom {
                    name: "vix_up".into(),
                    formula: "consecutive_up(close) >= 2".into(),
                    description: None,
                }),
            }),
            right: Box::new(SignalSpec::CrossSymbol {
                symbol: "GLD".into(),
                signal: Box::new(SignalSpec::Custom {
                    name: "gld_down".into(),
                    formula: "consecutive_down(close) >= 3".into(),
                    description: None,
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
    fn build_signal_or_combinator() {
        let spec = SignalSpec::Or {
            left: Box::new(SignalSpec::Custom {
                name: "rsi_low".into(),
                formula: "rsi(close, 14) < 30".into(),
                description: None,
            }),
            right: Box::new(SignalSpec::Custom {
                name: "macd_bull".into(),
                formula: "macd_hist(close) > 0".into(),
                description: None,
            }),
        };
        let signal = build_signal(&spec);
        assert_eq!(signal.name(), "or");
    }

    #[test]
    fn signal_spec_serde_round_trip_and_combinator() {
        let spec = SignalSpec::And {
            left: Box::new(SignalSpec::Custom {
                name: "rsi_low".into(),
                formula: "rsi(close, 14) < 30".into(),
                description: None,
            }),
            right: Box::new(SignalSpec::Custom {
                name: "price_above_sma".into(),
                formula: "close > sma(close, 20)".into(),
                description: None,
            }),
        };
        let json = serde_json::to_string(&spec).unwrap();
        let parsed: SignalSpec = serde_json::from_str(&json).unwrap();
        if let SignalSpec::And { left, right } = parsed {
            assert!(matches!(*left, SignalSpec::Custom { .. }));
            assert!(matches!(*right, SignalSpec::Custom { .. }));
        } else {
            panic!("expected And");
        }
    }

    #[test]
    fn signal_spec_serde_round_trip_or_combinator() {
        let spec = SignalSpec::Or {
            left: Box::new(SignalSpec::Custom {
                name: "gap_up".into(),
                formula: "open / close.shift(1) - 1 > 0.02".into(),
                description: None,
            }),
            right: Box::new(SignalSpec::Custom {
                name: "gap_down".into(),
                formula: "open / close.shift(1) - 1 < -0.02".into(),
                description: None,
            }),
        };
        let json = serde_json::to_string(&spec).unwrap();
        let parsed: SignalSpec = serde_json::from_str(&json).unwrap();
        if let SignalSpec::Or { left, right } = parsed {
            assert!(matches!(*left, SignalSpec::Custom { .. }));
            assert!(matches!(*right, SignalSpec::Custom { .. }));
        } else {
            panic!("expected Or");
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
        let valid_categories = ["cross-symbol", "formula_functions"];
        for info in SIGNAL_CATALOG {
            assert!(
                valid_categories.contains(&info.category),
                "unexpected category: {}",
                info.category
            );
        }
    }
}
