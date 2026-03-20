pub use super::builders::build_signal;
pub use super::spec::SignalSpec;

/// Metadata about a signal for the `list_signals` catalog.
pub struct SignalInfo {
    pub name: &'static str,
    pub category: &'static str,
    pub description: &'static str,
    pub params: &'static str,
    pub formula_example: &'static str,
}

pub const SIGNAL_CATALOG: &[SignalInfo] = &[
    // ── Momentum ──────────────────────────────────────────────────────
    SignalInfo {
        name: "RSI Below",
        category: "momentum",
        description: "RSI below threshold (oversold)",
        params: "column, period, threshold",
        formula_example: "rsi(close, 14) < 30",
    },
    SignalInfo {
        name: "RSI Above",
        category: "momentum",
        description: "RSI above threshold (overbought)",
        params: "column, period, threshold",
        formula_example: "rsi(close, 14) > 70",
    },
    SignalInfo {
        name: "MACD Bullish",
        category: "momentum",
        description: "MACD histogram positive (bullish momentum)",
        params: "column",
        formula_example: "macd_hist(close) > 0",
    },
    SignalInfo {
        name: "MACD Signal Cross Up",
        category: "momentum",
        description: "MACD line crosses above signal line",
        params: "column",
        formula_example: "macd_line(close) > macd_signal(close)",
    },
    SignalInfo {
        name: "MACD Signal Cross Down",
        category: "momentum",
        description: "MACD line crosses below signal line",
        params: "column",
        formula_example: "macd_line(close) < macd_signal(close)",
    },
    SignalInfo {
        name: "MACD Line",
        category: "momentum",
        description: "MACD line value (12/26/9 default)",
        params: "column",
        formula_example: "macd_line(close)",
    },
    SignalInfo {
        name: "Stochastic Oversold",
        category: "momentum",
        description: "Stochastic %K below threshold (oversold)",
        params: "close, high, low, period",
        formula_example: "stochastic(close, high, low, 14) < 20",
    },
    SignalInfo {
        name: "Stochastic Overbought",
        category: "momentum",
        description: "Stochastic %K above threshold (overbought)",
        params: "close, high, low, period",
        formula_example: "stochastic(close, high, low, 14) > 80",
    },
    SignalInfo {
        name: "Rate of Change",
        category: "momentum",
        description: "Rate of change (%) over a period",
        params: "column, period",
        formula_example: "roc(close, 10) > 5",
    },
    SignalInfo {
        name: "MFI Oversold",
        category: "momentum",
        description: "Money Flow Index below threshold (oversold)",
        params: "close, high, low, volume, period",
        formula_example: "mfi(close, high, low, volume, 14) < 20",
    },
    SignalInfo {
        name: "MFI Overbought",
        category: "momentum",
        description: "Money Flow Index above threshold (overbought)",
        params: "close, high, low, volume, period",
        formula_example: "mfi(close, high, low, volume, 14) > 80",
    },
    SignalInfo {
        name: "Consecutive Up",
        category: "momentum",
        description: "Count of consecutive bar rises",
        params: "column",
        formula_example: "consecutive_up(close) >= 3",
    },
    SignalInfo {
        name: "Consecutive Down",
        category: "momentum",
        description: "Count of consecutive bar falls",
        params: "column",
        formula_example: "consecutive_down(close) >= 3",
    },
    SignalInfo {
        name: "Williams %R Oversold",
        category: "momentum",
        description: "Williams %R below threshold (oversold, near -100)",
        params: "high, low, close, period",
        formula_example: "williams_r(high, low, close, 14) < -80",
    },
    SignalInfo {
        name: "Williams %R Overbought",
        category: "momentum",
        description: "Williams %R above threshold (overbought, near 0)",
        params: "high, low, close, period",
        formula_example: "williams_r(high, low, close, 14) > -20",
    },
    SignalInfo {
        name: "CCI Oversold",
        category: "momentum",
        description: "Commodity Channel Index below threshold (oversold)",
        params: "column, period",
        formula_example: "cci(close, 20) < -100",
    },
    SignalInfo {
        name: "CCI Overbought",
        category: "momentum",
        description: "Commodity Channel Index above threshold (overbought)",
        params: "column, period",
        formula_example: "cci(close, 20) > 100",
    },
    SignalInfo {
        name: "PPO Bullish",
        category: "momentum",
        description: "Percentage Price Oscillator positive (bullish momentum)",
        params: "column, short_period, long_period",
        formula_example: "ppo(close, 12, 26) > 0",
    },
    SignalInfo {
        name: "CMO Oversold",
        category: "momentum",
        description: "Chande Momentum Oscillator below threshold (oversold)",
        params: "column, period",
        formula_example: "cmo(close, 14) < -50",
    },
    // ── Overlap ───────────────────────────────────────────────────────
    SignalInfo {
        name: "Price Above SMA",
        category: "overlap",
        description: "Price above Simple Moving Average",
        params: "column, period",
        formula_example: "close > sma(close, 50)",
    },
    SignalInfo {
        name: "Price Below SMA",
        category: "overlap",
        description: "Price below Simple Moving Average",
        params: "column, period",
        formula_example: "close < sma(close, 50)",
    },
    SignalInfo {
        name: "Price Above EMA",
        category: "overlap",
        description: "Price above Exponential Moving Average",
        params: "column, period",
        formula_example: "close > ema(close, 20)",
    },
    SignalInfo {
        name: "Price Below EMA",
        category: "overlap",
        description: "Price below Exponential Moving Average",
        params: "column, period",
        formula_example: "close < ema(close, 20)",
    },
    SignalInfo {
        name: "Bollinger Upper Break",
        category: "overlap",
        description: "Price breaks above Bollinger upper band (SMA + 2σ)",
        params: "column, period",
        formula_example: "close > bbands_upper(close, 20)",
    },
    SignalInfo {
        name: "Bollinger Lower Break",
        category: "overlap",
        description: "Price breaks below Bollinger lower band (SMA - 2σ)",
        params: "column, period",
        formula_example: "close < bbands_lower(close, 20)",
    },
    SignalInfo {
        name: "Bollinger Mid Cross",
        category: "overlap",
        description: "Price crosses above Bollinger middle band (SMA)",
        params: "column, period",
        formula_example: "close > bbands_mid(close, 20)",
    },
    SignalInfo {
        name: "Keltner Upper Break",
        category: "overlap",
        description: "Price breaks above upper Keltner Channel",
        params: "close, high, low, period, mult",
        formula_example: "close > keltner_upper(close, high, low, 20, 2.0)",
    },
    SignalInfo {
        name: "Keltner Lower Break",
        category: "overlap",
        description: "Price breaks below lower Keltner Channel",
        params: "close, high, low, period, mult",
        formula_example: "close < keltner_lower(close, high, low, 20, 2.0)",
    },
    // ── Trend ─────────────────────────────────────────────────────────
    SignalInfo {
        name: "Supertrend Bullish",
        category: "trend",
        description: "Price above Supertrend line (bullish trend)",
        params: "close, high, low, period, multiplier",
        formula_example: "close > supertrend(close, high, low, 10, 3.0)",
    },
    SignalInfo {
        name: "Aroon Up Strong",
        category: "trend",
        description: "Aroon Up indicator above threshold (strong uptrend)",
        params: "high, low, period",
        formula_example: "aroon_up(high, low, 25) > 70",
    },
    SignalInfo {
        name: "Aroon Down Weak",
        category: "trend",
        description: "Aroon Down indicator below threshold (weakening downtrend)",
        params: "high, low, period",
        formula_example: "aroon_down(high, low, 25) < 30",
    },
    SignalInfo {
        name: "Aroon Oscillator Positive",
        category: "trend",
        description: "Aroon Oscillator (Up - Down) positive (bullish bias)",
        params: "high, low, period",
        formula_example: "aroon_osc(high, low, 25) > 0",
    },
    SignalInfo {
        name: "ADX Strong Trend",
        category: "trend",
        description: "Average Directional Index above threshold (strong trend)",
        params: "high, low, close, period",
        formula_example: "adx(high, low, close, 14) > 25",
    },
    SignalInfo {
        name: "+DI Above -DI",
        category: "trend",
        description: "Positive DI above Negative DI (bullish trend)",
        params: "high, low, close, period",
        formula_example: "plus_di(high, low, close, 14) > minus_di(high, low, close, 14)",
    },
    SignalInfo {
        name: "Parabolic SAR Bullish",
        category: "trend",
        description: "Price above Parabolic SAR (bullish, SAR below price)",
        params: "high, low, accel, max_accel",
        formula_example: "close > psar(high, low, 0.02, 0.2)",
    },
    SignalInfo {
        name: "TSI Bullish",
        category: "trend",
        description: "True Strength Index positive (bullish momentum)",
        params: "column, fast_period, slow_period",
        formula_example: "tsi(close, 13, 25) > 0",
    },
    SignalInfo {
        name: "VPT Above MA",
        category: "volume",
        description: "Volume Price Trend above its moving average",
        params: "close, volume",
        formula_example: "vpt(close, volume) > sma(vpt(close, volume), 20)",
    },
    // ── Channels ────────────────────────────────────────────────────
    SignalInfo {
        name: "Donchian Upper Break",
        category: "overlap",
        description: "Price breaks above Donchian Channel upper band",
        params: "high, low, period",
        formula_example: "close > donchian_upper(high, low, 20)",
    },
    SignalInfo {
        name: "Donchian Lower Break",
        category: "overlap",
        description: "Price breaks below Donchian Channel lower band",
        params: "high, low, period",
        formula_example: "close < donchian_lower(high, low, 20)",
    },
    SignalInfo {
        name: "Ichimoku Cloud Bullish",
        category: "overlap",
        description: "Price above Ichimoku Cloud (Senkou Span A and B)",
        params: "high, low, close",
        formula_example: "close > ichimoku_senkou_a(high, low, close) and close > ichimoku_senkou_b(high, low, close)",
    },
    SignalInfo {
        name: "Ichimoku TK Cross",
        category: "overlap",
        description: "Tenkan-sen crosses above Kijun-sen (bullish signal)",
        params: "high, low, close",
        formula_example: "ichimoku_tenkan(high, low, close) > ichimoku_kijun(high, low, close)",
    },
    SignalInfo {
        name: "Envelope Upper Break",
        category: "overlap",
        description: "Price breaks above MA Envelope upper band",
        params: "column, period, pct",
        formula_example: "close > envelope_upper(close, 20, 2.5)",
    },
    SignalInfo {
        name: "Envelope Lower Break",
        category: "overlap",
        description: "Price breaks below MA Envelope lower band",
        params: "column, period, pct",
        formula_example: "close < envelope_lower(close, 20, 2.5)",
    },
    // ── Volatility ────────────────────────────────────────────────────
    SignalInfo {
        name: "ATR High",
        category: "volatility",
        description: "Average True Range above threshold (high volatility)",
        params: "close, high, low, period",
        formula_example: "atr(close, high, low, 14) > 2.0",
    },
    SignalInfo {
        name: "True Range",
        category: "volatility",
        description: "True Range above threshold",
        params: "close, high, low",
        formula_example: "tr(close, high, low) > 2.0",
    },
    SignalInfo {
        name: "Z-Score Extreme",
        category: "volatility",
        description: "Z-score deviation from rolling mean (extreme move)",
        params: "column, period",
        formula_example: "zscore(close, 20) < -2",
    },
    SignalInfo {
        name: "Ulcer Index High",
        category: "volatility",
        description: "Ulcer Index above threshold (high downside risk)",
        params: "column, period",
        formula_example: "ulcer(close, 14) > 5",
    },
    // ── Volume ────────────────────────────────────────────────────────
    SignalInfo {
        name: "OBV Positive",
        category: "volume",
        description: "On-Balance Volume positive (accumulation)",
        params: "close, volume",
        formula_example: "obv(close, volume) > 0",
    },
    SignalInfo {
        name: "CMF Positive",
        category: "volume",
        description: "Chaikin Money Flow positive (buying pressure)",
        params: "close, high, low, volume, period",
        formula_example: "cmf(close, high, low, volume, 20) > 0",
    },
    SignalInfo {
        name: "Relative Volume Spike",
        category: "volume",
        description: "Relative volume above threshold (volume spike)",
        params: "volume, period",
        formula_example: "rel_volume(volume, 20) > 2.0",
    },
    SignalInfo {
        name: "A/D Line Positive",
        category: "volume",
        description: "Accumulation/Distribution line positive (accumulation)",
        params: "high, low, close, volume",
        formula_example: "ad(high, low, close, volume) > 0",
    },
    SignalInfo {
        name: "VPT Positive",
        category: "volume",
        description: "Volume Price Trend positive (volume-confirmed trend)",
        params: "close, volume",
        formula_example: "vpt(close, volume) > 0",
    },
    SignalInfo {
        name: "PVI Above SMA",
        category: "volume",
        description: "Positive Volume Index above its moving average",
        params: "close, volume",
        formula_example: "pvi(close, volume) > sma(pvi(close, volume), 255)",
    },
    SignalInfo {
        name: "NVI Above SMA",
        category: "volume",
        description: "Negative Volume Index above its moving average",
        params: "close, volume",
        formula_example: "nvi(close, volume) > sma(nvi(close, volume), 255)",
    },
    // ── Price ─────────────────────────────────────────────────────────
    SignalInfo {
        name: "IBS Low",
        category: "price",
        description: "Internal Bar Strength low (close near bar low)",
        params: "close, high, low",
        formula_example: "range_pct(close, high, low) < 0.2",
    },
    // ── IV ────────────────────────────────────────────────────────────
    SignalInfo {
        name: "IV Percentile Low",
        category: "iv",
        description: "IV Percentile below threshold (low implied volatility)",
        params: "column, period",
        formula_example: "rank(iv, 252) < 10",
    },
    SignalInfo {
        name: "IV Rank High",
        category: "iv",
        description: "IV Rank above threshold (elevated implied volatility)",
        params: "column, period",
        formula_example: "iv_rank(iv, 252) > 50",
    },
    // ── Datetime ─────────────────────────────────────────────────────
    SignalInfo {
        name: "Day of Week Filter",
        category: "datetime",
        description: "Filter by day of week (1=Mon..7=Sun, ISO 8601)",
        params: "(none)",
        formula_example: "day_of_week() == 1",
    },
    SignalInfo {
        name: "Month Filter",
        category: "datetime",
        description: "Filter by month (1-12), useful for seasonal patterns",
        params: "(none)",
        formula_example: "month() >= 11 or month() <= 4",
    },
    SignalInfo {
        name: "Week of Year",
        category: "datetime",
        description: "Filter by ISO week number (1-53)",
        params: "(none)",
        formula_example: "week_of_year() <= 10",
    },
    SignalInfo {
        name: "Time Window",
        category: "datetime",
        description: "Filter by hour of day (0-23), useful for intraday patterns",
        params: "(none)",
        formula_example: "hour() >= 9 and hour() <= 15",
    },
    // ── Utility ───────────────────────────────────────────────────────
    SignalInfo {
        name: "Conditional",
        category: "utility",
        description: "Conditional expression (if/then/else)",
        params: "condition, then, else",
        formula_example: "if(close > sma(close, 50), 1, 0)",
    },
];

/// Collect all secondary symbols referenced in a signal tree (via formula cross-symbol syntax).
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
        SignalSpec::And { left, right } | SignalSpec::Or { left, right } => {
            collect_cross_symbols_inner(left, out, visited_saved, depth);
            collect_cross_symbols_inner(right, out, visited_saved, depth);
        }
        SignalSpec::Saved { name } => {
            if !visited_saved.insert(name.clone()) {
                return;
            }
            if let Ok((loaded_spec, _)) = super::storage::load_signal(name) {
                collect_cross_symbols_inner(&loaded_spec, out, visited_saved, depth + 1);
            }
        }
        SignalSpec::Formula { formula } => {
            out.extend(extract_formula_cross_symbols(formula));
        }
    }
}

/// Extract cross-symbol references from a formula string.
///
/// Tokenizes the formula and identifies `Ident` tokens that are NOT valid column
/// names and NOT known function names. Also handles `Ident.Ident` dot-access patterns.
/// Returns the set of uppercase symbol names.
pub fn extract_formula_cross_symbols(formula: &str) -> std::collections::HashSet<String> {
    use super::custom_funcs::KNOWN_FUNCTIONS;

    let Ok(tokens) = super::custom::tokenize(formula) else {
        return std::collections::HashSet::new();
    };

    let valid_columns: &[&str] = &["close", "open", "high", "low", "volume", "adjclose", "iv"];
    let mut symbols = std::collections::HashSet::new();
    let mut i = 0;

    while i < tokens.len() {
        if let super::custom::Token::Ident(ref name) = tokens[i] {
            let lower = name.to_lowercase();

            // Skip keywords that are handled as separate tokens (and, or, not, true, false)
            // — these are already tokenized differently, so they won't be Ident tokens.

            // Known column → skip
            if valid_columns.contains(&lower.as_str()) {
                i += 1;
                continue;
            }

            // Known function followed by '(' → skip
            if KNOWN_FUNCTIONS.contains(&lower.as_str()) {
                i += 1;
                continue;
            }

            // This is a cross-symbol reference
            symbols.insert(name.to_uppercase());

            // If followed by Dot + Ident, skip those tokens (they're the column accessor)
            if i + 2 < tokens.len()
                && matches!(tokens[i + 1], super::custom::Token::Dot)
                && matches!(tokens[i + 2], super::custom::Token::Ident(_))
            {
                i += 3;
                continue;
            }
        }
        i += 1;
    }

    symbols
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_has_all_signals() {
        // 62 signals across 9 domain categories (CrossSymbol removed)
        assert_eq!(SIGNAL_CATALOG.len(), 62);
    }

    #[test]
    fn collect_cross_symbols_empty_for_plain() {
        let spec = SignalSpec::Formula {
            formula: "consecutive_up(close) >= 2".into(),
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
    fn catalog_entries_have_non_empty_fields() {
        for info in SIGNAL_CATALOG {
            assert!(!info.name.is_empty());
            assert!(!info.category.is_empty());
            assert!(!info.description.is_empty());
            assert!(!info.params.is_empty());
            assert!(!info.formula_example.is_empty());
        }
    }

    #[test]
    fn catalog_categories_are_valid() {
        let valid_categories = [
            "momentum",
            "overlap",
            "trend",
            "volatility",
            "volume",
            "price",
            "iv",
            "datetime",
            "utility",
        ];
        for info in SIGNAL_CATALOG {
            assert!(
                valid_categories.contains(&info.category),
                "unexpected category: {}",
                info.category
            );
        }
    }

    // ── Formula cross-symbol extraction tests ───────────────────────────

    #[test]
    fn extract_formula_cross_symbols_basic() {
        let syms = extract_formula_cross_symbols("VIX / VIX3M < 0.9");
        assert_eq!(syms.len(), 2);
        assert!(syms.contains("VIX"));
        assert!(syms.contains("VIX3M"));
    }

    #[test]
    fn extract_formula_cross_symbols_empty_for_plain() {
        let syms = extract_formula_cross_symbols("close > sma(close, 20)");
        assert!(syms.is_empty());
    }

    #[test]
    fn extract_formula_cross_symbols_dot_syntax() {
        let syms = extract_formula_cross_symbols("VIX.high / VIX3M.low > 1.1");
        assert_eq!(syms.len(), 2);
        assert!(syms.contains("VIX"));
        assert!(syms.contains("VIX3M"));
    }

    #[test]
    fn extract_formula_cross_symbols_mixed() {
        let syms = extract_formula_cross_symbols("VIX > 30 and rsi(close, 14) < 30");
        assert_eq!(syms.len(), 1);
        assert!(syms.contains("VIX"));
    }

    #[test]
    fn extract_formula_cross_symbols_in_function() {
        let syms = extract_formula_cross_symbols("sma(VIX, 20) > 15");
        assert_eq!(syms.len(), 1);
        assert!(syms.contains("VIX"));
    }

    #[test]
    fn collect_cross_symbols_detects_formula_refs() {
        // Formula inside an And combinator should detect cross-symbol refs
        let spec = SignalSpec::And {
            left: Box::new(SignalSpec::Formula {
                formula: "VIX / VIX3M < 0.9".into(),
            }),
            right: Box::new(SignalSpec::Formula {
                formula: "rsi(close, 14) < 30".into(),
            }),
        };
        let symbols = collect_cross_symbols(&spec);
        assert_eq!(symbols.len(), 2);
        assert!(symbols.contains("VIX"));
        assert!(symbols.contains("VIX3M"));
    }
}
