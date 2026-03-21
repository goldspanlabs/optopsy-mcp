//! Formula rewriting for `hmm_regime()` calls.
//!
//! Scans a formula string for `hmm_regime(...)` patterns, extracts arguments,
//! resolves named regime aliases to integer literals, and rewrites the formula
//! to reference pre-computed `__hmm_regime_*` columns.

/// Extracted HMM regime call parameters.
#[derive(Debug, Clone, PartialEq)]
pub struct HmmCall {
    /// Symbol to fit HMM on (uppercase). `None` = use primary backtest symbol.
    pub symbol: Option<String>,
    /// Number of HMM states (2-4).
    pub n_regimes: usize,
    /// Years of pre-backtest data for fitting.
    pub fit_years: usize,
    /// Posterior probability threshold for regime switching.
    pub threshold: f64,
}

/// Result of rewriting a formula: the rewritten string + extracted HMM calls.
#[derive(Debug, Clone)]
pub struct RewriteResult {
    /// The rewritten formula string (`hmm_regime` calls replaced with column refs).
    pub formula: String,
    /// Deduplicated HMM calls found in the formula.
    pub calls: Vec<HmmCall>,
    /// Column names injected (one per unique call).
    pub injected_columns: Vec<String>,
}

/// Default posterior threshold.
pub const DEFAULT_THRESHOLD: f64 = 0.65;

/// All regime alias names that must not leak past rewriting.
const REGIME_ALIASES: &[&str] = &[
    "bearish",
    "bullish",
    "neutral",
    "strong_bear",
    "mild_bear",
    "mild_bull",
    "strong_bull",
];

/// Build the column name for an HMM regime call.
pub fn column_name(symbol: &str, n_regimes: usize, fit_years: usize, threshold: f64) -> String {
    let thresh_int = (threshold * 100.0).round() as u32;
    format!("__hmm_regime_{symbol}_{n_regimes}_{fit_years}_{thresh_int}")
}

/// Return the valid alias names for a given `n_regimes`.
pub fn aliases_for(n_regimes: usize) -> &'static [&'static str] {
    match n_regimes {
        2 => &["bearish", "bullish"],
        3 => &["bearish", "neutral", "bullish"],
        4 => &["strong_bear", "mild_bear", "mild_bull", "strong_bull"],
        _ => &[],
    }
}

/// Resolve a named alias to its integer index for the given `n_regimes`.
pub fn alias_to_index(alias: &str, n_regimes: usize) -> Option<usize> {
    aliases_for(n_regimes).iter().position(|&a| a == alias)
}

/// Extract all `hmm_regime(...)` calls from a formula string.
pub fn extract_hmm_calls(formula: &str) -> Result<Vec<HmmCall>, String> {
    let mut calls = Vec::new();
    let mut search_from = 0;

    while let Some(start) = formula[search_from..].find("hmm_regime(") {
        let abs_start = search_from + start;
        let args_start = abs_start + "hmm_regime(".len();

        let mut depth = 1;
        let mut end = args_start;
        for (i, ch) in formula[args_start..].char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = args_start + i;
                        break;
                    }
                }
                _ => {}
            }
        }
        if depth != 0 {
            return Err("Unclosed parenthesis in hmm_regime() call".to_string());
        }

        let args_str = &formula[args_start..end];
        let call = parse_hmm_args(args_str)?;
        calls.push(call);
        search_from = end + 1;
    }

    Ok(calls)
}

fn parse_hmm_args(args_str: &str) -> Result<HmmCall, String> {
    let parts: Vec<&str> = args_str.split(',').map(str::trim).collect();

    match parts.len() {
        2 => {
            let n_regimes = parse_int(parts[0], "n_regimes")?;
            let fit_years = parse_int(parts[1], "fit_years")?;
            validate_params(n_regimes, fit_years, DEFAULT_THRESHOLD)?;
            Ok(HmmCall {
                symbol: None,
                n_regimes,
                fit_years,
                threshold: DEFAULT_THRESHOLD,
            })
        }
        3 => {
            if parts[0]
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic())
            {
                let symbol = parts[0].to_uppercase();
                let n_regimes = parse_int(parts[1], "n_regimes")?;
                let fit_years = parse_int(parts[2], "fit_years")?;
                validate_params(n_regimes, fit_years, DEFAULT_THRESHOLD)?;
                Ok(HmmCall {
                    symbol: Some(symbol),
                    n_regimes,
                    fit_years,
                    threshold: DEFAULT_THRESHOLD,
                })
            } else {
                let n_regimes = parse_int(parts[0], "n_regimes")?;
                let fit_years = parse_int(parts[1], "fit_years")?;
                let threshold = parse_float(parts[2], "threshold")?;
                validate_params(n_regimes, fit_years, threshold)?;
                Ok(HmmCall {
                    symbol: None,
                    n_regimes,
                    fit_years,
                    threshold,
                })
            }
        }
        4 => {
            let symbol = parts[0].to_uppercase();
            if !symbol
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic())
            {
                return Err(format!(
                    "hmm_regime first arg must be a symbol identifier, got '{}'",
                    parts[0]
                ));
            }
            let n_regimes = parse_int(parts[1], "n_regimes")?;
            let fit_years = parse_int(parts[2], "fit_years")?;
            let threshold = parse_float(parts[3], "threshold")?;
            validate_params(n_regimes, fit_years, threshold)?;
            Ok(HmmCall {
                symbol: Some(symbol),
                n_regimes,
                fit_years,
                threshold,
            })
        }
        n => Err(format!("hmm_regime expects 2, 3, or 4 arguments, got {n}")),
    }
}

fn parse_int(s: &str, name: &str) -> Result<usize, String> {
    s.parse::<usize>()
        .map_err(|_| format!("{name} must be an integer, got '{s}'"))
}

fn parse_float(s: &str, name: &str) -> Result<f64, String> {
    s.parse::<f64>()
        .map_err(|_| format!("{name} must be a number, got '{s}'"))
}

fn validate_params(n_regimes: usize, fit_years: usize, threshold: f64) -> Result<(), String> {
    if !(2..=4).contains(&n_regimes) {
        return Err(format!("n_regimes must be 2-4, got {n_regimes}"));
    }
    if !(1..=50).contains(&fit_years) {
        return Err(format!("fit_years must be 1-50, got {fit_years}"));
    }
    if threshold <= 0.5 || threshold > 1.0 {
        return Err(format!(
            "threshold must be between 0.5 (exclusive) and 1.0 (inclusive), got {threshold}"
        ));
    }
    Ok(())
}

/// Rewrite a formula string, replacing `hmm_regime(...)` calls and their comparison
/// operators/aliases with column references and integer literals.
///
/// `primary_symbol` is used when the formula omits the symbol arg.
pub fn rewrite_formula(formula: &str, primary_symbol: &str) -> Result<RewriteResult, String> {
    if !formula.contains("hmm_regime(") {
        check_alias_leak(formula)?;
        return Ok(RewriteResult {
            formula: formula.to_string(),
            calls: vec![],
            injected_columns: vec![],
        });
    }

    let mut result = formula.to_string();
    let mut all_calls = Vec::new();
    let mut seen_columns = std::collections::HashSet::new();

    loop {
        let Some(start) = result.find("hmm_regime(") else {
            break;
        };
        let args_start = start + "hmm_regime(".len();

        let mut depth = 1;
        let mut paren_end = args_start;
        for (i, ch) in result[args_start..].char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        paren_end = args_start + i;
                        break;
                    }
                }
                _ => {}
            }
        }
        if depth != 0 {
            return Err("Unclosed parenthesis in hmm_regime() call".to_string());
        }

        let args_str = &result[args_start..paren_end];
        let mut call = parse_hmm_args(args_str)?;

        let sym = call
            .symbol
            .clone()
            .unwrap_or_else(|| primary_symbol.to_uppercase());
        call.symbol = Some(sym.clone());

        let col_name = column_name(&sym, call.n_regimes, call.fit_years, call.threshold);

        let after = &result[paren_end + 1..];
        let after_trimmed = after.trim_start();

        let (op, op_len) = if after_trimmed.starts_with("==") {
            ("==", 2)
        } else if after_trimmed.starts_with("!=") {
            ("!=", 2)
        } else if after_trimmed.starts_with(">=") {
            (">=", 2)
        } else if after_trimmed.starts_with("<=") {
            ("<=", 2)
        } else if after_trimmed.starts_with('>') {
            (">", 1)
        } else if after_trimmed.starts_with('<') {
            ("<", 1)
        } else {
            return Err(
                "hmm_regime() must be used in a comparison (==, !=, >, <, >=, <=)".to_string(),
            );
        };

        let op_start_in_after = after.len() - after_trimmed.len();
        let rhs_start = op_start_in_after + op_len;
        let rhs_trimmed = after[rhs_start..].trim_start();

        let (rhs_value, rhs_token_len) = parse_rhs(rhs_trimmed, call.n_regimes)?;

        let expr_end = paren_end
            + 1
            + rhs_start
            + (after[rhs_start..].len() - rhs_trimmed.len())
            + rhs_token_len;

        let replacement = format!("{col_name} {op} {rhs_value}");
        result.replace_range(start..expr_end, &replacement);

        if seen_columns.insert(col_name.clone()) {
            all_calls.push(call);
        }
    }

    check_alias_leak(&result)?;

    let injected_columns: Vec<String> = all_calls
        .iter()
        .map(|c| {
            let sym = c.symbol.as_deref().unwrap_or(primary_symbol);
            column_name(sym, c.n_regimes, c.fit_years, c.threshold)
        })
        .collect();

    Ok(RewriteResult {
        formula: result,
        calls: all_calls,
        injected_columns,
    })
}

fn parse_rhs(rhs: &str, n_regimes: usize) -> Result<(String, usize), String> {
    let token_end = rhs.find(|c: char| !c.is_ascii_digit()).unwrap_or(rhs.len());
    if token_end > 0 && rhs[..token_end].parse::<usize>().is_ok() {
        let val = &rhs[..token_end];
        return Ok((val.to_string(), token_end));
    }

    let token_end = rhs
        .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .unwrap_or(rhs.len());
    if token_end == 0 {
        return Err("Expected a regime name or integer after comparison operator".to_string());
    }
    let name = &rhs[..token_end];

    if let Some(idx) = alias_to_index(name, n_regimes) {
        Ok((idx.to_string(), token_end))
    } else {
        let valid = aliases_for(n_regimes).join(", ");
        Err(format!(
            "unknown regime name '{name}' for n_regimes={n_regimes}; expected: {valid}"
        ))
    }
}

fn check_alias_leak(formula: &str) -> Result<(), String> {
    for token in formula.split(|c: char| !c.is_ascii_alphanumeric() && c != '_') {
        if REGIME_ALIASES.contains(&token) {
            return Err(format!(
                "regime alias '{token}' found outside hmm_regime() context. \
                 Named aliases (bearish, bullish, etc.) can only be used in \
                 hmm_regime() comparisons."
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_two_args_no_symbol() {
        let calls = extract_hmm_calls("hmm_regime(3, 5) == 2").unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0],
            HmmCall {
                symbol: None,
                n_regimes: 3,
                fit_years: 5,
                threshold: DEFAULT_THRESHOLD,
            }
        );
    }

    #[test]
    fn test_extract_three_args_with_symbol() {
        let calls = extract_hmm_calls("hmm_regime(SPY, 3, 5) == bullish").unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].symbol, Some("SPY".to_string()));
        assert_eq!(calls[0].n_regimes, 3);
        assert_eq!(calls[0].fit_years, 5);
        assert!((calls[0].threshold - DEFAULT_THRESHOLD).abs() < 1e-10);
    }

    #[test]
    fn test_extract_four_args_with_threshold() {
        let calls = extract_hmm_calls("hmm_regime(SPY, 3, 5, 0.8) == 1").unwrap();
        assert_eq!(calls.len(), 1);
        assert!((calls[0].threshold - 0.8).abs() < 1e-10);
    }

    #[test]
    fn test_extract_three_args_disambiguate_threshold() {
        let calls = extract_hmm_calls("hmm_regime(3, 5, 0.8) >= 1").unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].symbol, None);
        assert_eq!(calls[0].n_regimes, 3);
        assert_eq!(calls[0].fit_years, 5);
        assert!((calls[0].threshold - 0.8).abs() < 1e-10);
    }

    #[test]
    fn test_extract_wrong_arg_count() {
        assert!(extract_hmm_calls("hmm_regime(1) == 0").is_err());
        assert!(extract_hmm_calls("hmm_regime(A, 3, 5, 0.8, 99) == 0").is_err());
    }

    #[test]
    fn test_extract_n_regimes_out_of_range() {
        assert!(extract_hmm_calls("hmm_regime(1, 5) == 0").is_err());
        assert!(extract_hmm_calls("hmm_regime(5, 5) == 0").is_err());
    }

    #[test]
    fn test_extract_threshold_out_of_range() {
        assert!(extract_hmm_calls("hmm_regime(3, 5, 0.3) == 0").is_err());
    }

    #[test]
    fn test_rewrite_with_named_alias() {
        let result = rewrite_formula("hmm_regime(SPY, 3, 5) == bullish", "AAPL").unwrap();
        assert_eq!(result.formula, "__hmm_regime_SPY_3_5_65 == 2");
    }

    #[test]
    fn test_rewrite_with_numeric() {
        let result = rewrite_formula("hmm_regime(3, 5) >= 1", "SPY").unwrap();
        assert_eq!(result.formula, "__hmm_regime_SPY_3_5_65 >= 1");
    }

    #[test]
    fn test_rewrite_preserves_surrounding() {
        let result =
            rewrite_formula("hmm_regime(3, 5) == 1 and rsi(close, 14) < 30", "SPY").unwrap();
        assert_eq!(
            result.formula,
            "__hmm_regime_SPY_3_5_65 == 1 and rsi(close, 14) < 30"
        );
    }

    #[test]
    fn test_rewrite_no_symbol_uses_primary() {
        let result = rewrite_formula("hmm_regime(2, 5) == bullish", "AAPL").unwrap();
        assert_eq!(result.formula, "__hmm_regime_AAPL_2_5_65 == 1");
    }

    #[test]
    fn test_rewrite_multiple_calls() {
        let result = rewrite_formula(
            "hmm_regime(SPY, 3, 5) == bullish and hmm_regime(QQQ, 2, 3) == bearish",
            "AAPL",
        )
        .unwrap();
        assert!(result.formula.contains("__hmm_regime_SPY_3_5_65 == 2"));
        assert!(result.formula.contains("__hmm_regime_QQQ_2_3_65 == 0"));
        assert_eq!(result.calls.len(), 2);
    }

    #[test]
    fn test_rewrite_deduplicates_same_calls() {
        let result = rewrite_formula(
            "hmm_regime(SPY, 3, 5) == bullish or hmm_regime(SPY, 3, 5) != bearish",
            "AAPL",
        )
        .unwrap();
        assert_eq!(result.calls.len(), 1);
    }

    #[test]
    fn test_rewrite_unknown_alias_errors() {
        let err = rewrite_formula("hmm_regime(3, 5) == foobar", "SPY").unwrap_err();
        assert!(err.contains("unknown regime name"));
        assert!(err.contains("bearish, neutral, bullish"));
    }

    #[test]
    fn test_alias_leak_detected() {
        let err = rewrite_formula("close > 100 and bullish", "SPY").unwrap_err();
        assert!(err.contains("regime alias"));
    }

    #[test]
    fn test_no_hmm_calls_passthrough() {
        let result = rewrite_formula("rsi(close, 14) < 30", "SPY").unwrap();
        assert_eq!(result.formula, "rsi(close, 14) < 30");
        assert!(result.calls.is_empty());
    }

    #[test]
    fn test_column_name_format() {
        assert_eq!(column_name("SPY", 3, 5, 0.65), "__hmm_regime_SPY_3_5_65");
        assert_eq!(column_name("QQQ", 2, 10, 0.8), "__hmm_regime_QQQ_2_10_80");
    }

    #[test]
    fn test_alias_to_index() {
        assert_eq!(alias_to_index("bearish", 2), Some(0));
        assert_eq!(alias_to_index("bullish", 2), Some(1));
        assert_eq!(alias_to_index("bearish", 3), Some(0));
        assert_eq!(alias_to_index("neutral", 3), Some(1));
        assert_eq!(alias_to_index("bullish", 3), Some(2));
        assert_eq!(alias_to_index("strong_bear", 4), Some(0));
        assert_eq!(alias_to_index("mild_bear", 4), Some(1));
        assert_eq!(alias_to_index("mild_bull", 4), Some(2));
        assert_eq!(alias_to_index("strong_bull", 4), Some(3));
        assert_eq!(alias_to_index("bullish", 4), None);
        assert_eq!(alias_to_index("foobar", 3), None);
    }

    #[test]
    fn test_aliases_for_n_regimes() {
        assert_eq!(aliases_for(2), &["bearish", "bullish"]);
        assert_eq!(aliases_for(3), &["bearish", "neutral", "bullish"]);
        assert_eq!(
            aliases_for(4),
            &["strong_bear", "mild_bear", "mild_bull", "strong_bull"]
        );
    }

    #[test]
    fn test_rewrite_with_not_equal_alias() {
        let result = rewrite_formula("hmm_regime(SPY, 3, 5) != bearish", "AAPL").unwrap();
        assert_eq!(result.formula, "__hmm_regime_SPY_3_5_65 != 0");
    }

    #[test]
    fn test_rewrite_explicit_threshold() {
        let result = rewrite_formula("hmm_regime(SPY, 3, 5, 0.8) == bullish", "AAPL").unwrap();
        assert_eq!(result.formula, "__hmm_regime_SPY_3_5_80 == 2");
        assert!((result.calls[0].threshold - 0.8).abs() < 1e-10);
    }
}
