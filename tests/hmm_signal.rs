//! Integration tests for hmm_regime() formula function in backtests.

use optopsy_mcp::signals::hmm_rewrite;

#[test]
fn test_hmm_rewrite_roundtrip() {
    let result = hmm_rewrite::rewrite_formula(
        "hmm_regime(SPY, 3, 5) == bullish and rsi(close, 14) < 30",
        "AAPL",
    )
    .unwrap();

    assert_eq!(
        result.formula,
        "__hmm_regime_SPY_3_5_65 == 2 and rsi(close, 14) < 30"
    );
    assert_eq!(result.calls.len(), 1);
    assert_eq!(result.calls[0].symbol, Some("SPY".to_string()));
    assert_eq!(result.calls[0].n_regimes, 3);
    assert_eq!(result.injected_columns, vec!["__hmm_regime_SPY_3_5_65"]);
}

#[test]
fn test_hmm_forward_filter_no_lookahead() {
    use optopsy_mcp::engine::hmm;

    let mut data = Vec::with_capacity(400);
    let mut seed: u64 = 42;
    for i in 0..400 {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let noise = (seed >> 11) as f64 / (1u64 << 53) as f64 * 0.01 - 0.005;
        let mean = if i < 200 { -0.01 } else { 0.02 };
        data.push(mean + noise);
    }

    let hmm_model = hmm::fit(&data[..200], 2);

    let full_result = hmm::forward_filter(&hmm_model, &data[200..], 0.65);
    let partial_result = hmm::forward_filter(&hmm_model, &data[200..300], 0.65);

    assert_eq!(
        &full_result[..100],
        &partial_result[..],
        "Forward filter results should be identical regardless of future data"
    );
}

#[test]
fn test_cross_symbol_extractor_ignores_hmm_columns() {
    use optopsy_mcp::signals::registry::extract_formula_cross_symbols;

    let syms =
        extract_formula_cross_symbols("__hmm_regime_SPY_3_5_65 == 2 and VIX > 20");
    assert!(!syms.contains("__HMM_REGIME_SPY_3_5_65"));
    assert!(syms.contains("VIX"));
}

#[test]
fn test_parser_handles_hmm_column_name() {
    use optopsy_mcp::signals::custom::parse_formula;

    let expr = parse_formula("__hmm_regime_SPY_3_5_65 == 2").unwrap();
    let fmt = format!("{expr:?}");
    assert!(fmt.contains("__hmm_regime_SPY_3_5_65"));
    assert!(!fmt.contains("_close"));
}

#[test]
fn test_preprocess_no_hmm_passthrough() {
    use optopsy_mcp::signals::preprocess_hmm_regime;
    use polars::prelude::*;

    let dates = Series::new("datetime".into(), &[1_000_000i64, 2_000_000, 3_000_000]);
    let closes = Series::new("close".into(), &[100.0, 101.0, 102.0]);
    let df = DataFrame::new(3, vec![dates.into(), closes.into()]).unwrap();

    let (rewritten, new_df) =
        preprocess_hmm_regime("rsi(close, 14) < 30", "SPY", &df, None, "datetime")
            .unwrap();
    assert_eq!(rewritten, "rsi(close, 14) < 30");
    assert_eq!(new_df.width(), df.width());
}
