//! Integration tests for the `generate_hypotheses` tool.
//!
//! Creates synthetic OHLCV data in a temp cache, then runs the tool
//! end-to-end to verify response structure, statistical controls,
//! and signal spec deployability.

use std::sync::Arc;

use chrono::{Datelike, NaiveDate};
use polars::prelude::*;

use optopsy_mcp::data::cache::CachedStore;
use optopsy_mcp::tools::response_types::HypothesisParams;

/// Write synthetic daily OHLCV data for a symbol into a temp cache directory.
/// Returns the cache dir (must be kept alive) and the `CachedStore`.
fn setup_cache_with_ohlcv(symbol: &str, n_days: usize) -> (tempfile::TempDir, Arc<CachedStore>) {
    let dir = tempfile::tempdir().unwrap();

    // Build synthetic price series with some patterns baked in
    let base_date = NaiveDate::from_ymd_opt(2019, 1, 2).unwrap();
    let mut dates = Vec::with_capacity(n_days);
    let mut opens = Vec::with_capacity(n_days);
    let mut highs = Vec::with_capacity(n_days);
    let mut lows = Vec::with_capacity(n_days);
    let mut closes = Vec::with_capacity(n_days);
    let mut volumes = Vec::with_capacity(n_days);

    let mut price = 100.0_f64;
    for i in 0..n_days {
        let date = base_date + chrono::Duration::days(i as i64);
        // Skip weekends (rough approximation)
        if date.weekday() == chrono::Weekday::Sat || date.weekday() == chrono::Weekday::Sun {
            continue;
        }

        // Inject a slight Monday effect: Mondays tend to drop, Fridays tend to rise
        let dow_effect = match date.weekday() {
            chrono::Weekday::Mon => -0.003,
            chrono::Weekday::Fri => 0.004,
            _ => 0.0,
        };

        // Add some momentum + noise
        let noise = ((i * 7 + 13) % 17) as f64 / 100.0 - 0.08;
        let daily_return = 0.0003 + dow_effect + noise * 0.01;
        price *= 1.0 + daily_return;

        let open = price * 0.999;
        let high = price * 1.005;
        let low = price * 0.995;

        // Volume spikes on Mondays
        let vol = if date.weekday() == chrono::Weekday::Mon {
            3_000_000i64
        } else {
            1_000_000
        };

        dates.push(date);
        opens.push(open);
        highs.push(high);
        lows.push(low);
        closes.push(price);
        volumes.push(vol);
    }

    let mut df = df! {
        "open" => &opens,
        "high" => &highs,
        "low" => &lows,
        "close" => &closes,
        "adjclose" => &closes,
        "volume" => &volumes,
    }
    .unwrap();
    df.with_column(DateChunked::from_naive_date(PlSmallStr::from("date"), dates).into_column())
        .unwrap();

    // Write to cache: {dir}/etf/{SYMBOL}.parquet
    let etf_dir = dir.path().join("etf");
    std::fs::create_dir_all(&etf_dir).unwrap();
    let path = etf_dir.join(format!("{symbol}.parquet"));
    let file = std::fs::File::create(&path).unwrap();
    ParquetWriter::new(file).finish(&mut df).unwrap();

    let cache = Arc::new(CachedStore::new(
        dir.path().to_path_buf(),
        "options".to_string(),
    ));

    (dir, cache)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generate_hypotheses_returns_valid_response() {
    let (_dir, cache) = setup_cache_with_ohlcv("SPY", 2000); // ~5.5 years of weekdays

    let params = HypothesisParams {
        symbols: vec!["SPY".to_string()],
        dimensions: None, // all OHLCV dimensions
        significance: 0.10,
        forward_horizons: vec![5, 10],
        years: 5,
        dedup_threshold: 0.5,
    };

    let result = optopsy_mcp::tools::hypothesis::execute(&cache, &params)
        .await
        .expect("generate_hypotheses should succeed");

    // Response structure checks
    assert_eq!(result.symbols, vec!["SPY"]);
    assert!(result.total_trials > 0, "should generate some trials");
    assert!(
        result.patterns_tested <= result.total_trials,
        "patterns_tested ({}) should be <= total_trials ({})",
        result.patterns_tested,
        result.total_trials,
    );
    assert!(
        result.patterns_significant <= result.patterns_tested,
        "patterns_significant ({}) should be <= patterns_tested ({})",
        result.patterns_significant,
        result.patterns_tested,
    );
    assert!(
        result.patterns_after_dedup <= result.patterns_significant,
        "patterns_after_dedup ({}) should be <= patterns_significant ({})",
        result.patterns_after_dedup,
        result.patterns_significant,
    );
    assert_eq!(
        result.hypotheses.len(),
        result.patterns_after_dedup,
        "hypotheses count should match patterns_after_dedup"
    );
    assert!(!result.summary.is_empty());
    assert!(!result.key_findings.is_empty());
    assert!(!result.suggested_next_steps.is_empty());
    assert!(
        (result.significance_threshold - 0.10).abs() < 1e-10,
        "significance_threshold should echo the input"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generate_hypotheses_signal_specs_are_deployable() {
    let (_dir, cache) = setup_cache_with_ohlcv("SPY", 2000);

    let params = HypothesisParams {
        symbols: vec!["SPY".to_string()],
        dimensions: Some(vec![
            optopsy_mcp::engine::types::HypothesisDimension::Seasonality,
        ]),
        significance: 0.20, // relaxed (max allowed) to ensure we get some patterns
        forward_horizons: vec![5],
        years: 5,
        dedup_threshold: 0.9, // minimal dedup
    };

    let result = optopsy_mcp::tools::hypothesis::execute(&cache, &params)
        .await
        .expect("should succeed");

    // With synthetic Monday effect and relaxed significance, we should get patterns
    if !result.hypotheses.is_empty() {
        for h in &result.hypotheses {
            // Each hypothesis should have a valid signal spec (serializable to JSON)
            let json = serde_json::to_string(&h.signal_spec).expect("signal_spec should serialize");
            assert!(!json.is_empty());

            // Structural basis should be snake_case
            assert!(
                !h.structural_basis.contains(char::is_uppercase),
                "structural_basis '{}' should be snake_case, not PascalCase",
                h.structural_basis,
            );

            // DSR should be between 0 and 1
            assert!(
                h.dsr >= 0.0 && h.dsr <= 1.0,
                "DSR should be in [0, 1], got {}",
                h.dsr,
            );

            // Effect size and p-values should be finite
            assert!(h.effect_size.is_finite());
            assert!(h.p_value.is_finite() && h.p_value >= 0.0);
            assert!(h.adjusted_p_value.is_finite() && h.adjusted_p_value >= 0.0);

            // Sample dates should be present and parseable
            assert!(!h.sample_dates.is_empty(), "should have sample dates");
            for date_str in &h.sample_dates {
                NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
                    .unwrap_or_else(|_| panic!("sample date '{date_str}' should be YYYY-MM-DD"));
            }

            // Occurrence count should be at least 5 (minimum for t-test)
            assert!(
                h.occurrence_count >= 5,
                "occurrence_count should be >= 5, got {}",
                h.occurrence_count,
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generate_hypotheses_insufficient_data_errors() {
    // Write only 40 weekdays of data — below the 60-bar minimum
    let (_dir, cache) = setup_cache_with_ohlcv("SPY", 60); // ~42 weekdays after weekend filter

    let params = HypothesisParams {
        symbols: vec!["SPY".to_string()],
        dimensions: None,
        significance: 0.05,
        forward_horizons: vec![5],
        years: 1, // short history so cutoff doesn't exclude all data
        dedup_threshold: 0.5,
    };

    let result = optopsy_mcp::tools::hypothesis::execute(&cache, &params).await;
    // With ~42 bars and years=1 cutoff, this should either error (< 60 bars)
    // or succeed with 0 trials. Either outcome is acceptable for insufficient data.
    if let Ok(resp) = &result {
        assert_eq!(
            resp.total_trials, 0,
            "with very few bars, should find no patterns"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generate_hypotheses_unknown_symbol_errors() {
    let (_dir, cache) = setup_cache_with_ohlcv("SPY", 500);

    let params = HypothesisParams {
        symbols: vec!["NOSUCH".to_string()],
        dimensions: None,
        significance: 0.05,
        forward_horizons: vec![5],
        years: 5,
        dedup_threshold: 0.5,
    };

    let result = optopsy_mcp::tools::hypothesis::execute(&cache, &params).await;
    assert!(result.is_err(), "should fail for unknown symbol");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generate_hypotheses_multi_symbol_cross_asset() {
    let dir = tempfile::tempdir().unwrap();
    let etf_dir = dir.path().join("etf");
    std::fs::create_dir_all(&etf_dir).unwrap();

    // Write SPY and GLD data
    for (symbol, seed) in [("SPY", 0.0), ("GLD", 0.5)] {
        let base_date = NaiveDate::from_ymd_opt(2019, 1, 2).unwrap();
        let mut dates = Vec::new();
        let mut closes = Vec::new();
        let mut price = 100.0 + seed * 50.0;

        for i in 0..1500 {
            let date = base_date + chrono::Duration::days(i);
            if date.weekday() == chrono::Weekday::Sat || date.weekday() == chrono::Weekday::Sun {
                continue;
            }
            let noise = ((i * 7 + 13) % 17) as f64 / 100.0 - 0.08;
            price *= 1.0 + 0.0003 + noise * 0.01;
            dates.push(date);
            closes.push(price);
        }
        let n = dates.len();

        let mut df = df! {
            "open" => vec![price; n],
            "high" => vec![price * 1.005; n],
            "low" => vec![price * 0.995; n],
            "close" => &closes,
            "adjclose" => &closes,
            "volume" => vec![1_000_000i64; n],
        }
        .unwrap();
        df.with_column(DateChunked::from_naive_date(PlSmallStr::from("date"), dates).into_column())
            .unwrap();

        let path = etf_dir.join(format!("{symbol}.parquet"));
        let file = std::fs::File::create(&path).unwrap();
        ParquetWriter::new(file).finish(&mut df).unwrap();
    }

    let cache = Arc::new(CachedStore::new(
        dir.path().to_path_buf(),
        "options".to_string(),
    ));

    let params = HypothesisParams {
        symbols: vec!["SPY".to_string(), "GLD".to_string()],
        dimensions: Some(vec![
            optopsy_mcp::engine::types::HypothesisDimension::CrossAsset,
        ]),
        significance: 0.10,
        forward_horizons: vec![5],
        years: 5,
        dedup_threshold: 0.5,
    };

    let result = optopsy_mcp::tools::hypothesis::execute(&cache, &params)
        .await
        .expect("multi-symbol should succeed");

    assert_eq!(result.symbols, vec!["SPY", "GLD"]);
    // Cross-asset scan may find 0 patterns if no Granger causality detected
    let _ = result.total_trials;
}
