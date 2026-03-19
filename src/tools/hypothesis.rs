//! Generate trading hypotheses by scanning multiple dimensions for statistically
//! significant patterns. Returns ranked hypotheses with deployable signal specs.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;

use crate::data::cache::CachedStore;
use crate::engine::hypothesis::{generate_hypotheses, HypothesisConfig};
use crate::engine::types::HypothesisDimension;
use crate::tools::ai_format;
use crate::tools::ai_helpers::compute_years_cutoff;
use crate::tools::response_types::{HypothesisParams, HypothesisResponse, PriceBar};

/// Execute the `generate_hypotheses` tool.
pub async fn execute(
    cache: &Arc<CachedStore>,
    params: &HypothesisParams,
) -> Result<HypothesisResponse> {
    let config = HypothesisConfig {
        forward_horizons: params.forward_horizons.clone(),
        significance: params.significance,
        dedup_threshold: params.dedup_threshold,
    };

    // Determine which dimensions to scan
    let dimensions: Vec<HypothesisDimension> = params
        .dimensions
        .clone()
        .unwrap_or_else(|| HypothesisDimension::ohlcv_dimensions().to_vec());

    let cutoff_str = compute_years_cutoff(params.years);

    // Load OHLCV for each symbol
    let mut all_prices: HashMap<String, Vec<PriceBar>> = HashMap::new();
    for sym in &params.symbols {
        let upper = sym.to_uppercase();
        let resp = crate::tools::raw_prices::load_and_execute(
            cache,
            &upper,
            Some(&cutoff_str),
            None,
            None, // no limit — need full history
            crate::engine::types::Interval::Daily,
            None,
        )
        .await
        .with_context(|| format!("Failed to load OHLCV data for {upper}"))?;
        all_prices.insert(upper, resp.prices);
    }

    // For single-symbol mode, scan the first symbol
    let primary_symbol = params.symbols[0].to_uppercase();
    let primary_prices = all_prices
        .get(&primary_symbol)
        .ok_or_else(|| anyhow::anyhow!("No data loaded for {primary_symbol}"))?;

    if primary_prices.len() < 60 {
        anyhow::bail!(
            "Insufficient data for {primary_symbol}: need at least 60 bars, have {}",
            primary_prices.len()
        );
    }

    // Build cross-asset prices (all symbols except primary)
    let cross_asset_prices: HashMap<String, Vec<PriceBar>> = all_prices
        .iter()
        .filter(|(sym, _)| *sym != &primary_symbol)
        .map(|(sym, prices)| (sym.clone(), prices.clone()))
        .collect();

    // Optionally compute regime labels for stability scoring
    let returns: Vec<f64> = primary_prices
        .windows(2)
        .map(|w| {
            if w[0].close > 0.0 {
                w[1].close / w[0].close - 1.0
            } else {
                f64::NAN
            }
        })
        .collect();

    let regime_labels = if returns.len() >= 100 {
        let (labels, _, _) = compute_regime_labels(&returns);
        Some(labels)
    } else {
        None
    };

    // Run the hypothesis engine
    let (total_trials, hypotheses) = generate_hypotheses(
        primary_prices,
        &config,
        &dimensions,
        regime_labels.as_deref(),
        &cross_asset_prices,
    );

    let patterns_significant = hypotheses.len();

    Ok(ai_format::format_hypotheses(
        &params.symbols,
        total_trials,
        params.significance,
        total_trials, // patterns_tested
        patterns_significant,
        &hypotheses,
    ))
}

/// Compute simple volatility-based regime labels for stability scoring.
fn compute_regime_labels(returns: &[f64]) -> (Vec<usize>, Vec<String>, usize) {
    let n_regimes = 2;
    let lookback = 20;
    let annualization = 252.0_f64.sqrt();
    let rolling_vol = crate::stats::rolling::rolling_apply(returns, lookback, |w| {
        crate::stats::std_dev(w) * annualization
    });

    let valid_vols: Vec<f64> = rolling_vol
        .iter()
        .copied()
        .filter(|v| v.is_finite())
        .collect();
    if valid_vols.is_empty() {
        return (
            vec![n_regimes; returns.len()],
            vec!["Low Vol".into(), "High Vol".into()],
            n_regimes,
        );
    }

    let median_vol = crate::stats::median(&valid_vols);

    let labels: Vec<usize> = rolling_vol
        .iter()
        .map(|&v| {
            if v.is_finite() {
                usize::from(v > median_vol)
            } else {
                n_regimes // sentinel
            }
        })
        .collect();

    (labels, vec!["Low Vol".into(), "High Vol".into()], n_regimes)
}
