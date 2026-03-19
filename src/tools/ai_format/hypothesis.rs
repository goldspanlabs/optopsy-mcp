//! Format hypothesis generation results into AI-enriched responses.

use crate::engine::hypothesis::ScoredHypothesis;
use crate::tools::response_types::{DiscoveredPattern, HypothesisResponse};

/// Format hypothesis generation results into an AI-enriched response.
#[allow(clippy::too_many_lines)]
pub fn format_hypotheses(
    symbols: &[String],
    total_trials: usize,
    significance: f64,
    patterns_tested: usize,
    patterns_significant: usize,
    hypotheses: &[ScoredHypothesis],
) -> HypothesisResponse {
    let patterns_after_dedup = hypotheses.len();

    let summary = if hypotheses.is_empty() {
        format!(
            "Scanned {total_trials} patterns ({patterns_tested} tested) across {}. \
             No statistically significant patterns survived BH-FDR correction at α={significance}. \
             This is expected — most random patterns are false discoveries.",
            symbols.join(", "),
        )
    } else {
        let top = &hypotheses[0];
        format!(
            "Scanned {total_trials} patterns ({patterns_tested} tested) across {}. \
             {patterns_significant} survived BH-FDR correction (α={significance}), \
             {patterns_after_dedup} after deduplication. Top: {} (DSR={:.2}, adj. p={:.4}).",
            symbols.join(", "),
            top.pattern.description,
            top.dsr,
            top.adjusted_p_value,
        )
    };

    // Build key findings
    let mut key_findings = Vec::new();
    if hypotheses.is_empty() {
        key_findings.push(
            "No significant patterns found. This is a healthy result — \
             it means the BH-FDR correction is working to filter false discoveries."
                .to_string(),
        );
    } else {
        // Top 3 patterns
        for (i, h) in hypotheses.iter().take(3).enumerate() {
            key_findings.push(format!(
                "#{}: {} — {:.2}% mean forward return over {} days \
                 (DSR={:.2}, adj. p={:.4}, basis: {})",
                i + 1,
                h.pattern.description,
                h.effect_size * 100.0,
                h.pattern.forward_horizon,
                h.dsr,
                h.adjusted_p_value,
                h.pattern.structural_basis.explanation(),
            ));
        }

        // Structural vs empirical breakdown
        let structural_count = hypotheses
            .iter()
            .filter(|h| {
                h.pattern.structural_basis != crate::engine::types::StructuralBasis::EmpiricalOnly
            })
            .count();
        let empirical_count = hypotheses.len() - structural_count;
        if empirical_count > 0 {
            key_findings.push(format!(
                "{structural_count} patterns have known structural drivers, \
                 {empirical_count} are empirical-only (flagged, weighted lower in ranking)."
            ));
        }
    }

    // Suggested next steps
    let mut suggested_next_steps = vec![
        "⚠️ These are HYPOTHESES to investigate, not confirmed strategies. \
         Always validate with walk-forward analysis before deploying."
            .to_string(),
    ];

    if hypotheses.is_empty() {
        suggested_next_steps.push(
            "[RETRY] Try different symbols, longer history (increase `years`), or relaxed \
             significance threshold."
                .to_string(),
        );
        suggested_next_steps.push(
            "[EXPLORE] Use `aggregate_prices` with group_by=day_of_week or month to manually \
             inspect seasonal patterns."
                .to_string(),
        );
    } else {
        suggested_next_steps.push(
            "[BACKTEST] Run the top hypothesis signal spec through `run_stock_backtest` \
             to see full trade-level performance."
                .to_string(),
        );
        suggested_next_steps
            .push("[VALIDATE] Use `walk_forward` to check out-of-sample stability.".to_string());
        suggested_next_steps.push(
            "[COMPARE] Test across multiple symbols to check if the pattern is asset-specific \
             or market-wide."
                .to_string(),
        );
    }

    // Convert scored hypotheses to response format
    let discovered: Vec<DiscoveredPattern> = hypotheses
        .iter()
        .map(|h| {
            let sample_dates: Vec<String> = h
                .pattern
                .signal_dates
                .iter()
                .take(10)
                .map(|d| d.format("%Y-%m-%d").to_string())
                .collect();

            DiscoveredPattern {
                dimension: h.pattern.dimension.to_string(),
                description: h.pattern.description.clone(),
                structural_basis: h.pattern.structural_basis.to_string(),
                structural_explanation: h.pattern.structural_basis.explanation().to_string(),
                signal_spec: h.pattern.signal_spec.clone(),
                forward_horizon: h.pattern.forward_horizon,
                p_value: h.p_value,
                adjusted_p_value: h.adjusted_p_value,
                effect_size: h.effect_size,
                occurrence_count: h.pattern.signal_dates.len(),
                sharpe: h.sharpe,
                dsr: h.dsr,
                regime_stability: h.regime_stability,
                cluster_id: h.cluster_id,
                sample_dates,
            }
        })
        .collect();

    HypothesisResponse {
        summary,
        symbols: symbols.to_vec(),
        total_trials,
        significance_threshold: significance,
        patterns_tested,
        patterns_significant,
        patterns_after_dedup,
        hypotheses: discovered,
        key_findings,
        suggested_next_steps,
    }
}
