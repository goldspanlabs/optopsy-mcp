//! Hypothesis generation engine: scans multiple dimensions for statistically
//! significant trading patterns, applies rigorous statistical controls, and
//! returns ranked hypotheses with deployable signal specs.

use chrono::{Datelike, NaiveDate};
use statrs::distribution::{ContinuousCDF, Normal};
use std::collections::{HashMap, HashSet};

use crate::engine::multiple_comparisons::benjamini_hochberg;
use crate::engine::types::{HypothesisDimension, StructuralBasis};
use crate::signals::registry::SignalSpec;
use crate::stats;
use crate::tools::response_types::PriceBar;

/// Internal representation of a pattern discovered by a scanner before scoring.
#[derive(Debug, Clone)]
pub struct RawPattern {
    pub dimension: HypothesisDimension,
    pub description: String,
    pub structural_basis: StructuralBasis,
    pub signal_spec: SignalSpec,
    pub forward_horizon: usize,
    /// Dates when this pattern's signal fires
    pub signal_dates: Vec<NaiveDate>,
    /// Forward returns on signal dates
    pub signal_returns: Vec<f64>,
}

/// A scored and ranked hypothesis ready for output.
#[derive(Debug, Clone)]
pub struct ScoredHypothesis {
    pub pattern: RawPattern,
    pub p_value: f64,
    pub adjusted_p_value: f64,
    pub effect_size: f64,
    pub sharpe: f64,
    pub dsr: f64,
    pub regime_stability: Option<f64>,
    pub cluster_id: usize,
    pub composite_score: f64,
}

/// Parameters controlling hypothesis generation.
pub struct HypothesisConfig {
    pub forward_horizons: Vec<usize>,
    pub significance: f64,
    pub dedup_threshold: f64,
}

// ── Orchestrator ────────────────────────────────────────────────────────────

/// Main entry point: scan dimensions, filter by significance, compute DSR,
/// deduplicate, score, and rank patterns.
///
/// Returns `(total_trials, patterns_tested, patterns_significant_pre_dedup, scored_hypotheses)`.
#[allow(clippy::implicit_hasher, clippy::too_many_lines)]
pub fn generate_hypotheses(
    prices: &[PriceBar],
    config: &HypothesisConfig,
    dimensions: &[HypothesisDimension],
    regime_labels: Option<&[usize]>,
    cross_asset_prices: &HashMap<String, Vec<PriceBar>>,
) -> (usize, usize, usize, Vec<ScoredHypothesis>) {
    if prices.len() < 60 {
        return (0, 0, 0, vec![]);
    }

    // Run enabled scanners, collecting all raw patterns
    let mut all_patterns: Vec<RawPattern> = Vec::new();
    for &dim in dimensions {
        let patterns = match dim {
            HypothesisDimension::Seasonality => scan_seasonality(prices, &config.forward_horizons),
            HypothesisDimension::PriceAction => scan_price_action(prices, &config.forward_horizons),
            HypothesisDimension::MeanReversion => {
                scan_mean_reversion(prices, &config.forward_horizons)
            }
            HypothesisDimension::Volume => scan_volume(prices, &config.forward_horizons),
            HypothesisDimension::VolatilityRegime => {
                scan_volatility_regime(prices, &config.forward_horizons)
            }
            HypothesisDimension::CrossAsset => scan_cross_asset(
                prices,
                &config.forward_horizons,
                cross_asset_prices,
                config.significance,
            ),
            HypothesisDimension::Microstructure => {
                scan_microstructure(prices, &config.forward_horizons)
            }
            HypothesisDimension::Autocorrelation => {
                scan_autocorrelation(prices, &config.forward_horizons)
            }
            HypothesisDimension::OptionsStructure => {
                // Options scanner requires options DataFrame — skip if not available
                vec![]
            }
        };
        all_patterns.extend(patterns);
    }

    let total_trials = all_patterns.len();
    if total_trials == 0 {
        return (0, 0, 0, vec![]);
    }

    // Run t-test on each pattern's signal returns
    let mut labels = Vec::with_capacity(total_trials);
    let mut p_values = Vec::with_capacity(total_trials);
    let mut valid_patterns: Vec<(usize, &RawPattern)> = Vec::new();

    for (i, pat) in all_patterns.iter().enumerate() {
        if pat.signal_returns.len() < 5 {
            continue;
        }
        if let Some(result) = stats::t_test_one_sample(&pat.signal_returns, 0.0) {
            labels.push(format!("{i}"));
            p_values.push(result.p_value);
            valid_patterns.push((i, pat));
        }
    }

    let patterns_tested = valid_patterns.len();

    if valid_patterns.is_empty() {
        return (total_trials, patterns_tested, 0, vec![]);
    }

    // Apply BH-FDR correction across ALL tested patterns
    let bh_result = benjamini_hochberg(&labels, &p_values, config.significance);

    // Filter to significant patterns and compute scores
    let mut scored: Vec<ScoredHypothesis> = Vec::new();
    for (bh_idx, corrected) in bh_result.results.iter().enumerate() {
        if !corrected.is_significant {
            continue;
        }
        let (_orig_idx, pat) = &valid_patterns[bh_idx];
        let returns = &pat.signal_returns;
        let n_obs = returns.len();

        let m = stats::mean(returns);
        let sd = stats::std_dev(returns);
        let sharpe = if sd > 0.0 {
            m / sd * (252.0_f64).sqrt()
        } else {
            0.0
        };
        let skew = stats::skewness(returns);
        let kurt = stats::kurtosis(returns);
        let dsr = compute_dsr(sharpe, patterns_tested, skew, kurt, n_obs);

        let regime_stab =
            regime_labels.map(|rl| score_regime_stability(returns, &pat.signal_dates, prices, rl));

        let structural_weight = pat.structural_basis.weight();
        let stability_factor = regime_stab.map_or(1.0, |cv| 1.0 / (1.0 + cv));
        let composite = structural_weight * dsr.max(0.0) * stability_factor;

        scored.push(ScoredHypothesis {
            pattern: (*pat).clone(),
            p_value: corrected.original_p_value,
            adjusted_p_value: corrected.adjusted_p_value,
            effect_size: m,
            sharpe,
            dsr,
            regime_stability: regime_stab,
            cluster_id: 0,
            composite_score: composite,
        });
    }

    let patterns_significant = scored.len();

    // Deduplicate by signal date overlap
    scored = deduplicate_patterns(scored, config.dedup_threshold, prices);

    // Sort by composite score descending
    scored.sort_by(|a, b| {
        b.composite_score
            .partial_cmp(&a.composite_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    (total_trials, patterns_tested, patterns_significant, scored)
}

// ── Forward returns ─────────────────────────────────────────────────────────

/// Get forward returns for specific signal dates, returning both the surviving
/// indices and their corresponding returns (aligned 1:1).
///
/// When signal dates are sparse and median gap > horizon, use all signal dates
/// (they're already non-overlapping). Otherwise use purged observations.
fn get_signal_returns(
    prices: &[PriceBar],
    signal_indices: &[usize],
    horizon: usize,
) -> (Vec<usize>, Vec<f64>) {
    if signal_indices.is_empty() || horizon == 0 {
        return (vec![], vec![]);
    }

    let closes: Vec<f64> = prices.iter().map(|p| p.close).collect();
    let n = closes.len();

    // Check if signal dates are naturally non-overlapping
    let skip_purge = if signal_indices.len() >= 2 {
        let mut gaps: Vec<usize> = signal_indices.windows(2).map(|w| w[1] - w[0]).collect();
        gaps.sort_unstable();
        let median_gap = gaps[gaps.len() / 2];
        median_gap >= horizon
    } else {
        true
    };

    let mut used_indices = Vec::new();
    let mut returns = Vec::new();
    if skip_purge {
        for &idx in signal_indices {
            if idx + horizon < n && closes[idx] > 0.0 {
                let ret = closes[idx + horizon] / closes[idx] - 1.0;
                if ret.is_finite() {
                    used_indices.push(idx);
                    returns.push(ret);
                }
            }
        }
    } else {
        // Purge: keep only every h-th signal to avoid overlap
        let mut last_used: Option<usize> = None;
        for &idx in signal_indices {
            if let Some(last) = last_used {
                if idx < last + horizon {
                    continue;
                }
            }
            if idx + horizon < n && closes[idx] > 0.0 {
                let ret = closes[idx + horizon] / closes[idx] - 1.0;
                if ret.is_finite() {
                    used_indices.push(idx);
                    returns.push(ret);
                    last_used = Some(idx);
                }
            }
        }
    }
    (used_indices, returns)
}

// ── Statistical functions ───────────────────────────────────────────────────

/// Deflated Sharpe Ratio (Bailey & Lopez de Prado, 2014).
///
/// Adjusts the observed Sharpe for the number of trials, skewness, and kurtosis.
/// Returns a probability that the observed SR exceeds the expected maximum SR
/// under the null hypothesis of i.i.d. returns across `n_trials` strategies.
pub fn compute_dsr(sharpe: f64, n_trials: usize, skew: f64, kurt: f64, n_obs: usize) -> f64 {
    if n_trials == 0 || n_obs < 3 {
        return 0.0;
    }

    let n = n_obs as f64;
    let nt = n_trials as f64;

    // Expected maximum Sharpe under null (Euler-Mascheroni approximation)
    // E[max(SR)] ≈ (1 - γ) * Φ⁻¹(1 - 1/N) + γ * Φ⁻¹(1 - 1/(N*e))
    // Simplified: E[max(SR)] ≈ sqrt(2 * ln(N)) - (ln(π) + ln(ln(N))) / (2 * sqrt(2 * ln(N)))
    let expected_max_sr = if nt <= 1.0 {
        0.0
    } else {
        let ln_n = nt.ln();
        let a = (2.0 * ln_n).sqrt();
        let b = (std::f64::consts::PI.ln() + ln_n.ln()) / (2.0 * a);
        a - b
    };

    // Standard error of Sharpe accounting for non-normality (Lo 2002)
    // var(SR) = [1 + (skew/2)*SR + ((raw_kurt-1)/4)*SR²] / (n-1)
    // stats::kurtosis() returns excess kurtosis (normal=0), so raw_kurt = kurt + 3,
    // and (raw_kurt - 1)/4 = (kurt + 2)/4.
    let sr2 = sharpe * sharpe;
    let var_sr = (1.0 + 0.5 * skew * sharpe + ((kurt + 2.0) / 4.0) * sr2) / (n - 1.0);
    let se_sr = if var_sr > 0.0 {
        var_sr.sqrt()
    } else {
        return 0.0;
    };

    // DSR = Φ((SR - E[max(SR)]) / se(SR))
    let z = (sharpe - expected_max_sr) / se_sr;
    let normal = Normal::new(0.0, 1.0).unwrap();
    normal.cdf(z)
}

/// Deduplicate patterns by signal date overlap using Jaccard similarity.
///
/// Patterns with Jaccard > threshold are grouped into clusters via connected
/// components. The highest-DSR pattern per cluster survives.
fn deduplicate_patterns(
    mut patterns: Vec<ScoredHypothesis>,
    threshold: f64,
    _prices: &[PriceBar],
) -> Vec<ScoredHypothesis> {
    let n = patterns.len();
    if n <= 1 {
        if let Some(p) = patterns.first_mut() {
            p.cluster_id = 0;
        }
        return patterns;
    }

    // Build boolean date masks as HashSets for fast intersection
    let date_sets: Vec<HashSet<NaiveDate>> = patterns
        .iter()
        .map(|p| p.pattern.signal_dates.iter().copied().collect())
        .collect();

    // Build adjacency list (connected components via Jaccard > threshold)
    let mut adj: Vec<Vec<usize>> = vec![vec![]; n];
    for i in 0..n {
        for j in (i + 1)..n {
            let intersection = date_sets[i].intersection(&date_sets[j]).count();
            let union = date_sets[i].union(&date_sets[j]).count();
            if union > 0 {
                let jaccard = intersection as f64 / union as f64;
                if jaccard > threshold {
                    adj[i].push(j);
                    adj[j].push(i);
                }
            }
        }
    }

    // Find connected components via BFS
    let mut component = vec![usize::MAX; n];
    let mut cluster_id = 0;
    for start in 0..n {
        if component[start] != usize::MAX {
            continue;
        }
        let mut queue = vec![start];
        component[start] = cluster_id;
        while let Some(node) = queue.pop() {
            for &neighbor in &adj[node] {
                if component[neighbor] == usize::MAX {
                    component[neighbor] = cluster_id;
                    queue.push(neighbor);
                }
            }
        }
        cluster_id += 1;
    }

    // Assign cluster IDs
    for (i, p) in patterns.iter_mut().enumerate() {
        p.cluster_id = component[i];
    }

    // Keep highest-DSR pattern per cluster
    let mut best_per_cluster: HashMap<usize, usize> = HashMap::new();
    for (i, p) in patterns.iter().enumerate() {
        let entry = best_per_cluster.entry(p.cluster_id).or_insert(i);
        if p.dsr > patterns[*entry].dsr {
            *entry = i;
        }
    }

    let keep: HashSet<usize> = best_per_cluster.values().copied().collect();
    patterns
        .into_iter()
        .enumerate()
        .filter(|(i, _)| keep.contains(i))
        .map(|(_, p)| p)
        .collect()
}

/// Score regime stability: coefficient of variation of Sharpe across regimes.
/// Lower CV = more stable across regimes = better.
fn score_regime_stability(
    returns: &[f64],
    signal_dates: &[NaiveDate],
    prices: &[PriceBar],
    regime_labels: &[usize],
) -> f64 {
    // Map signal dates to regime labels
    let date_to_idx: HashMap<NaiveDate, usize> = prices
        .iter()
        .enumerate()
        .filter_map(|(i, p)| epoch_to_naive_date(p.date).map(|d| (d, i)))
        .collect();

    let mut regime_returns: HashMap<usize, Vec<f64>> = HashMap::new();
    for (ret, date) in returns.iter().zip(signal_dates) {
        if let Some(&idx) = date_to_idx.get(date) {
            if idx < regime_labels.len() {
                regime_returns
                    .entry(regime_labels[idx])
                    .or_default()
                    .push(*ret);
            }
        }
    }

    // Compute Sharpe per regime
    let sharpes: Vec<f64> = regime_returns
        .values()
        .filter(|rets| rets.len() >= 3)
        .map(|rets| {
            let m = stats::mean(rets);
            let sd = stats::std_dev(rets);
            if sd > 0.0 {
                m / sd
            } else {
                0.0
            }
        })
        .collect();

    if sharpes.len() < 2 {
        return 0.0;
    }

    // Coefficient of variation
    let mean_sharpe = stats::mean(&sharpes);
    let std_sharpe = stats::std_dev(&sharpes);
    if mean_sharpe.abs() < 1e-10 {
        return std_sharpe.abs() * 10.0; // penalize inconsistent near-zero sharpe
    }
    (std_sharpe / mean_sharpe.abs()).abs()
}

/// Map a dimension + pattern type to a known market mechanism.
fn tag_structural_basis(dimension: HypothesisDimension, pattern_type: &str) -> StructuralBasis {
    match dimension {
        HypothesisDimension::Seasonality => {
            if pattern_type.contains("month") || pattern_type.contains("quarter") {
                StructuralBasis::RebalancingFlows
            } else if pattern_type.contains("expiration") || pattern_type.contains("settlement") {
                StructuralBasis::SettlementMechanics
            } else {
                StructuralBasis::RebalancingFlows
            }
        }
        HypothesisDimension::PriceAction => StructuralBasis::MomentumBehavioral,
        HypothesisDimension::MeanReversion => StructuralBasis::MeanReversionStatArb,
        HypothesisDimension::Volume => StructuralBasis::LiquidityPremium,
        HypothesisDimension::VolatilityRegime => StructuralBasis::VarianceRiskPremium,
        HypothesisDimension::CrossAsset => StructuralBasis::MacroTransmission,
        HypothesisDimension::Microstructure => StructuralBasis::OvernightRiskPremium,
        HypothesisDimension::Autocorrelation => StructuralBasis::EmpiricalOnly,
        HypothesisDimension::OptionsStructure => StructuralBasis::HedgingDemand,
    }
}

// ── Helper ──────────────────────────────────────────────────────────────────

fn epoch_to_naive_date(epoch: i64) -> Option<NaiveDate> {
    chrono::DateTime::from_timestamp(epoch, 0).map(|dt| dt.date_naive())
}

/// Convert `PriceBar` slice to (dates, closes, returns) vectors.
fn prices_to_returns(prices: &[PriceBar]) -> (Vec<NaiveDate>, Vec<f64>, Vec<f64>) {
    let dates: Vec<NaiveDate> = prices
        .iter()
        .filter_map(|p| epoch_to_naive_date(p.date))
        .collect();
    let closes: Vec<f64> = prices.iter().map(|p| p.close).collect();
    let returns: Vec<f64> = closes
        .windows(2)
        .map(|w| {
            if w[0] > 0.0 {
                w[1] / w[0] - 1.0
            } else {
                f64::NAN
            }
        })
        .collect();
    (dates, closes, returns)
}

/// Find indices in prices where a boolean condition is true, and get forward returns.
fn scan_condition(
    prices: &[PriceBar],
    condition: impl Fn(usize, &[PriceBar]) -> bool,
    horizon: usize,
    description: &str,
    dimension: HypothesisDimension,
    pattern_type: &str,
    signal_spec: SignalSpec,
) -> Option<RawPattern> {
    let mut signal_indices: Vec<usize> = Vec::new();
    for i in 0..prices.len() {
        if condition(i, prices) {
            signal_indices.push(i);
        }
    }

    if signal_indices.len() < 5 {
        return None;
    }

    let (used_indices, signal_returns) = get_signal_returns(prices, &signal_indices, horizon);
    if signal_returns.len() < 5 {
        return None;
    }

    // Build dates from the indices that survived purging (aligned with signal_returns)
    let signal_dates: Vec<NaiveDate> = used_indices
        .iter()
        .filter_map(|&i| epoch_to_naive_date(prices[i].date))
        .collect();

    Some(RawPattern {
        dimension,
        description: description.to_string(),
        structural_basis: tag_structural_basis(dimension, pattern_type),
        signal_spec,
        forward_horizon: horizon,
        signal_dates,
        signal_returns,
    })
}

// ── Scanners ────────────────────────────────────────────────────────────────

/// Seasonality: day-of-week, month, turn-of-month, quarter-end effects.
fn scan_seasonality(prices: &[PriceBar], horizons: &[usize]) -> Vec<RawPattern> {
    let mut patterns = Vec::new();

    for &h in horizons {
        // Day of week effects (1=Mon..7=Sun)
        for dow in 1..=5u32 {
            let dow_name = match dow {
                1 => "Monday",
                2 => "Tuesday",
                3 => "Wednesday",
                4 => "Thursday",
                5 => "Friday",
                _ => continue,
            };
            if let Some(pat) = scan_condition(
                prices,
                |i, p| {
                    epoch_to_naive_date(p[i].date)
                        .is_some_and(|d| d.weekday().number_from_monday() == dow)
                },
                h,
                &format!("{dow_name} {h}-day forward return"),
                HypothesisDimension::Seasonality,
                "day_of_week",
                SignalSpec::Formula {
                    formula: format!("day_of_week() == {dow}"),
                },
            ) {
                patterns.push(pat);
            }
        }

        // Month effects
        for month in 1..=12u32 {
            let month_name = match month {
                1 => "January",
                2 => "February",
                3 => "March",
                4 => "April",
                5 => "May",
                6 => "June",
                7 => "July",
                8 => "August",
                9 => "September",
                10 => "October",
                11 => "November",
                12 => "December",
                _ => unreachable!(),
            };
            if let Some(pat) = scan_condition(
                prices,
                |i, p| epoch_to_naive_date(p[i].date).is_some_and(|d| d.month() == month),
                h,
                &format!("{month_name} {h}-day forward return"),
                HypothesisDimension::Seasonality,
                "month",
                SignalSpec::Formula {
                    formula: format!("month() == {month}"),
                },
            ) {
                patterns.push(pat);
            }
        }

        // Turn of month (calendar day >= 28 or <= 3)
        if let Some(pat) = scan_condition(
            prices,
            |i, p| epoch_to_naive_date(p[i].date).is_some_and(|d| d.day() >= 28 || d.day() <= 3),
            h,
            &format!("Turn-of-month (day >= 28 or <= 3) {h}-day forward return"),
            HypothesisDimension::Seasonality,
            "month",
            SignalSpec::Formula {
                formula: "day_of_month() >= 28 or day_of_month() <= 3".to_string(),
            },
        ) {
            patterns.push(pat);
        }
    }

    patterns
}

/// Price action: momentum, consecutive moves, large moves.
#[allow(clippy::too_many_lines)]
fn scan_price_action(prices: &[PriceBar], horizons: &[usize]) -> Vec<RawPattern> {
    let mut patterns = Vec::new();
    let closes: Vec<f64> = prices.iter().map(|p| p.close).collect();

    for &h in horizons {
        // 3 consecutive down days
        if let Some(pat) = scan_condition(
            prices,
            |i, p| {
                i >= 3
                    && p[i].close < p[i - 1].close
                    && p[i - 1].close < p[i - 2].close
                    && p[i - 2].close < p[i - 3].close
            },
            h,
            &format!("3 consecutive down days → {h}-day forward return"),
            HypothesisDimension::PriceAction,
            "consecutive_down",
            SignalSpec::Formula {
                formula: "change(close, 1) < 0 and close[1] < close[2] and close[2] < close[3]"
                    .to_string(),
            },
        ) {
            patterns.push(pat);
        }

        // 3 consecutive up days
        if let Some(pat) = scan_condition(
            prices,
            |i, p| {
                i >= 3
                    && p[i].close > p[i - 1].close
                    && p[i - 1].close > p[i - 2].close
                    && p[i - 2].close > p[i - 3].close
            },
            h,
            &format!("3 consecutive up days → {h}-day forward return"),
            HypothesisDimension::PriceAction,
            "consecutive_up",
            SignalSpec::Formula {
                formula: "change(close, 1) > 0 and close[1] > close[2] and close[2] > close[3]"
                    .to_string(),
            },
        ) {
            patterns.push(pat);
        }

        // 20-day momentum positive (price up >5%)
        if closes.len() > 20 {
            if let Some(pat) = scan_condition(
                prices,
                |i, p| {
                    i >= 20 && p[i - 20].close > 0.0 && (p[i].close / p[i - 20].close - 1.0) > 0.05
                },
                h,
                &format!("20-day momentum > 5% → {h}-day forward return"),
                HypothesisDimension::PriceAction,
                "momentum",
                SignalSpec::Formula {
                    formula: "pct_change(close, 20) > 0.05".to_string(),
                },
            ) {
                patterns.push(pat);
            }
        }

        // 20-day momentum negative (price down >5%)
        if closes.len() > 20 {
            if let Some(pat) = scan_condition(
                prices,
                |i, p| {
                    i >= 20 && p[i - 20].close > 0.0 && (p[i].close / p[i - 20].close - 1.0) < -0.05
                },
                h,
                &format!("20-day momentum < -5% → {h}-day forward return"),
                HypothesisDimension::PriceAction,
                "momentum",
                SignalSpec::Formula {
                    formula: "pct_change(close, 20) < -0.05".to_string(),
                },
            ) {
                patterns.push(pat);
            }
        }

        // Large single-day drop (> 2%)
        if let Some(pat) = scan_condition(
            prices,
            |i, p| i >= 1 && p[i - 1].close > 0.0 && (p[i].close / p[i - 1].close - 1.0) < -0.02,
            h,
            &format!("Single-day drop > 2% → {h}-day forward return"),
            HypothesisDimension::PriceAction,
            "large_move",
            SignalSpec::Formula {
                formula: "pct_change(close, 1) < -0.02".to_string(),
            },
        ) {
            patterns.push(pat);
        }

        // Large single-day rally (> 2%)
        if let Some(pat) = scan_condition(
            prices,
            |i, p| i >= 1 && p[i - 1].close > 0.0 && (p[i].close / p[i - 1].close - 1.0) > 0.02,
            h,
            &format!("Single-day rally > 2% → {h}-day forward return"),
            HypothesisDimension::PriceAction,
            "large_move",
            SignalSpec::Formula {
                formula: "pct_change(close, 1) > 0.02".to_string(),
            },
        ) {
            patterns.push(pat);
        }
    }

    patterns
}

/// Mean reversion: Bollinger band touches, RSI extremes, z-score extremes.
fn scan_mean_reversion(prices: &[PriceBar], horizons: &[usize]) -> Vec<RawPattern> {
    let mut patterns = Vec::new();
    let closes: Vec<f64> = prices.iter().map(|p| p.close).collect();
    let n = closes.len();

    if n < 20 {
        return patterns;
    }

    // Precompute 20-day rolling mean and std
    let sma20 = stats::rolling::rolling_apply(&closes, 20, stats::mean);
    let std20 = stats::rolling::rolling_apply(&closes, 20, stats::std_dev);

    for &h in horizons {
        // Below lower Bollinger Band (close < SMA20 - 2*std20)
        if let Some(pat) = scan_condition(
            prices,
            |i, _p| {
                i < sma20.len()
                    && i < std20.len()
                    && sma20[i].is_finite()
                    && std20[i].is_finite()
                    && std20[i] > 0.0
                    && closes[i] < sma20[i] - 2.0 * std20[i]
            },
            h,
            &format!("Below lower Bollinger Band (2σ) → {h}-day forward return"),
            HypothesisDimension::MeanReversion,
            "bollinger_lower",
            SignalSpec::Formula {
                formula: "close < sma(close, 20) - 2 * std(close, 20)".to_string(),
            },
        ) {
            patterns.push(pat);
        }

        // Above upper Bollinger Band (close > SMA20 + 2*std20)
        if let Some(pat) = scan_condition(
            prices,
            |i, _p| {
                i < sma20.len()
                    && i < std20.len()
                    && sma20[i].is_finite()
                    && std20[i].is_finite()
                    && std20[i] > 0.0
                    && closes[i] > sma20[i] + 2.0 * std20[i]
            },
            h,
            &format!("Above upper Bollinger Band (2σ) → {h}-day forward return"),
            HypothesisDimension::MeanReversion,
            "bollinger_upper",
            SignalSpec::Formula {
                formula: "close > sma(close, 20) + 2 * std(close, 20)".to_string(),
            },
        ) {
            patterns.push(pat);
        }

        // Z-score < -2 (extreme below mean)
        if let Some(pat) = scan_condition(
            prices,
            |i, _p| {
                i < sma20.len()
                    && i < std20.len()
                    && sma20[i].is_finite()
                    && std20[i].is_finite()
                    && std20[i] > 0.0
                    && (closes[i] - sma20[i]) / std20[i] < -2.0
            },
            h,
            &format!("Z-score < -2 (20-day) → {h}-day forward return"),
            HypothesisDimension::MeanReversion,
            "zscore",
            SignalSpec::Formula {
                formula: "zscore(close, 20) < -2".to_string(),
            },
        ) {
            patterns.push(pat);
        }
    }

    patterns
}

/// Volume: volume spikes, low volume, volume trends.
fn scan_volume(prices: &[PriceBar], horizons: &[usize]) -> Vec<RawPattern> {
    let mut patterns = Vec::new();
    let volumes: Vec<f64> = prices.iter().map(|p| p.volume as f64).collect();
    let n = volumes.len();

    if n < 20 {
        return patterns;
    }

    let sma_vol = stats::rolling::rolling_apply(&volumes, 20, stats::mean);

    for &h in horizons {
        // Volume spike: volume > 2x 20-day average
        if let Some(pat) = scan_condition(
            prices,
            |i, p| {
                i < sma_vol.len()
                    && sma_vol[i].is_finite()
                    && sma_vol[i] > 0.0
                    && (p[i].volume as f64) > 2.0 * sma_vol[i]
            },
            h,
            &format!("Volume spike > 2x avg → {h}-day forward return"),
            HypothesisDimension::Volume,
            "volume_spike",
            SignalSpec::Formula {
                formula: "volume > 2 * sma(volume, 20)".to_string(),
            },
        ) {
            patterns.push(pat);
        }

        // Low volume: volume < 0.5x 20-day average
        if let Some(pat) = scan_condition(
            prices,
            |i, p| {
                i < sma_vol.len()
                    && sma_vol[i].is_finite()
                    && sma_vol[i] > 0.0
                    && (p[i].volume as f64) < 0.5 * sma_vol[i]
            },
            h,
            &format!("Low volume < 0.5x avg → {h}-day forward return"),
            HypothesisDimension::Volume,
            "low_volume",
            SignalSpec::Formula {
                formula: "volume < 0.5 * sma(volume, 20)".to_string(),
            },
        ) {
            patterns.push(pat);
        }
    }

    patterns
}

/// Volatility regime: high/low vol environments.
fn scan_volatility_regime(prices: &[PriceBar], horizons: &[usize]) -> Vec<RawPattern> {
    let mut patterns = Vec::new();
    let (_, _, returns) = prices_to_returns(prices);

    if returns.len() < 40 {
        return patterns;
    }

    // Compute rolling 20-day realized vol
    let rolling_vol = stats::rolling::rolling_apply(&returns, 20, stats::std_dev);
    let valid_vols: Vec<f64> = rolling_vol
        .iter()
        .copied()
        .filter(|v| v.is_finite())
        .collect();

    if valid_vols.len() < 20 {
        return patterns;
    }

    let vol_p25 = stats::percentile(&valid_vols, 25.0);
    let vol_p75 = stats::percentile(&valid_vols, 75.0);

    for &h in horizons {
        // Low vol regime
        if let Some(pat) = scan_condition(
            prices,
            |i, _p| {
                // i in returns is offset by 1 from prices
                i > 0
                    && i - 1 < rolling_vol.len()
                    && rolling_vol[i - 1].is_finite()
                    && rolling_vol[i - 1] < vol_p25
            },
            h,
            &format!("Low volatility regime (< p25) → {h}-day forward return"),
            HypothesisDimension::VolatilityRegime,
            "low_vol",
            SignalSpec::Formula {
                formula:
                    "std(pct_change(close, 1), 20) < sma(std(pct_change(close, 1), 20), 252) * 0.75"
                        .to_string(),
            },
        ) {
            patterns.push(pat);
        }

        // High vol regime
        if let Some(pat) = scan_condition(
            prices,
            |i, _p| {
                i > 0
                    && i - 1 < rolling_vol.len()
                    && rolling_vol[i - 1].is_finite()
                    && rolling_vol[i - 1] > vol_p75
            },
            h,
            &format!("High volatility regime (> p75) → {h}-day forward return"),
            HypothesisDimension::VolatilityRegime,
            "high_vol",
            SignalSpec::Formula {
                formula:
                    "std(pct_change(close, 1), 20) > sma(std(pct_change(close, 1), 20), 252) * 1.25"
                        .to_string(),
            },
        ) {
            patterns.push(pat);
        }
    }

    patterns
}

/// Cross-asset: lead/lag relationships with other symbols.
fn scan_cross_asset(
    prices: &[PriceBar],
    horizons: &[usize],
    cross_asset_prices: &HashMap<String, Vec<PriceBar>>,
    significance: f64,
) -> Vec<RawPattern> {
    let mut patterns = Vec::new();

    let (target_dates, _, target_returns) = prices_to_returns(prices);
    if target_returns.len() < 60 {
        return patterns;
    }

    for (symbol, other_prices) in cross_asset_prices {
        let (other_dates, _, other_returns) = prices_to_returns(other_prices);
        if other_returns.len() < 60 {
            continue;
        }

        // Align by date
        let other_date_map: HashMap<NaiveDate, usize> = other_dates
            .iter()
            .enumerate()
            .map(|(i, d)| (*d, i))
            .collect();

        let mut aligned_target = Vec::new();
        let mut aligned_other = Vec::new();
        for (i, date) in target_dates.iter().enumerate() {
            if i < target_returns.len() {
                if let Some(&j) = other_date_map.get(date) {
                    if j < other_returns.len()
                        && target_returns[i].is_finite()
                        && other_returns[j].is_finite()
                    {
                        aligned_target.push(target_returns[i]);
                        aligned_other.push(other_returns[j]);
                    }
                }
            }
        }

        if aligned_target.len() < 60 {
            continue;
        }

        // Test Granger causality: does cross-asset lead target?
        if let Some((f_stat, p_val)) = stats::granger_f_test(&aligned_other, &aligned_target, 5) {
            if p_val < significance {
                for &h in horizons {
                    // Cross-asset spike leads target
                    if let Some(pat) = scan_condition(
                        prices,
                        |i, _p| {
                            if i == 0 { return false; }
                            let date = epoch_to_naive_date(prices[i].date);
                            if let Some(d) = date {
                                if let Some(&j) = other_date_map.get(&d) {
                                    if j < other_returns.len() {
                                        return other_returns[j] > 0.02 || other_returns[j] < -0.02;
                                    }
                                }
                            }
                            false
                        },
                        h,
                        &format!("{symbol} large move (>2%) leads target → {h}-day forward return (Granger F={f_stat:.1}, p={p_val:.4})"),
                        HypothesisDimension::CrossAsset,
                        "cross_asset",
                        SignalSpec::CrossSymbol {
                            symbol: symbol.clone(),
                            signal: Box::new(SignalSpec::Formula {
                                formula: "abs(pct_change(close, 1)) > 0.02".to_string(),
                            }),
                        },
                    ) {
                        patterns.push(pat);
                    }
                }
            }
        }
    }

    patterns
}

/// Microstructure: overnight gaps.
fn scan_microstructure(prices: &[PriceBar], horizons: &[usize]) -> Vec<RawPattern> {
    let mut patterns = Vec::new();

    for &h in horizons {
        // Gap down > 1%
        if let Some(pat) = scan_condition(
            prices,
            |i, p| i >= 1 && p[i - 1].close > 0.0 && (p[i].open / p[i - 1].close - 1.0) < -0.01,
            h,
            &format!("Gap down > 1% → {h}-day forward return"),
            HypothesisDimension::Microstructure,
            "gap_down",
            SignalSpec::Formula {
                formula: "gap() < -0.01".to_string(),
            },
        ) {
            patterns.push(pat);
        }

        // Gap up > 1%
        if let Some(pat) = scan_condition(
            prices,
            |i, p| i >= 1 && p[i - 1].close > 0.0 && (p[i].open / p[i - 1].close - 1.0) > 0.01,
            h,
            &format!("Gap up > 1% → {h}-day forward return"),
            HypothesisDimension::Microstructure,
            "gap_up",
            SignalSpec::Formula {
                formula: "gap() > 0.01".to_string(),
            },
        ) {
            patterns.push(pat);
        }

        // Large intraday range (> 2x average range)
        if prices.len() >= 20 {
            let ranges: Vec<f64> = prices
                .iter()
                .map(|p| {
                    if p.low > 0.0 {
                        (p.high - p.low) / p.low
                    } else {
                        0.0
                    }
                })
                .collect();
            let avg_range = stats::rolling::rolling_apply(&ranges, 20, stats::mean);

            if let Some(pat) = scan_condition(
                prices,
                |i, p| {
                    if i >= avg_range.len() || !avg_range[i].is_finite() || avg_range[i] <= 0.0 {
                        return false;
                    }
                    let range = if p[i].low > 0.0 {
                        (p[i].high - p[i].low) / p[i].low
                    } else {
                        0.0
                    };
                    range > 2.0 * avg_range[i]
                },
                h,
                &format!("Large intraday range > 2x avg → {h}-day forward return"),
                HypothesisDimension::Microstructure,
                "large_range",
                SignalSpec::Formula {
                    formula: "(high - low) / low > 2 * sma((high - low) / low, 20)".to_string(),
                },
            ) {
                patterns.push(pat);
            }
        }
    }

    patterns
}

/// Autocorrelation: serial correlation patterns.
fn scan_autocorrelation(prices: &[PriceBar], horizons: &[usize]) -> Vec<RawPattern> {
    let mut patterns = Vec::new();
    let (_, _, returns) = prices_to_returns(prices);

    if returns.len() < 60 {
        return patterns;
    }

    // Check lag-1 autocorrelation
    let acf_1 = stats::lagged_pearson(&returns, &returns, 1);

    for &h in horizons {
        // Positive autocorrelation (trending): buy after positive return
        if acf_1 > 0.05 {
            if let Some(pat) = scan_condition(
                prices,
                |i, p| i >= 1 && p[i - 1].close > 0.0 && (p[i].close / p[i - 1].close - 1.0) > 0.0,
                h,
                &format!(
                    "Positive return (ACF1={acf_1:.3}) → {h}-day forward return (momentum regime)"
                ),
                HypothesisDimension::Autocorrelation,
                "positive_acf",
                SignalSpec::Formula {
                    formula: "pct_change(close, 1) > 0".to_string(),
                },
            ) {
                patterns.push(pat);
            }
        }

        // Negative autocorrelation (mean-reverting): buy after negative return
        if acf_1 < -0.05 {
            if let Some(pat) = scan_condition(
                prices,
                |i, p| {
                    i >= 1
                        && p[i - 1].close > 0.0
                        && (p[i].close / p[i - 1].close - 1.0) < 0.0
                },
                h,
                &format!("Negative return (ACF1={acf_1:.3}) → {h}-day forward return (mean-reversion regime)"),
                HypothesisDimension::Autocorrelation,
                "negative_acf",
                SignalSpec::Formula {
                    formula: "pct_change(close, 1) < 0".to_string(),
                },
            ) {
                patterns.push(pat);
            }
        }

        // 5-day autocorrelation (weekly effect)
        if returns.len() >= 60 {
            let acf_5 = stats::lagged_pearson(&returns, &returns, 5);
            if acf_5.abs() > 0.05 {
                let direction = if acf_5 > 0.0 { "positive" } else { "negative" };
                if let Some(pat) = scan_condition(
                    prices,
                    |i, p| {
                        i >= 5
                            && p[i - 5].close > 0.0
                            && if acf_5 > 0.0 {
                                (p[i].close / p[i - 5].close - 1.0) > 0.0
                            } else {
                                (p[i].close / p[i - 5].close - 1.0) < 0.0
                            }
                    },
                    h,
                    &format!("5-day {direction} return (ACF5={acf_5:.3}) → {h}-day forward return"),
                    HypothesisDimension::Autocorrelation,
                    "weekly_acf",
                    SignalSpec::Formula {
                        formula: if acf_5 > 0.0 {
                            "pct_change(close, 5) > 0".to_string()
                        } else {
                            "pct_change(close, 5) < 0".to_string()
                        },
                    },
                ) {
                    patterns.push(pat);
                }
            }
        }
    }

    patterns
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn synthetic_prices(n: usize) -> Vec<PriceBar> {
        let base_epoch: i64 = 1_577_836_800; // 2020-01-01T00:00:00Z
        let mut prices = Vec::with_capacity(n);
        let mut close = 100.0_f64;
        for i in 0..n {
            let delta = if i % 5 == 0 {
                1.5
            } else if i % 3 == 0 {
                -0.8
            } else {
                0.3
            };
            close = (close + delta).max(50.0);
            prices.push(PriceBar {
                date: base_epoch + (i as i64) * 86400,
                open: close - 0.1,
                high: close + 0.5,
                low: close - 0.5,
                close,
                adjclose: Some(close),
                volume: 1_000_000 + (i as u64 % 7) * 200_000,
            });
        }
        prices
    }

    #[test]
    fn test_compute_dsr_basic() {
        // With zero trials or zero obs, DSR should be 0
        assert_eq!(compute_dsr(1.0, 0, 0.0, 3.0, 100), 0.0);
        assert_eq!(compute_dsr(1.0, 10, 0.0, 3.0, 2), 0.0);

        // Normal returns (skew=0, kurt=0 excess): moderate Sharpe should yield moderate DSR
        let dsr = compute_dsr(1.5, 10, 0.0, 0.0, 252);
        assert!(dsr > 0.0 && dsr <= 1.0, "DSR={dsr}");

        // Higher Sharpe should yield higher DSR
        let dsr_high = compute_dsr(3.0, 10, 0.0, 0.0, 252);
        assert!(dsr_high > dsr, "Higher SR should yield higher DSR");

        // More trials should lower DSR (higher expected max SR)
        let dsr_many = compute_dsr(1.5, 1000, 0.0, 0.0, 252);
        assert!(dsr_many < dsr, "More trials should lower DSR");
    }

    #[test]
    fn test_forward_returns_purging() {
        let prices = synthetic_prices(100);
        // Dense signals: every bar → purging should kick in for horizon=5
        let signal_indices: Vec<usize> = (0..100).collect();
        let (used_indices, returns) = get_signal_returns(&prices, &signal_indices, 5);

        // Purging should space indices by at least the horizon
        assert!(
            returns.len() >= 10 && returns.len() <= 25,
            "Got {} observations",
            returns.len()
        );
        assert_eq!(
            used_indices.len(),
            returns.len(),
            "Indices and returns must be aligned"
        );

        // All surviving indices should be spaced by at least h
        for w in used_indices.windows(2) {
            assert!(
                w[1] - w[0] >= 5,
                "Purged indices should be spaced by at least the horizon"
            );
        }
    }

    #[test]
    fn test_signal_returns_sparse_dates() {
        let prices = synthetic_prices(200);
        // Sparse signals: every 30 bars (well above any horizon)
        let signal_indices: Vec<usize> = (0..200).step_by(30).collect();
        let (used_indices, returns) = get_signal_returns(&prices, &signal_indices, 10);
        // With sparse dates (gap=30 > horizon=10), should use all signals
        assert!(returns.len() >= 5, "Sparse signals should not be purged");
        assert_eq!(
            used_indices.len(),
            returns.len(),
            "Indices and returns must be aligned"
        );
    }

    #[test]
    fn test_deduplication() {
        let prices = synthetic_prices(100);
        let dates: Vec<NaiveDate> = prices
            .iter()
            .filter_map(|p| epoch_to_naive_date(p.date))
            .collect();

        // Two patterns with identical signal dates should be merged
        let p1 = ScoredHypothesis {
            pattern: RawPattern {
                dimension: HypothesisDimension::Seasonality,
                description: "Pattern A".into(),
                structural_basis: StructuralBasis::RebalancingFlows,
                signal_spec: SignalSpec::Formula {
                    formula: "a".into(),
                },
                forward_horizon: 5,
                signal_dates: dates[..20].to_vec(),
                signal_returns: vec![0.01; 20],
            },
            p_value: 0.01,
            adjusted_p_value: 0.02,
            effect_size: 0.01,
            sharpe: 1.5,
            dsr: 0.8,
            regime_stability: None,
            cluster_id: 0,
            composite_score: 0.8,
        };

        let mut p2 = p1.clone();
        p2.pattern.description = "Pattern B".into();
        p2.dsr = 0.5; // lower DSR

        let result = deduplicate_patterns(vec![p1, p2], 0.5, &prices);
        assert_eq!(result.len(), 1, "Identical signals should deduplicate to 1");
        assert_eq!(
            result[0].pattern.description, "Pattern A",
            "Higher DSR should survive"
        );
    }

    #[test]
    fn test_generate_hypotheses_smoke() {
        let prices = synthetic_prices(500);
        let config = HypothesisConfig {
            forward_horizons: vec![5, 10],
            significance: 0.10, // relaxed for synthetic data
            dedup_threshold: 0.5,
        };
        let dims = vec![
            HypothesisDimension::Seasonality,
            HypothesisDimension::PriceAction,
        ];
        let (total_trials, patterns_tested, _patterns_sig, _hypotheses) =
            generate_hypotheses(&prices, &config, &dims, None, &HashMap::new());
        assert!(
            patterns_tested <= total_trials,
            "patterns_tested should be <= total_trials"
        );
        assert!(total_trials > 0, "Should generate some trials");
        // We don't assert hypotheses > 0 since synthetic data may not have significant patterns
    }
}
