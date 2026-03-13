//! Permutation testing for backtest statistical significance.
//!
//! Shuffles entry candidates across dates, re-runs the simulation N times,
//! and compares real results against the random distribution to produce p-values.

use std::collections::{BTreeMap, HashSet};
use std::hash::BuildHasher;

use anyhow::Result;
use chrono::NaiveDate;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::event_sim;
use super::metrics;
use super::types::{BacktestParams, BacktestResult, EntryCandidate};
use super::vectorized_sim;
use crate::strategies;

/// Parameters controlling the permutation test.
#[derive(Debug, Clone)]
pub struct PermutationParams {
    pub num_permutations: usize,
    pub seed: Option<u64>,
}

/// Result for a single metric's permutation test.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MetricPermutationResult {
    pub metric_name: String,
    pub real_value: f64,
    pub p_value: f64,
    pub mean_permuted: f64,
    pub std_permuted: f64,
    pub percentile_5: f64,
    pub percentile_95: f64,
    pub histogram: Vec<HistogramBucket>,
}

/// A single histogram bucket for the permuted distribution.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct HistogramBucket {
    pub bin_start: f64,
    pub bin_end: f64,
    pub count: usize,
}

/// Output of the full permutation test.
#[derive(Debug, Clone)]
pub struct PermutationOutput {
    pub num_permutations: usize,
    pub num_completed: usize,
    pub real_result: BacktestResult,
    pub metric_results: Vec<MetricPermutationResult>,
}

/// Collected metric values from a single permutation run.
struct PermMetrics {
    sharpe: f64,
    total_pnl: f64,
    win_rate: f64,
    profit_factor: f64,
    cagr: f64,
}

/// Run permutation test: real backtest + N shuffled reruns.
pub fn run_permutation_test<S1: BuildHasher, S2: BuildHasher>(
    df: &polars::prelude::DataFrame,
    params: &BacktestParams,
    perm_params: &PermutationParams,
    entry_dates: &Option<HashSet<NaiveDate, S1>>,
    exit_dates: Option<&HashSet<NaiveDate, S2>>,
) -> Result<PermutationOutput> {
    let strategy_def = strategies::find_strategy(&params.strategy)
        .ok_or_else(|| anyhow::anyhow!("Unknown strategy: {}", params.strategy))?;

    // Run real backtest
    let real_result = crate::engine::core::run_backtest(df, params)?;

    // Build shared infrastructure once
    let (price_table, trading_days, _date_index) = event_sim::build_price_table(df)?;
    let carry_index = vectorized_sim::build_carry_index(&price_table);

    // Build candidates (same as vectorized path)
    let mut candidates = event_sim::find_entry_candidates(df, &strategy_def, params)?;
    if let Some(ref allowed_dates) = entry_dates {
        candidates.retain(|date, _| allowed_dates.contains(date));
    }

    if candidates.is_empty() {
        return Ok(PermutationOutput {
            num_permutations: perm_params.num_permutations,
            num_completed: 0,
            real_result,
            metric_results: vec![],
        });
    }

    let ctx = SimContext {
        price_table: &price_table,
        carry_index: &carry_index,
        trading_days: &trading_days,
        strategy_def: &strategy_def,
        params,
    };

    let perm_metrics = run_shuffled_permutations(&candidates, &ctx, perm_params, exit_dates)?;

    let num_completed = perm_metrics.len();
    let real_total_pnl: f64 = real_result.trade_log.iter().map(|t| t.pnl).sum();

    let metric_results = vec![
        compute_metric_result(
            "sharpe",
            real_result.metrics.sharpe,
            &extract_field(&perm_metrics, |m| m.sharpe),
        ),
        compute_metric_result(
            "total_pnl",
            real_total_pnl,
            &extract_field(&perm_metrics, |m| m.total_pnl),
        ),
        compute_metric_result(
            "win_rate",
            real_result.metrics.win_rate,
            &extract_field(&perm_metrics, |m| m.win_rate),
        ),
        compute_metric_result(
            "profit_factor",
            real_result.metrics.profit_factor,
            &extract_field(&perm_metrics, |m| m.profit_factor),
        ),
        compute_metric_result(
            "cagr",
            real_result.metrics.cagr,
            &extract_field(&perm_metrics, |m| m.cagr),
        ),
    ];

    Ok(PermutationOutput {
        num_permutations: perm_params.num_permutations,
        num_completed,
        real_result,
        metric_results,
    })
}

/// Shared infrastructure built once and reused across all permutations.
struct SimContext<'a> {
    price_table: &'a super::types::PriceTable,
    carry_index: &'a vectorized_sim::CarryIndex,
    trading_days: &'a [NaiveDate],
    strategy_def: &'a super::types::StrategyDef,
    params: &'a BacktestParams,
}

/// Run N shuffled permutations and collect metrics from each.
fn run_shuffled_permutations<S: BuildHasher>(
    candidates: &BTreeMap<NaiveDate, Vec<EntryCandidate>>,
    ctx: &SimContext<'_>,
    perm_params: &PermutationParams,
    exit_dates: Option<&HashSet<NaiveDate, S>>,
) -> Result<Vec<PermMetrics>> {
    let date_keys: Vec<NaiveDate> = candidates.keys().copied().collect();
    let group_sizes: Vec<usize> = date_keys.iter().map(|d| candidates[d].len()).collect();
    let all_candidates: Vec<EntryCandidate> = candidates
        .values()
        .flat_map(|v| v.iter().cloned())
        .collect();

    let mut rng = match perm_params.seed {
        Some(seed) => rand::rngs::StdRng::seed_from_u64(seed),
        None => rand::rngs::StdRng::from_os_rng(),
    };

    let mut perm_metrics: Vec<PermMetrics> = Vec::with_capacity(perm_params.num_permutations);

    for i in 0..perm_params.num_permutations {
        let mut pool = all_candidates.clone();
        pool.shuffle(&mut rng);

        let mut shuffled = BTreeMap::new();
        let mut offset = 0;
        for (idx, &date) in date_keys.iter().enumerate() {
            let size = group_sizes[idx];
            let slice = &pool[offset..offset + size];
            let mut group: Vec<EntryCandidate> = Vec::with_capacity(size);
            for mut c in slice.iter().cloned() {
                // Skip candidates whose expiration is before the shuffled entry date
                // to avoid negative DTE producing degenerate 1-day trades.
                if date > c.expiration {
                    continue;
                }
                c.entry_date = date;
                group.push(c);
            }
            shuffled.insert(date, group);
            offset += size;
        }

        match vectorized_sim::run_with_candidates(
            &shuffled,
            ctx.price_table,
            ctx.carry_index,
            ctx.trading_days,
            ctx.strategy_def,
            ctx.params,
            exit_dates,
        ) {
            Ok((trade_log, equity_curve, _)) => {
                let m = metrics::calculate_metrics(
                    &equity_curve,
                    &trade_log,
                    ctx.params.capital,
                    252.0,
                )?;
                let total_pnl: f64 = trade_log.iter().map(|t| t.pnl).sum();
                perm_metrics.push(PermMetrics {
                    sharpe: m.sharpe,
                    total_pnl,
                    win_rate: m.win_rate,
                    profit_factor: m.profit_factor,
                    cagr: m.cagr,
                });
            }
            Err(e) => {
                tracing::warn!(permutation = i, error = %e, "Permutation failed, skipping");
            }
        }
    }

    Ok(perm_metrics)
}

fn extract_field(metrics: &[PermMetrics], f: fn(&PermMetrics) -> f64) -> Vec<f64> {
    metrics.iter().map(f).collect()
}

fn compute_metric_result(
    name: &str,
    real_value: f64,
    permuted_values: &[f64],
) -> MetricPermutationResult {
    if permuted_values.is_empty() {
        return MetricPermutationResult {
            metric_name: name.to_string(),
            real_value,
            p_value: 1.0,
            mean_permuted: 0.0,
            std_permuted: 0.0,
            percentile_5: 0.0,
            percentile_95: 0.0,
            histogram: vec![],
        };
    }

    let n = permuted_values.len() as f64;

    // p-value: fraction of permutations >= real value
    let count_gte = permuted_values.iter().filter(|&&v| v >= real_value).count();
    let p_value = count_gte as f64 / n;

    // Mean and std
    let mean = permuted_values.iter().sum::<f64>() / n;
    let variance = if permuted_values.len() > 1 {
        permuted_values
            .iter()
            .map(|v| (v - mean).powi(2))
            .sum::<f64>()
            / (n - 1.0)
    } else {
        0.0
    };
    let std = variance.sqrt();

    // Percentiles
    let mut sorted = permuted_values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p5 = percentile(&sorted, 5.0);
    let p95 = percentile(&sorted, 95.0);

    // Histogram (10 buckets)
    let histogram = build_histogram(&sorted, 10);

    MetricPermutationResult {
        metric_name: name.to_string(),
        real_value,
        p_value,
        mean_permuted: mean,
        std_permuted: std,
        percentile_5: p5,
        percentile_95: p95,
        histogram,
    }
}

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (pct / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn build_histogram(sorted: &[f64], num_buckets: usize) -> Vec<HistogramBucket> {
    if sorted.is_empty() || num_buckets == 0 {
        return vec![];
    }

    let min = sorted[0];
    let max = sorted[sorted.len() - 1];

    if (max - min).abs() < f64::EPSILON {
        return vec![HistogramBucket {
            bin_start: min,
            bin_end: max,
            count: sorted.len(),
        }];
    }

    let width = (max - min) / num_buckets as f64;
    let mut buckets: Vec<HistogramBucket> = (0..num_buckets)
        .map(|i| HistogramBucket {
            bin_start: min + width * i as f64,
            bin_end: min + width * (i + 1) as f64,
            count: 0,
        })
        .collect();

    for &val in sorted {
        let idx = ((val - min) / width).floor() as usize;
        let idx = idx.min(num_buckets - 1);
        buckets[idx].count += 1;
    }

    buckets
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_percentile_basic() {
        let sorted = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert!((percentile(&sorted, 0.0) - 1.0).abs() < f64::EPSILON);
        assert!((percentile(&sorted, 50.0) - 3.0).abs() < f64::EPSILON);
        assert!((percentile(&sorted, 100.0) - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_percentile_empty() {
        assert!((percentile(&[], 50.0) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_percentile_single() {
        assert!((percentile(&[42.0], 50.0) - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_histogram_empty() {
        assert!(build_histogram(&[], 10).is_empty());
    }

    #[test]
    fn test_histogram_single_value() {
        let h = build_histogram(&[5.0, 5.0, 5.0], 10);
        assert_eq!(h.len(), 1);
        assert_eq!(h[0].count, 3);
    }

    #[test]
    fn test_histogram_counts_sum() {
        let sorted: Vec<f64> = (0..100).map(f64::from).collect();
        let h = build_histogram(&sorted, 10);
        let total: usize = h.iter().map(|b| b.count).sum();
        assert_eq!(total, 100);
        assert_eq!(h.len(), 10);
    }

    #[test]
    fn test_compute_metric_real_exceeds_all() {
        let permuted = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = compute_metric_result("test", 10.0, &permuted);
        assert!((result.p_value - 0.0).abs() < f64::EPSILON);
        assert_eq!(result.metric_name, "test");
        assert!((result.real_value - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_compute_metric_real_below_all() {
        let permuted = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = compute_metric_result("test", -1.0, &permuted);
        assert!((result.p_value - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_compute_metric_empty_permuted() {
        let result = compute_metric_result("test", 5.0, &[]);
        assert!((result.p_value - 1.0).abs() < f64::EPSILON);
        assert!(result.histogram.is_empty());
    }

    #[test]
    fn test_compute_metric_mean_and_std() {
        let permuted = vec![2.0, 4.0, 6.0];
        let result = compute_metric_result("test", 5.0, &permuted);
        assert!((result.mean_permuted - 4.0).abs() < f64::EPSILON);
        assert!((result.std_permuted - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_compute_metric_p_value_fraction() {
        // 3 out of 5 values >= 3.0
        let permuted = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = compute_metric_result("test", 3.0, &permuted);
        assert!((result.p_value - 0.6).abs() < f64::EPSILON);
    }
}
