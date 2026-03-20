//! Monte Carlo simulation tool for forward-looking risk analysis.
//!
//! Generates thousands of synthetic equity paths using block-bootstrapped
//! historical returns to produce confidence intervals on terminal wealth,
//! max drawdown distributions, and ruin probabilities.

use anyhow::{Context, Result};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;

use crate::data::cache::CachedStore;
use crate::stats;
use crate::tools::response_types::{
    DrawdownDistribution, HistogramBin, MonteCarloPercentilePath, MonteCarloResponse, RuinAnalysis,
};

/// Block size for bootstrap resampling (trading days).
const BLOCK_SIZE: usize = 21;

/// Execute Monte Carlo simulation.
pub async fn execute(
    cache: &Arc<CachedStore>,
    symbol: &str,
    n_simulations: usize,
    horizon_days: usize,
    initial_capital: f64,
    years: u32,
    seed: Option<u64>,
) -> Result<MonteCarloResponse> {
    let upper = symbol.to_uppercase();
    let cutoff =
        chrono::Utc::now().date_naive() - chrono::Duration::days(i64::from(years) * 365);
    let cutoff_str = cutoff.format("%Y-%m-%d").to_string();

    let resp = crate::tools::raw_prices::load_and_execute(
        cache,
        &upper,
        Some(&cutoff_str),
        None,
        None,
        crate::engine::types::Interval::Daily,
        None,
    )
    .await
    .context("Failed to load OHLCV data")?;

    if resp.prices.len() < 60 {
        anyhow::bail!("Insufficient price data for {upper}: need at least 60 bars for Monte Carlo");
    }

    // Compute daily returns
    let returns: Vec<f64> = resp
        .prices
        .windows(2)
        .filter_map(|w| {
            if w[0].close == 0.0 {
                None
            } else {
                Some((w[1].close - w[0].close) / w[0].close)
            }
        })
        .filter(|r| r.is_finite())
        .collect();

    if returns.len() < 30 {
        anyhow::bail!("Insufficient return observations: {}", returns.len());
    }

    // Run simulations
    let mut rng = match seed {
        Some(s) => StdRng::seed_from_u64(s),
        None => StdRng::from_os_rng(),
    };

    let mut terminal_values = Vec::with_capacity(n_simulations);
    let mut max_drawdowns = Vec::with_capacity(n_simulations);

    for _ in 0..n_simulations {
        let (terminal, max_dd) =
            simulate_path(&returns, horizon_days, initial_capital, &mut rng);
        terminal_values.push(terminal);
        max_drawdowns.push(max_dd);
    }

    // Sort for percentile extraction
    terminal_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    max_drawdowns.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    // Percentile paths
    let percentiles = [5.0, 25.0, 50.0, 75.0, 95.0];
    let labels = ["5th", "25th", "50th (median)", "75th", "95th"];
    let percentile_paths: Vec<MonteCarloPercentilePath> = percentiles
        .iter()
        .zip(labels.iter())
        .map(|(&pct, &label)| {
            let idx =
                ((pct / 100.0 * terminal_values.len() as f64).floor() as usize).min(terminal_values.len() - 1);
            let tv = terminal_values[idx];
            MonteCarloPercentilePath {
                label: label.to_string(),
                percentile: pct,
                terminal_value: tv,
                total_return_pct: (tv - initial_capital) / initial_capital * 100.0,
            }
        })
        .collect();

    // Ruin analysis
    let prob_loss_10 =
        terminal_values.iter().filter(|&&v| v < initial_capital * 0.9).count() as f64
            / n_simulations as f64;
    let prob_loss_25 =
        terminal_values.iter().filter(|&&v| v < initial_capital * 0.75).count() as f64
            / n_simulations as f64;
    let prob_loss_50 =
        terminal_values.iter().filter(|&&v| v < initial_capital * 0.5).count() as f64
            / n_simulations as f64;
    let prob_negative =
        terminal_values.iter().filter(|&&v| v < initial_capital).count() as f64
            / n_simulations as f64;

    let ruin_analysis = RuinAnalysis {
        prob_loss_10pct: prob_loss_10,
        prob_loss_25pct: prob_loss_25,
        prob_loss_50pct: prob_loss_50,
        prob_negative_return: prob_negative,
    };

    // Max drawdown distribution
    let dd_mean = max_drawdowns.iter().sum::<f64>() / max_drawdowns.len() as f64;
    let dd_median = stats::median(&max_drawdowns);
    let dd_p5 = stats::percentile(&max_drawdowns, 5.0);
    let dd_p95 = stats::percentile(&max_drawdowns, 95.0);
    let dd_worst = max_drawdowns.last().copied().unwrap_or(0.0);

    let drawdown_distribution = DrawdownDistribution {
        mean: dd_mean * 100.0,
        median: dd_median * 100.0,
        percentile_5: dd_p5 * 100.0,
        percentile_95: dd_p95 * 100.0,
        worst: dd_worst * 100.0,
    };

    // Terminal histogram
    let hist = stats::histogram(
        &terminal_values.iter().map(|v| (v - initial_capital) / initial_capital * 100.0).collect::<Vec<_>>(),
        30,
    );
    let terminal_histogram: Vec<HistogramBin> = hist
        .into_iter()
        .map(|b| HistogramBin {
            lower: b.lower,
            upper: b.upper,
            count: b.count,
            frequency: b.frequency,
        })
        .collect();

    // AI response
    let median_return = percentile_paths
        .iter()
        .find(|p| p.label.contains("median"))
        .map_or(0.0, |p| p.total_return_pct);

    let summary = format!(
        "Monte Carlo simulation for {upper}: {n_simulations} paths over {horizon_days} days. \
         Median terminal return={median_return:.1}%, P(loss)={:.1}%, \
         median max drawdown={:.1}%.",
        prob_negative * 100.0,
        dd_median * 100.0,
    );

    let key_findings = vec![
        format!(
            "90% confidence band: [{:.1}%, {:.1}%] terminal return",
            percentile_paths.first().map_or(0.0, |p| p.total_return_pct),
            percentile_paths.last().map_or(0.0, |p| p.total_return_pct),
        ),
        format!(
            "Probability of loss: {:.1}% (>10% loss: {:.1}%, >25% loss: {:.1}%)",
            prob_negative * 100.0,
            prob_loss_10 * 100.0,
            prob_loss_25 * 100.0,
        ),
        format!(
            "Max drawdown: median={:.1}%, 95th percentile={:.1}%, worst={:.1}%",
            dd_median * 100.0,
            dd_p95 * 100.0,
            dd_worst * 100.0,
        ),
        format!(
            "Block bootstrap with {BLOCK_SIZE}-day blocks preserves autocorrelation structure"
        ),
    ];

    let suggested_next_steps = vec![
        format!(
            "[NEXT] Call drawdown_analysis(symbol=\"{upper}\") to compare simulated vs historical drawdowns"
        ),
        format!(
            "[THEN] Call regime_detect(symbol=\"{upper}\") to see if risk varies across market regimes"
        ),
        "[TIP] Use the ruin probabilities to size positions — ensure P(ruin) is below your tolerance"
            .to_string(),
    ];

    Ok(MonteCarloResponse {
        summary,
        symbol: upper,
        n_simulations,
        horizon_days,
        initial_capital,
        percentile_paths,
        ruin_analysis,
        drawdown_distribution,
        terminal_histogram,
        key_findings,
        suggested_next_steps,
    })
}

/// Simulate one equity path using block bootstrap resampling.
/// Returns (terminal_equity, max_drawdown_fraction).
fn simulate_path(
    returns: &[f64],
    horizon: usize,
    initial_capital: f64,
    rng: &mut StdRng,
) -> (f64, f64) {
    let n = returns.len();
    let mut equity = initial_capital;
    let mut peak = initial_capital;
    let mut max_dd = 0.0_f64;
    let mut days_simulated = 0;

    while days_simulated < horizon {
        // Pick a random block start
        let block_start = rng.random_range(0..n.saturating_sub(BLOCK_SIZE).max(1));
        let block_end = (block_start + BLOCK_SIZE).min(n);

        for &ret in &returns[block_start..block_end] {
            equity *= 1.0 + ret;
            if equity > peak {
                peak = equity;
            }
            let dd = if peak > 0.0 {
                (peak - equity) / peak
            } else {
                0.0
            };
            if dd > max_dd {
                max_dd = dd;
            }
            days_simulated += 1;
            if days_simulated >= horizon {
                break;
            }
        }
    }

    (equity, max_dd)
}
