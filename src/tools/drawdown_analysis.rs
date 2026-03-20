//! Drawdown distribution analysis tool.
//!
//! Decomposes the equity curve into individual drawdown episodes and computes
//! detailed distribution statistics: episode durations, depths, time-to-recovery,
//! underwater curve, and Ulcer Index. Complements the aggregate `max_drawdown`
//! metric in `PerformanceMetrics` with a full distributional view.

use anyhow::{Context, Result};
use std::sync::Arc;

use crate::data::cache::CachedStore;
use crate::tools::response_types::{
    DrawdownAnalysisResponse, DrawdownEpisode, DrawdownStats, UnderwaterPoint,
};

/// Execute the drawdown distribution analysis.
pub async fn execute(
    cache: &Arc<CachedStore>,
    symbol: &str,
    years: u32,
) -> Result<DrawdownAnalysisResponse> {
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

    if resp.prices.len() < 10 {
        anyhow::bail!("Insufficient price data for {upper}: need at least 10 bars");
    }

    // Build equity curve from close prices (treat first close as initial capital)
    let closes: Vec<f64> = resp.prices.iter().map(|p| p.close).collect();
    let dates: Vec<i64> = resp.prices.iter().map(|p| p.date).collect();
    let initial = closes[0];

    // Compute per-bar drawdown percentage and detect episodes
    let mut peak = initial;
    let mut episodes: Vec<DrawdownEpisode> = Vec::new();
    let mut underwater: Vec<UnderwaterPoint> = Vec::new();
    let mut dd_sq_sum = 0.0;
    let mut current_episode_start: Option<usize> = None;
    let mut current_episode_peak = initial;

    for (i, &close) in closes.iter().enumerate() {
        if close >= peak {
            // New high — close current drawdown episode if active
            if let Some(start_idx) = current_episode_start.take() {
                // Find the trough in this episode
                let episode_slice = &closes[start_idx..=i];
                let trough_offset = episode_slice
                    .iter()
                    .enumerate()
                    .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                    .map_or(0, |(idx, _)| idx);
                let trough_idx = start_idx + trough_offset;
                let depth = (current_episode_peak - closes[trough_idx]) / current_episode_peak;
                let duration = i - start_idx;
                let recovery_bars = i - trough_idx;

                if depth > 0.001 {
                    // Skip trivially small drawdowns (< 0.1%)
                    episodes.push(DrawdownEpisode {
                        start_date: dates[start_idx],
                        trough_date: dates[trough_idx],
                        recovery_date: Some(dates[i]),
                        depth_pct: depth * 100.0,
                        duration_bars: duration,
                        recovery_bars,
                        peak_equity: current_episode_peak,
                        trough_equity: closes[trough_idx],
                    });
                }
            }
            peak = close;
        } else if current_episode_start.is_none() {
            // Start new drawdown episode
            current_episode_start = Some(i);
            current_episode_peak = peak;
        }

        let dd_pct = if peak > 0.0 {
            (peak - close) / peak * 100.0
        } else {
            0.0
        };
        dd_sq_sum += dd_pct * dd_pct;
        underwater.push(UnderwaterPoint {
            date: dates[i],
            drawdown_pct: -dd_pct, // Negative to show as underwater
        });
    }

    // Handle trailing (unrecovered) drawdown
    if let Some(start_idx) = current_episode_start {
        let episode_slice = &closes[start_idx..];
        let trough_offset = episode_slice
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map_or(0, |(idx, _)| idx);
        let trough_idx = start_idx + trough_offset;
        let depth = (current_episode_peak - closes[trough_idx]) / current_episode_peak;
        let duration = closes.len() - start_idx;

        if depth > 0.001 {
            episodes.push(DrawdownEpisode {
                start_date: dates[start_idx],
                trough_date: dates[trough_idx],
                recovery_date: None, // Still in drawdown
                depth_pct: depth * 100.0,
                duration_bars: duration,
                recovery_bars: 0,
                peak_equity: current_episode_peak,
                trough_equity: closes[trough_idx],
            });
        }
    }

    // Sort episodes by depth (worst first)
    episodes.sort_by(|a, b| {
        b.depth_pct
            .partial_cmp(&a.depth_pct)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // Compute aggregate stats
    let n_episodes = episodes.len();
    let ulcer_index = (dd_sq_sum / closes.len() as f64).sqrt();

    let (avg_depth, max_depth, avg_duration, max_duration, avg_recovery, pct_recovered) =
        if episodes.is_empty() {
            (0.0, 0.0, 0.0, 0, 0.0, 100.0)
        } else {
            let depths: Vec<f64> = episodes.iter().map(|e| e.depth_pct).collect();
            let durations: Vec<usize> = episodes.iter().map(|e| e.duration_bars).collect();
            let recovered_episodes: Vec<&DrawdownEpisode> =
                episodes.iter().filter(|e| e.recovery_date.is_some()).collect();
            let recovery_bars: Vec<usize> =
                recovered_episodes.iter().map(|e| e.recovery_bars).collect();

            let avg_d = depths.iter().sum::<f64>() / depths.len() as f64;
            let max_d = depths
                .iter()
                .copied()
                .fold(0.0_f64, f64::max);
            let avg_dur = durations.iter().sum::<usize>() as f64 / durations.len() as f64;
            let max_dur = durations.iter().copied().max().unwrap_or(0);
            let avg_rec = if recovery_bars.is_empty() {
                0.0
            } else {
                recovery_bars.iter().sum::<usize>() as f64 / recovery_bars.len() as f64
            };
            let pct_rec = recovered_episodes.len() as f64 / episodes.len() as f64 * 100.0;

            (avg_d, max_d, avg_dur, max_dur, avg_rec, pct_rec)
        };

    let stats = DrawdownStats {
        total_episodes: n_episodes,
        avg_depth_pct: avg_depth,
        max_depth_pct: max_depth,
        avg_duration_bars: avg_duration,
        max_duration_bars: max_duration,
        avg_recovery_bars: avg_recovery,
        pct_recovered: pct_recovered,
        ulcer_index,
    };

    // Subsample underwater curve to max 500 points
    let underwater = if underwater.len() > 500 {
        let step = underwater.len() / 500;
        underwater
            .into_iter()
            .step_by(step)
            .take(500)
            .collect()
    } else {
        underwater
    };

    // Limit episodes to top 20 by depth
    let top_episodes: Vec<DrawdownEpisode> = episodes.into_iter().take(20).collect();

    // Build AI response
    let summary = format!(
        "Drawdown analysis for {upper}: {} episodes over {} bars. \
         Max depth={max_depth:.2}%, avg depth={avg_depth:.2}%, \
         avg duration={avg_duration:.0} bars, Ulcer Index={ulcer_index:.3}.",
        n_episodes,
        closes.len(),
    );

    let mut key_findings = vec![
        format!("{n_episodes} drawdown episodes detected (>{:.1}% threshold)", 0.1),
        format!("Worst drawdown: {max_depth:.2}% deep, max duration: {max_duration} bars"),
        format!("Average drawdown: {avg_depth:.2}% deep, avg duration: {avg_duration:.0} bars"),
        format!("Ulcer Index: {ulcer_index:.3} — {}", if ulcer_index > 10.0 {
            "high sustained drawdown pain"
        } else if ulcer_index > 5.0 {
            "moderate drawdown pain"
        } else {
            "low drawdown pain"
        }),
    ];
    if pct_recovered < 100.0 {
        key_findings.push(format!(
            "{pct_recovered:.0}% of drawdowns recovered — currently in an open drawdown"
        ));
    }

    let suggested_next_steps = vec![
        format!(
            "[NEXT] Call rolling_metric(symbol=\"{upper}\", metric=\"drawdown\") to see rolling max drawdown"
        ),
        format!(
            "[THEN] Call regime_detect(symbol=\"{upper}\") to check if drawdowns cluster in specific regimes"
        ),
        format!(
            "[TIP] Compare Ulcer Index across strategies — lower = less sustained pain"
        ),
    ];

    Ok(DrawdownAnalysisResponse {
        summary,
        symbol: upper,
        total_bars: closes.len(),
        stats,
        episodes: top_episodes,
        underwater,
        key_findings,
        suggested_next_steps,
    })
}
