//! Shared helper functions and threshold constants for AI-enriched tool responses.
//!
//! Centralises assessment logic (Sharpe tiers, p-value interpretation, data quality
//! warnings) so that all formatting modules use consistent language and thresholds.

use anyhow::Context;

use crate::engine::types::{to_display_name, BacktestParams};

use super::response_types::{BacktestParamsSummary, PriceBar};

// ── Assessment thresholds ────────────────────────────────────────────────────
// Centralised so they can be tuned (or made configurable) in one place.

/// Sharpe ratio tier boundaries (descending).
pub(crate) const SHARPE_EXCELLENT: f64 = 1.5;
pub(crate) const SHARPE_STRONG: f64 = 1.0;
pub(crate) const SHARPE_MODERATE: f64 = 0.5;

/// P-value significance tiers.
pub(crate) const P_HIGHLY_SIGNIFICANT: f64 = 0.01;
pub(crate) const P_SIGNIFICANT: f64 = 0.05;
pub(crate) const P_MARGINALLY_SIGNIFICANT: f64 = 0.10;

/// Walk-forward Sharpe decay thresholds.
pub(crate) const WF_SHARPE_DECAY_HIGH: f64 = 0.5;
pub(crate) const WF_SHARPE_DECAY_LOW: f64 = 0.1;

/// Walk-forward profitable window thresholds (percentage).
pub(crate) const WF_PROFITABLE_WINDOWS_GOOD: f64 = 70.0;
pub(crate) const WF_PROFITABLE_WINDOWS_BAD: f64 = 50.0;

/// Sweep overall score tiers.
pub(crate) const SWEEP_SCORE_WEAK: f64 = 0.5;
pub(crate) const SWEEP_SCORE_MODERATE: f64 = 0.7;

/// Build a serialisable parameter summary from backtest params for inclusion in responses.
pub(crate) fn build_params_summary(params: &BacktestParams) -> BacktestParamsSummary {
    BacktestParamsSummary {
        display_name: to_display_name(&params.strategy),
        strategy: params.strategy.clone(),
        leg_deltas: params.leg_deltas.clone(),
        entry_dte: params.entry_dte.clone(),
        exit_dte: params.exit_dte,
        slippage: params.slippage.clone(),
        commission: params.commission.clone(),
        capital: params.capital,
        quantity: params.quantity,
        multiplier: params.multiplier,
        max_positions: params.max_positions,
        stop_loss: params.stop_loss,
        take_profit: params.take_profit,
        max_hold_days: params.max_hold_days,
        selector: params.selector.clone(),
        entry_signal: params.entry_signal.as_ref().map(|s| {
            serde_json::to_value(s).unwrap_or_else(|e| {
                tracing::warn!(error = %e, "Failed to serialize entry_signal");
                serde_json::Value::Null
            })
        }),
        exit_signal: params.exit_signal.as_ref().map(|s| {
            serde_json::to_value(s).unwrap_or_else(|e| {
                tracing::warn!(error = %e, "Failed to serialize exit_signal");
                serde_json::Value::Null
            })
        }),
        min_net_premium: params.min_net_premium,
        max_net_premium: params.max_net_premium,
        min_net_delta: params.min_net_delta,
        max_net_delta: params.max_net_delta,
        min_days_between_entries: params.min_days_between_entries,
        expiration_filter: params.expiration_filter.clone(),
        exit_net_delta: params.exit_net_delta,
        sizing: params.sizing.clone(),
    }
}

/// Return a human-readable label for a Sharpe ratio value (e.g. "excellent", "poor").
pub(crate) fn assess_sharpe(sharpe: f64) -> &'static str {
    if sharpe >= SHARPE_EXCELLENT {
        "excellent"
    } else if sharpe >= SHARPE_STRONG {
        "strong"
    } else if sharpe >= SHARPE_MODERATE {
        "moderate"
    } else if sharpe >= 0.0 {
        "weak"
    } else {
        "poor"
    }
}

/// Format a P&L value as a signed dollar string (e.g. "+$150.00" or "-$42.50").
pub(crate) fn format_pnl(value: f64) -> String {
    if value >= 0.0 {
        format!("+${value:.2}")
    } else {
        format!("-${:.2}", value.abs())
    }
}

/// Generate key findings from walk-forward aggregate statistics.
pub(crate) fn walk_forward_findings(
    agg: &crate::engine::walk_forward::WalkForwardAggregate,
) -> Vec<String> {
    let mut findings = Vec::new();
    if agg.failed_windows > 0 {
        findings.push(format!(
            "{} window(s) failed and were excluded from aggregate statistics",
            agg.failed_windows
        ));
    }
    if agg.avg_train_test_sharpe_decay > WF_SHARPE_DECAY_HIGH {
        findings.push(format!(
            "High train→test Sharpe decay ({:.2}) suggests overfitting risk",
            agg.avg_train_test_sharpe_decay
        ));
    } else if agg.avg_train_test_sharpe_decay < WF_SHARPE_DECAY_LOW {
        findings.push(format!(
            "Low train→test Sharpe decay ({:.2}) indicates robust strategy",
            agg.avg_train_test_sharpe_decay
        ));
    }
    if agg.pct_profitable_windows >= WF_PROFITABLE_WINDOWS_GOOD {
        findings.push(format!(
            "{:.0}% of test windows profitable — strong consistency",
            agg.pct_profitable_windows
        ));
    } else if agg.pct_profitable_windows < WF_PROFITABLE_WINDOWS_BAD {
        findings.push(format!(
            "Only {:.0}% of test windows profitable — strategy may be unreliable",
            agg.pct_profitable_windows
        ));
    }
    if agg.std_test_sharpe > 1.0 {
        findings.push(format!(
            "High variance in test Sharpe (σ={:.2}) — performance is inconsistent across windows",
            agg.std_test_sharpe
        ));
    }
    findings.push(format!(
        "Average out-of-sample Sharpe is {} ({:.2})",
        assess_sharpe(agg.avg_test_sharpe),
        agg.avg_test_sharpe
    ));
    findings
}

/// Convert an epoch timestamp (seconds) to a `YYYY-MM-DD` date string.
///
/// Returns the raw integer as a string if the timestamp is out of range.
pub(crate) fn epoch_to_date_string(epoch: i64) -> String {
    chrono::DateTime::from_timestamp(epoch, 0).map_or_else(
        || format!("{epoch}"),
        |dt| dt.naive_utc().format("%Y-%m-%d").to_string(),
    )
}

/// Format an epoch timestamp as date-only or full datetime depending on interval.
///
/// For intraday intervals, returns `YYYY-MM-DD HH:MM` to avoid collapsing
/// multiple bars onto the same date label. For daily and above, returns `YYYY-MM-DD`.
pub(crate) fn epoch_to_timestamp_string(
    epoch: i64,
    interval: crate::engine::types::Interval,
) -> String {
    if interval.is_intraday() {
        chrono::DateTime::from_timestamp(epoch, 0).map_or_else(
            || format!("{epoch}"),
            |dt| dt.naive_utc().format("%Y-%m-%d %H:%M").to_string(),
        )
    } else {
        epoch_to_date_string(epoch)
    }
}

/// Return a significance label for a p-value (e.g. "highly significant", "not significant").
pub(crate) fn interpret_p_value(p: f64) -> &'static str {
    if p < P_HIGHLY_SIGNIFICANT {
        "highly significant"
    } else if p < P_SIGNIFICANT {
        "significant"
    } else if p < P_MARGINALLY_SIGNIFICANT {
        "marginally significant"
    } else {
        "not significant"
    }
}

// ── Shared utility helpers ──────────────────────────────────────────────────

/// Compute a date cutoff string (YYYY-MM-DD) going back `years` from today.
pub(crate) fn compute_years_cutoff(years: u32) -> String {
    let cutoff = chrono::Utc::now().date_naive() - chrono::Duration::days(i64::from(years) * 365);
    cutoff.format("%Y-%m-%d").to_string()
}

/// Evenly subsample a vector down to at most `max` elements.
pub(crate) fn subsample_to_max<T: Clone>(data: Vec<T>, max: usize) -> Vec<T> {
    let n = data.len();
    if n <= max {
        return data;
    }
    let mut indices: Vec<usize> = (0..max).map(|i| i * (n - 1) / (max - 1)).collect();
    indices.dedup();
    indices.iter().map(|&i| data[i].clone()).collect()
}

/// Compute simple returns and corresponding dates from a price series.
///
/// Emits `NaN` for bars where the prior close is zero, preserving index alignment.
pub(crate) fn compute_returns(prices: &[PriceBar]) -> (Vec<f64>, Vec<i64>) {
    let returns: Vec<f64> = prices
        .windows(2)
        .map(|w| {
            if w[0].close == 0.0 {
                f64::NAN
            } else {
                (w[1].close - w[0].close) / w[0].close
            }
        })
        .collect();
    let dates: Vec<i64> = prices[1..].iter().map(|p| p.date).collect();
    (returns, dates)
}

/// Compute p-value for a Pearson correlation coefficient.
pub(crate) fn pearson_p_value(r: f64, n: usize) -> Option<f64> {
    if n <= 2 {
        return None;
    }
    let r_sq = r * r;
    let denom = (1.0 - r_sq).max(0.0);
    if denom < f64::EPSILON {
        Some(0.0)
    } else {
        let t_stat = r * ((n as f64 - 2.0) / denom).sqrt();
        statrs::distribution::StudentsT::new(0.0, 1.0, (n - 2) as f64)
            .ok()
            .map(|d| {
                use statrs::distribution::ContinuousCDF;
                2.0 * (1.0 - d.cdf(t_stat.abs()))
            })
    }
}

/// Parse a date string parameter with a descriptive error.
pub(crate) fn parse_date_param(
    date_str: &str,
    param_name: &str,
) -> anyhow::Result<chrono::NaiveDate> {
    date_str
        .parse::<chrono::NaiveDate>()
        .with_context(|| format!("Invalid {param_name}: {date_str}"))
}

/// Validate that a string value is one of the allowed choices.
pub(crate) fn validate_choice<'a>(
    value: &'a str,
    valid: &[&str],
    field_name: &str,
) -> anyhow::Result<&'a str> {
    if valid.contains(&value) {
        Ok(value)
    } else {
        anyhow::bail!(
            "Invalid {field_name}: \"{value}\". Must be one of: {}",
            valid.join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::types::Interval;

    // ─── epoch_to_date_string ───────────────────────────────────────────

    #[test]
    fn epoch_to_date_string_known_date() {
        // 2020-01-01 00:00:00 UTC
        assert_eq!(epoch_to_date_string(1_577_836_800), "2020-01-01");
    }

    #[test]
    fn epoch_to_date_string_with_time_component() {
        // 2024-06-15 14:30:00 UTC — time should be discarded
        assert_eq!(epoch_to_date_string(1_718_461_800), "2024-06-15");
    }

    // ─── epoch_to_timestamp_string ──────────────────────────────────────

    #[test]
    fn timestamp_string_daily_returns_date_only() {
        let epoch = 1_718_461_800; // 2024-06-15 14:30:00 UTC
        let result = epoch_to_timestamp_string(epoch, Interval::Daily);
        assert_eq!(result, "2024-06-15");
    }

    #[test]
    fn timestamp_string_midnight_intraday_shows_zeros() {
        // 2024-01-02 00:00:00 UTC
        let epoch = 1_704_153_600;
        let result = epoch_to_timestamp_string(epoch, Interval::Hour1);
        assert_eq!(result, "2024-01-02 00:00");
    }

    #[test]
    fn subsample_respects_max() {
        let data: Vec<i32> = (0..1000).collect();
        let result = subsample_to_max(data, 500);
        assert_eq!(result.len(), 500);
        assert_eq!(result[0], 0);
        assert_eq!(result[499], 999);
    }

    #[test]
    fn subsample_smaller_than_max() {
        let data: Vec<i32> = (0..100).collect();
        let result = subsample_to_max(data, 500);
        assert_eq!(
            result.len(),
            100,
            "should not change if already within limit"
        );
    }

    #[test]
    fn timestamp_string_all_non_intraday_match_date_string() {
        let epoch = 1_718_461_800;
        let date_only = epoch_to_date_string(epoch);
        for interval in [Interval::Daily, Interval::Weekly, Interval::Monthly] {
            assert_eq!(
                epoch_to_timestamp_string(epoch, interval),
                date_only,
                "non-intraday interval {interval} should match epoch_to_date_string"
            );
        }
    }
}
