//! Walk-forward optimization engine.
//!
//! Splits historical data into train/test windows, optimizes parameters
//! on each training window, and validates on the out-of-sample test window.

use std::collections::HashMap;

use anyhow::{bail, Result};
use chrono::NaiveDate;
use schemars::JsonSchema;
use serde::Deserialize;

/// Walk-forward mode.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WfMode {
    Rolling,
    Anchored,
}

/// Objective metric to optimize.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WfObjective {
    Sharpe,
    Sortino,
    ProfitFactor,
    Cagr,
}

/// A single train/test window boundary.
#[derive(Debug, Clone)]
pub struct WindowBounds {
    pub train_start: NaiveDate,
    pub train_end: NaiveDate,
    pub test_start: NaiveDate,
    pub test_end: NaiveDate,
}

/// Compute window boundaries for walk-forward analysis.
pub fn compute_windows(
    dates: &[NaiveDate],
    n_windows: usize,
    train_pct: f64,
    mode: &WfMode,
) -> Result<Vec<WindowBounds>> {
    if dates.len() < 2 {
        bail!("Need at least 2 dates for walk-forward analysis");
    }
    if n_windows < 1 {
        bail!("n_windows must be >= 1");
    }
    if !(0.1..=0.95).contains(&train_pct) {
        bail!("train_pct must be between 0.1 and 0.95");
    }

    let total = dates.len();
    let mut windows = Vec::new();

    match mode {
        WfMode::Rolling => {
            let test_size = total / (n_windows + 1).max(2);
            let train_size =
                ((test_size as f64) * train_pct / (1.0 - train_pct)).round() as usize;

            if train_size < 2 || test_size < 1 {
                bail!("Not enough data for {n_windows} windows with train_pct={train_pct}");
            }

            for i in 0..n_windows {
                let test_start_idx = train_size + i * test_size;
                if test_start_idx >= total {
                    break;
                }
                let test_end_idx = (test_start_idx + test_size).min(total) - 1;
                let train_start_idx = test_start_idx.saturating_sub(train_size);

                if train_start_idx >= test_start_idx {
                    break;
                }

                windows.push(WindowBounds {
                    train_start: dates[train_start_idx],
                    train_end: dates[test_start_idx - 1],
                    test_start: dates[test_start_idx],
                    test_end: dates[test_end_idx],
                });
            }
        }
        WfMode::Anchored => {
            let test_size = total / (n_windows + 1).max(2);
            let min_train = ((total as f64) * train_pct / (n_windows as f64 + 1.0)).round()
                as usize;

            if min_train < 2 || test_size < 1 {
                bail!("Not enough data for {n_windows} anchored windows");
            }

            for i in 0..n_windows {
                let test_start_idx = min_train + i * test_size;
                if test_start_idx >= total {
                    break;
                }
                let test_end_idx = (test_start_idx + test_size).min(total) - 1;

                windows.push(WindowBounds {
                    train_start: dates[0],
                    train_end: dates[test_start_idx - 1],
                    test_start: dates[test_start_idx],
                    test_end: dates[test_end_idx],
                });
            }
        }
    }

    if windows.is_empty() {
        bail!(
            "Could not construct any valid windows from {total} dates with n_windows={n_windows}"
        );
    }

    Ok(windows)
}

/// Generate all cartesian product combinations from a param grid.
pub fn cartesian_product(
    grid: &HashMap<String, Vec<serde_json::Value>>,
) -> Vec<HashMap<String, serde_json::Value>> {
    let keys: Vec<&String> = grid.keys().collect();
    if keys.is_empty() {
        return vec![HashMap::new()];
    }

    let mut combos = vec![HashMap::new()];

    for key in &keys {
        let values = &grid[*key];
        let mut new_combos = Vec::new();
        for combo in &combos {
            for val in values {
                let mut new_combo = combo.clone();
                new_combo.insert((*key).clone(), val.clone());
                new_combos.push(new_combo);
            }
        }
        combos = new_combos;
    }

    combos
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_dates(n: usize) -> Vec<NaiveDate> {
        (0..n)
            .map(|i| {
                NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()
                    + chrono::Duration::days(i as i64)
            })
            .collect()
    }

    #[test]
    fn test_rolling_windows() {
        let dates = make_dates(100);
        let windows = compute_windows(&dates, 3, 0.70, &WfMode::Rolling).unwrap();
        assert!(!windows.is_empty());
        assert!(windows.len() <= 3);

        for w in &windows {
            assert!(w.train_start < w.train_end);
            assert!(w.train_end < w.test_start);
            assert!(w.test_start <= w.test_end);
        }

        // Windows should not overlap in test periods
        for pair in windows.windows(2) {
            assert!(pair[0].test_end < pair[1].test_start);
        }
    }

    #[test]
    fn test_anchored_windows() {
        let dates = make_dates(100);
        let windows = compute_windows(&dates, 3, 0.70, &WfMode::Anchored).unwrap();
        assert!(!windows.is_empty());

        for w in &windows {
            assert_eq!(w.train_start, dates[0]);
            assert!(w.train_end < w.test_start);
        }

        if windows.len() >= 2 {
            assert!(windows[1].train_end >= windows[0].train_end);
        }
    }

    #[test]
    fn test_insufficient_data() {
        let dates = make_dates(1);
        assert!(compute_windows(&dates, 3, 0.70, &WfMode::Rolling).is_err());
    }

    #[test]
    fn test_cartesian_product() {
        let mut grid = HashMap::new();
        grid.insert(
            "a".to_string(),
            vec![serde_json::json!(1), serde_json::json!(2)],
        );
        grid.insert(
            "b".to_string(),
            vec![serde_json::json!("x"), serde_json::json!("y")],
        );

        let combos = super::cartesian_product(&grid);
        assert_eq!(combos.len(), 4);
        for c in &combos {
            assert!(c.contains_key("a"));
            assert!(c.contains_key("b"));
        }
    }

    #[test]
    fn test_cartesian_product_empty() {
        let grid = HashMap::new();
        let combos = super::cartesian_product(&grid);
        assert_eq!(combos.len(), 1);
        assert!(combos[0].is_empty());
    }
}
