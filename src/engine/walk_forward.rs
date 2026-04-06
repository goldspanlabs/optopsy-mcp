//! Walk-forward optimization engine.
//!
//! Splits historical data into train/test windows, optimizes parameters
//! on each training window, and validates on the out-of-sample test window.

use std::collections::HashMap;

use anyhow::{bail, Result};
use chrono::NaiveDate;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// BTreeMap used for stable JSON serialization of param maps
use std::collections::BTreeMap;

use crate::engine::types::{BacktestResult, EquityPoint, PerformanceMetrics};
use crate::scripting::engine::{run_script_backtest, CancelCallback, DataLoader};

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
            // Derive sizes so that train_size + n_windows * test_size <= total.
            // Each window pair is (train_size, test_size) with train_pct = train / (train + test).
            // Solving: test_size = total * (1 - train_pct) / (train_pct + n_windows * (1 - train_pct))
            let denom = train_pct + (n_windows as f64) * (1.0 - train_pct);
            let test_size = ((total as f64) * (1.0 - train_pct) / denom).floor() as usize;
            let train_size = ((test_size as f64) * train_pct / (1.0 - train_pct)).round() as usize;

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
            let min_train =
                ((total as f64) * train_pct / (n_windows as f64 + 1.0)).round() as usize;

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
#[allow(clippy::implicit_hasher)]
pub fn cartesian_product(
    grid: &HashMap<String, Vec<serde_json::Value>>,
) -> Vec<HashMap<String, serde_json::Value>> {
    let keys: Vec<&String> = grid.keys().collect();
    if keys.is_empty() {
        return vec![HashMap::new()];
    }

    keys.iter().fold(vec![HashMap::new()], |combos, key| {
        let values = &grid[*key];
        combos
            .into_iter()
            .flat_map(|combo| {
                values.iter().map(move |val| {
                    let mut c = combo.clone();
                    c.insert((*key).clone(), val.clone());
                    c
                })
            })
            .collect()
    })
}

/// Parameters for walk-forward optimization.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct WalkForwardParams {
    pub strategy: String,
    pub symbol: String,
    pub capital: f64,
    pub params_grid: HashMap<String, Vec<Value>>,
    #[serde(default = "default_objective")]
    pub objective: WfObjective,
    #[serde(default = "default_n_windows")]
    pub n_windows: usize,
    #[serde(default = "default_mode")]
    pub mode: WfMode,
    #[serde(default = "default_train_pct")]
    pub train_pct: f64,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub profile: Option<String>,
    /// Pre-resolved script source; if None, reads from filesystem.
    #[serde(skip)]
    pub script_source: Option<String>,
    /// Base parameters (strategy-specific) to merge before each run.
    #[serde(default)]
    pub base_params: Option<HashMap<String, Value>>,
}

fn default_objective() -> WfObjective {
    WfObjective::Sharpe
}
fn default_n_windows() -> usize {
    5
}
fn default_mode() -> WfMode {
    WfMode::Rolling
}
fn default_train_pct() -> f64 {
    0.70
}

/// Result for a single walk-forward window.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WfWindowResult {
    pub window_idx: usize,
    pub train_start: String,
    pub train_end: String,
    pub test_start: String,
    pub test_end: String,
    pub best_params: HashMap<String, Value>,
    pub in_sample_metric: f64,
    pub out_of_sample_metric: f64,
}

/// Full walk-forward optimization response.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct WalkForwardResponse {
    pub windows: Vec<WfWindowResult>,
    pub stitched_equity: Vec<EquityPoint>,
    pub stitched_metrics: PerformanceMetrics,
    pub efficiency_ratio: f64,
    pub objective: String,
    pub mode: String,
    pub execution_time_ms: u64,
    pub profitable_windows: usize,
    pub total_windows: usize,
    pub param_stability: String,
}

/// Extract the target metric from a backtest result.
fn extract_metric(result: &BacktestResult, objective: &WfObjective) -> f64 {
    match objective {
        WfObjective::Sharpe => result.metrics.sharpe,
        WfObjective::Sortino => result.metrics.sortino,
        WfObjective::ProfitFactor => result.metrics.profit_factor,
        WfObjective::Cagr => result.metrics.cagr,
    }
}

/// Run walk-forward optimization.
#[allow(clippy::too_many_lines)]
pub async fn execute(
    params: WalkForwardParams,
    data_loader: &dyn DataLoader,
    is_cancelled: &CancelCallback,
    on_progress: impl Fn(usize, usize),
) -> Result<WalkForwardResponse> {
    let start = std::time::Instant::now();

    // Load script source (use pre-resolved source if available, else read from filesystem)
    let script_source = if let Some(src) = params.script_source {
        src
    } else {
        let trading_path = format!("scripts/strategies/{}.trading", params.strategy);
        let rhai_path = format!("scripts/strategies/{}.rhai", params.strategy);
        let script_path = if tokio::fs::metadata(&trading_path).await.is_ok() {
            trading_path
        } else {
            rhai_path
        };
        let raw = tokio::fs::read_to_string(&script_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read strategy script '{script_path}': {e}"))?;
        // Auto-transpile Trading DSL to Rhai
        if crate::scripting::dsl::is_trading_dsl(&raw) {
            crate::scripting::dsl::transpile(&raw)
                .map_err(|e| anyhow::anyhow!("DSL transpile error: {e}"))?
        } else {
            raw
        }
    };

    // Load OHLCV data to get the date range
    let start_date = params
        .start_date
        .as_deref()
        .and_then(|s| s.parse::<NaiveDate>().ok());
    let end_date = params
        .end_date
        .as_deref()
        .and_then(|s| s.parse::<NaiveDate>().ok());
    let ohlcv = data_loader
        .load_ohlcv(&params.symbol, start_date, end_date)
        .await?;

    // Extract unique sorted dates — intersect with options data range if the
    // strategy uses options, so walk-forward windows don't span dates with no
    // options chain data.
    let mut dates = crate::engine::ohlcv::extract_unique_dates(&ohlcv)?;

    // Check if script uses options by looking for "options: true" in the source
    let uses_options = script_source.contains("options: true");
    if uses_options {
        if let Ok(options_df) = data_loader
            .load_options(&params.symbol, start_date, end_date)
            .await
        {
            if let Ok(opt_dates) = crate::engine::ohlcv::extract_unique_dates(&options_df) {
                if let (Some(&opt_start), Some(&opt_end)) = (opt_dates.first(), opt_dates.last()) {
                    dates.retain(|d| *d >= opt_start && *d <= opt_end);
                    tracing::info!(
                        "Walk-forward: narrowed date range to options data ({} to {}, {} dates)",
                        opt_start,
                        opt_end,
                        dates.len()
                    );
                }
            }
        }
    }

    // Compute window boundaries
    let windows = compute_windows(&dates, params.n_windows, params.train_pct, &params.mode)?;

    // Generate param combos
    let combos = cartesian_product(&params.params_grid);
    if combos.is_empty() {
        bail!("params_grid produced no parameter combinations");
    }

    // Build base params: start from caller-provided params, then ensure symbol and CAPITAL.
    let mut base_params: HashMap<String, Value> = params.base_params.unwrap_or_default();
    base_params.insert("symbol".to_string(), serde_json::json!(params.symbol));
    base_params.insert("CAPITAL".to_string(), serde_json::json!(params.capital));

    // Profile merge if requested
    let base_params = if let Some(ref profile_name) = params.profile {
        use crate::scripting::stdlib::{
            load_profiles_registry, merge_profile_params, parse_script_meta,
        };
        let registry = load_profiles_registry();
        let meta = parse_script_meta(&params.strategy, &script_source);
        merge_profile_params(
            profile_name,
            &registry,
            meta.profiles.as_ref(),
            &base_params,
        )
    } else {
        base_params
    };

    let mut window_results = Vec::new();
    let mut all_oos_equity: Vec<EquityPoint> = Vec::new();
    let mut is_metrics_sum = 0.0_f64;
    let mut oos_metrics_sum = 0.0_f64;

    // Total steps: each window has (combos training runs + 1 OOS run)
    let steps_per_window = combos.len() + 1;
    let total_steps = windows.len() * steps_per_window;

    // Precomputed options data — captured from the first backtest run within
    // each window and reused for combos in that same window. Reset per window
    // because each window has different date bounds.
    let mut precomputed: Option<crate::scripting::engine::PrecomputedOptionsData>;

    for (idx, window) in windows.iter().enumerate() {
        if is_cancelled() {
            break;
        }

        let window_base = idx * steps_per_window;

        // Reset precomputed options for each window (date range changes)
        precomputed = None;

        // --- Training phase: sweep all combos ---
        let mut best_metric = f64::NEG_INFINITY;
        let mut best_params = combos[0].clone();

        for (combo_idx, combo) in combos.iter().enumerate() {
            if is_cancelled() {
                break;
            }
            let mut run_params = base_params.clone();
            run_params.extend(combo.clone());
            run_params.insert(
                "START_DATE".to_string(),
                serde_json::json!(window.train_start.to_string()),
            );
            run_params.insert(
                "END_DATE".to_string(),
                serde_json::json!(window.train_end.to_string()),
            );

            match run_script_backtest(
                &script_source,
                &run_params,
                data_loader,
                None,
                precomputed.as_ref(),
                Some(is_cancelled),
            )
            .await
            {
                Err(e) => {
                    tracing::warn!(combo = combo_idx, "Walk-forward training run failed: {e:#}");
                }
                Ok(result) => {
                    // Capture precomputed data from the first successful run
                    if precomputed.is_none() {
                        precomputed = result.precomputed_options;
                    }
                    let metric = extract_metric(&result.result, &params.objective);
                    if metric.is_finite() && metric > best_metric {
                        best_metric = metric;
                        best_params = combo.clone();
                    }
                }
            }

            on_progress(window_base + combo_idx + 1, total_steps);
        }

        let is_metric = best_metric;

        if is_cancelled() {
            break;
        }

        // --- Test phase: run best params on OOS window ---
        let mut oos_params = base_params.clone();
        oos_params.extend(best_params.clone());
        oos_params.insert(
            "START_DATE".to_string(),
            serde_json::json!(window.test_start.to_string()),
        );
        oos_params.insert(
            "END_DATE".to_string(),
            serde_json::json!(window.test_end.to_string()),
        );

        // Don't reuse precomputed options for OOS — the test window has different
        // dates and needs its own options data loaded fresh.
        let oos_result = run_script_backtest(
            &script_source,
            &oos_params,
            data_loader,
            None,
            None,
            Some(is_cancelled),
        )
        .await?;
        let oos_metric = extract_metric(&oos_result.result, &params.objective);

        all_oos_equity.extend(oos_result.result.equity_curve.clone());

        is_metrics_sum += if is_metric.is_finite() {
            is_metric
        } else {
            0.0
        };
        oos_metrics_sum += if oos_metric.is_finite() {
            oos_metric
        } else {
            0.0
        };

        window_results.push(WfWindowResult {
            window_idx: idx,
            train_start: window.train_start.to_string(),
            train_end: window.train_end.to_string(),
            test_start: window.test_start.to_string(),
            test_end: window.test_end.to_string(),
            best_params,
            in_sample_metric: is_metric,
            out_of_sample_metric: oos_metric,
        });

        on_progress(window_base + steps_per_window, total_steps);
    }

    let efficiency_ratio = if is_metrics_sum.abs() > f64::EPSILON {
        oos_metrics_sum / is_metrics_sum
    } else {
        0.0
    };

    let stitched_metrics = if all_oos_equity.is_empty() {
        PerformanceMetrics::default()
    } else {
        crate::engine::metrics::calculate_metrics(&all_oos_equity, &[], params.capital, 252.0)?
    };

    let objective_str = match params.objective {
        WfObjective::Sharpe => "sharpe",
        WfObjective::Sortino => "sortino",
        WfObjective::ProfitFactor => "profit_factor",
        WfObjective::Cagr => "cagr",
    };
    let mode_str = match params.mode {
        WfMode::Rolling => "rolling",
        WfMode::Anchored => "anchored",
    };

    // Compute new summary fields
    let profitable_windows = window_results
        .iter()
        .filter(|w| match objective_str {
            "profit_factor" => w.out_of_sample_metric > 1.0,
            _ => w.out_of_sample_metric > 0.0,
        })
        .count();
    let total_windows = window_results.len();

    let param_stability = {
        use std::collections::HashSet;
        let unique_params: HashSet<String> = window_results
            .iter()
            .map(|w| {
                let sorted: BTreeMap<_, _> = w.best_params.iter().collect();
                serde_json::to_string(&sorted).unwrap_or_default()
            })
            .collect();
        let n_unique = unique_params.len();
        let ratio = if total_windows > 0 {
            n_unique as f64 / total_windows as f64
        } else {
            0.0
        };
        if n_unique <= 1 {
            "stable".to_string()
        } else if ratio <= 0.5 {
            "moderate".to_string()
        } else {
            "unstable".to_string()
        }
    };

    Ok(WalkForwardResponse {
        windows: window_results,
        stitched_equity: all_oos_equity,
        stitched_metrics,
        efficiency_ratio,
        objective: objective_str.to_string(),
        mode: mode_str.to_string(),
        execution_time_ms: start.elapsed().as_millis() as u64,
        profitable_windows,
        total_windows,
        param_stability,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_dates(n: usize) -> Vec<NaiveDate> {
        (0..n)
            .map(|i| {
                NaiveDate::from_ymd_opt(2020, 1, 1).unwrap() + chrono::Duration::days(i as i64)
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

    #[test]
    fn test_empty_dates() {
        let dates: Vec<NaiveDate> = vec![];
        assert!(compute_windows(&dates, 3, 0.70, &WfMode::Rolling).is_err());
    }

    #[test]
    fn test_single_window_rolling() {
        let dates = make_dates(100);
        let windows = compute_windows(&dates, 1, 0.70, &WfMode::Rolling).unwrap();
        assert_eq!(windows.len(), 1);
        assert!(windows[0].train_start < windows[0].train_end);
        assert!(windows[0].train_end < windows[0].test_start);
    }

    #[test]
    fn test_single_window_anchored() {
        let dates = make_dates(100);
        let windows = compute_windows(&dates, 1, 0.70, &WfMode::Anchored).unwrap();
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].train_start, dates[0]);
    }

    #[test]
    fn test_extreme_train_pct_high() {
        let dates = make_dates(200);
        let windows = compute_windows(&dates, 3, 0.95, &WfMode::Rolling).unwrap();
        for w in &windows {
            assert!(w.train_start < w.train_end);
            assert!(w.train_end < w.test_start);
        }
    }

    #[test]
    fn test_extreme_train_pct_low() {
        let dates = make_dates(200);
        let windows = compute_windows(&dates, 3, 0.10, &WfMode::Rolling).unwrap();
        for w in &windows {
            assert!(w.train_start < w.train_end);
            assert!(w.train_end < w.test_start);
        }
    }

    #[test]
    fn test_train_pct_boundary_rejected() {
        let dates = make_dates(100);
        assert!(compute_windows(&dates, 3, 0.05, &WfMode::Rolling).is_err());
        assert!(compute_windows(&dates, 3, 0.99, &WfMode::Rolling).is_err());
    }

    #[test]
    fn test_many_windows_with_small_data() {
        let dates = make_dates(20);
        let windows =
            compute_windows(&dates, 10, 0.70, &WfMode::Rolling).expect("should produce windows");
        assert!(windows.len() <= 10);
        for w in &windows {
            assert!(w.train_end < w.test_start);
        }
    }

    #[test]
    fn test_rolling_windows_no_test_overlap() {
        let dates = make_dates(500);
        let windows = compute_windows(&dates, 5, 0.70, &WfMode::Rolling).unwrap();
        for (i, pair) in windows.windows(2).enumerate() {
            assert!(
                pair[0].test_end < pair[1].test_start,
                "Test periods must not overlap: window {i} ends {:?}, window {} starts {:?}",
                pair[0].test_end,
                i + 1,
                pair[1].test_start,
            );
        }
    }

    #[test]
    fn test_anchored_train_grows() {
        let dates = make_dates(500);
        let windows = compute_windows(&dates, 5, 0.70, &WfMode::Anchored).unwrap();
        for w in &windows {
            assert_eq!(w.train_start, dates[0]);
        }
        for pair in windows.windows(2) {
            assert!(pair[1].train_end >= pair[0].train_end);
        }
    }

    #[test]
    fn test_cartesian_product_single_param() {
        let mut grid = HashMap::new();
        grid.insert(
            "x".to_string(),
            vec![
                serde_json::json!(1),
                serde_json::json!(2),
                serde_json::json!(3),
            ],
        );
        let combos = cartesian_product(&grid);
        assert_eq!(combos.len(), 3);
    }

    #[test]
    fn test_cartesian_product_large_grid() {
        let mut grid = HashMap::new();
        grid.insert(
            "a".to_string(),
            vec![serde_json::json!(1), serde_json::json!(2)],
        );
        grid.insert(
            "b".to_string(),
            vec![serde_json::json!(3), serde_json::json!(4)],
        );
        grid.insert(
            "c".to_string(),
            vec![serde_json::json!(5), serde_json::json!(6)],
        );
        let combos = cartesian_product(&grid);
        assert_eq!(combos.len(), 8);
        for c in &combos {
            assert_eq!(c.len(), 3);
        }
    }
}
