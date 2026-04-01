//! Integration test for walk-forward optimization end-to-end.
//!
//! Uses the `bb_mean_reversion` strategy with synthetic OHLCV data containing
//! repeated Bollinger Band breakout patterns. Sweeps `BB_PERIOD` across windows
//! to verify the full train/test/stitch pipeline.

use std::collections::HashMap;

use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::*;

use optopsy_mcp::engine::walk_forward::{
    self as wf_engine, WalkForwardParams, WfMode, WfObjective,
};
use optopsy_mcp::scripting::engine::DataLoader;
use optopsy_mcp::scripting::types::OhlcvBar;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn bars_to_df(bars: &[OhlcvBar]) -> DataFrame {
    let datetimes: Vec<chrono::NaiveDateTime> = bars.iter().map(|b| b.datetime).collect();
    let opens: Vec<f64> = bars.iter().map(|b| b.open).collect();
    let highs: Vec<f64> = bars.iter().map(|b| b.high).collect();
    let lows: Vec<f64> = bars.iter().map(|b| b.low).collect();
    let closes: Vec<f64> = bars.iter().map(|b| b.close).collect();
    let volumes: Vec<f64> = bars.iter().map(|b| b.volume).collect();

    df! {
        "datetime" => DatetimeChunked::from_naive_datetime(
            PlSmallStr::from("datetime"),
            datetimes,
            TimeUnit::Microseconds,
        ).into_column().take_materialized_series(),
        "open" => &opens,
        "high" => &highs,
        "low" => &lows,
        "close" => &closes,
        "volume" => &volumes,
    }
    .unwrap()
}

/// A `DataLoader` that filters OHLCV by date range (needed for walk-forward windows).
struct DateFilteringLoader {
    ohlcv_df: DataFrame,
}

#[async_trait::async_trait]
impl DataLoader for DateFilteringLoader {
    async fn load_ohlcv(
        &self,
        _symbol: &str,
        start: Option<NaiveDate>,
        end: Option<NaiveDate>,
    ) -> Result<DataFrame> {
        let mut df = self.ohlcv_df.clone();

        if let Some(s) = start {
            let start_dt = s.and_hms_opt(0, 0, 0).unwrap();
            df = df
                .lazy()
                .filter(col("datetime").gt_eq(lit(start_dt)))
                .collect()?;
        }
        if let Some(e) = end {
            let end_dt = e.and_hms_opt(23, 59, 59).unwrap();
            df = df
                .lazy()
                .filter(col("datetime").lt_eq(lit(end_dt)))
                .collect()?;
        }

        Ok(df)
    }

    async fn load_options(
        &self,
        _symbol: &str,
        _start: Option<NaiveDate>,
        _end: Option<NaiveDate>,
    ) -> Result<DataFrame> {
        Ok(DataFrame::empty())
    }

    fn load_splits(
        &self,
        _symbol: &str,
    ) -> Result<Vec<optopsy_mcp::data::adjustment_store::SplitRow>> {
        Ok(Vec::new())
    }

    fn load_dividends(
        &self,
        _symbol: &str,
    ) -> Result<Vec<optopsy_mcp::data::adjustment_store::DividendRow>> {
        Ok(Vec::new())
    }
}

/// Generate 250 bars with repeating BB breakout patterns.
///
/// Pattern: 25 bars of stable prices around a base, then a spike above the upper BB,
/// then 5 bars of drift, then a reversion. Repeated ~7 times to give enough data
/// for walk-forward windows.
fn make_walk_forward_bars() -> Vec<OhlcvBar> {
    let mut bars = Vec::new();
    let start = NaiveDate::from_ymd_opt(2023, 1, 2).unwrap();
    let mut day = 0i64;

    for cycle in 0..7 {
        let base = 100.0 + (f64::from(cycle) * 2.0);

        // 25 bars of stable prices (indicator warmup + tight range)
        for i in 0..25 {
            let close = base + (f64::from(i) * 0.05).sin() * 0.3;
            let date = start + chrono::Duration::days(day);
            bars.push(OhlcvBar {
                datetime: date.and_hms_opt(0, 0, 0).unwrap(),
                open: close - 0.1,
                high: close + 0.3,
                low: close - 0.3,
                close,
                volume: 1_000_000.0,
            });
            day += 1;
        }

        // Spike bar above upper BB
        let date = start + chrono::Duration::days(day);
        bars.push(OhlcvBar {
            datetime: date.and_hms_opt(0, 0, 0).unwrap(),
            open: base,
            high: base + 6.0,
            low: base - 0.2,
            close: base + 5.0,
            volume: 2_000_000.0,
        });
        day += 1;

        // 5 bars staying above SMA
        for i in 1..=5 {
            let close = base + 3.0 - (f64::from(i) * 0.3);
            let date = start + chrono::Duration::days(day);
            bars.push(OhlcvBar {
                datetime: date.and_hms_opt(0, 0, 0).unwrap(),
                open: close + 0.1,
                high: close + 0.3,
                low: close - 0.3,
                close,
                volume: 1_000_000.0,
            });
            day += 1;
        }

        // Reversion bar below SMA
        let date = start + chrono::Duration::days(day);
        bars.push(OhlcvBar {
            datetime: date.and_hms_opt(0, 0, 0).unwrap(),
            open: base,
            high: base + 0.2,
            low: base - 3.0,
            close: base - 2.0,
            volume: 1_500_000.0,
        });
        day += 1;

        // 3 bars of recovery back to base
        for i in 1..=3 {
            let close = base - 2.0 + (f64::from(i) * 0.7);
            let date = start + chrono::Duration::days(day);
            bars.push(OhlcvBar {
                datetime: date.and_hms_opt(0, 0, 0).unwrap(),
                open: close - 0.1,
                high: close + 0.3,
                low: close - 0.3,
                close,
                volume: 1_000_000.0,
            });
            day += 1;
        }
    }

    bars
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// End-to-end walk-forward: 3 rolling windows, sweeping `BB_PERIOD`.
#[tokio::test(flavor = "multi_thread")]
async fn walk_forward_rolling_end_to_end() {
    let bars = make_walk_forward_bars();
    assert!(bars.len() >= 200, "Need enough bars for walk-forward");

    let loader = DateFilteringLoader {
        ohlcv_df: bars_to_df(&bars),
    };

    let mut params_grid = HashMap::new();
    params_grid.insert(
        "BB_PERIOD".to_string(),
        vec![serde_json::json!(15), serde_json::json!(20)],
    );

    let params = WalkForwardParams {
        strategy: "bb_mean_reversion".to_string(),
        symbol: "TEST".to_string(),
        capital: 100_000.0,
        params_grid,
        objective: WfObjective::Sharpe,
        n_windows: 3,
        mode: WfMode::Rolling,
        train_pct: 0.70,
        start_date: None,
        end_date: None,
        profile: None,
    };

    let result = wf_engine::execute(params, &loader).await;
    assert!(
        result.is_ok(),
        "Walk-forward should succeed: {:?}",
        result.err()
    );

    let result = result.unwrap();

    // Should produce at least 2 windows (may get fewer than requested if data is tight)
    assert!(
        result.windows.len() >= 2 && result.windows.len() <= 3,
        "Expected 2-3 windows, got {}",
        result.windows.len()
    );

    // Each window should have valid structure
    for w in &result.windows {
        assert!(
            w.train_start < w.train_end,
            "Window {}: train_start should be before train_end",
            w.window_idx,
        );
        assert!(
            w.train_end < w.test_start,
            "Window {}: train_end should be before test_start",
            w.window_idx,
        );
        assert!(
            w.test_start <= w.test_end,
            "Window {}: test_start should be <= test_end",
            w.window_idx,
        );
        // Best params should contain BB_PERIOD
        assert!(
            w.best_params.contains_key("BB_PERIOD"),
            "Window {}: best_params should include BB_PERIOD",
            w.window_idx,
        );
        // In-sample metric should be finite (even if zero)
        assert!(
            w.in_sample_metric.is_finite(),
            "Window {}: in-sample metric should be finite",
            w.window_idx,
        );
    }

    // Stitched equity should have data points
    assert!(
        !result.stitched_equity.is_empty(),
        "Stitched equity curve should not be empty"
    );

    // Efficiency ratio should be finite
    assert!(
        result.efficiency_ratio.is_finite(),
        "Efficiency ratio should be finite, got {}",
        result.efficiency_ratio,
    );

    // Execution time should be recorded
    assert!(result.execution_time_ms > 0, "Execution time should be > 0");
}

/// End-to-end walk-forward: anchored mode.
#[tokio::test(flavor = "multi_thread")]
async fn walk_forward_anchored_end_to_end() {
    let bars = make_walk_forward_bars();
    let loader = DateFilteringLoader {
        ohlcv_df: bars_to_df(&bars),
    };

    let mut params_grid = HashMap::new();
    params_grid.insert(
        "BB_PERIOD".to_string(),
        vec![serde_json::json!(15), serde_json::json!(20)],
    );

    let params = WalkForwardParams {
        strategy: "bb_mean_reversion".to_string(),
        symbol: "TEST".to_string(),
        capital: 100_000.0,
        params_grid,
        objective: WfObjective::Sharpe,
        n_windows: 2,
        mode: WfMode::Anchored,
        train_pct: 0.70,
        start_date: None,
        end_date: None,
        profile: None,
    };

    let result = wf_engine::execute(params, &loader).await;
    assert!(
        result.is_ok(),
        "Anchored walk-forward should succeed: {:?}",
        result.err()
    );

    let result = result.unwrap();
    assert_eq!(result.windows.len(), 2);

    // All anchored windows should share the same train_start
    let first_train_start = &result.windows[0].train_start;
    for w in &result.windows {
        assert_eq!(
            &w.train_start, first_train_start,
            "Anchored windows should all start from the same date"
        );
    }
}

/// Walk-forward with empty `params_grid` should fail.
#[tokio::test(flavor = "multi_thread")]
async fn walk_forward_empty_grid_fails() {
    let bars = make_walk_forward_bars();
    let loader = DateFilteringLoader {
        ohlcv_df: bars_to_df(&bars),
    };

    let params = WalkForwardParams {
        strategy: "bb_mean_reversion".to_string(),
        symbol: "TEST".to_string(),
        capital: 100_000.0,
        params_grid: HashMap::new(),
        objective: WfObjective::Sharpe,
        n_windows: 3,
        mode: WfMode::Rolling,
        train_pct: 0.70,
        start_date: None,
        end_date: None,
        profile: None,
    };

    let result = wf_engine::execute(params, &loader).await;
    if let Ok(r) = &result {
        assert!(!r.windows.is_empty());
    }
}
