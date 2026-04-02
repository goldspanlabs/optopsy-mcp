//! Benchmark: grid sweep of wheel strategy on SPY, sweeping `CALL_DTE` 7..28.
//!
//! Run with: `cargo test --release --test sweep_benchmark -- --nocapture --ignored`
//!
//! This test is `#[ignore]` by default because it requires real SPY data in
//! `data/options/SPY.parquet` and takes 30-120 seconds depending on hardware.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use serde_json::{json, Value};

use optopsy_mcp::data::cache::CachedStore;
use optopsy_mcp::engine::sweep::{run_grid_sweep, GridSweepConfig};
use optopsy_mcp::scripting::engine::{CachingDataLoader, CancelCallback, DataLoader};

fn wheel_base_params() -> HashMap<String, Value> {
    let mut params = HashMap::new();
    params.insert("SYMBOL".to_string(), json!("SPY"));
    params.insert("CAPITAL".to_string(), json!(100_000));
    params.insert("PUT_DELTA".to_string(), json!(0.30));
    params.insert("PUT_DTE".to_string(), json!(45));
    params.insert("CALL_DELTA".to_string(), json!(0.30));
    params.insert("CALL_DTE".to_string(), json!(30));
    params.insert("SLIPPAGE".to_string(), json!("mid"));
    params.insert("MULTIPLIER".to_string(), json!(100));
    params
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires real SPY data"]
async fn sweep_wheel_call_dte_benchmark() {
    let data_dir = PathBuf::from("data");

    // Verify SPY data exists
    let spy_path = data_dir.join("options").join("SPY.parquet");
    assert!(
        spy_path.exists(),
        "SPY options data not found at {spy_path:?}"
    );

    let script_source =
        std::fs::read_to_string("scripts/strategies/wheel.rhai").expect("wheel.rhai not found");

    let cache = Arc::new(CachedStore::new(data_dir, "options".to_string()));
    let loader: Arc<dyn DataLoader> = Arc::new(CachingDataLoader::new(cache, None));
    let is_cancelled: CancelCallback = Box::new(|| false);

    // Sweep CALL_DTE from 7 to 28 in steps of 1 (22 combinations)
    let call_dte_values: Vec<Value> = (7..=28).map(|v| json!(v)).collect();
    let n_combos = call_dte_values.len();

    let mut param_grid = HashMap::new();
    param_grid.insert("CALL_DTE".to_string(), call_dte_values);

    let config = GridSweepConfig {
        script_source,
        base_params: wheel_base_params(),
        param_grid,
        objective: "sharpe".to_string(),
    };

    let sep = "=".repeat(70);
    println!("\n{sep}");
    println!("  Wheel strategy sweep: CALL_DTE 7..28 ({n_combos} combinations)");
    println!("  Symbol: SPY | Objective: sharpe");
    println!("{sep}");

    let wall_start = Instant::now();

    let on_progress = |done: usize, total: usize| {
        if done.is_multiple_of(5) || done == total - 1 {
            println!("  Progress: {}/{total}", done + 1);
        }
    };

    let result = run_grid_sweep(&config, Arc::clone(&loader), &is_cancelled, on_progress)
        .await
        .expect("sweep failed");

    let wall_ms = wall_start.elapsed().as_millis();

    println!("\n--- Results ---");
    println!("  Combinations run:    {}", result.combinations_run);
    println!("  Combinations failed: {}", result.combinations_failed);
    println!(
        "  execution_time_ms:   {} (internal timer)",
        result.execution_time_ms
    );
    println!("  Wall clock:          {wall_ms} ms");
    println!("  Avg per combo:       {} ms", wall_ms / n_combos as u128);

    if let Some(best) = &result.best_result {
        println!("\n  Best result (rank #{}):", best.rank);
        println!("    CALL_DTE:       {:?}", best.params.get("CALL_DTE"));
        println!("    Sharpe:         {:.4}", best.sharpe);
        println!("    P&L:            ${:.2}", best.pnl);
        println!("    Trades:         {}", best.trades);
        println!("    Win rate:       {:.1}%", best.win_rate * 100.0);
        println!("    Max drawdown:   {:.2}%", best.max_drawdown * 100.0);
    }

    println!("\n  All ranked results:");
    for r in &result.ranked_results {
        println!(
            "    #{:>2} CALL_DTE={:>2}  sharpe={:>7.4}  pnl=${:>10.2}  trades={:>3}  win={:.0}%",
            r.rank,
            r.params
                .get("CALL_DTE")
                .and_then(Value::as_i64)
                .unwrap_or(0),
            r.sharpe,
            r.pnl,
            r.trades,
            r.win_rate * 100.0
        );
    }

    println!("\n{sep}");
    println!(
        "  TOTAL WALL TIME: {wall_ms} ms ({:.1} s)",
        wall_ms as f64 / 1000.0
    );
    println!("{sep}\n");

    // Basic assertions
    assert_eq!(result.combinations_total, n_combos);
    assert!(result.combinations_run > 0, "No successful runs");
    assert!(result.best_result.is_some(), "No best result");
}
