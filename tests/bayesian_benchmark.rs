//! Benchmark: Bayesian sweep of wheel strategy on SPY, sweeping `CALL_DTE` 7..28.
//!
//! Run with: `cargo test --release --test bayesian_benchmark -- --nocapture --ignored`
//!
//! This test is `#[ignore]` by default because it requires real SPY data.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use serde_json::{json, Value};

use optopsy_mcp::data::cache::CachedStore;
use optopsy_mcp::engine::bayesian::{run_bayesian, BayesianConfig};
use optopsy_mcp::scripting::engine::{CachingDataLoader, CancelCallback};

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
async fn bayesian_wheel_call_dte_benchmark() {
    let data_dir = PathBuf::from("data");
    let spy_path = data_dir.join("options").join("SPY.parquet");
    assert!(
        spy_path.exists(),
        "SPY options data not found at {spy_path:?}"
    );

    let script_source =
        std::fs::read_to_string("scripts/strategies/wheel.rhai").expect("wheel.rhai not found");

    let cache = Arc::new(CachedStore::new(data_dir, "options".to_string()));
    let loader = CachingDataLoader::new(cache, None);
    let is_cancelled: CancelCallback = Box::new(|| false);

    let max_evaluations = 50;
    let initial_samples = 15;

    // 2D sweep: CALL_DELTA + PUT_DELTA, float, step=0.01
    let continuous_params = vec![
        ("CALL_DELTA".to_string(), 0.15, 0.45, false, Some(0.01)),
        ("PUT_DELTA".to_string(), 0.15, 0.45, false, Some(0.01)),
    ];

    let config = BayesianConfig {
        script_source,
        base_params: wheel_base_params(),
        continuous_params,
        max_evaluations,
        initial_samples,
        objective: "sharpe".to_string(),
    };

    let sep = "=".repeat(70);
    println!("\n{sep}");
    println!(
        "  Bayesian sweep: CALL_DELTA + PUT_DELTA 0.15..0.45 ({max_evaluations} evals, {initial_samples} initial)"
    );
    println!("  Symbol: SPY | Objective: sharpe | Step: 0.01");
    println!("{sep}");

    let wall_start = Instant::now();

    let on_progress = |done: usize, total: usize| {
        if done.is_multiple_of(5) || done == total - 1 {
            let elapsed = wall_start.elapsed().as_secs();
            println!("  Progress: {}/{total}  ({elapsed}s elapsed)", done + 1);
        }
    };

    let result = run_bayesian(&config, &loader, &is_cancelled, on_progress)
        .await
        .expect("bayesian sweep failed");

    let wall_ms = wall_start.elapsed().as_millis();

    println!("\n--- Results ---");
    println!("  Evaluations run:     {}", result.combinations_run);
    println!("  Evaluations failed:  {}", result.combinations_failed);
    println!(
        "  execution_time_ms:   {} (internal timer)",
        result.execution_time_ms
    );
    println!("  Wall clock:          {wall_ms} ms");
    println!(
        "  Avg per eval:        {} ms",
        wall_ms / max_evaluations as u128
    );

    if let Some(best) = &result.best_result {
        println!("\n  Best result (rank #{}):", best.rank);
        for (k, v) in &best.params {
            println!("    {k}: {v}");
        }
        println!("    Sharpe:         {:.4}", best.sharpe);
        println!("    P&L:            ${:.2}", best.pnl);
        println!("    Trades:         {}", best.trades);
        println!("    Win rate:       {:.1}%", best.win_rate * 100.0);
        println!("    Max drawdown:   {:.2}%", best.max_drawdown * 100.0);
    }

    if let Some(trace) = &result.convergence_trace {
        println!("\n  Convergence trace (best sharpe over iterations):");
        for (i, val) in trace.iter().enumerate() {
            if i % 5 == 0 || i == trace.len() - 1 {
                println!("    iter {i:>2}: {val:.4}");
            }
        }
    }

    println!("\n  All ranked results:");
    for r in &result.ranked_results {
        let cd = r
            .params
            .get("CALL_DELTA")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let pd = r
            .params
            .get("PUT_DELTA")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        println!(
            "    #{:>2} call_d={:.2} put_d={:.2}  sharpe={:>7.4}  pnl=${:>10.2}  trades={:>3}",
            r.rank, cd, pd, r.sharpe, r.pnl, r.trades,
        );
    }

    println!("\n{sep}");
    println!(
        "  TOTAL WALL TIME: {wall_ms} ms ({:.1} s)",
        wall_ms as f64 / 1000.0
    );
    println!("{sep}\n");

    assert!(result.combinations_run > 0);
    assert!(result.best_result.is_some());
}
