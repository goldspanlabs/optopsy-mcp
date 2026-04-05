//! Sequential benchmark: runs wheel backtests in a loop (compatible with pre-optimization code).
//!
//! Run with: `cargo test --release --test sweep_benchmark_compat -- --nocapture --ignored`

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use serde_json::{json, Value};

use optopsy_mcp::data::cache::CachedStore;
use optopsy_mcp::scripting::engine::{
    run_script_backtest, CachingDataLoader, CancelCallback, PrecomputedOptionsData,
};

fn wheel_base_params() -> HashMap<String, Value> {
    let mut params = HashMap::new();
    params.insert("symbol".to_string(), json!("SPY"));
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
#[allow(clippy::too_many_lines)]
async fn sequential_wheel_sweep_benchmark() {
    let data_dir = PathBuf::from("data");
    let spy_path = data_dir.join("options").join("SPY.parquet");
    assert!(
        spy_path.exists(),
        "SPY options data not found at {spy_path:?}"
    );

    let trading_source = std::fs::read_to_string("scripts/strategies/wheel.trading")
        .expect("wheel.trading not found");
    let script_source = optopsy_mcp::scripting::dsl::transpile(&trading_source)
        .expect("wheel.trading should transpile");

    let cache = Arc::new(CachedStore::new(data_dir, "options".to_string()));
    let loader = CachingDataLoader::new(cache, None);
    let is_cancelled: CancelCallback = Box::new(|| false);

    let call_dte_values: Vec<i64> = (7..=28).collect();
    let n_combos = call_dte_values.len();

    let sep = "=".repeat(70);
    println!("\n{sep}");
    println!("  SEQUENTIAL wheel sweep: CALL_DTE 7..28 ({n_combos} combinations)");
    println!("  Symbol: SPY | No parallelization (apples-to-apples comparison)");
    println!("{sep}");

    let mut precomputed: Option<PrecomputedOptionsData> = None;
    let mut combo_times: Vec<u128> = Vec::new();
    let wall_start = Instant::now();

    for (idx, dte) in call_dte_values.iter().enumerate() {
        let mut params = wheel_base_params();
        params.insert("CALL_DTE".to_string(), json!(dte));

        let combo_start = Instant::now();
        let bt = run_script_backtest(
            &script_source,
            &params,
            &loader,
            None,
            precomputed.as_ref(),
            Some(&is_cancelled),
        )
        .await
        .unwrap_or_else(|e| panic!("CALL_DTE={dte} failed: {e}"));

        let combo_ms = combo_start.elapsed().as_millis();
        combo_times.push(combo_ms);

        if precomputed.is_none() {
            precomputed.clone_from(&bt.precomputed_options);
        }

        let m = &bt.result.metrics;
        println!(
            "  [{:>2}/{}] CALL_DTE={:>2}  sharpe={:>7.4}  pnl=${:>10.2}  trades={:>3}  time={combo_ms}ms",
            idx + 1,
            n_combos,
            dte,
            m.sharpe,
            bt.result.total_pnl,
            bt.result.trade_count,
        );
    }

    let wall_ms = wall_start.elapsed().as_millis();
    let first_ms = combo_times[0];
    let rest_avg = if combo_times.len() > 1 {
        combo_times[1..].iter().sum::<u128>() / (combo_times.len() - 1) as u128
    } else {
        0
    };

    println!("\n{sep}");
    println!(
        "  SEQUENTIAL TOTAL: {wall_ms} ms ({:.1} s)",
        wall_ms as f64 / 1000.0
    );
    println!("  First combo (cold): {first_ms} ms (includes data load + price table build)");
    println!("  Subsequent avg:     {rest_avg} ms (precomputed options reused)");
    println!("  Combos run:         {n_combos}");
    println!("{sep}\n");
}
