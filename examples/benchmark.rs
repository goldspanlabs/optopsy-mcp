//! Benchmark: optopsy-mcp (Rust) — long_call evaluate on SPY data.

use std::time::Instant;

use polars::prelude::*;

use optopsy_mcp::data::parquet::normalize_quote_datetime;
use optopsy_mcp::engine::core::evaluate_strategy;
use optopsy_mcp::engine::types::{EvaluateParams, Slippage, TargetRange};

fn main() {
    let cache_path = shellexpand::tilde("~/.optopsy/cache/options/SPY.parquet").to_string();

    // --- Load data ---
    println!("Loading SPY options data...");
    let t0 = Instant::now();
    let df = LazyFrame::scan_parquet(cache_path.as_str().into(), ScanArgsParquet::default())
        .expect("Failed to scan parquet")
        .collect()
        .expect("Failed to collect parquet");
    let df = normalize_quote_datetime(df).expect("Failed to normalize dates");
    let t_load = t0.elapsed();
    let n_rows = df.height();
    println!("  Loaded {} rows in {:.3}s", n_rows, t_load.as_secs_f64());

    // --- Evaluate: long_call with matching params ---
    let params = EvaluateParams {
        strategy: "long_call".to_string(),
        leg_deltas: vec![TargetRange {
            target: 0.30,
            min: 0.20,
            max: 0.40,
        }],
        max_entry_dte: 45,
        exit_dte: 7,
        dte_interval: 7,
        delta_interval: 0.05,
        slippage: Slippage::Mid,
        commission: None,
        min_bid_ask: 0.05,
    };

    // Warm-up run
    println!("Warm-up run...");
    let _ = evaluate_strategy(&df, &params).expect("evaluate failed");

    // Timed runs
    let n_runs = 5;
    let mut times = Vec::new();
    println!("Running {} timed iterations...", n_runs);
    for i in 0..n_runs {
        let t0 = Instant::now();
        let result = evaluate_strategy(&df, &params).expect("evaluate failed");
        let elapsed = t0.elapsed().as_secs_f64();
        times.push(elapsed);
        if i == n_runs - 1 {
            println!("  Run {}: {:.3}s ({} groups)", i + 1, elapsed, result.len());
        } else {
            println!("  Run {}: {:.3}s", i + 1, elapsed);
        }
    }

    let avg: f64 = times.iter().sum::<f64>() / times.len() as f64;
    let best = times.iter().cloned().reduce(f64::min).unwrap();
    let worst = times.iter().cloned().reduce(f64::max).unwrap();

    println!("\n{}", "=".repeat(50));
    println!("optopsy-mcp (Rust) — long_call evaluate");
    println!("  Data rows : {}", n_rows);
    println!("  Load time : {:.3}s", t_load.as_secs_f64());
    println!("  Avg time  : {:.3}s", avg);
    println!("  Best time : {:.3}s", best);
    println!("  Worst time: {:.3}s", worst);
    println!("{}", "=".repeat(50));
}
