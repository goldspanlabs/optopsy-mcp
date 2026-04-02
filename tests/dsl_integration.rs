//! Integration tests proving that `.trading` DSL files transpile into valid Rhai
//! that the engine can compile, configure, and execute.

use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::*;

use optopsy_mcp::scripting::dsl;
use optopsy_mcp::scripting::engine::{run_script_backtest, DataLoader};
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

struct TestDataLoader {
    ohlcv_df: DataFrame,
}

#[async_trait::async_trait]
impl DataLoader for TestDataLoader {
    async fn load_ohlcv(
        &self,
        _symbol: &str,
        _start: Option<NaiveDate>,
        _end: Option<NaiveDate>,
    ) -> Result<DataFrame> {
        Ok(self.ohlcv_df.clone())
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
        Ok(vec![])
    }

    fn load_dividends(
        &self,
        _symbol: &str,
    ) -> Result<Vec<optopsy_mcp::data::adjustment_store::DividendRow>> {
        Ok(vec![])
    }
}

fn default_params() -> std::collections::HashMap<String, serde_json::Value> {
    let mut params = std::collections::HashMap::new();
    params.insert("SYMBOL".to_string(), serde_json::json!("SPY"));
    params.insert("CAPITAL".to_string(), serde_json::json!(100_000));
    params
}

/// Build an engine and register the `extern()` overloads with param injection.
fn build_test_engine(
    params: &std::collections::HashMap<String, serde_json::Value>,
) -> rhai::Engine {
    let mut engine = optopsy_mcp::scripting::registration::build_engine();
    let p3 = params.clone();
    engine.register_fn(
        "extern",
        move |name: &str, default: rhai::Dynamic, _desc: &str| -> rhai::Dynamic {
            if let Some(value) = p3.get(name) {
                optopsy_mcp::scripting::stdlib::json_to_dynamic(value)
            } else {
                default
            }
        },
    );
    let p4 = params.clone();
    engine.register_fn(
        "extern",
        move |name: &str,
              default: rhai::Dynamic,
              _desc: &str,
              _opts: rhai::Array|
              -> rhai::Dynamic {
            if let Some(value) = p4.get(name) {
                optopsy_mcp::scripting::stdlib::json_to_dynamic(value)
            } else {
                default
            }
        },
    );
    engine
}

/// Transpile a `.trading` file, compile with the engine, and call `config()`.
/// Returns the config map for further assertions.
fn transpile_compile_and_configure(
    trading_path: &str,
    params: &std::collections::HashMap<String, serde_json::Value>,
) -> rhai::Map {
    // 1. Read the .trading file
    let trading_source =
        std::fs::read_to_string(trading_path).unwrap_or_else(|e| panic!("{trading_path}: {e}"));

    // 2. Verify it's detected as DSL
    assert!(
        dsl::is_trading_dsl(&trading_source),
        "{trading_path} should be detected as Trading DSL"
    );

    // 3. Transpile to Rhai
    let rhai_source = dsl::transpile(&trading_source)
        .unwrap_or_else(|e| panic!("{trading_path} should transpile without errors: {e}"));

    // 4. Compile
    let engine = build_test_engine(params);
    let ast = engine.compile(&rhai_source).unwrap_or_else(|e| {
        panic!(
            "{trading_path} generated Rhai should compile.\nError: {e}\n\nGenerated Rhai:\n{rhai_source}"
        )
    });

    // 5. Initialize scope and evaluate top-level (extern calls, state vars)
    let mut scope = rhai::Scope::new();
    optopsy_mcp::scripting::stdlib::inject_params_map(&mut scope, params);
    let _ = engine
        .eval_ast_with_scope::<rhai::Dynamic>(&mut scope, &ast)
        .unwrap_or_else(|e| panic!("{trading_path} top-level eval failed: {e}"));

    // 6. Call config()
    let options = rhai::CallFnOptions::new()
        .eval_ast(false)
        .rewind_scope(false);
    let config: rhai::Dynamic = engine
        .call_fn_with_options(options, &mut scope, &ast, "config", ())
        .unwrap_or_else(|e| panic!("{trading_path} config() failed: {e}"));

    config.cast::<rhai::Map>()
}

/// Generate 250 bars with a clear uptrend (for SMA crossover).
///
/// Phase 1 (bars 0-209): steady climb from 100 to 200 (crosses above SMA200)
/// Phase 2 (bars 210-239): continues up to 230
/// Phase 3 (bars 240-249): drops sharply to 150 (crosses below SMA200)
fn make_uptrend_bars() -> Vec<OhlcvBar> {
    let mut bars = Vec::new();
    let base_date = NaiveDate::from_ymd_opt(2023, 1, 2).unwrap();

    for i in 0..250_i32 {
        let close = if i < 210 {
            // Steady climb: 100 → 200
            100.0 + f64::from(i) * (100.0 / 210.0)
        } else if i < 240 {
            // Continue up: 200 → 230
            200.0 + f64::from(i - 210)
        } else {
            // Drop: 230 → 150
            230.0 - f64::from(i - 240) * 8.0
        };

        let date = base_date + chrono::Duration::days(i64::from(i));
        bars.push(OhlcvBar {
            datetime: date.and_hms_opt(0, 0, 0).unwrap(),
            open: close - 0.5,
            high: close + 1.0,
            low: close - 1.0,
            close,
            volume: 1_000_000.0,
        });
    }

    bars
}

// ---------------------------------------------------------------------------
// Test: SMA Crossover (.trading) — compile + config
// ---------------------------------------------------------------------------

#[test]
fn dsl_sma_crossover_compiles_and_configures() {
    let params = default_params();
    let config =
        transpile_compile_and_configure("scripts/strategies/sma_crossover.trading", &params);

    // Verify config fields
    assert_eq!(
        config
            .get("symbol")
            .unwrap()
            .clone()
            .into_immutable_string()
            .unwrap()
            .as_str(),
        "SPY"
    );
    assert_eq!(
        config
            .get("interval")
            .unwrap()
            .clone()
            .into_immutable_string()
            .unwrap()
            .as_str(),
        "daily"
    );

    // Verify data block
    let data = config.get("data").unwrap().clone().cast::<rhai::Map>();
    assert!(data.get("ohlcv").unwrap().as_bool().unwrap());

    // Verify indicators
    let indicators = data
        .get("indicators")
        .unwrap()
        .clone()
        .cast::<rhai::Array>();
    let ind_strs: Vec<String> = indicators
        .iter()
        .map(|d| d.clone().into_immutable_string().unwrap().to_string())
        .collect();
    assert!(ind_strs.contains(&"sma:50".to_string()));
    assert!(ind_strs.contains(&"sma:200".to_string()));
    assert!(ind_strs.contains(&"rsi:14".to_string()));
}

// ---------------------------------------------------------------------------
// Test: SMA Crossover (.trading) — full backtest
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn dsl_sma_crossover_runs_backtest() {
    let bars = make_uptrend_bars();
    let loader = TestDataLoader {
        ohlcv_df: bars_to_df(&bars),
    };

    // Transpile
    let trading_source =
        std::fs::read_to_string("scripts/strategies/sma_crossover.trading").unwrap();
    let rhai_source = dsl::transpile(&trading_source).unwrap();

    let params = default_params();
    let result = run_script_backtest(&rhai_source, &params, &loader, None, None, None).await;
    assert!(
        result.is_ok(),
        "SMA crossover backtest should succeed: {:?}",
        result.err()
    );

    let result = result.unwrap();

    // With 250 bars of uptrend then crash, we should get at least 1 trade
    assert!(
        result.result.trade_count >= 1,
        "Expected at least 1 trade, got {}. Warnings: {:?}",
        result.result.trade_count,
        result.result.warnings,
    );

    // Verify trade log has entries
    assert!(
        !result.result.trade_log.is_empty(),
        "Trade log should not be empty"
    );
}

// ---------------------------------------------------------------------------
// Test: Iron Condor Income (.trading) — compile + config
// ---------------------------------------------------------------------------

#[test]
fn dsl_iron_condor_income_compiles_and_configures() {
    let params = default_params();
    let config =
        transpile_compile_and_configure("scripts/strategies/iron_condor_income.trading", &params);

    // Options strategy must request options data
    let data = config.get("data").unwrap().clone().cast::<rhai::Map>();
    assert!(data.get("ohlcv").unwrap().as_bool().unwrap());
    assert!(data.get("options").unwrap().as_bool().unwrap());

    // Engine settings
    let engine_cfg = config.get("engine").unwrap().clone().cast::<rhai::Map>();
    assert_eq!(
        engine_cfg
            .get("slippage")
            .unwrap()
            .clone()
            .into_immutable_string()
            .unwrap()
            .as_str(),
        "mid"
    );
    assert_eq!(
        engine_cfg
            .get("expiration_filter")
            .unwrap()
            .clone()
            .into_immutable_string()
            .unwrap()
            .as_str(),
        "monthly"
    );

    // Defaults
    let defaults = config.get("defaults").unwrap().clone().cast::<rhai::Map>();
    assert_eq!(defaults.get("max_positions").unwrap().as_int().unwrap(), 1);
}

// ---------------------------------------------------------------------------
// Test: Mean Reversion Pairs (.trading) — compile + config
// ---------------------------------------------------------------------------

#[test]
fn dsl_mean_reversion_pairs_compiles_and_configures() {
    let params = default_params();
    let config =
        transpile_compile_and_configure("scripts/strategies/mean_reversion_pairs.trading", &params);

    // Should have cross_symbols inside the data block
    let data = config
        .get("data")
        .expect("data should be in config")
        .clone()
        .cast::<rhai::Map>();
    let cross = data
        .get("cross_symbols")
        .expect("cross_symbols should be in config.data")
        .clone()
        .cast::<rhai::Array>();
    let syms: Vec<String> = cross
        .iter()
        .map(|d| d.clone().into_immutable_string().unwrap().to_string())
        .collect();
    assert!(syms.contains(&"QQQ".to_string()));
}

// ---------------------------------------------------------------------------
// Test: transpiled Rhai matches hand-written equivalence
// ---------------------------------------------------------------------------

/// Verify that the transpiler output contains all expected Rhai callbacks.
#[test]
fn dsl_transpiled_has_all_callbacks() {
    let trading_source =
        std::fs::read_to_string("scripts/strategies/sma_crossover.trading").unwrap();
    let rhai_source = dsl::transpile(&trading_source).unwrap();

    // Must have all the callbacks the engine expects
    assert!(rhai_source.contains("fn config()"), "missing config()");
    assert!(rhai_source.contains("fn on_bar(ctx)"), "missing on_bar()");
    assert!(
        rhai_source.contains("fn on_exit_check(ctx, pos)"),
        "missing on_exit_check()"
    );
    assert!(
        rhai_source.contains("fn on_position_closed(ctx, pos, exit_type)"),
        "missing on_position_closed()"
    );
}

/// Verify the iron condor transpile produces valid options-specific Rhai.
#[test]
fn dsl_iron_condor_transpile_has_strategy_call() {
    let trading_source =
        std::fs::read_to_string("scripts/strategies/iron_condor_income.trading").unwrap();
    let rhai_source = dsl::transpile(&trading_source).unwrap();

    // Strategy call should be ctx-qualified
    assert!(
        rhai_source.contains("ctx.iron_condor("),
        "iron_condor call should be ctx-qualified in generated Rhai"
    );

    // Spread should be null-checked before pushing to actions
    assert!(
        rhai_source.contains("if __spread != ()"),
        "strategy open should null-check the spread"
    );
}

// ---------------------------------------------------------------------------
// Test: DSL detection works correctly
// ---------------------------------------------------------------------------

#[test]
fn dsl_detection_on_real_files() {
    // .trading files should be detected as DSL
    for entry in std::fs::read_dir("scripts/strategies").unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "trading") {
            let source = std::fs::read_to_string(&path).unwrap();
            assert!(
                dsl::is_trading_dsl(&source),
                "{} should be detected as Trading DSL",
                path.display()
            );
        }
    }

    // .rhai files should NOT be detected as DSL
    for entry in std::fs::read_dir("scripts/strategies").unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "rhai") {
            let source = std::fs::read_to_string(&path).unwrap();
            assert!(
                !dsl::is_trading_dsl(&source),
                "{} should NOT be detected as Trading DSL",
                path.display()
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Test: All .rhai strategy scripts compile with DSL custom syntax
// ---------------------------------------------------------------------------

#[test]
fn all_rhai_strategies_compile_with_dsl_syntax() {
    let params = default_params();
    let engine = build_test_engine(&params);

    let mut rhai_files: Vec<std::path::PathBuf> = std::fs::read_dir("scripts/strategies")
        .expect("scripts/strategies directory should exist")
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "rhai") {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    rhai_files.sort();

    assert!(
        !rhai_files.is_empty(),
        "Expected at least one .rhai file in scripts/strategies/, found: {}",
        rhai_files.len()
    );

    for path in &rhai_files {
        let source = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("Failed to read {}: {e}", path.display()));

        engine.compile(&source).unwrap_or_else(|e| {
            panic!(
                "{} should compile successfully with build_engine().\nError: {e}",
                path.display()
            )
        });
    }
}
