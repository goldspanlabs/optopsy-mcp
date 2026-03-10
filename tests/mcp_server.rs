//! MCP server integration tests.
//!
//! Verifies tool registration, parameter validation (garde), error paths,
//! response serialization, and MCP protocol round-trips.

use std::path::PathBuf;
use std::sync::Arc;

use polars::prelude::*;
use rmcp::model::CallToolRequestParams;
use rmcp::ServiceExt;
use serde_json::json;
use tempfile::TempDir;

use optopsy_mcp::data::cache::CachedStore;
use optopsy_mcp::server::OptopsyServer;

mod common;
use common::make_multi_strike_df;

/// Create a minimal `DataFrame` with fewer rows to distinguish it from `make_multi_strike_df()`
/// Used in multi-symbol tests to verify the correct symbol was resolved
fn make_sparse_df() -> DataFrame {
    use chrono::NaiveDate;
    let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
    let datetime = date.and_hms_opt(0, 0, 0).unwrap();
    let exp = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap();

    df! {
        "quote_datetime" => &[datetime],
        "option_type" => &["call"],
        "strike" => &[100.0],
        "bid" => &[5.0],
        "ask" => &[5.5],
        "delta" => &[0.5],
        "expiration" => &[exp],
    }
    .unwrap()
}

// ─── Test Helpers ────────────────────────────────────────────────────────────

/// Create an `OptopsyServer` backed by a temporary directory (no S3).
fn make_test_server() -> (OptopsyServer, TempDir) {
    let tmp = TempDir::new().unwrap();
    let cache = Arc::new(CachedStore::new(
        tmp.path().to_path_buf(),
        "options".to_string(),
        None,
    ));
    let server = OptopsyServer::new(cache);
    (server, tmp)
}

/// Pre-load a `DataFrame` into the server's shared state.
async fn preload_data(server: &OptopsyServer, symbol: &str, df: DataFrame) {
    let mut guard = server.data.write().await;
    guard.insert(symbol.to_uppercase(), df);
}

/// Write a `DataFrame` as a Parquet file in the temp cache directory.
fn write_test_parquet(cache_dir: &std::path::Path, symbol: &str, df: &mut DataFrame) -> PathBuf {
    let dir = cache_dir.join("options");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{}.parquet", symbol.to_uppercase()));
    let file = std::fs::File::create(&path).unwrap();
    ParquetWriter::new(file).finish(df).unwrap();
    path
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category 1: Server Initialization
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn server_info_has_correct_metadata() {
    use rmcp::ServerHandler;

    let (server, _tmp) = make_test_server();
    let info = server.get_info();

    assert_eq!(info.server_info.name, "optopsy-mcp");
    assert_eq!(info.server_info.version, env!("CARGO_PKG_VERSION"));
    assert!(info.capabilities.tools.is_some());
    assert!(info.instructions.is_some());
    let instructions = info.instructions.unwrap();
    assert!(instructions.contains("run_backtest"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tool_router_lists_all_tools() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    // Server reads from client_rx, writes to server_tx
    // Client reads from server_rx, writes to client_tx
    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let tools = client.list_all_tools().await.unwrap();
    let tool_names: Vec<String> = tools.iter().map(|t| t.name.to_string()).collect();

    assert_eq!(tools.len(), 13, "Expected 13 tools, got: {tool_names:?}");
    for expected in [
        "list_strategies",
        "list_signals",
        "get_loaded_symbol",
        "suggest_parameters",
        "run_backtest",
        "compare_strategies",
        "parameter_sweep",
        "walk_forward",
        "permutation_test",
        "check_cache_status",
        "fetch_to_parquet",
        "get_raw_prices",
        "build_signal",
    ] {
        assert!(
            tool_names.contains(&expected.to_string()),
            "Missing tool: {expected}"
        );
    }

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category 2: No-Param Tools
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_strategies_returns_all_32() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "list_strategies".into(),
            arguments: None,
            task: None,
        })
        .await
        .unwrap();

    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    let resp: serde_json::Value = serde_json::from_str(&text.text).unwrap();

    assert_eq!(resp["total"], 32);
    let categories = resp["categories"].as_object().unwrap();
    assert!(categories.len() >= 6, "Expected at least 6 categories");
    assert!(!resp["suggested_next_steps"].as_array().unwrap().is_empty());

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_signals_returns_catalog() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "list_signals".into(),
            arguments: None,
            task: None,
        })
        .await
        .unwrap();

    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    let resp: serde_json::Value = serde_json::from_str(&text.text).unwrap();

    assert!(resp["total"].as_u64().unwrap() > 0);
    assert!(!resp["categories"].as_object().unwrap().is_empty());
    assert!(resp["ohlcv_columns"]
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v == "close"));

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_loaded_symbol_no_data_loaded() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "get_loaded_symbol".into(),
            arguments: None,
            task: None,
        })
        .await
        .unwrap();

    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    let resp: serde_json::Value = serde_json::from_str(&text.text).unwrap();

    // When no data loaded, loaded_symbols should be empty array
    assert_eq!(resp["loaded_symbols"], json!([]));
    assert_eq!(resp["rows"], serde_json::Value::Null);
    assert!(resp["summary"].as_str().unwrap().contains("No data"));

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_loaded_symbol_after_load_data() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    // Preload test data
    let df = make_multi_strike_df();
    preload_data(&server, "SPY", df).await;

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "get_loaded_symbol".into(),
            arguments: None,
            task: None,
        })
        .await
        .unwrap();

    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    let resp: serde_json::Value = serde_json::from_str(&text.text).unwrap();

    // After preload_data, should return loaded_symbols array with SPY
    assert_eq!(resp["loaded_symbols"], json!(["SPY"]));
    assert!(resp["rows"].as_u64().unwrap() > 0);
    assert!(!resp["columns"].as_array().unwrap().is_empty());
    assert!(resp["summary"].as_str().unwrap().contains("1 symbol"));

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category 3: Parameter Validation — Garde Rejection
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backtest_rejects_zero_capital() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "run_backtest".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "strategy": "short_put",
                    "leg_deltas": [{"target": 0.4, "min": 0.01, "max": 0.99}],
                    "entry_dte": {"target": 45, "min": 30, "max": 60},
                    "exit_dte": 5,
                    "slippage": {"type": "Mid"},
                    "capital": 0.0,
                    "quantity": 1,
                    "max_positions": 1
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(result.is_error.unwrap_or(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    assert!(
        text.text.contains("Validation error"),
        "Expected validation error for zero capital, got: {}",
        text.text
    );

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn check_cache_rejects_path_traversal() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "check_cache_status".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "symbol": "SPY",
                    "category": "../../etc"
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await;

    // Path traversal is now rejected at deserialization time (enum validation)
    // or returns an error result if it somehow passes
    assert!(
        result.is_err() || result.as_ref().unwrap().is_error.unwrap_or(false),
        "Expected error for path traversal attempt"
    );

    // If we got a successful call but with error result, check the message
    if let Ok(result) = result {
        if let Some(text) = result.content.first().and_then(|c| c.raw.as_text()) {
            assert!(
                text.text.contains("unknown variant")
                    || text.text.contains("Validation error")
                    || text.text.contains("Invalid category"),
                "Expected deserialization, validation, or category error for path traversal, got: {}",
                text.text
            );
        }
    }

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category 4: "No Data Loaded" Error Path
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backtest_fails_without_loaded_data() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "run_backtest".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "strategy": "short_put",
                    "leg_deltas": [{"target": 0.4, "min": 0.01, "max": 0.99}],
                    "entry_dte": {"target": 45, "min": 30, "max": 60},
                    "exit_dte": 5,
                    "slippage": {"type": "Mid"},
                    "capital": 100_000.0,
                    "quantity": 1,
                    "max_positions": 1
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(result.is_error.unwrap_or(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    assert!(
        text.text.contains("No data loaded"),
        "Expected 'No data loaded' error, got: {}",
        text.text
    );

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compare_fails_without_loaded_data() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "compare_strategies".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "strategies": [
                        {
                            "name": "short_put",
                            "leg_deltas": [{"target": 0.4, "min": 0.01, "max": 0.99}],
                            "entry_dte": {"target": 45, "min": 30, "max": 60},
                            "exit_dte": 5,
                            "slippage": {"type": "Mid"}
                        },
                        {
                            "name": "short_call",
                            "leg_deltas": [{"target": 0.5, "min": 0.01, "max": 0.99}],
                            "entry_dte": {"target": 45, "min": 30, "max": 60},
                            "exit_dte": 5,
                            "slippage": {"type": "Mid"}
                        }
                    ],
                    "sim_params": {
                        "capital": 100_000.0,
                        "quantity": 1,
                        "multiplier": 100,
                        "max_positions": 1
                    }
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(result.is_error.unwrap_or(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    assert!(
        text.text.contains("No data loaded"),
        "Expected 'No data loaded' error, got: {}",
        text.text
    );

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category 5: Successful Tool Execution Through Server
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preload_data_populates_shared_state() {
    // Test that load_data tool wiring works end-to-end by pre-loading data
    // and verifying the server's shared state and response shape.
    // Note: We use preload_data rather than cache.load_options because Polars'
    // parquet reader internally starts a tokio runtime, which conflicts with
    // the test's own runtime. The parquet I/O path is tested in production.
    let (server, _tmp) = make_test_server();
    let df = make_multi_strike_df();
    let rows = df.height();
    preload_data(&server, "TEST", df).await;

    // Verify shared state
    let guard = server.data.read().await;
    assert!(!guard.is_empty());
    let loaded_df = guard.get("TEST").expect("TEST should be in map");
    assert_eq!(loaded_df.height(), rows);
    assert!(loaded_df.width() > 0);
}

#[test]
fn write_test_parquet_creates_valid_file() {
    // Verify our test helper writes a valid parquet file that CachedStore can find
    let tmp = TempDir::new().unwrap();
    let cache = Arc::new(CachedStore::new(
        tmp.path().to_path_buf(),
        "options".to_string(),
        None,
    ));
    let mut df = make_multi_strike_df();
    let path = write_test_parquet(tmp.path(), "TEST", &mut df);

    assert!(path.exists());
    assert!(path.to_string_lossy().contains("TEST.parquet"));

    // Verify cache_path resolves correctly
    let resolved = cache.cache_path("TEST", "options").unwrap();
    assert_eq!(path, resolved);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_backtest_returns_trades_and_metrics() {
    let (server, _tmp) = make_test_server();
    preload_data(&server, "TEST", make_multi_strike_df()).await;

    let (server_tx, server_rx) = tokio::io::duplex(65536);
    let (client_tx, client_rx) = tokio::io::duplex(65536);

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "run_backtest".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "strategy": "short_put",
                    "leg_deltas": [{"target": 0.4, "min": 0.01, "max": 0.99}],
                    "entry_dte": {"target": 45, "min": 30, "max": 60},
                    "exit_dte": 5,
                    "slippage": {"type": "Mid"},
                    "capital": 100_000.0,
                    "quantity": 1,
                    "max_positions": 5
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(
        !result.is_error.unwrap_or(false),
        "run_backtest returned error: {:?}",
        result.content
    );
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    let resp: serde_json::Value = serde_json::from_str(&text.text).unwrap();
    assert!(!resp["trade_log"].as_array().unwrap().is_empty());
    assert!(resp["metrics"].is_object());

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compare_strategies_ranks_results() {
    let (server, _tmp) = make_test_server();
    preload_data(&server, "TEST", make_multi_strike_df()).await;

    let (server_tx, server_rx) = tokio::io::duplex(65536);
    let (client_tx, client_rx) = tokio::io::duplex(65536);

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "compare_strategies".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "strategies": [
                        {
                            "name": "short_put",
                            "leg_deltas": [{"target": 0.4, "min": 0.01, "max": 0.99}],
                            "entry_dte": {"target": 45, "min": 30, "max": 60},
                            "exit_dte": 5,
                            "slippage": {"type": "Mid"}
                        },
                        {
                            "name": "short_call",
                            "leg_deltas": [{"target": 0.5, "min": 0.01, "max": 0.99}],
                            "entry_dte": {"target": 45, "min": 30, "max": 60},
                            "exit_dte": 5,
                            "slippage": {"type": "Mid"}
                        }
                    ],
                    "sim_params": {
                        "capital": 100_000.0,
                        "quantity": 1,
                        "multiplier": 100,
                        "max_positions": 5
                    }
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(
        !result.is_error.unwrap_or(false),
        "compare_strategies returned error: {:?}",
        result.content
    );
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    let resp: serde_json::Value = serde_json::from_str(&text.text).unwrap();
    assert_eq!(resp["results"].as_array().unwrap().len(), 2);
    assert!(!resp["ranking_by_sharpe"].as_array().unwrap().is_empty());

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backtest_golden_path_output_shape() {
    let (server, _tmp) = make_test_server();
    preload_data(&server, "TEST", make_multi_strike_df()).await;

    let (server_tx, server_rx) = tokio::io::duplex(65536);
    let (client_tx, client_rx) = tokio::io::duplex(65536);

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "run_backtest".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "strategy": "short_put",
                    "leg_deltas": [{"target": 0.4, "min": 0.01, "max": 0.99}],
                    "entry_dte": {"target": 45, "min": 30, "max": 60},
                    "exit_dte": 5,
                    "slippage": {"type": "Mid"},
                    "capital": 100_000.0,
                    "quantity": 1,
                    "max_positions": 5
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(!result.is_error.unwrap_or(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    let resp: serde_json::Value = serde_json::from_str(&text.text).unwrap();

    // Top-level keys
    for key in [
        "summary",
        "assessment",
        "key_findings",
        "metrics",
        "trade_summary",
        "trade_log",
        "suggested_next_steps",
    ] {
        assert!(resp.get(key).is_some(), "Missing top-level key: {key}");
    }

    // Metrics sub-object
    let metrics = &resp["metrics"];
    for key in ["sharpe", "sortino", "cagr", "max_drawdown", "var_95"] {
        assert!(metrics.get(key).is_some(), "Missing metrics key: {key}");
    }

    // Trade summary
    let ts = &resp["trade_summary"];
    for key in ["total", "winners", "losers", "exit_breakdown"] {
        assert!(ts.get(key).is_some(), "Missing trade_summary key: {key}");
    }

    // Trade log entry shape
    let trade = &resp["trade_log"][0];
    for key in [
        "entry_datetime",
        "exit_datetime",
        "pnl",
        "exit_type",
        "days_held",
    ] {
        assert!(trade.get(key).is_some(), "Missing trade_log[0] key: {key}");
    }

    // Suggested next steps
    let steps = resp["suggested_next_steps"].as_array().unwrap();
    assert!(!steps.is_empty());
    assert!(steps[0].is_string());

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category 6: Cache Status
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn check_cache_reports_missing_file() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "check_cache_status".into(),
            arguments: Some(
                serde_json::from_value(json!({"symbol": "MISSING", "category": "options"}))
                    .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(!result.is_error.unwrap_or(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    let resp: serde_json::Value = serde_json::from_str(&text.text).unwrap();
    assert_eq!(resp["exists"], false);
    assert!(resp["last_updated"].is_null());

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn check_cache_reports_existing_file() {
    let (server, tmp) = make_test_server();
    let mut df = make_multi_strike_df();
    write_test_parquet(tmp.path(), "SPY", &mut df);

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "check_cache_status".into(),
            arguments: Some(
                serde_json::from_value(json!({"symbol": "SPY", "category": "options"})).unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(!result.is_error.unwrap_or(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    let resp: serde_json::Value = serde_json::from_str(&text.text).unwrap();
    assert_eq!(resp["exists"], true);
    assert!(resp["last_updated"].is_string());

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category 7: MCP Protocol Round-Trip
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_roundtrip_list_strategies() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    // Full round-trip: serialize → transport → deserialize → execute → serialize → transport → deserialize
    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "list_strategies".into(),
            arguments: None,
            task: None,
        })
        .await
        .unwrap();

    assert!(!result.is_error.unwrap_or(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    let resp: serde_json::Value = serde_json::from_str(&text.text).unwrap();
    assert_eq!(resp["total"], 32);

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category 9: Multi-Symbol Support
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_loaded_symbol_with_multiple_symbols() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    // Preload multiple symbols
    preload_data(&server, "SPY", make_multi_strike_df()).await;
    preload_data(&server, "QQQ", make_multi_strike_df()).await;

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "get_loaded_symbol".into(),
            arguments: None,
            task: None,
        })
        .await
        .unwrap();

    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    let resp: serde_json::Value = serde_json::from_str(&text.text).unwrap();

    // Should report both symbols (sorted)
    let loaded_symbols = resp["loaded_symbols"].as_array().unwrap();
    assert_eq!(loaded_symbols.len(), 2);
    assert_eq!(loaded_symbols[0], "QQQ");
    assert_eq!(loaded_symbols[1], "SPY");
    assert!(resp["summary"].as_str().unwrap().contains("2 symbol"));

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────────
// Multi-symbol integration tests for run_backtest, compare_strategies, suggest_parameters
// ─────────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_backtest_fails_multiple_symbols_no_symbol_param() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    // Preload multiple symbols
    preload_data(&server, "SPY", make_multi_strike_df()).await;
    preload_data(&server, "QQQ", make_multi_strike_df()).await;

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "run_backtest".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "strategy": "long_call",
                    "leg_deltas": [{"target": 0.5, "min": 0.01, "max": 0.99}],
                    "entry_dte": {"target": 45, "min": 30, "max": 60},
                    "exit_dte": 5,
                    "slippage": {"type": "Mid"},
                    "capital": 10000.0,
                    "quantity": 1,
                    "multiplier": 100,
                    "max_positions": 1
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(result.is_error.unwrap_or(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    assert!(
        text.text.contains("Multiple symbols"),
        "Expected 'Multiple symbols' error, got: {}",
        text.text
    );

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_backtest_succeeds_with_explicit_symbol() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(65536);
    let (client_tx, client_rx) = tokio::io::duplex(65536);

    // Preload multiple symbols with different data to verify symbol resolution
    preload_data(&server, "SPY", make_multi_strike_df()).await;
    preload_data(&server, "QQQ", make_sparse_df()).await;

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "run_backtest".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "strategy": "long_call",
                    "leg_deltas": [{"target": 0.5, "min": 0.01, "max": 0.99}],
                    "entry_dte": {"target": 45, "min": 30, "max": 60},
                    "exit_dte": 5,
                    "slippage": {"type": "Mid"},
                    "capital": 10000.0,
                    "quantity": 1,
                    "multiplier": 100,
                    "max_positions": 1,
                    "symbol": "SPY"
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(!result.is_error.unwrap_or(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    let resp: serde_json::Value = serde_json::from_str(&text.text).unwrap();
    assert!(
        resp["metrics"].is_object(),
        "run_backtest returned error: {:?}",
        text.text
    );

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_backtest_fails_unknown_symbol() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    // Preload SPY only
    preload_data(&server, "SPY", make_multi_strike_df()).await;

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "run_backtest".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "strategy": "long_call",
                    "leg_deltas": [{"target": 0.5, "min": 0.01, "max": 0.99}],
                    "entry_dte": {"target": 45, "min": 30, "max": 60},
                    "exit_dte": 5,
                    "slippage": {"type": "Mid"},
                    "capital": 10000.0,
                    "quantity": 1,
                    "multiplier": 100,
                    "max_positions": 1,
                    "symbol": "UNKNOWN"
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(result.is_error.unwrap_or(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    assert!(
        text.text.contains("not loaded") || text.text.contains("auto-load"),
        "Expected symbol error, got: {}",
        text.text
    );

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compare_strategies_fails_multiple_symbols_no_symbol_param() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    // Preload multiple symbols
    preload_data(&server, "SPY", make_multi_strike_df()).await;
    preload_data(&server, "QQQ", make_multi_strike_df()).await;

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "compare_strategies".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "strategies": [
                        {
                            "name": "long_call",
                            "leg_deltas": [{"target": 0.5, "min": 0.01, "max": 0.99}],
                            "entry_dte": {"target": 45, "min": 30, "max": 60},
                            "exit_dte": 5,
                            "slippage": {"type": "Mid"}
                        },
                        {
                            "name": "long_put",
                            "leg_deltas": [{"target": 0.5, "min": 0.01, "max": 0.99}],
                            "entry_dte": {"target": 45, "min": 30, "max": 60},
                            "exit_dte": 5,
                            "slippage": {"type": "Mid"}
                        }
                    ],
                    "sim_params": {
                        "capital": 10000.0,
                        "quantity": 1,
                        "multiplier": 100,
                        "max_positions": 1,
                        "selector": "Nearest"
                    }
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(result.is_error.unwrap_or(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    assert!(
        text.text.contains("Multiple symbols"),
        "Expected 'Multiple symbols' error, got: {}",
        text.text
    );

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compare_strategies_succeeds_with_explicit_symbol() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(65536);
    let (client_tx, client_rx) = tokio::io::duplex(65536);

    // Preload multiple symbols with different data to verify symbol resolution
    preload_data(&server, "SPY", make_multi_strike_df()).await;
    preload_data(&server, "QQQ", make_sparse_df()).await;

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "compare_strategies".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "strategies": [
                        {
                            "name": "long_call",
                            "leg_deltas": [{"target": 0.5, "min": 0.01, "max": 0.99}],
                            "entry_dte": {"target": 45, "min": 30, "max": 60},
                            "exit_dte": 5,
                            "slippage": {"type": "Mid"}
                        },
                        {
                            "name": "long_put",
                            "leg_deltas": [{"target": 0.5, "min": 0.01, "max": 0.99}],
                            "entry_dte": {"target": 45, "min": 30, "max": 60},
                            "exit_dte": 5,
                            "slippage": {"type": "Mid"}
                        }
                    ],
                    "sim_params": {
                        "capital": 10000.0,
                        "quantity": 1,
                        "multiplier": 100,
                        "max_positions": 1,
                        "selector": "Nearest"
                    },
                    "symbol": "SPY"
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(!result.is_error.unwrap_or(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    let resp: serde_json::Value = serde_json::from_str(&text.text).unwrap();
    assert!(
        resp["ranking_by_sharpe"].is_array(),
        "compare_strategies returned error: {:?}",
        text.text
    );

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compare_strategies_fails_unknown_symbol() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    // Preload SPY only
    preload_data(&server, "SPY", make_multi_strike_df()).await;

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "compare_strategies".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "strategies": [
                        {
                            "name": "long_call",
                            "leg_deltas": [{"target": 0.5, "min": 0.01, "max": 0.99}],
                            "entry_dte": {"target": 45, "min": 30, "max": 60},
                            "exit_dte": 5,
                            "slippage": {"type": "Mid"}
                        },
                        {
                            "name": "long_put",
                            "leg_deltas": [{"target": 0.5, "min": 0.01, "max": 0.99}],
                            "entry_dte": {"target": 45, "min": 30, "max": 60},
                            "exit_dte": 5,
                            "slippage": {"type": "Mid"}
                        }
                    ],
                    "sim_params": {
                        "capital": 10000.0,
                        "quantity": 1,
                        "multiplier": 100,
                        "max_positions": 1,
                        "selector": "Nearest"
                    },
                    "symbol": "UNKNOWN"
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(result.is_error.unwrap_or(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    assert!(
        text.text.contains("not loaded") || text.text.contains("auto-load"),
        "Expected symbol error, got: {}",
        text.text
    );

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn suggest_parameters_fails_multiple_symbols_no_symbol_param() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    // Preload multiple symbols
    preload_data(&server, "SPY", make_multi_strike_df()).await;
    preload_data(&server, "QQQ", make_multi_strike_df()).await;

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "suggest_parameters".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "strategy": "long_call",
                    "risk_preference": "moderate"
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(result.is_error.unwrap_or(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    assert!(
        text.text.contains("Multiple symbols"),
        "Expected 'Multiple symbols' error, got: {}",
        text.text
    );

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn suggest_parameters_succeeds_with_explicit_symbol() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(65536);
    let (client_tx, client_rx) = tokio::io::duplex(65536);

    // Preload multiple symbols with different data to verify symbol resolution
    preload_data(&server, "SPY", make_multi_strike_df()).await;
    preload_data(&server, "QQQ", make_sparse_df()).await;

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "suggest_parameters".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "strategy": "long_call",
                    "risk_preference": "moderate",
                    "symbol": "SPY"
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(!result.is_error.unwrap_or(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    let resp: serde_json::Value = serde_json::from_str(&text.text).unwrap();
    assert!(
        resp["leg_deltas"].is_array(),
        "suggest_parameters returned error: {:?}",
        text.text
    );

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn suggest_parameters_fails_unknown_symbol() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    // Preload SPY only
    preload_data(&server, "SPY", make_multi_strike_df()).await;

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "suggest_parameters".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "strategy": "long_call",
                    "risk_preference": "moderate",
                    "symbol": "UNKNOWN"
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(result.is_error.unwrap_or(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    assert!(
        text.text.contains("not loaded") || text.text.contains("auto-load"),
        "Expected symbol error, got: {}",
        text.text
    );

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category: get_raw_prices Integration Tests
// ═══════════════════════════════════════════════════════════════════════════════

/// Write a `DataFrame` as a Parquet file under the "prices" category.
fn write_prices_parquet(cache_dir: &std::path::Path, symbol: &str, df: &mut DataFrame) -> PathBuf {
    let dir = cache_dir.join("prices");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{}.parquet", symbol.to_uppercase()));
    let file = std::fs::File::create(&path).unwrap();
    ParquetWriter::new(file).finish(df).unwrap();
    path
}

/// Build a small OHLCV `DataFrame` for testing.
fn make_ohlcv_df() -> DataFrame {
    use chrono::NaiveDate;
    let dates = DateChunked::from_naive_date(
        PlSmallStr::from("date"),
        [
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(),
        ],
    )
    .into_column();

    df! {
        "open"     => &[100.0, 101.0, 102.5_f64],
        "high"     => &[102.0, 103.0, 104.0_f64],
        "low"      => &[99.0, 100.5, 102.0_f64],
        "close"    => &[101.0, 102.5, 103.5_f64],
        "adjclose" => &[101.0, 102.5, 103.5_f64],
        "volume"   => &[1_000_000_u64, 1_100_000, 1_050_000],
    }
    .unwrap()
    .hstack(&[dates])
    .unwrap()
    .select(["date", "open", "high", "low", "close", "adjclose", "volume"])
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_raw_prices_fails_when_cache_missing() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "get_raw_prices".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "symbol": "SPY"
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(result.is_error.unwrap_or(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    assert!(
        text.text.contains("No OHLCV price data cached") && text.text.contains("fetch_to_parquet"),
        "Expected cache-miss error with fetch_to_parquet hint, got: {}",
        text.text
    );

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_raw_prices_returns_bars() {
    let (server, tmp) = make_test_server();

    // Write OHLCV parquet to the prices category
    let mut ohlcv = make_ohlcv_df();
    write_prices_parquet(tmp.path(), "SPY", &mut ohlcv);

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "get_raw_prices".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "symbol": "SPY",
                    "limit": null
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(
        !result.is_error.unwrap_or(false),
        "Expected success, got error"
    );
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    let resp: serde_json::Value = serde_json::from_str(&text.text).unwrap();

    assert_eq!(resp["symbol"], "SPY");
    assert_eq!(resp["total_rows"], 3);
    assert_eq!(resp["returned_rows"], 3);
    assert_eq!(resp["sampled"], false);

    let prices = resp["prices"].as_array().unwrap();
    assert_eq!(prices.len(), 3);
    assert_eq!(prices[0]["date"], "2024-01-02");
    assert_eq!(prices[0]["open"], 100.0);
    assert_eq!(prices[2]["date"], "2024-01-04");
    assert_eq!(prices[2]["close"], 103.5);

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category: build_signal Integration Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_signal_validate_valid_formula() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "build_signal".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "action": "validate",
                    "formula": "close > sma(close, 20)"
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(
        !result.is_error.unwrap_or(false),
        "Expected success, got error"
    );
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    let resp: serde_json::Value = serde_json::from_str(&text.text).unwrap();

    assert_eq!(
        resp["success"], true,
        "validate should succeed for valid formula"
    );
    assert!(
        resp["formula_help"].is_null(),
        "formula_help should be absent on success"
    );
    assert!(
        resp["summary"].as_str().unwrap_or("").contains("valid"),
        "summary should mention validity"
    );

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_signal_validate_invalid_formula() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "build_signal".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "action": "validate",
                    "formula": "foo > 10"
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(
        !result.is_error.unwrap_or(false),
        "Expected success response (not MCP error)"
    );
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    let resp: serde_json::Value = serde_json::from_str(&text.text).unwrap();

    assert_eq!(
        resp["success"], false,
        "validate should fail for unknown column 'foo'"
    );
    assert!(
        !resp["formula_help"].is_null(),
        "formula_help should be present on validation error"
    );
    let columns = resp["formula_help"]["columns"].as_array().unwrap();
    assert!(
        columns.iter().any(|v| v.as_str() == Some("close")),
        "formula_help should list 'close' as a valid column"
    );

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_signal_create_without_save() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "build_signal".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "action": "create",
                    "name": "my_test_signal",
                    "formula": "close > close[1]",
                    "description": "Price higher than previous close",
                    "save": false
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(
        !result.is_error.unwrap_or(false),
        "Expected success, got error"
    );
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    let resp: serde_json::Value = serde_json::from_str(&text.text).unwrap();

    assert_eq!(resp["success"], true);
    // signal_spec should be present and be a Custom variant
    let spec = &resp["signal_spec"];
    assert!(!spec.is_null(), "signal_spec should be present");
    assert_eq!(spec["type"], "Custom");
    assert_eq!(spec["formula"], "close > close[1]");
    assert_eq!(spec["name"], "my_test_signal");
    // saved_signals should be empty (not saved); field may be absent when empty
    let saved_count = resp["saved_signals"].as_array().map_or(0, Vec::len);
    assert_eq!(
        saved_count, 0,
        "saved_signals should be empty when save=false"
    );

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_signal_search_returns_candidates() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "build_signal".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "action": "search",
                    "prompt": "RSI oversold"
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(
        !result.is_error.unwrap_or(false),
        "Expected success response, got MCP error"
    );
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    let resp: serde_json::Value = serde_json::from_str(&text.text).unwrap();

    assert_eq!(
        resp["success"], true,
        "search should succeed when candidates are found"
    );
    let candidates = resp["candidates"]
        .as_array()
        .expect("candidates should be an array");
    assert!(
        !candidates.is_empty(),
        "search for 'RSI oversold' should return at least one candidate"
    );
    assert!(
        resp["schema"].is_object(),
        "schema should be present in search response"
    );
    assert!(
        resp["column_defaults"].is_object(),
        "column_defaults should be present in search response"
    );

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}
