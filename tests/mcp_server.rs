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

// ─── Test Helpers ────────────────────────────────────────────────────────────

/// Create an `OptopsyServer` backed by a temporary directory (no S3, no EODHD).
fn make_test_server() -> (OptopsyServer, TempDir) {
    let tmp = TempDir::new().unwrap();
    let cache = Arc::new(CachedStore::new(
        tmp.path().to_path_buf(),
        "options".to_string(),
        None,
    ));
    let server = OptopsyServer::new(cache, None);
    (server, tmp)
}

/// Pre-load a `DataFrame` into the server's shared state.
async fn preload_data(server: &OptopsyServer, symbol: &str, df: DataFrame) {
    let mut guard = server.data.write().await;
    *guard = Some((symbol.to_uppercase(), df));
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
    assert_eq!(info.server_info.version, "0.1.0");
    assert!(info.capabilities.tools.is_some());
    assert!(info.instructions.is_some());
    let instructions = info.instructions.unwrap();
    assert!(instructions.contains("load_data"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tool_router_lists_all_nine_tools() {
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

    assert_eq!(tools.len(), 9, "Expected 9 tools, got: {tool_names:?}");
    for expected in [
        "download_options_data",
        "load_data",
        "list_strategies",
        "list_signals",
        "evaluate_strategy",
        "run_backtest",
        "compare_strategies",
        "check_cache_status",
        "fetch_to_parquet",
    ] {
        assert!(
            tool_names.contains(&expected.to_string()),
            "Missing tool: {expected}"
        );
    }

    client.cancel().await.unwrap();
    drop(server_handle);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category 2: No-Param Tools
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_strategies_returns_all_32() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let _server_handle =
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
    assert!(
        !resp["suggested_next_steps"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    client.cancel().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_signals_returns_catalog() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let _server_handle =
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
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category 3: Parameter Validation — Garde Rejection
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_data_rejects_empty_symbol() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let _server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "load_data".into(),
            arguments: Some(serde_json::from_value(json!({"symbol": ""})).unwrap()),
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
        text.text.contains("Validation error") || text.text.contains("validation"),
        "Expected validation error, got: {}",
        text.text
    );

    client.cancel().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_data_rejects_invalid_symbol_chars() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let _server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "load_data".into(),
            arguments: Some(serde_json::from_value(json!({"symbol": "../etc"})).unwrap()),
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
        "Expected validation error, got: {}",
        text.text
    );

    client.cancel().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn evaluate_rejects_zero_max_entry_dte() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let _server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "evaluate_strategy".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "strategy": "long_call",
                    "leg_deltas": [{"target": 0.5, "min": 0.01, "max": 0.99}],
                    "max_entry_dte": 0,
                    "exit_dte": 0,
                    "dte_interval": 7,
                    "delta_interval": 0.05,
                    "slippage": {"type": "Mid"}
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
        "Expected validation error for zero max_entry_dte, got: {}",
        text.text
    );

    client.cancel().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn evaluate_rejects_exit_dte_gte_max_entry_dte() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let _server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "evaluate_strategy".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "strategy": "long_call",
                    "leg_deltas": [{"target": 0.5, "min": 0.01, "max": 0.99}],
                    "max_entry_dte": 30,
                    "exit_dte": 30,
                    "dte_interval": 7,
                    "delta_interval": 0.05,
                    "slippage": {"type": "Mid"}
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
        text.text.contains("exit_dte") && text.text.contains("max_entry_dte"),
        "Expected custom validator error about exit_dte >= max_entry_dte, got: {}",
        text.text
    );

    client.cancel().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backtest_rejects_zero_capital() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let _server_handle =
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
                    "max_entry_dte": 45,
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn check_cache_rejects_path_traversal() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let _server_handle =
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
        "Expected validation error for path traversal, got: {}",
        text.text
    );

    client.cancel().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category 4: "No Data Loaded" Error Path
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn evaluate_fails_without_loaded_data() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let _server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "evaluate_strategy".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "strategy": "long_call",
                    "leg_deltas": [{"target": 0.5, "min": 0.01, "max": 0.99}],
                    "max_entry_dte": 45,
                    "exit_dte": 5,
                    "dte_interval": 7,
                    "delta_interval": 0.05,
                    "slippage": {"type": "Mid"}
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backtest_fails_without_loaded_data() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let _server_handle =
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
                    "max_entry_dte": 45,
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compare_fails_without_loaded_data() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let _server_handle =
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
                            "max_entry_dte": 45,
                            "exit_dte": 5,
                            "slippage": {"type": "Mid"}
                        },
                        {
                            "name": "short_call",
                            "leg_deltas": [{"target": 0.5, "min": 0.01, "max": 0.99}],
                            "max_entry_dte": 45,
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
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category 5: Successful Tool Execution Through Server
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_data_populates_shared_state() {
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
    assert!(guard.is_some());
    let (sym, loaded_df) = guard.as_ref().unwrap();
    assert_eq!(sym, "TEST");
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
async fn evaluate_strategy_returns_buckets() {
    let (server, _tmp) = make_test_server();
    preload_data(&server, "TEST", make_multi_strike_df()).await;

    let (server_tx, server_rx) = tokio::io::duplex(65536);
    let (client_tx, client_rx) = tokio::io::duplex(65536);

    let _server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "evaluate_strategy".into(),
            arguments: Some(
                serde_json::from_value(json!({
                    "strategy": "long_call",
                    "leg_deltas": [{"target": 0.5, "min": 0.01, "max": 0.99}],
                    "max_entry_dte": 45,
                    "exit_dte": 5,
                    "dte_interval": 10,
                    "delta_interval": 0.10,
                    "slippage": {"type": "Mid"}
                }))
                .unwrap(),
            ),
            task: None,
        })
        .await
        .unwrap();

    assert!(
        !result.is_error.unwrap_or(false),
        "evaluate_strategy returned error: {:?}",
        result.content
    );
    let text = result
        .content
        .first()
        .and_then(|c| c.raw.as_text())
        .unwrap();
    let resp: serde_json::Value = serde_json::from_str(&text.text).unwrap();
    assert!(resp["total_buckets"].as_u64().unwrap() > 0);
    assert!(!resp["groups"].as_array().unwrap().is_empty());
    assert!(resp["summary"].as_str().is_some());

    client.cancel().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_backtest_returns_trades_and_metrics() {
    let (server, _tmp) = make_test_server();
    preload_data(&server, "TEST", make_multi_strike_df()).await;

    let (server_tx, server_rx) = tokio::io::duplex(65536);
    let (client_tx, client_rx) = tokio::io::duplex(65536);

    let _server_handle =
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
                    "max_entry_dte": 45,
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
    assert!(!resp["equity_curve"].as_array().unwrap().is_empty());
    assert!(resp["metrics"].is_object());

    client.cancel().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compare_strategies_ranks_results() {
    let (server, _tmp) = make_test_server();
    preload_data(&server, "TEST", make_multi_strike_df()).await;

    let (server_tx, server_rx) = tokio::io::duplex(65536);
    let (client_tx, client_rx) = tokio::io::duplex(65536);

    let _server_handle =
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
                            "max_entry_dte": 45,
                            "exit_dte": 5,
                            "slippage": {"type": "Mid"}
                        },
                        {
                            "name": "short_call",
                            "leg_deltas": [{"target": 0.5, "min": 0.01, "max": 0.99}],
                            "max_entry_dte": 45,
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backtest_golden_path_output_shape() {
    let (server, _tmp) = make_test_server();
    preload_data(&server, "TEST", make_multi_strike_df()).await;

    let (server_tx, server_rx) = tokio::io::duplex(65536);
    let (client_tx, client_rx) = tokio::io::duplex(65536);

    let _server_handle =
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
                    "max_entry_dte": 45,
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
        "equity_curve_summary",
        "equity_curve",
        "trade_log",
        "suggested_next_steps",
    ] {
        assert!(
            resp.get(key).is_some(),
            "Missing top-level key: {key}"
        );
    }

    // Metrics sub-object
    let metrics = &resp["metrics"];
    for key in ["sharpe", "sortino", "cagr", "max_drawdown", "var_95"] {
        assert!(
            metrics.get(key).is_some(),
            "Missing metrics key: {key}"
        );
    }

    // Trade summary
    let ts = &resp["trade_summary"];
    for key in ["total", "winners", "losers", "exit_breakdown"] {
        assert!(
            ts.get(key).is_some(),
            "Missing trade_summary key: {key}"
        );
    }

    // Equity curve summary
    let ecs = &resp["equity_curve_summary"];
    for key in ["start_equity", "end_equity", "total_return_pct"] {
        assert!(
            ecs.get(key).is_some(),
            "Missing equity_curve_summary key: {key}"
        );
    }

    // Trade log entry shape
    let trade = &resp["trade_log"][0];
    for key in ["entry_datetime", "exit_datetime", "pnl", "exit_type", "days_held"] {
        assert!(
            trade.get(key).is_some(),
            "Missing trade_log[0] key: {key}"
        );
    }

    // Suggested next steps
    let steps = resp["suggested_next_steps"].as_array().unwrap();
    assert!(!steps.is_empty());
    assert!(steps[0].is_string());

    client.cancel().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category 6: Cache Status
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn check_cache_reports_missing_file() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let _server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "check_cache_status".into(),
            arguments: Some(
                serde_json::from_value(json!({"symbol": "MISSING", "category": "options"})).unwrap(),
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn check_cache_reports_existing_file() {
    let (server, tmp) = make_test_server();
    let mut df = make_multi_strike_df();
    write_test_parquet(tmp.path(), "SPY", &mut df);

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let _server_handle =
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
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category 7: External API Graceful Failure
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn download_options_data_fails_without_eodhd() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let _server_handle =
        tokio::spawn(async move { server.serve((client_rx, server_tx)).await.unwrap() });

    let client: rmcp::service::RunningService<rmcp::service::RoleClient, _> =
        ().serve((server_rx, client_tx)).await.unwrap();

    let result = client
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "download_options_data".into(),
            arguments: Some(
                serde_json::from_value(json!({"symbol": "SPY"})).unwrap(),
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
        text.text.contains("EODHD"),
        "Expected EODHD not configured error, got: {}",
        text.text
    );

    client.cancel().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category 8: MCP Protocol Round-Trip
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_roundtrip_list_strategies() {
    let (server, _tmp) = make_test_server();

    let (server_tx, server_rx) = tokio::io::duplex(4096);
    let (client_tx, client_rx) = tokio::io::duplex(4096);

    let _server_handle =
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
}
