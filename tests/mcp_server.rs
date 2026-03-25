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

// ─── Test Helpers ────────────────────────────────────────────────────────────

/// Create an `OptopsyServer` backed by a temporary directory.
fn make_test_server() -> (OptopsyServer, TempDir) {
    let tmp = TempDir::new().unwrap();
    let cache = Arc::new(CachedStore::new(
        tmp.path().to_path_buf(),
        "options".to_string(),
    ));
    let server = OptopsyServer::new(cache);
    (server, tmp)
}

/// Pre-load a `DataFrame` into the server's shared state.

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
    assert!(instructions.contains("run_script"));
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

    assert_eq!(tools.len(), 21, "Expected 21 tools, got: {tool_names:?}");
    for expected in [
        "list_symbols",
        "list_strategies",
        "run_script",
        "parameter_sweep",
        "walk_forward",
        "permutation_test",
        "get_raw_prices",
        "build_signal",
        "aggregate_prices",
        "distribution",
        "correlate",
        "rolling_metric",
        "regime_detect",
        "generate_hypotheses",
        "drawdown_analysis",
        "cointegration_test",
        "monte_carlo",
        "factor_attribution",
        "portfolio_optimize",
        "benchmark_analysis",
        "bayesian_optimize",
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
async fn list_strategies_returns_all_31() {
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

    assert_eq!(resp["total"], 31);
    let categories = resp["categories"].as_object().unwrap();
    assert!(categories.len() >= 6, "Expected at least 6 categories");
    assert!(!resp["suggested_next_steps"].as_array().unwrap().is_empty());

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_signal_catalog_returns_catalog() {
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
                    "action": "catalog"
                }))
                .unwrap(),
            ),
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

    assert!(resp["success"].as_bool().unwrap());
    let catalog = &resp["catalog"];
    assert!(catalog["total"].as_u64().unwrap() > 0);
    assert!(!catalog["categories"].as_object().unwrap().is_empty());

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
    assert_eq!(resp["total"], 31);

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category: get_raw_prices Integration Tests
// ═══════════════════════════════════════════════════════════════════════════════

/// Write a `DataFrame` as a Parquet file under the "stocks" category.
fn write_prices_parquet(cache_dir: &std::path::Path, symbol: &str, df: &mut DataFrame) -> PathBuf {
    let dir = cache_dir.join("stocks");
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

// get_raw_prices auto-fetches from Yahoo Finance when cache is missing,
// so no "fails when cache missing" test is needed. The auto-fetch behavior
// is covered by the Yahoo Finance integration in the fetch module.

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
    assert_eq!(prices[0]["date"], 1_704_153_600); // 2024-01-02T00:00:00Z
    assert_eq!(prices[0]["open"], 100.0);
    assert_eq!(prices[2]["date"], 1_704_326_400); // 2024-01-04T00:00:00Z
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
                    "formula": "unknown_func(close) > 10"
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
        "validate should fail for unknown function 'unknown_func'"
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
    // signal_spec should be present and be a Formula variant
    let spec = &resp["signal_spec"];
    assert!(!spec.is_null(), "signal_spec should be present");
    assert_eq!(spec["type"], "Formula");
    assert_eq!(spec["formula"], "close > close[1]");
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

    // Search now only searches saved custom signals (not built-in catalog).
    // With no saved signals, success is false and summary provides guidance.
    assert_eq!(
        resp["success"], false,
        "search with no saved signals should return success=false"
    );
    assert!(
        resp["summary"]
            .as_str()
            .unwrap()
            .contains("No saved custom signals"),
        "summary should explain no saved signals matched"
    );
    let next_steps = resp["suggested_next_steps"]
        .as_array()
        .expect("suggested_next_steps should be an array");
    assert!(
        !next_steps.is_empty(),
        "should suggest next steps when no results found"
    );

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}
