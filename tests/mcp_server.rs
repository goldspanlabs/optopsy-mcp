//! MCP server integration tests.
//!
//! Verifies tool registration, parameter validation (garde), error paths,
//! response serialization, and MCP protocol round-trips.

use std::sync::Arc;

use rmcp::ServiceExt;
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

    assert_eq!(tools.len(), 13, "Expected 13 tools, got: {tool_names:?}");
    for expected in [
        "run_script",
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
    ] {
        assert!(
            tool_names.contains(&expected.to_string()),
            "Missing tool: {expected}"
        );
    }

    client.cancel().await.unwrap();
    server_handle.await.unwrap();
}
