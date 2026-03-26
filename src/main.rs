//! Entry point for the optopsy-mcp server.
//!
//! Supports two transport modes: stdio (default, for Claude Desktop) and HTTP
//! (when `PORT` env var is set, for cloud deployment).

// float_cmp: only in tests where assert_eq! on f64 is intentional.
#![cfg_attr(test, allow(clippy::float_cmp))]

use anyhow::Result;
use rmcp::ServiceExt;
use std::sync::Arc;
use tracing_subscriber::{self, EnvFilter};

use optopsy_mcp::{data, server};

/// Query parameters for the `/prices/{symbol}` REST endpoint.
#[derive(serde::Deserialize)]
struct PricesQuery {
    start_date: Option<String>,
    end_date: Option<String>,
    interval: Option<optopsy_mcp::engine::types::Interval>,
    limit: Option<usize>,
    tail: Option<bool>,
}

async fn prices_handler(
    cache: Arc<data::cache::CachedStore>,
    axum::extract::Path(symbol): axum::extract::Path<String>,
    axum::extract::Query(query): axum::extract::Query<PricesQuery>,
) -> Result<
    axum::Json<optopsy_mcp::tools::response_types::RawPricesResponse>,
    (axum::http::StatusCode, String),
> {
    let interval = query.interval.unwrap_or_default();
    optopsy_mcp::tools::raw_prices::load_and_execute(
        &cache,
        &symbol,
        query.start_date.as_deref(),
        query.end_date.as_deref(),
        query.limit,
        interval,
        query.tail,
    )
    .await
    .map(axum::Json)
    .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let cache = Arc::new(data::cache::CachedStore::from_env()?);

    if let Ok(port) = std::env::var("PORT") {
        // HTTP mode — used by Railway and other cloud platforms
        use rmcp::transport::streamable_http_server::{
            session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
        };

        let prices_cache = cache.clone();
        let service = StreamableHttpService::new(
            move || Ok(server::OptopsyServer::new(cache.clone())),
            LocalSessionManager::default().into(),
            StreamableHttpServerConfig::default(),
        );

        let app = axum::Router::new()
            .route(
                "/scripts",
                axum::routing::get(|| async {
                    let scripts =
                        tokio::task::spawn_blocking(optopsy_mcp::scripting::stdlib::list_scripts)
                            .await
                            .unwrap_or_default();
                    axum::Json(scripts)
                }),
            )
            .route(
                "/prices/{symbol}",
                axum::routing::get({
                    let cache = prices_cache.clone();
                    move |path, query| prices_handler(cache.clone(), path, query)
                }),
            )
            .nest_service("/mcp", service)
            .route("/health", axum::routing::get(|| async { "ok" }));

        let addr = format!("0.0.0.0:{port}");
        tracing::info!("Starting optopsy-mcp HTTP server on {addr}");

        let listener = tokio::net::TcpListener::bind(&addr).await?;
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await?;
    } else {
        // stdio mode — used for local development with Claude Desktop
        tracing::info!("Starting optopsy-mcp MCP server (stdio)");

        let server = server::OptopsyServer::new(cache);
        let service = server.serve(rmcp::transport::stdio()).await?;
        service.waiting().await?;
    }

    Ok(())
}
