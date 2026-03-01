// float_cmp: only in tests where assert_eq! on f64 is intentional.
#![cfg_attr(test, allow(clippy::float_cmp))]

use anyhow::Result;
use rmcp::ServiceExt;
use std::sync::Arc;
use tracing_subscriber::{self, EnvFilter};

mod data;
mod engine;
mod server;
mod signals;
mod strategies;
mod tools;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let cache = Arc::new(data::cache::CachedStore::from_env()?);

    // Create EODHD provider if API key is configured
    let eodhd = data::eodhd::EodhdProvider::from_env(&cache.cache_dir()).map(Arc::new);
    if eodhd.is_some() {
        tracing::info!("EODHD data provider configured");
    }

    if let Ok(port) = std::env::var("PORT") {
        // HTTP mode — used by Railway and other cloud platforms
        use rmcp::transport::streamable_http_server::{
            session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
        };

        let service = StreamableHttpService::new(
            move || Ok(server::OptopsyServer::new(cache.clone(), eodhd.clone())),
            LocalSessionManager::default().into(),
            StreamableHttpServerConfig::default(),
        );

        let app = axum::Router::new()
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

        let server = server::OptopsyServer::new(cache, eodhd);
        let service = server.serve(rmcp::transport::stdio()).await?;
        service.waiting().await?;
    }

    Ok(())
}
