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

use optopsy_mcp::bootstrap::AppServices;
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

async fn run_http(
    port: String,
    cache: Arc<data::cache::CachedStore>,
    services: AppServices,
) -> Result<()> {
    use rmcp::transport::streamable_http_server::{
        session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
    };

    let prices_cache = Arc::clone(&cache);
    let app_state = services.build_app_state(Arc::clone(&cache));
    let task_manager = Arc::clone(&services.task_manager);
    let services_for_mcp = services.clone();

    let service = StreamableHttpService::new(
        move || Ok(services_for_mcp.build_server(Arc::clone(&cache))),
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig::default(),
    );

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            task_manager.cleanup(chrono::Duration::minutes(10));
        }
    });

    let app = server::router::build_api_router(app_state)
        .route(
            "/prices/{symbol}",
            axum::routing::get({
                let cache = Arc::clone(&prices_cache);
                move |path, query| prices_handler(Arc::clone(&cache), path, query)
            }),
        )
        .nest_service("/mcp", service);

    let addr = format!("0.0.0.0:{port}");
    tracing::info!("Starting optopsy-mcp HTTP server on {addr}");

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    Ok(())
}

async fn run_stdio(cache: Arc<data::cache::CachedStore>, services: AppServices) -> Result<()> {
    tracing::info!("Starting optopsy-mcp MCP server (stdio)");
    let server = services.build_server(cache);
    let service = server.serve(rmcp::transport::stdio()).await?;
    service.waiting().await?;
    Ok(())
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let cache = Arc::new(data::cache::CachedStore::from_env()?);
    let services = AppServices::from_env()?;

    if let Ok(port) = std::env::var("PORT") {
        run_http(port, cache, services).await?;
    } else {
        run_stdio(cache, services).await?;
    }

    Ok(())
}
