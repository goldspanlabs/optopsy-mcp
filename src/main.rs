//! Entry point for the optopsy-mcp server.
//!
//! Supports two transport modes: stdio (default, for Claude Desktop) and HTTP
//! (when `PORT` env var is set, for cloud deployment).

// float_cmp: only in tests where assert_eq! on f64 is intentional.
#![cfg_attr(test, allow(clippy::float_cmp))]

use anyhow::{Context, Result};
use rmcp::ServiceExt;
use std::sync::Arc;
use tracing_subscriber::{self, EnvFilter};

use optopsy_mcp::data::database::Database;
use optopsy_mcp::data::traits::{self, StrategyStore};
use optopsy_mcp::server::state::AppState;
use optopsy_mcp::server::task_manager::TaskManager;
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
#[allow(clippy::too_many_lines)]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

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

        let data_root = std::env::var("DATA_ROOT").unwrap_or_else(|_| "data".to_string());
        let db_path = std::path::PathBuf::from(&data_root).join("optopsy.db");
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create data directory: {}", parent.display())
            })?;
        }
        let db = Database::open(&db_path)?;

        let run_store: Arc<dyn optopsy_mcp::data::traits::RunStore> = Arc::new(db.runs());
        let adjustment_store = Arc::new(db.adjustments());

        let strategy_store: Arc<dyn StrategyStore> = Arc::new(db.strategies());
        let seeded = traits::seed_strategies_if_empty(
            strategy_store.as_ref(),
            std::path::Path::new("scripts/strategies"),
        )?;
        if seeded > 0 {
            tracing::info!("Seeded {seeded} strategies from scripts/strategies/");
        }

        let chat_store: Arc<dyn optopsy_mcp::data::traits::ChatStore> = Arc::new(db.chat());

        let max_concurrent_tasks = std::env::var("MAX_CONCURRENT_TASKS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1usize);
        let task_manager = Arc::new(TaskManager::new(max_concurrent_tasks));

        let forward_test_store = Arc::new(db.forward_tests());
        let server = server::OptopsyServer::with_all_stores(
            cache.clone(),
            strategy_store.clone(),
            run_store.clone(),
            adjustment_store.clone(),
        )
        .with_forward_test_store(forward_test_store.clone());

        let app_state = AppState {
            server,
            run_store,
            chat_store,
            task_manager: Arc::clone(&task_manager),
            forward_test_store: forward_test_store.clone(),
        };

        let strategy_store_for_mcp = strategy_store.clone();
        let run_store_for_mcp = app_state.run_store.clone();
        let adjustment_store_for_mcp = adjustment_store.clone();
        let forward_test_store_for_mcp = forward_test_store.clone();
        let service = StreamableHttpService::new(
            move || {
                let srv = server::OptopsyServer::with_all_stores(
                    cache.clone(),
                    strategy_store_for_mcp.clone(),
                    run_store_for_mcp.clone(),
                    adjustment_store_for_mcp.clone(),
                )
                .with_forward_test_store(forward_test_store_for_mcp.clone());
                Ok(srv)
            },
            LocalSessionManager::default().into(),
            StreamableHttpServerConfig::default(),
        );

        // Spawn background task cleanup (remove terminal tasks older than 10 minutes)
        let cleanup_tm = Arc::clone(&task_manager);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                cleanup_tm.cleanup(chrono::Duration::minutes(10));
            }
        });

        let app = server::router::build_api_router(app_state)
            .route(
                "/prices/{symbol}",
                axum::routing::get({
                    let cache = prices_cache.clone();
                    move |path, query| prices_handler(cache.clone(), path, query)
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
    } else {
        // stdio mode — used for local development with Claude Desktop
        tracing::info!("Starting optopsy-mcp MCP server (stdio)");

        // Set up stores for stdio mode — single database file
        let data_root = std::env::var("DATA_ROOT").unwrap_or_else(|_| "data".to_string());
        let db_path = std::path::PathBuf::from(&data_root).join("optopsy.db");
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create data directory: {}", parent.display())
            })?;
        }
        let db = Database::open(&db_path)?;
        let strategy_store: Arc<dyn StrategyStore> = Arc::new(db.strategies());
        let run_store: Arc<dyn traits::RunStore> = Arc::new(db.runs());
        let adjustment_store = Arc::new(db.adjustments());
        traits::seed_strategies_if_empty(
            strategy_store.as_ref(),
            std::path::Path::new("scripts/strategies"),
        )?;

        let forward_test_store = Arc::new(db.forward_tests());
        let server = server::OptopsyServer::with_all_stores(
            cache,
            strategy_store,
            run_store,
            adjustment_store,
        )
        .with_forward_test_store(forward_test_store);
        let service = server.serve(rmcp::transport::stdio()).await?;
        service.waiting().await?;
    }

    Ok(())
}
