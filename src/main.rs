//! Entry point for the optopsy-mcp server.
//!
//! Supports two transport modes: stdio (default, for Claude Desktop) and HTTP
//! (when `PORT` env var is set, for cloud deployment).

// float_cmp: only in tests where assert_eq! on f64 is intentional.
#![cfg_attr(test, allow(clippy::float_cmp))]

use anyhow::{Context, Result};
use rmcp::ServiceExt;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tracing_subscriber::{self, EnvFilter};

use optopsy_mcp::data::database::Database;
use optopsy_mcp::data::traits::StrategyStore;
use optopsy_mcp::server::handlers::backtests::{self, AppState};
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

        let data_root = std::env::var("DATA_ROOT")
            .unwrap_or_else(|_| shellexpand::tilde("~/.optopsy/cache").to_string());
        let db_path = std::path::PathBuf::from(&data_root).join("optopsy.db");
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create data directory: {}", parent.display())
            })?;
        }
        let db = Database::open(&db_path)?;

        let backtest_store: Arc<dyn optopsy_mcp::data::traits::BacktestStore> =
            Arc::new(db.backtests());

        let seeded = db.seed_strategies_if_empty(std::path::Path::new("scripts/strategies"))?;
        if seeded > 0 {
            tracing::info!("Seeded {seeded} strategies from scripts/strategies/");
        }
        let strategy_store: Arc<dyn StrategyStore> = Arc::new(db.strategies());

        let app_state = AppState {
            server: server::OptopsyServer::with_strategy_store(
                cache.clone(),
                strategy_store.clone(),
            ),
            backtest_store,
        };

        let strategy_store_for_mcp = strategy_store.clone();
        let service = StreamableHttpService::new(
            move || {
                Ok(server::OptopsyServer::with_strategy_store(
                    cache.clone(),
                    strategy_store_for_mcp.clone(),
                ))
            },
            LocalSessionManager::default().into(),
            StreamableHttpServerConfig::default(),
        );

        let backtest_routes = axum::Router::new()
            .route(
                "/backtests",
                axum::routing::post(backtests::create_backtest).get(backtests::list_backtests),
            )
            .route(
                "/backtests/{id}",
                axum::routing::get(backtests::get_backtest).delete(backtests::delete_backtest),
            )
            .route(
                "/backtests/{id}/trades",
                axum::routing::get(backtests::get_backtest_trades),
            )
            .route(
                "/backtests/{id}/analysis",
                axum::routing::patch(backtests::set_backtest_analysis),
            )
            .route(
                "/backtests/stream",
                axum::routing::post(backtests::create_backtest_stream),
            )
            .route(
                "/walk-forward",
                axum::routing::post(optopsy_mcp::server::handlers::walk_forward::run_walk_forward),
            )
            .with_state(app_state);

        let strategy_store_for_routes = strategy_store.clone();
        let app = axum::Router::new()
            .route(
                "/strategies",
                axum::routing::get({
                    let store = strategy_store_for_routes.clone();
                    move || async move {
                        let store = store.clone();
                        let scripts = tokio::task::spawn_blocking(move || {
                            store.list_scripts().unwrap_or_default()
                        })
                        .await
                        .unwrap_or_default();
                        axum::Json(scripts)
                    }
                }),
            )
            .route(
                "/profiles",
                axum::routing::get(optopsy_mcp::server::handlers::profiles::list_profiles)
                    .with_state(
                        Some(strategy_store_for_routes.clone()) as Option<Arc<dyn StrategyStore>>
                    ),
            )
            .route(
                "/prices/{symbol}",
                axum::routing::get({
                    let cache = prices_cache.clone();
                    move |path, query| prices_handler(cache.clone(), path, query)
                }),
            )
            .merge(backtest_routes)
            .nest_service("/mcp", service)
            .route("/health", axum::routing::get(|| async { "ok" }))
            .layer(CorsLayer::permissive());

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
        let data_root = std::env::var("DATA_ROOT")
            .unwrap_or_else(|_| shellexpand::tilde("~/.optopsy/cache").to_string());
        let db_path = std::path::PathBuf::from(&data_root).join("optopsy.db");
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create data directory: {}", parent.display())
            })?;
        }
        let db = Database::open(&db_path)?;
        db.seed_strategies_if_empty(std::path::Path::new("scripts/strategies"))?;
        let strategy_store: Arc<dyn StrategyStore> = Arc::new(db.strategies());

        let server = server::OptopsyServer::with_strategy_store(cache, strategy_store);
        let service = server.serve(rmcp::transport::stdio()).await?;
        service.waiting().await?;
    }

    Ok(())
}
