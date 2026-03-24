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

/// Query parameters for the `/api/indicators/compute` REST endpoint.
#[derive(serde::Deserialize)]
struct IndicatorComputeQuery {
    symbol: String,
    indicators: String,
    start_date: Option<String>,
    end_date: Option<String>,
    interval: Option<optopsy_mcp::engine::types::Interval>,
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

/// List chartable indicators (saved signals with chart config).
async fn indicators_list_handler(
) -> Result<axum::Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let signals = optopsy_mcp::signals::storage::list_saved_signals()
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let indicators: Vec<serde_json::Value> = signals
        .into_iter()
        .filter(|s| s.chartable)
        .filter_map(|s| {
            let (_, _, chart) = optopsy_mcp::signals::storage::load_signal(&s.name).ok()?;
            let chart = chart?;
            Some(serde_json::json!({
                "name": s.name,
                "label": chart.label,
                "display_type": chart.display_type,
                "thresholds": chart.thresholds,
            }))
        })
        .collect();

    Ok(axum::Json(serde_json::json!({ "indicators": indicators })))
}

/// Compute indicator data for given symbol and indicator names.
async fn indicators_compute_handler(
    cache: Arc<data::cache::CachedStore>,
    axum::extract::Query(query): axum::extract::Query<IndicatorComputeQuery>,
) -> Result<axum::Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    use polars::prelude::*;

    let symbol = query.symbol.to_uppercase();
    let interval = query.interval.unwrap_or_default();
    let indicator_names: Vec<String> = query
        .indicators
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    let ohlcv_path = cache
        .find_ohlcv(&symbol)
        .ok_or_else(|| {
            (
                axum::http::StatusCode::NOT_FOUND,
                format!("No OHLCV data found for {symbol}"),
            )
        })?
        .to_string_lossy()
        .to_string();

    let start = query
        .start_date
        .as_deref()
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());
    let end = query
        .end_date
        .as_deref()
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());

    // Collect indicator formulas and cross-symbol references
    let mut cross_symbols = std::collections::HashSet::new();
    let mut chart_indicators = Vec::new();

    for name in &indicator_names {
        if let Ok((spec, _, Some(chart))) = optopsy_mcp::signals::storage::load_signal(name) {
            let formula = chart
                .expression
                .clone()
                .or_else(|| match &spec {
                    optopsy_mcp::signals::registry::SignalSpec::Formula { formula } => {
                        Some(formula.clone())
                    }
                    _ => None,
                })
                .unwrap_or_default();
            if !formula.is_empty() {
                cross_symbols.extend(
                    optopsy_mcp::signals::registry::extract_formula_cross_symbols(&formula),
                );
                chart_indicators.push((formula, chart));
            }
        }
    }

    // Resolve cross-symbol OHLCV paths
    let mut cross_paths: Vec<(String, String)> = Vec::new();
    for sym in &cross_symbols {
        if let Some(path) = cache.find_ohlcv(sym) {
            cross_paths.push((sym.clone(), path.to_string_lossy().to_string()));
        }
    }

    // Load and compute on blocking thread
    let indicator_data = tokio::task::spawn_blocking(
        move || -> Result<Vec<optopsy_mcp::signals::helpers::IndicatorData>, String> {
            let ohlcv_df = optopsy_mcp::engine::stock_sim::load_ohlcv_df(&ohlcv_path, start, end)
                .map_err(|e| e.to_string())?;
            let ohlcv_df = optopsy_mcp::engine::stock_sim::resample_ohlcv(&ohlcv_df, interval)
                .map_err(|e| e.to_string())?;
            let date_col = optopsy_mcp::engine::stock_sim::detect_date_col(&ohlcv_df);

            // Join cross-symbol data (resampled to same interval as primary)
            let mut enriched_df = ohlcv_df;
            for (sym, path) in &cross_paths {
                if let Ok(cross_df) =
                    optopsy_mcp::engine::stock_sim::load_ohlcv_df(path, None, None)
                {
                    // Resample cross-symbol to same interval so date columns match
                    let Ok(cross_df) =
                        optopsy_mcp::engine::stock_sim::resample_ohlcv(&cross_df, interval)
                    else {
                        continue;
                    };
                    let cross_date_col =
                        optopsy_mcp::engine::stock_sim::detect_date_col(&cross_df);
                    if let Ok(joined) = enriched_df
                        .clone()
                        .lazy()
                        .join(
                            cross_df.lazy().select([
                                col(cross_date_col),
                                col("close").alias(format!("{sym}_close")),
                            ]),
                            [col(date_col)],
                            [col(cross_date_col)],
                            JoinArgs::new(JoinType::Left),
                        )
                        .collect()
                    {
                        enriched_df = joined;
                    }
                }
            }

            Ok(chart_indicators
                .iter()
                .filter_map(|(formula, chart)| {
                    optopsy_mcp::signals::indicators::compute_formula_indicator(
                        formula,
                        chart,
                        &enriched_df,
                        date_col,
                    )
                })
                .collect())
        },
    )
    .await
    .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok(axum::Json(
        serde_json::json!({ "indicator_data": indicator_data }),
    ))
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
                "/signals",
                axum::routing::get(|| async { axum::Json(optopsy_mcp::tools::signals::execute()) }),
            )
            .route(
                "/strategies",
                axum::routing::get(|| async {
                    axum::Json(optopsy_mcp::tools::strategies::execute())
                }),
            )
            .route(
                "/prices/{symbol}",
                axum::routing::get({
                    let cache = prices_cache.clone();
                    move |path, query| prices_handler(cache.clone(), path, query)
                }),
            )
            .route(
                "/api/indicators",
                axum::routing::get(indicators_list_handler),
            )
            .route(
                "/api/indicators/compute",
                axum::routing::get({
                    let cache = prices_cache.clone();
                    move |query| indicators_compute_handler(cache.clone(), query)
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
