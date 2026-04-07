//! Axum router builder for the optopsy REST API.
//!
//! [`build_api_router`] assembles all route groups into a single [`Router`]
//! that can be used both in production (from `main.rs`) and in integration
//! tests (via `tower::ServiceExt::oneshot`).

use axum::Router;
use tower_http::cors::CorsLayer;

use crate::server::handlers::{
    backtests, chat as chat_handlers, forward_tests, hypotheses, pipeline, profiles, runs,
    strategies, sweeps, tasks,
};
use crate::server::state::AppState;

/// Build the full REST API router from `state`.
///
/// Includes all route groups (strategy, chat, run, task, misc) merged together
/// with a permissive CORS layer.
///
/// **Not included** (handled by the caller in `main.rs`):
/// - `/prices/{symbol}` — requires a `CachedStore` Arc captured outside `AppState`
/// - `/mcp` — MCP-specific `StreamableHttpService`
#[allow(clippy::too_many_lines)]
pub fn build_api_router(state: AppState) -> Router {
    let strategy_routes = Router::new()
        .route(
            "/strategies",
            axum::routing::get(strategies::list_strategies).post(strategies::create_strategy),
        )
        .route(
            "/strategies/validate",
            axum::routing::post(strategies::validate_script),
        )
        .route(
            "/strategies/{id}",
            axum::routing::get(strategies::get_strategy)
                .put(strategies::update_strategy)
                .delete(strategies::delete_strategy),
        )
        .route(
            "/strategies/{id}/source",
            axum::routing::get(strategies::get_strategy_source),
        )
        .route(
            "/strategies/{id}/validate",
            axum::routing::post(strategies::validate_stored_strategy),
        )
        .with_state(state.clone());

    let chat_routes = Router::new()
        .route(
            "/threads",
            axum::routing::get(chat_handlers::list_threads).post(chat_handlers::create_thread),
        )
        .route(
            "/threads/{id}",
            axum::routing::get(chat_handlers::get_thread)
                .patch(chat_handlers::update_thread)
                .delete(chat_handlers::delete_thread),
        )
        .route(
            "/threads/{id}/messages",
            axum::routing::get(chat_handlers::get_messages)
                .post(chat_handlers::upsert_message)
                .delete(chat_handlers::delete_messages),
        )
        .route(
            "/threads/{id}/results",
            axum::routing::get(chat_handlers::get_results)
                .put(chat_handlers::replace_results)
                .post(chat_handlers::replace_results),
        )
        .route(
            "/threads/{id}/results/{key}",
            axum::routing::delete(chat_handlers::delete_result),
        )
        .with_state(state.clone());

    let analysis_routes = Router::new()
        .route(
            "/hypotheses",
            axum::routing::post(hypotheses::generate_hypotheses),
        )
        .with_state(state.clone());

    let misc_routes = Router::new()
        .route("/profiles", axum::routing::get(profiles::list_profiles))
        .route("/health", axum::routing::get(|| async { "ok" }))
        .with_state(state.clone());

    let run_routes = Router::new()
        .route(
            "/runs",
            axum::routing::get(runs::list_runs).post(backtests::create_backtest),
        )
        .route(
            "/runs/{id}",
            axum::routing::get(runs::get_run).delete(runs::delete_run),
        )
        .route(
            "/runs/{id}/analysis",
            axum::routing::patch(runs::set_run_analysis),
        )
        .route("/runs/sweep", axum::routing::post(sweeps::create_sweep))
        .route(
            "/runs/sweep/{sweepId}",
            axum::routing::get(runs::get_sweep_detail).delete(runs::delete_sweep),
        )
        .route(
            "/runs/sweep/{sweepId}/analysis",
            axum::routing::patch(runs::set_sweep_analysis),
        )
        .route(
            "/runs/sweep/{sweepId}/validations",
            axum::routing::get(runs::get_walk_forward_validations),
        )
        .route(
            "/runs/walk-forward/{id}",
            axum::routing::delete(runs::delete_walk_forward_validation),
        )
        .route(
            "/runs/walk-forward/{id}/analysis",
            axum::routing::patch(runs::set_walk_forward_analysis),
        )
        .route(
            "/walk-forward",
            axum::routing::post(crate::server::handlers::walk_forward::run_walk_forward),
        )
        .route(
            "/runs/baseline-validation",
            axum::routing::post(pipeline::create_baseline_validation),
        )
        .route(
            "/runs/workflows",
            axum::routing::post(pipeline::create_workflow),
        )
        .with_state(state.clone());

    let task_routes = Router::new()
        .route("/tasks", axum::routing::get(tasks::list_tasks))
        .route(
            "/tasks/backtest",
            axum::routing::post(tasks::submit_backtest),
        )
        .route("/tasks/sweep", axum::routing::post(tasks::submit_sweep))
        .route(
            "/tasks/pipeline",
            axum::routing::post(tasks::submit_pipeline),
        )
        .route(
            "/tasks/walk-forward",
            axum::routing::post(tasks::submit_walk_forward),
        )
        .route(
            "/tasks/baseline-validation",
            axum::routing::post(tasks::submit_baseline_validation),
        )
        .route(
            "/tasks/workflows",
            axum::routing::post(tasks::submit_workflow),
        )
        .route(
            "/tasks/{id}",
            axum::routing::get(tasks::get_task).delete(tasks::cancel_task),
        )
        .route("/tasks/{id}/stream", axum::routing::get(tasks::stream_task))
        .with_state(state.clone());

    let forward_test_routes = Router::new()
        .route(
            "/forward-tests",
            axum::routing::get(forward_tests::list_forward_tests)
                .post(forward_tests::create_forward_test),
        )
        .route(
            "/forward-tests/{id}",
            axum::routing::get(forward_tests::get_forward_test)
                .patch(forward_tests::update_forward_test)
                .delete(forward_tests::delete_forward_test),
        )
        .route(
            "/forward-tests/{id}/step",
            axum::routing::post(forward_tests::step_forward_test),
        )
        .with_state(state);

    Router::new()
        .merge(strategy_routes)
        .merge(chat_routes)
        .merge(run_routes)
        .merge(task_routes)
        .merge(forward_test_routes)
        .merge(analysis_routes)
        .merge(misc_routes)
        .layer(CorsLayer::permissive())
}
