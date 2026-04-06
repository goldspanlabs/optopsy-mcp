//! REST API handlers for creating and streaming backtests.

use axum::{
    extract::State,
    response::sse::{Event, Sse},
    Json,
};
use futures::{stream::Stream, StreamExt};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::application::backtests;
use crate::application::error::{ApplicationError, ApplicationErrorKind};
use crate::server::state::AppState;
use crate::tools::run_script::RunScriptParams;

fn app_error_message(error: &ApplicationError) -> String {
    match error.kind() {
        ApplicationErrorKind::Storage | ApplicationErrorKind::Internal => {
            format!("DB insert failed: {error}")
        }
        _ => error.to_string(),
    }
}

/// Request body for `POST /runs`.
#[derive(Debug, Deserialize)]
pub struct CreateBacktestRequest {
    pub strategy: String,
    pub params: HashMap<String, Value>,
    #[serde(default)]
    pub profile: Option<String>,
}

// ──────────────────────────────────────────────────────────────────────────────
// Handlers
// ──────────────────────────────────────────────────────────────────────────────

/// `POST /runs` — Run a strategy with SSE progress updates (legacy endpoint).
///
/// Cancellation is handled via `/tasks/*` endpoints; this endpoint runs to completion.
#[allow(clippy::unused_async, clippy::too_many_lines)]
pub async fn create_backtest(
    State(state): State<AppState>,
    Json(req): Json<CreateBacktestRequest>,
) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Event>(64);

    tokio::spawn(async move {
        // Set up progress tracking via shared atomic
        let progress_current = Arc::new(AtomicUsize::new(0));
        let progress_total = Arc::new(AtomicUsize::new(0));

        let cur = Arc::clone(&progress_current);
        let tot = Arc::clone(&progress_total);
        let progress_cb: crate::scripting::engine::ProgressCallback =
            Box::new(move |current, total| {
                cur.store(current, Ordering::Relaxed);
                tot.store(total, Ordering::Relaxed);
            });

        // Spawn a ticker that reads atomics and sends SSE progress events
        let progress_tx = tx.clone();
        let pc = Arc::clone(&progress_current);
        let pt = Arc::clone(&progress_total);
        let ticker = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(200));
            loop {
                interval.tick().await;
                let c = pc.load(Ordering::Relaxed);
                let t = pt.load(Ordering::Relaxed);
                if t > 0 {
                    let evt = Event::default()
                        .event("progress")
                        .data(format!(r#"{{"current":{c},"total":{t}}}"#));
                    if progress_tx.send(evt).await.is_err() {
                        break;
                    }
                }
            }
        });

        // Run the actual backtest
        let run_params = RunScriptParams {
            strategy: Some(req.strategy.clone()),
            script: None,
            params: req.params.clone(),
            profile: req.profile.clone(),
        };

        let result = backtests::execute_script_with_progress(
            &state.server,
            run_params,
            Some(progress_cb),
            None,
        )
        .await;

        // Stop the progress ticker
        ticker.abort();

        match result {
            Ok(exec_result) => {
                let strategy_key = exec_result
                    .resolved_strategy_id
                    .unwrap_or_else(|| req.strategy.clone());
                let response = exec_result.response;

                match backtests::persist_backtest(
                    &*state.run_store,
                    &strategy_key,
                    &req.params,
                    &response,
                    "manual",
                    None,
                ) {
                    Ok((id, _)) => {
                        if let Ok(Some(detail)) = state.run_store.get_run(&id) {
                            let json = serde_json::to_string(&detail).unwrap_or_default();
                            let _ = tx.send(Event::default().event("result").data(json)).await;
                        }
                    }
                    Err(error) => {
                        let _ = tx
                            .send(
                                Event::default()
                                    .event("error")
                                    .data(app_error_message(&error)),
                            )
                            .await;
                    }
                }
            }
            Err(e) => {
                let _ = tx
                    .send(Event::default().event("error").data(e.to_string()))
                    .await;
            }
        }

        let _ = tx.send(Event::default().event("done").data("")).await;
    });

    Sse::new(tokio_stream::wrappers::ReceiverStream::new(rx).map(Ok))
}
