//! REST API handlers for sweep CRUD and execution.

use axum::{
    extract::State,
    response::sse::{Event, Sse},
    Json,
};
use futures::stream::{Stream, StreamExt};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::application::sweeps;
use crate::scripting::engine::CancelCallback;
use crate::server::state::AppState;

// ──────────────────────────────────────────────────────────────────────────────
// Request / query types
// ──────────────────────────────────────────────────────────────────────────────

pub use crate::application::sweeps::{CreateSweepRequest, SweepParamDef};

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

// ──────────────────────────────────────────────────────────────────────────────
// Handlers
// ──────────────────────────────────────────────────────────────────────────────

/// `POST /runs/sweep` — Run a sweep with SSE progress updates.
#[allow(clippy::too_many_lines, clippy::unused_async)]
pub async fn create_sweep(
    State(state): State<AppState>,
    Json(req): Json<CreateSweepRequest>,
) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Event>(64);

    tokio::spawn(async move {
        let current = Arc::new(AtomicUsize::new(0));
        let total = Arc::new(AtomicUsize::new(0));

        // Spawn a ticker that sends SSE progress events
        let progress_tx = tx.clone();
        let cur_clone = Arc::clone(&current);
        let tot_clone = Arc::clone(&total);
        let ticker = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
            loop {
                interval.tick().await;
                let c = cur_clone.load(Ordering::Relaxed);
                let t = tot_clone.load(Ordering::Relaxed);
                if t > 0 {
                    let data = format!("{{\"current\":{c},\"total\":{t}}}");
                    if progress_tx
                        .send(Event::default().event("progress").data(data))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                if c >= t && t > 0 {
                    break;
                }
            }
        });

        let cur_cb = Arc::clone(&current);
        let tot_cb = Arc::clone(&total);
        let progress: crate::scripting::engine::ProgressCallback = Box::new(move |cur, tot| {
            cur_cb.store(cur, Ordering::Relaxed);
            tot_cb.store(tot, Ordering::Relaxed);
        });
        let is_cancelled: CancelCallback = Box::new(|| false);

        let sweep_result = sweeps::execute_sweep(
            &state.server,
            state.run_store.as_ref(),
            &req,
            "manual",
            None,
            Some(progress),
            Some(&is_cancelled),
        )
        .await;

        ticker.abort();

        match sweep_result {
            Ok(result) => {
                tracing::info!(
                    "Sweep completed: combinations_run={}, ranked_results={}",
                    result.response.combinations_run,
                    result.response.ranked_results.len()
                );
                if let Ok(Some(detail)) = state.run_store.get_sweep(&result.sweep_id) {
                    let json = serde_json::to_string(&detail).unwrap_or_default();
                    let _ = tx.send(Event::default().event("result").data(json)).await;
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
