//! Shared queued-task orchestration helpers.

use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use serde_json::Value;

use crate::server::task_manager::{TaskInfo, TaskManager};

/// Create a progress callback that writes into a task's atomic counters.
pub fn progress_callback(task: &Arc<TaskInfo>) -> crate::scripting::engine::ProgressCallback {
    let task_for_progress = Arc::clone(task);
    Box::new(move |current, total| {
        task_for_progress
            .progress_current
            .store(current, Ordering::Relaxed);
        task_for_progress
            .progress_total
            .store(total, Ordering::Relaxed);
    })
}

/// Create a cancellation callback bound to a task's cancellation token.
pub fn cancel_callback(task: &Arc<TaskInfo>) -> crate::scripting::engine::CancelCallback {
    let token = task.cancellation_token.clone();
    Box::new(move || token.is_cancelled())
}

/// Run a queued task through the common wait/mark/cancel lifecycle.
pub async fn execute_queued_task<Fut>(
    task_manager: Arc<TaskManager>,
    task: Arc<TaskInfo>,
    work: Fut,
) where
    Fut: Future<Output = Result<(Value, String), String>>,
{
    let permit = tokio::select! {
        p = task_manager.acquire_permit() => p,
        () = task.cancellation_token.cancelled() => {
            task_manager.mark_cancelled(&task.id);
            return;
        }
    };

    if task.cancellation_token.is_cancelled() {
        task_manager.mark_cancelled(&task.id);
        drop(permit);
        return;
    }

    task_manager.mark_running(&task.id);

    let result = work.await;

    drop(permit);

    if task.cancellation_token.is_cancelled() {
        task_manager.mark_cancelled(&task.id);
        return;
    }

    match result {
        Ok((result_json, result_id)) => {
            task_manager.mark_completed(&task.id, result_json, result_id);
        }
        Err(error) => task_manager.mark_failed(&task.id, error),
    }
}
