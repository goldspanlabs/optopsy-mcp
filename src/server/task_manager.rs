//! Task manager for tracking long-running backtest tasks.
//!
//! Manages task lifecycle: Queued → Running → Completed/Failed/Cancelled.
//! Uses a semaphore to limit concurrent executions and `DashMap` for concurrent access.

use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use dashmap::DashMap;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

// ── Enums ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    Single,
    Sweep,
    WalkForward,
    Pipeline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl TaskStatus {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Queued,
            1 => Self::Running,
            2 => Self::Completed,
            4 => Self::Cancelled,
            _ => Self::Failed, // covers 3 (Failed) and any other invalid values
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

// ── Task structs ─────────────────────────────────────────────────────────────

pub struct TaskMutable {
    pub started_at: Option<chrono::DateTime<Utc>>,
    pub completed_at: Option<chrono::DateTime<Utc>>,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub result_id: Option<String>,
}

pub struct TaskInfo {
    pub id: String,
    pub kind: TaskKind,
    pub strategy: String,
    pub symbol: String,
    pub thread_id: Option<String>,
    pub params: serde_json::Value,
    pub created_at: chrono::DateTime<Utc>,
    pub progress_current: AtomicUsize,
    pub progress_total: AtomicUsize,
    pub cancellation_token: CancellationToken,
    status: AtomicU8,
    pub mutable: Mutex<TaskMutable>,
}

impl TaskInfo {
    pub fn status(&self) -> TaskStatus {
        TaskStatus::from_u8(self.status.load(Ordering::Acquire))
    }

    fn set_status(&self, s: TaskStatus) {
        self.status.store(s as u8, Ordering::Release);
    }
}

// ── TaskManager ───────────────────────────────────────────────────────────────

pub struct TaskManager {
    tasks: DashMap<String, Arc<TaskInfo>>,
    semaphore: Arc<Semaphore>,
    max_concurrent: usize,
}

impl TaskManager {
    /// Create a new `TaskManager` with the given concurrency limit.
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            tasks: DashMap::new(),
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            max_concurrent,
        }
    }

    /// Register a new task in the Queued state and return its `Arc<TaskInfo>`.
    pub fn register(
        &self,
        kind: TaskKind,
        strategy: impl Into<String>,
        symbol: impl Into<String>,
        thread_id: Option<String>,
        params: serde_json::Value,
    ) -> Arc<TaskInfo> {
        let id = uuid::Uuid::new_v4().to_string();
        let task = Arc::new(TaskInfo {
            id: id.clone(),
            kind,
            strategy: strategy.into(),
            symbol: symbol.into(),
            thread_id,
            params,
            created_at: Utc::now(),
            progress_current: AtomicUsize::new(0),
            progress_total: AtomicUsize::new(0),
            cancellation_token: CancellationToken::new(),
            status: AtomicU8::new(TaskStatus::Queued as u8),
            mutable: Mutex::new(TaskMutable {
                started_at: None,
                completed_at: None,
                result: None,
                error: None,
                result_id: None,
            }),
        });
        self.tasks.insert(id, Arc::clone(&task));
        task
    }

    /// Acquire a semaphore permit, blocking until a slot is available.
    pub async fn acquire_permit(&self) -> OwnedSemaphorePermit {
        Arc::clone(&self.semaphore)
            .acquire_owned()
            .await
            .expect("semaphore closed")
    }

    /// Transition a task to Running and record `started_at`.
    pub fn mark_running(&self, task_id: &str) {
        if let Some(task) = self.tasks.get(task_id) {
            task.set_status(TaskStatus::Running);
            if let Ok(mut m) = task.mutable.lock() {
                m.started_at = Some(Utc::now());
            }
        }
    }

    /// Transition a task to Completed, storing the result and `result_id`.
    pub fn mark_completed(&self, task_id: &str, result: serde_json::Value, result_id: String) {
        if let Some(task) = self.tasks.get(task_id) {
            task.set_status(TaskStatus::Completed);
            if let Ok(mut m) = task.mutable.lock() {
                m.result = Some(result);
                m.result_id = Some(result_id);
                m.completed_at = Some(Utc::now());
            }
        }
    }

    /// Transition a task to Failed, storing the error message.
    pub fn mark_failed(&self, task_id: &str, error: String) {
        if let Some(task) = self.tasks.get(task_id) {
            task.set_status(TaskStatus::Failed);
            if let Ok(mut m) = task.mutable.lock() {
                m.error = Some(error);
                m.completed_at = Some(Utc::now());
            }
        }
    }

    /// Transition a task to Cancelled (idempotent — no-op if already terminal).
    pub fn mark_cancelled(&self, task_id: &str) {
        if let Some(task) = self.tasks.get(task_id) {
            if !task.status().is_terminal() {
                task.set_status(TaskStatus::Cancelled);
                if let Ok(mut m) = task.mutable.lock() {
                    m.completed_at = Some(Utc::now());
                }
            }
        }
    }

    /// Cancel a task: trigger its `CancellationToken` and set status to Cancelled.
    /// Returns `false` if the task is already in a terminal state or not found.
    pub fn cancel(&self, task_id: &str) -> bool {
        if let Some(task) = self.tasks.get(task_id) {
            let status = task.status();
            if status.is_terminal() {
                return false;
            }
            task.cancellation_token.cancel();
            task.set_status(TaskStatus::Cancelled);
            if let Ok(mut m) = task.mutable.lock() {
                m.completed_at = Some(Utc::now());
            }
            true
        } else {
            false
        }
    }

    /// Retrieve a task by ID.
    pub fn get(&self, task_id: &str) -> Option<Arc<TaskInfo>> {
        self.tasks.get(task_id).map(|e| Arc::clone(e.value()))
    }

    /// List all non-terminal (Queued + Running) tasks, sorted by `created_at` ascending.
    pub fn list_active(&self) -> Vec<Arc<TaskInfo>> {
        let mut active: Vec<Arc<TaskInfo>> = self
            .tasks
            .iter()
            .filter(|e| !e.value().status().is_terminal())
            .map(|e| Arc::clone(e.value()))
            .collect();
        active.sort_by_key(|t| t.created_at);
        active
    }

    /// Return the 1-indexed queue position of a Queued task, or `None` if not queued.
    pub fn queue_position(&self, task_id: &str) -> Option<usize> {
        let task = self.tasks.get(task_id)?;
        if task.status() != TaskStatus::Queued {
            return None;
        }
        // Collect all queued tasks sorted by created_at.
        let mut queued: Vec<Arc<TaskInfo>> = self
            .tasks
            .iter()
            .filter(|e| e.value().status() == TaskStatus::Queued)
            .map(|e| Arc::clone(e.value()))
            .collect();
        queued.sort_by_key(|t| t.created_at);
        queued
            .iter()
            .position(|t| t.id == task_id)
            .map(|pos| pos + 1)
    }

    /// Remove terminal tasks whose `completed_at` is older than `max_age`.
    pub fn cleanup(&self, max_age: chrono::Duration) {
        let cutoff = Utc::now() - max_age;
        self.tasks.retain(|_, task| {
            if !task.status().is_terminal() {
                return true;
            }
            // Keep if completed_at is missing or within max_age.
            if let Ok(m) = task.mutable.lock() {
                match m.completed_at {
                    Some(ts) => ts > cutoff,
                    None => false, // terminal with no completed_at → remove
                }
            } else {
                // Poisoned mutex → remove the task
                false
            }
        });
    }

    /// Return the configured concurrency limit.
    pub fn max_concurrent(&self) -> usize {
        self.max_concurrent
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn make_manager(limit: usize) -> TaskManager {
        TaskManager::new(limit)
    }

    fn register_task(mgr: &TaskManager) -> Arc<TaskInfo> {
        mgr.register(
            TaskKind::Single,
            "test_strategy",
            "SPY",
            None,
            serde_json::json!({}),
        )
    }

    // 1. register creates a Queued task
    #[test]
    fn test_register_creates_queued_task() {
        let mgr = make_manager(2);
        let task = register_task(&mgr);

        assert_eq!(task.status(), TaskStatus::Queued);
        assert_eq!(task.kind, TaskKind::Single);
        assert_eq!(task.strategy, "test_strategy");
        assert_eq!(task.symbol, "SPY");
        assert_eq!(mgr.list_active().len(), 1);
    }

    // 2. acquire permit starts immediately when a slot is available
    #[tokio::test]
    async fn test_acquire_permit_starts_immediately_when_available() {
        let mgr = make_manager(2);
        let task = register_task(&mgr);

        let _permit = mgr.acquire_permit().await;
        mgr.mark_running(&task.id);

        assert_eq!(task.status(), TaskStatus::Running);
        let m = task.mutable.lock().unwrap();
        assert!(m.started_at.is_some());
    }

    // 3. concurrency limit: second task stays Queued until permit dropped
    #[tokio::test]
    async fn test_concurrency_limit_queues_second_task() {
        let mgr = Arc::new(make_manager(1));
        let task1 = register_task(&mgr);
        let task2 = register_task(&mgr);

        // Acquire the only permit for task1.
        let permit = mgr.acquire_permit().await;
        mgr.mark_running(&task1.id);

        assert_eq!(task1.status(), TaskStatus::Running);
        assert_eq!(task2.status(), TaskStatus::Queued);

        // Drop permit; task2 should now be able to acquire.
        drop(permit);

        let mgr2 = Arc::clone(&mgr);
        let id2 = task2.id.clone();
        let handle = tokio::spawn(async move {
            let _permit2 = mgr2.acquire_permit().await;
            mgr2.mark_running(&id2);
        });
        handle.await.unwrap();

        assert_eq!(task2.status(), TaskStatus::Running);
    }

    // 4. cancel a Queued task
    #[test]
    fn test_cancel_queued_task() {
        let mgr = make_manager(2);
        let task = register_task(&mgr);

        let result = mgr.cancel(&task.id);

        assert!(result);
        assert_eq!(task.status(), TaskStatus::Cancelled);
        assert!(task.cancellation_token.is_cancelled());
    }

    // 5. cancel a Completed task returns false, status stays Completed
    #[test]
    fn test_cancel_completed_task_returns_false() {
        let mgr = make_manager(2);
        let task = register_task(&mgr);

        mgr.mark_completed(&task.id, serde_json::json!({"ok": true}), "rid".to_string());
        assert_eq!(task.status(), TaskStatus::Completed);

        let result = mgr.cancel(&task.id);
        assert!(!result);
        assert_eq!(task.status(), TaskStatus::Completed);
    }

    // 6. queue_position returns 1-indexed positions; shifts after one starts running
    #[test]
    fn test_queue_position() {
        let mgr = make_manager(1);
        let t1 = register_task(&mgr);
        std::thread::sleep(Duration::from_millis(2));
        let t2 = register_task(&mgr);
        std::thread::sleep(Duration::from_millis(2));
        let t3 = register_task(&mgr);

        assert_eq!(mgr.queue_position(&t1.id), Some(1));
        assert_eq!(mgr.queue_position(&t2.id), Some(2));
        assert_eq!(mgr.queue_position(&t3.id), Some(3));

        // Move t1 to Running — it should no longer appear in queue.
        mgr.mark_running(&t1.id);
        assert_eq!(mgr.queue_position(&t1.id), None);
        assert_eq!(mgr.queue_position(&t2.id), Some(1));
        assert_eq!(mgr.queue_position(&t3.id), Some(2));
    }

    // 7. get returns None for unknown ID
    #[test]
    fn test_get_returns_none_for_unknown_id() {
        let mgr = make_manager(2);
        assert!(mgr.get("does-not-exist").is_none());
    }

    // 8. cleanup removes old terminal tasks
    #[test]
    fn test_cleanup_removes_old_terminal_tasks() {
        let mgr = make_manager(2);
        let task = register_task(&mgr);
        let id = task.id.clone();

        mgr.mark_completed(&id, serde_json::json!(null), "rid".to_string());

        // Cleanup with zero max_age: everything completed before now is removed.
        mgr.cleanup(chrono::Duration::zero());
        assert!(mgr.get(&id).is_none());
    }

    // 9. cleanup keeps active (non-terminal) tasks
    #[test]
    fn test_cleanup_keeps_active_tasks() {
        let mgr = make_manager(2);
        let task = register_task(&mgr);
        let id = task.id.clone();

        mgr.cleanup(chrono::Duration::zero());
        assert!(mgr.get(&id).is_some());
        assert_eq!(task.status(), TaskStatus::Queued);
    }

    // 10. mark_cancelled is idempotent
    #[test]
    fn test_mark_cancelled_is_idempotent() {
        let mgr = make_manager(2);
        let task = register_task(&mgr);

        mgr.mark_cancelled(&task.id);
        mgr.mark_cancelled(&task.id); // should not panic
        assert_eq!(task.status(), TaskStatus::Cancelled);
    }
}
