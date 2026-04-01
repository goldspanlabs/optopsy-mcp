//! Shared application state for all REST route handlers.

use std::sync::Arc;

use crate::data::traits::{ChatStore, RunStore};
use crate::server::task_manager::TaskManager;
use crate::server::OptopsyServer;

/// Shared application state passed to all axum handlers via `State`.
#[derive(Clone)]
pub struct AppState {
    pub server: OptopsyServer,
    pub run_store: Arc<dyn RunStore>,
    pub chat_store: Arc<dyn ChatStore>,
    /// Task manager for queued/running backtest and sweep tasks.
    pub task_manager: Arc<TaskManager>,
}
