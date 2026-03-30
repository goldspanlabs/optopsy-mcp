//! Shared application state for all REST route handlers.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::data::traits::{ChatStore, RunStore};
use crate::server::OptopsyServer;

/// Shared application state passed to all axum handlers via `State`.
#[derive(Clone)]
pub struct AppState {
    pub server: OptopsyServer,
    pub run_store: Arc<dyn RunStore>,
    pub chat_store: Arc<dyn ChatStore>,
    /// Set of sweep run IDs that have been requested to cancel.
    pub sweep_cancellations: Arc<Mutex<HashSet<String>>>,
}
