//! Shared application state for all REST route handlers.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::data::traits::{BacktestStore, ChatStore, SweepStore};
use crate::server::OptopsyServer;

/// Shared application state passed to all axum handlers via `State`.
#[derive(Clone)]
pub struct AppState {
    pub server: OptopsyServer,
    pub backtest_store: Arc<dyn BacktestStore>,
    pub chat_store: Arc<dyn ChatStore>,
    pub sweep_store: Arc<dyn SweepStore>,
    /// Set of sweep run IDs that have been requested to cancel.
    pub sweep_cancellations: Arc<Mutex<HashSet<String>>>,
}
