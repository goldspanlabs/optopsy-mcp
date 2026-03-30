//! Shared application state for all REST route handlers.

use std::sync::Arc;

use crate::data::traits::{BacktestStore, ChatStore, SweepStore};
use crate::server::OptopsyServer;

/// Shared application state passed to all axum handlers via `State`.
#[derive(Clone)]
pub struct AppState {
    pub server: OptopsyServer,
    pub backtest_store: Arc<dyn BacktestStore>,
    pub chat_store: Arc<dyn ChatStore>,
    pub sweep_store: Arc<dyn SweepStore>,
}
