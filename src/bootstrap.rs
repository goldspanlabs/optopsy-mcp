//! Runtime bootstrap helpers for validated store/task wiring.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::data::adjustment_store::SqliteAdjustmentStore;
use crate::data::database::Database;
use crate::data::forward_test_store::SqliteForwardTestStore;
use crate::data::traits::{self, ChatStore, RunStore, StrategyStore};
use crate::server::state::AppState;
use crate::server::task_manager::TaskManager;
use crate::server::OptopsyServer;

/// Validated runtime services shared across transports.
#[derive(Clone)]
pub struct AppServices {
    pub strategy_store: Arc<dyn StrategyStore>,
    pub run_store: Arc<dyn RunStore>,
    pub chat_store: Arc<dyn ChatStore>,
    pub adjustment_store: Arc<SqliteAdjustmentStore>,
    pub forward_test_store: Arc<SqliteForwardTestStore>,
    pub task_manager: Arc<TaskManager>,
}

impl AppServices {
    /// Build the shared runtime services from environment configuration.
    pub fn from_env() -> Result<Self> {
        let data_root = std::env::var("DATA_ROOT").unwrap_or_else(|_| "data".to_string());
        let db_path = PathBuf::from(&data_root).join("optopsy.db");
        ensure_parent_dir(&db_path)?;

        let db = Database::open(&db_path)?;
        let strategy_store: Arc<dyn StrategyStore> = Arc::new(db.strategies());
        let run_store: Arc<dyn RunStore> = Arc::new(db.runs());
        let chat_store: Arc<dyn ChatStore> = Arc::new(db.chat());
        let adjustment_store = Arc::new(db.adjustments());
        let forward_test_store = Arc::new(db.forward_tests());

        let seeded = traits::seed_strategies_if_empty(
            strategy_store.as_ref(),
            Path::new("scripts/strategies"),
        )?;
        if seeded > 0 {
            tracing::info!("Seeded {seeded} strategies from scripts/strategies/");
        }

        let max_concurrent_tasks = std::env::var("MAX_CONCURRENT_TASKS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1usize);
        let task_manager = Arc::new(TaskManager::new(max_concurrent_tasks));

        Ok(Self {
            strategy_store,
            run_store,
            chat_store,
            adjustment_store,
            forward_test_store,
            task_manager,
        })
    }

    /// Construct a fully-wired server for the given cache.
    pub fn build_server(&self, cache: Arc<crate::data::cache::CachedStore>) -> OptopsyServer {
        OptopsyServer::with_all_stores(
            cache,
            Arc::clone(&self.strategy_store),
            Arc::clone(&self.run_store),
            Arc::clone(&self.adjustment_store),
        )
        .with_forward_test_store(Arc::clone(&self.forward_test_store))
    }

    /// Construct the shared HTTP application state for REST handlers.
    pub fn build_app_state(&self, cache: Arc<crate::data::cache::CachedStore>) -> AppState {
        AppState {
            server: self.build_server(cache),
            run_store: Arc::clone(&self.run_store),
            chat_store: Arc::clone(&self.chat_store),
            task_manager: Arc::clone(&self.task_manager),
            forward_test_store: Arc::clone(&self.forward_test_store),
        }
    }
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create data directory: {}", parent.display()))?;
    }
    Ok(())
}
