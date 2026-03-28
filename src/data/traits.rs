//! Abstract storage traits for backtests and strategies.
//!
//! These traits decouple the application from a specific database backend.
//! The default implementation uses `SQLite` (see `backtest_store` and `strategy_store`),
//! but alternative backends (e.g. Postgres) can be swapped in by implementing
//! these traits.

use std::path::Path;

use anyhow::Result;
use serde_json::Value;

use super::backtest_store::{BacktestDetail, BacktestSummary, MetricsRow, TradeRow};
use super::strategy_store::StrategyRow;
use crate::scripting::stdlib::{parse_script_meta, ScriptMeta};

// ──────────────────────────────────────────────────────────────────────────────
// BacktestStore trait
// ──────────────────────────────────────────────────────────────────────────────

/// Storage backend for backtest results, metrics, and trade records.
pub trait BacktestStore: Send + Sync {
    /// Insert a new backtest result. Returns `(id, created_at)`.
    #[allow(clippy::too_many_arguments)]
    fn insert(
        &self,
        strategy_key: &str,
        symbol: &str,
        capital: f64,
        params: &Value,
        metrics: &MetricsRow,
        trades: &[TradeRow],
        result_json: &str,
        execution_time_ms: i64,
        hypothesis: Option<&str>,
        tags: Option<&[String]>,
        regime: Option<&[String]>,
    ) -> Result<(String, String)>;

    /// Retrieve a full backtest detail by id.
    fn get_detail(&self, id: &str) -> Result<Option<BacktestDetail>>;

    /// Save AI-generated analysis text for a backtest.
    fn set_analysis(&self, id: &str, analysis: &str) -> Result<bool>;

    /// List backtest summaries with optional filters.
    fn list(
        &self,
        strategy: Option<&str>,
        symbol: Option<&str>,
        tag: Option<&str>,
        regime: Option<&str>,
    ) -> Result<Vec<BacktestSummary>>;

    /// Delete a backtest by id. Returns `true` if a row was deleted.
    fn delete(&self, id: &str) -> Result<bool>;

    /// Retrieve all trades for a given backtest.
    fn get_trades(&self, backtest_id: &str) -> Result<Vec<TradeRow>>;
}

// ──────────────────────────────────────────────────────────────────────────────
// StrategyStore trait
// ──────────────────────────────────────────────────────────────────────────────

/// Storage backend for Rhai strategy scripts and their metadata.
pub trait StrategyStore: Send + Sync {
    /// Get a single strategy by id.
    fn get(&self, id: &str) -> Result<Option<StrategyRow>>;

    /// Get just the source code for a strategy (hot path for `run_script`).
    fn get_source(&self, id: &str) -> Result<Option<String>>;

    /// Return the number of strategies in the store.
    fn count(&self) -> Result<usize>;

    /// List all strategies, ordered by name.
    fn list(&self) -> Result<Vec<StrategyRow>>;

    /// List strategies as `ScriptMeta` with extern params extracted.
    fn list_scripts(&self) -> Result<Vec<ScriptMeta>>;

    /// Insert or update a strategy.
    fn upsert(&self, row: &StrategyRow) -> Result<()>;

    /// Delete a strategy by id. Returns `true` if a row was deleted.
    fn delete(&self, id: &str) -> Result<bool>;
}

// ──────────────────────────────────────────────────────────────────────────────
// ChatStore trait
// ──────────────────────────────────────────────────────────────────────────────

/// A thread row stored in the database.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ThreadRow {
    pub id: String,
    pub title: Option<String>,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
}

/// A message row stored in the database.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MessageRow {
    pub id: String,
    pub thread_id: String,
    pub parent_id: Option<String>,
    pub format: String,
    pub content: String,
    pub created_at: String,
}

/// A result row stored in the database.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ResultRow {
    pub id: i64,
    pub thread_id: String,
    pub key: String,
    #[serde(rename = "type")]
    pub result_type: String,
    pub label: String,
    pub tool_call_id: Option<String>,
    pub params: String,
    pub data: Option<String>,
    pub created_at: String,
}

/// Input for upserting a result (no id or `created_at` — DB assigns those).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ResultInput {
    pub key: String,
    #[serde(rename = "type")]
    pub result_type: String,
    pub label: String,
    pub tool_call_id: Option<String>,
    pub params: String,
    pub data: Option<String>,
}

/// Storage backend for chat threads, messages, and results.
pub trait ChatStore: Send + Sync {
    /// List all threads, ordered by most recently updated.
    fn list_threads(&self) -> Result<Vec<ThreadRow>>;

    /// Get a single thread by id.
    fn get_thread(&self, id: &str) -> Result<Option<ThreadRow>>;

    /// Create a new thread with the given id. Returns the created row.
    fn create_thread(&self, id: &str) -> Result<ThreadRow>;

    /// Update a thread's title and/or status.
    fn update_thread(&self, id: &str, title: Option<&str>, status: Option<&str>) -> Result<bool>;

    /// Delete a thread (cascades to messages and results).
    fn delete_thread(&self, id: &str) -> Result<bool>;

    /// Get messages for a thread with pagination.
    fn get_messages(&self, thread_id: &str, limit: i64, offset: i64) -> Result<Vec<MessageRow>>;

    /// Insert or update a message.
    fn upsert_message(&self, msg: &MessageRow) -> Result<()>;

    /// Delete all messages for a thread.
    fn delete_messages(&self, thread_id: &str) -> Result<bool>;

    /// Get all results for a thread.
    fn get_results(&self, thread_id: &str) -> Result<Vec<ResultRow>>;

    /// Replace all results for a thread (delete existing, insert new).
    fn replace_all_results(&self, thread_id: &str, results: &[ResultInput]) -> Result<()>;

    /// Delete a single result by `thread_id` and key.
    fn delete_result(&self, thread_id: &str, key: &str) -> Result<bool>;
}

// ──────────────────────────────────────────────────────────────────────────────
// Backend-agnostic seeding
// ──────────────────────────────────────────────────────────────────────────────

/// One-time migration: seed strategies from `.rhai` files if the store is empty.
///
/// Works with any `StrategyStore` backend (`SQLite`, Postgres, etc.). Call on
/// startup — if the store already has strategies, this is a no-op. Returns
/// the number of strategies seeded.
pub fn seed_strategies_if_empty(store: &dyn StrategyStore, scripts_dir: &Path) -> Result<usize> {
    if store.count()? > 0 {
        return Ok(0);
    }

    let Ok(entries) = std::fs::read_dir(scripts_dir) else {
        return Ok(0);
    };

    let mut seeded = 0;
    for entry in entries.flatten() {
        let filename = entry.file_name().to_string_lossy().to_string();
        let Some(id) = filename.strip_suffix(".rhai") else {
            continue;
        };
        let Ok(source) = std::fs::read_to_string(entry.path()) else {
            continue;
        };

        let meta = parse_script_meta(id, &source);
        store.upsert(&StrategyRow {
            id: id.to_string(),
            name: meta.name,
            description: meta.description,
            category: meta.category,
            hypothesis: meta.hypothesis,
            tags: meta.tags,
            regime: meta.regime,
            source,
            created_at: String::new(),
            updated_at: String::new(),
        })?;
        seeded += 1;
    }

    Ok(seeded)
}
