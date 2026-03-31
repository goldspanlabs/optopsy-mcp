//! Abstract storage traits for runs, strategies, and chat.
//!
//! These traits decouple the application from a specific database backend.
//! The default implementation uses `SQLite` (see `run_store`, `strategy_store`,
//! and `chat_store`), but alternative backends can be swapped in by implementing
//! these traits.

use std::path::Path;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::strategy_store::StrategyRow;
use crate::scripting::stdlib::{parse_script_meta, ScriptMeta};

// ──────────────────────────────────────────────────────────────────────────────
// RunStore types
// ──────────────────────────────────────────────────────────────────────────────

/// A single trade record associated with a run.
///
/// Field names and types intentionally mirror `TradeRecord` (the MCP response
/// struct) so that the REST API and the MCP tool return identical JSON shapes,
/// and the frontend can use one `TradeLogEntry` interface for both.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRow {
    pub trade_id: i64,
    pub entry_datetime: i64,
    pub exit_datetime: i64,
    pub entry_cost: f64,
    pub exit_proceeds: f64,
    pub entry_amount: f64,
    pub entry_label: String,
    pub exit_amount: f64,
    pub exit_label: String,
    pub pnl: f64,
    pub days_held: i64,
    pub exit_type: String,
    pub legs: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub computed_quantity: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_equity: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stock_entry_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stock_exit_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stock_pnl: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
}

/// Summary view of a run (no trades, no result_json).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSummary {
    pub id: String,
    pub sweep_id: Option<String>,
    pub strategy_id: Option<String>,
    pub strategy_name: Option<String>,
    pub symbol: String,
    pub params: Value,
    pub total_return: Option<f64>,
    pub win_rate: Option<f64>,
    pub max_drawdown: Option<f64>,
    pub sharpe: Option<f64>,
    pub sortino: Option<f64>,
    pub cagr: Option<f64>,
    pub profit_factor: Option<f64>,
    pub trade_count: Option<i64>,
    pub created_at: String,
}

/// Full run detail including trades and result blob.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunDetail {
    pub id: String,
    pub sweep_id: Option<String>,
    pub strategy_id: Option<String>,
    pub strategy_name: Option<String>,
    pub symbol: String,
    pub capital: f64,
    pub params: Value,
    pub total_return: Option<f64>,
    pub win_rate: Option<f64>,
    pub max_drawdown: Option<f64>,
    pub sharpe: Option<f64>,
    pub sortino: Option<f64>,
    pub cagr: Option<f64>,
    pub profit_factor: Option<f64>,
    pub trade_count: Option<i64>,
    pub expectancy: Option<f64>,
    pub var_95: Option<f64>,
    pub result_json: Option<Value>,
    pub trades: Vec<TradeRow>,
    pub execution_time_ms: Option<i64>,
    pub analysis: Option<String>,
    pub hypothesis: Option<String>,
    pub tags: Option<String>,
    pub regime: Option<String>,
    pub created_at: String,
}

/// A row in the unified list — either a standalone run or a sweep group.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RunRow {
    #[serde(rename = "single")]
    Single(RunSummary),
    #[serde(rename = "sweep")]
    Sweep {
        sweep_id: String,
        strategy_id: Option<String>,
        strategy_name: Option<String>,
        symbol: String,
        combinations: i64,
        best_return: Option<f64>,
        best_win_rate: Option<f64>,
        best_max_dd: Option<f64>,
        best_sharpe: Option<f64>,
        best_sortino: Option<f64>,
        best_cagr: Option<f64>,
        best_profit_factor: Option<f64>,
        best_trade_count: Option<i64>,
        created_at: String,
    },
}

/// Aggregate overview stats for the runs list header.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunsOverview {
    pub total_runs: i64,
    pub last_run_at: Option<String>,
    pub best_return: Option<f64>,
    pub best_sharpe: Option<f64>,
    pub avg_win_rate: Option<f64>,
    pub avg_sharpe: Option<f64>,
}

/// Combined response for the runs list endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunsListResponse {
    pub overview: RunsOverview,
    pub rows: Vec<RunRow>,
}

/// Full sweep detail with its child runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweepDetail {
    pub id: String,
    pub strategy_id: Option<String>,
    pub strategy_name: Option<String>,
    pub symbol: String,
    pub sweep_config: Value,
    pub objective: String,
    pub mode: String,
    pub combinations: i64,
    pub execution_time_ms: Option<i64>,
    pub analysis: Option<String>,
    pub created_at: String,
    pub runs: Vec<RunSummary>,
}

// ──────────────────────────────────────────────────────────────────────────────
// RunStore trait
// ──────────────────────────────────────────────────────────────────────────────

/// Storage backend for unified backtest runs and sweep sessions.
pub trait RunStore: Send + Sync {
    /// Insert a new run. Returns `created_at` timestamp.
    #[allow(clippy::too_many_arguments)]
    fn insert_run(
        &self,
        id: &str,
        sweep_id: Option<&str>,
        strategy_id: Option<&str>,
        symbol: &str,
        capital: f64,
        params: &Value,
        total_return: Option<f64>,
        win_rate: Option<f64>,
        max_drawdown: Option<f64>,
        sharpe: Option<f64>,
        sortino: Option<f64>,
        cagr: Option<f64>,
        profit_factor: Option<f64>,
        trade_count: Option<i64>,
        expectancy: Option<f64>,
        var_95: Option<f64>,
        result_json: &str,
        execution_time_ms: Option<i64>,
        hypothesis: Option<&str>,
        tags: Option<&str>,
        regime: Option<&str>,
    ) -> Result<String>;

    /// Insert trades for a run.
    fn insert_trades(&self, run_id: &str, trades: &[TradeRow]) -> Result<()>;

    /// Insert a new sweep session. Returns `created_at` timestamp.
    #[allow(clippy::too_many_arguments)]
    fn insert_sweep(
        &self,
        id: &str,
        strategy_id: Option<&str>,
        symbol: &str,
        sweep_config: &Value,
        objective: &str,
        mode: &str,
        combinations: i64,
        execution_time_ms: Option<i64>,
    ) -> Result<String>;

    /// List all runs and sweeps, newest first.
    fn list(&self) -> Result<RunsListResponse>;

    /// Get full detail for a single run by id.
    fn get_run(&self, id: &str) -> Result<Option<RunDetail>>;

    /// Get full detail for a sweep by id, including its child runs.
    fn get_sweep(&self, id: &str) -> Result<Option<SweepDetail>>;

    /// Delete a run by id. Returns `true` if a row was deleted.
    fn delete_run(&self, id: &str) -> Result<bool>;

    /// Delete a sweep and its runs (CASCADE). Returns `true` if a row was deleted.
    fn delete_sweep(&self, id: &str) -> Result<bool>;

    /// Save AI-generated analysis text for a run.
    fn set_run_analysis(&self, id: &str, analysis: &str) -> Result<bool>;

    /// Save AI-generated analysis text for a sweep.
    fn set_sweep_analysis(&self, id: &str, analysis: &str) -> Result<bool>;
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

    /// Get a strategy's source by display name (case-insensitive).
    /// Returns `Option<(id, source)>` — the resolved UUID and source code.
    fn get_source_by_name(&self, name: &str) -> Result<Option<(String, String)>>;

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
    pub strategy_id: Option<String>,
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

    /// List threads for a strategy, ordered by most recently updated.
    fn list_threads_for_strategy(&self, strategy_id: &str) -> Result<Vec<ThreadRow>>;

    /// Get a single thread by id.
    fn get_thread(&self, id: &str) -> Result<Option<ThreadRow>>;

    /// Create a new thread with the given id. Returns the created row.
    fn create_thread(&self, id: &str) -> Result<ThreadRow>;

    /// Create a thread associated with a strategy.
    fn create_strategy_thread(&self, id: &str, strategy_id: &str) -> Result<ThreadRow>;

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
            id: uuid::Uuid::new_v4().to_string(),
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
