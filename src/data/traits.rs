//! Abstract storage traits for backtests and strategies.
//!
//! These traits decouple the application from a specific database backend.
//! The default implementation uses `SQLite` (see `backtest_store` and `strategy_store`),
//! but alternative backends (e.g. Postgres) can be swapped in by implementing
//! these traits.

use anyhow::Result;
use serde_json::Value;

use super::backtest_store::{BacktestDetail, BacktestSummary, MetricsRow, TradeRow};
use super::strategy_store::StrategyRow;
use crate::scripting::stdlib::ScriptMeta;

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

    /// List all strategies, ordered by name.
    fn list(&self) -> Result<Vec<StrategyRow>>;

    /// List strategies as `ScriptMeta` with extern params extracted.
    fn list_scripts(&self) -> Result<Vec<ScriptMeta>>;

    /// Insert or update a strategy.
    fn upsert(&self, row: &StrategyRow) -> Result<()>;

    /// Delete a strategy by id. Returns `true` if a row was deleted.
    fn delete(&self, id: &str) -> Result<bool>;
}
