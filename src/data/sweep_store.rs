//! `SQLite`-backed storage for sweep results.
//!
//! Provides [`SqliteSweepStore`] which implements the [`SweepStore`](super::traits::SweepStore)
//! trait for persisting and querying parameter sweep sessions.

use anyhow::{Context, Result};
use rusqlite::OptionalExtension;

use super::database::DbConnection;
use super::traits::{SweepDetail, SweepSummary};

// ──────────────────────────────────────────────────────────────────────────────
// SqliteSweepStore
// ──────────────────────────────────────────────────────────────────────────────

/// `SQLite`-backed store for sweep results.
///
/// Does not own or manage the database connection — receives a shared
/// [`DbConnection`] from [`Database`](super::database::Database).
#[derive(Clone)]
pub struct SqliteSweepStore {
    pub(crate) conn: DbConnection,
}

impl SqliteSweepStore {
    /// Create a new store using a shared database connection.
    ///
    /// Schema must already be initialised by [`Database`](super::database::Database).
    pub fn new(conn: DbConnection) -> Self {
        Self { conn }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // CRUD methods
    // ──────────────────────────────────────────────────────────────────────────

    /// Insert a new sweep result and return its generated UUID and `created_at` timestamp.
    #[allow(clippy::too_many_arguments)]
    pub fn insert(
        &self,
        strategy_key: &str,
        symbol: &str,
        mode: &str,
        objective: &str,
        sweep_config: &serde_json::Value,
        result_json: &str,
        combinations_total: i64,
        execution_time_ms: i64,
    ) -> Result<(String, String)> {
        let id = uuid::Uuid::new_v4().to_string();
        let created_at = chrono::Utc::now().to_rfc3339();
        let sweep_config_str =
            serde_json::to_string(sweep_config).context("Failed to serialize sweep_config")?;

        let conn = self.conn.lock().expect("mutex poisoned");
        conn.execute(
            "INSERT INTO sweeps
                (id, strategy_key, symbol, mode, objective, sweep_config,
                 result_json, combinations_total, execution_time_ms, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            rusqlite::params![
                id,
                strategy_key,
                symbol,
                mode,
                objective,
                sweep_config_str,
                result_json,
                combinations_total,
                execution_time_ms,
                created_at,
            ],
        )
        .context("Failed to insert into sweeps")?;

        Ok((id, created_at))
    }

    /// Retrieve a full sweep detail by its UUID.
    ///
    /// Returns `None` if the id does not exist.
    pub fn get_detail(&self, id: &str) -> Result<Option<SweepDetail>> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let row = conn
            .query_row(
                "SELECT id, strategy_key, symbol, mode, objective, sweep_config,
                        result_json, combinations_total, execution_time_ms, created_at, analysis
                 FROM sweeps WHERE id = ?1",
                rusqlite::params![id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, i64>(7)?,
                        row.get::<_, i64>(8)?,
                        row.get::<_, String>(9)?,
                        row.get::<_, Option<String>>(10)?,
                    ))
                },
            )
            .optional()
            .context("Failed to query sweep detail")?;

        let Some((
            id,
            strategy_key,
            symbol,
            mode,
            objective,
            sweep_config_str,
            result_json_str,
            combinations_total,
            execution_time_ms,
            created_at,
            analysis,
        )) = row
        else {
            return Ok(None);
        };

        let sweep_config: serde_json::Value =
            serde_json::from_str(&sweep_config_str).context("Failed to parse sweep_config")?;
        let result: serde_json::Value =
            serde_json::from_str(&result_json_str).context("Failed to parse result_json")?;

        Ok(Some(SweepDetail {
            id,
            strategy_key,
            symbol,
            mode,
            objective,
            sweep_config,
            result,
            combinations_total,
            execution_time_ms,
            created_at,
            analysis,
        }))
    }

    /// List sweep summaries, optionally filtered by strategy key.
    ///
    /// Results are ordered newest-first by `created_at`.
    pub fn list(&self, strategy_key: Option<&str>) -> Result<Vec<SweepSummary>> {
        let conn = self.conn.lock().expect("mutex poisoned");

        let (sql, params_vec): (String, Vec<Box<dyn rusqlite::types::ToSql>>) =
            if let Some(key) = strategy_key {
                (
                    "SELECT id, strategy_key, symbol, mode, objective, result_json,
                            combinations_total, execution_time_ms, created_at
                     FROM sweeps WHERE strategy_key = ?1
                     ORDER BY created_at DESC"
                        .to_string(),
                    vec![Box::new(key.to_owned())],
                )
            } else {
                (
                    "SELECT id, strategy_key, symbol, mode, objective, result_json,
                            combinations_total, execution_time_ms, created_at
                     FROM sweeps ORDER BY created_at DESC"
                        .to_string(),
                    vec![],
                )
            };

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(std::convert::AsRef::as_ref).collect();

        let mut stmt = conn
            .prepare(&sql)
            .context("Failed to prepare sweep list query")?;

        let rows = stmt
            .query_map(params_refs.as_slice(), |row| {
                let result_json_str: String = row.get(5)?;
                let result_json = serde_json::from_str::<serde_json::Value>(&result_json_str).ok();
                let best_sharpe = result_json
                    .as_ref()
                    .and_then(|v| v.get("best_sharpe").and_then(serde_json::Value::as_f64));
                let combinations_run = result_json
                    .as_ref()
                    .and_then(|v| {
                        v.get("combinations_run")
                            .and_then(serde_json::Value::as_i64)
                    })
                    .unwrap_or(0);

                Ok(SweepSummary {
                    id: row.get(0)?,
                    strategy_key: row.get(1)?,
                    symbol: row.get(2)?,
                    mode: row.get(3)?,
                    objective: row.get(4)?,
                    combinations_total: row.get(6)?,
                    combinations_run,
                    execution_time_ms: row.get(7)?,
                    best_sharpe,
                    created_at: row.get(8)?,
                })
            })
            .context("Failed to query sweep list")?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to collect sweep summaries")?;

        Ok(rows)
    }

    /// Delete a sweep by id. Returns `true` if a row was deleted.
    pub fn delete(&self, id: &str) -> Result<bool> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let n = conn
            .execute("DELETE FROM sweeps WHERE id = ?1", rusqlite::params![id])
            .context("Failed to delete sweep")?;
        Ok(n > 0)
    }

    /// Save AI-generated analysis text for a sweep.
    pub fn set_analysis(&self, id: &str, analysis: &str) -> Result<bool> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let rows = conn
            .execute(
                "UPDATE sweeps SET analysis = ?2 WHERE id = ?1",
                rusqlite::params![id, analysis],
            )
            .context("Failed to update sweep analysis")?;
        Ok(rows > 0)
    }
}

impl super::traits::SweepStore for SqliteSweepStore {
    fn insert(
        &self,
        strategy_key: &str,
        symbol: &str,
        mode: &str,
        objective: &str,
        sweep_config: &serde_json::Value,
        result_json: &str,
        combinations_total: i64,
        execution_time_ms: i64,
    ) -> Result<(String, String)> {
        SqliteSweepStore::insert(
            self,
            strategy_key,
            symbol,
            mode,
            objective,
            sweep_config,
            result_json,
            combinations_total,
            execution_time_ms,
        )
    }

    fn get_detail(&self, id: &str) -> Result<Option<SweepDetail>> {
        SqliteSweepStore::get_detail(self, id)
    }

    fn list(&self, strategy_key: Option<&str>) -> Result<Vec<SweepSummary>> {
        SqliteSweepStore::list(self, strategy_key)
    }

    fn delete(&self, id: &str) -> Result<bool> {
        SqliteSweepStore::delete(self, id)
    }

    fn set_analysis(&self, id: &str, analysis: &str) -> Result<bool> {
        SqliteSweepStore::set_analysis(self, id, analysis)
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_store() -> SqliteSweepStore {
        crate::data::database::Database::open_in_memory()
            .expect("open_in_memory")
            .sweeps()
    }

    #[test]
    fn test_insert_and_get_detail() {
        let store = make_store();
        let config = serde_json::json!({"dte": [30, 45, 60]});
        let result = serde_json::json!({"best_sharpe": 1.5, "results": []});
        let result_str = serde_json::to_string(&result).unwrap();

        let (id, created_at) = store
            .insert(
                "strat_a",
                "SPY",
                "grid",
                "sharpe",
                &config,
                &result_str,
                100,
                5000,
            )
            .expect("insert");

        assert!(!id.is_empty());
        assert!(!created_at.is_empty());

        let detail = store.get_detail(&id).unwrap().expect("should exist");
        assert_eq!(detail.strategy_key, "strat_a");
        assert_eq!(detail.symbol, "SPY");
        assert_eq!(detail.mode, "grid");
        assert_eq!(detail.objective, "sharpe");
        assert_eq!(detail.combinations_total, 100);
        assert_eq!(detail.execution_time_ms, 5000);
        assert!(detail.analysis.is_none());
    }

    #[test]
    fn test_get_detail_not_found() {
        let store = make_store();
        assert!(store.get_detail("nonexistent").unwrap().is_none());
    }

    #[test]
    fn test_list_all_and_filtered() {
        let store = make_store();
        let config = serde_json::json!({});
        let result = serde_json::json!({"best_sharpe": 2.0});
        let result_str = serde_json::to_string(&result).unwrap();

        store
            .insert(
                "strat_a",
                "SPY",
                "grid",
                "sharpe",
                &config,
                &result_str,
                10,
                100,
            )
            .unwrap();
        store
            .insert(
                "strat_a",
                "QQQ",
                "grid",
                "sharpe",
                &config,
                &result_str,
                20,
                200,
            )
            .unwrap();
        store
            .insert(
                "strat_b",
                "SPY",
                "bayesian",
                "sharpe",
                &config,
                &result_str,
                30,
                300,
            )
            .unwrap();

        // All
        assert_eq!(store.list(None).unwrap().len(), 3);

        // Filtered
        assert_eq!(store.list(Some("strat_a")).unwrap().len(), 2);
        assert_eq!(store.list(Some("strat_b")).unwrap().len(), 1);
        assert_eq!(store.list(Some("nonexistent")).unwrap().len(), 0);
    }

    #[test]
    fn test_list_extracts_best_sharpe() {
        let store = make_store();
        let config = serde_json::json!({});
        let result = serde_json::json!({"best_sharpe": 1.75});
        let result_str = serde_json::to_string(&result).unwrap();

        store
            .insert(
                "strat_a",
                "SPY",
                "grid",
                "sharpe",
                &config,
                &result_str,
                10,
                100,
            )
            .unwrap();

        let summaries = store.list(None).unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].best_sharpe, Some(1.75));
    }

    #[test]
    fn test_delete() {
        let store = make_store();
        let config = serde_json::json!({});

        let (id, _) = store
            .insert("s", "SPY", "grid", "sharpe", &config, "{}", 1, 1)
            .unwrap();

        assert!(store.delete(&id).unwrap());
        assert!(store.get_detail(&id).unwrap().is_none());
        assert!(!store.delete(&id).unwrap());
    }

    #[test]
    fn test_set_analysis() {
        let store = make_store();
        let config = serde_json::json!({});

        let (id, _) = store
            .insert("s", "SPY", "grid", "sharpe", &config, "{}", 1, 1)
            .unwrap();

        assert!(store.set_analysis(&id, "Great results").unwrap());
        let detail = store.get_detail(&id).unwrap().expect("should exist");
        assert_eq!(detail.analysis.as_deref(), Some("Great results"));

        // Non-existent id
        assert!(!store.set_analysis("nonexistent", "text").unwrap());
    }
}
