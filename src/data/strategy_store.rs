//! SQLite-backed storage for strategy scripts.
//!
//! Provides [`StrategyStore`] for persisting and querying Rhai strategy scripts,
//! their metadata, and source code. Supports seeding built-in strategies from
//! the `scripts/strategies/` directory on startup.

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::scripting::stdlib::{extract_extern_params, parse_script_meta, ScriptMeta};

// ──────────────────────────────────────────────────────────────────────────────
// Row types
// ──────────────────────────────────────────────────────────────────────────────

/// A strategy row stored in the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyRow {
    /// Unique identifier (filename stem, e.g. `ibs_mean_reversion`).
    pub id: String,
    /// Human-readable display name.
    pub name: String,
    /// One-line description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Strategy category for UI grouping (e.g. "stock", "options", "wheel").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    /// Research hypothesis this strategy is testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hypothesis: Option<String>,
    /// Searchable tags (stored as JSON array string in DB).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    /// Expected market regime(s) (stored as JSON array string in DB).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regime: Option<Vec<String>>,
    /// Full Rhai script source code.
    pub source: String,
    /// Whether this strategy was seeded from built-in files.
    pub is_builtin: bool,
    pub created_at: String,
    pub updated_at: String,
}

impl StrategyRow {
    /// Convert to `ScriptMeta`, extracting `extern()` params from source.
    pub fn into_script_meta(self) -> ScriptMeta {
        let params = extract_extern_params(&self.source);
        let profiles = {
            let meta = parse_script_meta(&self.id, &self.source);
            meta.profiles
        };

        ScriptMeta {
            id: self.id,
            name: self.name,
            description: self.description,
            category: self.category,
            params,
            hypothesis: self.hypothesis,
            tags: self.tags,
            regime: self.regime,
            profiles,
        }
    }

    /// Convert to `ScriptMeta` without extracting extern params (fast path for listing).
    pub fn to_script_meta_fast(&self) -> ScriptMeta {
        let profiles = {
            let meta = parse_script_meta(&self.id, &self.source);
            meta.profiles
        };

        ScriptMeta {
            id: self.id.clone(),
            name: self.name.clone(),
            description: self.description.clone(),
            category: self.category.clone(),
            params: Vec::new(),
            hypothesis: self.hypothesis.clone(),
            tags: self.tags.clone(),
            regime: self.regime.clone(),
            profiles,
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// StrategyStore
// ──────────────────────────────────────────────────────────────────────────────

/// Thread-safe `SQLite` store for strategy scripts.
#[derive(Clone)]
pub struct StrategyStore {
    conn: Arc<Mutex<Connection>>,
}

impl StrategyStore {
    /// Open (or create) a file-based `SQLite` database and initialise the schema.
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)
            .with_context(|| format!("Failed to open SQLite database at {}", path.display()))?;
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        store.init_schema()?;
        Ok(store)
    }

    /// Open an in-memory `SQLite` database (intended for tests).
    pub fn open_in_memory() -> Result<Self> {
        let conn =
            Connection::open_in_memory().context("Failed to open in-memory SQLite database")?;
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        store.init_schema()?;
        Ok(store)
    }

    /// Create the `strategies` table if it does not already exist.
    fn init_schema(&self) -> Result<()> {
        let conn = self.conn.lock().expect("mutex poisoned");
        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;

            CREATE TABLE IF NOT EXISTS strategies (
                id              TEXT PRIMARY KEY,
                name            TEXT NOT NULL,
                description     TEXT,
                category        TEXT,
                hypothesis      TEXT,
                tags            TEXT,
                regime          TEXT,
                source          TEXT NOT NULL,
                is_builtin      INTEGER NOT NULL DEFAULT 0,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_strategies_category
                ON strategies(category);
            ",
        )
        .context("Failed to initialise strategies schema")?;
        Ok(())
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Seeding
    // ──────────────────────────────────────────────────────────────────────────

    /// Seed built-in strategies from `.rhai` files in the given directory.
    ///
    /// Uses `INSERT OR REPLACE` for rows with `is_builtin = 1`, so re-running
    /// on startup is idempotent and updates built-in scripts without touching
    /// user-created strategies.
    ///
    /// Returns the number of scripts seeded.
    pub fn seed_builtins(&self, scripts_dir: &Path) -> Result<usize> {
        let Ok(entries) = std::fs::read_dir(scripts_dir) else {
            return Ok(0); // directory doesn't exist — skip silently
        };

        let now = chrono::Utc::now().to_rfc3339();
        let conn = self.conn.lock().expect("mutex poisoned");
        let mut count = 0;

        for entry in entries.flatten() {
            let filename = entry.file_name().to_string_lossy().to_string();
            let Some(id) = filename.strip_suffix(".rhai") else {
                continue;
            };

            let Ok(source) = std::fs::read_to_string(entry.path()) else {
                continue;
            };

            let meta = parse_script_meta(id, &source);
            let tags_json = meta
                .tags
                .as_ref()
                .map(|t| serde_json::to_string(t).unwrap_or_default());
            let regime_json = meta
                .regime
                .as_ref()
                .map(|r| serde_json::to_string(r).unwrap_or_default());

            conn.execute(
                "INSERT INTO strategies (id, name, description, category, hypothesis, tags, regime, source, is_builtin, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 1, ?9, ?10)
                 ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    description = excluded.description,
                    category = excluded.category,
                    hypothesis = excluded.hypothesis,
                    tags = excluded.tags,
                    regime = excluded.regime,
                    source = excluded.source,
                    updated_at = excluded.updated_at
                 WHERE is_builtin = 1",
                rusqlite::params![
                    id,
                    meta.name,
                    meta.description,
                    meta.category,
                    meta.hypothesis,
                    tags_json,
                    regime_json,
                    source,
                    now,
                    now,
                ],
            ).with_context(|| format!("Failed to seed strategy '{id}'"))?;

            count += 1;
        }

        Ok(count)
    }

    // ──────────────────────────────────────────────────────────────────────────
    // CRUD
    // ──────────────────────────────────────────────────────────────────────────

    /// Get a single strategy by id. Returns `None` if not found.
    pub fn get(&self, id: &str) -> Result<Option<StrategyRow>> {
        let conn = self.conn.lock().expect("mutex poisoned");
        conn.query_row(
            "SELECT id, name, description, category, hypothesis, tags, regime, source, is_builtin, created_at, updated_at
             FROM strategies WHERE id = ?1",
            rusqlite::params![id],
            |row| Ok(row_to_strategy(row)),
        )
        .optional()
        .context("Failed to query strategy")
    }

    /// Get just the source code for a strategy (hot path for `run_script`).
    pub fn get_source(&self, id: &str) -> Result<Option<String>> {
        let conn = self.conn.lock().expect("mutex poisoned");
        conn.query_row(
            "SELECT source FROM strategies WHERE id = ?1",
            rusqlite::params![id],
            |row| row.get(0),
        )
        .optional()
        .context("Failed to query strategy source")
    }

    /// List all strategies, ordered by name.
    pub fn list(&self) -> Result<Vec<StrategyRow>> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT id, name, description, category, hypothesis, tags, regime, source, is_builtin, created_at, updated_at
                 FROM strategies ORDER BY name",
            )
            .context("Failed to prepare list query")?;

        let rows = stmt
            .query_map([], |row| Ok(row_to_strategy(row)))
            .context("Failed to query strategies")?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to collect strategies")?;

        Ok(rows)
    }

    /// List strategies as `ScriptMeta` with extern params extracted.
    pub fn list_scripts(&self) -> Result<Vec<ScriptMeta>> {
        let rows = self.list()?;
        Ok(rows
            .into_iter()
            .map(StrategyRow::into_script_meta)
            .collect())
    }

    /// Insert or update a strategy.
    pub fn upsert(&self, row: &StrategyRow) -> Result<()> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let tags_json = row
            .tags
            .as_ref()
            .map(|t| serde_json::to_string(t).unwrap_or_default());
        let regime_json = row
            .regime
            .as_ref()
            .map(|r| serde_json::to_string(r).unwrap_or_default());
        let now = chrono::Utc::now().to_rfc3339();

        conn.execute(
            "INSERT INTO strategies (id, name, description, category, hypothesis, tags, regime, source, is_builtin, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                description = excluded.description,
                category = excluded.category,
                hypothesis = excluded.hypothesis,
                tags = excluded.tags,
                regime = excluded.regime,
                source = excluded.source,
                updated_at = excluded.updated_at",
            rusqlite::params![
                row.id,
                row.name,
                row.description,
                row.category,
                row.hypothesis,
                tags_json,
                regime_json,
                row.source,
                i32::from(row.is_builtin),
                now,
                now,
            ],
        )
        .context("Failed to upsert strategy")?;
        Ok(())
    }

    /// Delete a strategy by id. Returns `true` if a row was deleted.
    pub fn delete(&self, id: &str) -> Result<bool> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let n = conn
            .execute(
                "DELETE FROM strategies WHERE id = ?1",
                rusqlite::params![id],
            )
            .context("Failed to delete strategy")?;
        Ok(n > 0)
    }
}

/// Map a rusqlite row to a `StrategyRow`.
fn row_to_strategy(row: &rusqlite::Row) -> StrategyRow {
    let tags_str: Option<String> = row.get(5).unwrap_or(None);
    let regime_str: Option<String> = row.get(6).unwrap_or(None);

    StrategyRow {
        id: row.get(0).unwrap_or_default(),
        name: row.get(1).unwrap_or_default(),
        description: row.get(2).unwrap_or(None),
        category: row.get(3).unwrap_or(None),
        hypothesis: row.get(4).unwrap_or(None),
        tags: tags_str.and_then(|s| serde_json::from_str(&s).ok()),
        regime: regime_str.and_then(|s| serde_json::from_str(&s).ok()),
        source: row.get(7).unwrap_or_default(),
        is_builtin: row.get::<_, i32>(8).unwrap_or(0) != 0,
        created_at: row.get(9).unwrap_or_default(),
        updated_at: row.get(10).unwrap_or_default(),
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_row(id: &str, name: &str) -> StrategyRow {
        StrategyRow {
            id: id.to_string(),
            name: name.to_string(),
            description: Some("A test strategy".to_string()),
            category: Some("stock".to_string()),
            hypothesis: None,
            tags: Some(vec!["test".to_string()]),
            regime: None,
            source: "fn config() { #{} }".to_string(),
            is_builtin: false,
            created_at: String::new(),
            updated_at: String::new(),
        }
    }

    #[test]
    fn test_init_creates_table() {
        let store = StrategyStore::open_in_memory().expect("open_in_memory");
        let conn = store.conn.lock().expect("mutex");
        let tables: Vec<String> = {
            let mut stmt = conn
                .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                .unwrap();
            stmt.query_map([], |row| row.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert!(tables.contains(&"strategies".to_string()));
    }

    #[test]
    fn test_upsert_and_get() {
        let store = StrategyStore::open_in_memory().expect("open_in_memory");
        let row = sample_row("test_strat", "Test Strategy");
        store.upsert(&row).unwrap();

        let fetched = store.get("test_strat").unwrap().expect("should exist");
        assert_eq!(fetched.id, "test_strat");
        assert_eq!(fetched.name, "Test Strategy");
        assert_eq!(fetched.category.as_deref(), Some("stock"));
        assert_eq!(fetched.tags, Some(vec!["test".to_string()]));
        assert!(!fetched.is_builtin);
    }

    #[test]
    fn test_get_source() {
        let store = StrategyStore::open_in_memory().expect("open_in_memory");
        let row = sample_row("src_test", "Source Test");
        store.upsert(&row).unwrap();

        let source = store.get_source("src_test").unwrap().expect("should exist");
        assert_eq!(source, "fn config() { #{} }");
        assert!(store.get_source("nonexistent").unwrap().is_none());
    }

    #[test]
    fn test_list() {
        let store = StrategyStore::open_in_memory().expect("open_in_memory");
        store.upsert(&sample_row("b_strat", "B Strategy")).unwrap();
        store.upsert(&sample_row("a_strat", "A Strategy")).unwrap();

        let list = store.list().unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].name, "A Strategy"); // sorted by name
        assert_eq!(list[1].name, "B Strategy");
    }

    #[test]
    fn test_delete() {
        let store = StrategyStore::open_in_memory().expect("open_in_memory");
        store.upsert(&sample_row("del_test", "Delete Me")).unwrap();

        assert!(store.delete("del_test").unwrap());
        assert!(store.get("del_test").unwrap().is_none());
        assert!(!store.delete("del_test").unwrap()); // already gone
    }

    #[test]
    fn test_upsert_updates_existing() {
        let store = StrategyStore::open_in_memory().expect("open_in_memory");
        let mut row = sample_row("update_test", "Original");
        store.upsert(&row).unwrap();

        row.name = "Updated".to_string();
        row.source = "fn config() { #{ updated: true } }".to_string();
        store.upsert(&row).unwrap();

        let fetched = store.get("update_test").unwrap().expect("should exist");
        assert_eq!(fetched.name, "Updated");
        assert!(fetched.source.contains("updated"));

        // Should still be only one row
        assert_eq!(store.list().unwrap().len(), 1);
    }

    #[test]
    fn test_seed_builtins_from_dir() {
        let store = StrategyStore::open_in_memory().expect("open_in_memory");

        // Seed from actual scripts directory if it exists
        let scripts_dir = std::path::Path::new("scripts/strategies");
        if scripts_dir.exists() {
            let count = store.seed_builtins(scripts_dir).unwrap();
            assert!(count > 0, "Should seed at least one built-in script");

            let list = store.list().unwrap();
            assert_eq!(list.len(), count);
            assert!(list.iter().all(|r| r.is_builtin));
        }
    }

    #[test]
    fn test_seed_does_not_overwrite_user_scripts() {
        let store = StrategyStore::open_in_memory().expect("open_in_memory");

        // Create a user script with the same id as a built-in would have
        let scripts_dir = std::path::Path::new("scripts/strategies");
        if !scripts_dir.exists() {
            return;
        }

        // First seed builtins
        store.seed_builtins(scripts_dir).unwrap();

        // Simulate user modifying a built-in by flipping is_builtin to 0
        {
            let conn = store.conn.lock().expect("mutex");
            conn.execute(
                "UPDATE strategies SET is_builtin = 0, source = 'user modified' WHERE rowid = 1",
                [],
            )
            .unwrap();
        }

        // Re-seed — should NOT overwrite the user's version
        store.seed_builtins(scripts_dir).unwrap();

        let conn = store.conn.lock().expect("mutex");
        let source: String = conn
            .query_row(
                "SELECT source FROM strategies WHERE is_builtin = 0 LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(source, "user modified");
    }

    #[test]
    fn test_seed_nonexistent_dir() {
        let store = StrategyStore::open_in_memory().expect("open_in_memory");
        let count = store
            .seed_builtins(std::path::Path::new("/nonexistent/path"))
            .unwrap();
        assert_eq!(count, 0);
    }
}
