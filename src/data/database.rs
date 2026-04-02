//! Shared `SQLite` database connection and schema management.
//!
//! [`Database`] owns a single connection to the `SQLite` database file, creates
//! all tables on open, and provides factory methods for creating stores.
//! The database is ephemeral — delete the file to reset.

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use refinery::embed_migrations;
use rusqlite::Connection;

embed_migrations!("migrations");

/// Shared connection handle used by all `SQLite`-backed stores.
pub type DbConnection = Arc<Mutex<Connection>>;

/// Manages a single `SQLite` database connection and schema lifecycle.
///
/// Construct via [`Database::open`] or [`Database::open_in_memory`], then use
/// [`runs()`](Database::runs) and [`strategies()`](Database::strategies)
/// to create store instances.
#[derive(Clone)]
pub struct Database {
    conn: DbConnection,
}

impl Database {
    /// Open (or create) a file-based `SQLite` database and initialise schema.
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)
            .with_context(|| format!("Failed to open SQLite database at {}", path.display()))?;
        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        db.init_schema()?;
        Ok(db)
    }

    /// Open an in-memory `SQLite` database (intended for tests).
    pub fn open_in_memory() -> Result<Self> {
        let conn =
            Connection::open_in_memory().context("Failed to open in-memory SQLite database")?;
        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        db.init_schema()?;
        Ok(db)
    }

    /// Create a [`SqliteAdjustmentStore`](super::adjustment_store::SqliteAdjustmentStore)
    /// backed by this database's connection.
    pub fn adjustments(&self) -> super::adjustment_store::SqliteAdjustmentStore {
        super::adjustment_store::SqliteAdjustmentStore::new(self.conn.clone())
    }

    /// Create a [`SqliteRunStore`](super::run_store::SqliteRunStore)
    /// backed by this database's connection.
    pub fn runs(&self) -> super::run_store::SqliteRunStore {
        super::run_store::SqliteRunStore::new(self.conn.clone())
    }

    /// Create a [`SqliteStrategyStore`](super::strategy_store::SqliteStrategyStore)
    /// backed by this database's connection.
    pub fn strategies(&self) -> super::strategy_store::SqliteStrategyStore {
        super::strategy_store::SqliteStrategyStore::new(self.conn.clone())
    }

    /// Create a [`SqliteChatStore`](super::chat_store::SqliteChatStore)
    /// backed by this database's connection.
    pub fn chat(&self) -> super::chat_store::SqliteChatStore {
        super::chat_store::SqliteChatStore::new(self.conn.clone())
    }

    /// Return the shared database connection handle.
    pub fn connection(&self) -> DbConnection {
        self.conn.clone()
    }

    /// Run all pending migrations (schema creation + incremental changes).
    ///
    /// The initial schema is defined in `migrations/V1__initial_schema.sql`.
    /// Refinery tracks applied migrations in `refinery_schema_history` and
    /// only applies new ones on each startup.
    fn init_schema(&self) -> Result<()> {
        let mut conn = self.conn.lock().expect("mutex poisoned");
        conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;")
            .context("Failed to set database PRAGMAs")?;
        migrations::runner()
            .run(&mut *conn)
            .context("Failed to run database migrations")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_creates_all_tables() {
        let db = Database::open_in_memory().expect("open_in_memory");
        let conn = db.conn.lock().expect("mutex");
        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap();
        let tables: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert!(tables.contains(&"runs".to_string()));
        assert!(tables.contains(&"sweeps".to_string()));
        assert!(tables.contains(&"trades".to_string()));
        assert!(tables.contains(&"strategies".to_string()));
        assert!(tables.contains(&"threads".to_string()));
        assert!(tables.contains(&"messages".to_string()));
        assert!(tables.contains(&"results".to_string()));
        assert!(tables.contains(&"splits".to_string()));
        assert!(tables.contains(&"dividends".to_string()));
    }

    #[test]
    fn test_stores_share_connection() {
        let db = Database::open_in_memory().expect("open_in_memory");
        let runs = db.runs();
        let strategies = db.strategies();
        assert!(Arc::ptr_eq(&runs.conn, &strategies.conn));
    }

    #[test]
    fn test_seed_only_runs_when_empty() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("test_strat.rhai"),
            "//! name: Test\n//! category: stock\nfn config() { #{} }\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("another.rhai"),
            "//! name: Another\nfn config() { #{} }\n",
        )
        .unwrap();

        let db = Database::open_in_memory().expect("open_in_memory");
        let strategies = db.strategies();

        // First seed populates
        let count = crate::data::traits::seed_strategies_if_empty(&strategies, dir.path()).unwrap();
        assert_eq!(count, 2);

        // Second seed is a no-op
        let count2 =
            crate::data::traits::seed_strategies_if_empty(&strategies, dir.path()).unwrap();
        assert_eq!(count2, 0);

        // Table still has the original rows
        assert_eq!(strategies.list().unwrap().len(), 2);
    }
}
