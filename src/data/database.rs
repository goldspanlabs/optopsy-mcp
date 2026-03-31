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

    /// Create all tables and indices if they do not already exist.
    #[allow(clippy::too_many_lines)]
    fn init_schema(&self) -> Result<()> {
        let mut conn = self.conn.lock().expect("mutex poisoned");
        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;

            -- Sweep sessions
            CREATE TABLE IF NOT EXISTS sweeps (
                id                TEXT PRIMARY KEY,
                strategy_id       TEXT REFERENCES strategies(id),
                symbol            TEXT NOT NULL,
                sweep_config      TEXT NOT NULL CHECK(json_valid(sweep_config)),
                objective         TEXT NOT NULL DEFAULT 'sharpe',
                mode              TEXT NOT NULL DEFAULT 'grid',
                combinations      INTEGER NOT NULL,
                execution_time_ms INTEGER,
                analysis          TEXT,
                created_at        TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_sweeps_created ON sweeps(created_at DESC);

            -- Runs (unified backtest results)
            CREATE TABLE IF NOT EXISTS runs (
                id                TEXT PRIMARY KEY,
                sweep_id          TEXT REFERENCES sweeps(id) ON DELETE CASCADE,
                strategy_id       TEXT REFERENCES strategies(id),
                symbol            TEXT NOT NULL,
                capital           REAL NOT NULL,
                params            TEXT NOT NULL CHECK(json_valid(params)),
                total_return      REAL,
                win_rate          REAL,
                max_drawdown      REAL,
                sharpe            REAL,
                sortino           REAL,
                cagr              REAL,
                profit_factor     REAL,
                trade_count       INTEGER,
                expectancy        REAL,
                var_95            REAL,
                result_json       TEXT CHECK(json_valid(result_json)),
                execution_time_ms INTEGER,
                analysis          TEXT,
                hypothesis        TEXT,
                tags              TEXT,
                regime            TEXT,
                created_at        TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_runs_sweep_id ON runs(sweep_id);
            CREATE INDEX IF NOT EXISTS idx_runs_strategy ON runs(strategy_id);
            CREATE INDEX IF NOT EXISTS idx_runs_created ON runs(created_at DESC);

            -- Trades (mirrors TradeRecord / FE TradeLogEntry shape)
            CREATE TABLE IF NOT EXISTS trades (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id              TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                trade_id            INTEGER NOT NULL,
                entry_datetime      INTEGER NOT NULL,
                exit_datetime       INTEGER NOT NULL,
                entry_cost          REAL,
                exit_proceeds       REAL,
                entry_amount        REAL,
                entry_label         TEXT,
                exit_amount         REAL,
                exit_label          TEXT,
                pnl                 REAL,
                days_held           INTEGER,
                exit_type           TEXT,
                legs                TEXT,
                computed_quantity   INTEGER,
                entry_equity        REAL,
                stock_entry_price   REAL,
                stock_exit_price    REAL,
                stock_pnl           REAL,
                [group]             TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_trades_run_id ON trades(run_id);

            -- Strategies
            CREATE TABLE IF NOT EXISTS strategies (
                id              TEXT PRIMARY KEY,
                name            TEXT NOT NULL,
                description     TEXT,
                category        TEXT,
                hypothesis      TEXT,
                tags            TEXT,
                regime          TEXT,
                source          TEXT NOT NULL,
                thread_id       TEXT,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_strategies_category
                ON strategies(category);

            -- Threads
            CREATE TABLE IF NOT EXISTS threads (
                id          TEXT PRIMARY KEY,
                strategy_id TEXT,
                title       TEXT,
                status      TEXT NOT NULL DEFAULT 'regular',
                created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            );

            CREATE INDEX IF NOT EXISTS idx_threads_strategy_id
                ON threads(strategy_id);

            -- Messages
            CREATE TABLE IF NOT EXISTS messages (
                id          TEXT PRIMARY KEY,
                thread_id   TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
                parent_id   TEXT,
                format      TEXT NOT NULL DEFAULT 'aui/v0',
                content     TEXT NOT NULL,
                created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            );

            CREATE INDEX IF NOT EXISTS idx_messages_thread_id
                ON messages(thread_id);

            -- Results
            CREATE TABLE IF NOT EXISTS results (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id       TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
                key             TEXT NOT NULL,
                type            TEXT NOT NULL,
                label           TEXT NOT NULL,
                tool_call_id    TEXT,
                params          TEXT NOT NULL DEFAULT '{}',
                data            TEXT,
                created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                UNIQUE(thread_id, key)
            );

            CREATE INDEX IF NOT EXISTS idx_results_thread_id
                ON results(thread_id);

            CREATE INDEX IF NOT EXISTS idx_results_tool_call_id
                ON results(tool_call_id);
            ",
        )
        .context("Failed to initialise database schema")?;

        // Run pending migrations (refinery tracks applied migrations in
        // `refinery_schema_history` and only applies new ones).
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
