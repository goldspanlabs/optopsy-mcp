//! Shared `SQLite` database connection, schema management, and one-time migrations.
//!
//! [`Database`] owns a single connection to the `SQLite` database file, runs all
//! schema migrations on open, and provides factory methods for creating stores.

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use rusqlite::Connection;

/// Shared connection handle used by all `SQLite`-backed stores.
pub type DbConnection = Arc<Mutex<Connection>>;

/// Manages a single `SQLite` database connection and schema lifecycle.
///
/// Construct via [`Database::open`] or [`Database::open_in_memory`], then use
/// [`backtests()`](Database::backtests) and [`strategies()`](Database::strategies)
/// to create store instances.
#[derive(Clone)]
pub struct Database {
    conn: DbConnection,
}

impl Database {
    /// Open (or create) a file-based `SQLite` database and run all migrations.
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

    /// Create a [`SqliteBacktestStore`](super::backtest_store::SqliteBacktestStore)
    /// backed by this database's connection.
    pub fn backtests(&self) -> super::backtest_store::SqliteBacktestStore {
        super::backtest_store::SqliteBacktestStore::new(self.conn.clone())
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

    /// Create all tables and indices if they do not already exist.
    #[allow(clippy::too_many_lines)]
    fn init_schema(&self) -> Result<()> {
        let conn = self.conn.lock().expect("mutex poisoned");
        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;

            -- Backtests
            CREATE TABLE IF NOT EXISTS backtests (
                id                  TEXT PRIMARY KEY,
                strategy_key        TEXT NOT NULL,
                symbol              TEXT NOT NULL,
                capital             REAL NOT NULL,
                params              TEXT NOT NULL,
                result_json         TEXT NOT NULL DEFAULT '{}',
                execution_time_ms   INTEGER NOT NULL,
                created_at          TEXT NOT NULL,
                hypothesis          TEXT,
                tags                TEXT,
                regime              TEXT,
                analysis            TEXT
            );

            CREATE TABLE IF NOT EXISTS backtest_metrics (
                backtest_id     TEXT PRIMARY KEY REFERENCES backtests(id) ON DELETE CASCADE,
                sharpe          REAL,
                sortino         REAL,
                cagr            REAL,
                max_drawdown    REAL,
                win_rate        REAL,
                profit_factor   REAL,
                total_pnl       REAL,
                trade_count     INTEGER,
                expectancy      REAL,
                var_95          REAL
            );

            CREATE TABLE IF NOT EXISTS trades (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                backtest_id         TEXT NOT NULL REFERENCES backtests(id) ON DELETE CASCADE,
                trade_id            INTEGER NOT NULL,
                entry_datetime      TEXT NOT NULL,
                exit_datetime       TEXT NOT NULL,
                entry_cost          REAL,
                exit_proceeds       REAL,
                pnl                 REAL,
                pnl_pct             REAL,
                days_held           INTEGER,
                exit_type           TEXT,
                legs                TEXT,
                computed_quantity   INTEGER,
                entry_equity        REAL,
                group_label         TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_backtests_strategy_symbol
                ON backtests(strategy_key, symbol);

            CREATE INDEX IF NOT EXISTS idx_trades_backtest_id
                ON trades(backtest_id);

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
                title       TEXT,
                status      TEXT NOT NULL DEFAULT 'regular',
                created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            );

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

        // Migrations for existing databases
        Self::migrate(&conn)?;
        Ok(())
    }

    /// Run incremental migrations for columns added after initial schema.
    fn migrate(conn: &Connection) -> Result<()> {
        // Add thread_id to strategies if missing
        let cols: Vec<String> = conn
            .prepare("PRAGMA table_info(strategies)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if !cols.iter().any(|c| c == "thread_id") {
            conn.execute_batch("ALTER TABLE strategies ADD COLUMN thread_id TEXT")?;
        }

        // Migrate slug-based strategy IDs to UUIDs
        // UUIDs are 36 chars; slugs are shorter
        let slug_strategies: Vec<String> = conn
            .prepare("SELECT id FROM strategies WHERE length(id) < 36")?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        for old_id in &slug_strategies {
            let new_id = uuid::Uuid::new_v4().to_string();
            conn.execute(
                "UPDATE strategies SET id = ?1 WHERE id = ?2",
                rusqlite::params![new_id, old_id],
            )?;
            conn.execute(
                "UPDATE backtests SET strategy_key = ?1 WHERE strategy_key = ?2",
                rusqlite::params![new_id, old_id],
            )?;
        }

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

        assert!(tables.contains(&"backtests".to_string()));
        assert!(tables.contains(&"backtest_metrics".to_string()));
        assert!(tables.contains(&"trades".to_string()));
        assert!(tables.contains(&"strategies".to_string()));
        assert!(tables.contains(&"threads".to_string()));
        assert!(tables.contains(&"messages".to_string()));
        assert!(tables.contains(&"results".to_string()));
    }

    #[test]
    fn test_stores_share_connection() {
        let db = Database::open_in_memory().expect("open_in_memory");
        let backtests = db.backtests();
        let strategies = db.strategies();
        assert!(Arc::ptr_eq(&backtests.conn, &strategies.conn));
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
