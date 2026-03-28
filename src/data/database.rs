//! Shared `SQLite` database connection, schema management, and one-time migrations.
//!
//! [`Database`] owns a single connection to the `SQLite` database file, runs all
//! schema migrations on open, and provides factory methods for creating stores.

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use rusqlite::Connection;

use crate::scripting::stdlib::parse_script_meta;

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

    /// One-time migration: seed strategies from `.rhai` files if the table is empty.
    ///
    /// Call this on startup. If the `strategies` table already has rows, this is
    /// a no-op. Returns the number of strategies seeded (0 if table was non-empty
    /// or the scripts directory doesn't exist).
    pub fn seed_strategies_if_empty(&self, scripts_dir: &Path) -> Result<usize> {
        let conn = self.conn.lock().expect("mutex poisoned");

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM strategies", [], |row| row.get(0))?;
        if count > 0 {
            return Ok(0);
        }

        let Ok(entries) = std::fs::read_dir(scripts_dir) else {
            return Ok(0);
        };

        let now = chrono::Utc::now().to_rfc3339();
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
            let tags_json = meta
                .tags
                .as_ref()
                .map(|t| serde_json::to_string(t).unwrap_or_default());
            let regime_json = meta
                .regime
                .as_ref()
                .map(|r| serde_json::to_string(r).unwrap_or_default());

            conn.execute(
                "INSERT INTO strategies (id, name, description, category, hypothesis, tags, regime, source, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
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
            )
            .with_context(|| format!("Failed to seed strategy '{id}'"))?;

            seeded += 1;
        }

        Ok(seeded)
    }

    /// Create all tables and indices if they do not already exist.
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
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_strategies_category
                ON strategies(category);
            ",
        )
        .context("Failed to initialise database schema")?;
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
        let db = Database::open_in_memory().expect("open_in_memory");
        let scripts_dir = std::path::Path::new("scripts/strategies");
        if !scripts_dir.exists() {
            return;
        }

        // First seed populates
        let count = db.seed_strategies_if_empty(scripts_dir).unwrap();
        assert!(count > 0);

        // Second seed is a no-op
        let count2 = db.seed_strategies_if_empty(scripts_dir).unwrap();
        assert_eq!(count2, 0);

        // Table still has the original rows
        let strategies = db.strategies();
        assert_eq!(strategies.list().unwrap().len(), count);
    }
}
