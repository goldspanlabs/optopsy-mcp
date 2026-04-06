//! SQLite-backed storage for forward test sessions, snapshots, and trades.

use anyhow::{Context, Result};
use rusqlite::params;
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};

use super::database::DbConnection;

// ──────────────────────────────────────────────────────────────────────────────
// Types
// ──────────────────────────────────────────────────────────────────────────────

/// A forward test session row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardTestSession {
    pub id: String,
    pub strategy: String,
    pub symbol: String,
    pub params: serde_json::Value,
    pub status: String,
    pub capital: f64,
    pub current_equity: f64,
    pub last_bar_date: Option<String>,
    pub total_trades: i64,
    pub realized_pnl: f64,
    pub engine_state: serde_json::Value,
    pub baseline_sharpe: Option<f64>,
    pub baseline_win_rate: Option<f64>,
    pub baseline_max_dd: Option<f64>,
    pub created_at: String,
    pub updated_at: String,
}

/// A daily snapshot row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardTestSnapshot {
    pub date: String,
    pub equity: f64,
    pub daily_pnl: f64,
    pub cumulative_pnl: f64,
    pub open_positions: i64,
    pub trades_today: i64,
    pub details: serde_json::Value,
}

/// A forward test trade row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardTestTrade {
    pub trade_id: i64,
    pub action: String,
    pub date: String,
    pub symbol: String,
    pub description: Option<String>,
    pub entry_cost: Option<f64>,
    pub exit_proceeds: Option<f64>,
    pub pnl: Option<f64>,
    pub exit_type: Option<String>,
    pub details: serde_json::Value,
}

// ──────────────────────────────────────────────────────────────────────────────
// Store
// ──────────────────────────────────────────────────────────────────────────────

/// SQLite-backed forward test store.
#[derive(Clone)]
pub struct SqliteForwardTestStore {
    pub(crate) conn: DbConnection,
}

impl SqliteForwardTestStore {
    pub fn new(conn: DbConnection) -> Self {
        Self { conn }
    }

    /// Create a new forward test session.
    pub fn create_session(&self, session: &ForwardTestSession) -> Result<()> {
        let conn = self.conn.lock().expect("mutex poisoned");
        conn.execute(
            "INSERT INTO forward_test_sessions
             (id, strategy, symbol, params, status, capital, current_equity,
              last_bar_date, total_trades, realized_pnl, engine_state,
              baseline_sharpe, baseline_win_rate, baseline_max_dd,
              created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![
                session.id,
                session.strategy,
                session.symbol,
                serde_json::to_string(&session.params)?,
                session.status,
                session.capital,
                session.current_equity,
                session.last_bar_date,
                session.total_trades,
                session.realized_pnl,
                serde_json::to_string(&session.engine_state)?,
                session.baseline_sharpe,
                session.baseline_win_rate,
                session.baseline_max_dd,
                session.created_at,
                session.updated_at,
            ],
        )
        .context("Failed to insert forward test session")?;
        Ok(())
    }

    /// Get a session by ID.
    pub fn get_session(&self, id: &str) -> Result<Option<ForwardTestSession>> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT id, strategy, symbol, params, status, capital, current_equity,
                    last_bar_date, total_trades, realized_pnl, engine_state,
                    baseline_sharpe, baseline_win_rate, baseline_max_dd,
                    created_at, updated_at
             FROM forward_test_sessions WHERE id = ?1",
        )?;
        let row = stmt
            .query_row(params![id], |row| {
                Ok(ForwardTestSession {
                    id: row.get(0)?,
                    strategy: row.get(1)?,
                    symbol: row.get(2)?,
                    params: serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(3)?)
                        .unwrap_or_default(),
                    status: row.get(4)?,
                    capital: row.get(5)?,
                    current_equity: row.get(6)?,
                    last_bar_date: row.get(7)?,
                    total_trades: row.get(8)?,
                    realized_pnl: row.get(9)?,
                    engine_state: serde_json::from_str::<serde_json::Value>(
                        &row.get::<_, String>(10)?,
                    )
                    .unwrap_or_default(),
                    baseline_sharpe: row.get(11)?,
                    baseline_win_rate: row.get(12)?,
                    baseline_max_dd: row.get(13)?,
                    created_at: row.get(14)?,
                    updated_at: row.get(15)?,
                })
            })
            .optional()?;
        Ok(row)
    }

    /// List all sessions, optionally filtered by status.
    pub fn list_sessions(&self, status: Option<&str>) -> Result<Vec<ForwardTestSession>> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let (sql, filter): (String, Vec<Box<dyn rusqlite::types::ToSql>>) = match status {
            Some(s) => (
                "SELECT id, strategy, symbol, params, status, capital, current_equity,
                        last_bar_date, total_trades, realized_pnl, engine_state,
                        baseline_sharpe, baseline_win_rate, baseline_max_dd,
                        created_at, updated_at
                 FROM forward_test_sessions WHERE status = ?1
                 ORDER BY created_at DESC"
                    .to_string(),
                vec![Box::new(s.to_string())],
            ),
            None => (
                "SELECT id, strategy, symbol, params, status, capital, current_equity,
                        last_bar_date, total_trades, realized_pnl, engine_state,
                        baseline_sharpe, baseline_win_rate, baseline_max_dd,
                        created_at, updated_at
                 FROM forward_test_sessions
                 ORDER BY created_at DESC"
                    .to_string(),
                vec![],
            ),
        };
        let mut stmt = conn.prepare(&sql)?;
        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            filter.iter().map(|b| b.as_ref()).collect();
        let rows = stmt
            .query_map(params_ref.as_slice(), |row| {
                Ok(ForwardTestSession {
                    id: row.get(0)?,
                    strategy: row.get(1)?,
                    symbol: row.get(2)?,
                    params: serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(3)?)
                        .unwrap_or_default(),
                    status: row.get(4)?,
                    capital: row.get(5)?,
                    current_equity: row.get(6)?,
                    last_bar_date: row.get(7)?,
                    total_trades: row.get(8)?,
                    realized_pnl: row.get(9)?,
                    engine_state: serde_json::from_str::<serde_json::Value>(
                        &row.get::<_, String>(10)?,
                    )
                    .unwrap_or_default(),
                    baseline_sharpe: row.get(11)?,
                    baseline_win_rate: row.get(12)?,
                    baseline_max_dd: row.get(13)?,
                    created_at: row.get(14)?,
                    updated_at: row.get(15)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Update session state after processing new bars.
    pub fn update_session_state(
        &self,
        id: &str,
        current_equity: f64,
        last_bar_date: &str,
        total_trades: i64,
        realized_pnl: f64,
        engine_state: &serde_json::Value,
    ) -> Result<()> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE forward_test_sessions
             SET current_equity = ?1, last_bar_date = ?2, total_trades = ?3,
                 realized_pnl = ?4, engine_state = ?5, updated_at = ?6
             WHERE id = ?7",
            params![
                current_equity,
                last_bar_date,
                total_trades,
                realized_pnl,
                serde_json::to_string(engine_state)?,
                now,
                id,
            ],
        )
        .context("Failed to update forward test session")?;
        Ok(())
    }

    /// Update session status (active/paused/stopped).
    pub fn update_session_status(&self, id: &str, status: &str) -> Result<()> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE forward_test_sessions SET status = ?1, updated_at = ?2 WHERE id = ?3",
            params![status, now, id],
        )
        .context("Failed to update session status")?;
        Ok(())
    }

    /// Delete a session and all associated data (CASCADE handles snapshots + trades).
    pub fn delete_session(&self, id: &str) -> Result<()> {
        let conn = self.conn.lock().expect("mutex poisoned");
        conn.execute(
            "DELETE FROM forward_test_sessions WHERE id = ?1",
            params![id],
        )
        .context("Failed to delete forward test session")?;
        Ok(())
    }

    /// Insert a daily snapshot.
    pub fn insert_snapshot(&self, session_id: &str, snap: &ForwardTestSnapshot) -> Result<()> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "INSERT OR REPLACE INTO forward_test_snapshots
             (session_id, date, equity, daily_pnl, cumulative_pnl,
              open_positions, trades_today, details, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                session_id,
                snap.date,
                snap.equity,
                snap.daily_pnl,
                snap.cumulative_pnl,
                snap.open_positions,
                snap.trades_today,
                serde_json::to_string(&snap.details)?,
                now,
            ],
        )
        .context("Failed to insert forward test snapshot")?;
        Ok(())
    }

    /// Get all snapshots for a session, ordered by date.
    pub fn get_snapshots(&self, session_id: &str) -> Result<Vec<ForwardTestSnapshot>> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT date, equity, daily_pnl, cumulative_pnl, open_positions,
                    trades_today, details
             FROM forward_test_snapshots
             WHERE session_id = ?1 ORDER BY date ASC",
        )?;
        let rows = stmt
            .query_map(params![session_id], |row| {
                Ok(ForwardTestSnapshot {
                    date: row.get(0)?,
                    equity: row.get(1)?,
                    daily_pnl: row.get(2)?,
                    cumulative_pnl: row.get(3)?,
                    open_positions: row.get(4)?,
                    trades_today: row.get(5)?,
                    details: serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(6)?)
                        .unwrap_or_default(),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Insert a trade record.
    pub fn insert_trade(&self, session_id: &str, trade: &ForwardTestTrade) -> Result<()> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO forward_test_trades
             (session_id, trade_id, action, date, symbol, description,
              entry_cost, exit_proceeds, pnl, exit_type, details, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                session_id,
                trade.trade_id,
                trade.action,
                trade.date,
                trade.symbol,
                trade.description,
                trade.entry_cost,
                trade.exit_proceeds,
                trade.pnl,
                trade.exit_type,
                serde_json::to_string(&trade.details)?,
                now,
            ],
        )
        .context("Failed to insert forward test trade")?;
        Ok(())
    }

    /// Get all trades for a session, ordered by date.
    pub fn get_trades(&self, session_id: &str) -> Result<Vec<ForwardTestTrade>> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT trade_id, action, date, symbol, description,
                    entry_cost, exit_proceeds, pnl, exit_type, details
             FROM forward_test_trades
             WHERE session_id = ?1 ORDER BY date ASC, id ASC",
        )?;
        let rows = stmt
            .query_map(params![session_id], |row| {
                Ok(ForwardTestTrade {
                    trade_id: row.get(0)?,
                    action: row.get(1)?,
                    date: row.get(2)?,
                    symbol: row.get(3)?,
                    description: row.get(4)?,
                    entry_cost: row.get(5)?,
                    exit_proceeds: row.get(6)?,
                    pnl: row.get(7)?,
                    exit_type: row.get(8)?,
                    details: serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(9)?)
                        .unwrap_or_default(),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}
