//! `SQLite`-backed storage for chat threads, messages, and results.
//!
//! Provides [`SqliteChatStore`] which implements the [`ChatStore`](super::traits::ChatStore)
//! trait for persisting chat conversation state.

use std::collections::HashMap;

use anyhow::{Context, Result};
use rusqlite::OptionalExtension;

use super::database::DbConnection;
use super::traits::{ChatStore, MessageRow, ResultInput, ResultRow, ThreadRow};

/// SQL expression for current UTC timestamp in ISO-8601 format.
const SQL_NOW: &str = "strftime('%Y-%m-%dT%H:%M:%fZ','now')";

// ──────────────────────────────────────────────────────────────────────────────
// SqliteChatStore
// ──────────────────────────────────────────────────────────────────────────────

/// `SQLite`-backed store for chat threads, messages, and results.
///
/// Does not own or manage the database connection — receives a shared
/// [`DbConnection`] from [`Database`](super::database::Database).
#[derive(Clone)]
pub struct SqliteChatStore {
    pub(crate) conn: DbConnection,
}

impl SqliteChatStore {
    /// Create a new store using a shared database connection.
    ///
    /// Schema must already be initialised by [`Database`](super::database::Database).
    pub fn new(conn: DbConnection) -> Self {
        Self { conn }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Trait implementation
// ──────────────────────────────────────────────────────────────────────────────

impl ChatStore for SqliteChatStore {
    fn list_threads(&self) -> Result<Vec<ThreadRow>> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT id, title, status, created_at, updated_at
                 FROM threads ORDER BY updated_at DESC",
            )
            .context("Failed to prepare list_threads query")?;

        let rows = stmt
            .query_map([], row_to_thread)
            .context("Failed to query threads")?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to collect threads")?;

        Ok(rows)
    }

    fn get_thread(&self, id: &str) -> Result<Option<ThreadRow>> {
        let conn = self.conn.lock().expect("mutex poisoned");
        conn.query_row(
            "SELECT id, title, status, created_at, updated_at
             FROM threads WHERE id = ?1",
            rusqlite::params![id],
            row_to_thread,
        )
        .optional()
        .context("Failed to query thread")
    }

    fn create_thread(&self, id: &str) -> Result<ThreadRow> {
        let conn = self.conn.lock().expect("mutex poisoned");
        conn.execute(
            &format!(
                "INSERT OR IGNORE INTO threads (id, created_at, updated_at)
                 VALUES (?1, {SQL_NOW}, {SQL_NOW})"
            ),
            rusqlite::params![id],
        )
        .context("Failed to insert thread")?;

        conn.query_row(
            "SELECT id, title, status, created_at, updated_at
             FROM threads WHERE id = ?1",
            rusqlite::params![id],
            row_to_thread,
        )
        .context("Failed to select created thread")
    }

    fn update_thread(&self, id: &str, title: Option<&str>, status: Option<&str>) -> Result<bool> {
        let conn = self.conn.lock().expect("mutex poisoned");

        let mut set_clauses = Vec::new();
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(t) = title {
            set_clauses.push("title = ?".to_string());
            params.push(Box::new(t.to_string()));
        }
        if let Some(s) = status {
            set_clauses.push("status = ?".to_string());
            params.push(Box::new(s.to_string()));
        }

        if set_clauses.is_empty() {
            return Ok(false);
        }

        set_clauses.push(format!("updated_at = {SQL_NOW}"));

        let sql = format!("UPDATE threads SET {} WHERE id = ?", set_clauses.join(", "));
        params.push(Box::new(id.to_string()));

        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(std::convert::AsRef::as_ref).collect();

        let n = conn
            .execute(&sql, param_refs.as_slice())
            .context("Failed to update thread")?;

        Ok(n > 0)
    }

    fn delete_thread(&self, id: &str) -> Result<bool> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let n = conn
            .execute("DELETE FROM threads WHERE id = ?1", rusqlite::params![id])
            .context("Failed to delete thread")?;
        Ok(n > 0)
    }

    fn get_messages(&self, thread_id: &str, limit: i64, offset: i64) -> Result<Vec<MessageRow>> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT id, thread_id, parent_id, format, content, created_at
                 FROM messages WHERE thread_id = ?1
                 ORDER BY created_at ASC
                 LIMIT ?2 OFFSET ?3",
            )
            .context("Failed to prepare get_messages query")?;

        let rows = stmt
            .query_map(rusqlite::params![thread_id, limit, offset], row_to_message)
            .context("Failed to query messages")?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to collect messages")?;

        Ok(rows)
    }

    fn upsert_message(&self, msg: &MessageRow) -> Result<()> {
        let conn = self.conn.lock().expect("mutex poisoned");

        // Auto-create thread if it does not exist
        conn.execute(
            &format!(
                "INSERT OR IGNORE INTO threads (id, created_at, updated_at)
                 VALUES (?1, {SQL_NOW}, {SQL_NOW})"
            ),
            rusqlite::params![msg.thread_id],
        )
        .context("Failed to auto-create thread")?;

        conn.execute(
            &format!(
                "INSERT INTO messages (id, thread_id, parent_id, format, content, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, {SQL_NOW})
                 ON CONFLICT(id) DO UPDATE SET
                    parent_id = excluded.parent_id,
                    format = excluded.format,
                    content = excluded.content"
            ),
            rusqlite::params![
                msg.id,
                msg.thread_id,
                msg.parent_id,
                msg.format,
                msg.content
            ],
        )
        .context("Failed to upsert message")?;

        // Touch thread's updated_at
        conn.execute(
            &format!("UPDATE threads SET updated_at = {SQL_NOW} WHERE id = ?1"),
            rusqlite::params![msg.thread_id],
        )
        .context("Failed to touch thread updated_at")?;

        Ok(())
    }

    fn delete_messages(&self, thread_id: &str) -> Result<bool> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let n = conn
            .execute(
                "DELETE FROM messages WHERE thread_id = ?1",
                rusqlite::params![thread_id],
            )
            .context("Failed to delete messages")?;
        Ok(n > 0)
    }

    fn get_results(&self, thread_id: &str) -> Result<Vec<ResultRow>> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT id, thread_id, key, type, label, tool_call_id, params, data, created_at
                 FROM results WHERE thread_id = ?1
                 ORDER BY created_at ASC",
            )
            .context("Failed to prepare get_results query")?;

        let rows = stmt
            .query_map(rusqlite::params![thread_id], row_to_result)
            .context("Failed to query results")?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to collect results")?;

        Ok(rows)
    }

    fn replace_all_results(&self, thread_id: &str, results: &[ResultInput]) -> Result<()> {
        let conn = self.conn.lock().expect("mutex poisoned");

        // Delete existing results for this thread
        conn.execute(
            "DELETE FROM results WHERE thread_id = ?1",
            rusqlite::params![thread_id],
        )
        .context("Failed to delete existing results")?;

        // Deduplicate by key — last wins
        let mut deduped: HashMap<&str, &ResultInput> = HashMap::new();
        for r in results {
            deduped.insert(&r.key, r);
        }

        // Insert each unique result
        let mut stmt = conn
            .prepare(&format!(
                "INSERT INTO results (thread_id, key, type, label, tool_call_id, params, data, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, {SQL_NOW})"
            ))
            .context("Failed to prepare insert result")?;

        for r in deduped.values() {
            stmt.execute(rusqlite::params![
                thread_id,
                r.key,
                r.result_type,
                r.label,
                r.tool_call_id,
                r.params,
                r.data,
            ])
            .context("Failed to insert result")?;
        }

        Ok(())
    }

    fn delete_result(&self, thread_id: &str, key: &str) -> Result<bool> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let n = conn
            .execute(
                "DELETE FROM results WHERE thread_id = ?1 AND key = ?2",
                rusqlite::params![thread_id, key],
            )
            .context("Failed to delete result")?;
        Ok(n > 0)
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Row mappers
// ──────────────────────────────────────────────────────────────────────────────

fn row_to_thread(row: &rusqlite::Row) -> rusqlite::Result<ThreadRow> {
    Ok(ThreadRow {
        id: row.get(0)?,
        title: row.get(1)?,
        status: row.get(2)?,
        created_at: row.get(3)?,
        updated_at: row.get(4)?,
    })
}

fn row_to_message(row: &rusqlite::Row) -> rusqlite::Result<MessageRow> {
    Ok(MessageRow {
        id: row.get(0)?,
        thread_id: row.get(1)?,
        parent_id: row.get(2)?,
        format: row.get(3)?,
        content: row.get(4)?,
        created_at: row.get(5)?,
    })
}

fn row_to_result(row: &rusqlite::Row) -> rusqlite::Result<ResultRow> {
    Ok(ResultRow {
        id: row.get(0)?,
        thread_id: row.get(1)?,
        key: row.get(2)?,
        result_type: row.get(3)?,
        label: row.get(4)?,
        tool_call_id: row.get(5)?,
        params: row.get(6)?,
        data: row.get(7)?,
        created_at: row.get(8)?,
    })
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> SqliteChatStore {
        crate::data::database::Database::open_in_memory()
            .expect("open_in_memory")
            .chat()
    }

    #[test]
    fn test_create_and_get_thread() {
        let s = store();
        let t = s.create_thread("t1").unwrap();
        assert_eq!(t.id, "t1");
        assert_eq!(t.status, "regular");

        let fetched = s.get_thread("t1").unwrap().expect("should exist");
        assert_eq!(fetched.id, "t1");
    }

    #[test]
    fn test_update_thread() {
        let s = store();
        s.create_thread("t1").unwrap();
        assert!(s.update_thread("t1", Some("Hello"), None).unwrap());

        let t = s.get_thread("t1").unwrap().unwrap();
        assert_eq!(t.title.as_deref(), Some("Hello"));
    }

    #[test]
    fn test_delete_thread_cascades() {
        let s = store();
        s.create_thread("t1").unwrap();
        let msg = MessageRow {
            id: "m1".into(),
            thread_id: "t1".into(),
            parent_id: None,
            format: "aui/v0".into(),
            content: "{}".into(),
            created_at: String::new(),
        };
        s.upsert_message(&msg).unwrap();

        assert!(s.delete_thread("t1").unwrap());
        assert!(s.get_thread("t1").unwrap().is_none());
        assert!(s.get_messages("t1", 100, 0).unwrap().is_empty());
    }

    #[test]
    fn test_upsert_message_auto_creates_thread() {
        let s = store();
        let msg = MessageRow {
            id: "m1".into(),
            thread_id: "auto_t".into(),
            parent_id: None,
            format: "aui/v0".into(),
            content: r#"{"text":"hi"}"#.into(),
            created_at: String::new(),
        };
        s.upsert_message(&msg).unwrap();

        assert!(s.get_thread("auto_t").unwrap().is_some());
        let msgs = s.get_messages("auto_t", 100, 0).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].id, "m1");
    }

    #[test]
    fn test_replace_all_results_deduplicates() {
        let s = store();
        s.create_thread("t1").unwrap();

        let inputs = vec![
            ResultInput {
                key: "k1".into(),
                result_type: "chart".into(),
                label: "First".into(),
                tool_call_id: None,
                params: "{}".into(),
                data: Some("a".into()),
            },
            ResultInput {
                key: "k1".into(),
                result_type: "chart".into(),
                label: "Second".into(),
                tool_call_id: None,
                params: "{}".into(),
                data: Some("b".into()),
            },
        ];
        s.replace_all_results("t1", &inputs).unwrap();

        let results = s.get_results("t1").unwrap();
        assert_eq!(results.len(), 1);
        // Last wins — label should be "Second"
        assert_eq!(results[0].label, "Second");
    }

    #[test]
    fn test_delete_result() {
        let s = store();
        s.create_thread("t1").unwrap();
        let inputs = vec![ResultInput {
            key: "k1".into(),
            result_type: "chart".into(),
            label: "L".into(),
            tool_call_id: None,
            params: "{}".into(),
            data: None,
        }];
        s.replace_all_results("t1", &inputs).unwrap();

        assert!(s.delete_result("t1", "k1").unwrap());
        assert!(!s.delete_result("t1", "k1").unwrap());
    }
}
