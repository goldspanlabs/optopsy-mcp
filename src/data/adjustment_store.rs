//! SQLite-backed store for splits and dividends data.

use anyhow::Result;
use chrono::NaiveDate;

use super::database::DbConnection;

/// A single stock split event.
#[derive(Debug, Clone)]
pub struct SplitRow {
    pub symbol: String,
    pub date: NaiveDate,
    pub ratio: f64,
}

/// A single cash dividend event.
#[derive(Debug, Clone)]
pub struct DividendRow {
    pub symbol: String,
    pub date: NaiveDate,
    pub amount: f64,
}

/// `SQLite` implementation of adjustment data queries.
pub struct SqliteAdjustmentStore {
    pub(crate) conn: DbConnection,
}

impl SqliteAdjustmentStore {
    pub fn new(conn: DbConnection) -> Self {
        Self { conn }
    }

    /// Load all splits for a symbol, sorted chronologically (oldest first).
    pub fn splits(&self, symbol: &str) -> Result<Vec<SplitRow>> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT symbol, date, ratio FROM splits WHERE symbol = ?1 ORDER BY date ASC",
        )?;
        let rows = stmt
            .query_map([symbol], |row| {
                let date_str: String = row.get(1)?;
                Ok(SplitRow {
                    symbol: row.get(0)?,
                    date: NaiveDate::parse_from_str(&date_str, "%Y-%m-%d")
                        .unwrap_or(NaiveDate::MIN),
                    ratio: row.get(2)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Load all dividends for a symbol, sorted chronologically (oldest first).
    pub fn dividends(&self, symbol: &str) -> Result<Vec<DividendRow>> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT symbol, date, amount FROM dividends WHERE symbol = ?1 ORDER BY date ASC",
        )?;
        let rows = stmt
            .query_map([symbol], |row| {
                let date_str: String = row.get(1)?;
                Ok(DividendRow {
                    symbol: row.get(0)?,
                    date: NaiveDate::parse_from_str(&date_str, "%Y-%m-%d")
                        .unwrap_or(NaiveDate::MIN),
                    amount: row.get(2)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::database::Database;

    #[test]
    fn test_query_splits() {
        let db = Database::open_in_memory().expect("open_in_memory");
        let store = db.adjustments();
        let splits = store.splits("AAPL").expect("query splits");
        // V2 migration seeds AAPL splits
        assert!(!splits.is_empty(), "AAPL should have splits from seed data");
        // Check the most recent split (4:1 on 2020-08-31)
        let latest = splits.last().unwrap();
        assert_eq!(latest.date, NaiveDate::from_ymd_opt(2020, 8, 31).unwrap());
        assert!((latest.ratio - 4.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_query_dividends() {
        let db = Database::open_in_memory().expect("open_in_memory");
        let store = db.adjustments();
        let divs = store.dividends("AAPL").expect("query dividends");
        assert!(
            !divs.is_empty(),
            "AAPL should have dividends from seed data"
        );
    }

    #[test]
    fn test_no_data_returns_empty() {
        let db = Database::open_in_memory().expect("open_in_memory");
        let store = db.adjustments();
        let splits = store.splits("ZZZZZ").expect("query splits");
        assert!(splits.is_empty());
    }
}
