//! `SQLite`-backed storage for unified backtest runs and sweep sessions.
//!
//! Provides [`SqliteRunStore`] which implements the [`RunStore`](super::traits::RunStore)
//! trait for persisting and querying backtest runs, their trades, and sweep sessions.

use anyhow::{Context, Result};
use rusqlite::OptionalExtension;
use serde_json::Value;

use super::database::DbConnection;
use super::traits::{
    RunDetail, RunRow, RunStore, RunSummary, RunsListResponse, RunsOverview, SweepDetail,
    SweepParamRange, TradeRow, WalkForwardValidation,
};
use crate::server::sanitize::sanitize_opt;

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Flatten `Option<f64>` through `sanitize_opt`, converting NaN/Infinity to `None`.
fn sanitize_option(v: Option<f64>) -> Option<f64> {
    v.and_then(sanitize_opt)
}

// ──────────────────────────────────────────────────────────────────────────────
// SqliteRunStore
// ──────────────────────────────────────────────────────────────────────────────

/// `SQLite`-backed store for unified backtest runs and sweep sessions.
///
/// Does not own or manage the database connection — receives a shared
/// [`DbConnection`] from [`Database`](super::database::Database).
#[derive(Clone)]
pub struct SqliteRunStore {
    pub(crate) conn: DbConnection,
}

impl SqliteRunStore {
    /// Create a new store using a shared database connection.
    ///
    /// Schema must already be initialised by [`Database`](super::database::Database).
    pub fn new(conn: DbConnection) -> Self {
        Self { conn }
    }
}

impl RunStore for SqliteRunStore {
    #[allow(clippy::too_many_arguments)]
    fn insert_run(
        &self,
        id: &str,
        sweep_id: Option<&str>,
        strategy_id: Option<&str>,
        symbol: &str,
        capital: f64,
        params: &Value,
        total_return: Option<f64>,
        win_rate: Option<f64>,
        max_drawdown: Option<f64>,
        sharpe: Option<f64>,
        sortino: Option<f64>,
        cagr: Option<f64>,
        profit_factor: Option<f64>,
        trade_count: Option<i64>,
        expectancy: Option<f64>,
        var_95: Option<f64>,
        result_json: &str,
        execution_time_ms: Option<i64>,
        hypothesis: Option<&str>,
        tags: Option<&str>,
        regime: Option<&str>,
        source: &str,
        thread_id: Option<&str>,
    ) -> Result<String> {
        let created_at = chrono::Utc::now().to_rfc3339();
        let params_str = serde_json::to_string(params).context("Failed to serialize params")?;

        let conn = self.conn.lock().expect("mutex poisoned");
        conn.execute(
            "INSERT INTO runs
                (id, sweep_id, strategy_id, symbol, capital, params,
                 total_return, win_rate, max_drawdown, sharpe, sortino, cagr,
                 profit_factor, trade_count, expectancy, var_95,
                 result_json, execution_time_ms, hypothesis, tags, regime,
                 source, thread_id, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                     ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24)",
            rusqlite::params![
                id,
                sweep_id,
                strategy_id,
                symbol,
                capital,
                params_str,
                sanitize_option(total_return),
                sanitize_option(win_rate),
                sanitize_option(max_drawdown),
                sanitize_option(sharpe),
                sanitize_option(sortino),
                sanitize_option(cagr),
                sanitize_option(profit_factor),
                trade_count,
                sanitize_option(expectancy),
                sanitize_option(var_95),
                result_json,
                execution_time_ms,
                hypothesis,
                tags,
                regime,
                source,
                thread_id,
                created_at,
            ],
        )
        .context("Failed to insert into runs")?;

        Ok(created_at)
    }

    fn insert_trades(&self, run_id: &str, trades: &[TradeRow]) -> Result<()> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let tx = conn.unchecked_transaction()?;
        for trade in trades {
            tx.execute(
                "INSERT INTO trades
                    (run_id, trade_id, entry_datetime, exit_datetime, entry_cost,
                     exit_proceeds, entry_amount, entry_label, exit_amount, exit_label,
                     pnl, days_held, exit_type, legs, computed_quantity, entry_equity,
                     stock_entry_price, stock_exit_price, stock_pnl, [group])
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20)",
                rusqlite::params![
                    run_id,
                    trade.trade_id,
                    trade.entry_datetime,
                    trade.exit_datetime,
                    trade.entry_cost,
                    trade.exit_proceeds,
                    trade.entry_amount,
                    trade.entry_label,
                    trade.exit_amount,
                    trade.exit_label,
                    trade.pnl,
                    trade.days_held,
                    trade.exit_type,
                    trade.legs.to_string(),
                    trade.computed_quantity,
                    trade.entry_equity,
                    trade.stock_entry_price,
                    trade.stock_exit_price,
                    trade.stock_pnl,
                    trade.group,
                ],
            )
            .context("Failed to insert trade")?;
        }
        tx.commit()?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_sweep(
        &self,
        id: &str,
        strategy_id: Option<&str>,
        symbol: &str,
        sweep_config: &Value,
        objective: &str,
        mode: &str,
        combinations: i64,
        execution_time_ms: Option<i64>,
        source: &str,
        thread_id: Option<&str>,
    ) -> Result<String> {
        let created_at = chrono::Utc::now().to_rfc3339();
        let sweep_config_str =
            serde_json::to_string(sweep_config).context("Failed to serialize sweep_config")?;

        let conn = self.conn.lock().expect("mutex poisoned");
        conn.execute(
            "INSERT INTO sweeps
                (id, strategy_id, symbol, sweep_config, objective, mode,
                 combinations, execution_time_ms, source, thread_id, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            rusqlite::params![
                id,
                strategy_id,
                symbol,
                sweep_config_str,
                objective,
                mode,
                combinations,
                execution_time_ms,
                source,
                thread_id,
                created_at,
            ],
        )
        .context("Failed to insert into sweeps")?;

        Ok(created_at)
    }

    #[allow(clippy::too_many_lines)]
    fn list(&self, tag: Option<&str>) -> Result<RunsListResponse> {
        let conn = self.conn.lock().expect("mutex poisoned");

        // Build overview
        let overview = conn
            .query_row(
                "SELECT
                    COUNT(*) as total_runs,
                    MAX(created_at) as last_run_at,
                    MAX(total_return) as best_return,
                    MAX(sharpe) as best_sharpe,
                    AVG(win_rate) as avg_win_rate,
                    AVG(sharpe) as avg_sharpe
                 FROM runs",
                [],
                |row| {
                    Ok(RunsOverview {
                        total_runs: row.get(0)?,
                        last_run_at: row.get(1)?,
                        best_return: row.get(2)?,
                        best_sharpe: row.get(3)?,
                        avg_win_rate: row.get(4)?,
                        avg_sharpe: row.get(5)?,
                    })
                },
            )
            .context("Failed to query runs overview")?;

        // Collect standalone runs (no sweep_id), optionally filtered by tag
        let mut rows: Vec<RunRow> = Vec::new();
        let tag_filter = tag.map(|t| format!("%{}%", t.to_lowercase()));

        {
            let sql = if tag_filter.is_some() {
                "SELECT r.id, r.sweep_id, r.strategy_id, s.name as strategy_name,
                        r.symbol, r.params, r.total_return, r.win_rate,
                        r.max_drawdown, r.sharpe, r.sortino, r.cagr,
                        r.profit_factor, r.trade_count, r.tags,
                        r.source, r.thread_id, r.created_at
                 FROM runs r
                 LEFT JOIN strategies s ON s.id = r.strategy_id
                 WHERE r.sweep_id IS NULL AND LOWER(COALESCE(r.tags, '')) LIKE ?1
                 ORDER BY r.created_at DESC"
            } else {
                "SELECT r.id, r.sweep_id, r.strategy_id, s.name as strategy_name,
                        r.symbol, r.params, r.total_return, r.win_rate,
                        r.max_drawdown, r.sharpe, r.sortino, r.cagr,
                        r.profit_factor, r.trade_count, r.tags,
                        r.source, r.thread_id, r.created_at
                 FROM runs r
                 LEFT JOIN strategies s ON s.id = r.strategy_id
                 WHERE r.sweep_id IS NULL
                 ORDER BY r.created_at DESC"
            };
            let mut stmt = conn
                .prepare(sql)
                .context("Failed to prepare standalone runs query")?;

            let map_row = |row: &rusqlite::Row| {
                let params_str: String = row.get(5)?;
                let params: Value = serde_json::from_str(&params_str)
                    .unwrap_or(Value::Object(serde_json::Map::default()));
                Ok(RunRow::Single(RunSummary {
                    id: row.get(0)?,
                    sweep_id: row.get(1)?,
                    strategy_id: row.get(2)?,
                    strategy_name: row.get(3)?,
                    symbol: row.get(4)?,
                    params,
                    total_return: row.get(6)?,
                    win_rate: row.get(7)?,
                    max_drawdown: row.get(8)?,
                    sharpe: row.get(9)?,
                    sortino: row.get(10)?,
                    cagr: row.get(11)?,
                    profit_factor: row.get(12)?,
                    trade_count: row.get(13)?,
                    tags: row.get(14)?,
                    source: row
                        .get::<_, Option<String>>(15)?
                        .unwrap_or_else(|| "manual".to_string()),
                    thread_id: row.get(16)?,
                    created_at: row.get(17)?,
                }))
            };

            let single_rows = if let Some(ref filter) = tag_filter {
                stmt.query_map(rusqlite::params![filter], map_row)
            } else {
                stmt.query_map([], map_row)
            }
            .context("Failed to query standalone runs")?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to collect standalone runs")?;

            rows.extend(single_rows);
        }

        // Collect sweep rows
        {
            let mut stmt = conn
                .prepare(
                    "SELECT sw.id, sw.strategy_id, s.name as strategy_name,
                            sw.symbol, sw.combinations,
                            br.total_return as best_return,
                            br.win_rate as best_win_rate,
                            br.max_drawdown as best_max_drawdown,
                            br.sharpe as best_sharpe,
                            br.sortino as best_sortino,
                            br.cagr as best_cagr,
                            br.profit_factor as best_profit_factor,
                            br.trade_count as best_trade_count,
                            sw.source, sw.thread_id,
                            sw.created_at,
                            wfv.best_wfe, wfv.wf_count,
                            sw.sweep_config
                     FROM sweeps sw
                     LEFT JOIN strategies s ON s.id = sw.strategy_id
                     LEFT JOIN (
                         SELECT r1.*
                         FROM runs r1
                         INNER JOIN (
                             SELECT sweep_id, MAX(sharpe) as max_sharpe
                             FROM runs
                             WHERE sweep_id IS NOT NULL
                             GROUP BY sweep_id
                         ) r2 ON r1.sweep_id = r2.sweep_id AND r1.sharpe = r2.max_sharpe
                     ) br ON br.sweep_id = sw.id
                     LEFT JOIN (
                         SELECT sweep_id,
                                MAX(efficiency_ratio) as best_wfe,
                                COUNT(*) as wf_count
                         FROM walk_forward_validations
                         WHERE status = 'completed'
                         GROUP BY sweep_id
                     ) wfv ON wfv.sweep_id = sw.id
                     ORDER BY sw.created_at DESC",
                )
                .context("Failed to prepare sweep list query")?;

            let sweep_rows = stmt
                .query_map([], |row| {
                    // Extract sweep_params from sweep_config JSON
                    let sweep_params: Option<Vec<SweepParamRange>> =
                        row.get::<_, Option<String>>(18)?
                            .and_then(|config_str| {
                                serde_json::from_str::<Value>(&config_str).ok()
                            })
                            .and_then(|config| {
                                config.get("sweep_params").and_then(|sp| {
                                    serde_json::from_value::<Vec<Value>>(sp.clone()).ok()
                                })
                            })
                            .map(|params| {
                                params
                                    .into_iter()
                                    .filter_map(|p| {
                                        Some(SweepParamRange {
                                            name: p.get("name")?.as_str()?.to_string(),
                                            start: p.get("start")?.as_f64()?,
                                            stop: p.get("stop")?.as_f64()?,
                                        })
                                    })
                                    .collect()
                            });

                    Ok(RunRow::Sweep {
                        sweep_id: row.get(0)?,
                        strategy_id: row.get(1)?,
                        strategy_name: row.get(2)?,
                        symbol: row.get(3)?,
                        combinations: row.get(4)?,
                        best_return: row.get(5)?,
                        best_win_rate: row.get(6)?,
                        best_max_drawdown: row.get(7)?,
                        best_sharpe: row.get(8)?,
                        best_sortino: row.get(9)?,
                        best_cagr: row.get(10)?,
                        best_profit_factor: row.get(11)?,
                        best_trade_count: row.get(12)?,
                        source: row
                            .get::<_, Option<String>>(13)?
                            .unwrap_or_else(|| "manual".to_string()),
                        thread_id: row.get(14)?,
                        created_at: row.get(15)?,
                        sweep_params,
                        wf_best_efficiency: row.get(16)?,
                        wf_validation_count: row.get(17)?,
                    })
                })
                .context("Failed to query sweep rows")?
                .collect::<Result<Vec<_>, _>>()
                .context("Failed to collect sweep rows")?;

            rows.extend(sweep_rows);
        }

        // Sort all rows by created_at descending
        rows.sort_by(|a, b| {
            let a_date = match a {
                RunRow::Single(s) => &s.created_at,
                RunRow::Sweep { created_at, .. } => created_at,
            };
            let b_date = match b {
                RunRow::Single(s) => &s.created_at,
                RunRow::Sweep { created_at, .. } => created_at,
            };
            b_date.cmp(a_date)
        });

        Ok(RunsListResponse { overview, rows })
    }

    #[allow(clippy::too_many_lines)]
    fn get_run(&self, id: &str) -> Result<Option<RunDetail>> {
        let conn = self.conn.lock().expect("mutex poisoned");

        let row = conn
            .query_row(
                "SELECT r.id, r.sweep_id, r.strategy_id, s.name as strategy_name,
                        r.symbol, r.capital, r.params,
                        r.total_return, r.win_rate, r.max_drawdown, r.sharpe,
                        r.sortino, r.cagr, r.profit_factor, r.trade_count,
                        r.expectancy, r.var_95, r.result_json,
                        r.execution_time_ms, r.analysis, r.hypothesis,
                        r.tags, r.regime, r.source, r.thread_id, r.created_at
                 FROM runs r
                 LEFT JOIN strategies s ON s.id = r.strategy_id
                 WHERE r.id = ?1",
                rusqlite::params![id],
                |row| {
                    let params_str: String = row.get(6)?;
                    let params: Value = serde_json::from_str(&params_str)
                        .unwrap_or(Value::Object(serde_json::Map::default()));
                    let result_json_str: Option<String> = row.get(17)?;
                    let result_json: Option<Value> =
                        result_json_str.and_then(|s| serde_json::from_str(&s).ok());

                    Ok(RunDetail {
                        id: row.get(0)?,
                        sweep_id: row.get(1)?,
                        strategy_id: row.get(2)?,
                        strategy_name: row.get(3)?,
                        symbol: row.get(4)?,
                        capital: row.get(5)?,
                        params,
                        total_return: row.get(7)?,
                        win_rate: row.get(8)?,
                        max_drawdown: row.get(9)?,
                        sharpe: row.get(10)?,
                        sortino: row.get(11)?,
                        cagr: row.get(12)?,
                        profit_factor: row.get(13)?,
                        trade_count: row.get(14)?,
                        expectancy: row.get(15)?,
                        var_95: row.get(16)?,
                        result_json,
                        trades: Vec::new(), // filled below
                        execution_time_ms: row.get(18)?,
                        analysis: row.get(19)?,
                        hypothesis: row.get(20)?,
                        tags: row.get(21)?,
                        regime: row.get(22)?,
                        source: row
                            .get::<_, Option<String>>(23)?
                            .unwrap_or_else(|| "manual".to_string()),
                        thread_id: row.get(24)?,
                        created_at: row.get(25)?,
                    })
                },
            )
            .optional()
            .context("Failed to query run detail")?;

        let Some(mut detail) = row else {
            return Ok(None);
        };

        // Load trades
        let mut stmt = conn
            .prepare(
                "SELECT trade_id, entry_datetime, exit_datetime, entry_cost, exit_proceeds,
                        entry_amount, entry_label, exit_amount, exit_label,
                        pnl, days_held, exit_type, legs,
                        computed_quantity, entry_equity,
                        stock_entry_price, stock_exit_price, stock_pnl, [group]
                 FROM trades
                 WHERE run_id = ?1
                 ORDER BY trade_id ASC",
            )
            .context("Failed to prepare trades query")?;

        detail.trades = stmt
            .query_map(rusqlite::params![id], |row| {
                Ok(TradeRow {
                    trade_id: row.get(0)?,
                    entry_datetime: row.get(1)?,
                    exit_datetime: row.get(2)?,
                    entry_cost: row.get(3)?,
                    exit_proceeds: row.get(4)?,
                    entry_amount: row.get(5)?,
                    entry_label: row.get(6)?,
                    exit_amount: row.get(7)?,
                    exit_label: row.get(8)?,
                    pnl: row.get(9)?,
                    days_held: row.get(10)?,
                    exit_type: row.get(11)?,
                    legs: {
                        let s: String = row.get(12)?;
                        serde_json::from_str(&s).unwrap_or(Value::Array(vec![]))
                    },
                    computed_quantity: row.get(13)?,
                    entry_equity: row.get(14)?,
                    stock_entry_price: row.get(15)?,
                    stock_exit_price: row.get(16)?,
                    stock_pnl: row.get(17)?,
                    group: row.get(18)?,
                })
            })
            .context("Failed to query trades")?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to collect trades")?;

        Ok(Some(detail))
    }

    #[allow(clippy::too_many_lines)]
    fn get_sweep(&self, id: &str) -> Result<Option<SweepDetail>> {
        let conn = self.conn.lock().expect("mutex poisoned");

        let row = conn
            .query_row(
                "SELECT sw.id, sw.strategy_id, s.name as strategy_name,
                        sw.symbol, sw.sweep_config, sw.objective, sw.mode,
                        sw.combinations, sw.execution_time_ms, sw.analysis,
                        sw.source, sw.thread_id, sw.created_at
                 FROM sweeps sw
                 LEFT JOIN strategies s ON s.id = sw.strategy_id
                 WHERE sw.id = ?1",
                rusqlite::params![id],
                |row| {
                    let config_str: String = row.get(4)?;
                    let sweep_config: Value = serde_json::from_str(&config_str)
                        .unwrap_or(Value::Object(serde_json::Map::default()));

                    Ok(SweepDetail {
                        id: row.get(0)?,
                        strategy_id: row.get(1)?,
                        strategy_name: row.get(2)?,
                        symbol: row.get(3)?,
                        sweep_config,
                        objective: row.get(5)?,
                        mode: row.get(6)?,
                        combinations: row.get(7)?,
                        execution_time_ms: row.get(8)?,
                        analysis: row.get(9)?,
                        source: row
                            .get::<_, Option<String>>(10)?
                            .unwrap_or_else(|| "manual".to_string()),
                        thread_id: row.get(11)?,
                        created_at: row.get(12)?,
                        runs: Vec::new(),        // filled below
                        validations: Vec::new(), // filled below
                    })
                },
            )
            .optional()
            .context("Failed to query sweep detail")?;

        let Some(mut detail) = row else {
            return Ok(None);
        };

        // Load walk-forward validations for this sweep
        {
            let mut wfv_stmt = conn
                .prepare(
                    "SELECT id, sweep_id, n_windows, train_pct, mode, objective,
                            efficiency_ratio, profitable_windows, total_windows,
                            param_stability, analysis, status, execution_time_ms,
                            created_at
                     FROM walk_forward_validations
                     WHERE sweep_id = ?1
                     ORDER BY created_at DESC",
                )
                .context("Failed to prepare walk-forward validations query")?;

            detail.validations = wfv_stmt
                .query_map(rusqlite::params![id], |row| {
                    Ok(WalkForwardValidation {
                        id: row.get(0)?,
                        sweep_id: row.get(1)?,
                        n_windows: row.get(2)?,
                        train_pct: row.get(3)?,
                        mode: row.get(4)?,
                        objective: row.get(5)?,
                        efficiency_ratio: row.get(6)?,
                        profitable_windows: row.get(7)?,
                        total_windows: row.get(8)?,
                        param_stability: row.get(9)?,
                        analysis: row.get(10)?,
                        status: row.get(11)?,
                        execution_time_ms: row.get(12)?,
                        created_at: row.get(13)?,
                    })
                })
                .context("Failed to query walk-forward validations")?
                .collect::<Result<Vec<_>, _>>()
                .context("Failed to collect walk-forward validations")?;
        }

        // Load runs for this sweep
        let mut stmt = conn
            .prepare(
                "SELECT r.id, r.sweep_id, r.strategy_id, s.name as strategy_name,
                        r.symbol, r.params, r.total_return, r.win_rate,
                        r.max_drawdown, r.sharpe, r.sortino, r.cagr,
                        r.profit_factor, r.trade_count, r.tags,
                        r.source, r.thread_id, r.created_at
                 FROM runs r
                 LEFT JOIN strategies s ON s.id = r.strategy_id
                 WHERE r.sweep_id = ?1
                 ORDER BY r.sharpe DESC NULLS LAST",
            )
            .context("Failed to prepare sweep runs query")?;

        detail.runs = stmt
            .query_map(rusqlite::params![id], |row| {
                let params_str: String = row.get(5)?;
                let params: Value = serde_json::from_str(&params_str)
                    .unwrap_or(Value::Object(serde_json::Map::default()));
                Ok(RunSummary {
                    id: row.get(0)?,
                    sweep_id: row.get(1)?,
                    strategy_id: row.get(2)?,
                    strategy_name: row.get(3)?,
                    symbol: row.get(4)?,
                    params,
                    total_return: row.get(6)?,
                    win_rate: row.get(7)?,
                    max_drawdown: row.get(8)?,
                    sharpe: row.get(9)?,
                    sortino: row.get(10)?,
                    cagr: row.get(11)?,
                    profit_factor: row.get(12)?,
                    trade_count: row.get(13)?,
                    tags: row.get(14)?,
                    source: row
                        .get::<_, Option<String>>(15)?
                        .unwrap_or_else(|| "manual".to_string()),
                    thread_id: row.get(16)?,
                    created_at: row.get(17)?,
                })
            })
            .context("Failed to query sweep runs")?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to collect sweep runs")?;

        Ok(Some(detail))
    }

    fn delete_run(&self, id: &str) -> Result<bool> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let n = conn
            .execute("DELETE FROM runs WHERE id = ?1", rusqlite::params![id])
            .context("Failed to delete run")?;
        Ok(n > 0)
    }

    fn delete_sweep(&self, id: &str) -> Result<bool> {
        let conn = self.conn.lock().expect("mutex poisoned");
        // CASCADE will delete associated runs and their trades
        let n = conn
            .execute("DELETE FROM sweeps WHERE id = ?1", rusqlite::params![id])
            .context("Failed to delete sweep")?;
        Ok(n > 0)
    }

    fn set_run_analysis(&self, id: &str, analysis: &str) -> Result<bool> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let rows = conn
            .execute(
                "UPDATE runs SET analysis = ?2 WHERE id = ?1",
                rusqlite::params![id, analysis],
            )
            .context("Failed to update run analysis")?;
        Ok(rows > 0)
    }

    fn set_sweep_analysis(&self, id: &str, analysis: &str) -> Result<bool> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let rows = conn
            .execute(
                "UPDATE sweeps SET analysis = ?2 WHERE id = ?1",
                rusqlite::params![id, analysis],
            )
            .context("Failed to update sweep analysis")?;
        Ok(rows > 0)
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_walk_forward_validation(
        &self,
        id: &str,
        sweep_id: &str,
        n_windows: i64,
        train_pct: f64,
        mode: &str,
        objective: &str,
        efficiency_ratio: Option<f64>,
        profitable_windows: Option<i64>,
        total_windows: Option<i64>,
        param_stability: Option<&str>,
        status: &str,
        execution_time_ms: Option<i64>,
    ) -> Result<String> {
        let created_at = chrono::Utc::now().to_rfc3339();
        let conn = self.conn.lock().expect("mutex poisoned");
        conn.execute(
            "INSERT INTO walk_forward_validations
                (id, sweep_id, n_windows, train_pct, mode, objective,
                 efficiency_ratio, profitable_windows, total_windows,
                 param_stability, status, execution_time_ms, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            rusqlite::params![
                id,
                sweep_id,
                n_windows,
                train_pct,
                mode,
                objective,
                efficiency_ratio,
                profitable_windows,
                total_windows,
                param_stability,
                status,
                execution_time_ms,
                created_at,
            ],
        )
        .context("Failed to insert walk-forward validation")?;
        Ok(created_at)
    }

    fn get_walk_forward_validations(&self, sweep_id: &str) -> Result<Vec<WalkForwardValidation>> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT id, sweep_id, n_windows, train_pct, mode, objective,
                        efficiency_ratio, profitable_windows, total_windows,
                        param_stability, analysis, status, execution_time_ms,
                        created_at
                 FROM walk_forward_validations
                 WHERE sweep_id = ?1
                 ORDER BY created_at DESC",
            )
            .context("Failed to prepare walk-forward validations query")?;

        let rows = stmt
            .query_map(rusqlite::params![sweep_id], |row| {
                Ok(WalkForwardValidation {
                    id: row.get(0)?,
                    sweep_id: row.get(1)?,
                    n_windows: row.get(2)?,
                    train_pct: row.get(3)?,
                    mode: row.get(4)?,
                    objective: row.get(5)?,
                    efficiency_ratio: row.get(6)?,
                    profitable_windows: row.get(7)?,
                    total_windows: row.get(8)?,
                    param_stability: row.get(9)?,
                    analysis: row.get(10)?,
                    status: row.get(11)?,
                    execution_time_ms: row.get(12)?,
                    created_at: row.get(13)?,
                })
            })
            .context("Failed to query walk-forward validations")?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to collect walk-forward validations")?;

        Ok(rows)
    }

    fn set_walk_forward_analysis(&self, validation_id: &str, analysis: &str) -> Result<bool> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let updated = conn.execute(
            "UPDATE walk_forward_validations SET analysis = ?1 WHERE id = ?2",
            rusqlite::params![analysis, validation_id],
        )?;
        Ok(updated > 0)
    }

    fn delete_walk_forward_validation(&self, id: &str) -> Result<bool> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let n = conn
            .execute(
                "DELETE FROM walk_forward_validations WHERE id = ?1",
                rusqlite::params![id],
            )
            .context("Failed to delete walk-forward validation")?;
        Ok(n > 0)
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_store() -> SqliteRunStore {
        crate::data::database::Database::open_in_memory()
            .expect("open_in_memory")
            .runs()
    }

    fn sample_trades() -> Vec<TradeRow> {
        vec![
            TradeRow {
                trade_id: 1,
                entry_datetime: 1_704_844_800, // 2024-01-10
                exit_datetime: 1_705_708_800,  // 2024-01-20
                entry_cost: 500.0,
                exit_proceeds: 700.0,
                entry_amount: 500.0,
                entry_label: "DR".to_string(),
                exit_amount: 700.0,
                exit_label: "CR".to_string(),
                pnl: 200.0,
                days_held: 10,
                exit_type: "TakeProfit".to_string(),
                legs: Value::Array(vec![]),
                computed_quantity: Some(2),
                entry_equity: Some(10000.0),
                stock_entry_price: None,
                stock_exit_price: None,
                stock_pnl: None,
                group: None,
            },
            TradeRow {
                trade_id: 2,
                entry_datetime: 1_706_745_600, // 2024-02-01
                exit_datetime: 1_707_955_200,  // 2024-02-15
                entry_cost: 600.0,
                exit_proceeds: 550.0,
                entry_amount: 600.0,
                entry_label: "DR".to_string(),
                exit_amount: 550.0,
                exit_label: "CR".to_string(),
                pnl: -50.0,
                days_held: 14,
                exit_type: "StopLoss".to_string(),
                legs: Value::Array(vec![]),
                computed_quantity: None,
                entry_equity: Some(10200.0),
                stock_entry_price: None,
                stock_exit_price: None,
                stock_pnl: None,
                group: Some("group-A".to_string()),
            },
        ]
    }

    #[test]
    fn test_insert_run_and_get() {
        let store = make_store();
        let params = serde_json::json!({"dte": 45, "delta": 0.3});
        let id = uuid::Uuid::new_v4().to_string();

        let created_at = store
            .insert_run(
                &id,
                None,
                None,
                "SPY",
                50_000.0,
                &params,
                Some(0.15),
                Some(0.55),
                Some(-0.12),
                Some(1.5),
                Some(2.0),
                Some(0.15),
                Some(1.8),
                Some(42),
                Some(119.05),
                Some(-300.0),
                "{}",
                Some(1234),
                None,
                None,
                None,
                "manual",
                None,
            )
            .expect("insert_run");
        assert!(!created_at.is_empty());

        // Insert trades
        let trades = sample_trades();
        store.insert_trades(&id, &trades).expect("insert_trades");

        // Get detail
        let detail = store.get_run(&id).unwrap().expect("should exist");
        assert_eq!(detail.symbol, "SPY");
        assert_eq!(detail.capital, 50_000.0);
        assert_eq!(detail.sharpe, Some(1.5));
        assert_eq!(detail.trades.len(), 2);
        assert_eq!(detail.trades[0].trade_id, 1);
        assert_eq!(detail.trades[1].exit_type, "StopLoss");
    }

    #[test]
    fn test_get_run_not_found() {
        let store = make_store();
        assert!(store.get_run("nonexistent").unwrap().is_none());
    }

    #[test]
    fn test_insert_sweep_and_runs() {
        let store = make_store();
        let sweep_id = uuid::Uuid::new_v4().to_string();
        let config = serde_json::json!({"dte": [30, 45, 60]});

        let created_at = store
            .insert_sweep(
                &sweep_id,
                None,
                "SPY",
                &config,
                "sharpe",
                "grid",
                3,
                Some(5000),
                "manual",
                None,
            )
            .expect("insert_sweep");
        assert!(!created_at.is_empty());

        // Insert runs for this sweep
        for i in 0..3 {
            let run_id = uuid::Uuid::new_v4().to_string();
            let params = serde_json::json!({"dte": 30 + i * 15});
            store
                .insert_run(
                    &run_id,
                    Some(&sweep_id),
                    None,
                    "SPY",
                    50_000.0,
                    &params,
                    Some(0.10 + f64::from(i) * 0.05),
                    Some(0.50),
                    Some(-0.10),
                    Some(1.0 + f64::from(i) * 0.5),
                    None,
                    None,
                    None,
                    Some(10),
                    None,
                    None,
                    "{}",
                    Some(100),
                    None,
                    None,
                    None,
                    "manual",
                    None,
                )
                .unwrap();
        }

        // Get sweep detail
        let detail = store.get_sweep(&sweep_id).unwrap().expect("should exist");
        assert_eq!(detail.symbol, "SPY");
        assert_eq!(detail.combinations, 3);
        assert_eq!(detail.runs.len(), 3);
        // Ordered by sharpe DESC
        assert!(detail.runs[0].sharpe >= detail.runs[1].sharpe);
    }

    #[test]
    fn test_list_mixed() {
        let store = make_store();
        let params = serde_json::json!({});

        // Insert a standalone run
        let run_id = uuid::Uuid::new_v4().to_string();
        store
            .insert_run(
                &run_id,
                None,
                None,
                "SPY",
                10_000.0,
                &params,
                Some(0.05),
                Some(0.50),
                Some(-0.10),
                Some(1.2),
                None,
                None,
                None,
                Some(5),
                None,
                None,
                "{}",
                Some(50),
                None,
                None,
                None,
                "manual",
                None,
            )
            .unwrap();

        // Insert a sweep with runs
        let sweep_id = uuid::Uuid::new_v4().to_string();
        store
            .insert_sweep(
                &sweep_id,
                None,
                "QQQ",
                &params,
                "sharpe",
                "grid",
                2,
                Some(100),
                "manual",
                None,
            )
            .unwrap();

        let sweep_run_id = uuid::Uuid::new_v4().to_string();
        store
            .insert_run(
                &sweep_run_id,
                Some(&sweep_id),
                None,
                "QQQ",
                10_000.0,
                &params,
                Some(0.10),
                Some(0.60),
                Some(-0.05),
                Some(2.0),
                None,
                None,
                None,
                Some(10),
                None,
                None,
                "{}",
                Some(50),
                None,
                None,
                None,
                "manual",
                None,
            )
            .unwrap();

        let response = store.list(None).unwrap();
        assert_eq!(response.overview.total_runs, 2); // 2 runs total in runs table
        assert_eq!(response.rows.len(), 2); // 1 single + 1 sweep
    }

    #[test]
    fn test_delete_run() {
        let store = make_store();
        let params = serde_json::json!({});
        let id = uuid::Uuid::new_v4().to_string();

        store
            .insert_run(
                &id, None, None, "SPY", 10_000.0, &params, None, None, None, None, None, None,
                None, None, None, None, "{}", None, None, None, None, "manual", None,
            )
            .unwrap();

        assert!(store.delete_run(&id).unwrap());
        assert!(store.get_run(&id).unwrap().is_none());
        assert!(!store.delete_run(&id).unwrap());
    }

    #[test]
    fn test_delete_sweep_cascades() {
        let store = make_store();
        let params = serde_json::json!({});
        let sweep_id = uuid::Uuid::new_v4().to_string();
        let run_id = uuid::Uuid::new_v4().to_string();

        store
            .insert_sweep(
                &sweep_id, None, "SPY", &params, "sharpe", "grid", 1, None, "manual", None,
            )
            .unwrap();
        store
            .insert_run(
                &run_id,
                Some(&sweep_id),
                None,
                "SPY",
                10_000.0,
                &params,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "{}",
                None,
                None,
                None,
                None,
                "manual",
                None,
            )
            .unwrap();

        // Deleting sweep should cascade to runs
        assert!(store.delete_sweep(&sweep_id).unwrap());
        assert!(store.get_sweep(&sweep_id).unwrap().is_none());
        assert!(store.get_run(&run_id).unwrap().is_none());
    }

    #[test]
    fn test_set_analysis_on_run() {
        let store = make_store();
        let params = serde_json::json!({});
        let id = uuid::Uuid::new_v4().to_string();

        store
            .insert_run(
                &id, None, None, "SPY", 10_000.0, &params, None, None, None, None, None, None,
                None, None, None, None, "{}", None, None, None, None, "manual", None,
            )
            .unwrap();

        assert!(store.set_run_analysis(&id, "Good results").unwrap());
        let detail = store.get_run(&id).unwrap().expect("should exist");
        assert_eq!(detail.analysis.as_deref(), Some("Good results"));

        // Non-existent id
        assert!(!store.set_run_analysis("nonexistent", "text").unwrap());
    }

    #[test]
    fn test_set_analysis_on_sweep() {
        let store = make_store();
        let params = serde_json::json!({});
        let sweep_id = uuid::Uuid::new_v4().to_string();

        store
            .insert_sweep(
                &sweep_id, None, "SPY", &params, "sharpe", "grid", 1, None, "manual", None,
            )
            .unwrap();

        assert!(store
            .set_sweep_analysis(&sweep_id, "Sweep analysis")
            .unwrap());
        let detail = store.get_sweep(&sweep_id).unwrap().expect("should exist");
        assert_eq!(detail.analysis.as_deref(), Some("Sweep analysis"));
    }

    #[test]
    fn test_sanitize_nan_infinity() {
        let store = make_store();
        let params = serde_json::json!({});
        let id = uuid::Uuid::new_v4().to_string();

        store
            .insert_run(
                &id,
                None,
                None,
                "SPY",
                10_000.0,
                &params,
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(1.5),
                None,
                None,
                None,
                None,
                None,
                None,
                "{}",
                None,
                None,
                None,
                None,
                "manual",
                None,
            )
            .unwrap();

        let detail = store.get_run(&id).unwrap().expect("should exist");
        // NaN and Infinity should be stored as NULL → None
        assert!(detail.total_return.is_none());
        assert!(detail.win_rate.is_none());
        assert!(detail.max_drawdown.is_none());
        // Finite value should be preserved
        assert_eq!(detail.sharpe, Some(1.5));
    }
}
