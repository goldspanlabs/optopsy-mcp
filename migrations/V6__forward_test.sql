-- Forward testing (paper trading) sessions and daily snapshots.
-- Tracks strategy execution state between incremental runs.

CREATE TABLE IF NOT EXISTS forward_test_sessions (
    id              TEXT PRIMARY KEY,
    strategy        TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    params          TEXT NOT NULL CHECK(json_valid(params)),
    status          TEXT NOT NULL DEFAULT 'active',    -- active, paused, stopped
    capital         REAL NOT NULL,
    current_equity  REAL NOT NULL,
    last_bar_date   TEXT,                              -- ISO date of last processed bar
    total_trades    INTEGER NOT NULL DEFAULT 0,
    realized_pnl    REAL NOT NULL DEFAULT 0.0,
    -- Serialized engine state (positions, orders, trackers)
    engine_state    TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(engine_state)),
    -- Baseline backtest metrics for drift detection
    baseline_sharpe     REAL,
    baseline_win_rate   REAL,
    baseline_max_dd     REAL,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fwd_sessions_status ON forward_test_sessions(status);
CREATE INDEX IF NOT EXISTS idx_fwd_sessions_strategy ON forward_test_sessions(strategy);

CREATE TABLE IF NOT EXISTS forward_test_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT NOT NULL REFERENCES forward_test_sessions(id) ON DELETE CASCADE,
    date            TEXT NOT NULL,                     -- ISO date
    equity          REAL NOT NULL,
    daily_pnl       REAL NOT NULL DEFAULT 0.0,
    cumulative_pnl  REAL NOT NULL DEFAULT 0.0,
    open_positions  INTEGER NOT NULL DEFAULT 0,
    trades_today    INTEGER NOT NULL DEFAULT 0,
    -- JSON details: trades opened/closed, signals fired
    details         TEXT DEFAULT '{}' CHECK(json_valid(details)),
    created_at      TEXT NOT NULL,
    UNIQUE(session_id, date)
);

CREATE INDEX IF NOT EXISTS idx_fwd_snapshots_session ON forward_test_snapshots(session_id);

CREATE TABLE IF NOT EXISTS forward_test_trades (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT NOT NULL REFERENCES forward_test_sessions(id) ON DELETE CASCADE,
    trade_id        INTEGER NOT NULL,
    action          TEXT NOT NULL,                     -- 'open' or 'close'
    date            TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    description     TEXT,
    entry_cost      REAL,
    exit_proceeds   REAL,
    pnl             REAL,
    exit_type       TEXT,
    details         TEXT DEFAULT '{}' CHECK(json_valid(details)),
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fwd_trades_session ON forward_test_trades(session_id);
