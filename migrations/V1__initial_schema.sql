-- Initial schema: all tables and indices.
-- Note: PRAGMAs (WAL, foreign_keys) are set in init_schema() before
-- refinery runs, since they cannot execute inside a transaction.

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
    source            TEXT NOT NULL DEFAULT 'manual',
    thread_id         TEXT,
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
    source            TEXT NOT NULL DEFAULT 'manual',
    thread_id         TEXT,
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

CREATE INDEX IF NOT EXISTS idx_strategies_category ON strategies(category);

-- Threads
CREATE TABLE IF NOT EXISTS threads (
    id          TEXT PRIMARY KEY,
    strategy_id TEXT,
    title       TEXT,
    status      TEXT NOT NULL DEFAULT 'regular',
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_threads_strategy_id ON threads(strategy_id);

-- Messages
CREATE TABLE IF NOT EXISTS messages (
    id          TEXT PRIMARY KEY,
    thread_id   TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    parent_id   TEXT,
    format      TEXT NOT NULL DEFAULT 'aui/v0',
    content     TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_messages_thread_id ON messages(thread_id);

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

CREATE INDEX IF NOT EXISTS idx_results_thread_id ON results(thread_id);
CREATE INDEX IF NOT EXISTS idx_results_tool_call_id ON results(tool_call_id);
