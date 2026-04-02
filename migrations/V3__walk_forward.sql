-- Walk-forward validations (1:N with sweeps)
CREATE TABLE IF NOT EXISTS walk_forward_validations (
    id                  TEXT PRIMARY KEY,
    sweep_id            TEXT NOT NULL REFERENCES sweeps(id) ON DELETE CASCADE,
    n_windows           INTEGER NOT NULL,
    train_pct           REAL NOT NULL,
    mode                TEXT NOT NULL DEFAULT 'rolling',
    objective           TEXT NOT NULL DEFAULT 'sharpe',
    efficiency_ratio    REAL,
    profitable_windows  INTEGER,
    total_windows       INTEGER,
    param_stability     TEXT,
    analysis            TEXT,
    status              TEXT NOT NULL DEFAULT 'completed',
    execution_time_ms   INTEGER,
    created_at          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wfv_sweep ON walk_forward_validations(sweep_id);
CREATE INDEX IF NOT EXISTS idx_wfv_created ON walk_forward_validations(created_at DESC);
