-- Add source + thread_id columns to runs and sweeps tables.
ALTER TABLE runs ADD COLUMN source TEXT NOT NULL DEFAULT 'manual';
ALTER TABLE runs ADD COLUMN thread_id TEXT;
ALTER TABLE sweeps ADD COLUMN source TEXT NOT NULL DEFAULT 'manual';
ALTER TABLE sweeps ADD COLUMN thread_id TEXT;
