-- Add unique constraint on strategy name to prevent duplicates.
CREATE UNIQUE INDEX idx_strategies_name_unique ON strategies(name);
