-- Walk-forward validation columns on sweeps
ALTER TABLE sweeps ADD COLUMN wf_efficiency_ratio REAL;
ALTER TABLE sweeps ADD COLUMN wf_profitable_windows INTEGER;
ALTER TABLE sweeps ADD COLUMN wf_total_windows INTEGER;
ALTER TABLE sweeps ADD COLUMN wf_param_stability TEXT;
ALTER TABLE sweeps ADD COLUMN wf_config TEXT CHECK(wf_config IS NULL OR json_valid(wf_config));
ALTER TABLE sweeps ADD COLUMN wf_analysis TEXT;
ALTER TABLE sweeps ADD COLUMN wf_status TEXT;
