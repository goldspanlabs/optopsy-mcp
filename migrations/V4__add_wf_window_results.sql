-- Add window_results column to store per-window JSON data
ALTER TABLE walk_forward_validations ADD COLUMN window_results TEXT;
