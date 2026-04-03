-- Add permutation test columns to runs table.
-- p_value: raw (unadjusted) p-value from sign-flip permutation test.
-- significant: whether the combo is significant after BH-FDR correction (0/1).
ALTER TABLE runs ADD COLUMN p_value REAL;
ALTER TABLE runs ADD COLUMN significant INTEGER;
