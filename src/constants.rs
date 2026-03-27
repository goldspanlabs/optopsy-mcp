//! Shared numeric constants used throughout the codebase.

/// Trading days per year (US equities).
pub const TRADING_DAYS_PER_YEAR: f64 = 252.0;

/// Calendar days per year (for date cutoffs).
pub const CALENDAR_DAYS_PER_YEAR: i64 = 365;

/// Standard statistical significance threshold (alpha = 0.05).
pub const P_VALUE_THRESHOLD: f64 = 0.05;

/// Default years of price history for analysis tools.
pub const DEFAULT_ANALYSIS_YEARS: u32 = 5;
