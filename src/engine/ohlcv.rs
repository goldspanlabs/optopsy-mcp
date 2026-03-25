//! General OHLCV DataFrame utilities.
//!
//! Re-exports from `stock_sim` that are used across the codebase (scripting,
//! signals, server, tools). These will move here permanently when `stock_sim`
//! is removed.

pub use super::stock_sim::{bars_from_df, detect_date_col, load_ohlcv_df, resample_ohlcv};
