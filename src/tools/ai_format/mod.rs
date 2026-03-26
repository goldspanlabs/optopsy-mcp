//! AI response formatting layer.

mod data;
mod hypothesis;
mod stats;

pub use data::{format_list_symbols, format_raw_prices};
pub use hypothesis::format_hypotheses;
pub use stats::{
    format_aggregate_prices, format_correlate, format_distribution, format_regime_detect,
    format_rolling_metric,
};
