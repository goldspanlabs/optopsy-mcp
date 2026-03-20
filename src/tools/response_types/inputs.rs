//! Input/parameter types shared across tool and server layers.

use garde::Validate;
use schemars::JsonSchema;
use serde::Deserialize;

/// Default years of history for analysis tools.
pub fn default_analysis_years() -> u32 {
    5
}

/// Default label for trade P&L source.
fn default_pnl_label() -> String {
    "Trade P&L".to_string()
}

/// Default correlation field.
fn default_corr_field() -> String {
    "return".to_string()
}

/// Default significance threshold for hypothesis testing.
pub fn default_significance() -> f64 {
    0.05
}

/// Default Jaccard similarity threshold for deduplication.
pub fn default_dedup_threshold() -> f64 {
    0.5
}

/// Source for distribution analysis: either price returns or raw trade P&L values.
#[derive(Debug, Clone, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
#[serde(tag = "type")]
pub enum DistributionSource {
    /// Compute returns from OHLCV price data
    #[serde(rename = "price_returns")]
    PriceReturns {
        /// Ticker symbol
        #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
        symbol: String,
        /// Years of history (default: 5)
        #[serde(default = "default_analysis_years")]
        #[garde(range(min = 1, max = 50))]
        years: u32,
    },
    /// Use pre-computed values (e.g., trade P&L array from a backtest)
    #[serde(rename = "trade_pnl")]
    TradePnl {
        /// Array of P&L values
        #[garde(length(min = 1))]
        values: Vec<f64>,
        /// Label for this dataset
        #[serde(default = "default_pnl_label")]
        #[garde(length(min = 1))]
        label: String,
    },
}

/// Series specification for correlation analysis.
#[derive(Debug, Clone, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct CorrelationSeries {
    /// Ticker symbol
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Price field: "close", "open", "high", "low", "volume", "return" (default)
    #[serde(default = "default_corr_field")]
    #[garde(length(min = 1))]
    pub field: String,
}
