//! Response types for data tools: `list_symbols`, `raw_prices`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A group of symbols belonging to a single data category.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SymbolCategory {
    /// Category name (e.g. "options", "etf", "stocks", "futures", "indices")
    pub category: String,
    /// Total symbols cached in this category
    pub count: usize,
    /// Matching symbols (populated only when a search query is provided; empty in summary mode)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub symbols: Vec<String>,
}

/// Response for `list_symbols` — cached data summary or search results.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ListSymbolsResponse {
    pub summary: String,
    /// Total symbols cached across all categories
    pub total: usize,
    /// Number of symbols matching the query (equals `total` when no query)
    pub total_matches: usize,
    pub categories: Vec<SymbolCategory>,
    pub suggested_next_steps: Vec<String>,
}

/// Start and end date strings for a data range.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DateRange {
    pub start: Option<i64>,
    pub end: Option<i64>,
}

/// A single OHLCV price bar for `get_raw_prices`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PriceBar {
    pub date: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    /// Adjusted close price (if available)
    pub adjclose: Option<f64>,
    pub volume: u64,
}

/// Response for `get_raw_prices` — returns actual price data points for charting
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RawPricesResponse {
    pub summary: String,
    pub symbol: String,
    /// Total rows in the cached dataset (before sampling)
    pub total_rows: usize,
    /// Number of price bars returned in this response
    pub returned_rows: usize,
    /// Whether the data was down-sampled to fit the limit
    pub sampled: bool,
    pub date_range: DateRange,
    /// Raw OHLCV price bars — use directly for chart generation
    pub prices: Vec<PriceBar>,
    pub suggested_next_steps: Vec<String>,
}
