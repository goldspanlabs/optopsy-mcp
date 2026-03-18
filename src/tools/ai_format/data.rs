//! Format strategy-listing and raw-price results into AI-enriched responses.
//!
//! Builds structured responses for `get_raw_prices` and `list_strategies`
//! with row counts, date ranges, column lists, and suggested next steps
//! tailored to each tool's output.

use std::collections::HashMap;

use crate::tools::response_types::{
    DateRange, ListSymbolsResponse, PriceBar, RawPricesResponse, StrategiesResponse, StrategyInfo,
    SymbolCategory,
};

/// Format the full strategy list into a categorized summary response.
pub fn format_strategies(strategies: Vec<StrategyInfo>) -> StrategiesResponse {
    let total = strategies.len();
    let mut categories: HashMap<String, usize> = HashMap::new();
    for s in &strategies {
        *categories.entry(s.category.clone()).or_default() += 1;
    }

    let cat_parts: Vec<String> = {
        let mut sorted: Vec<_> = categories.iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(a.1));
        sorted
            .iter()
            .map(|(cat, count)| format!("{cat} ({count})"))
            .collect()
    };

    let summary = if total == 0 {
        "No strategies are currently available.".to_string()
    } else {
        format!(
            "{} strategies available across {} categories: {}.",
            total,
            categories.len(),
            cat_parts.join(", "),
        )
    };

    StrategiesResponse {
        summary,
        total,
        categories,
        strategies,
        suggested_next_steps: vec![
            "[NEXT] Call run_options_backtest({ strategy: \"<chosen_strategy>\", symbol }) for full simulation".to_string(),
            "[THEN] Call parameter_sweep to optimize across deltas and DTEs".to_string(),
        ],
    }
}

/// Format raw OHLCV price bars into a response suitable for chart generation.
pub fn format_raw_prices(
    symbol: &str,
    total_rows: usize,
    returned_rows: usize,
    sampled: bool,
    date_range: DateRange,
    prices: Vec<PriceBar>,
) -> RawPricesResponse {
    let summary = if sampled {
        format!(
            "Returning {returned_rows} sampled price bars for {symbol} (from {total_rows} total). \
             Use these data points directly to generate charts or perform analysis."
        )
    } else {
        format!(
            "Returning {returned_rows} price bars for {symbol}. \
             Use these data points directly to generate charts or perform analysis."
        )
    };

    RawPricesResponse {
        summary,
        symbol: symbol.to_string(),
        total_rows,
        returned_rows,
        sampled,
        date_range,
        prices,
        suggested_next_steps: vec![
            "[TIP] Use the prices array to generate a line chart (close prices), candlestick chart (OHLC), or area chart.".to_string(),
            "[TIP] Combine with backtest trade_log data to overlay strategy performance on price action.".to_string(),
        ],
    }
}

/// Format cached symbol listing into an AI-enriched response.
pub fn format_list_symbols(total: usize, categories: Vec<SymbolCategory>) -> ListSymbolsResponse {
    let summary = if total == 0 {
        "No cached data found. Place .parquet files under the cache directory.".to_string()
    } else {
        let parts: Vec<String> = categories
            .iter()
            .map(|c| format!("{} ({})", c.category, c.count))
            .collect();
        format!(
            "{total} symbols cached across {} categories: {}.",
            categories.len(),
            parts.join(", "),
        )
    };

    ListSymbolsResponse {
        summary,
        total,
        categories,
        suggested_next_steps: vec![
            "[NEXT] Call run_options_backtest({ symbol: \"<symbol>\" }) to backtest an options strategy".to_string(),
            "[NEXT] Call run_stock_backtest({ symbol: \"<symbol>\", entry_signal: ... }) for a stock backtest".to_string(),
            "[NEXT] Call get_raw_prices({ symbol: \"<symbol>\" }) to view price data for charting".to_string(),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_strategies_category_counts() {
        let strategies = vec![
            StrategyInfo {
                name: "long_call".to_string(),
                display_name: "Long Call".to_string(),
                category: "Singles".to_string(),
                legs: 1,
                description: "Buy a call".to_string(),
                default_deltas: vec![],
            },
            StrategyInfo {
                name: "short_put".to_string(),
                display_name: "Short Put".to_string(),
                category: "Singles".to_string(),
                legs: 1,
                description: "Sell a put".to_string(),
                default_deltas: vec![],
            },
            StrategyInfo {
                name: "bull_call_spread".to_string(),
                display_name: "Bull Call Spread".to_string(),
                category: "Spreads".to_string(),
                legs: 2,
                description: "Bullish spread".to_string(),
                default_deltas: vec![],
            },
        ];
        let response = format_strategies(strategies);
        assert_eq!(response.total, 3);
        assert_eq!(response.categories["Singles"], 2);
        assert_eq!(response.categories["Spreads"], 1);
        assert!(response.summary.contains('3'));
    }

    #[test]
    fn format_list_symbols_empty() {
        let response = format_list_symbols(0, vec![]);
        assert_eq!(response.total, 0);
        assert!(response.categories.is_empty());
        assert!(response.summary.contains("No cached data"));
    }

    #[test]
    fn format_list_symbols_with_categories() {
        let categories = vec![
            SymbolCategory {
                category: "etf".to_string(),
                count: 2,
                symbols: vec!["QQQ".to_string(), "SPY".to_string()],
            },
            SymbolCategory {
                category: "stocks".to_string(),
                count: 1,
                symbols: vec!["AAPL".to_string()],
            },
        ];
        let response = format_list_symbols(3, categories);
        assert_eq!(response.total, 3);
        assert_eq!(response.categories.len(), 2);
        assert!(response.summary.contains("3 symbols"));
        assert!(response.summary.contains("2 categories"));
        assert!(response.summary.contains("etf (2)"));
    }
}
