//! Format raw-price and symbol-listing results into AI-enriched responses.
//!
//! Builds structured responses for `get_raw_prices` and `list_symbols`
//! with row counts, date ranges, column lists, and suggested next steps
//! tailored to each tool's output.

use crate::tools::response_types::{
    DateRange, ListSymbolsResponse, PriceBar, RawPricesResponse, SymbolCategory,
};

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
pub fn format_list_symbols(
    total: usize,
    total_matches: usize,
    categories: Vec<SymbolCategory>,
    query: Option<&str>,
) -> ListSymbolsResponse {
    let summary = if total == 0 {
        "No cached data found. Place .parquet files under the cache directory.".to_string()
    } else if let Some(q) = query {
        if total_matches == 0 {
            format!(
                "No symbols matching \"{q}\" found. {total} symbols cached — try a different query or omit query to see category counts."
            )
        } else {
            format!("Found {total_matches} symbols matching \"{q}\" ({total} total cached).")
        }
    } else {
        let parts: Vec<String> = categories
            .iter()
            .map(|c| format!("{} ({})", c.category, c.count))
            .collect();
        format!(
            "{total} symbols cached across {} categories: {}. Use query to search for specific symbols.",
            categories.len(),
            parts.join(", "),
        )
    };

    let suggested_next_steps = if query.is_some() && total_matches > 0 {
        vec![
            "[NEXT] Call run_options_backtest({ symbol: \"<symbol>\" }) to backtest an options strategy".to_string(),
            "[NEXT] Call run_stock_backtest({ symbol: \"<symbol>\", entry_signal: ... }) for a stock backtest".to_string(),
            "[NEXT] Call get_raw_prices({ symbol: \"<symbol>\" }) to view price data for charting".to_string(),
        ]
    } else {
        vec![
            "[NEXT] Call list_symbols({ query: \"SPY\" }) to search for a specific symbol"
                .to_string(),
            "[NEXT] Call run_script with a strategy name to run a backtest".to_string(),
        ]
    };

    ListSymbolsResponse {
        summary,
        total,
        total_matches,
        categories,
        suggested_next_steps,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_list_symbols_empty() {
        let response = format_list_symbols(0, 0, vec![], None);
        assert_eq!(response.total, 0);
        assert!(response.categories.is_empty());
        assert!(response.summary.contains("No cached data"));
    }

    #[test]
    fn format_list_symbols_summary_mode() {
        let categories = vec![
            SymbolCategory {
                category: "etf".to_string(),
                count: 100,
                symbols: vec![],
            },
            SymbolCategory {
                category: "stocks".to_string(),
                count: 200,
                symbols: vec![],
            },
        ];
        let response = format_list_symbols(300, 300, categories, None);
        assert_eq!(response.total, 300);
        assert_eq!(response.categories.len(), 2);
        assert!(response.summary.contains("300 symbols"));
        assert!(response.summary.contains("etf (100)"));
    }

    #[test]
    fn format_list_symbols_search_mode() {
        let categories = vec![SymbolCategory {
            category: "etf".to_string(),
            count: 100,
            symbols: vec!["SPY".to_string(), "SPLG".to_string()],
        }];
        let response = format_list_symbols(300, 2, categories, Some("SP"));
        assert_eq!(response.total, 300);
        assert_eq!(response.total_matches, 2);
        assert!(response.summary.contains("2 symbols matching"));
        assert!(response.summary.contains("\"SP\""));
    }

    #[test]
    fn format_list_symbols_search_no_matches() {
        let response = format_list_symbols(300, 0, vec![], Some("ZZZZZ"));
        assert_eq!(response.total_matches, 0);
        assert!(response.summary.contains("No symbols matching"));
    }
}
