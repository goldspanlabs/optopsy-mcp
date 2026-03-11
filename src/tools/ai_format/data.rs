//! Format data-loading and strategy-listing results into AI-enriched responses.
//!
//! Builds structured responses for `load_data`, `get_raw_prices`, and
//! `list_strategies` with row counts, date ranges, column lists, and
//! suggested next steps tailored to each tool's output.

use std::collections::HashMap;

use crate::tools::response_types::{
    DateRange, LoadDataResponse, PriceBar, RawPricesResponse, StrategiesResponse, StrategyInfo,
};

/// Format a data load result into a response with row count, date range, and next steps.
pub fn format_load_data(
    symbol: &str,
    rows: usize,
    symbols: Vec<String>,
    date_range: DateRange,
    columns: Vec<String>,
) -> LoadDataResponse {
    let symbol_list = if symbols.is_empty() {
        "unknown".to_string()
    } else {
        symbols.join(", ")
    };
    let start = date_range.start.as_deref().unwrap_or("unknown");
    let end = date_range.end.as_deref().unwrap_or("unknown");
    let summary =
        format!("Loaded {rows} rows of options data for {symbol_list} from {start} to {end}.",);

    LoadDataResponse {
        summary,
        symbol: symbol.to_string(),
        rows,
        symbols,
        date_range,
        columns,
        suggested_next_steps: vec![
            "[NEXT] Call list_strategies() to browse available strategies and choose one to analyze".to_string(),
            "[THEN] Call run_options_backtest({ strategy, symbol }) for full simulation".to_string(),
        ],
    }
}

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
    fn format_load_data_with_missing_dates() {
        let response = format_load_data(
            "SPY",
            1000,
            vec!["SPY".to_string()],
            DateRange {
                start: None,
                end: None,
            },
            vec!["col1".to_string()],
        );
        assert_eq!(response.rows, 1000);
        assert_eq!(response.symbol, "SPY");
        assert!(response.summary.contains("unknown"));
    }

    #[test]
    fn format_load_data_empty_symbols_shows_unknown() {
        let response = format_load_data(
            "QQQ",
            500,
            vec![],
            DateRange {
                start: Some("2024-01-01".to_string()),
                end: Some("2024-12-31".to_string()),
            },
            vec!["col1".to_string()],
        );
        assert_eq!(response.symbol, "QQQ");
        assert!(
            response.summary.contains("unknown"),
            "summary should fall back to 'unknown' when symbols is empty, got: {}",
            response.summary
        );
        assert!(!response.summary.contains("for  from"));
    }

    #[test]
    fn format_load_data_with_dates() {
        let response = format_load_data(
            "SPY",
            5000,
            vec!["SPY".to_string(), "QQQ".to_string()],
            DateRange {
                start: Some("2024-01-01".to_string()),
                end: Some("2024-12-31".to_string()),
            },
            vec!["col1".to_string(), "col2".to_string()],
        );
        assert_eq!(response.rows, 5000);
        assert_eq!(response.symbol, "SPY");
        assert!(response.summary.contains("SPY, QQQ"));
        assert!(response.summary.contains("2024-01-01"));
        assert!(response.summary.contains("2024-12-31"));
    }
}
