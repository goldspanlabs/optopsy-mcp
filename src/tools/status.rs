use polars::prelude::DataFrame;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::response_types::StatusResponse;

#[allow(clippy::implicit_hasher)]
pub async fn execute(data: &Arc<RwLock<HashMap<String, DataFrame>>>) -> StatusResponse {
    let guard = data.read().await;

    if guard.is_empty() {
        StatusResponse {
            summary: "No data currently loaded in memory.".to_string(),
            loaded_symbols: vec![],
            rows: None,
            date_range: None,
            columns: vec![],
            suggested_next_steps: vec![
                "Use load_data(symbol: ...) to load options chain data into memory".to_string(),
                "Use check_cache_status to verify data is cached before loading".to_string(),
            ],
        }
    } else {
        // Collect all symbols (sorted)
        let mut symbols: Vec<String> = guard.keys().cloned().collect();
        symbols.sort();

        // Aggregate row count
        let total_rows: usize = guard.values().map(DataFrame::height).sum();

        // Get columns from first symbol in sorted order (deterministic)
        let cols: Vec<String> = symbols
            .first()
            .and_then(|first_symbol| guard.get(first_symbol))
            .map(|df| {
                df.get_column_names()
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect()
            })
            .unwrap_or_default();

        let date_range = None;

        let summary = format!(
            "Data loaded in memory: {} symbol(s) ({} total rows, {} columns).",
            symbols.len(),
            total_rows,
            cols.len()
        );

        // Context-aware suggestions based on number of loaded symbols
        let suggested_next_steps = if symbols.len() == 1 {
            // Single symbol: no need to specify symbol parameter
            vec![
                "Use evaluate_strategy to analyze current data across DTE/delta buckets"
                    .to_string(),
                "Use run_backtest to simulate trading".to_string(),
                "Use suggest_parameters to get data-driven parameter recommendations".to_string(),
                "Use load_data with a different symbol to add more datasets".to_string(),
            ]
        } else {
            // Multiple symbols: must specify symbol parameter explicitly
            vec![
                format!(
                    "Use evaluate_strategy (specify symbol: \"{}\") to analyze data across DTE/delta buckets",
                    symbols[0]
                ),
                format!(
                    "Use run_backtest (specify symbol: \"{}\") to simulate trading",
                    symbols[0]
                ),
                format!(
                    "Use compare_strategies (specify symbol: \"{}\") to compare strategies side-by-side",
                    symbols[0]
                ),
                format!(
                    "Use suggest_parameters (specify symbol: \"{}\") for data-driven recommendations",
                    symbols[0]
                ),
                "Use load_data with another symbol to analyze additional datasets".to_string(),
            ]
        };

        StatusResponse {
            summary,
            loaded_symbols: symbols,
            rows: Some(total_rows),
            date_range,
            columns: cols,
            suggested_next_steps,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn status_no_data_loaded() {
        let data = Arc::new(RwLock::new(HashMap::new()));
        let response = execute(&data).await;
        assert!(response.loaded_symbols.is_empty());
        assert!(response.rows.is_none());
        assert_eq!(response.columns.len(), 0);
        assert!(response.summary.contains("No data"));
    }
}
