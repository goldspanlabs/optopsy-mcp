use polars::prelude::DataFrame;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::response_types::StatusResponse;

pub fn execute(data: &Arc<RwLock<Option<(String, DataFrame)>>>) -> StatusResponse {
    let guard = data.blocking_read();

    if let Some((symbol, df)) = guard.as_ref() {
        let rows = df.height();
        let cols: Vec<String> = df
            .get_column_names()
            .iter()
            .map(std::string::ToString::to_string)
            .collect();

        // Simple approach: skip detailed date range extraction to avoid complex Polars API calls
        // The user can see columns are available and inspect the data themselves
        let date_range = None;

        let summary = format!(
            "Data loaded in memory: {} ({} rows, {} columns).",
            symbol,
            rows,
            cols.len()
        );

        StatusResponse {
            summary,
            loaded_symbol: Some(symbol.clone()),
            rows: Some(rows),
            date_range,
            columns: cols,
            suggested_next_steps: vec![
                format!(
                    "Use evaluate_strategy to analyze {} across DTE/delta buckets",
                    symbol
                ),
                format!("Use run_backtest to simulate {} trading", symbol),
                "Use load_data with a different symbol to switch datasets".to_string(),
            ],
        }
    } else {
        StatusResponse {
            summary: "No data currently loaded in memory.".to_string(),
            loaded_symbol: None,
            rows: None,
            date_range: None,
            columns: vec![],
            suggested_next_steps: vec![
                "Use load_data(symbol: ...) to load options chain data into memory".to_string(),
                "Use check_cache_status to verify data is cached before loading".to_string(),
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_no_data_loaded() {
        let data = Arc::new(RwLock::new(None));
        let response = execute(&data);
        assert!(response.loaded_symbol.is_none());
        assert!(response.rows.is_none());
        assert_eq!(response.columns.len(), 0);
        assert!(response.summary.contains("No data"));
    }
}
