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
                "[NEXT] Call run_backtest({ strategy, symbol }) — data is auto-loaded".to_string(),
                "[TIP] Call check_cache_status to verify data is cached".to_string(),
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
            vec![
                "[NEXT] Call list_strategies() to browse available strategies".to_string(),
                "[THEN] Call run_backtest({ strategy, symbol }) for full simulation".to_string(),
            ]
        } else {
            vec![
                format!(
                    "[NEXT] Call list_strategies() to browse available strategies (specify symbol: \"{}\" in subsequent tools)",
                    symbols[0]
                ),
                format!(
                    "[THEN] Call run_backtest({{ strategy, symbol: \"{}\" }})",
                    symbols[0]
                ),
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
