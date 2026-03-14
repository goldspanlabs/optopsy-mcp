//! Check whether a Parquet cache file exists for a symbol and report its status.
//!
//! Inspects the local cache directory for options or OHLCV data, returning
//! file existence, last-modified timestamp, and the full file path.

use anyhow::Result;
use std::sync::Arc;

use crate::data::cache::CachedStore;

use super::response_types::CheckCacheResponse;

/// Return cache existence, last-updated timestamp, and file path for the given symbol.
pub fn execute(
    cache: &Arc<CachedStore>,
    symbol: &str,
    category: &str,
) -> Result<CheckCacheResponse> {
    let path = cache.cache_path(symbol, category)?;
    let file_path = path.display().to_string();

    let (exists, last_updated) = if path.exists() {
        let metadata = std::fs::metadata(&path)?;
        let modified = metadata.modified()?;
        let datetime: chrono::DateTime<chrono::Utc> = modified.into();
        (true, Some(datetime.to_rfc3339()))
    } else {
        (false, None)
    };

    let upper = symbol.to_uppercase();
    let summary = if exists {
        format!(
            "Cache hit for {upper} in '{category}'. Last updated: {}.",
            last_updated.as_deref().unwrap_or("unknown"),
        )
    } else {
        format!("No cached file for {upper} in '{category}' at {file_path}.")
    };

    let suggested_next_steps = if exists {
        let mut steps = vec![match category {
            "prices" => format!(
                "[NEXT] Call get_raw_prices({{ symbol: \"{upper}\" }}) or run_stock_backtest({{ symbol: \"{upper}\", entry_signal: ... }})"
            ),
            _ => format!(
                "[NEXT] Call run_options_backtest({{ strategy: \"<name>\", symbol: \"{upper}\" }})"
            ),
        }];
        steps.push("[TIP] Check last_updated to decide if data should be refreshed".to_string());
        steps
    } else {
        vec![match category {
            "prices" => format!(
                "[NEXT] Call get_raw_prices({{ symbol: \"{upper}\" }}) — OHLCV data is auto-fetched"
            ),
            _ => format!(
                "[NEXT] Call run_options_backtest({{ strategy: \"<name>\", symbol: \"{upper}\" }}) — data is auto-loaded"
            ),
        }]
    };

    Ok(CheckCacheResponse {
        summary,
        symbol: upper,
        exists,
        last_updated,
        file_path,
        suggested_next_steps,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn test_cache_dir() -> PathBuf {
        std::env::temp_dir().join("optopsy_test_cache_nonexistent")
    }

    #[test]
    fn cache_status_nonexistent_file() {
        let cache = Arc::new(CachedStore::new(
            test_cache_dir(),
            "options".to_string(),
            None,
        ));
        let result = execute(&cache, "NONEXISTENT", "options").unwrap();
        assert!(!result.exists);
        assert!(result.last_updated.is_none());
        assert!(result.file_path.contains("NONEXISTENT.parquet"));
        assert!(result.summary.contains("No cached file"));
    }

    #[test]
    fn cache_status_symbol_uppercased() {
        let cache = Arc::new(CachedStore::new(
            test_cache_dir(),
            "options".to_string(),
            None,
        ));
        let result = execute(&cache, "spy", "prices").unwrap();
        assert!(result.file_path.contains("SPY.parquet"));
        assert!(result.summary.contains("SPY"));
    }
}
