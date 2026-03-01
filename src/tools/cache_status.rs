use anyhow::Result;
use std::sync::Arc;

use crate::data::cache::CachedStore;

use super::response_types::CheckCacheResponse;

pub fn execute(
    cache: &Arc<CachedStore>,
    symbol: &str,
    category: &str,
) -> Result<CheckCacheResponse> {
    let path = cache.cache_path(symbol, category);
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
        vec![
            format!("Call load_data to load {upper} into memory for analysis."),
            "Check last_updated to decide if data should be refreshed with fetch_to_parquet."
                .to_string(),
        ]
    } else {
        vec![format!(
            "Call fetch_to_parquet to download {upper} data from Yahoo Finance."
        )]
    };

    Ok(CheckCacheResponse {
        summary,
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

    #[test]
    fn cache_status_nonexistent_file() {
        let cache = Arc::new(CachedStore::new(
            PathBuf::from("/tmp/optopsy_test_cache_nonexistent"),
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
            PathBuf::from("/tmp/optopsy_test_cache_nonexistent"),
            "options".to_string(),
            None,
        ));
        let result = execute(&cache, "spy", "prices").unwrap();
        assert!(result.file_path.contains("SPY.parquet"));
        assert!(result.summary.contains("SPY"));
    }
}
