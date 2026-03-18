use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use polars::prelude::*;
use std::path::PathBuf;

use super::parquet::ParquetStore;
use super::DataStore;

pub struct CachedStore {
    cache_dir: PathBuf,
    category: String,
}

impl CachedStore {
    /// Create a new `CachedStore`.
    ///
    /// - `cache_dir`: local directory for cached parquet files (e.g. `~/.optopsy/cache`)
    /// - `category`: subdirectory name (e.g. `"options"`)
    pub fn new(cache_dir: PathBuf, category: String) -> Self {
        Self {
            cache_dir,
            category,
        }
    }

    /// Return the cache directory path.
    pub fn cache_dir(&self) -> PathBuf {
        self.cache_dir.clone()
    }

    /// Build from environment variables.
    ///
    /// | Env Var | Default | Purpose |
    /// |---------|---------|---------|
    /// | `DATA_ROOT` | `~/.optopsy/cache` | Local cache directory |
    pub fn from_env() -> Result<Self> {
        let cache_dir = match std::env::var("DATA_ROOT") {
            Ok(val) => PathBuf::from(val),
            Err(_) => dirs_default_cache(),
        };

        Ok(Self::new(cache_dir, "options".to_string()))
    }

    /// Build the parquet file path for a symbol under a category.
    ///
    /// Validates path segments and returns `{cache_dir}/{category}/{SYMBOL}.parquet`.
    fn build_parquet_path(&self, symbol: &str, category: &str) -> Result<PathBuf> {
        validate_path_segment(category).with_context(|| format!("Invalid category: {category}"))?;
        validate_path_segment(symbol).with_context(|| format!("Invalid symbol: {symbol}"))?;
        Ok(self
            .cache_dir
            .join(category)
            .join(format!("{}.parquet", symbol.to_uppercase())))
    }

    /// Resolve the cache path for a symbol under an arbitrary category.
    pub fn cache_path(&self, symbol: &str, category: &str) -> Result<PathBuf> {
        self.build_parquet_path(symbol, category)
    }

    /// List all symbols available in a specific category directory.
    ///
    /// Validates the category segment before constructing the path.
    pub fn list_symbols_for_category(&self, category: &str) -> Result<Vec<String>> {
        validate_path_segment(category).with_context(|| format!("Invalid category: {category}"))?;
        list_parquet_stems(&self.cache_dir.join(category))
    }

    /// Search OHLCV categories in order (`equities`, `futures`, `indices`) and return
    /// the path of the first existing parquet file for the given symbol.
    pub fn find_ohlcv(&self, symbol: &str) -> Option<PathBuf> {
        for category in &["etf", "stocks", "futures", "indices"] {
            if let Ok(path) = self.build_parquet_path(symbol, category) {
                if path.exists() {
                    return Some(path);
                }
            }
        }
        None
    }

    /// Resolve the local path for a given symbol.
    fn local_path(&self, symbol: &str) -> Result<PathBuf> {
        self.build_parquet_path(symbol, &self.category)
    }

    /// Ensure a file exists locally under the given category, returning an error if not found.
    pub fn ensure_local_for(&self, symbol: &str, category: &str) -> Result<PathBuf> {
        let path = self.build_parquet_path(symbol, category)?;

        if path.exists() {
            tracing::info!(%symbol, path = %path.display(), "Cache hit (local parquet)");
            return Ok(path);
        }

        bail!(
            "No cached data found for {symbol}. Place the Parquet file at {}",
            path.display()
        );
    }
}

impl DataStore for CachedStore {
    async fn load_options(
        &self,
        symbol: &str,
        start_date: Option<NaiveDate>,
        end_date: Option<NaiveDate>,
    ) -> Result<DataFrame> {
        let path = self.ensure_local_for(symbol, &self.category)?;

        let store = ParquetStore::new(&path.to_string_lossy());
        store.load_options(symbol, start_date, end_date).await
    }

    fn list_symbols(&self) -> Result<Vec<String>> {
        list_parquet_stems(&self.cache_dir.join(&self.category))
    }

    fn date_range(&self, symbol: &str) -> Result<(NaiveDate, NaiveDate)> {
        let path = self.local_path(symbol)?;
        if !path.exists() {
            bail!("No cached file for symbol: {symbol}");
        }
        let store = ParquetStore::new(&path.to_string_lossy());
        store.date_range(symbol)
    }
}

/// Scan a directory for `.parquet` files and return their stems (sorted).
///
/// Returns an empty `Vec` if the directory does not exist.
fn list_parquet_stems(dir: &std::path::Path) -> Result<Vec<String>> {
    if !dir.exists() {
        return Ok(vec![]);
    }

    let mut symbols = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "parquet") {
            if let Some(stem) = path.file_stem() {
                symbols.push(stem.to_string_lossy().to_string());
            }
        }
    }
    symbols.sort();
    Ok(symbols)
}

/// Default cache directory: `~/.optopsy/cache`
fn dirs_default_cache() -> PathBuf {
    const TEMPLATE: &str = "~/.optopsy/cache";
    let expanded = shellexpand::tilde(TEMPLATE);
    // If tilde was not expanded (no home directory available), fall back to a tmp-based path
    if expanded.as_ref() == TEMPLATE {
        return std::env::temp_dir().join("optopsy").join("cache");
    }
    PathBuf::from(expanded.as_ref())
}

/// Ensure a path segment (category or symbol) contains only safe characters.
///
/// Rejects empty strings, absolute paths, and segments with directory separators or `..`.
pub(crate) fn validate_path_segment(segment: &str) -> Result<()> {
    if segment.is_empty() {
        bail!("path segment must not be empty");
    }
    // Reject absolute-path-like segments and traversal components
    if std::path::Path::new(segment)
        .components()
        .any(|c| !matches!(c, std::path::Component::Normal(_)))
    {
        bail!("path segment contains illegal characters or components: {segment}");
    }
    // Reject embedded separators (both Unix '/' and Windows '\') on any platform
    if segment.contains('/') || segment.contains('\\') {
        bail!("path segment must not contain path separators: {segment}");
    }
    Ok(())
}
