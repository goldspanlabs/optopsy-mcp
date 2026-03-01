use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use polars::prelude::*;
use s3::creds::Credentials;
use s3::{Bucket, Region};
use std::path::PathBuf;

use super::parquet::ParquetStore;
use super::DataStore;

pub struct CachedStore {
    cache_dir: PathBuf,
    bucket: Option<Box<Bucket>>,
    category: String,
}

impl CachedStore {
    /// Create a new `CachedStore`.
    ///
    /// - `cache_dir`: local directory for cached parquet files (e.g. `~/.optopsy/cache`)
    /// - `category`: subdirectory name (e.g. `"options"`)
    /// - `bucket`: optional S3 bucket for remote fetch-on-miss
    pub fn new(cache_dir: PathBuf, category: String, bucket: Option<Box<Bucket>>) -> Self {
        Self {
            cache_dir,
            bucket,
            category,
        }
    }

    /// Build from environment variables.
    ///
    /// | Env Var | Default | Purpose |
    /// |---------|---------|---------|
    /// | `DATA_ROOT` | `~/.optopsy/cache` | Local cache directory |
    /// | `S3_BUCKET` | (none) | Bucket name — if unset, S3 disabled |
    /// | `S3_ENDPOINT` | (none) | S3-compatible endpoint URL |
    /// | `AWS_ACCESS_KEY_ID` | (none) | S3 credentials |
    /// | `AWS_SECRET_ACCESS_KEY` | (none) | S3 credentials |
    pub fn from_env() -> Result<Self> {
        let cache_dir = match std::env::var("DATA_ROOT") {
            Ok(val) => PathBuf::from(val),
            Err(_) => dirs_default_cache(),
        };

        let bucket = match (std::env::var("S3_BUCKET"), std::env::var("S3_ENDPOINT")) {
            (Ok(bucket_name), Ok(endpoint)) => {
                let region = Region::Custom {
                    region: "auto".to_string(),
                    endpoint,
                };
                let credentials = Credentials::from_env_specific(
                    Some("AWS_ACCESS_KEY_ID"),
                    Some("AWS_SECRET_ACCESS_KEY"),
                    None,
                    None,
                )
                .context("Failed to load S3 credentials from environment")?;

                let bucket = Bucket::new(&bucket_name, region, credentials)
                    .context("Failed to create S3 bucket")?;
                Some(bucket)
            }
            _ => None,
        };

        Ok(Self::new(cache_dir, "options".to_string(), bucket))
    }

    /// Resolve the local path for a given symbol.
    fn local_path(&self, symbol: &str) -> PathBuf {
        self.cache_dir
            .join(&self.category)
            .join(format!("{symbol}.parquet"))
    }

    /// S3 object key for a given symbol.
    fn s3_key(&self, symbol: &str) -> String {
        format!("{}/{symbol}.parquet", self.category)
    }

    /// Ensure the file exists locally, fetching from S3 if needed.
    async fn ensure_local(&self, symbol: &str) -> Result<PathBuf> {
        let path = self.local_path(symbol);

        if path.exists() {
            return Ok(path);
        }

        // Try S3 fetch
        if let Some(bucket) = &self.bucket {
            let key = self.s3_key(symbol);
            tracing::info!(%symbol, %key, "Fetching from S3");

            let response = bucket
                .get_object(&key)
                .await
                .with_context(|| format!("S3 GET failed for key: {key}"))?;

            if response.status_code() != 200 {
                bail!(
                    "S3 returned status {} for key: {key}",
                    response.status_code()
                );
            }

            // Ensure parent directory exists
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("Failed to create cache dir: {}", parent.display()))?;
            }

            std::fs::write(&path, response.as_slice())
                .with_context(|| format!("Failed to write cache file: {}", path.display()))?;

            tracing::info!(%symbol, path = %path.display(), "Cached locally");
            return Ok(path);
        }

        bail!(
            "File not found: {} (no S3 configured for remote fetch)",
            path.display()
        );
    }
}

impl DataStore for CachedStore {
    fn load_options(
        &self,
        symbol: &str,
        start_date: Option<NaiveDate>,
        end_date: Option<NaiveDate>,
    ) -> Result<DataFrame> {
        // Block on async ensure_local within the sync trait method
        let path = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.ensure_local(symbol))
        })?;

        let store = ParquetStore::new(&path.to_string_lossy());
        store.load_options(symbol, start_date, end_date)
    }

    fn list_symbols(&self) -> Result<Vec<String>> {
        let category_dir = self.cache_dir.join(&self.category);
        if !category_dir.exists() {
            return Ok(vec![]);
        }

        let mut symbols = Vec::new();
        for entry in std::fs::read_dir(&category_dir)? {
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

    fn date_range(&self, symbol: &str) -> Result<(NaiveDate, NaiveDate)> {
        let path = self.local_path(symbol);
        if !path.exists() {
            bail!("No cached file for symbol: {symbol}");
        }
        let store = ParquetStore::new(&path.to_string_lossy());
        store.date_range(symbol)
    }
}

/// Default cache directory: `~/.optopsy/cache`
fn dirs_default_cache() -> PathBuf {
    dirs_home().join(".optopsy").join("cache")
}

fn dirs_home() -> PathBuf {
    std::env::var("HOME").map_or_else(|_| PathBuf::from("/tmp"), PathBuf::from)
}
