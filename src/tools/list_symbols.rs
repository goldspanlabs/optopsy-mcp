//! List all cached symbols grouped by data category.
//!
//! Scans each category directory (options, etf, stocks, futures, indices) under
//! the cache root and returns the available `.parquet` file stems as symbol names.

use anyhow::Result;
use std::sync::Arc;

use crate::data::cache::CachedStore;

use super::ai_format;
use super::response_types::ListSymbolsResponse;

/// All data categories to scan.
const CATEGORIES: &[&str] = &["options", "etf", "stocks", "futures", "indices"];

/// Scan the cache directory and return all symbols grouped by category.
pub fn execute(cache: &Arc<CachedStore>) -> Result<ListSymbolsResponse> {
    let mut categories = Vec::new();
    let mut total = 0usize;

    for &cat in CATEGORIES {
        let symbols = cache.list_symbols_for_category(cat)?;
        if !symbols.is_empty() {
            total += symbols.len();
            categories.push(super::response_types::SymbolCategory {
                category: cat.to_string(),
                count: symbols.len(),
                symbols,
            });
        }
    }

    Ok(ai_format::format_list_symbols(total, categories))
}
