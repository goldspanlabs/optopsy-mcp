//! Search cached symbols across all data categories.
//!
//! Without a query, returns category-level counts so the agent knows what data
//! exists. With a query, performs a case-insensitive prefix/substring search
//! and returns matching symbols with their category.

use anyhow::Result;
use std::sync::Arc;

use crate::data::cache::CachedStore;

use super::ai_format;
use super::response_types::ListSymbolsResponse;

/// All data categories to scan.
const CATEGORIES: &[&str] = &["options", "etf", "stocks", "futures", "indices"];

/// Maximum number of search results to return.
const MAX_RESULTS: usize = 50;

/// Scan the cache and return a summary or search results.
///
/// - `query = None`: returns category counts only (no symbol lists)
/// - `query = Some(q)`: returns symbols matching the query, grouped by category
pub fn execute(cache: &Arc<CachedStore>, query: Option<&str>) -> Result<ListSymbolsResponse> {
    let mut categories = Vec::new();
    let mut total = 0usize;

    let q = query.map(|s| s.trim().to_uppercase());
    let is_search = q.as_ref().is_some_and(|s| !s.is_empty());

    for &cat in CATEGORIES {
        let symbols = cache.list_symbols_for_category(cat)?;
        if symbols.is_empty() {
            continue;
        }

        let count = symbols.len();
        total += count;

        if is_search {
            let q_upper = q.as_ref().unwrap();
            let matches: Vec<String> = symbols
                .into_iter()
                .filter(|s| {
                    s.to_uppercase().starts_with(q_upper) || s.to_uppercase().contains(q_upper)
                })
                .collect();
            if !matches.is_empty() {
                categories.push(super::response_types::SymbolCategory {
                    category: cat.to_string(),
                    count,
                    symbols: matches,
                });
            }
        } else {
            // Summary mode: counts only, no symbol lists
            categories.push(super::response_types::SymbolCategory {
                category: cat.to_string(),
                count,
                symbols: vec![],
            });
        }
    }

    // Truncate search results to MAX_RESULTS across all categories
    if is_search {
        let mut remaining = MAX_RESULTS;
        for cat in &mut categories {
            if cat.symbols.len() > remaining {
                cat.symbols.truncate(remaining);
            }
            remaining = remaining.saturating_sub(cat.symbols.len());
        }
    }

    let total_matches = if is_search {
        categories.iter().map(|c| c.symbols.len()).sum()
    } else {
        total
    };

    Ok(ai_format::format_list_symbols(
        total,
        total_matches,
        categories,
        query,
    ))
}
