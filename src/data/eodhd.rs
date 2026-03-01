//! EODHD data provider for historical US equity options chains.
//!
//! Ports the Python `EODHDProvider` to Rust.  Key features:
//!
//! - **Bulk download** — fetches up to ~2 years of historical options chain for
//!   a symbol, split by option type and paginated in ~30-day windows to stay
//!   within the 10K-offset API cap.  Supports resumable downloads.
//! - **Rate limiting** — adaptive throttle based on `X-RateLimit-Remaining`
//!   header, with exponential backoff on 429 and 5xx errors.
//! - **Incremental save** — each pagination window is merged into the local
//!   parquet cache so progress is never lost on interruption.

use anyhow::{bail, Context, Result};
use chrono::{Duration, NaiveDate, Utc};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use polars::prelude::*;
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::time::sleep;

// ---------------------------------------------------------------------------
// Constants (mirrored from the Python provider)
// ---------------------------------------------------------------------------

const BASE_URL: &str = "https://eodhd.com/api/mp/unicornbay";
const PAGE_LIMIT: u32 = 1000;
const MAX_OFFSET: u32 = 10_000;
const TIMEOUT_SECS: u64 = 60;
const MAX_RETRIES: u32 = 5;
const MIN_REQUEST_INTERVAL_MS: u64 = 100;
const RATE_LIMIT_SLOW_THRESHOLD: u32 = 50;
pub const API_CALLS_PER_REQUEST: u32 = 10;
const MIN_WINDOW_DAYS: i64 = 1;
const HISTORY_DAYS: i64 = 730; // ~2 years

const FIELDS: &str = "\
    underlying_symbol,type,exp_date,expiration_type,tradetime,strike,\
    bid,ask,last,open,high,low,\
    volume,open_interest,\
    delta,gamma,theta,vega,rho,volatility,\
    midpoint,moneyness,theoretical,dte";

/// API column name → internal column name.
const COLUMN_MAP: &[(&str, &str)] = &[
    ("underlying_symbol", "underlying_symbol"),
    ("type", "option_type"),
    ("exp_date", "expiration"),
    ("expiration_type", "expiration_type"),
    ("tradetime", "quote_date"),
    ("strike", "strike"),
    ("bid", "bid"),
    ("ask", "ask"),
    ("last", "last"),
    ("open", "open"),
    ("high", "high"),
    ("low", "low"),
    ("volume", "volume"),
    ("open_interest", "open_interest"),
    ("delta", "delta"),
    ("gamma", "gamma"),
    ("theta", "theta"),
    ("vega", "vega"),
    ("rho", "rho"),
    ("volatility", "implied_volatility"),
    ("midpoint", "midpoint"),
    ("moneyness", "moneyness"),
    ("theoretical", "theoretical"),
    ("dte", "dte"),
];

const NUMERIC_COLS: &[&str] = &[
    "strike",
    "bid",
    "ask",
    "last",
    "open",
    "high",
    "low",
    "volume",
    "open_interest",
    "delta",
    "gamma",
    "theta",
    "vega",
    "rho",
    "implied_volatility",
    "midpoint",
    "moneyness",
    "theoretical",
    "dte",
];

const DEDUP_COLS: &[&str] = &[
    "quote_date",
    "expiration",
    "strike",
    "option_type",
    "expiration_type",
];

// ---------------------------------------------------------------------------
// API response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ApiResponse {
    meta: Option<ApiMeta>,
    data: Option<serde_json::Value>,
    links: Option<ApiLinks>,
}

#[derive(Debug, Deserialize)]
struct ApiMeta {
    fields: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct ApiLinks {
    next: Option<String>,
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Summary returned after a download completes.
pub struct DownloadSummary {
    pub symbol: String,
    pub new_rows: usize,
    pub total_rows: usize,
    pub was_resumed: bool,
    pub cached_rows: usize,
    pub api_requests: u32,
    pub date_min: Option<String>,
    pub date_max: Option<String>,
    pub errors: Vec<String>,
}

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

pub struct EodhdProvider {
    client: Client,
    cache_dir: PathBuf,
    api_key: String,
    last_request_time: Mutex<Instant>,
    request_count: AtomicU32,
}

impl EodhdProvider {
    /// Create from environment.  Returns `None` if `EODHD_API_KEY` is unset.
    pub fn from_env(cache_dir: &std::path::Path) -> Option<Self> {
        let api_key = std::env::var("EODHD_API_KEY").ok()?;
        if api_key.is_empty() {
            return None;
        }
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(TIMEOUT_SECS))
            .build()
            .ok()?;
        Some(Self {
            client,
            cache_dir: cache_dir.to_path_buf(),
            api_key,
            last_request_time: Mutex::new(Instant::now()),
            request_count: AtomicU32::new(0),
        })
    }

    // -- cache helpers ------------------------------------------------------

    fn cache_path(&self, symbol: &str) -> PathBuf {
        self.cache_dir
            .join("options")
            .join(format!("{symbol}.parquet"))
    }

    fn read_cache(&self, symbol: &str) -> Option<DataFrame> {
        let path = self.cache_path(symbol);
        if !path.exists() {
            return None;
        }
        let path_str = path.to_string_lossy().to_string();
        LazyFrame::scan_parquet(path_str.as_str().into(), ScanArgsParquet::default())
            .ok()?
            .collect()
            .ok()
    }

    fn save_parquet(&self, symbol: &str, df: &mut DataFrame) -> Result<()> {
        let path = self.cache_path(symbol);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create cache dir: {}", parent.display()))?;
        }
        // Write to a temp file then atomically rename to avoid corruption
        // from concurrent writes or interrupted I/O.
        let tmp_path = path.with_extension("parquet.tmp");
        let file = std::fs::File::create(&tmp_path)
            .with_context(|| format!("Failed to create temp file: {}", tmp_path.display()))?;
        ParquetWriter::new(file)
            .finish(df)
            .context("Failed to write parquet")?;
        std::fs::rename(&tmp_path, &path).with_context(|| {
            format!(
                "Failed to rename {} → {}",
                tmp_path.display(),
                path.display()
            )
        })?;
        Ok(())
    }

    /// Merge new rows into the existing cache and save.
    fn merge_and_save(&self, symbol: &str, new_df: DataFrame) -> Result<()> {
        let merged = if let Some(existing) = self.read_cache(symbol) {
            concat(
                [existing.lazy(), new_df.lazy()],
                UnionArgs {
                    rechunk: true,
                    to_supertypes: true,
                    diagonal: true,
                    ..Default::default()
                },
            )?
            .collect()?
        } else {
            new_df
        };

        // Deduplicate
        let available: Vec<String> = DEDUP_COLS
            .iter()
            .filter(|c| merged.schema().contains(c))
            .map(|c| (*c).to_string())
            .collect();
        let mut deduped = if available.is_empty() {
            merged
        } else {
            merged.unique::<String, String>(Some(&available), UniqueKeepStrategy::Last, None)?
        };

        // Sort by quote_date for efficient reads later
        if deduped.schema().contains("quote_date") {
            deduped = deduped
                .lazy()
                .sort(["quote_date"], SortMultipleOptions::default())
                .collect()?;
        }

        self.save_parquet(symbol, &mut deduped)
    }

    // -- HTTP ---------------------------------------------------------------

    /// Rate-limited GET with retry on transient errors and 429 backoff.
    async fn throttled_get(
        &self,
        url: &str,
        params: &[(String, String)],
    ) -> Result<reqwest::Response> {
        for attempt in 0..=MAX_RETRIES {
            // Enforce minimum interval between requests
            {
                let mut last = self.last_request_time.lock().await;
                let elapsed = last.elapsed();
                let min_interval = std::time::Duration::from_millis(MIN_REQUEST_INTERVAL_MS);
                if let Some(remaining) = min_interval.checked_sub(elapsed) {
                    sleep(remaining).await;
                }
                *last = Instant::now();
            }

            let resp = match self.client.get(url).query(params).send().await {
                Ok(r) => r,
                Err(e) => {
                    if attempt == MAX_RETRIES {
                        return Err(e.into());
                    }
                    let wait = 2u64.pow(attempt);
                    tracing::warn!(
                        "EODHD request error, retrying in {wait}s (attempt {}/{}): {e}",
                        attempt + 1,
                        MAX_RETRIES
                    );
                    sleep(std::time::Duration::from_secs(wait)).await;
                    continue;
                }
            };

            self.request_count.fetch_add(1, Ordering::Relaxed);

            let status = resp.status().as_u16();

            // 5xx — exponential backoff
            if status >= 500 {
                if attempt == MAX_RETRIES {
                    return Ok(resp);
                }
                let wait = 2u64.pow(attempt + 1);
                tracing::warn!(
                    "EODHD {status} server error, backing off {wait}s (attempt {}/{})",
                    attempt + 1,
                    MAX_RETRIES
                );
                sleep(std::time::Duration::from_secs(wait)).await;
                continue;
            }

            // 429 — exponential backoff
            if status == 429 {
                if attempt == MAX_RETRIES {
                    return Ok(resp);
                }
                let wait = 2u64.pow(attempt + 1);
                tracing::warn!(
                    "EODHD 429 rate limit, backing off {wait}s (attempt {}/{})",
                    attempt + 1,
                    MAX_RETRIES
                );
                sleep(std::time::Duration::from_secs(wait)).await;
                continue;
            }

            // Adaptive throttle based on remaining rate limit
            if let Some(remaining) = resp.headers().get("X-RateLimit-Remaining") {
                if let Ok(remaining_str) = remaining.to_str() {
                    if let Ok(remaining_int) = remaining_str.parse::<u32>() {
                        if remaining_int < RATE_LIMIT_SLOW_THRESHOLD {
                            tracing::info!(
                                "EODHD rate limit remaining: {remaining_int}, throttling"
                            );
                            sleep(std::time::Duration::from_secs(1)).await;
                        }
                    }
                }
            }

            return Ok(resp);
        }
        bail!("Max retries exceeded")
    }

    /// Return a human-readable error for known EODHD status codes, or None.
    fn check_response(status: u16) -> Option<String> {
        match status {
            401 => Some("EODHD API key is invalid or expired.".into()),
            403 => Some("EODHD API access denied. Check your subscription plan.".into()),
            429 => Some("EODHD rate limit exceeded. Try again later.".into()),
            s if s >= 500 => Some(format!(
                "EODHD server error ({s}). The API may be temporarily unavailable."
            )),
            _ => None,
        }
    }

    // -- pagination ---------------------------------------------------------

    /// Paginate through a single date window using compact mode.
    ///
    /// Returns `(rows, hit_cap, error)`.  `hit_cap` is true when the offset
    /// limit was reached, signalling more data likely exists beyond this window.
    async fn paginate_window(
        &self,
        base_params: &[(String, String)],
    ) -> (Vec<HashMap<String, String>>, bool, Option<String>) {
        let mut rows: Vec<HashMap<String, String>> = Vec::new();
        let mut url = format!("{BASE_URL}/options/eod");
        let mut offset: u32 = 0;
        let mut hit_cap = false;
        let mut use_base_params = true;

        loop {
            let params: Vec<(String, String)> = if use_base_params {
                let mut p: Vec<(String, String)> = base_params.to_vec();
                p.push(("api_token".into(), self.api_key.clone()));
                p.push(("compact".into(), "1".into()));
                p.push(("page[offset]".into(), offset.to_string()));
                p
            } else {
                vec![
                    ("api_token".into(), self.api_key.clone()),
                    ("compact".into(), "1".into()),
                ]
            };

            let resp = match self.throttled_get(&url, &params).await {
                Ok(r) => r,
                Err(e) => return (vec![], false, Some(format!("Request failed: {e}"))),
            };

            let status = resp.status().as_u16();

            if let Some(error) = Self::check_response(status) {
                return (vec![], false, Some(error));
            }

            // 422 — API rejects large offsets, treat as hitting the cap
            if status == 422 {
                hit_cap = true;
                break;
            }

            if !resp.status().is_success() {
                return (vec![], false, Some(format!("Unexpected status: {status}")));
            }

            let body: ApiResponse = match resp.json().await {
                Ok(b) => b,
                Err(e) => return (vec![], false, Some(format!("JSON parse error: {e}"))),
            };

            let fields = body
                .meta
                .as_ref()
                .and_then(|m| m.fields.as_ref())
                .cloned()
                .unwrap_or_default();

            let Some(data) = body.data else {
                break;
            };

            // Parse rows depending on format
            let page_rows = if fields.is_empty() {
                // Standard format: data is array of objects
                parse_standard_rows(&data)
            } else {
                // Compact format: data is array of arrays
                parse_compact_rows(&fields, &data)
            };

            if page_rows.is_empty() {
                break;
            }

            rows.extend(page_rows);

            offset += PAGE_LIMIT;
            let next_url = body.links.as_ref().and_then(|l| l.next.clone());

            match next_url {
                Some(next) if offset < MAX_OFFSET => {
                    url = next;
                    use_base_params = false;
                }
                Some(_) => {
                    // Have a next link but hit offset cap
                    hit_cap = true;
                    break;
                }
                None => break,
            }
        }

        (rows, hit_cap, None)
    }

    // -- window management --------------------------------------------------

    /// Generate (`from_date`, `to_date`) ~30-day windows, newest first.
    fn monthly_windows(start: NaiveDate, end: NaiveDate) -> Vec<(NaiveDate, NaiveDate)> {
        let mut windows = Vec::new();
        let mut cur = end;
        while cur > start {
            let q_start = (cur - Duration::days(30)).max(start);
            windows.push((q_start, cur));
            cur = q_start - Duration::days(1);
        }
        windows
    }

    /// Fetch a single date window, subdividing if the offset cap is hit.
    ///
    /// Returns `(total_rows_fetched_so_far, error)`.
    /// Uses `Box::pin` for the recursive calls to avoid infinite future sizes.
    fn fetch_window_recursive<'a>(
        &'a self,
        symbol: &'a str,
        option_type: &'a str,
        win_from: NaiveDate,
        win_to: NaiveDate,
        mut rows_fetched: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = (usize, Option<String>)> + Send + 'a>>
    {
        Box::pin(async move {
            let span_days = (win_to - win_from).num_days();

            tracing::info!(
                "Fetching {symbol} {option_type} options: {win_from} to {win_to} \
                 ({span_days} days) — {rows_fetched} total rows so far"
            );

            let from_str = win_from.format("%Y-%m-%d").to_string();
            let to_str = win_to.format("%Y-%m-%d").to_string();

            let base_params: Vec<(String, String)> = vec![
                ("filter[underlying_symbol]".into(), symbol.to_string()),
                ("filter[type]".into(), option_type.to_string()),
                ("filter[tradetime_from]".into(), from_str),
                ("filter[tradetime_to]".into(), to_str),
                ("fields[options-eod]".into(), FIELDS.to_string()),
                ("page[limit]".into(), PAGE_LIMIT.to_string()),
                ("sort".into(), "exp_date".to_string()),
            ];

            let (rows, hit_cap, error) = self.paginate_window(&base_params).await;

            let window_rows = rows.len();
            if !rows.is_empty() {
                match normalize_rows(&rows) {
                    Ok(df) => {
                        if let Err(e) = self.merge_and_save(symbol, df) {
                            tracing::warn!("Failed to save window data: {e}");
                        }
                    }
                    Err(e) => tracing::warn!("Failed to normalize window data: {e}"),
                }
                rows_fetched += window_rows;
            }

            if let Some(ref err_msg) = error {
                tracing::warn!("Error {win_from}–{win_to} ({option_type}): {err_msg} — skipping");
                return (rows_fetched, error);
            }

            if hit_cap && span_days > MIN_WINDOW_DAYS {
                // Undo partial count — subdivision will re-fetch this range
                rows_fetched -= window_rows;

                tracing::warn!(
                    "Offset cap hit for {symbol} {option_type} ({win_from} to {win_to}), \
                     subdividing into smaller windows"
                );

                let mid = win_from + Duration::days(span_days / 2);

                // First half
                let (fetched, first_err) = self
                    .fetch_window_recursive(symbol, option_type, win_from, mid, rows_fetched)
                    .await;
                rows_fetched = fetched;

                // Second half
                let (fetched, second_err) = self
                    .fetch_window_recursive(
                        symbol,
                        option_type,
                        mid + Duration::days(1),
                        win_to,
                        rows_fetched,
                    )
                    .await;
                rows_fetched = fetched;

                // Propagate the first error encountered
                return (rows_fetched, first_err.or(second_err));
            }

            (rows_fetched, None)
        })
    }

    /// Fetch all rows for a single option type using date windows.
    async fn fetch_all_for_type(
        &self,
        symbol: &str,
        option_type: &str,
        resume_from: Option<NaiveDate>,
        pb: &ProgressBar,
    ) -> (usize, Option<String>) {
        let today = Utc::now().date_naive();
        let start = resume_from.unwrap_or_else(|| today - Duration::days(HISTORY_DAYS));
        let end = today;

        if start >= end {
            pb.finish_with_message("up to date");
            return (0, None);
        }

        let windows = Self::monthly_windows(start, end);
        pb.set_length(windows.len() as u64);
        let mut rows_fetched: usize = 0;
        let mut last_error: Option<String> = None;

        for (win_from, win_to) in &windows {
            pb.set_message(format!("{win_from} → {win_to}"));

            let (fetched, error) = self
                .fetch_window_recursive(symbol, option_type, *win_from, *win_to, rows_fetched)
                .await;
            rows_fetched = fetched;
            if error.is_some() {
                last_error = error;
            }
            pb.inc(1);
        }

        pb.finish_with_message(format!("{rows_fetched} rows"));
        (rows_fetched, last_error)
    }

    // -- public API ---------------------------------------------------------

    /// Download up to ~2 years of historical options data for a symbol from EODHD.
    ///
    /// Splits by option type (call/put), uses ~30-day windows with recursive
    /// subdivision, and saves incrementally to the local parquet cache.
    /// Supports resume: only fetches data newer than the latest cached date.
    pub async fn download_options(&self, symbol: &str) -> Result<DownloadSummary> {
        let symbol = symbol.to_uppercase();
        self.request_count.store(0, Ordering::Relaxed);

        // Check for existing cached data to enable resume
        let cached_df = self.read_cache(&symbol);
        let cached_rows = cached_df.as_ref().map_or(0, DataFrame::height);
        let is_resume = cached_rows > 0;

        let mp = MultiProgress::new();
        let bar_style = ProgressStyle::default_bar()
            .template("  {prefix:.bold} [{bar:30.cyan/dim}] {pos}/{len} windows  {msg}")
            .expect("valid template")
            .progress_chars("=> ");

        let mut errors: Vec<String> = Vec::new();
        let mut new_rows_total: usize = 0;

        for option_type in &["call", "put"] {
            // Determine resume point from cache
            let resume_from = if let Some(ref cdf) = cached_df {
                find_resume_date(cdf, option_type)
            } else {
                None
            };

            if let Some(ref date) = resume_from {
                tracing::info!(
                    "Resuming {symbol} {option_type} options from {date} \
                     ({cached_rows} cached rows)"
                );
            }

            let pb = mp.add(ProgressBar::new(0));
            pb.set_style(bar_style.clone());
            pb.set_prefix(format!("{symbol} {option_type}s"));

            let (new_rows, error) = self
                .fetch_all_for_type(&symbol, option_type, resume_from, &pb)
                .await;

            if let Some(err) = error {
                pb.abandon_with_message(format!("error: {err}"));
                errors.push(format!("{option_type}: {err}"));
                tracing::warn!(
                    "Error fetching {symbol} {option_type} options \
                     (saved {new_rows} rows before error): {err}"
                );
            } else {
                tracing::info!("Downloaded {new_rows} new {option_type} rows for {symbol}");
            }

            new_rows_total += new_rows;
        }

        // Re-read the cache to build summary
        let final_df = self.read_cache(&symbol);

        let (total_rows, date_min, date_max) = match &final_df {
            Some(df) if df.height() > 0 => {
                let rows = df.height();
                let (dmin, dmax) = extract_date_range(df);
                (rows, dmin, dmax)
            }
            _ => {
                if !errors.is_empty() {
                    bail!(
                        "Download failed for {symbol}: {}. No data was saved.",
                        errors.join("; ")
                    );
                }
                bail!("No options data found for {symbol} on EODHD.");
            }
        };

        let api_requests = self.request_count.load(Ordering::Relaxed);

        Ok(DownloadSummary {
            symbol,
            new_rows: new_rows_total,
            total_rows,
            was_resumed: is_resume,
            cached_rows,
            api_requests,
            date_min,
            date_max,
            errors,
        })
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Parse compact API rows (array of arrays + field names) into row dicts.
fn parse_compact_rows(fields: &[String], data: &serde_json::Value) -> Vec<HashMap<String, String>> {
    let Some(arr) = data.as_array() else {
        return vec![];
    };
    arr.iter()
        .filter_map(|row| {
            let vals = row.as_array()?;
            let mut map = HashMap::new();
            for (i, field) in fields.iter().enumerate() {
                if let Some(val) = vals.get(i) {
                    let s = match val {
                        serde_json::Value::Null => continue,
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Number(n) => n.to_string(),
                        serde_json::Value::Bool(b) => b.to_string(),
                        other => other.to_string(),
                    };
                    map.insert(field.clone(), s);
                }
            }
            Some(map)
        })
        .collect()
}

/// Parse standard API rows (array of objects with `attributes` key).
fn parse_standard_rows(data: &serde_json::Value) -> Vec<HashMap<String, String>> {
    let Some(arr) = data.as_array() else {
        return vec![];
    };
    arr.iter()
        .filter_map(|row| {
            let obj = row
                .as_object()
                .and_then(|o| o.get("attributes"))
                .and_then(|a| a.as_object())
                .or_else(|| row.as_object())?;
            let mut map = HashMap::new();
            for (k, v) in obj {
                let s = match v {
                    serde_json::Value::Null => continue,
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    other => other.to_string(),
                };
                map.insert(k.clone(), s);
            }
            Some(map)
        })
        .collect()
}

/// Convert raw API rows into a normalized Polars `DataFrame`.
///
/// Applies column renames, date parsing, and numeric coercion.
fn normalize_rows(rows: &[HashMap<String, String>]) -> Result<DataFrame> {
    if rows.is_empty() {
        return Ok(DataFrame::empty());
    }

    let column_map: HashMap<&str, &str> = COLUMN_MAP.iter().copied().collect();

    // Collect all API field names actually present in the data
    let mut api_fields: Vec<String> = Vec::new();
    for row in rows {
        for key in row.keys() {
            if !api_fields.contains(key) {
                api_fields.push(key.clone());
            }
        }
    }

    // Build string columns, using the internal name.
    // Normalize option_type to lowercase ("call"/"put") during construction.
    let n = rows.len();
    let columns: Vec<Column> = api_fields
        .iter()
        .map(|api_name| {
            let fallback = api_name.as_str();
            let internal_name = *column_map.get(api_name.as_str()).unwrap_or(&fallback);
            if internal_name == "option_type" {
                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|row| row.get(api_name).map(|s| s.to_lowercase()))
                    .collect();
                Column::new(internal_name.into(), values)
            } else {
                let values: Vec<Option<&str>> = rows
                    .iter()
                    .map(|row| row.get(api_name).map(String::as_str))
                    .collect();
                Column::new(internal_name.into(), values)
            }
        })
        .collect();

    let df = DataFrame::new(n, columns).context("Failed to build DataFrame from API rows")?;

    // Cast date columns from string → Date, numeric columns → Float64
    let schema = df.schema().clone();
    let mut lf = df.lazy();

    if schema.contains("expiration") {
        lf = lf.with_column(col("expiration").cast(DataType::Date).alias("expiration"));
    }
    if schema.contains("quote_date") {
        lf = lf.with_column(col("quote_date").cast(DataType::Date).alias("quote_date"));
    }

    let numeric_exprs: Vec<Expr> = NUMERIC_COLS
        .iter()
        .filter(|c| schema.contains(c))
        .map(|c| col(*c).cast(DataType::Float64).alias(*c))
        .collect();
    if !numeric_exprs.is_empty() {
        lf = lf.with_columns(numeric_exprs);
    }

    lf.collect()
        .context("Failed to normalize DataFrame columns")
}

/// Find the latest `quote_date` for a given `option_type` to determine resume point.
fn find_resume_date(df: &DataFrame, option_type: &str) -> Option<NaiveDate> {
    // option_type column may contain "Call"/"Put" or "c"/"p" — match by first char
    let prefix = &option_type[..1]; // "c" or "p"

    let ot_col = df.column("option_type").ok()?;
    let ca = ot_col.str().ok()?;
    let mask: BooleanChunked = ca
        .into_iter()
        .map(|opt_val| opt_val.is_some_and(|v| v.to_lowercase().starts_with(prefix)))
        .collect();
    let filtered = df.filter(&mask).ok()?;

    if filtered.height() == 0 {
        return None;
    }

    let qd_col = filtered.column("quote_date").ok()?;
    let max_scalar = qd_col.max_reduce().ok()?;

    match max_scalar.value() {
        AnyValue::Date(days) => {
            let date = NaiveDate::from_num_days_from_ce_opt(*days + 719_163)?;
            // Resume from the day after the latest cached date
            Some(date + Duration::days(1))
        }
        _ => None,
    }
}

/// Extract the min and max `quote_date` as formatted strings.
fn extract_date_range(df: &DataFrame) -> (Option<String>, Option<String>) {
    let Some(col) = df.column("quote_date").ok() else {
        return (None, None);
    };

    let format_scalar = |s: Scalar| -> Option<String> {
        match s.value() {
            AnyValue::Date(days) => NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                .map(|d| d.format("%Y-%m-%d").to_string()),
            AnyValue::Null => None,
            other => Some(format!("{other}")),
        }
    };

    let min = col.min_reduce().ok().and_then(format_scalar);
    let max = col.max_reduce().ok().and_then(format_scalar);
    (min, max)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monthly_windows_generates_correct_ranges() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 3, 1).unwrap();
        let windows = EodhdProvider::monthly_windows(start, end);

        // Should be newest-first
        assert!(!windows.is_empty());
        assert_eq!(windows[0].1, end);

        // All windows should be within range
        for (from, to) in &windows {
            assert!(*from >= start);
            assert!(*to <= end);
            assert!(from <= to);
        }

        // Windows should cover the full range
        let last = windows.last().unwrap();
        assert_eq!(last.0, start);
    }

    #[test]
    fn parse_compact_rows_basic() {
        let fields = vec!["strike".to_string(), "bid".to_string()];
        let data = serde_json::json!([["100.0", "1.50"], ["105.0", "2.00"]]);
        let rows = parse_compact_rows(&fields, &data);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["strike"], "100.0");
        assert_eq!(rows[0]["bid"], "1.50");
        assert_eq!(rows[1]["strike"], "105.0");
    }

    #[test]
    fn parse_standard_rows_basic() {
        let data = serde_json::json!([
            {"attributes": {"strike": "100.0", "bid": "1.50"}},
            {"attributes": {"strike": "105.0", "bid": "2.00"}}
        ]);
        let rows = parse_standard_rows(&data);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["strike"], "100.0");
    }

    #[test]
    fn normalize_rows_applies_column_map() {
        let rows = vec![{
            let mut m = HashMap::new();
            m.insert("underlying_symbol".to_string(), "SPY".to_string());
            m.insert("type".to_string(), "Call".to_string());
            m.insert("exp_date".to_string(), "2024-03-15".to_string());
            m.insert("tradetime".to_string(), "2024-01-15".to_string());
            m.insert("strike".to_string(), "500.0".to_string());
            m.insert("bid".to_string(), "5.20".to_string());
            m.insert("ask".to_string(), "5.40".to_string());
            m.insert("delta".to_string(), "0.45".to_string());
            m
        }];
        let df = normalize_rows(&rows).unwrap();

        // Column renames applied
        assert!(df.schema().contains("option_type"));
        assert!(df.schema().contains("expiration"));
        assert!(df.schema().contains("quote_date"));
        assert!(!df.schema().contains("type"));
        assert!(!df.schema().contains("exp_date"));
        assert!(!df.schema().contains("tradetime"));

        // Numeric columns cast to f64
        assert_eq!(*df.column("strike").unwrap().dtype(), DataType::Float64);
        assert_eq!(*df.column("delta").unwrap().dtype(), DataType::Float64);

        // Date columns cast to Date
        assert_eq!(*df.column("expiration").unwrap().dtype(), DataType::Date);
        assert_eq!(*df.column("quote_date").unwrap().dtype(), DataType::Date);

        // option_type normalized to lowercase
        let ot = df.column("option_type").unwrap();
        assert_eq!(ot.str().unwrap().get(0).unwrap(), "call");
    }
}
