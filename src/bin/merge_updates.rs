//! Merge monthly OHLCV update files into existing parquet data.
#![allow(
    clippy::doc_markdown,
    clippy::too_many_lines,
    clippy::uninlined_format_args,
    clippy::needless_pass_by_value
)]
//!
//! Reads CSV files from zipped update archives, appends only rows with timestamps
//! AFTER the last timestamp in each existing parquet file.
//!
//! Usage:
//!   cargo run --release --bin merge-updates -- --data data --updates ~/Downloads
//!   cargo run --release --bin merge-updates -- --data data --updates ~/Downloads --dry-run

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use zip::read::ZipArchive;

struct UpdateConfig {
    zip_name_pattern: &'static str,
    category: &'static str,
    suffix: &'static str,
}

fn update_configs() -> Vec<UpdateConfig> {
    vec![
        UpdateConfig {
            zip_name_pattern: "stock_update_month_1min_adjsplitdiv",
            category: "stocks",
            suffix: "_month_1min_adjsplitdiv.txt",
        },
        UpdateConfig {
            zip_name_pattern: "etf_update_month_1min_adjsplitdiv",
            category: "etf",
            suffix: "_month_1min_adjsplitdiv.txt",
        },
        UpdateConfig {
            zip_name_pattern: "index_update_month_1min",
            category: "indices",
            suffix: "_month_1min.txt",
        },
    ]
}

fn find_update_zip(updates_dir: &Path, pattern: &str) -> Option<PathBuf> {
    std::fs::read_dir(updates_dir)
        .ok()?
        .filter_map(std::result::Result::ok)
        .map(|e| e.path())
        .find(|p| {
            p.extension().is_some_and(|ext| ext == "zip")
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(pattern))
        })
}

/// Get the last datetime value from an existing parquet file as a string for comparison.
fn get_last_datetime(parquet_path: &Path) -> Result<String> {
    use polars::prelude::*;

    let df = LazyFrame::scan_parquet(
        parquet_path.to_str().unwrap().into(),
        ScanArgsParquet::default(),
    )?
    .select([col("datetime")])
    .sort(["datetime"], SortMultipleOptions::default())
    .tail(1)
    .collect()?;

    let col = df.column("datetime")?;
    let ca = col.datetime()?;
    let ts = ca.phys.get(0).context("empty parquet file")?;

    // Convert timestamp to string for comparison
    let ndt = chrono::DateTime::from_timestamp_millis(ts)
        .context("invalid timestamp")?
        .naive_utc();
    Ok(ndt.format("%Y-%m-%d %H:%M:%S").to_string())
}

/// Merge update rows into an existing parquet file. Returns count of new rows added.
fn merge_one(
    parquet_path: &Path,
    new_lines: Vec<String>,
    last_dt: &str,
    dry_run: bool,
) -> Result<usize> {
    use polars::prelude::*;

    // Filter lines with datetime > last_dt (string comparison works for ISO format)
    let new_rows: Vec<&str> = new_lines
        .iter()
        .filter(|line| line.split(',').next().is_some_and(|dt| dt > last_dt))
        .map(String::as_str)
        .collect();

    if new_rows.is_empty() {
        return Ok(0);
    }

    if dry_run {
        return Ok(new_rows.len());
    }

    // Parse new rows
    let mut datetimes = Vec::new();
    let mut opens = Vec::new();
    let mut highs = Vec::new();
    let mut lows = Vec::new();
    let mut closes = Vec::new();
    let mut volumes: Vec<i64> = Vec::new();

    for line in &new_rows {
        let p: Vec<&str> = line.split(',').collect();
        if p.len() < 5 {
            continue;
        }
        datetimes.push(p[0].to_string());
        opens.push(p[1].parse().unwrap_or(f64::NAN));
        highs.push(p[2].parse().unwrap_or(f64::NAN));
        lows.push(p[3].parse().unwrap_or(f64::NAN));
        closes.push(p[4].parse().unwrap_or(f64::NAN));
        volumes.push(if p.len() >= 6 {
            p[5].trim().parse().unwrap_or(0)
        } else {
            0
        });
    }

    let n = datetimes.len();
    let dt_refs: Vec<&str> = datetimes.iter().map(String::as_str).collect();
    let dt_ca = StringChunked::new("tmp".into(), &dt_refs);
    let mut dt_series = dt_ca
        .as_datetime(
            Some("%Y-%m-%d %H:%M:%S"),
            TimeUnit::Milliseconds,
            false,
            false,
            None,
            &StringChunked::from_slice("ambiguous".into(), &["raise"]),
        )
        .context("datetime parse")?
        .into_series();
    dt_series.rename("datetime".into());

    let new_columns = vec![
        dt_series.into_column(),
        Column::new("open".into(), &opens),
        Column::new("high".into(), &highs),
        Column::new("low".into(), &lows),
        Column::new("close".into(), &closes),
        Column::new("volume".into(), &volumes),
        Column::new("adjclose".into(), &closes),
    ];

    let new_df = DataFrame::new(n, new_columns)?;

    // Read existing, vstack, sort, write
    let mut existing = LazyFrame::scan_parquet(
        parquet_path.to_str().unwrap().into(),
        ScanArgsParquet::default(),
    )?
    .collect()?;

    existing.vstack_mut(&new_df)?;
    let mut combined = existing
        .lazy()
        .sort(["datetime"], SortMultipleOptions::default())
        .collect()?;

    let mut file = File::create(parquet_path)?;
    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut combined)?;

    Ok(new_rows.len())
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    let home = std::env::var("HOME").unwrap_or_default();
    let mut data_dir = PathBuf::from("data");
    let mut updates_dir = PathBuf::from(&home).join("Downloads");
    let mut dry_run = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--data" => {
                i += 1;
                data_dir = PathBuf::from(args.get(i).expect("--data requires a path"));
            }
            "--updates" => {
                i += 1;
                updates_dir = PathBuf::from(args.get(i).expect("--updates requires a path"));
            }
            "--dry-run" => {
                dry_run = true;
            }
            "--help" | "-h" => {
                println!("Usage: merge-updates [OPTIONS]");
                println!();
                println!("Options:");
                println!(
                    "  --data <DIR>       Data directory with category subdirs (default: data)"
                );
                println!(
                    "  --updates <DIR>    Directory with update zip files (default: ~/Downloads)"
                );
                println!("  --dry-run          Show what would be done without writing");
                return Ok(());
            }
            _ => {}
        }
        i += 1;
    }

    if dry_run {
        println!("DRY RUN — no files will be modified\n");
    }

    let mut total_updated = 0usize;
    let mut total_new_rows = 0usize;
    let mut total_skipped = 0usize;

    for config in update_configs() {
        let Some(zip_path) = find_update_zip(&updates_dir, config.zip_name_pattern) else {
            println!(
                "SKIP {}: no matching zip in {}",
                config.category,
                updates_dir.display()
            );
            continue;
        };

        let cat_dir = data_dir.join(config.category);
        if !cat_dir.exists() {
            println!("SKIP {}: {} not found", config.category, cat_dir.display());
            continue;
        }

        println!(
            "Processing {}/ from {} ...",
            config.category,
            zip_path
                .file_name()
                .unwrap_or_default()
                .to_str()
                .unwrap_or("?")
        );

        let file = File::open(&zip_path)?;
        let mut archive = ZipArchive::new(BufReader::new(file))?;

        let mut updated = 0usize;
        let mut skipped = 0usize;
        let mut cat_new_rows = 0usize;

        let entry_count = archive.len();
        for idx in 0..entry_count {
            let entry = archive.by_index(idx)?;
            let name = entry.name().to_string();
            if !name.ends_with(config.suffix) {
                continue;
            }

            let symbol = name.replace(config.suffix, "");
            let parquet_path = cat_dir.join(format!("{symbol}.parquet"));

            if !parquet_path.exists() {
                skipped += 1;
                continue;
            }

            // Read all lines from the update file
            let lines: Vec<String> = BufReader::new(entry)
                .lines()
                .map_while(Result::ok)
                .filter(|l| !l.is_empty())
                .collect();

            if lines.is_empty() {
                skipped += 1;
                continue;
            }

            let Ok(last_dt) = get_last_datetime(&parquet_path) else {
                skipped += 1;
                continue;
            };

            match merge_one(&parquet_path, lines, &last_dt, dry_run) {
                Ok(0) => {
                    skipped += 1;
                }
                Ok(n) => {
                    updated += 1;
                    cat_new_rows += n;
                    if updated <= 5 {
                        println!("  {symbol}: +{n} rows");
                    }
                }
                Err(e) => {
                    eprintln!("  {symbol}: ERROR {e}");
                    skipped += 1;
                }
            }
        }

        if updated > 5 {
            println!("  ... and {} more", updated - 5);
        }
        println!(
            "  {}: {} updated, {} skipped, +{} new rows\n",
            config.category, updated, skipped, cat_new_rows
        );
        total_updated += updated;
        total_new_rows += cat_new_rows;
        total_skipped += skipped;
    }

    println!(
        "TOTAL: {} symbols updated, +{} new rows, {} skipped",
        total_updated, total_new_rows, total_skipped
    );
    if dry_run {
        println!("\nRe-run without --dry-run to apply changes.");
    }

    Ok(())
}
