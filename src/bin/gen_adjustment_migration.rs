//! One-time binary: reads splits/dividends TXT files and generates a SQL migration.
//!
//! Usage:
//!   cargo run --release --bin gen-adjustment-migration -- \
//!     --splits "/Volumes/Lexar EQ790/Historic Market Data/stocks/stock_splits" \
//!     --dividends "/Volumes/Lexar EQ790/Historic Market Data/stocks/stock_dividends" \
//!     --output migrations/V2__seed_splits_dividends.sql

use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    let mut splits_dir: Option<PathBuf> = None;
    let mut dividends_dir: Option<PathBuf> = None;
    let mut output_path = PathBuf::from("migrations/V2__seed_splits_dividends.sql");

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--splits" => {
                i += 1;
                splits_dir = Some(PathBuf::from(
                    args.get(i).expect("--splits requires a path"),
                ));
            }
            "--dividends" => {
                i += 1;
                dividends_dir = Some(PathBuf::from(
                    args.get(i).expect("--dividends requires a path"),
                ));
            }
            "--output" => {
                i += 1;
                output_path = PathBuf::from(args.get(i).expect("--output requires a path"));
            }
            "--help" | "-h" => {
                println!("Usage: gen-adjustment-migration --splits <DIR> --dividends <DIR> [--output <PATH>]");
                return Ok(());
            }
            _ => {}
        }
        i += 1;
    }

    let splits_dir = splits_dir.context("--splits is required")?;
    let dividends_dir = dividends_dir.context("--dividends is required")?;

    let mut out = File::create(&output_path)
        .with_context(|| format!("Cannot create: {}", output_path.display()))?;

    // Write DDL
    writeln!(out, "-- Auto-generated: splits and dividends seed data.")?;
    writeln!(out)?;
    writeln!(out, "CREATE TABLE IF NOT EXISTS splits (")?;
    writeln!(out, "    symbol TEXT NOT NULL,")?;
    writeln!(out, "    date   TEXT NOT NULL,")?;
    writeln!(out, "    ratio  REAL NOT NULL,")?;
    writeln!(out, "    PRIMARY KEY (symbol, date)")?;
    writeln!(out, ");")?;
    writeln!(out)?;
    writeln!(out, "CREATE TABLE IF NOT EXISTS dividends (")?;
    writeln!(out, "    symbol TEXT NOT NULL,")?;
    writeln!(out, "    date   TEXT NOT NULL,")?;
    writeln!(out, "    amount REAL NOT NULL,")?;
    writeln!(out, "    PRIMARY KEY (symbol, date)")?;
    writeln!(out, ");")?;
    writeln!(out)?;
    writeln!(
        out,
        "CREATE INDEX IF NOT EXISTS idx_splits_symbol ON splits(symbol);"
    )?;
    writeln!(
        out,
        "CREATE INDEX IF NOT EXISTS idx_dividends_symbol ON dividends(symbol);"
    )?;
    writeln!(out)?;

    // Write splits
    let splits_count = write_splits(&splits_dir, &mut out)?;
    println!("Splits: {splits_count} rows from {}", splits_dir.display());

    // Write dividends
    let divs_count = write_dividends(&dividends_dir, &mut out)?;
    println!(
        "Dividends: {divs_count} rows from {}",
        dividends_dir.display()
    );

    println!("Written to: {}", output_path.display());
    Ok(())
}

/// Read all split files and write batched INSERT statements.
fn write_splits(dir: &Path, out: &mut File) -> Result<usize> {
    let mut entries: Vec<(String, String, f64)> = Vec::new();

    let mut files: Vec<_> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension().is_some_and(|ext| ext == "txt")
                && !p
                    .file_name()
                    .unwrap_or_default()
                    .to_str()
                    .unwrap_or("")
                    .starts_with('_')
        })
        .collect();
    files.sort();

    for path in &files {
        let symbol = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_uppercase();
        let reader = BufReader::new(File::open(path)?);
        for line in reader.lines() {
            let line = match line {
                Ok(l) if !l.trim().is_empty() => l,
                _ => continue,
            };
            let parts: Vec<&str> = line.trim().split(',').collect();
            if parts.len() >= 2 {
                let date = parts[0].trim().to_string();
                let ratio: f64 = parts[1].trim().parse().unwrap_or(0.0);
                if ratio > 0.0 && date.len() == 10 {
                    entries.push((symbol.clone(), date, ratio));
                }
            }
        }
    }

    let count = entries.len();
    write_batched_inserts(out, "splits", &["symbol", "date", "ratio"], &entries, |e| {
        format!("('{}','{}',{})", e.0, e.1, e.2)
    })?;
    Ok(count)
}

/// Read all dividend files and write batched INSERT statements.
fn write_dividends(dir: &Path, out: &mut File) -> Result<usize> {
    let mut entries: Vec<(String, String, f64)> = Vec::new();

    let mut files: Vec<_> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "txt"))
        .collect();
    files.sort();

    for path in &files {
        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        // AAPL_divs.txt -> AAPL
        let symbol = stem.strip_suffix("_divs").unwrap_or(stem).to_uppercase();
        let reader = BufReader::new(File::open(path)?);
        for line in reader.lines() {
            let line = match line {
                Ok(l) if !l.trim().is_empty() => l,
                _ => continue,
            };
            let parts: Vec<&str> = line.trim().split(',').collect();
            if parts.len() >= 2 {
                let date = parts[0].trim().to_string();
                let amount: f64 = parts[1].trim().parse().unwrap_or(0.0);
                if amount > 0.0 && date.len() == 10 {
                    entries.push((symbol.clone(), date, amount));
                }
            }
        }
    }

    let count = entries.len();
    write_batched_inserts(
        out,
        "dividends",
        &["symbol", "date", "amount"],
        &entries,
        |e| format!("('{}','{}',{})", e.0, e.1, e.2),
    )?;
    Ok(count)
}

/// Write INSERT statements in batches of 500 rows.
fn write_batched_inserts<T>(
    out: &mut File,
    table: &str,
    columns: &[&str],
    rows: &[T],
    format_row: impl Fn(&T) -> String,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let cols = columns.join(", ");
    for chunk in rows.chunks(500) {
        writeln!(out, "INSERT OR IGNORE INTO {table} ({cols}) VALUES")?;
        for (i, row) in chunk.iter().enumerate() {
            let comma = if i + 1 < chunk.len() { "," } else { ";" };
            writeln!(out, "{}{comma}", format_row(row))?;
        }
        writeln!(out)?;
    }
    Ok(())
}
