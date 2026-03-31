//! Convert FirstRate Data OHLCV TXT/zip files to per-symbol parquet files.
#![allow(
    clippy::doc_markdown,
    clippy::too_many_lines,
    clippy::uninlined_format_args,
    clippy::too_many_arguments,
    clippy::case_sensitive_file_extension_comparisons,
    clippy::many_single_char_names,
    clippy::type_complexity
)]
//!
//! Handles ETFs, stocks, futures, and indices. Each category has its own input
//! layout (zips vs flat TXT files) and column schema (with/without volume).
//! Output: `{output_dir}/{category}/{SYMBOL}.parquet` with columns:
//! `datetime, open, high, low, close, volume, adjclose`
//!
//! Usage:
//!   cargo run --release --bin convert-ohlcv -- --input ~/Downloads --output data
//!   cargo run --release --bin convert-ohlcv -- --input ~/Downloads --output data --categories etf,futures

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use zip::read::ZipArchive;

fn parse_f64(s: &str) -> f64 {
    s.parse().unwrap_or(f64::NAN)
}

/// Extract symbol from filename like `SPY_full_1min_adjsplitdiv.txt`
fn extract_symbol(filename: &str) -> String {
    let base = Path::new(filename)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(filename);
    base.split("_full_")
        .next()
        .unwrap_or(base.split('_').next().unwrap_or(base))
        .to_uppercase()
}

/// Parse OHLCV lines into column vecs. Returns (datetime, open, high, low, close, volume).
/// Volume defaults to 0 if the line has fewer than 6 columns.
fn parse_ohlcv_lines(
    reader: impl BufRead,
) -> (
    Vec<String>,
    Vec<f64>,
    Vec<f64>,
    Vec<f64>,
    Vec<f64>,
    Vec<i64>,
) {
    let mut datetimes = Vec::new();
    let mut opens = Vec::new();
    let mut highs = Vec::new();
    let mut lows = Vec::new();
    let mut closes = Vec::new();
    let mut volumes = Vec::new();

    for line in reader.lines() {
        let line = match line {
            Ok(l) if !l.is_empty() => l,
            _ => continue,
        };
        let p: Vec<&str> = line.split(',').collect();
        if p.len() < 5 {
            continue;
        }
        datetimes.push(p[0].to_string());
        opens.push(parse_f64(p[1]));
        highs.push(parse_f64(p[2]));
        lows.push(parse_f64(p[3]));
        closes.push(parse_f64(p[4]));
        volumes.push(if p.len() >= 6 {
            p[5].trim().parse().unwrap_or(0)
        } else {
            0
        });
    }

    (datetimes, opens, highs, lows, closes, volumes)
}

/// Write OHLCV columns to a parquet file. Adds `adjclose` = `close`.
fn write_ohlcv_parquet(
    symbol: &str,
    datetimes: &[String],
    opens: &[f64],
    highs: &[f64],
    lows: &[f64],
    closes: &[f64],
    volumes: &[i64],
    output_path: &Path,
) -> Result<()> {
    use polars::prelude::*;

    let n = datetimes.len();
    if n == 0 {
        return Ok(());
    }

    let dt_refs: Vec<&str> = datetimes.iter().map(String::as_str).collect();
    let dt_ca = StringChunked::new("tmp".into(), &dt_refs);

    // Try datetime with time first, fall back to date-only
    let mut dt_series = dt_ca
        .as_datetime(
            Some("%Y-%m-%d %H:%M:%S"),
            TimeUnit::Milliseconds,
            false,
            false,
            None,
            &StringChunked::from_slice("ambiguous".into(), &["raise"]),
        )
        .unwrap_or_else(|_| {
            dt_ca
                .as_datetime(
                    Some("%Y-%m-%d"),
                    TimeUnit::Milliseconds,
                    false,
                    false,
                    None,
                    &StringChunked::from_slice("ambiguous".into(), &["raise"]),
                )
                .expect("datetime parse")
        })
        .into_series();
    dt_series.rename("datetime".into());

    let columns = vec![
        dt_series.into_column(),
        Column::new("open".into(), opens),
        Column::new("high".into(), highs),
        Column::new("low".into(), lows),
        Column::new("close".into(), &closes),
        Column::new("volume".into(), volumes),
        Column::new("adjclose".into(), closes), // adjclose = close (pre-adjusted data)
    ];

    let mut df =
        DataFrame::new(n, columns).with_context(|| format!("{symbol}: DataFrame build failed"))?;

    let mut file = File::create(output_path)
        .with_context(|| format!("Cannot create: {}", output_path.display()))?;

    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut df)?;

    Ok(())
}

/// Convert all TXT files inside a zip archive to per-symbol parquet files.
fn convert_zip(zip_path: &Path, output_dir: &Path) -> Result<usize> {
    let file =
        File::open(zip_path).with_context(|| format!("Cannot open: {}", zip_path.display()))?;
    let mut archive = ZipArchive::new(BufReader::new(file))
        .with_context(|| format!("Invalid zip: {}", zip_path.display()))?;

    let mut count = 0;

    for i in 0..archive.len() {
        let entry = archive.by_index(i)?;
        let name = entry.name().to_string();
        if !name.ends_with(".txt") {
            continue;
        }

        let symbol = extract_symbol(&name);
        let (dt, o, h, l, c, v) = parse_ohlcv_lines(BufReader::new(entry));
        let out_path = output_dir.join(format!("{symbol}.parquet"));
        write_ohlcv_parquet(&symbol, &dt, &o, &h, &l, &c, &v, &out_path)?;
        count += 1;
    }

    Ok(count)
}

/// Convert a single TXT file to parquet.
fn convert_txt(txt_path: &Path, output_dir: &Path) -> Result<String> {
    let symbol = extract_symbol(
        txt_path
            .file_name()
            .unwrap_or_default()
            .to_str()
            .unwrap_or(""),
    );
    let file = File::open(txt_path)?;
    let (dt, o, h, l, c, v) = parse_ohlcv_lines(BufReader::new(file));
    let out_path = output_dir.join(format!("{symbol}.parquet"));
    write_ohlcv_parquet(&symbol, &dt, &o, &h, &l, &c, &v, &out_path)?;
    Ok(symbol)
}

struct CategoryConfig {
    name: &'static str,
    input_subdir: &'static str,  // relative to input_dir
    output_subdir: &'static str, // relative to output_dir
    mode: InputMode,
}

enum InputMode {
    /// Directory of zip files, each containing per-symbol TXT files
    Zips,
    /// Directory of per-symbol TXT files
    FlatTxt,
}

fn categories() -> Vec<CategoryConfig> {
    vec![
        CategoryConfig {
            name: "etf",
            input_subdir: "etf",
            output_subdir: "etf",
            mode: InputMode::Zips,
        },
        CategoryConfig {
            name: "stocks",
            input_subdir: "stocks",
            output_subdir: "stocks",
            mode: InputMode::Zips,
        },
        CategoryConfig {
            name: "futures",
            input_subdir: "futures/futures_full_1min_contin_adj_ratio",
            output_subdir: "futures",
            mode: InputMode::FlatTxt,
        },
        CategoryConfig {
            name: "indices",
            input_subdir: "index/index_full_1min",
            output_subdir: "indices",
            mode: InputMode::FlatTxt,
        },
    ]
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    let home = std::env::var("HOME").unwrap_or_default();
    let mut input_dir = PathBuf::from(&home).join("Downloads");
    let mut output_dir = PathBuf::from("data");
    let mut category_filter: Option<HashSet<String>> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--input" => {
                i += 1;
                input_dir = PathBuf::from(args.get(i).expect("--input requires a path"));
            }
            "--output" => {
                i += 1;
                output_dir = PathBuf::from(args.get(i).expect("--output requires a path"));
            }
            "--categories" => {
                i += 1;
                let cats: HashSet<String> = args
                    .get(i)
                    .expect("--categories requires a list")
                    .split(',')
                    .map(|s| s.trim().to_lowercase())
                    .collect();
                category_filter = Some(cats);
            }
            "--help" | "-h" => {
                println!("Usage: convert-ohlcv [OPTIONS]");
                println!();
                println!("Options:");
                println!("  --input <DIR>              Input directory (default: ~/Downloads)");
                println!("  --output <DIR>             Output directory (default: data)");
                println!("  --categories etf,futures   Only convert specific categories");
                println!();
                println!("Categories: etf, stocks, futures, indices");
                return Ok(());
            }
            _ => {}
        }
        i += 1;
    }

    let total_start = Instant::now();
    let mut grand_total = 0usize;

    for cat in categories() {
        if let Some(ref filter) = category_filter {
            if !filter.contains(cat.name) {
                continue;
            }
        }

        let cat_input = input_dir.join(cat.input_subdir);
        let cat_output = output_dir.join(cat.output_subdir);

        if !cat_input.exists() {
            println!(
                "[{}] Input not found: {}, skipping",
                cat.name,
                cat_input.display()
            );
            continue;
        }

        fs::create_dir_all(&cat_output)?;

        match cat.mode {
            InputMode::Zips => {
                let mut zips: Vec<PathBuf> = fs::read_dir(&cat_input)?
                    .filter_map(std::result::Result::ok)
                    .map(|e| e.path())
                    .filter(|p| p.extension().is_some_and(|ext| ext == "zip"))
                    .collect();
                zips.sort();

                if zips.is_empty() {
                    println!("[{}] No zip files found, skipping", cat.name);
                    continue;
                }

                println!(
                    "\n[{}] {} zip files → {}",
                    cat.name,
                    zips.len(),
                    cat_output.display()
                );
                let pb = ProgressBar::new(zips.len() as u64);
                pb.set_style(
                    ProgressStyle::default_bar()
                        .template("[{elapsed_precise}] {bar:40} {pos}/{len} {msg}")
                        .unwrap(),
                );

                let mut total = 0;
                for zip_path in &zips {
                    let label = zip_path.file_name().and_then(|n| n.to_str()).unwrap_or("?");
                    pb.set_message(label.to_string());
                    total += convert_zip(zip_path, &cat_output)?;
                    pb.inc(1);
                }
                pb.finish_with_message("done");
                println!("[{}] {} symbols", cat.name, total);
                grand_total += total;
            }
            InputMode::FlatTxt => {
                let mut txts: Vec<PathBuf> = fs::read_dir(&cat_input)?
                    .filter_map(std::result::Result::ok)
                    .map(|e| e.path())
                    .filter(|p| p.extension().is_some_and(|ext| ext == "txt"))
                    .collect();
                txts.sort();

                if txts.is_empty() {
                    println!("[{}] No TXT files found, skipping", cat.name);
                    continue;
                }

                println!(
                    "\n[{}] {} files → {}",
                    cat.name,
                    txts.len(),
                    cat_output.display()
                );
                let pb = ProgressBar::new(txts.len() as u64);
                pb.set_style(
                    ProgressStyle::default_bar()
                        .template("[{elapsed_precise}] {bar:40} {pos}/{len}")
                        .unwrap(),
                );

                for txt_path in &txts {
                    convert_txt(txt_path, &cat_output)?;
                    pb.inc(1);
                }
                pb.finish_with_message("done");
                println!("[{}] {} symbols", cat.name, txts.len());
                grand_total += txts.len();
            }
        }
    }

    let elapsed = total_start.elapsed();
    println!(
        "\n── Complete ── {} symbols, {:.0}s ({:.1}m)",
        grand_total,
        elapsed.as_secs_f64(),
        elapsed.as_secs_f64() / 60.0
    );

    Ok(())
}
