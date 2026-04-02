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
//!   cargo run --release --bin convert-ohlcv -- --input stock_A.zip --output data/stocks
//!   cargo run --release --bin convert-ohlcv -- --input ~/Downloads/futures/ --output data/futures

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

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    let mut input: Option<PathBuf> = None;
    let mut output_dir = PathBuf::from("data");

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--input" => {
                i += 1;
                input = Some(PathBuf::from(args.get(i).expect("--input requires a path")));
            }
            "--output" => {
                i += 1;
                output_dir = PathBuf::from(args.get(i).expect("--output requires a path"));
            }
            "--help" | "-h" => {
                println!("Usage: convert-ohlcv --input <PATH> --output <DIR>");
                println!();
                println!("Options:");
                println!("  --input <PATH>   A zip file, or directory of zips/txt files");
                println!("  --output <DIR>   Output directory for parquet files (default: data)");
                println!();
                println!("Examples:");
                println!("  convert-ohlcv --input ~/Downloads/stock_A.zip --output data/stocks");
                println!("  convert-ohlcv --input ~/Downloads/futures/ --output data/futures");
                return Ok(());
            }
            _ => {}
        }
        i += 1;
    }

    let input = input.context("--input is required (zip file or directory)")?;
    anyhow::ensure!(input.exists(), "Input not found: {}", input.display());

    fs::create_dir_all(&output_dir)
        .with_context(|| format!("Cannot create output dir: {}", output_dir.display()))?;

    let total_start = Instant::now();
    let grand_total;

    if input.is_file() && input.extension().is_some_and(|ext| ext == "zip") {
        // Single zip file
        println!("Converting {} → {}", input.display(), output_dir.display());
        grand_total = convert_zip(&input, &output_dir)?;
    } else if input.is_dir() {
        // Directory: process all zips and txt files
        let mut zips: Vec<PathBuf> = fs::read_dir(&input)?
            .filter_map(std::result::Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "zip"))
            .collect();
        zips.sort();

        let mut txts: Vec<PathBuf> = fs::read_dir(&input)?
            .filter_map(std::result::Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "txt"))
            .collect();
        txts.sort();

        anyhow::ensure!(
            !zips.is_empty() || !txts.is_empty(),
            "No zip or txt files found in {}",
            input.display()
        );

        println!(
            "Input: {} ({} zips, {} txt files)",
            input.display(),
            zips.len(),
            txts.len()
        );
        println!("Output: {}", output_dir.display());

        let mut total = 0usize;

        if !zips.is_empty() {
            let pb = ProgressBar::new(zips.len() as u64);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("[{elapsed_precise}] {bar:40} {pos}/{len} {msg}")
                    .unwrap(),
            );
            for zip_path in &zips {
                let label = zip_path.file_name().and_then(|n| n.to_str()).unwrap_or("?");
                pb.set_message(label.to_string());
                total += convert_zip(zip_path, &output_dir)?;
                pb.inc(1);
            }
            pb.finish_with_message("done");
        }

        if !txts.is_empty() {
            let pb = ProgressBar::new(txts.len() as u64);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("[{elapsed_precise}] {bar:40} {pos}/{len}")
                    .unwrap(),
            );
            for txt_path in &txts {
                convert_txt(txt_path, &output_dir)?;
                pb.inc(1);
                total += 1;
            }
            pb.finish_with_message("done");
        }

        grand_total = total;
    } else {
        anyhow::bail!("Input must be a zip file or directory: {}", input.display());
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
