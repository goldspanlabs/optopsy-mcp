//! Convert FirstRate Data options chain zip files to per-symbol parquet files.
#![allow(
    clippy::doc_markdown,
    clippy::too_many_lines,
    clippy::uninlined_format_args
)]
//!
//! Streams one zip at a time to keep memory bounded. For each zip, reads all
//! per-symbol TXT entries, groups rows by symbol, and appends to per-symbol
//! parquet files. Final pass sorts each file by (date, expiration, strike).
//!
//! Usage:
//!   cargo run --release --bin convert-options -- --input ~/Downloads/options --output /path/to/output
//!   cargo run --release --bin convert-options -- --symbols SPY,QQQ

use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Instant;

use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use zip::read::ZipArchive;

fn parse_f64(s: &str) -> f64 {
    s.parse().unwrap_or(f64::NAN)
}

/// Extract symbol from filename like "SPY_2024_q1_option_chain.txt"
fn extract_symbol(filename: &str) -> Option<String> {
    let basename = Path::new(filename).file_name()?.to_str()?;
    let idx = basename.find("_20")?;
    Some(basename[..idx].to_uppercase())
}

fn find_zip_files(input_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut zips: Vec<PathBuf> = fs::read_dir(input_dir)
        .with_context(|| format!("Cannot read directory: {}", input_dir.display()))?
        .filter_map(std::result::Result::ok)
        .map(|e| e.path())
        .filter(|p| {
            p.extension().is_some_and(|ext| ext == "zip")
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.contains("option_chain"))
        })
        .collect();
    zips.sort();
    Ok(zips)
}

/// Process one zip: for each symbol's TXT entry, parse CSV and write a temporary
/// parquet file. Returns the list of (symbol, temp_path) pairs written.
fn process_zip(
    zip_path: &Path,
    symbol_filter: Option<&HashSet<String>>,
    output_dir: &Path,
    zip_idx: usize,
) -> Result<Vec<(String, PathBuf)>> {
    use polars::prelude::*;

    let file =
        File::open(zip_path).with_context(|| format!("Cannot open: {}", zip_path.display()))?;
    let mut archive = ZipArchive::new(BufReader::new(file))
        .with_context(|| format!("Invalid zip: {}", zip_path.display()))?;

    let mut written = Vec::new();

    for i in 0..archive.len() {
        let entry = archive.by_index(i)?;
        let name = entry.name().to_string();

        if !name.ends_with("_option_chain.txt") {
            continue;
        }

        let Some(symbol) = extract_symbol(&name) else {
            continue;
        };

        if let Some(filter) = symbol_filter {
            if !filter.contains(&symbol) {
                continue;
            }
        }

        // Parse CSV lines into column vecs directly (no intermediate struct)
        let mut dates = Vec::new();
        let mut strikes = Vec::new();
        let mut expirations = Vec::new();
        let mut option_types = Vec::new();
        let mut lasts = Vec::new();
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        let mut bid_ivs = Vec::new();
        let mut ask_ivs = Vec::new();
        let mut ois = Vec::new();
        let mut volumes = Vec::new();
        let mut deltas = Vec::new();
        let mut gammas = Vec::new();
        let mut vegas = Vec::new();
        let mut thetas = Vec::new();
        let mut rhos = Vec::new();

        for line in BufReader::new(entry).lines() {
            let line = match line {
                Ok(l) if !l.is_empty() => l,
                _ => continue,
            };
            let p: Vec<&str> = line.split(',').collect();
            if p.len() < 16 {
                continue;
            }
            dates.push(p[0].to_string());
            strikes.push(parse_f64(p[1]));
            expirations.push(p[2].to_string());
            option_types.push(p[3].to_string());
            lasts.push(parse_f64(p[4]));
            bids.push(parse_f64(p[5]));
            asks.push(parse_f64(p[6]));
            bid_ivs.push(parse_f64(p[7]));
            ask_ivs.push(parse_f64(p[8]));
            ois.push(parse_f64(p[9]));
            volumes.push(parse_f64(p[10]));
            deltas.push(parse_f64(p[11]));
            gammas.push(parse_f64(p[12]));
            vegas.push(parse_f64(p[13]));
            thetas.push(parse_f64(p[14]));
            rhos.push(parse_f64(p[15].trim()));
        }

        if dates.is_empty() {
            continue;
        }

        let n = dates.len();

        let parse_dates = |strs: &[String], name: &str| -> Column {
            let refs: Vec<&str> = strs.iter().map(String::as_str).collect();
            let ca = StringChunked::new("tmp".into(), &refs);
            let mut s = ca
                .as_date(Some("%Y-%m-%d"), false)
                .expect("date parse")
                .into_series();
            s.rename(name.into());
            s.into_column()
        };

        let columns = vec![
            parse_dates(&dates, "date"),
            Column::new("strike".into(), &strikes),
            parse_dates(&expirations, "expiration"),
            Column::new("option_type".into(), &option_types),
            Column::new("last".into(), &lasts),
            Column::new("bid".into(), &bids),
            Column::new("ask".into(), &asks),
            Column::new("bid_iv".into(), &bid_ivs),
            Column::new("ask_iv".into(), &ask_ivs),
            Column::new("open_interest".into(), &ois),
            Column::new("volume".into(), &volumes),
            Column::new("delta".into(), &deltas),
            Column::new("gamma".into(), &gammas),
            Column::new("vega".into(), &vegas),
            Column::new("theta".into(), &thetas),
            Column::new("rho".into(), &rhos),
        ];

        let df = DataFrame::new(n, columns)
            .with_context(|| format!("{symbol}: DataFrame build failed"))?;

        // Write to a temp parquet: {output}/.tmp/{SYMBOL}_{zip_idx}.parquet
        let tmp_dir = output_dir.join(".tmp");
        fs::create_dir_all(&tmp_dir)?;
        let tmp_path = tmp_dir.join(format!("{symbol}_{zip_idx}.parquet"));
        let mut out = File::create(&tmp_path)?;
        ParquetWriter::new(&mut out)
            .with_compression(ParquetCompression::Snappy)
            .finish(&mut df.clone())?;

        written.push((symbol, tmp_path));
    }

    Ok(written)
}

/// Merge all temp parquet files for a symbol into a single sorted parquet file.
fn merge_symbol(symbol: &str, tmp_files: &[PathBuf], output_dir: &Path) -> Result<()> {
    use polars::prelude::*;

    // Read and vstack iteratively to avoid deep query plan trees that blow the stack
    let mut combined: Option<DataFrame> = None;
    for path in tmp_files {
        let df =
            LazyFrame::scan_parquet(path.to_str().unwrap().into(), ScanArgsParquet::default())?
                .collect()?;
        if df.height() == 0 {
            continue;
        }
        combined = Some(match combined {
            None => df,
            Some(mut acc) => {
                acc.vstack_mut(&df)?;
                acc
            }
        });
    }

    let Some(mut combined) = combined else {
        return Ok(());
    };

    // Sort by (date, expiration, strike)
    combined = combined
        .lazy()
        .sort(
            ["date", "expiration", "strike"],
            SortMultipleOptions::default(),
        )
        .collect()?;

    let out_path = output_dir.join(format!("{symbol}.parquet"));
    let mut file = File::create(&out_path)?;
    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut combined)?;

    Ok(())
}

fn main() -> Result<()> {
    // Increase rayon thread stack size to avoid overflows on large symbols
    rayon::ThreadPoolBuilder::new()
        .stack_size(8 * 1024 * 1024) // 8 MB per thread
        .build_global()
        .ok();

    let args: Vec<String> = std::env::args().collect();

    let home = std::env::var("HOME").unwrap_or_default();
    let mut input_dir = PathBuf::from(&home).join("Downloads/options");
    let mut output_dir = PathBuf::from("data/options");
    let mut symbol_filter: Option<HashSet<String>> = None;
    let mut merge_only = false;

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
            "--symbols" => {
                i += 1;
                let syms: HashSet<String> = args
                    .get(i)
                    .expect("--symbols requires a list")
                    .split(',')
                    .map(|s| s.trim().to_uppercase())
                    .collect();
                symbol_filter = Some(syms);
            }
            "--merge-only" => {
                merge_only = true;
            }
            "--help" | "-h" => {
                println!("Usage: convert-options [OPTIONS]");
                println!();
                println!("Options:");
                println!("  --input <DIR>        Input directory with zip files (default: ~/Downloads/options)");
                println!("  --output <DIR>       Output directory for parquet files (default: data/options)");
                println!("  --symbols SPY,QQQ    Only convert specific symbols");
                println!("  --merge-only         Skip phase 1, only merge existing .tmp/ files");
                return Ok(());
            }
            _ => {}
        }
        i += 1;
    }

    fs::create_dir_all(&output_dir)
        .with_context(|| format!("Cannot create output dir: {}", output_dir.display()))?;

    println!("Output: {}", output_dir.display());
    if let Some(ref filter) = symbol_filter {
        println!("Symbol filter: {:?}", filter);
    }

    let total_start = Instant::now();
    let mut symbol_temps: HashMap<String, Vec<PathBuf>> = HashMap::new();

    if merge_only {
        // Rebuild symbol_temps map from existing .tmp/ directory
        let tmp_dir = output_dir.join(".tmp");
        anyhow::ensure!(
            tmp_dir.exists(),
            ".tmp/ directory not found in {}",
            output_dir.display()
        );
        println!("\n── Skipping Phase 1 (--merge-only), scanning .tmp/ ──");
        for entry in fs::read_dir(&tmp_dir)? {
            let path = entry?.path();
            if path.extension().is_some_and(|ext| ext == "parquet") {
                if let Some(name) = path.file_stem().and_then(|n| n.to_str()) {
                    // Filename: {SYMBOL}_{zip_idx}.parquet
                    if let Some(idx) = name.rfind('_') {
                        let symbol = name[..idx].to_string();
                        if let Some(ref filter) = symbol_filter {
                            if !filter.contains(&symbol) {
                                continue;
                            }
                        }
                        symbol_temps.entry(symbol).or_default().push(path);
                    }
                }
            }
        }
    } else {
        let zip_files = find_zip_files(&input_dir)?;
        if zip_files.is_empty() {
            anyhow::bail!("No option chain zip files found in {}", input_dir.display());
        }

        println!(
            "Found {} zip files in {}",
            zip_files.len(),
            input_dir.display()
        );

        // Phase 1: Process each zip sequentially, write temp parquet per symbol per zip
        println!("\n── Phase 1: Extracting zips to temp parquet ──");

        let pb = ProgressBar::new(zip_files.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40} {pos}/{len} {msg}")
                .unwrap(),
        );

        for (idx, zip_path) in zip_files.iter().enumerate() {
            let label = zip_path.file_name().and_then(|n| n.to_str()).unwrap_or("?");
            pb.set_message(label.to_string());

            let written = process_zip(zip_path, symbol_filter.as_ref(), &output_dir, idx)?;
            for (symbol, path) in written {
                symbol_temps.entry(symbol).or_default().push(path);
            }

            pb.inc(1);
        }
        pb.finish_with_message("done");
    }

    let num_symbols = symbol_temps.len();
    println!("Found {} symbols", num_symbols);

    // Phase 2: Merge temp files per symbol in parallel
    println!("\n── Phase 2: Merging and sorting ──");
    let pb = ProgressBar::new(num_symbols as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40} {pos}/{len} symbols")
            .unwrap(),
    );

    let mut entries: Vec<(String, Vec<PathBuf>)> = symbol_temps.into_iter().collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    let errors: Mutex<Vec<String>> = Mutex::new(Vec::new());

    entries.par_iter().for_each(|(symbol, tmp_files)| {
        if let Err(e) = merge_symbol(symbol, tmp_files, &output_dir) {
            errors.lock().unwrap().push(format!("{symbol}: {e}"));
        }
        pb.inc(1);
    });
    pb.finish_with_message("done");

    // Clean up temp dir
    let tmp_dir = output_dir.join(".tmp");
    if tmp_dir.exists() {
        let _ = fs::remove_dir_all(&tmp_dir);
    }

    let errors = errors.into_inner().unwrap();
    if !errors.is_empty() {
        println!("\n{} errors:", errors.len());
        for e in &errors {
            println!("  {e}");
        }
    }

    let elapsed = total_start.elapsed();
    let parquet_count = fs::read_dir(&output_dir)?
        .filter_map(std::result::Result::ok)
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
        .count();

    let total_size: u64 = fs::read_dir(&output_dir)?
        .filter_map(std::result::Result::ok)
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum();

    println!("\n── Complete ──");
    println!("Symbols: {parquet_count}");
    println!("Size: {:.1} GB", total_size as f64 / 1e9);
    println!(
        "Time: {:.0}s ({:.1}m)",
        elapsed.as_secs_f64(),
        elapsed.as_secs_f64() / 60.0
    );

    Ok(())
}
