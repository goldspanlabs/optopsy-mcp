//! Merge update files into existing parquet data.
#![allow(
    clippy::doc_markdown,
    clippy::too_many_lines,
    clippy::uninlined_format_args,
    clippy::needless_pass_by_value,
    clippy::items_after_statements,
    clippy::case_sensitive_file_extension_comparisons
)]
//!
//! Reads CSV files from a zipped update archive, appends only rows with dates
//! AFTER the last date in each existing parquet file.
//!
//! Supports two modes:
//! - `ohlcv` (default): OHLCV price data keyed on `datetime`
//! - `options`: Options chain data keyed on `date`
//!
//! Usage:
//!   merge-updates --input update.zip --data data/stocks
//!   merge-updates --input update.zip --data data/options --mode options --suffix _q1_option_chain.txt
//!   merge-updates --input update.zip --data data/stocks --dry-run

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use zip::read::ZipArchive;

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Ohlcv,
    Options,
}

/// Get the last date value from an existing parquet file as a string.
fn get_last_date(parquet_path: &Path, date_col: &str) -> Result<String> {
    use polars::prelude::*;

    let df = LazyFrame::scan_parquet(
        parquet_path.to_str().unwrap().into(),
        ScanArgsParquet::default(),
    )?
    .select([col(date_col)])
    .sort([date_col], SortMultipleOptions::default())
    .tail(1)
    .collect()?;

    let col_data = df.column(date_col)?;

    // Handle both datetime (OHLCV) and date (options) columns
    if let Ok(ca) = col_data.datetime() {
        let ts = ca.phys.get(0).context("empty parquet file")?;
        let ndt = chrono::DateTime::from_timestamp_millis(ts)
            .context("invalid timestamp")?
            .naive_utc();
        Ok(ndt.format("%Y-%m-%d %H:%M:%S").to_string())
    } else if let Ok(ca) = col_data.date() {
        let days = ca.phys.get(0).context("empty parquet file")?;
        let date = chrono::NaiveDate::from_num_days_from_ce_opt(days).context("invalid date")?;
        Ok(date.format("%Y-%m-%d").to_string())
    } else {
        anyhow::bail!("Column '{date_col}' is neither datetime nor date type");
    }
}

/// Extract symbol from filename by stripping the suffix.
fn extract_symbol(filename: &str, suffix: &str) -> Option<String> {
    let basename = Path::new(filename).file_name()?.to_str()?;
    basename.strip_suffix(suffix).map(str::to_uppercase)
}

/// Merge OHLCV update rows into an existing parquet file.
fn merge_ohlcv(
    parquet_path: &Path,
    new_lines: Vec<String>,
    last_dt: &str,
    dry_run: bool,
) -> Result<usize> {
    use polars::prelude::*;

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

/// Merge options update rows into an existing parquet file.
fn merge_options(
    parquet_path: &Path,
    new_lines: Vec<String>,
    last_date: &str,
    dry_run: bool,
) -> Result<usize> {
    use polars::prelude::*;

    // Keep rows where date > last_date (first CSV column)
    let new_rows: Vec<&str> = new_lines
        .iter()
        .filter(|line| line.split(',').next().is_some_and(|dt| dt > last_date))
        .map(String::as_str)
        .collect();

    if new_rows.is_empty() {
        return Ok(0);
    }

    if dry_run {
        return Ok(new_rows.len());
    }

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
    let mut vols = Vec::new();
    let mut deltas = Vec::new();
    let mut gammas = Vec::new();
    let mut vegas = Vec::new();
    let mut thetas = Vec::new();
    let mut rhos = Vec::new();

    fn pf(s: &str) -> f64 {
        s.parse().unwrap_or(f64::NAN)
    }

    for line in &new_rows {
        let p: Vec<&str> = line.split(',').collect();
        if p.len() < 16 {
            continue;
        }
        dates.push(p[0].to_string());
        strikes.push(pf(p[1]));
        expirations.push(p[2].to_string());
        option_types.push(p[3].to_string());
        lasts.push(pf(p[4]));
        bids.push(pf(p[5]));
        asks.push(pf(p[6]));
        bid_ivs.push(pf(p[7]));
        ask_ivs.push(pf(p[8]));
        ois.push(pf(p[9]));
        vols.push(pf(p[10]));
        deltas.push(pf(p[11]));
        gammas.push(pf(p[12]));
        vegas.push(pf(p[13]));
        thetas.push(pf(p[14]));
        rhos.push(pf(p[15].trim()));
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

    let new_columns = vec![
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
        Column::new("volume".into(), &vols),
        Column::new("delta".into(), &deltas),
        Column::new("gamma".into(), &gammas),
        Column::new("vega".into(), &vegas),
        Column::new("theta".into(), &thetas),
        Column::new("rho".into(), &rhos),
    ];

    let new_df = DataFrame::new(n, new_columns)?;

    let mut existing = LazyFrame::scan_parquet(
        parquet_path.to_str().unwrap().into(),
        ScanArgsParquet::default(),
    )?
    .collect()?;

    existing.vstack_mut(&new_df)?;
    let mut combined = existing
        .lazy()
        .sort(
            ["date", "expiration", "strike"],
            SortMultipleOptions::default(),
        )
        .collect()?;

    let mut file = File::create(parquet_path)?;
    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut combined)?;

    Ok(new_rows.len())
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    let mut input: Option<PathBuf> = None;
    let mut data_dir: Option<PathBuf> = None;
    let mut mode = Mode::Ohlcv;
    let mut suffix: Option<String> = None;
    let mut dry_run = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--input" => {
                i += 1;
                input = Some(PathBuf::from(args.get(i).expect("--input requires a path")));
            }
            "--data" => {
                i += 1;
                data_dir = Some(PathBuf::from(args.get(i).expect("--data requires a path")));
            }
            "--mode" => {
                i += 1;
                mode = match args.get(i).expect("--mode requires a value").as_str() {
                    "options" => Mode::Options,
                    _ => Mode::Ohlcv,
                };
            }
            "--suffix" => {
                i += 1;
                suffix = Some(args.get(i).expect("--suffix requires a value").clone());
            }
            "--dry-run" => {
                dry_run = true;
            }
            "--help" | "-h" => {
                println!("Usage: merge-updates --input <ZIP> --data <DIR> [OPTIONS]");
                println!();
                println!("Options:");
                println!("  --input <ZIP>      Update zip file");
                println!("  --data <DIR>       Directory with existing parquet files");
                println!("  --mode <MODE>      ohlcv (default) or options");
                println!("  --suffix <SUFFIX>  Filename suffix to strip for symbol extraction");
                println!("                     (default: auto-detect from first file)");
                println!("  --dry-run          Show what would be done without writing");
                return Ok(());
            }
            _ => {}
        }
        i += 1;
    }

    let input = input.context("--input is required (update zip file)")?;
    let data_dir = data_dir.context("--data is required (existing parquet directory)")?;

    anyhow::ensure!(input.exists(), "Input not found: {}", input.display());
    anyhow::ensure!(
        data_dir.exists(),
        "Data dir not found: {}",
        data_dir.display()
    );

    if dry_run {
        println!("DRY RUN — no files will be modified\n");
    }

    let date_col = if mode == Mode::Options {
        "date"
    } else {
        "datetime"
    };
    let mode_label = if mode == Mode::Options {
        "options"
    } else {
        "ohlcv"
    };

    println!(
        "Merging {} updates from {} into {}",
        mode_label,
        input.display(),
        data_dir.display()
    );

    let file = File::open(&input)?;
    let mut archive = ZipArchive::new(BufReader::new(file))?;

    // Auto-detect suffix from first .txt entry if not specified
    let suffix = suffix.unwrap_or_else(|| {
        for idx in 0..archive.len() {
            if let Ok(entry) = archive.by_index(idx) {
                let name = entry.name().to_string();
                if name.ends_with(".txt") {
                    // Find the pattern: everything after the symbol (first uppercase letters)
                    // e.g. "SPY_month_1min_adjsplitdiv.txt" → "_month_1min_adjsplitdiv.txt"
                    if let Some(pos) = name.find('_') {
                        return name[pos..].to_string();
                    }
                }
            }
        }
        ".txt".to_string()
    });

    println!("Suffix: {suffix}");

    let mut updated = 0usize;
    let mut skipped = 0usize;
    let mut total_new_rows = 0usize;

    for idx in 0..archive.len() {
        let entry = archive.by_index(idx)?;
        let name = entry.name().to_string();
        if !name.ends_with(".txt") {
            continue;
        }

        let Some(symbol) = extract_symbol(&name, &suffix) else {
            continue;
        };

        let parquet_path = data_dir.join(format!("{symbol}.parquet"));
        if !parquet_path.exists() {
            skipped += 1;
            continue;
        }

        let lines: Vec<String> = BufReader::new(entry)
            .lines()
            .map_while(Result::ok)
            .filter(|l| !l.is_empty())
            .collect();

        if lines.is_empty() {
            skipped += 1;
            continue;
        }

        let Ok(last_dt) = get_last_date(&parquet_path, date_col) else {
            skipped += 1;
            continue;
        };

        let result = if mode == Mode::Options {
            merge_options(&parquet_path, lines, &last_dt, dry_run)
        } else {
            merge_ohlcv(&parquet_path, lines, &last_dt, dry_run)
        };

        match result {
            Ok(0) => {
                skipped += 1;
            }
            Ok(n) => {
                updated += 1;
                total_new_rows += n;
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
        "\nTOTAL: {} symbols updated, +{} new rows, {} skipped",
        updated, total_new_rows, skipped
    );
    if dry_run {
        println!("\nRe-run without --dry-run to apply changes.");
    }

    Ok(())
}
