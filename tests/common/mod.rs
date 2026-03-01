#![allow(dead_code)]

use chrono::NaiveDate;
use optopsy_mcp::data::parquet::QUOTE_DATETIME_COL;
use optopsy_mcp::engine::types::TargetRange;
use polars::prelude::*;

/// Build a rich synthetic options `DataFrame` with calls+puts at 4 strikes across 3 dates,
/// with two expirations: near-term (Feb 16, DTE=32) and far-term (Mar 15, DTE=60).
///
/// Near-term calls (strikes 95/100/105/110):
///   | Strike | Jan 15 bid/ask | Jan 22 bid/ask | Feb 11 bid/ask | Delta |
///   |--------|---------------|---------------|---------------|-------|
///   | 95     | 8.00/8.50     | 7.00/7.50     | 5.00/5.50     | 0.70  |
///   | 100    | 5.00/5.50     | 4.00/4.50     | 2.00/2.50     | 0.50  |
///   | 105    | 3.00/3.50     | 2.20/2.70     | 1.00/1.50     | 0.35  |
///   | 110    | 1.50/2.00     | 1.00/1.50     | 0.30/0.80     | 0.20  |
///
/// Near-term puts (strikes 95/100/105/110):
///   | Strike | Jan 15 bid/ask | Jan 22 bid/ask | Feb 11 bid/ask | Delta  |
///   |--------|---------------|---------------|---------------|--------|
///   | 95     | 1.00/1.50     | 0.80/1.30     | 0.20/0.70     | -0.20  |
///   | 100    | 2.50/3.00     | 2.00/2.50     | 1.00/1.50     | -0.40  |
///   | 105    | 4.50/5.00     | 3.80/4.30     | 2.50/3.00     | -0.55  |
///   | 110    | 7.00/7.50     | 6.20/6.70     | 4.50/5.00     | -0.70  |
///
/// Far-term options have higher prices (more time value) and slower decay.
/// See source for exact far-term pricing data.
pub fn make_multi_strike_df() -> DataFrame {
    let exp_near = NaiveDate::from_ymd_opt(2024, 2, 16).unwrap(); // DTE=32 from Jan 15
    let exp_far = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(); // DTE=60 from Jan 15

    let dates = [
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), // entry
        NaiveDate::from_ymd_opt(2024, 1, 22).unwrap(), // mid
        NaiveDate::from_ymd_opt(2024, 2, 11).unwrap(), // exit (DTE=5 for near, DTE=33 for far)
    ];

    // Near-term call data per strike: (strike, delta, bids, asks)
    let call_data_near: Vec<(f64, f64, [f64; 3], [f64; 3])> = vec![
        (95.0, 0.70, [8.00, 7.00, 5.00], [8.50, 7.50, 5.50]),
        (100.0, 0.50, [5.00, 4.00, 2.00], [5.50, 4.50, 2.50]),
        (105.0, 0.35, [3.00, 2.20, 1.00], [3.50, 2.70, 1.50]),
        (110.0, 0.20, [1.50, 1.00, 0.30], [2.00, 1.50, 0.80]),
    ];

    // Near-term put data per strike: (strike, delta, bids, asks)
    let put_data_near: Vec<(f64, f64, [f64; 3], [f64; 3])> = vec![
        (95.0, -0.20, [1.00, 0.80, 0.20], [1.50, 1.30, 0.70]),
        (100.0, -0.40, [2.50, 2.00, 1.00], [3.00, 2.50, 1.50]),
        (105.0, -0.55, [4.50, 3.80, 2.50], [5.00, 4.30, 3.00]),
        (110.0, -0.70, [7.00, 6.20, 4.50], [7.50, 6.70, 5.00]),
    ];

    // Far-term call data: higher prices, slower decay
    let call_data_far: Vec<(f64, f64, [f64; 3], [f64; 3])> = vec![
        (95.0, 0.72, [10.00, 9.20, 7.00], [10.50, 9.70, 7.50]),
        (100.0, 0.52, [7.00, 6.30, 4.50], [7.50, 6.80, 5.00]),
        (105.0, 0.37, [4.50, 3.90, 2.80], [5.00, 4.40, 3.30]),
        (110.0, 0.22, [2.80, 2.30, 1.50], [3.30, 2.80, 2.00]),
    ];

    // Far-term put data: higher prices, slower decay
    let put_data_far: Vec<(f64, f64, [f64; 3], [f64; 3])> = vec![
        (95.0, -0.22, [2.00, 1.70, 1.00], [2.50, 2.20, 1.50]),
        (100.0, -0.42, [4.00, 3.50, 2.50], [4.50, 4.00, 3.00]),
        (105.0, -0.57, [6.50, 5.80, 4.30], [7.00, 6.30, 4.80]),
        (110.0, -0.72, [9.00, 8.30, 6.50], [9.50, 8.80, 7.00]),
    ];

    let mut quote_dates = Vec::new();
    let mut expirations_vec = Vec::new();
    let mut option_types = Vec::new();
    let mut strikes = Vec::new();
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    let mut deltas = Vec::new();

    // Helper to add rows for a given expiration and option data
    let mut add_rows =
        |data: &[(f64, f64, [f64; 3], [f64; 3])], opt_type: &'static str, exp: NaiveDate| {
            for (strike, delta_val, bid_arr, ask_arr) in data {
                for (i, date) in dates.iter().enumerate() {
                    quote_dates.push(date.and_hms_opt(0, 0, 0).unwrap());
                    expirations_vec.push(exp);
                    option_types.push(opt_type);
                    strikes.push(*strike);
                    bids.push(bid_arr[i]);
                    asks.push(ask_arr[i]);
                    deltas.push(*delta_val);
                }
            }
        };

    // Near-term expiration rows
    add_rows(&call_data_near, "call", exp_near);
    add_rows(&put_data_near, "put", exp_near);

    // Far-term expiration rows
    add_rows(&call_data_far, "call", exp_far);
    add_rows(&put_data_far, "put", exp_far);

    let mut df = df! {
        QUOTE_DATETIME_COL => &quote_dates,
        "option_type" => &option_types,
        "strike" => &strikes,
        "bid" => &bids,
        "ask" => &asks,
        "delta" => &deltas,
    }
    .unwrap();

    df.with_column(
        DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations_vec).into_column(),
    )
    .unwrap();

    df
}

pub fn delta(target: f64) -> TargetRange {
    TargetRange {
        target,
        min: 0.01,
        max: 0.99,
    }
}
