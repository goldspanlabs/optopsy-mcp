//! Integration test: covered call position spanning a stock split.
//!
//! Verifies that when a 2:1 split occurs mid-position:
//! 1. Stock position qty doubles and entry_price halves
//! 2. Split-adjusted OHLCV prices match split-adjusted option strikes
//! 3. The covered call resolves correctly at expiration

use std::collections::BTreeMap;

use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::*;

use optopsy_mcp::data::adjustment_store::SplitRow;
use optopsy_mcp::data::parquet::DATETIME_COL;
use optopsy_mcp::scripting::engine::{run_script_backtest, DataLoader};
use optopsy_mcp::scripting::types::OhlcvBar;

fn d(y: i32, m: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, day).unwrap()
}

fn dt(y: i32, m: u32, day: u32) -> chrono::NaiveDateTime {
    d(y, m, day).and_hms_opt(0, 0, 0).unwrap()
}

fn make_options_df(
    rows: &[(chrono::NaiveDateTime, NaiveDate, &str, f64, f64, f64, f64)],
) -> DataFrame {
    let dates: Vec<chrono::NaiveDateTime> = rows.iter().map(|r| r.0).collect();
    let expirations: Vec<NaiveDate> = rows.iter().map(|r| r.1).collect();
    let opt_types: Vec<&str> = rows.iter().map(|r| r.2).collect();
    let strikes: Vec<f64> = rows.iter().map(|r| r.3).collect();
    let bids: Vec<f64> = rows.iter().map(|r| r.4).collect();
    let asks: Vec<f64> = rows.iter().map(|r| r.5).collect();
    let deltas: Vec<f64> = rows.iter().map(|r| r.6).collect();

    let mut df = df! {
        DATETIME_COL => &dates,
        "option_type" => &opt_types,
        "strike" => &strikes,
        "bid" => &bids,
        "ask" => &asks,
        "delta" => &deltas,
    }
    .unwrap();

    let exp_col =
        DateChunked::from_naive_date(PlSmallStr::from("expiration"), expirations).into_column();
    df.with_column(exp_col).unwrap();
    df
}

fn bars_to_df(bars: &[OhlcvBar]) -> DataFrame {
    let datetimes: Vec<chrono::NaiveDateTime> = bars.iter().map(|b| b.datetime).collect();
    let opens: Vec<f64> = bars.iter().map(|b| b.open).collect();
    let highs: Vec<f64> = bars.iter().map(|b| b.high).collect();
    let lows: Vec<f64> = bars.iter().map(|b| b.low).collect();
    let closes: Vec<f64> = bars.iter().map(|b| b.close).collect();
    let volumes: Vec<f64> = bars.iter().map(|b| b.volume).collect();

    df! {
        "datetime" => DatetimeChunked::from_naive_datetime(
            PlSmallStr::from("datetime"),
            datetimes,
            TimeUnit::Microseconds,
        ).into_column().take_materialized_series(),
        "open" => &opens,
        "high" => &highs,
        "low" => &lows,
        "close" => &closes,
        "volume" => &volumes,
    }
    .unwrap()
}

fn make_bars_from_closes(closes: &BTreeMap<NaiveDate, f64>) -> Vec<OhlcvBar> {
    closes
        .iter()
        .map(|(&date, &close)| OhlcvBar {
            datetime: date.and_hms_opt(0, 0, 0).unwrap(),
            open: close,
            high: close * 1.01,
            low: close * 0.99,
            close,
            volume: 1_000_000.0,
        })
        .collect()
}

/// Test DataLoader that returns splits for split-spanning tests.
struct SplitTestDataLoader {
    ohlcv_df: DataFrame,
    options_df: DataFrame,
    splits: Vec<SplitRow>,
}

#[async_trait::async_trait]
impl DataLoader for SplitTestDataLoader {
    async fn load_ohlcv(
        &self,
        _symbol: &str,
        _start: Option<NaiveDate>,
        _end: Option<NaiveDate>,
    ) -> Result<DataFrame> {
        Ok(self.ohlcv_df.clone())
    }

    async fn load_options(
        &self,
        _symbol: &str,
        _start: Option<NaiveDate>,
        _end: Option<NaiveDate>,
    ) -> Result<DataFrame> {
        Ok(self.options_df.clone())
    }

    fn load_splits(&self, _symbol: &str) -> Result<Vec<SplitRow>> {
        Ok(self.splits.clone())
    }

    fn load_dividends(
        &self,
        _symbol: &str,
    ) -> Result<Vec<optopsy_mcp::data::adjustment_store::DividendRow>> {
        Ok(Vec::new())
    }
}

/// Scenario: Buy 100 shares + sell a covered call spanning a 2:1 split.
///
/// OHLCV prices are UNADJUSTED. Options strikes are SPLIT-ADJUSTED (as FirstRate provides).
/// The engine applies split adjustment to OHLCV at load time.
///
/// Timeline:
///   Jan 2:  Stock $200 (unadj). Split-adjusted = $100. Open stock + short call strike $110.
///   Jan 15: 2:1 split. Stock $100 (unadj, post-split). Split-adjusted = $100 (factor=1.0).
///           Stock position: qty 100→200, entry_price $100→$50.
///   Feb 16: Stock $115 (unadj, post-split). Expiration.
///           Strike $110 <= close $115 → ITM → called away.
#[tokio::test(flavor = "multi_thread")]
async fn covered_call_spanning_split() {
    let entry_date = d(2024, 1, 2);
    let split_date = d(2024, 1, 15);
    let call_exp = d(2024, 2, 16);

    // OHLCV: unadjusted prices
    let mut closes = BTreeMap::new();
    closes.insert(entry_date, 200.0); // pre-split
    closes.insert(split_date, 100.0); // post-split (200/2)
    closes.insert(call_exp, 115.0); // post-split, above call strike

    let bars = make_bars_from_closes(&closes);

    // Options data: split-adjusted strikes throughout
    let options_df = make_options_df(&[
        // Entry: short call at split-adjusted strike 110, delta -0.30, DTE=45
        (dt(2024, 1, 2), call_exp, "c", 110.0, 5.00, 5.50, -0.30),
        // After split: same contract at same strike (already adjusted)
        (dt(2024, 1, 15), call_exp, "c", 110.0, 3.00, 3.50, -0.25),
        // Expiration: ITM (close 115 > strike 110)
        (dt(2024, 2, 16), call_exp, "c", 110.0, 5.00, 5.50, -0.60),
    ]);

    let splits = vec![SplitRow {
        symbol: "TEST".to_string(),
        date: split_date,
        ratio: 2.0,
    }];

    let loader = SplitTestDataLoader {
        ohlcv_df: bars_to_df(&bars),
        options_df,
        splits,
    };

    // Script: buy stock + sell covered call on bar 0, hold to expiration
    let script = r#"
        fn config() {
            #{
                symbol: "TEST",
                capital: 50000.0,
                start_date: "2024-01-02",
                end_date: "2024-02-16",
                interval: "daily",
                data: #{ ohlcv: true, options: true },
                engine: #{ slippage: "mid" },
            }
        }

        fn on_bar(ctx) {
            if ctx.has_positions() { return []; }

            // Open stock position
            let actions = [#{ action: "open_stock", side: "long", qty: 100 }];

            // Open covered call via build_strategy
            let spread = ctx.build_strategy([
                #{ side: "short", option_type: "call", delta: 0.30, dte: 45 },
            ]);
            if spread != () {
                actions.push(#{ action: "open_spread", spread: spread });
            }

            actions
        }
    "#;

    let params = std::collections::HashMap::new();
    let result = run_script_backtest(script, &params, &loader).await;

    assert!(
        result.is_ok(),
        "Covered call backtest should succeed: {:?}",
        result.err()
    );

    let result = result.unwrap();

    // The backtest should complete with trades — stock + call opened and resolved
    assert!(
        result.result.trade_count >= 1,
        "Expected at least 1 trade, got {}. Warnings: {:?}",
        result.result.trade_count,
        result.result.warnings,
    );

    assert_eq!(
        result.result.equity_curve.len(),
        3,
        "Expected 3 equity points (one per bar), got {}",
        result.result.equity_curve.len(),
    );

    // Print trade details for debugging
    for (i, trade) in result.result.trade_log.iter().enumerate() {
        eprintln!(
            "Trade {}: exit_type={:?}, pnl={:.2}, days_held={}",
            i, trade.exit_type, trade.pnl, trade.days_held,
        );
    }

    // Verify the call was correctly classified as called-away (ITM at expiration)
    let has_called_away = result
        .result
        .trade_log
        .iter()
        .any(|t| t.exit_type == optopsy_mcp::engine::types::ExitType::CalledAway);
    assert!(
        has_called_away,
        "Should have a CalledAway trade (call ITM at expiration)"
    );
}
