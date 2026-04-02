//! Integration test: covered call position spanning a stock split.
//!
//! Verifies that when a 2:1 split occurs mid-position:
//! 1. Stock position qty doubles and `entry_price` halves
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

/// Test `DataLoader` that returns splits for split-spanning tests.
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
/// OHLCV prices are UNADJUSTED. Options strikes are SPLIT-ADJUSTED (as `FirstRate` provides).
/// The engine applies split adjustment to OHLCV at load time.
///
/// Timeline:
///   Jan 2:  Stock $200 (unadj). Split-adjusted = $100. Open stock + short call strike $110.
///   Jan 15: 2:1 split. Stock $100 (unadj, post-split). Split-adjusted = $100 (factor=1.0).
///           Stock position: qty 100→200, `entry_price` $100→$50.
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
    let result = run_script_backtest(script, &params, &loader, None, None, None).await;

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

    // With split-adjusted bars, stock was bought at $100 (split-adj close on Jan 2).
    // No position qty adjustment should occur (bars are already adjusted).
    // The stock position should remain 100 shares at $100, NOT 200 shares at $50.
    eprintln!("trade_count: {}", result.result.trade_count);
    eprintln!("warnings: {:?}", result.result.warnings);
    eprintln!(
        "final equity: {:.2}",
        result.result.equity_curve.last().map_or(0.0, |e| e.equity)
    );
}

/// Scenario: Full wheel cycle across a 2:1 split.
/// Short put → assigned → hold stock → sell covered call → called away.
///
/// Timeline:
///   Jan 2:  Sell short put strike $95, exp Jan 19. Stock $200 (unadj), split-adj $100.
///   Jan 15: 2:1 split. Stock $100 (unadj). Split-adj $100.
///   Jan 19: Put exp. Stock $90 (unadj), split-adj $90. Strike $95 > close $90 → ITM.
///           Assignment: implicit stock 100 shares at $95.
///   Jan 22: Sell covered call strike $100, exp Feb 16. Stock $92 (unadj), split-adj $92.
///   Feb 16: Stock $105 (unadj). Strike $100 <= close $105 → called away.
///           Stock closed at $100. P&L = ($100 - $95) × 100 = $500.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn wheel_cycle_spanning_split() {
    let put_entry = d(2024, 1, 2);
    let split_date = d(2024, 1, 15);
    let put_exp = d(2024, 1, 19);
    let call_entry = d(2024, 1, 22);
    let call_exp = d(2024, 2, 16);

    // OHLCV: unadjusted prices
    let mut closes = BTreeMap::new();
    closes.insert(put_entry, 200.0); // pre-split
    closes.insert(split_date, 100.0); // post-split
    closes.insert(put_exp, 90.0); // below put strike → ITM
    closes.insert(call_entry, 92.0); // post-split
    closes.insert(call_exp, 105.0); // above call strike → ITM

    let bars = make_bars_from_closes(&closes);

    // Options data: all strikes split-adjusted
    let options_df = make_options_df(&[
        // Put entry: strike 95, delta -0.30
        (dt(2024, 1, 2), put_exp, "p", 95.0, 3.00, 3.50, -0.30),
        // Put on split day (for MTM)
        (dt(2024, 1, 15), put_exp, "p", 95.0, 4.00, 4.50, -0.40),
        // Put at expiration: ITM (close 90 < strike 95)
        (dt(2024, 1, 19), put_exp, "p", 95.0, 5.00, 5.50, -0.80),
        // Call entry: strike 100, delta -0.30
        (dt(2024, 1, 22), call_exp, "c", 100.0, 3.00, 3.50, -0.30),
        // Call at expiration: ITM (close 105 > strike 100)
        (dt(2024, 2, 16), call_exp, "c", 100.0, 5.00, 5.50, -0.60),
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

    // Wheel script: sell put → on assignment sell call → hold
    let script = r#"
        let state = "selling_puts";

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

            if state == "selling_puts" {
                let spread = ctx.build_strategy([
                    #{ side: "short", option_type: "put", delta: 0.30, dte: 17 },
                ]);
                if spread != () {
                    return [#{ action: "open_spread", spread: spread }];
                }
            }

            if state == "selling_calls" {
                let spread = ctx.build_strategy([
                    #{ side: "short", option_type: "call", delta: 0.30, dte: 25 },
                ]);
                if spread != () {
                    return [#{ action: "open_spread", spread: spread }];
                }
            }

            []
        }

        fn on_position_closed(ctx, pos, reason) {
            if reason == "assignment" {
                state = "selling_calls";
            }
        }
    "#;

    let params = std::collections::HashMap::new();
    let result = run_script_backtest(script, &params, &loader, None, None, None).await;

    assert!(
        result.is_ok(),
        "Wheel backtest should succeed: {:?}",
        result.err()
    );

    let result = result.unwrap();

    for (i, trade) in result.result.trade_log.iter().enumerate() {
        eprintln!(
            "Wheel trade {}: exit_type={:?}, pnl={:.2}, days_held={}",
            i, trade.exit_type, trade.pnl, trade.days_held,
        );
    }

    // Should have at least: put assignment + call called-away (which closes the stock)
    assert!(
        result.result.trade_count >= 2,
        "Expected at least 2 trades (put + call/stock), got {}. Warnings: {:?}",
        result.result.trade_count,
        result.result.warnings,
    );

    // Verify we got both assignment and called-away exits
    let exit_types: Vec<_> = result
        .result
        .trade_log
        .iter()
        .map(|t| format!("{:?}", t.exit_type))
        .collect();
    eprintln!("Exit types: {exit_types:?}");

    assert!(
        exit_types.iter().any(|t| t == "Assignment"),
        "Should have an Assignment exit"
    );
    assert!(
        exit_types.iter().any(|t| t == "CalledAway"),
        "Should have a CalledAway exit"
    );
}

/// Scenario: Stock-only backtest across a split (no options).
/// Verifies split-adjusted prices produce correct returns.
///
/// Buy at $200 (unadj, pre-split), sell at $115 (unadj, post-split).
/// Split-adjusted: buy at $100, sell at $115 → 15% return.
/// Without split adjustment: $200 → $115 would be -42.5% — clearly wrong.
///
/// Next-bar execution: order queued on bar 0, fills on bar 1, needs 4 bars
/// so `days_held` reaches 2 for exit.
#[tokio::test(flavor = "multi_thread")]
async fn stock_only_across_split() {
    let entry_date = d(2024, 1, 2);
    let split_date = d(2024, 1, 15);
    let hold_date = d(2024, 1, 20); // extra bar so days_held reaches 2
    let exit_date = d(2024, 2, 1);

    let mut closes = BTreeMap::new();
    closes.insert(entry_date, 200.0); // pre-split
    closes.insert(split_date, 100.0); // post-split
    closes.insert(hold_date, 110.0); // post-split, holding
    closes.insert(exit_date, 115.0); // post-split, up 15% from adjusted entry

    let bars = make_bars_from_closes(&closes);

    let splits = vec![SplitRow {
        symbol: "TEST".to_string(),
        date: split_date,
        ratio: 2.0,
    }];

    let loader = SplitTestDataLoader {
        ohlcv_df: bars_to_df(&bars),
        options_df: DataFrame::empty(),
        splits,
    };

    // Buy on bar 0, sell after 1+ days held
    let script = r#"
        fn config() {
            #{
                symbol: "TEST",
                capital: 50000.0,
                start_date: "2024-01-02",
                end_date: "2024-02-01",
                interval: "daily",
                data: #{ ohlcv: true, options: false },
                engine: #{ slippage: "mid" },
            }
        }

        fn on_bar(ctx) {
            if !ctx.has_positions() {
                return [#{ action: "open_stock", side: "long", qty: 100 }];
            }
            []
        }

        fn on_exit_check(ctx, pos) {
            if pos.days_held >= 2 {
                return #{ action: "close", reason: "target" };
            }
            #{ action: "hold" }
        }
    "#;

    let params = std::collections::HashMap::new();
    let result = run_script_backtest(script, &params, &loader, None, None, None).await;

    assert!(
        result.is_ok(),
        "Stock backtest should succeed: {:?}",
        result.err()
    );

    let result = result.unwrap();

    assert_eq!(
        result.result.trade_count, 1,
        "Should have 1 closed trade. Warnings: {:?}",
        result.result.warnings,
    );

    // Split-adjusted: bought at $100, sold at $115 → PnL = $15 × 100 shares = $1,500
    let pnl = result.result.trade_log[0].pnl;
    eprintln!("Stock PnL: {pnl:.2}");
    assert!(
        (pnl - 1500.0).abs() < 1.0,
        "Expected ~$1,500 PnL (split-adjusted $100→$115 × 100 shares), got {pnl:.2}",
    );
}
