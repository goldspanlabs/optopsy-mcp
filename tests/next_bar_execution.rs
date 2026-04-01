//! Integration tests for the next-bar execution model.
//!
//! Verifies that orders queued on bar N execute on bar N+1 with correct fill
//! prices, that limit/stop orders respect price conditions, that `cancel_orders`
//! works end-to-end, and that position awareness variables are correct.

use std::collections::HashMap;

use anyhow::Result;
use chrono::NaiveDate;
use polars::prelude::*;

use optopsy_mcp::scripting::engine::{run_script_backtest, DataLoader};
use optopsy_mcp::scripting::types::OhlcvBar;

fn dt(y: i32, m: u32, day: u32) -> chrono::NaiveDateTime {
    NaiveDate::from_ymd_opt(y, m, day)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

struct TestDataLoader {
    ohlcv_df: DataFrame,
}

#[async_trait::async_trait]
impl DataLoader for TestDataLoader {
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
        Ok(DataFrame::empty())
    }

    fn load_splits(
        &self,
        _symbol: &str,
    ) -> Result<Vec<optopsy_mcp::data::adjustment_store::SplitRow>> {
        Ok(Vec::new())
    }

    fn load_dividends(
        &self,
        _symbol: &str,
    ) -> Result<Vec<optopsy_mcp::data::adjustment_store::DividendRow>> {
        Ok(Vec::new())
    }
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

fn default_params() -> HashMap<String, serde_json::Value> {
    let mut params = HashMap::new();
    params.insert("SYMBOL".to_string(), serde_json::json!("TEST"));
    params.insert("CAPITAL".to_string(), serde_json::json!(100_000.0));
    params
}

// ---------------------------------------------------------------------------
// Market order: verify next-bar fill at open price
// ---------------------------------------------------------------------------

/// Bar 0: script queues `buy_stock(100)` → market order
/// Bar 1: fills at bar 1 open (105.0), NOT bar 0 close (100.0)
/// Bar 2: exit triggers, close at bar 2 close (110.0)
/// Bar 3: extra bar (final bar orders cancelled)
/// Expected P&L: (110 - 105) * 100 = 500
#[tokio::test(flavor = "multi_thread")]
async fn market_order_fills_at_next_bar_open() {
    let bars = vec![
        OhlcvBar {
            datetime: dt(2024, 1, 2),
            open: 99.0,
            high: 101.0,
            low: 98.0,
            close: 100.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 3),
            open: 105.0,
            high: 106.0,
            low: 104.0,
            close: 105.5,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 4),
            open: 108.0,
            high: 112.0,
            low: 107.0,
            close: 110.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 5),
            open: 110.0,
            high: 111.0,
            low: 109.0,
            close: 110.0,
            volume: 1e6,
        },
    ];

    let loader = TestDataLoader {
        ohlcv_df: bars_to_df(&bars),
    };

    let script = r#"
        fn config() {
            #{
                symbol: params.SYMBOL,
                capital: params.CAPITAL,
                interval: "daily",
                data: #{ ohlcv: true, options: false },
                engine: #{ slippage: "mid" },
            }
        }

        fn on_bar(ctx) {
            if !ctx.has_positions() && ctx.bar_idx == 0 {
                return [buy_stock(100)];
            }
            []
        }

        fn on_exit_check(ctx, pos) {
            if pos.days_held >= 1 {
                return close_position("target");
            }
            hold_position()
        }
    "#;

    let result = run_script_backtest(script, &default_params(), &loader)
        .await
        .unwrap();

    assert_eq!(result.result.trade_count, 1, "Should have 1 trade");

    let trade = &result.result.trade_log[0];
    // Filled at bar 1 open = 105, closed at bar 2 close = 110
    // P&L = (110 - 105) * 100 = 500
    let pnl = trade.pnl;
    assert!(
        (pnl - 500.0).abs() < 1.0,
        "Expected ~500 PnL (fill at 105, exit at 110), got {pnl:.2}"
    );
}

// ---------------------------------------------------------------------------
// Limit order: fills only when price reaches limit
// ---------------------------------------------------------------------------

/// Bar 0: script queues `buy_limit(100, 98.0)` — buy if price dips to 98
/// Bar 1: low=99.0 > 98.0 → no fill
/// Bar 2: low=96.0 ≤ 98.0 → fills at 98.0
/// Bar 3: exit
/// Bar 4: extra bar
/// Expected P&L: (112 - 98) * 100 = 1400
#[tokio::test(flavor = "multi_thread")]
async fn limit_buy_fills_at_limit_price() {
    let bars = vec![
        OhlcvBar {
            datetime: dt(2024, 1, 2),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 3),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 4),
            open: 99.0,
            high: 100.0,
            low: 96.0,
            close: 97.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 5),
            open: 110.0,
            high: 113.0,
            low: 109.0,
            close: 112.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 8),
            open: 112.0,
            high: 113.0,
            low: 111.0,
            close: 112.0,
            volume: 1e6,
        },
    ];

    let loader = TestDataLoader {
        ohlcv_df: bars_to_df(&bars),
    };

    let script = r#"
        fn config() {
            #{
                symbol: params.SYMBOL,
                capital: params.CAPITAL,
                interval: "daily",
                data: #{ ohlcv: true, options: false },
                engine: #{ slippage: "mid" },
            }
        }

        fn on_bar(ctx) {
            if !ctx.has_positions() && ctx.bar_idx == 0 {
                return [buy_limit(100, 98.0)];
            }
            []
        }

        fn on_exit_check(ctx, pos) {
            if pos.days_held >= 1 {
                return close_position("target");
            }
            hold_position()
        }
    "#;

    let result = run_script_backtest(script, &default_params(), &loader)
        .await
        .unwrap();

    assert_eq!(
        result.result.trade_count, 1,
        "Should have 1 trade. Warnings: {:?}",
        result.result.warnings
    );

    let pnl = result.result.trade_log[0].pnl;
    // Limit order fills at 98.0 on bar 2 (low=96 reaches limit), exit at bar 3 close=112
    // P&L = (112 - 98) * 100 = 1400
    assert!(
        (pnl - 1400.0).abs() < 1.0,
        "Expected ~1400 PnL (limit fill at 98, exit at 112), got {pnl:.2}"
    );
}

// ---------------------------------------------------------------------------
// Stop order: fills when price breaches stop level
// ---------------------------------------------------------------------------

/// Bar 0: script queues `buy_stop(100, 105.0)` — buy on breakout above 105
/// Bar 1: high=103 < 105 → no fill
/// Bar 2: high=107 ≥ 105 → fills at 105.0
/// Bar 3: exit
/// Bar 4: extra bar
/// Expected P&L: (115 - 105) * 100 = 1000
#[tokio::test(flavor = "multi_thread")]
async fn stop_buy_fills_on_breakout() {
    let bars = vec![
        OhlcvBar {
            datetime: dt(2024, 1, 2),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 3),
            open: 100.0,
            high: 103.0,
            low: 99.0,
            close: 102.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 4),
            open: 103.0,
            high: 107.0,
            low: 102.0,
            close: 106.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 5),
            open: 113.0,
            high: 116.0,
            low: 112.0,
            close: 115.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 8),
            open: 115.0,
            high: 116.0,
            low: 114.0,
            close: 115.0,
            volume: 1e6,
        },
    ];

    let loader = TestDataLoader {
        ohlcv_df: bars_to_df(&bars),
    };

    let script = r#"
        fn config() {
            #{
                symbol: params.SYMBOL,
                capital: params.CAPITAL,
                interval: "daily",
                data: #{ ohlcv: true, options: false },
                engine: #{ slippage: "mid" },
            }
        }

        fn on_bar(ctx) {
            if !ctx.has_positions() && ctx.bar_idx == 0 {
                return [buy_stop(100, 105.0)];
            }
            []
        }

        fn on_exit_check(ctx, pos) {
            if pos.days_held >= 1 {
                return close_position("target");
            }
            hold_position()
        }
    "#;

    let result = run_script_backtest(script, &default_params(), &loader)
        .await
        .unwrap();

    assert_eq!(
        result.result.trade_count, 1,
        "Should have 1 trade. Warnings: {:?}",
        result.result.warnings
    );

    let pnl = result.result.trade_log[0].pnl;
    // Stop fills at 105.0 on bar 2, exit at bar 3 close = 115
    // P&L = (115 - 105) * 100 = 1000
    assert!(
        (pnl - 1000.0).abs() < 1.0,
        "Expected ~1000 PnL (stop fill at 105, exit at 115), got {pnl:.2}"
    );
}

// ---------------------------------------------------------------------------
// cancel_orders: queued orders are cancelled before fill
// ---------------------------------------------------------------------------

/// Bar 0: script queues `buy_limit(100, 90.0)` — limit order that won't fill immediately
/// Bar 1: low=99 > 90, limit not reached → still pending. Script calls `cancel_orders()`
/// Bar 2: low=85 < 90 — would have filled, but order was cancelled on bar 1
/// Bar 3: script queues a new `buy_stock` (market)
/// Bar 4: new order fills at bar 4 open (95)
/// Bar 5: exit
/// Bar 6: extra
/// Verifies: only 1 trade (from bar 3 order), not 2 (cancelled limit never filled)
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn cancel_orders_prevents_fill() {
    let bars = vec![
        OhlcvBar {
            datetime: dt(2024, 1, 2),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 3),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 4),
            open: 90.0,
            high: 91.0,
            low: 85.0,
            close: 88.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 5),
            open: 92.0,
            high: 93.0,
            low: 91.0,
            close: 92.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 8),
            open: 95.0,
            high: 96.0,
            low: 94.0,
            close: 95.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 9),
            open: 98.0,
            high: 100.0,
            low: 97.0,
            close: 99.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 10),
            open: 99.0,
            high: 100.0,
            low: 98.0,
            close: 99.0,
            volume: 1e6,
        },
    ];

    let loader = TestDataLoader {
        ohlcv_df: bars_to_df(&bars),
    };

    let script = r#"
        fn config() {
            #{
                symbol: params.SYMBOL,
                capital: params.CAPITAL,
                interval: "daily",
                data: #{ ohlcv: true, options: false },
                engine: #{ slippage: "mid" },
            }
        }

        fn on_bar(ctx) {
            if ctx.bar_idx == 0 {
                // Queue a limit buy at 90 — won't fill on bar 1 (low=99)
                return [buy_limit(100, 90.0)];
            }
            if ctx.bar_idx == 1 {
                // Cancel the pending limit order before it can fill on bar 2
                return [cancel_orders()];
            }
            if ctx.bar_idx == 3 && !ctx.has_positions() {
                // Queue a fresh market buy
                return [buy_stock(100)];
            }
            []
        }

        fn on_exit_check(ctx, pos) {
            if pos.days_held >= 1 {
                return close_position("target");
            }
            hold_position()
        }
    "#;

    let result = run_script_backtest(script, &default_params(), &loader)
        .await
        .unwrap();

    assert_eq!(
        result.result.trade_count, 1,
        "Should have 1 trade (cancelled limit never filled). Warnings: {:?}",
        result.result.warnings,
    );

    let pnl = result.result.trade_log[0].pnl;
    // Market order from bar 3 fills at bar 4 open (95), exit at bar 5 close (99)
    // P&L = (99 - 95) * 100 = 400
    assert!(
        (pnl - 400.0).abs() < 1.0,
        "Expected ~400 PnL (entry at 95, exit at 99), got {pnl:.2}"
    );
}

// ---------------------------------------------------------------------------
// Position awareness: ctx.market_position and ctx.bars_since_entry
// ---------------------------------------------------------------------------

/// Verifies that position awareness variables are correctly exposed to scripts.
/// Script records `ctx.market_position` and `ctx.bars_since_entry` into metadata
/// so we can assert on them.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn position_awareness_exposed_to_script() {
    let bars = vec![
        OhlcvBar {
            datetime: dt(2024, 1, 2),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 3),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 4),
            open: 101.0,
            high: 102.0,
            low: 100.0,
            close: 101.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 5),
            open: 102.0,
            high: 103.0,
            low: 101.0,
            close: 102.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 8),
            open: 103.0,
            high: 104.0,
            low: 102.0,
            close: 103.0,
            volume: 1e6,
        },
    ];

    let loader = TestDataLoader {
        ohlcv_df: bars_to_df(&bars),
    };

    // Script opens a position on bar 0 (fills bar 1), then records awareness on bars 2-3
    let script = r#"
        let mp_bar2 = 0;
        let mp_bar3 = 0;
        let bse_bar2 = -1;
        let bse_bar3 = -1;
        let entry_px = 0.0;

        fn config() {
            #{
                symbol: params.SYMBOL,
                capital: params.CAPITAL,
                interval: "daily",
                data: #{ ohlcv: true, options: false },
                engine: #{ slippage: "mid" },
            }
        }

        fn on_bar(ctx) {
            if ctx.bar_idx == 0 {
                return [buy_stock(100)];
            }
            // Record awareness on bar 2 (position filled on bar 1, so 1 bar since entry)
            if ctx.bar_idx == 2 {
                mp_bar2 = ctx.market_position;
                bse_bar2 = ctx.bars_since_entry;
                entry_px = ctx.entry_price;
            }
            if ctx.bar_idx == 3 {
                mp_bar3 = ctx.market_position;
                bse_bar3 = ctx.bars_since_entry;
            }
            []
        }

        fn on_end(ctx) {
            #{
                mp_bar2: mp_bar2,
                mp_bar3: mp_bar3,
                bse_bar2: bse_bar2,
                bse_bar3: bse_bar3,
                entry_px: entry_px,
            }
        }
    "#;

    let result = run_script_backtest(script, &default_params(), &loader)
        .await
        .unwrap();

    let meta = result.metadata.expect("on_end should return metadata");

    let mp_bar2 = meta.get("mp_bar2").unwrap().as_int().unwrap();
    let mp_bar3 = meta.get("mp_bar3").unwrap().as_int().unwrap();
    let bse_bar2 = meta.get("bse_bar2").unwrap().as_int().unwrap();
    let bse_bar3 = meta.get("bse_bar3").unwrap().as_int().unwrap();
    let entry_px = meta.get("entry_px").unwrap().as_float().unwrap();

    // Position filled on bar 1, so:
    assert_eq!(mp_bar2, 1, "market_position should be 1 (long) on bar 2");
    assert_eq!(mp_bar3, 1, "market_position should be 1 (long) on bar 3");
    assert_eq!(
        bse_bar2, 1,
        "bars_since_entry should be 1 on bar 2 (entered bar 1)"
    );
    assert_eq!(
        bse_bar3, 2,
        "bars_since_entry should be 2 on bar 3 (entered bar 1)"
    );
    assert!(
        (entry_px - 100.0).abs() < 0.01,
        "entry_price should be bar 1 open (100.0), got {entry_px}"
    );
}

// ---------------------------------------------------------------------------
// Multiple orders queued same bar: both fill next bar
// ---------------------------------------------------------------------------

/// Script queues two `buy_stock` orders on bar 0. Both should fill on bar 1.
#[tokio::test(flavor = "multi_thread")]
async fn multiple_orders_same_bar_both_fill() {
    let bars = vec![
        OhlcvBar {
            datetime: dt(2024, 1, 2),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 3),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 4),
            open: 101.0,
            high: 102.0,
            low: 100.0,
            close: 101.0,
            volume: 1e6,
        },
        OhlcvBar {
            datetime: dt(2024, 1, 5),
            open: 102.0,
            high: 103.0,
            low: 101.0,
            close: 102.0,
            volume: 1e6,
        },
    ];

    let loader = TestDataLoader {
        ohlcv_df: bars_to_df(&bars),
    };

    let script = r#"
        fn config() {
            #{
                symbol: params.SYMBOL,
                capital: params.CAPITAL,
                interval: "daily",
                data: #{ ohlcv: true, options: false },
                engine: #{ slippage: "mid" },
                auto_close_on_end: true,
            }
        }

        fn on_bar(ctx) {
            if ctx.bar_idx == 0 {
                return [buy_stock(50), buy_stock(30)];
            }
            []
        }
    "#;

    let result = run_script_backtest(script, &default_params(), &loader)
        .await
        .unwrap();

    // Both orders should fill on bar 1, then auto-close at end
    assert_eq!(
        result.result.trade_count, 2,
        "Should have 2 trades (both orders filled). Warnings: {:?}",
        result.result.warnings
    );
}
