use anyhow::Result;
use chrono::NaiveDateTime;
use std::collections::BTreeMap;

use super::core::RawTrade;
#[allow(clippy::wildcard_imports)]
use super::types::*;

#[allow(dead_code)]
pub struct SimResult {
    pub equity_curve: Vec<EquityPoint>,
    pub trade_log: Vec<TradeRecord>,
}

/// Run simulation: trade selection, position management, equity curve
#[allow(
    dead_code,
    clippy::unnecessary_wraps,
    clippy::cast_sign_loss,
    clippy::float_cmp
)]
pub fn simulate(
    mut trades: Vec<RawTrade>,
    capital: f64,
    max_positions: i32,
    selector: &TradeSelector,
) -> Result<SimResult> {
    // Sort trades by entry datetime
    trades.sort_by_key(|t| t.entry_datetime);

    // Group trades by entry datetime
    let mut by_datetime: BTreeMap<NaiveDateTime, Vec<RawTrade>> = BTreeMap::new();
    for trade in trades {
        by_datetime
            .entry(trade.entry_datetime)
            .or_default()
            .push(trade);
    }

    let mut equity = capital;
    let mut equity_curve = Vec::new();
    let mut trade_log = Vec::new();
    let mut active_positions: Vec<(NaiveDateTime, NaiveDateTime)> = Vec::new(); // (entry, exit)
    let mut trade_id = 0usize;

    for (dt, candidates) in &by_datetime {
        // Remove expired positions
        active_positions.retain(|(_, exit)| exit > dt);

        if active_positions.len() >= max_positions as usize {
            continue;
        }

        // Select trade from candidates
        let selected = select_trade(candidates, selector);

        if let Some(trade) = selected {
            // Check no overlap with existing positions
            let overlaps = active_positions
                .iter()
                .any(|(entry, exit)| trade.entry_datetime < *exit && trade.exit_datetime > *entry);

            if overlaps && active_positions.len() >= max_positions as usize {
                continue;
            }

            active_positions.push((trade.entry_datetime, trade.exit_datetime));

            equity += trade.pnl;
            equity_curve.push(EquityPoint {
                datetime: trade.exit_datetime,
                equity,
            });

            trade_id += 1;
            trade_log.push(TradeRecord {
                trade_id,
                entry_datetime: trade.entry_datetime,
                exit_datetime: trade.exit_datetime,
                entry_cost: trade.entry_cost,
                exit_proceeds: trade.exit_proceeds,
                pnl: trade.pnl,
                days_held: trade.days_held,
                exit_type: trade.exit_type.clone(),
            });
        }
    }

    // Ensure equity curve starts at beginning
    if equity_curve.is_empty() || equity_curve[0].equity != capital {
        if let Some(first_dt) = by_datetime.keys().next() {
            equity_curve.insert(
                0,
                EquityPoint {
                    datetime: *first_dt,
                    equity: capital,
                },
            );
        }
    }

    Ok(SimResult {
        equity_curve,
        trade_log,
    })
}

#[allow(dead_code)]
fn select_trade<'a>(candidates: &'a [RawTrade], selector: &TradeSelector) -> Option<&'a RawTrade> {
    if candidates.is_empty() {
        return None;
    }

    match selector {
        TradeSelector::First => candidates.first(),
        TradeSelector::Nearest => {
            // Already sorted by delta distance in filtering
            candidates.first()
        }
        TradeSelector::HighestPremium => candidates.iter().max_by(|a, b| {
            a.entry_cost
                .abs()
                .partial_cmp(&b.entry_cost.abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        }),
        TradeSelector::LowestPremium => candidates.iter().min_by(|a, b| {
            a.entry_cost
                .abs()
                .partial_cmp(&b.entry_cost.abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn make_raw_trade(entry_day: u32, exit_day: u32, pnl: f64, entry_cost: f64) -> RawTrade {
        let entry = NaiveDate::from_ymd_opt(2024, 1, entry_day)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let exit = NaiveDate::from_ymd_opt(2024, 1, exit_day)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        RawTrade {
            entry_datetime: entry,
            exit_datetime: exit,
            entry_cost,
            exit_proceeds: entry_cost + pnl,
            pnl,
            days_held: i64::from(exit_day - entry_day),
            exit_type: ExitType::DteExit,
        }
    }

    #[test]
    fn simulate_basic_equity_curve() {
        let trades = vec![
            make_raw_trade(1, 10, 100.0, 500.0),
            make_raw_trade(15, 25, -50.0, 500.0),
        ];
        let result = simulate(trades, 10000.0, 5, &TradeSelector::First).unwrap();
        assert_eq!(result.trade_log.len(), 2);
        // Final equity: 10000 + 100 - 50 = 10050
        let final_eq = result.equity_curve.last().unwrap().equity;
        assert!((final_eq - 10050.0).abs() < 1e-10);
    }

    #[test]
    fn simulate_max_positions_respected() {
        // Two trades at the same time, max_positions=1
        let trades = vec![
            make_raw_trade(1, 20, 100.0, 500.0),
            make_raw_trade(1, 20, 200.0, 600.0),
        ];
        let result = simulate(trades, 10000.0, 1, &TradeSelector::First).unwrap();
        // Should only take 1 trade
        assert_eq!(result.trade_log.len(), 1);
    }

    #[test]
    fn simulate_highest_premium_selector() {
        let trades = vec![
            make_raw_trade(1, 10, 50.0, 300.0),
            make_raw_trade(1, 10, 100.0, 800.0), // higher premium
        ];
        let result = simulate(trades, 10000.0, 1, &TradeSelector::HighestPremium).unwrap();
        assert_eq!(result.trade_log.len(), 1);
        assert!((result.trade_log[0].entry_cost - 800.0).abs() < 1e-10);
    }

    #[test]
    fn simulate_lowest_premium_selector() {
        let trades = vec![
            make_raw_trade(1, 10, 50.0, 300.0), // lower premium
            make_raw_trade(1, 10, 100.0, 800.0),
        ];
        let result = simulate(trades, 10000.0, 1, &TradeSelector::LowestPremium).unwrap();
        assert_eq!(result.trade_log.len(), 1);
        assert!((result.trade_log[0].entry_cost - 300.0).abs() < 1e-10);
    }

    #[test]
    fn simulate_non_overlapping_trades() {
        // Trades don't overlap, should all be taken
        let trades = vec![
            make_raw_trade(1, 5, 50.0, 500.0),
            make_raw_trade(6, 10, 75.0, 500.0),
            make_raw_trade(11, 15, 25.0, 500.0),
        ];
        let result = simulate(trades, 10000.0, 1, &TradeSelector::First).unwrap();
        assert_eq!(result.trade_log.len(), 3);
        let final_eq = result.equity_curve.last().unwrap().equity;
        assert!((final_eq - 10150.0).abs() < 1e-10);
    }

    #[test]
    fn simulate_empty_trades() {
        let trades = vec![];
        let result = simulate(trades, 10000.0, 5, &TradeSelector::First).unwrap();
        assert_eq!(result.trade_log.len(), 0);
        assert!(result.equity_curve.is_empty());
    }
}
