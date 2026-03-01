use super::core::RawTrade;
use super::types::ExitType;

/// Apply early exit rules to trades
#[allow(dead_code)]
pub fn apply_early_exits(
    trades: Vec<RawTrade>,
    stop_loss: Option<f64>,
    take_profit: Option<f64>,
    max_hold_days: Option<i32>,
) -> Vec<RawTrade> {
    trades
        .into_iter()
        .map(|mut trade| {
            // Check stop loss (as fraction of entry cost)
            if let Some(sl) = stop_loss {
                let loss_threshold = trade.entry_cost.abs() * sl;
                if trade.pnl < -loss_threshold {
                    trade.pnl = -loss_threshold;
                    trade.exit_type = ExitType::StopLoss;
                }
            }

            // Check take profit (as fraction of entry cost)
            if let Some(tp) = take_profit {
                let profit_threshold = trade.entry_cost.abs() * tp;
                if trade.pnl > profit_threshold {
                    trade.pnl = profit_threshold;
                    trade.exit_type = ExitType::TakeProfit;
                }
            }

            // Check max hold days
            if let Some(max_days) = max_hold_days {
                if trade.days_held > i64::from(max_days) {
                    trade.days_held = i64::from(max_days);
                    if matches!(trade.exit_type, ExitType::DteExit) {
                        trade.exit_type = ExitType::MaxHold;
                    }
                }
            }

            trade
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn make_trade(pnl: f64, entry_cost: f64, days_held: i64) -> RawTrade {
        let dt = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let exit_dt = NaiveDate::from_ymd_opt(2024, 1, 1 + days_held as u32)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        RawTrade {
            entry_datetime: dt,
            exit_datetime: exit_dt,
            entry_cost,
            exit_proceeds: entry_cost + pnl,
            pnl,
            days_held,
            exit_type: ExitType::DteExit,
        }
    }

    #[test]
    fn no_exits_passthrough() {
        let trades = vec![make_trade(100.0, 500.0, 10)];
        let result = apply_early_exits(trades, None, None, None);
        assert!((result[0].pnl - 100.0).abs() < f64::EPSILON);
        assert!(matches!(result[0].exit_type, ExitType::DteExit));
    }

    #[test]
    fn stop_loss_triggers() {
        // entry_cost = 500, stop_loss = 0.5 → threshold = 250
        // pnl = -300 < -250 → capped to -250
        let trades = vec![make_trade(-300.0, 500.0, 10)];
        let result = apply_early_exits(trades, Some(0.5), None, None);
        assert!((result[0].pnl - (-250.0)).abs() < 1e-10);
        assert!(matches!(result[0].exit_type, ExitType::StopLoss));
    }

    #[test]
    fn stop_loss_no_trigger() {
        // pnl = -100 > -250 → no trigger
        let trades = vec![make_trade(-100.0, 500.0, 10)];
        let result = apply_early_exits(trades, Some(0.5), None, None);
        assert!((result[0].pnl - (-100.0)).abs() < 1e-10);
        assert!(matches!(result[0].exit_type, ExitType::DteExit));
    }

    #[test]
    fn take_profit_triggers() {
        // entry_cost = 500, take_profit = 0.5 → threshold = 250
        // pnl = 300 > 250 → capped to 250
        let trades = vec![make_trade(300.0, 500.0, 10)];
        let result = apply_early_exits(trades, None, Some(0.5), None);
        assert!((result[0].pnl - 250.0).abs() < 1e-10);
        assert!(matches!(result[0].exit_type, ExitType::TakeProfit));
    }

    #[test]
    fn take_profit_no_trigger() {
        let trades = vec![make_trade(100.0, 500.0, 10)];
        let result = apply_early_exits(trades, None, Some(0.5), None);
        assert!((result[0].pnl - 100.0).abs() < 1e-10);
    }

    #[test]
    fn max_hold_days_triggers() {
        let trades = vec![make_trade(100.0, 500.0, 15)];
        let result = apply_early_exits(trades, None, None, Some(10));
        assert_eq!(result[0].days_held, 10);
        assert!(matches!(result[0].exit_type, ExitType::MaxHold));
    }

    #[test]
    fn max_hold_days_no_trigger() {
        let trades = vec![make_trade(100.0, 500.0, 5)];
        let result = apply_early_exits(trades, None, None, Some(10));
        assert_eq!(result[0].days_held, 5);
        assert!(matches!(result[0].exit_type, ExitType::DteExit));
    }

    #[test]
    fn stop_loss_takes_priority_over_take_profit() {
        // Both triggered: pnl = -300, sl threshold = 250, tp threshold = 250
        // Stop loss checked first → exit_type = StopLoss, then TP won't override since pnl is negative
        let trades = vec![make_trade(-300.0, 500.0, 10)];
        let result = apply_early_exits(trades, Some(0.5), Some(0.5), None);
        assert!(matches!(result[0].exit_type, ExitType::StopLoss));
    }
}
