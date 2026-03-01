use anyhow::Result;

use super::types::{EquityPoint, PerformanceMetrics, TradeRecord};

/// Maximum finite value for profit factor when there are no losing trades.
/// Avoids `f64::INFINITY` which is not valid JSON.
const MAX_PROFIT_FACTOR: f64 = 999.99;

/// Minimum number of calendar days (based on equity curve timestamp span)
/// required to report CAGR and Calmar.
/// Below this threshold these annualized metrics are misleadingly inflated.
const MIN_CALENDAR_DAYS_FOR_ANNUALIZED: f64 = 25.0;

pub(crate) const DEFAULT_METRICS: PerformanceMetrics = PerformanceMetrics {
    sharpe: 0.0,
    sortino: 0.0,
    max_drawdown: 0.0,
    win_rate: 0.0,
    profit_factor: 0.0,
    calmar: 0.0,
    var_95: 0.0,
    total_return_pct: 0.0,
    cagr: 0.0,
    avg_trade_pnl: 0.0,
    avg_winner: 0.0,
    avg_loser: 0.0,
    avg_days_held: 0.0,
    max_consecutive_losses: 0,
    expectancy: 0.0,
};

/// Trade-level metrics extracted from the trade log.
struct TradeMetrics {
    win_rate: f64,
    profit_factor: f64,
    avg_trade_pnl: f64,
    avg_winner: f64,
    avg_loser: f64,
    avg_days_held: f64,
    max_consecutive_losses: usize,
    expectancy: f64,
}

/// Calculate performance metrics from equity curve and trade log
#[allow(clippy::unnecessary_wraps, clippy::cast_precision_loss)]
pub fn calculate_metrics(
    equity_curve: &[EquityPoint],
    trade_log: &[TradeRecord],
    initial_capital: f64,
) -> Result<PerformanceMetrics> {
    if initial_capital <= 0.0 {
        return Ok(DEFAULT_METRICS);
    }

    // Trade-level metrics are always computed (even with minimal equity data)
    let tm = compute_trade_metrics(trade_log);

    // Equity-curve-derived metrics require at least 2 points
    let (sharpe, sortino, max_drawdown, var_95, total_return_pct, cagr, calmar) =
        if equity_curve.len() < 2 {
            (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        } else {
            compute_equity_metrics(equity_curve, initial_capital)
        };

    Ok(PerformanceMetrics {
        sharpe,
        sortino,
        max_drawdown,
        win_rate: tm.win_rate,
        profit_factor: tm.profit_factor,
        calmar,
        var_95,
        total_return_pct,
        cagr,
        avg_trade_pnl: tm.avg_trade_pnl,
        avg_winner: tm.avg_winner,
        avg_loser: tm.avg_loser,
        avg_days_held: tm.avg_days_held,
        max_consecutive_losses: tm.max_consecutive_losses,
        expectancy: tm.expectancy,
    })
}

/// Compute equity-curve-derived metrics (Sharpe, Sortino, max DD, `VaR`, total return, CAGR, Calmar).
/// Assumes `equity_curve.len() >= 2`.
#[allow(clippy::cast_precision_loss)]
fn compute_equity_metrics(
    equity_curve: &[EquityPoint],
    initial_capital: f64,
) -> (f64, f64, f64, f64, f64, f64, f64) {
    let mut returns = Vec::new();
    let mut prev_equity = initial_capital;
    for point in equity_curve {
        if prev_equity > 0.0 {
            returns.push((point.equity - prev_equity) / prev_equity);
        }
        prev_equity = point.equity;
    }

    if returns.is_empty() {
        return (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
    }

    let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
    let std_return = std_dev(&returns);
    let downside_std = downside_deviation(&returns);

    // Annualize (assume ~252 trading days)
    let annualization = (252.0_f64).sqrt();
    // Use actual date span from equity curve for CAGR/Calmar.
    let first_dt = equity_curve.first().unwrap().datetime;
    let last_dt = equity_curve.last().unwrap().datetime;
    let calendar_days = (last_dt - first_dt).num_days().max(0) as f64;

    let sharpe = if std_return > 0.0 {
        mean_return / std_return * annualization
    } else {
        0.0
    };

    let sortino = if downside_std > 0.0 {
        mean_return / downside_std * annualization
    } else {
        0.0
    };

    let max_drawdown = calculate_max_drawdown(equity_curve);
    let var_95 = calculate_var(&returns, 0.05);

    let final_equity = equity_curve.last().unwrap().equity;
    let total_return = (final_equity - initial_capital) / initial_capital;
    let total_return_pct = total_return * 100.0;

    let (cagr, calmar) = if calendar_days >= MIN_CALENDAR_DAYS_FOR_ANNUALIZED {
        let years = calendar_days / 365.0;
        let cagr = if final_equity > 0.0 && initial_capital > 0.0 {
            (final_equity / initial_capital).powf(1.0 / years) - 1.0
        } else {
            0.0
        };
        let calmar = if max_drawdown > 0.0 {
            cagr / max_drawdown
        } else {
            0.0
        };
        (cagr, calmar)
    } else {
        (0.0, 0.0)
    };

    (
        sharpe,
        sortino,
        max_drawdown,
        var_95,
        total_return_pct,
        cagr,
        calmar,
    )
}

#[allow(clippy::cast_precision_loss)]
fn compute_trade_metrics(trade_log: &[TradeRecord]) -> TradeMetrics {
    if trade_log.is_empty() {
        return TradeMetrics {
            win_rate: 0.0,
            profit_factor: 0.0,
            avg_trade_pnl: 0.0,
            avg_winner: 0.0,
            avg_loser: 0.0,
            avg_days_held: 0.0,
            max_consecutive_losses: 0,
            expectancy: 0.0,
        };
    }

    let total = trade_log.len() as f64;
    let mut winner_count = 0usize;
    let mut loser_count = 0usize;
    let mut winner_pnl_sum = 0.0_f64;
    let mut loser_pnl_sum = 0.0_f64;
    let mut total_pnl = 0.0_f64;
    let mut total_days = 0_i64;
    let mut current_loss_streak = 0usize;
    let mut max_loss_streak = 0usize;

    for t in trade_log {
        total_pnl += t.pnl;
        total_days += t.days_held;

        if t.pnl > 0.0 {
            winner_count += 1;
            winner_pnl_sum += t.pnl;
            current_loss_streak = 0;
        } else if t.pnl < 0.0 {
            loser_count += 1;
            loser_pnl_sum += t.pnl;
            current_loss_streak += 1;
            if current_loss_streak > max_loss_streak {
                max_loss_streak = current_loss_streak;
            }
        } else {
            // Zero-PnL (scratch) trades: neutral — don't affect win/loss or streaks
            current_loss_streak = 0;
        }
    }

    let win_rate = winner_count as f64 / total;
    let loss_rate = loser_count as f64 / total;

    let profit_factor = if loser_pnl_sum < 0.0 {
        winner_pnl_sum / loser_pnl_sum.abs()
    } else if winner_pnl_sum > 0.0 {
        MAX_PROFIT_FACTOR
    } else {
        0.0
    };

    let avg_trade_pnl = total_pnl / total;
    let avg_winner = if winner_count > 0 {
        winner_pnl_sum / winner_count as f64
    } else {
        0.0
    };
    let avg_loser = if loser_count > 0 {
        loser_pnl_sum / loser_count as f64
    } else {
        0.0
    };
    let avg_days_held = total_days as f64 / total;

    let expectancy = (win_rate * avg_winner) + (loss_rate * avg_loser);

    TradeMetrics {
        win_rate,
        profit_factor,
        avg_trade_pnl,
        avg_winner,
        avg_loser,
        avg_days_held,
        max_consecutive_losses: max_loss_streak,
        expectancy,
    }
}

#[allow(clippy::cast_precision_loss)]
fn std_dev(data: &[f64]) -> f64 {
    if data.len() < 2 {
        return 0.0;
    }
    let mean = data.iter().sum::<f64>() / data.len() as f64;
    let variance = data.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (data.len() - 1) as f64;
    variance.sqrt()
}

#[allow(clippy::cast_precision_loss)]
fn downside_deviation(returns: &[f64]) -> f64 {
    if returns.len() < 2 {
        return 0.0;
    }
    let negative_returns: Vec<f64> = returns.iter().filter(|r| **r < 0.0).copied().collect();
    if negative_returns.is_empty() {
        return 0.0;
    }
    let variance = negative_returns.iter().map(|r| r.powi(2)).sum::<f64>() / returns.len() as f64;
    variance.sqrt()
}

fn calculate_max_drawdown(equity_curve: &[EquityPoint]) -> f64 {
    let mut peak = equity_curve[0].equity;
    let mut max_dd = 0.0;

    for point in equity_curve {
        if point.equity > peak {
            peak = point.equity;
        }
        let dd = (peak - point.equity) / peak;
        if dd > max_dd {
            max_dd = dd;
        }
    }

    max_dd
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn calculate_var(returns: &[f64], confidence: f64) -> f64 {
    let mut sorted = returns.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let index = (confidence * sorted.len() as f64).floor() as usize;
    let index = index.min(sorted.len() - 1);

    -sorted[index] // VaR is typically reported as positive number
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::types::ExitType;
    use chrono::NaiveDate;

    fn make_equity_curve(values: &[f64]) -> Vec<EquityPoint> {
        values
            .iter()
            .enumerate()
            .map(|(i, &eq)| EquityPoint {
                datetime: NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    + chrono::Duration::days({
                        #[allow(clippy::cast_possible_wrap)]
                        let days = i as i64;
                        days
                    }),
                equity: eq,
            })
            .collect()
    }

    fn make_trade(pnl: f64, days_held: i64) -> TradeRecord {
        let dt = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        TradeRecord {
            trade_id: 1,
            entry_datetime: dt,
            exit_datetime: dt + chrono::Duration::days(days_held),
            entry_cost: 100.0,
            exit_proceeds: 100.0 + pnl,
            pnl,
            days_held,
            exit_type: ExitType::Expiration,
        }
    }

    #[test]
    fn single_point_returns_zeros() {
        let curve = make_equity_curve(&[10000.0]);
        let m = calculate_metrics(&curve, &[], 10000.0).unwrap();
        assert!((m.sharpe - 0.0).abs() < f64::EPSILON);
        assert!((m.max_drawdown - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn known_equity_curve_metrics() {
        let curve = make_equity_curve(&[10100.0, 10050.0, 10200.0, 10150.0, 10300.0]);
        let trades = vec![
            make_trade(100.0, 5),
            make_trade(-50.0, 3),
            make_trade(150.0, 7),
        ];
        let m = calculate_metrics(&curve, &trades, 10000.0).unwrap();

        // Trade-level win rate: 2 winners out of 3
        assert!((m.win_rate - 2.0 / 3.0).abs() < 1e-10);
        assert!(m.max_drawdown > 0.0);
        assert!(m.sharpe != 0.0);
        assert!(m.profit_factor > 1.0);
    }

    #[test]
    fn all_wins_profit_factor_capped() {
        let curve = make_equity_curve(&[10100.0, 10200.0, 10300.0]);
        let trades = vec![make_trade(100.0, 5), make_trade(200.0, 10)];
        let m = calculate_metrics(&curve, &trades, 10000.0).unwrap();
        assert!((m.win_rate - 1.0).abs() < f64::EPSILON);
        assert!((m.profit_factor - MAX_PROFIT_FACTOR).abs() < f64::EPSILON);
        assert!(m.profit_factor.is_finite());
    }

    #[test]
    fn all_losses() {
        let curve = make_equity_curve(&[10000.0, 9900.0, 9800.0, 9700.0]);
        let trades = vec![
            make_trade(-100.0, 5),
            make_trade(-100.0, 5),
            make_trade(-100.0, 5),
        ];
        let m = calculate_metrics(&curve, &trades, 10000.0).unwrap();
        assert!((m.win_rate - 0.0).abs() < f64::EPSILON);
        assert!((m.profit_factor - 0.0).abs() < f64::EPSILON);
        assert!(m.max_drawdown > 0.0);
    }

    #[test]
    fn max_drawdown_calculation() {
        // Peak at 10200, trough at 9800 → dd = 400/10200 ≈ 0.0392
        let curve = make_equity_curve(&[10000.0, 10200.0, 9800.0, 10100.0]);
        let m = calculate_metrics(&curve, &[], 10000.0).unwrap();
        let expected_dd = (10200.0 - 9800.0) / 10200.0;
        assert!((m.max_drawdown - expected_dd).abs() < 1e-10);
    }

    #[test]
    fn flat_equity_zero_std() {
        let curve = make_equity_curve(&[10000.0, 10000.0, 10000.0, 10000.0]);
        let m = calculate_metrics(&curve, &[], 10000.0).unwrap();
        assert!((m.sharpe - 0.0).abs() < f64::EPSILON); // std is 0
        assert!((m.max_drawdown - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn var_95_positive_for_losses() {
        let curve = make_equity_curve(&[
            10000.0, 9900.0, 9950.0, 9850.0, 9800.0, 9750.0, 9700.0, 9650.0, 9600.0, 9550.0,
            9500.0, 9450.0, 9400.0, 9350.0, 9300.0, 9250.0, 9200.0, 9150.0, 9100.0, 9050.0, 9000.0,
        ]);
        let m = calculate_metrics(&curve, &[], 10000.0).unwrap();
        assert!(m.var_95 > 0.0);
    }

    #[test]
    fn cagr_one_year_matches_total_return() {
        // 366 points = 365 calendar days = 1 year, so CAGR should equal total return
        let mut values = vec![10000.0];
        for i in 1..=365 {
            values.push(10000.0 + f64::from(i) * 2.76); // end at ~11008
        }
        let curve = make_equity_curve(&values);
        let m = calculate_metrics(&curve, &[], 10000.0).unwrap();
        let total_ret = (curve.last().unwrap().equity - 10000.0) / 10000.0;
        assert!(
            (m.cagr - total_ret).abs() < 1e-10,
            "CAGR {:.10} should equal total return {:.10}",
            m.cagr,
            total_ret
        );
    }

    #[test]
    fn cagr_zero_for_short_backtests() {
        // 10 trading days — below MIN_CALENDAR_DAYS_FOR_ANNUALIZED threshold
        let mut values = vec![10000.0];
        for i in 1..=10 {
            values.push(10000.0 + f64::from(i) * 50.0);
        }
        let curve = make_equity_curve(&values);
        let m = calculate_metrics(&curve, &[], 10000.0).unwrap();
        assert_eq!(m.cagr, 0.0, "CAGR should be 0 for short backtests");
        assert_eq!(m.calmar, 0.0, "Calmar should be 0 for short backtests");
        // total_return_pct should still be populated
        assert!(m.total_return_pct > 0.0);
    }

    #[test]
    fn calmar_annualized() {
        // 126 calendar days (~0.35 year, above MIN_CALENDAR_DAYS_FOR_ANNUALIZED threshold)
        let mut values = Vec::new();
        for i in 0..127 {
            // 127 points = 126 calendar day span
            values.push(10000.0 + f64::from(i) * 10.0);
        }
        // Add a dip for drawdown
        values[63] = 9500.0;
        let curve = make_equity_curve(&values);
        let m = calculate_metrics(&curve, &[], 10000.0).unwrap();
        // Calmar should be CAGR / max_drawdown
        if m.max_drawdown > 0.0 {
            assert!((m.calmar - m.cagr / m.max_drawdown).abs() < 1e-10);
        }
    }

    #[test]
    fn expectancy_calculation() {
        let trades = vec![
            make_trade(200.0, 5),
            make_trade(-100.0, 3),
            make_trade(150.0, 7),
            make_trade(-50.0, 2),
        ];
        let curve = make_equity_curve(&[10000.0, 10200.0, 10100.0, 10250.0, 10200.0]);
        let m = calculate_metrics(&curve, &trades, 10000.0).unwrap();

        // win_rate = 2/4 = 0.5, avg_winner = 175, avg_loser = -75
        assert!((m.win_rate - 0.5).abs() < 1e-10);
        assert!((m.avg_winner - 175.0).abs() < 1e-10);
        assert!((m.avg_loser - (-75.0)).abs() < 1e-10);
        // expectancy = 0.5 * 175 + 0.5 * (-75) = 50
        assert!((m.expectancy - 50.0).abs() < 1e-10);
    }

    #[test]
    fn max_consecutive_losses() {
        let trades = vec![
            make_trade(100.0, 5),
            make_trade(-50.0, 3),
            make_trade(-30.0, 2),
            make_trade(-20.0, 1),
            make_trade(80.0, 4),
            make_trade(-10.0, 1),
        ];
        let curve = make_equity_curve(&[10000.0, 10100.0]);
        let m = calculate_metrics(&curve, &trades, 10000.0).unwrap();
        assert_eq!(m.max_consecutive_losses, 3);
    }

    #[test]
    fn zero_pnl_trades_are_neutral() {
        let trades = vec![
            make_trade(-50.0, 3),
            make_trade(0.0, 1), // scratch — should break loss streak
            make_trade(-30.0, 2),
        ];
        let curve = make_equity_curve(&[10000.0, 9920.0]);
        let m = calculate_metrics(&curve, &trades, 10000.0).unwrap();
        // Zero-PnL breaks the streak, so max consecutive losses is 1, not 2
        assert_eq!(m.max_consecutive_losses, 1);
        // 0 winners, 2 losers, 1 scratch — win_rate = 0/3
        assert_eq!(m.win_rate, 0.0);
        // avg_loser should only include actual losers
        assert!((m.avg_loser - (-40.0)).abs() < 1e-10); // (-50 + -30) / 2
    }

    #[test]
    fn avg_days_held() {
        let trades = vec![
            make_trade(100.0, 10),
            make_trade(-50.0, 20),
            make_trade(75.0, 30),
        ];
        let curve = make_equity_curve(&[10000.0, 10100.0]);
        let m = calculate_metrics(&curve, &trades, 10000.0).unwrap();
        assert!((m.avg_days_held - 20.0).abs() < 1e-10);
    }

    #[test]
    fn total_return_pct() {
        let curve = make_equity_curve(&[10000.0, 10500.0, 11000.0]);
        let m = calculate_metrics(&curve, &[], 10000.0).unwrap();
        assert!((m.total_return_pct - 10.0).abs() < 1e-10);
    }
}
