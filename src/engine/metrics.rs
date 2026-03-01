use anyhow::Result;

use super::types::{EquityPoint, PerformanceMetrics, TradeRecord};

const DEFAULT_METRICS: PerformanceMetrics = PerformanceMetrics {
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

/// Calculate performance metrics from equity curve and trade log
#[allow(clippy::unnecessary_wraps, clippy::cast_precision_loss)]
pub fn calculate_metrics(
    equity_curve: &[EquityPoint],
    trade_log: &[TradeRecord],
    initial_capital: f64,
) -> Result<PerformanceMetrics> {
    if equity_curve.len() < 2 {
        return Ok(DEFAULT_METRICS);
    }

    // Calculate daily returns from equity curve
    let mut returns = Vec::new();
    let mut prev_equity = initial_capital;
    for point in equity_curve {
        if prev_equity > 0.0 {
            returns.push((point.equity - prev_equity) / prev_equity);
        }
        prev_equity = point.equity;
    }

    if returns.is_empty() {
        return Ok(DEFAULT_METRICS);
    }

    let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
    let std_return = std_dev(&returns);
    let downside_std = downside_deviation(&returns);

    // Annualize (assume ~252 trading days)
    let annualization = (252.0_f64).sqrt();
    let num_trading_days = equity_curve.len() as f64;

    // Sharpe ratio (from equity curve)
    let sharpe = if std_return > 0.0 {
        mean_return / std_return * annualization
    } else {
        0.0
    };

    // Sortino ratio (from equity curve)
    let sortino = if downside_std > 0.0 {
        mean_return / downside_std * annualization
    } else {
        0.0
    };

    // Max drawdown (from equity curve)
    let max_drawdown = calculate_max_drawdown(equity_curve);

    // VaR 95% (from equity curve)
    let var_95 = calculate_var(&returns, 0.05);

    // Total return
    let final_equity = equity_curve.last().unwrap().equity;
    let total_return = (final_equity - initial_capital) / initial_capital;
    let total_return_pct = total_return * 100.0;

    // CAGR: annualized compound return
    let years = num_trading_days / 252.0;
    let cagr = if years > 0.0 && final_equity > 0.0 && initial_capital > 0.0 {
        (final_equity / initial_capital).powf(1.0 / years) - 1.0
    } else {
        0.0
    };

    // Calmar ratio: annualized return / max drawdown
    let annualized_return = cagr;
    let calmar = if max_drawdown > 0.0 {
        annualized_return / max_drawdown
    } else {
        0.0
    };

    // Trade-level metrics
    let (win_rate, profit_factor, avg_trade_pnl, avg_winner, avg_loser, avg_days_held, max_consecutive_losses, expectancy) =
        compute_trade_metrics(trade_log);

    Ok(PerformanceMetrics {
        sharpe,
        sortino,
        max_drawdown,
        win_rate,
        profit_factor,
        calmar,
        var_95,
        total_return_pct,
        cagr,
        avg_trade_pnl,
        avg_winner,
        avg_loser,
        avg_days_held,
        max_consecutive_losses,
        expectancy,
    })
}

#[allow(clippy::cast_precision_loss)]
fn compute_trade_metrics(trade_log: &[TradeRecord]) -> (f64, f64, f64, f64, f64, f64, usize, f64) {
    if trade_log.is_empty() {
        return (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, 0.0);
    }

    let total = trade_log.len() as f64;
    let mut winner_count = 0usize;
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
        } else {
            loser_pnl_sum += t.pnl; // negative value
            current_loss_streak += 1;
            if current_loss_streak > max_loss_streak {
                max_loss_streak = current_loss_streak;
            }
        }
    }

    let loser_count = trade_log.len() - winner_count;
    let win_rate = winner_count as f64 / total;
    let loss_rate = 1.0 - win_rate;

    let profit_factor = if loser_pnl_sum < 0.0 {
        winner_pnl_sum / loser_pnl_sum.abs()
    } else if winner_pnl_sum > 0.0 {
        f64::INFINITY
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

    (
        win_rate,
        profit_factor,
        avg_trade_pnl,
        avg_winner,
        avg_loser,
        avg_days_held,
        max_loss_streak,
        expectancy,
    )
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
                    + chrono::Duration::days(i as i64),
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
        assert_eq!(m.sharpe, 0.0);
        assert_eq!(m.max_drawdown, 0.0);
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
    fn all_wins_profit_factor_infinite() {
        let curve = make_equity_curve(&[10100.0, 10200.0, 10300.0]);
        let trades = vec![make_trade(100.0, 5), make_trade(200.0, 10)];
        let m = calculate_metrics(&curve, &trades, 10000.0).unwrap();
        assert_eq!(m.win_rate, 1.0);
        assert!(m.profit_factor.is_infinite());
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
        assert_eq!(m.win_rate, 0.0);
        assert_eq!(m.profit_factor, 0.0);
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
        assert_eq!(m.sharpe, 0.0); // std is 0
        assert_eq!(m.max_drawdown, 0.0);
    }

    #[test]
    fn var_95_positive_for_losses() {
        let curve = make_equity_curve(&[
            10000.0, 9900.0, 9950.0, 9850.0, 9800.0, 9750.0, 9700.0, 9650.0, 9600.0, 9550.0,
            9500.0, 9450.0, 9400.0, 9350.0, 9300.0, 9250.0, 9200.0, 9150.0, 9100.0, 9050.0,
            9000.0,
        ]);
        let m = calculate_metrics(&curve, &[], 10000.0).unwrap();
        assert!(m.var_95 > 0.0);
    }

    #[test]
    fn cagr_one_year_matches_total_return() {
        // 252 trading days = 1 year, so CAGR should equal total return
        let mut values = vec![10000.0];
        for i in 1..=252 {
            values.push(10000.0 + i as f64 * 4.0); // end at ~11008
        }
        let curve = make_equity_curve(&values);
        let m = calculate_metrics(&curve, &[], 10000.0).unwrap();
        let total_ret = (curve.last().unwrap().equity - 10000.0) / 10000.0;
        assert!((m.cagr - total_ret).abs() < 0.01);
    }

    #[test]
    fn calmar_annualized() {
        // 126 trading days = ~0.5 year
        let mut values = Vec::new();
        for i in 0..126 {
            values.push(10000.0 + i as f64 * 10.0);
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
