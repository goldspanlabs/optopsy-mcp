//! Performance metric calculations from equity curves and trade logs.
//!
//! Computes Sharpe, Sortino, CAGR, Calmar, `VaR`, max drawdown, win rate,
//! profit factor, expectancy, and other risk/return statistics.

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

/// Calculate performance metrics from equity curve and trade log.
///
/// `bars_per_year` controls the annualization factor (e.g. 252 for daily, 252×390 for 1-min).
#[allow(clippy::unnecessary_wraps)]
pub fn calculate_metrics(
    equity_curve: &[EquityPoint],
    trade_log: &[TradeRecord],
    initial_capital: f64,
    bars_per_year: f64,
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
            compute_equity_metrics(equity_curve, initial_capital, bars_per_year)
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
fn compute_equity_metrics(
    equity_curve: &[EquityPoint],
    initial_capital: f64,
    bars_per_year: f64,
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

    let annualization = bars_per_year.sqrt();
    // Use actual date span from equity curve for CAGR/Calmar.
    let first_dt = equity_curve
        .first()
        .expect("equity_curve guaranteed non-empty by caller guard")
        .datetime;
    let last_dt = equity_curve
        .last()
        .expect("equity_curve guaranteed non-empty by caller guard")
        .datetime;
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

    let max_drawdown = calculate_max_drawdown(equity_curve, initial_capital);
    let var_95 = calculate_var(&returns, 0.05);

    let final_equity = equity_curve
        .last()
        .expect("equity_curve guaranteed non-empty by caller guard")
        .equity;
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

fn std_dev(data: &[f64]) -> f64 {
    if data.len() < 2 {
        return 0.0;
    }
    let mean = data.iter().sum::<f64>() / data.len() as f64;
    let variance = data.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (data.len() - 1) as f64;
    variance.sqrt()
}

fn downside_deviation(returns: &[f64]) -> f64 {
    if returns.len() < 2 {
        return 0.0;
    }
    let negative_returns: Vec<f64> = returns.iter().filter(|r| **r < 0.0).copied().collect();
    if negative_returns.is_empty() {
        return 0.0;
    }
    let variance =
        negative_returns.iter().map(|r| r.powi(2)).sum::<f64>() / (returns.len() - 1) as f64;
    variance.sqrt()
}

fn calculate_max_drawdown(equity_curve: &[EquityPoint], initial_capital: f64) -> f64 {
    let mut peak = initial_capital;
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
    if returns.is_empty() {
        return 0.0;
    }
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
        TradeRecord::new(
            1,
            dt,
            dt + chrono::Duration::days(days_held),
            100.0,
            100.0 + pnl,
            pnl,
            days_held,
            ExitType::Expiration,
            vec![],
        )
    }

    #[test]
    fn single_point_returns_zeros() {
        let curve = make_equity_curve(&[10000.0]);
        let m = calculate_metrics(&curve, &[], 10000.0, 252.0).unwrap();
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
        let m = calculate_metrics(&curve, &trades, 10000.0, 252.0).unwrap();

        // Trade-level win rate: 2 winners out of 3
        assert!((m.win_rate - 2.0 / 3.0).abs() < 1e-10);
        assert!(m.max_drawdown > 0.0);
        assert!(m.sharpe != 0.0);
        assert!(m.profit_factor > 1.0);
    }

    #[test]
    fn sharpe_exact_value() {
        // Capital=10000, equity=[11000, 9900, 10890] → returns [0.1, -0.1, 0.1]
        // mean = 1/30, std (sample) = 1/sqrt(75)
        // sharpe = mean/std * sqrt(252) = sqrt(21) ≈ 4.58257569...
        let curve = make_equity_curve(&[11000.0, 9900.0, 10890.0]);
        let m = calculate_metrics(&curve, &[], 10000.0, 252.0).unwrap();
        let expected_sharpe = 21.0_f64.sqrt();
        assert!(
            (m.sharpe - expected_sharpe).abs() < 1e-10,
            "Sharpe {:.12} should equal sqrt(21) = {:.12}",
            m.sharpe,
            expected_sharpe
        );
    }

    #[test]
    fn sortino_exact_value() {
        // Same curve: returns [0.1, -0.1, 0.1]
        // negative returns: [-0.1], downside_dev = sqrt(0.01/(3-1)) = sqrt(0.005)
        // sortino = mean/downside_dev * sqrt(252) = (1/30)/sqrt(0.005)*sqrt(252) = sqrt(56)
        let curve = make_equity_curve(&[11000.0, 9900.0, 10890.0]);
        let m = calculate_metrics(&curve, &[], 10000.0, 252.0).unwrap();
        let expected_sortino = 56.0_f64.sqrt();
        assert!(
            (m.sortino - expected_sortino).abs() < 1e-10,
            "Sortino {:.12} should equal sqrt(56) = {:.12}",
            m.sortino,
            expected_sortino
        );
    }

    #[test]
    fn var_95_exact_value() {
        // Returns [0.1, -0.1, 0.1], sorted: [-0.1, 0.1, 0.1]
        // VaR index = floor(0.05 * 3) = 0 → sorted[0] = -0.1 → VaR = 0.1
        let curve = make_equity_curve(&[11000.0, 9900.0, 10890.0]);
        let m = calculate_metrics(&curve, &[], 10000.0, 252.0).unwrap();
        assert!(
            (m.var_95 - 0.1).abs() < 1e-10,
            "VaR 95% {:.12} should equal 0.1",
            m.var_95
        );
    }

    #[test]
    fn sortino_zero_when_no_negative_returns() {
        // All positive returns → downside deviation is 0 → Sortino is 0
        let curve = make_equity_curve(&[10100.0, 10200.0, 10300.0]);
        let m = calculate_metrics(&curve, &[], 10000.0, 252.0).unwrap();
        assert!(m.sharpe > 0.0, "Sharpe should be positive");
        assert!(
            (m.sortino - 0.0).abs() < f64::EPSILON,
            "Sortino should be 0 when no negative returns, got {}",
            m.sortino
        );
    }

    #[test]
    fn all_wins_profit_factor_capped() {
        let curve = make_equity_curve(&[10100.0, 10200.0, 10300.0]);
        let trades = vec![make_trade(100.0, 5), make_trade(200.0, 10)];
        let m = calculate_metrics(&curve, &trades, 10000.0, 252.0).unwrap();
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
        let m = calculate_metrics(&curve, &trades, 10000.0, 252.0).unwrap();
        assert!((m.win_rate - 0.0).abs() < f64::EPSILON);
        assert!((m.profit_factor - 0.0).abs() < f64::EPSILON);
        assert!(m.max_drawdown > 0.0);
    }

    #[test]
    fn max_drawdown_calculation() {
        // Peak at 10200, trough at 9800 → dd = 400/10200 ≈ 0.0392
        let curve = make_equity_curve(&[10000.0, 10200.0, 9800.0, 10100.0]);
        let m = calculate_metrics(&curve, &[], 10000.0, 252.0).unwrap();
        let expected_dd = (10200.0 - 9800.0) / 10200.0;
        assert!((m.max_drawdown - expected_dd).abs() < 1e-10);
    }

    #[test]
    fn flat_equity_zero_std() {
        let curve = make_equity_curve(&[10000.0, 10000.0, 10000.0, 10000.0]);
        let m = calculate_metrics(&curve, &[], 10000.0, 252.0).unwrap();
        assert!((m.sharpe - 0.0).abs() < f64::EPSILON); // std is 0
        assert!((m.max_drawdown - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn var_95_positive_for_losses() {
        let curve = make_equity_curve(&[
            10000.0, 9900.0, 9950.0, 9850.0, 9800.0, 9750.0, 9700.0, 9650.0, 9600.0, 9550.0,
            9500.0, 9450.0, 9400.0, 9350.0, 9300.0, 9250.0, 9200.0, 9150.0, 9100.0, 9050.0, 9000.0,
        ]);
        let m = calculate_metrics(&curve, &[], 10000.0, 252.0).unwrap();
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
        let m = calculate_metrics(&curve, &[], 10000.0, 252.0).unwrap();
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
        let m = calculate_metrics(&curve, &[], 10000.0, 252.0).unwrap();
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
        let m = calculate_metrics(&curve, &[], 10000.0, 252.0).unwrap();
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
        let m = calculate_metrics(&curve, &trades, 10000.0, 252.0).unwrap();

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
        let m = calculate_metrics(&curve, &trades, 10000.0, 252.0).unwrap();
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
        let m = calculate_metrics(&curve, &trades, 10000.0, 252.0).unwrap();
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
        let m = calculate_metrics(&curve, &trades, 10000.0, 252.0).unwrap();
        assert!((m.avg_days_held - 20.0).abs() < 1e-10);
    }

    #[test]
    fn total_return_pct() {
        let curve = make_equity_curve(&[10000.0, 10500.0, 11000.0]);
        let m = calculate_metrics(&curve, &[], 10000.0, 252.0).unwrap();
        assert!((m.total_return_pct - 10.0).abs() < 1e-10);
    }

    // ── Intraday annualization tests ────────────────────────────────────

    #[test]
    fn intraday_sharpe_scales_with_bars_per_year() {
        // Same equity curve, different bars_per_year: Sharpe should scale by sqrt ratio
        let curve = make_equity_curve(&[11000.0, 9900.0, 10890.0]);

        let m_daily = calculate_metrics(&curve, &[], 10000.0, 252.0).unwrap();
        let m_5min = calculate_metrics(&curve, &[], 10000.0, 252.0 * 78.0).unwrap();

        // Sharpe = mean/std * sqrt(bars_per_year)
        // Ratio should be sqrt(252*78) / sqrt(252) = sqrt(78)
        let expected_ratio = 78.0_f64.sqrt();
        if m_daily.sharpe.abs() > 1e-10 {
            let actual_ratio = m_5min.sharpe / m_daily.sharpe;
            assert!(
                (actual_ratio - expected_ratio).abs() < 0.01,
                "Sharpe ratio scaling wrong: expected {expected_ratio}, got {actual_ratio}"
            );
        }
    }

    #[test]
    fn intraday_sortino_scales_with_bars_per_year() {
        // Curve with a downside move to trigger non-zero Sortino
        let curve = make_equity_curve(&[11000.0, 9900.0, 10890.0]);

        let m_daily = calculate_metrics(&curve, &[], 10000.0, 252.0).unwrap();
        let m_hourly = calculate_metrics(&curve, &[], 10000.0, 252.0 * 7.0).unwrap();

        // Sortino = mean/downside_dev * sqrt(bars_per_year)
        let expected_ratio = 7.0_f64.sqrt();
        if m_daily.sortino.abs() > 1e-10 {
            let actual_ratio = m_hourly.sortino / m_daily.sortino;
            assert!(
                (actual_ratio - expected_ratio).abs() < 0.01,
                "Sortino ratio scaling wrong: expected {expected_ratio}, got {actual_ratio}"
            );
        }
    }

    #[test]
    fn bars_per_year_values_correct() {
        use crate::engine::types::Interval;
        assert!((Interval::Daily.bars_per_year() - 252.0).abs() < f64::EPSILON);
        assert!((Interval::Weekly.bars_per_year() - 52.0).abs() < f64::EPSILON);
        assert!((Interval::Monthly.bars_per_year() - 12.0).abs() < f64::EPSILON);
        assert!((Interval::Min1.bars_per_year() - 252.0 * 390.0).abs() < f64::EPSILON);
        assert!((Interval::Min5.bars_per_year() - 252.0 * 78.0).abs() < f64::EPSILON);
        assert!((Interval::Min10.bars_per_year() - 252.0 * 39.0).abs() < f64::EPSILON);
        assert!((Interval::Min15.bars_per_year() - 252.0 * 26.0).abs() < f64::EPSILON);
        assert!((Interval::Min30.bars_per_year() - 252.0 * 13.0).abs() < f64::EPSILON);
        assert!((Interval::Hour1.bars_per_year() - 252.0 * 7.0).abs() < f64::EPSILON);
        assert!((Interval::Hour4.bars_per_year() - 252.0 * 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn metrics_exact_values_from_known_curve() {
        // ── Hand-crafted equity curve ──
        // initial_capital = 10000
        // equity points: [10000, 10500, 9800, 10200, 10600]
        //
        // max_drawdown calculation (calculate_max_drawdown starts peak at initial_capital):
        //   point 10000: peak=10000, dd=(10000-10000)/10000 = 0
        //   point 10500: peak=10500, dd=(10500-10500)/10500 = 0
        //   point  9800: peak=10500, dd=(10500-9800)/10500 = 700/10500 = 0.066666...
        //   point 10200: peak=10500, dd=(10500-10200)/10500 = 300/10500 = 0.028571...
        //   point 10600: peak=10600, dd=0
        // max_drawdown = 700/10500 = 2/30 = 1/15
        let curve = make_equity_curve(&[10000.0, 10500.0, 9800.0, 10200.0, 10600.0]);

        // ── Hand-crafted trades ──
        // Winners: 200, 300  (sum = 500)
        // Losers: -100, -50  (sum = -150, abs = 150)
        // profit_factor = 500 / 150 = 10/3 = 3.333...
        //
        // win_rate = 2/4 = 0.5
        // avg_winner = 500/2 = 250
        // avg_loser = -150/2 = -75
        // avg_trade_pnl = (200+300-100-50)/4 = 350/4 = 87.5
        // expectancy = 0.5 * 250 + 0.5 * (-75) = 125 - 37.5 = 87.5
        let trades = vec![
            make_trade(200.0, 5),
            make_trade(300.0, 7),
            make_trade(-100.0, 3),
            make_trade(-50.0, 2),
        ];
        let m = calculate_metrics(&curve, &trades, 10000.0, 252.0).unwrap();

        // max_drawdown = 700/10500 = 1/15
        let expected_max_dd = 700.0 / 10500.0;
        assert!(
            (m.max_drawdown - expected_max_dd).abs() < 1e-10,
            "max_drawdown: expected {expected_max_dd}, got {}",
            m.max_drawdown
        );

        // profit_factor = 500/150 = 10/3
        let expected_pf = 500.0 / 150.0;
        assert!(
            (m.profit_factor - expected_pf).abs() < 1e-10,
            "profit_factor: expected {expected_pf}, got {}",
            m.profit_factor
        );

        // win_rate = 0.5
        assert!(
            (m.win_rate - 0.5).abs() < 1e-10,
            "win_rate: expected 0.5, got {}",
            m.win_rate
        );

        // avg_winner = 250
        assert!(
            (m.avg_winner - 250.0).abs() < 1e-10,
            "avg_winner: expected 250, got {}",
            m.avg_winner
        );

        // avg_loser = -75
        assert!(
            (m.avg_loser - (-75.0)).abs() < 1e-10,
            "avg_loser: expected -75, got {}",
            m.avg_loser
        );

        // expectancy = 87.5
        assert!(
            (m.expectancy - 87.5).abs() < 1e-10,
            "expectancy: expected 87.5, got {}",
            m.expectancy
        );

        // Calmar = CAGR / max_drawdown
        // Equity curve spans 4 calendar days (indices 0..4), which is < MIN_CALENDAR_DAYS_FOR_ANNUALIZED (25)
        // so calmar should be 0.0
        assert!(
            (m.calmar - 0.0).abs() < 1e-10,
            "calmar: expected 0.0 (too few days), got {}",
            m.calmar
        );
    }

    #[test]
    fn var_95_exact_from_known_returns() {
        // ── Build 21-point equity curve so we get exactly 20 returns ──
        // (returns are computed from initial_capital through each equity point)
        //
        // We want the sorted returns to have a known worst value.
        // Start at 10000, then:
        //   point[0] = 10000 → return = (10000-10000)/10000 = 0.0
        //   point[1] = 9700  → return = (9700-10000)/10000 = -0.03  (this is our worst)
        //   point[2] = 9894  → return = (9894-9700)/9700 = 0.02
        //   ...remaining points: small positive returns from 9894 upward
        //
        // Actually, the code computes returns[i] = (equity[i] - prev) / prev,
        // starting with prev = initial_capital. So returns has equity_curve.len() elements
        // (one per equity point, since prev starts at initial_capital).
        //
        // For 21 points we get 21 returns.
        // VaR index = floor(0.05 * 21) = floor(1.05) = 1
        // So VaR = -sorted[1] (the 2nd worst return, 0-indexed)
        //
        // Let's craft it so sorted returns are:
        //   [-0.05, -0.03, 0.01, 0.01, 0.01, ... 0.01]
        //   VaR = -sorted[1] = -(-0.03) = 0.03
        //
        // Build the curve:
        //   initial_capital = 10000
        //   eq[0] = 9500  → ret = (9500-10000)/10000 = -0.05
        //   eq[1] = 9215  → ret = (9215-9500)/9500 = -0.03
        //   eq[2..20]: each +1% from previous
        let mut values = Vec::with_capacity(21);
        values.push(9500.0); // ret = -0.05
        values.push(9500.0 * 0.97); // ret = -0.03 → 9215.0
        let mut prev = values[1];
        for _ in 2..21 {
            let next = prev * 1.01;
            values.push(next);
            prev = next;
        }
        let curve = make_equity_curve(&values);
        let m = calculate_metrics(&curve, &[], 10000.0, 252.0).unwrap();

        // Verify: 21 returns, sorted[0] ~ -0.05, sorted[1] = -0.03
        // VaR index = floor(0.05 * 21) = 1, VaR = -sorted[1] = 0.03
        assert!(
            (m.var_95 - 0.03).abs() < 1e-10,
            "VaR 95%: expected 0.03, got {}",
            m.var_95
        );
    }

    #[test]
    fn is_intraday_correct_for_all_intervals() {
        use crate::engine::types::Interval;
        assert!(!Interval::Daily.is_intraday());
        assert!(!Interval::Weekly.is_intraday());
        assert!(!Interval::Monthly.is_intraday());
        assert!(Interval::Min1.is_intraday());
        assert!(Interval::Min5.is_intraday());
        assert!(Interval::Min10.is_intraday());
        assert!(Interval::Min15.is_intraday());
        assert!(Interval::Min30.is_intraday());
        assert!(Interval::Hour1.is_intraday());
        assert!(Interval::Hour4.is_intraday());
    }
}
