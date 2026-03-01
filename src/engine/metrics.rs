use anyhow::Result;

use super::types::{EquityPoint, PerformanceMetrics};

/// Calculate performance metrics from equity curve
#[allow(clippy::unnecessary_wraps, clippy::cast_precision_loss)]
pub fn calculate_metrics(
    equity_curve: &[EquityPoint],
    initial_capital: f64,
) -> Result<PerformanceMetrics> {
    if equity_curve.len() < 2 {
        return Ok(PerformanceMetrics {
            sharpe: 0.0,
            sortino: 0.0,
            max_drawdown: 0.0,
            win_rate: 0.0,
            profit_factor: 0.0,
            calmar: 0.0,
            var_95: 0.0,
        });
    }

    // Calculate returns
    let mut returns = Vec::new();
    let mut prev_equity = initial_capital;
    for point in equity_curve {
        if prev_equity > 0.0 {
            returns.push((point.equity - prev_equity) / prev_equity);
        }
        prev_equity = point.equity;
    }

    if returns.is_empty() {
        return Ok(PerformanceMetrics {
            sharpe: 0.0,
            sortino: 0.0,
            max_drawdown: 0.0,
            win_rate: 0.0,
            profit_factor: 0.0,
            calmar: 0.0,
            var_95: 0.0,
        });
    }

    let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
    let std_return = std_dev(&returns);
    let downside_std = downside_deviation(&returns);

    // Annualize (assume ~252 trading days)
    let annualization = (252.0_f64).sqrt();

    // Sharpe ratio
    let sharpe = if std_return > 0.0 {
        mean_return / std_return * annualization
    } else {
        0.0
    };

    // Sortino ratio
    let sortino = if downside_std > 0.0 {
        mean_return / downside_std * annualization
    } else {
        0.0
    };

    // Max drawdown
    let max_drawdown = calculate_max_drawdown(equity_curve);

    // Win rate
    let wins = returns.iter().filter(|r| **r > 0.0).count();
    let win_rate = wins as f64 / returns.len() as f64;

    // Profit factor
    let total_gains: f64 = returns.iter().filter(|r| **r > 0.0).sum();
    let total_losses: f64 = returns.iter().filter(|r| **r < 0.0).map(|r| r.abs()).sum();
    let profit_factor = if total_losses > 0.0 {
        total_gains / total_losses
    } else if total_gains > 0.0 {
        f64::INFINITY
    } else {
        0.0
    };

    // Calmar ratio (annualized return / max drawdown)
    let total_return = (equity_curve.last().unwrap().equity - initial_capital) / initial_capital;
    let calmar = if max_drawdown > 0.0 {
        total_return / max_drawdown
    } else {
        0.0
    };

    // VaR 95%
    let var_95 = calculate_var(&returns, 0.05);

    Ok(PerformanceMetrics {
        sharpe,
        sortino,
        max_drawdown,
        win_rate,
        profit_factor,
        calmar,
        var_95,
    })
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

    #[test]
    fn single_point_returns_zeros() {
        let curve = make_equity_curve(&[10000.0]);
        let m = calculate_metrics(&curve, 10000.0).unwrap();
        assert_eq!(m.sharpe, 0.0);
        assert_eq!(m.max_drawdown, 0.0);
    }

    #[test]
    fn known_equity_curve_metrics() {
        // Returns include first point vs initial_capital (0.0 return), then subsequent diffs
        // Use a curve where first point differs from initial capital
        let curve = make_equity_curve(&[10100.0, 10050.0, 10200.0, 10150.0, 10300.0]);
        let m = calculate_metrics(&curve, 10000.0).unwrap();

        // Returns: +0.01, -0.00495, +0.01493, -0.00490, +0.01478
        // 3 wins out of 5 = 0.6
        assert!((m.win_rate - 0.6).abs() < 1e-10);
        assert!(m.max_drawdown > 0.0);
        assert!(m.sharpe != 0.0);
        assert!(m.profit_factor > 1.0);
    }

    #[test]
    fn all_wins_profit_factor_infinite() {
        // Start curve above initial capital so all returns are positive
        let curve = make_equity_curve(&[10100.0, 10200.0, 10300.0]);
        let m = calculate_metrics(&curve, 10000.0).unwrap();
        assert_eq!(m.win_rate, 1.0);
        assert!(m.profit_factor.is_infinite());
    }

    #[test]
    fn all_losses() {
        let curve = make_equity_curve(&[10000.0, 9900.0, 9800.0, 9700.0]);
        let m = calculate_metrics(&curve, 10000.0).unwrap();
        assert_eq!(m.win_rate, 0.0);
        assert_eq!(m.profit_factor, 0.0);
        assert!(m.max_drawdown > 0.0);
    }

    #[test]
    fn max_drawdown_calculation() {
        // Peak at 10200, trough at 9800 → dd = 400/10200 ≈ 0.0392
        let curve = make_equity_curve(&[10000.0, 10200.0, 9800.0, 10100.0]);
        let m = calculate_metrics(&curve, 10000.0).unwrap();
        let expected_dd = (10200.0 - 9800.0) / 10200.0;
        assert!((m.max_drawdown - expected_dd).abs() < 1e-10);
    }

    #[test]
    fn flat_equity_zero_std() {
        let curve = make_equity_curve(&[10000.0, 10000.0, 10000.0, 10000.0]);
        let m = calculate_metrics(&curve, 10000.0).unwrap();
        assert_eq!(m.sharpe, 0.0); // std is 0
        assert_eq!(m.max_drawdown, 0.0);
    }

    #[test]
    fn var_95_positive_for_losses() {
        let curve = make_equity_curve(&[
            10000.0, 9900.0, 9950.0, 9850.0, 9800.0, 9750.0, 9700.0, 9650.0, 9600.0, 9550.0,
            9500.0, 9450.0, 9400.0, 9350.0, 9300.0, 9250.0, 9200.0, 9150.0, 9100.0, 9050.0, 9000.0,
        ]);
        let m = calculate_metrics(&curve, 10000.0).unwrap();
        assert!(m.var_95 > 0.0); // VaR is positive for losses
    }
}
