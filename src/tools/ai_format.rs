use std::collections::HashMap;

use crate::engine::types::{
    BacktestParams, BacktestResult, CompareResult, EquityPoint, ExitType, EvaluateParams,
    GroupStats, TradeRecord,
};

use super::response_types::{
    BacktestResponse, CompareResponse, DateRange, EquityCurveSummary, EvaluateResponse,
    LoadDataResponse, StrategiesResponse, StrategyInfo, TradeStat, TradeSummary,
};

fn assess_sharpe(sharpe: f64) -> &'static str {
    if sharpe >= 1.5 {
        "excellent"
    } else if sharpe >= 1.0 {
        "strong"
    } else if sharpe >= 0.5 {
        "moderate"
    } else if sharpe >= 0.0 {
        "weak"
    } else {
        "poor"
    }
}

fn format_pnl(value: f64) -> String {
    if value >= 0.0 {
        format!("+${value:.2}")
    } else {
        format!("-${:.2}", value.abs())
    }
}

fn exit_type_name(exit_type: &ExitType) -> &'static str {
    match exit_type {
        ExitType::Expiration => "Expiration",
        ExitType::StopLoss => "StopLoss",
        ExitType::TakeProfit => "TakeProfit",
        ExitType::MaxHold => "MaxHold",
        ExitType::DteExit => "DteExit",
        ExitType::Adjustment => "Adjustment",
    }
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn sample_equity_curve(curve: &[EquityPoint], max_points: usize) -> Vec<EquityPoint> {
    if curve.len() <= max_points {
        return curve.to_vec();
    }
    let step = (curve.len() - 1) as f64 / (max_points - 1) as f64;
    (0..max_points)
        .map(|i| {
            let idx = (i as f64 * step).round() as usize;
            curve[idx.min(curve.len() - 1)].clone()
        })
        .collect()
}

#[allow(clippy::cast_precision_loss)]
fn compute_trade_summary(trade_log: &[TradeRecord]) -> TradeSummary {
    let total = trade_log.len();
    let winners: Vec<&TradeRecord> = trade_log.iter().filter(|t| t.pnl > 0.0).collect();
    let losers: Vec<&TradeRecord> = trade_log.iter().filter(|t| t.pnl <= 0.0).collect();

    let avg_pnl = if total > 0 {
        trade_log.iter().map(|t| t.pnl).sum::<f64>() / total as f64
    } else {
        0.0
    };

    let avg_winner = if winners.is_empty() {
        0.0
    } else {
        winners.iter().map(|t| t.pnl).sum::<f64>() / winners.len() as f64
    };

    let avg_loser = if losers.is_empty() {
        0.0
    } else {
        losers.iter().map(|t| t.pnl).sum::<f64>() / losers.len() as f64
    };

    let avg_days_held = if total > 0 {
        trade_log.iter().map(|t| t.days_held as f64).sum::<f64>() / total as f64
    } else {
        0.0
    };

    let mut exit_breakdown: HashMap<String, usize> = HashMap::new();
    for t in trade_log {
        *exit_breakdown
            .entry(exit_type_name(&t.exit_type).to_string())
            .or_default() += 1;
    }

    let best = trade_log
        .iter()
        .max_by(|a, b| a.pnl.partial_cmp(&b.pnl).unwrap_or(std::cmp::Ordering::Equal));
    let worst = trade_log
        .iter()
        .min_by(|a, b| a.pnl.partial_cmp(&b.pnl).unwrap_or(std::cmp::Ordering::Equal));

    let best_trade = best.map_or(
        TradeStat {
            pnl: 0.0,
            date: String::new(),
        },
        |t| TradeStat {
            pnl: t.pnl,
            date: t.entry_datetime.format("%Y-%m-%d").to_string(),
        },
    );

    let worst_trade = worst.map_or(
        TradeStat {
            pnl: 0.0,
            date: String::new(),
        },
        |t| TradeStat {
            pnl: t.pnl,
            date: t.entry_datetime.format("%Y-%m-%d").to_string(),
        },
    );

    TradeSummary {
        total,
        winners: winners.len(),
        losers: losers.len(),
        avg_pnl,
        avg_winner,
        avg_loser,
        avg_days_held,
        exit_breakdown,
        best_trade,
        worst_trade,
    }
}

fn compute_equity_summary(curve: &[EquityPoint], capital: f64) -> EquityCurveSummary {
    let start_equity = if curve.is_empty() {
        capital
    } else {
        curve[0].equity
    };
    let end_equity = curve.last().map_or(capital, |p| p.equity);
    let peak_equity = curve
        .iter()
        .map(|p| p.equity)
        .fold(capital, f64::max);
    let trough_equity = curve
        .iter()
        .map(|p| p.equity)
        .fold(capital, f64::min);
    let total_return_pct = if capital > 0.0 {
        (end_equity - capital) / capital * 100.0
    } else {
        0.0
    };

    EquityCurveSummary {
        start_equity,
        end_equity,
        total_return_pct,
        peak_equity,
        trough_equity,
        num_points: curve.len(),
        sampled_curve: sample_equity_curve(curve, 50),
    }
}

fn most_common_exit(trade_log: &[TradeRecord]) -> String {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for t in trade_log {
        *counts.entry(exit_type_name(&t.exit_type)).or_default() += 1;
    }
    counts
        .into_iter()
        .max_by_key(|(_, c)| *c)
        .map_or_else(|| "N/A".to_string(), |(name, _)| name.to_string())
}

pub fn format_backtest(result: BacktestResult, params: &BacktestParams) -> BacktestResponse {
    let m = &result.metrics;
    let assessment = assess_sharpe(m.sharpe);
    let return_pct = if params.capital > 0.0 {
        result.total_pnl / params.capital * 100.0
    } else {
        0.0
    };

    let summary = format!(
        "Backtest of {}: {} trades. Net P&L: {} ({:.1}% return on ${:.0}). \
         Sharpe {:.2} indicates {} risk-adjusted returns.",
        params.strategy,
        result.trade_count,
        format_pnl(result.total_pnl),
        return_pct,
        params.capital,
        m.sharpe,
        assessment,
    );

    let mut key_findings = Vec::new();

    // Win rate + profit factor
    let win_pct = m.win_rate * 100.0;
    if m.profit_factor.is_finite() {
        key_findings.push(format!(
            "Win rate of {win_pct:.0}% with profit factor {:.2}{}",
            m.profit_factor,
            if m.profit_factor >= 1.5 {
                " — consistently profitable"
            } else if m.profit_factor >= 1.0 {
                " — marginally profitable"
            } else {
                " — losses exceed wins"
            }
        ));
    } else {
        key_findings.push(format!(
            "Win rate of {win_pct:.0}% with no losing trades"
        ));
    }

    // Drawdown
    let dd_pct = m.max_drawdown * 100.0;
    key_findings.push(format!(
        "Max drawdown of {dd_pct:.1}%{}",
        if m.calmar > 1.0 {
            format!(" is moderate relative to returns (Calmar {:.2})", m.calmar)
        } else if m.calmar > 0.0 {
            format!(" is high relative to returns (Calmar {:.2})", m.calmar)
        } else {
            " with negative or zero returns".to_string()
        }
    ));

    // VaR
    let var_pct = m.var_95 * 100.0;
    key_findings.push(format!(
        "VaR 95% of {var_pct:.1}% — daily risk is {}",
        if var_pct < 2.0 {
            "contained"
        } else if var_pct < 5.0 {
            "moderate"
        } else {
            "elevated"
        }
    ));

    // Trade behavior
    let trade_summary = compute_trade_summary(&result.trade_log);
    let common_exit = most_common_exit(&result.trade_log);
    key_findings.push(format!(
        "Average hold of {:.1} days, most common exit: {}",
        trade_summary.avg_days_held, common_exit
    ));

    let equity_curve_summary = compute_equity_summary(&result.equity_curve, params.capital);

    let mut suggested_next_steps = vec![
        format!(
            "Use compare_strategies to benchmark {} against similar strategies",
            params.strategy
        ),
        format!(
            "Use evaluate_strategy to find the optimal DTE/delta bucket for {}",
            params.strategy
        ),
    ];

    if m.sharpe < 1.0 {
        suggested_next_steps.push(
            "Consider adjusting stop_loss/take_profit thresholds to improve risk-adjusted returns"
                .to_string(),
        );
    }
    if m.max_drawdown > 0.15 {
        suggested_next_steps.push(
            "Max drawdown is significant — consider tighter risk management or position sizing"
                .to_string(),
        );
    }

    BacktestResponse {
        summary,
        assessment: assessment.to_string(),
        key_findings,
        metrics: result.metrics,
        trade_summary,
        equity_curve_summary,
        equity_curve: result.equity_curve,
        trade_log: result.trade_log,
        suggested_next_steps,
    }
}

pub fn format_evaluate(groups: Vec<GroupStats>, params: &EvaluateParams) -> EvaluateResponse {
    let total_buckets = groups.len();
    let total_trades: usize = groups.iter().map(|g| g.count).sum();

    let best_bucket = groups
        .iter()
        .max_by(|a, b| a.median.partial_cmp(&b.median).unwrap_or(std::cmp::Ordering::Equal))
        .cloned();

    let worst_bucket = groups
        .iter()
        .min_by(|a, b| a.median.partial_cmp(&b.median).unwrap_or(std::cmp::Ordering::Equal))
        .cloned();

    let highest_win_rate_bucket = groups
        .iter()
        .max_by(|a, b| {
            a.win_rate
                .partial_cmp(&b.win_rate)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .cloned();

    let summary = if let Some(ref best) = best_bucket {
        format!(
            "Evaluated {} across {} DTE/delta buckets ({} total trades). \
             Best bucket: DTE {}, Delta {} with median P&L ${:.2} and {:.0}% win rate.",
            params.strategy,
            total_buckets,
            total_trades,
            best.dte_range,
            best.delta_range,
            best.median,
            best.win_rate * 100.0,
        )
    } else {
        format!(
            "Evaluated {} but no buckets were produced. Check DTE/delta parameters.",
            params.strategy,
        )
    };

    let mut suggested_next_steps = Vec::new();
    if let Some(ref best) = best_bucket {
        suggested_next_steps.push(format!(
            "Run run_backtest targeting DTE {} and delta {} for a full simulation",
            best.dte_range, best.delta_range,
        ));
    }
    suggested_next_steps.push(format!(
        "Narrow delta_interval (currently {:.2}) for finer granularity around the best bucket",
        params.delta_interval,
    ));
    suggested_next_steps.push(format!(
        "Use compare_strategies to benchmark {} against alternatives",
        params.strategy,
    ));

    EvaluateResponse {
        summary,
        total_buckets,
        total_trades,
        best_bucket,
        worst_bucket,
        highest_win_rate_bucket,
        groups,
        suggested_next_steps,
    }
}

pub fn format_compare(results: Vec<CompareResult>) -> CompareResponse {
    let mut by_sharpe = results.clone();
    by_sharpe.sort_by(|a, b| {
        b.sharpe
            .partial_cmp(&a.sharpe)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let ranking_by_sharpe: Vec<String> = by_sharpe.iter().map(|r| r.strategy.clone()).collect();

    let mut by_pnl = results.clone();
    by_pnl.sort_by(|a, b| {
        b.pnl
            .partial_cmp(&a.pnl)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let ranking_by_pnl: Vec<String> = by_pnl.iter().map(|r| r.strategy.clone()).collect();

    let best_overall = ranking_by_sharpe
        .first()
        .cloned()
        .unwrap_or_default();

    let summary = if results.is_empty() {
        "No strategies to compare.".to_string()
    } else {
        let best_sharpe = &by_sharpe[0];
        let best_pnl = &by_pnl[0];
        format!(
            "Compared {} strategies. Best by Sharpe: {} ({:.2}). Best by P&L: {} ({}).",
            results.len(),
            best_sharpe.strategy,
            best_sharpe.sharpe,
            best_pnl.strategy,
            format_pnl(best_pnl.pnl),
        )
    };

    let mut suggested_next_steps = Vec::new();
    if !best_overall.is_empty() {
        suggested_next_steps.push(format!(
            "Run run_backtest on {best_overall} for detailed trade-level analysis",
        ));
        suggested_next_steps.push(format!(
            "Use evaluate_strategy on {best_overall} to find optimal DTE/delta parameters",
        ));
    }

    CompareResponse {
        summary,
        ranking_by_sharpe,
        ranking_by_pnl,
        best_overall,
        results,
        suggested_next_steps,
    }
}

pub fn format_load_data(
    rows: usize,
    symbols: Vec<String>,
    date_range: DateRange,
    columns: Vec<String>,
) -> LoadDataResponse {
    let symbol_list = symbols.join(", ");
    let summary = format!(
        "Loaded {} rows of options data for {} from {} to {}.",
        rows, symbol_list, date_range.start, date_range.end,
    );

    LoadDataResponse {
        summary,
        rows,
        symbols,
        date_range,
        columns,
        suggested_next_steps: vec![
            "Use list_strategies to see all available option strategies".to_string(),
            "Use evaluate_strategy for statistical analysis across DTE/delta buckets".to_string(),
            "Use run_backtest for a full simulation with equity curve and trade log".to_string(),
        ],
    }
}

pub fn format_strategies(strategies: Vec<StrategyInfo>) -> StrategiesResponse {
    let total = strategies.len();
    let mut categories: HashMap<String, usize> = HashMap::new();
    for s in &strategies {
        *categories.entry(s.category.clone()).or_default() += 1;
    }

    let cat_parts: Vec<String> = {
        let mut sorted: Vec<_> = categories.iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(a.1));
        sorted
            .iter()
            .map(|(cat, count)| format!("{cat} ({count})"))
            .collect()
    };

    let summary = format!(
        "{} strategies available across {} categories: {}.",
        total,
        categories.len(),
        cat_parts.join(", "),
    );

    StrategiesResponse {
        summary,
        total,
        categories,
        strategies,
        suggested_next_steps: vec![
            "Use evaluate_strategy with a strategy name to analyze its statistical performance"
                .to_string(),
            "Use run_backtest to simulate a strategy with specific parameters".to_string(),
            "Use compare_strategies to benchmark multiple strategies side by side".to_string(),
        ],
    }
}
