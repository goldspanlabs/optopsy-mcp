use std::collections::HashMap;

use crate::engine::types::{
    BacktestParams, BacktestResult, CompareResult, EquityPoint, EvaluateParams, ExitType,
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
    if max_points == 0 {
        return vec![];
    }
    if max_points == 1 {
        return curve.last().cloned().into_iter().collect();
    }
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

fn compute_trade_summary(
    trade_log: &[TradeRecord],
    metrics: &crate::engine::types::PerformanceMetrics,
) -> TradeSummary {
    let total = trade_log.len();

    // Only compute presentation-layer fields (best/worst trade, exit breakdown)
    // Trade-level averages come from PerformanceMetrics (already computed in metrics.rs)
    let mut winner_count: usize = 0;
    let mut loser_count: usize = 0;
    let mut exit_breakdown: HashMap<String, usize> = HashMap::new();
    let mut best: Option<&TradeRecord> = None;
    let mut worst: Option<&TradeRecord> = None;

    for t in trade_log {
        if t.pnl > 0.0 {
            winner_count += 1;
        } else if t.pnl < 0.0 {
            loser_count += 1;
        }
        // Zero-PnL (scratch) trades: neutral

        *exit_breakdown
            .entry(exit_type_name(&t.exit_type).to_string())
            .or_default() += 1;

        if best.is_none_or(|b| t.pnl > b.pnl) {
            best = Some(t);
        }
        if worst.is_none_or(|w| t.pnl < w.pnl) {
            worst = Some(t);
        }
    }

    let to_trade_stat = |t: Option<&TradeRecord>| {
        t.map(|t| TradeStat {
            pnl: t.pnl,
            date: t.entry_datetime.format("%Y-%m-%d").to_string(),
        })
    };

    TradeSummary {
        total,
        winners: winner_count,
        losers: loser_count,
        avg_pnl: metrics.avg_trade_pnl,
        avg_winner: metrics.avg_winner,
        avg_loser: metrics.avg_loser,
        avg_days_held: metrics.avg_days_held,
        exit_breakdown,
        best_trade: to_trade_stat(best),
        worst_trade: to_trade_stat(worst),
    }
}

fn compute_equity_summary(
    curve: &[EquityPoint],
    capital: f64,
    metrics: &crate::engine::types::PerformanceMetrics,
) -> EquityCurveSummary {
    let start_equity = if curve.is_empty() {
        capital
    } else {
        curve[0].equity
    };
    let end_equity = curve.last().map_or(capital, |p| p.equity);
    let peak_equity = curve
        .iter()
        .map(|p| p.equity)
        .fold(f64::NEG_INFINITY, f64::max)
        .max(capital);
    let trough_equity = curve
        .iter()
        .map(|p| p.equity)
        .fold(f64::INFINITY, f64::min)
        .min(capital);

    EquityCurveSummary {
        start_equity,
        end_equity,
        total_return_pct: metrics.total_return_pct,
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

fn backtest_key_findings(
    m: &crate::engine::types::PerformanceMetrics,
    trade_log: &[TradeRecord],
) -> Vec<String> {
    let mut findings = Vec::new();

    // Win rate + profit factor
    let win_pct = m.win_rate * 100.0;
    findings.push(format!(
        "Win rate of {win_pct:.0}% with profit factor {:.2}{}",
        m.profit_factor,
        if m.profit_factor >= 999.0 {
            " — no losing trades"
        } else if m.profit_factor >= 1.5 {
            " — consistently profitable"
        } else if m.profit_factor >= 1.0 {
            " — marginally profitable"
        } else {
            " — losses exceed wins"
        }
    ));

    // CAGR + total return
    findings.push(format!(
        "Total return {:.1}%, CAGR {:.1}%",
        m.total_return_pct,
        m.cagr * 100.0,
    ));

    // Drawdown
    let dd_pct = m.max_drawdown * 100.0;
    findings.push(format!(
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
    findings.push(format!(
        "VaR 95% of {var_pct:.1}% — daily risk is {}",
        if var_pct < 2.0 {
            "contained"
        } else if var_pct < 5.0 {
            "moderate"
        } else {
            "elevated"
        }
    ));

    // Expectancy + trade behavior
    let common_exit = most_common_exit(trade_log);
    findings.push(format!(
        "Expectancy {} per trade, avg hold {:.1} days, max losing streak: {}. Most common exit: {}",
        format_pnl(m.expectancy),
        m.avg_days_held,
        m.max_consecutive_losses,
        common_exit
    ));

    findings
}

pub fn format_backtest(result: BacktestResult, params: &BacktestParams) -> BacktestResponse {
    let m = &result.metrics;
    let trade_summary = compute_trade_summary(&result.trade_log, m);
    let equity_curve_summary = compute_equity_summary(&result.equity_curve, params.capital, m);

    // Zero-trade early branch: metrics are not meaningful
    if result.trade_log.is_empty() {
        return BacktestResponse {
            summary: format!(
                "Backtest of {}: no trades were executed. \
                 Check entry filters (DTE, delta) and data coverage.",
                params.strategy,
            ),
            assessment: "N/A".to_string(),
            key_findings: vec![
                "No trades matched the entry criteria during the backtest period".to_string(),
            ],
            metrics: result.metrics,
            trade_summary,
            equity_curve_summary,
            equity_curve: result.equity_curve,
            trade_log: result.trade_log,
            suggested_next_steps: vec![
                "Widen DTE range or delta targets to capture more entry opportunities".to_string(),
                format!(
                    "Use evaluate_strategy to explore which DTE/delta buckets have data for {}",
                    params.strategy
                ),
                "Verify the loaded dataset covers the expected date range and symbols".to_string(),
            ],
        };
    }

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

    let key_findings = backtest_key_findings(m, &result.trade_log);

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
        .max_by(|a, b| {
            a.median
                .partial_cmp(&b.median)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .cloned();

    let worst_bucket = groups
        .iter()
        .min_by(|a, b| {
            a.median
                .partial_cmp(&b.median)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
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
            "Use run_backtest targeting DTE {} and delta {} for a full simulation",
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
    // Build index-based rankings to avoid cloning the full results vec
    let mut sharpe_indices: Vec<usize> = (0..results.len()).collect();
    sharpe_indices.sort_by(|&a, &b| {
        results[b]
            .sharpe
            .partial_cmp(&results[a].sharpe)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let ranking_by_sharpe: Vec<String> = sharpe_indices
        .iter()
        .map(|&i| results[i].strategy.clone())
        .collect();

    let mut pnl_indices: Vec<usize> = (0..results.len()).collect();
    pnl_indices.sort_by(|&a, &b| {
        results[b]
            .pnl
            .partial_cmp(&results[a].pnl)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let ranking_by_pnl: Vec<String> = pnl_indices
        .iter()
        .map(|&i| results[i].strategy.clone())
        .collect();

    let best_overall = ranking_by_sharpe.first().cloned();

    let summary = if results.is_empty() {
        "No strategies to compare.".to_string()
    } else {
        let best_sharpe_idx = sharpe_indices[0];
        let best_pnl_idx = pnl_indices[0];
        let best_return_idx = results
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| {
                a.total_return_pct
                    .partial_cmp(&b.total_return_pct)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map_or(0, |(i, _)| i);
        format!(
            "Compared {} strategies. Best by Sharpe: {} ({:.2}). Best by P&L: {} ({}). \
             Best return: {} ({:.1}%).",
            results.len(),
            results[best_sharpe_idx].strategy,
            results[best_sharpe_idx].sharpe,
            results[best_pnl_idx].strategy,
            format_pnl(results[best_pnl_idx].pnl),
            results[best_return_idx].strategy,
            results[best_return_idx].total_return_pct,
        )
    };

    let mut suggested_next_steps = Vec::new();
    if let Some(ref best) = best_overall {
        suggested_next_steps.push(format!(
            "Use run_backtest on {best} for detailed trade-level analysis",
        ));
        suggested_next_steps.push(format!(
            "Use evaluate_strategy on {best} to find optimal DTE/delta parameters",
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
    let symbol_list = if symbols.is_empty() {
        "unknown".to_string()
    } else {
        symbols.join(", ")
    };
    let start = date_range.start.as_deref().unwrap_or("unknown");
    let end = date_range.end.as_deref().unwrap_or("unknown");
    let summary =
        format!("Loaded {rows} rows of options data for {symbol_list} from {start} to {end}.",);

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

    let summary = if total == 0 {
        "No strategies are currently available.".to_string()
    } else {
        format!(
            "{} strategies available across {} categories: {}.",
            total,
            categories.len(),
            cat_parts.join(", "),
        )
    };

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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDateTime;

    fn make_trade(pnl: f64, days_held: i64, exit_type: ExitType) -> TradeRecord {
        let dt = NaiveDateTime::parse_from_str("2024-01-15 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap();
        TradeRecord {
            trade_id: 1,
            entry_datetime: dt,
            exit_datetime: dt,
            entry_cost: 100.0,
            exit_proceeds: 100.0 + pnl,
            pnl,
            days_held,
            exit_type,
        }
    }

    fn make_equity_point(days_offset: i64, equity: f64) -> EquityPoint {
        let dt = NaiveDateTime::parse_from_str("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
            + chrono::Duration::days(days_offset);
        EquityPoint {
            datetime: dt,
            equity,
        }
    }

    #[test]
    fn assess_sharpe_thresholds() {
        assert_eq!(assess_sharpe(2.0), "excellent");
        assert_eq!(assess_sharpe(1.5), "excellent");
        assert_eq!(assess_sharpe(1.2), "strong");
        assert_eq!(assess_sharpe(1.0), "strong");
        assert_eq!(assess_sharpe(0.7), "moderate");
        assert_eq!(assess_sharpe(0.5), "moderate");
        assert_eq!(assess_sharpe(0.3), "weak");
        assert_eq!(assess_sharpe(0.0), "weak");
        assert_eq!(assess_sharpe(-0.5), "poor");
    }

    #[test]
    fn trade_summary_empty_log() {
        let metrics = crate::engine::metrics::DEFAULT_METRICS;
        let summary = compute_trade_summary(&[], &metrics);
        assert_eq!(summary.total, 0);
        assert_eq!(summary.winners, 0);
        assert_eq!(summary.losers, 0);
        assert_eq!(summary.avg_pnl, 0.0);
        assert_eq!(summary.avg_winner, 0.0);
        assert_eq!(summary.avg_loser, 0.0);
        assert_eq!(summary.avg_days_held, 0.0);
        assert!(summary.best_trade.is_none());
        assert!(summary.worst_trade.is_none());
    }

    #[test]
    fn trade_summary_mixed_trades() {
        let trades = vec![
            make_trade(100.0, 10, ExitType::Expiration),
            make_trade(-50.0, 5, ExitType::StopLoss),
            make_trade(200.0, 20, ExitType::TakeProfit),
        ];
        let metrics = crate::engine::types::PerformanceMetrics {
            avg_trade_pnl: 250.0 / 3.0,
            avg_winner: 150.0,
            avg_loser: -50.0,
            avg_days_held: 35.0 / 3.0,
            ..crate::engine::metrics::DEFAULT_METRICS
        };
        let summary = compute_trade_summary(&trades, &metrics);
        assert_eq!(summary.total, 3);
        assert_eq!(summary.winners, 2);
        assert_eq!(summary.losers, 1);
        assert!((summary.avg_pnl - 250.0 / 3.0).abs() < 1e-10);
        assert!((summary.avg_winner - 150.0).abs() < 1e-10);
        assert!((summary.avg_loser - -50.0).abs() < 1e-10);
        assert!((summary.best_trade.unwrap().pnl - 200.0).abs() < 1e-10);
        assert!((summary.worst_trade.unwrap().pnl - -50.0).abs() < 1e-10);
        assert_eq!(summary.exit_breakdown["Expiration"], 1);
        assert_eq!(summary.exit_breakdown["StopLoss"], 1);
        assert_eq!(summary.exit_breakdown["TakeProfit"], 1);
    }

    #[test]
    fn equity_summary_empty_curve() {
        let metrics = crate::engine::metrics::DEFAULT_METRICS;
        let summary = compute_equity_summary(&[], 100_000.0, &metrics);
        assert_eq!(summary.start_equity, 100_000.0);
        assert_eq!(summary.end_equity, 100_000.0);
        assert_eq!(summary.total_return_pct, 0.0);
        assert_eq!(summary.num_points, 0);
    }

    #[test]
    fn equity_summary_with_data() {
        let curve = vec![
            make_equity_point(0, 100_000.0),
            make_equity_point(1, 105_000.0),
            make_equity_point(2, 95_000.0),
            make_equity_point(3, 110_000.0),
        ];
        let metrics = crate::engine::types::PerformanceMetrics {
            total_return_pct: 10.0,
            ..crate::engine::metrics::DEFAULT_METRICS
        };
        let summary = compute_equity_summary(&curve, 100_000.0, &metrics);
        assert_eq!(summary.start_equity, 100_000.0);
        assert_eq!(summary.end_equity, 110_000.0);
        assert!((summary.total_return_pct - 10.0).abs() < 1e-10);
        assert_eq!(summary.peak_equity, 110_000.0);
        assert_eq!(summary.trough_equity, 95_000.0);
        assert_eq!(summary.num_points, 4);
    }

    #[test]
    fn sample_equity_curve_no_downsample() {
        let curve = vec![make_equity_point(0, 100.0), make_equity_point(1, 110.0)];
        let sampled = sample_equity_curve(&curve, 50);
        assert_eq!(sampled.len(), 2);
    }

    #[test]
    fn sample_equity_curve_downsamples() {
        let curve: Vec<EquityPoint> = (0..100)
            .map(|i| make_equity_point(i, 100.0 + i as f64))
            .collect();
        let sampled = sample_equity_curve(&curve, 10);
        assert_eq!(sampled.len(), 10);
        // First and last points should be preserved
        assert!((sampled[0].equity - 100.0).abs() < 1e-10);
        assert!((sampled[9].equity - 199.0).abs() < 1e-10);
    }

    #[test]
    fn format_compare_empty_results() {
        let response = format_compare(vec![]);
        assert_eq!(response.summary, "No strategies to compare.");
        assert!(response.ranking_by_sharpe.is_empty());
        assert!(response.ranking_by_pnl.is_empty());
        assert!(response.best_overall.is_none());
    }

    #[test]
    fn format_compare_rankings_correct() {
        let results = vec![
            CompareResult {
                strategy: "alpha".to_string(),
                trades: 10,
                pnl: 500.0,
                sharpe: 0.8,
                sortino: 1.0,
                max_dd: 0.05,
                win_rate: 0.6,
                profit_factor: 1.5,
                calmar: 1.0,
                total_return_pct: 5.0,
            },
            CompareResult {
                strategy: "beta".to_string(),
                trades: 20,
                pnl: 300.0,
                sharpe: 1.5,
                sortino: 2.0,
                max_dd: 0.03,
                win_rate: 0.7,
                profit_factor: 2.5,
                calmar: 2.0,
                total_return_pct: 3.0,
            },
            CompareResult {
                strategy: "gamma".to_string(),
                trades: 15,
                pnl: 1000.0,
                sharpe: 1.2,
                sortino: 1.5,
                max_dd: 0.08,
                win_rate: 0.65,
                profit_factor: 2.0,
                calmar: 1.5,
                total_return_pct: 10.0,
            },
        ];
        let response = format_compare(results);
        assert_eq!(response.ranking_by_sharpe, vec!["beta", "gamma", "alpha"]);
        assert_eq!(response.ranking_by_pnl, vec!["gamma", "alpha", "beta"]);
        assert_eq!(response.best_overall, Some("beta".to_string()));
        assert!(response.summary.contains("beta"));
        assert!(response.summary.contains("gamma"));
    }

    #[test]
    fn format_evaluate_empty_groups() {
        let params = EvaluateParams {
            strategy: "test_strat".to_string(),
            leg_deltas: vec![],
            max_entry_dte: 45,
            exit_dte: 0,
            dte_interval: 7,
            delta_interval: 0.05,
            slippage: crate::engine::types::Slippage::Mid,
            commission: None,
        };
        let response = format_evaluate(vec![], &params);
        assert_eq!(response.total_buckets, 0);
        assert_eq!(response.total_trades, 0);
        assert!(response.best_bucket.is_none());
        assert!(response.worst_bucket.is_none());
        assert!(response.summary.contains("no buckets"));
    }

    #[test]
    fn format_evaluate_finds_best_worst() {
        let params = EvaluateParams {
            strategy: "test_strat".to_string(),
            leg_deltas: vec![],
            max_entry_dte: 45,
            exit_dte: 0,
            dte_interval: 7,
            delta_interval: 0.05,
            slippage: crate::engine::types::Slippage::Mid,
            commission: None,
        };
        let groups = vec![
            GroupStats {
                dte_range: "(0, 7]".to_string(),
                delta_range: "(0.10, 0.15]".to_string(),
                count: 10,
                mean: 50.0,
                std: 20.0,
                min: -10.0,
                q25: 30.0,
                median: 45.0,
                q75: 60.0,
                max: 100.0,
                win_rate: 0.7,
                profit_factor: 2.0,
            },
            GroupStats {
                dte_range: "(7, 14]".to_string(),
                delta_range: "(0.15, 0.20]".to_string(),
                count: 5,
                mean: -20.0,
                std: 30.0,
                min: -80.0,
                q25: -40.0,
                median: -15.0,
                q75: 5.0,
                max: 20.0,
                win_rate: 0.4,
                profit_factor: 0.5,
            },
        ];
        let response = format_evaluate(groups, &params);
        assert_eq!(response.total_buckets, 2);
        assert_eq!(response.total_trades, 15);
        let best = response.best_bucket.unwrap();
        assert_eq!(best.dte_range, "(0, 7]");
        let worst = response.worst_bucket.unwrap();
        assert_eq!(worst.dte_range, "(7, 14]");
    }

    #[test]
    fn format_strategies_category_counts() {
        let strategies = vec![
            StrategyInfo {
                name: "long_call".to_string(),
                category: "Singles".to_string(),
                legs: 1,
                description: "Buy a call".to_string(),
            },
            StrategyInfo {
                name: "short_put".to_string(),
                category: "Singles".to_string(),
                legs: 1,
                description: "Sell a put".to_string(),
            },
            StrategyInfo {
                name: "bull_call_spread".to_string(),
                category: "Spreads".to_string(),
                legs: 2,
                description: "Bullish spread".to_string(),
            },
        ];
        let response = format_strategies(strategies);
        assert_eq!(response.total, 3);
        assert_eq!(response.categories["Singles"], 2);
        assert_eq!(response.categories["Spreads"], 1);
        assert!(response.summary.contains('3'));
    }

    #[test]
    fn format_load_data_with_missing_dates() {
        let response = format_load_data(
            1000,
            vec!["SPY".to_string()],
            DateRange {
                start: None,
                end: None,
            },
            vec!["col1".to_string()],
        );
        assert_eq!(response.rows, 1000);
        assert!(response.summary.contains("unknown"));
    }

    #[test]
    fn format_load_data_empty_symbols_shows_unknown() {
        let response = format_load_data(
            500,
            vec![],
            DateRange {
                start: Some("2024-01-01".to_string()),
                end: Some("2024-12-31".to_string()),
            },
            vec!["col1".to_string()],
        );
        assert!(
            response.summary.contains("unknown"),
            "summary should fall back to 'unknown' when symbols is empty, got: {}",
            response.summary
        );
        assert!(!response.summary.contains("for  from"));
    }

    #[test]
    fn format_load_data_with_dates() {
        let response = format_load_data(
            5000,
            vec!["SPY".to_string(), "QQQ".to_string()],
            DateRange {
                start: Some("2024-01-01".to_string()),
                end: Some("2024-12-31".to_string()),
            },
            vec!["col1".to_string(), "col2".to_string()],
        );
        assert_eq!(response.rows, 5000);
        assert!(response.summary.contains("SPY, QQQ"));
        assert!(response.summary.contains("2024-01-01"));
        assert!(response.summary.contains("2024-12-31"));
    }

    fn make_backtest_params(strategy: &str, capital: f64) -> BacktestParams {
        BacktestParams {
            strategy: strategy.to_string(),
            leg_deltas: vec![],
            max_entry_dte: 45,
            exit_dte: 0,
            slippage: crate::engine::types::Slippage::Mid,
            commission: None,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital,
            quantity: 1,
            multiplier: 100,
            max_positions: 1,
            selector: crate::engine::types::TradeSelector::default(),
            adjustment_rules: vec![],
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn make_backtest_result(
        total_pnl: f64,
        sharpe: f64,
        max_drawdown: f64,
        win_rate: f64,
        profit_factor: f64,
        calmar: f64,
        trades: Vec<TradeRecord>,
        equity: Vec<EquityPoint>,
    ) -> BacktestResult {
        // Compute realistic trade-level fields from the trades
        let total = trades.len();
        let winners: Vec<f64> = trades
            .iter()
            .filter(|t| t.pnl > 0.0)
            .map(|t| t.pnl)
            .collect();
        let losers: Vec<f64> = trades
            .iter()
            .filter(|t| t.pnl < 0.0)
            .map(|t| t.pnl)
            .collect();
        let avg_trade_pnl = if total > 0 {
            total_pnl / total as f64
        } else {
            0.0
        };
        let avg_winner = if winners.is_empty() {
            0.0
        } else {
            winners.iter().sum::<f64>() / winners.len() as f64
        };
        let avg_loser = if losers.is_empty() {
            0.0
        } else {
            losers.iter().sum::<f64>() / losers.len() as f64
        };
        let avg_days_held = if total > 0 {
            trades.iter().map(|t| t.days_held).sum::<i64>() as f64 / total as f64
        } else {
            0.0
        };
        let expectancy = (win_rate * avg_winner) + ((1.0 - win_rate) * avg_loser);

        BacktestResult {
            trade_count: total,
            total_pnl,
            metrics: crate::engine::types::PerformanceMetrics {
                sharpe,
                sortino: sharpe * 1.2,
                max_drawdown,
                win_rate,
                profit_factor,
                calmar,
                var_95: 0.03,
                total_return_pct: 0.0,
                cagr: 0.0,
                avg_trade_pnl,
                avg_winner,
                avg_loser,
                avg_days_held,
                max_consecutive_losses: 0,
                expectancy,
            },
            equity_curve: equity,
            trade_log: trades,
        }
    }

    #[test]
    fn format_backtest_excellent_sharpe() {
        let trades = vec![
            make_trade(200.0, 10, ExitType::Expiration),
            make_trade(150.0, 5, ExitType::TakeProfit),
        ];
        let result = make_backtest_result(350.0, 1.8, 0.05, 0.9, 3.0, 2.0, trades, vec![]);
        let params = make_backtest_params("short_put", 100_000.0);
        let response = format_backtest(result, &params);

        assert!(response.summary.contains("excellent"));
        assert_eq!(response.assessment, "excellent");
        assert!(response.summary.contains("short_put"));
        assert!(response.summary.contains("2 trades"));
        // Low drawdown + high sharpe means no risk warnings
        assert!(!response
            .suggested_next_steps
            .iter()
            .any(|s| s.contains("drawdown")));
        assert!(!response
            .suggested_next_steps
            .iter()
            .any(|s| s.contains("stop_loss")));
    }

    #[test]
    fn format_backtest_poor_sharpe_high_drawdown() {
        let trades = vec![
            make_trade(-100.0, 30, ExitType::StopLoss),
            make_trade(-200.0, 20, ExitType::Expiration),
            make_trade(50.0, 10, ExitType::Expiration),
        ];
        let result = make_backtest_result(-250.0, -0.5, 0.25, 0.33, 0.3, -0.2, trades, vec![]);
        let params = make_backtest_params("long_call", 10_000.0);
        let response = format_backtest(result, &params);

        assert!(response.summary.contains("poor"));
        assert_eq!(response.assessment, "poor");
        // Should suggest risk improvements
        assert!(response
            .suggested_next_steps
            .iter()
            .any(|s| s.contains("stop_loss") || s.contains("risk")));
        assert!(response
            .suggested_next_steps
            .iter()
            .any(|s| s.contains("drawdown") || s.contains("risk management")));
        // Key findings should mention losses exceed wins
        assert!(response
            .key_findings
            .iter()
            .any(|f| f.contains("losses exceed wins")));
    }

    #[test]
    fn format_backtest_all_wins_capped_profit_factor() {
        let trades = vec![
            make_trade(100.0, 5, ExitType::TakeProfit),
            make_trade(200.0, 7, ExitType::TakeProfit),
        ];
        let result = make_backtest_result(300.0, 1.2, 0.02, 1.0, 999.99, 5.0, trades, vec![]);
        let params = make_backtest_params("bull_call_spread", 50_000.0);
        let response = format_backtest(result, &params);

        assert_eq!(response.assessment, "strong");
        // Capped profit factor (999.99) should show no losing trades
        assert!(response
            .key_findings
            .iter()
            .any(|f| f.contains("no losing trades")));
        // Trade summary should have 0 losers
        assert_eq!(response.trade_summary.losers, 0);
        assert_eq!(response.trade_summary.winners, 2);
    }

    #[test]
    fn format_backtest_equity_peak_trough_from_curve() {
        let curve = vec![
            make_equity_point(0, 100_000.0),
            make_equity_point(1, 95_000.0),
            make_equity_point(2, 110_000.0),
        ];
        let trades = vec![make_trade(10_000.0, 2, ExitType::Expiration)];
        let result = make_backtest_result(10_000.0, 1.0, 0.05, 1.0, 999.99, 2.0, trades, curve);
        let params = make_backtest_params("test", 100_000.0);
        let response = format_backtest(result, &params);

        assert_eq!(response.equity_curve_summary.peak_equity, 110_000.0);
        assert_eq!(response.equity_curve_summary.trough_equity, 95_000.0);
        assert_eq!(response.equity_curve_summary.num_points, 3);
    }

    #[test]
    fn format_backtest_zero_trades() {
        let result = make_backtest_result(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, vec![], vec![]);
        let params = make_backtest_params("iron_condor", 100_000.0);
        let response = format_backtest(result, &params);

        assert!(response.summary.contains("no trades"));
        assert_eq!(response.assessment, "N/A");
        assert!(response
            .key_findings
            .iter()
            .any(|f| f.contains("No trades matched")));
        // Should not contain misleading metric findings
        assert!(!response
            .key_findings
            .iter()
            .any(|f| f.contains("Win rate") || f.contains("drawdown")));
        // Suggested steps should guide toward fixing entry criteria
        assert!(response
            .suggested_next_steps
            .iter()
            .any(|s| s.contains("Widen") || s.contains("DTE")));
    }
}
