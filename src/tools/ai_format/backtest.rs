use crate::engine::types::{
    to_display_name, BacktestParams, BacktestResult, CompareEntry, CompareResult,
};
use crate::tools::ai_helpers::{
    assess_sharpe, backtest_key_findings, build_backtest_quality, build_params_summary,
    compute_trade_summary, format_pnl, DRAWDOWN_HIGH, SHARPE_NEEDS_IMPROVEMENT,
};
use crate::tools::response_types::{
    BacktestResponse, CompareResponse, CompareStrategyEntry, UnderlyingPrice,
};

#[allow(clippy::too_many_lines)]
pub fn format_backtest(
    result: BacktestResult,
    params: &BacktestParams,
    underlying_prices: Vec<UnderlyingPrice>,
) -> BacktestResponse {
    let m = &result.metrics;
    let trade_summary = compute_trade_summary(&result.trade_log, m);
    let data_quality = build_backtest_quality(&result.quality);

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
                "No trades matched the entry criteria.".to_string(),
                "Consider widening DTE range, delta range, or checking data availability."
                    .to_string(),
            ],
            parameters: build_params_summary(params),
            metrics: result.metrics,
            trade_summary,
            trade_log: vec![],
            data_quality,
            underlying_prices,
            suggested_next_steps: vec![
                "Widen entry_dte or leg_deltas ranges and re-run".to_string(),
                "Check that data covers the desired date range".to_string(),
            ],
        };
    }

    let assessment = assess_sharpe(m.sharpe);
    let pnl_str = format_pnl(result.total_pnl);

    let summary = format!(
        "Backtest of {}: {assessment} performance (Sharpe {:.2}). \
         {} trades, {} total P&L, {:.1}% win rate, {:.1}% max drawdown.",
        params.strategy,
        m.sharpe,
        result.trade_count,
        pnl_str,
        m.win_rate * 100.0,
        m.max_drawdown * 100.0,
    );

    let key_findings = backtest_key_findings(m, &result.trade_log);

    let mut suggested_next_steps = Vec::new();
    if m.sharpe < SHARPE_NEEDS_IMPROVEMENT {
        suggested_next_steps.push(
            "[ITERATE] Try different delta targets or DTE ranges to improve Sharpe".to_string(),
        );
    }
    if m.max_drawdown > DRAWDOWN_HIGH {
        suggested_next_steps.push(
            "[RISK] High drawdown detected — consider adding stop_loss or reducing position size"
                .to_string(),
        );
        if params.stop_loss.is_none() {
            suggested_next_steps.push(
                "[RISK] No stop_loss set — try adding one (e.g., stop_loss: 1.5) to cap individual trade risk"
                    .to_string(),
            );
        }
    }
    suggested_next_steps.push(format!(
        "[NEXT] Use parameter_sweep to optimize {} parameters across delta/DTE grids",
        params.strategy
    ));
    suggested_next_steps.push(format!(
        "[VALIDATE] Use walk_forward to check {} for overfitting",
        params.strategy
    ));

    BacktestResponse {
        summary,
        assessment: assessment.to_string(),
        key_findings,
        parameters: build_params_summary(params),
        metrics: result.metrics,
        trade_summary,
        trade_log: result.trade_log,
        data_quality,
        underlying_prices,
        suggested_next_steps,
    }
}

pub fn format_compare(
    results: Vec<CompareResult>,
    labeled_entries: &[CompareEntry],
) -> CompareResponse {
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
    if !results.is_empty() {
        // Use the actual strategy name (not the display label) for actionable next steps
        let best_idx = sharpe_indices[0];
        let strategy_name = &labeled_entries[best_idx].name;
        let best_label = &results[best_idx].strategy;
        suggested_next_steps.push(format!(
            "[NEXT] Use run_backtest(strategy=\"{strategy_name}\") on {best_label} for detailed trade-level analysis",
        ));
        suggested_next_steps.push(format!(
            "[ITERATE] Use compare_strategies with parameter variations of {strategy_name} to further optimize",
        ));
    }

    let strategies_compared = labeled_entries
        .iter()
        .map(|entry| CompareStrategyEntry {
            display_name: to_display_name(&entry.name),
            name: entry.name.clone(),
            leg_deltas: entry.leg_deltas.clone(),
            entry_dte: entry.entry_dte.clone(),
            exit_dte: entry.exit_dte,
            slippage: entry.slippage.clone(),
            commission: entry.commission.clone(),
        })
        .collect();

    CompareResponse {
        summary,
        strategies_compared,
        ranking_by_sharpe,
        ranking_by_pnl,
        best_overall,
        results,
        suggested_next_steps,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::types::{
        CompareEntry, DteRange, EquityPoint, ExitType, PerformanceMetrics, TradeRecord,
    };
    use crate::tools::ai_helpers::{assess_sharpe, compute_trade_summary};
    use chrono::NaiveDateTime;

    fn make_trade(pnl: f64, days_held: i64, exit_type: ExitType) -> TradeRecord {
        let dt = NaiveDateTime::parse_from_str("2024-01-15 09:30:00", "%Y-%m-%d %H:%M:%S").unwrap();
        TradeRecord::new(
            1,
            dt,
            dt,
            100.0,
            100.0 + pnl,
            pnl,
            days_held,
            exit_type,
            vec![],
        )
    }

    fn make_backtest_params(strategy: &str, capital: f64) -> BacktestParams {
        BacktestParams {
            strategy: strategy.to_string(),
            leg_deltas: vec![],
            entry_dte: DteRange {
                target: 45,
                min: 10,
                max: 60,
            },
            exit_dte: 0,
            slippage: crate::engine::types::Slippage::Mid,
            commission: None,
            min_bid_ask: 0.0,
            stop_loss: None,
            take_profit: None,
            max_hold_days: None,
            capital,
            quantity: 1,
            multiplier: 100,
            max_positions: 1,
            selector: crate::engine::types::TradeSelector::default(),
            adjustment_rules: vec![],
            entry_signal: None,
            exit_signal: None,
            ohlcv_path: None,
            cross_ohlcv_paths: std::collections::HashMap::new(),
            min_net_premium: None,
            max_net_premium: None,
            min_net_delta: None,
            max_net_delta: None,
            min_days_between_entries: None,
            expiration_filter: crate::engine::types::ExpirationFilter::Any,
            exit_net_delta: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn make_backtest_result(
        _total_pnl: f64,
        sharpe: f64,
        max_drawdown: f64,
        profit_factor: f64,
        calmar: f64,
        trades: Vec<TradeRecord>,
        equity: Vec<EquityPoint>,
    ) -> BacktestResult {
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
        let sum_pnl: f64 = trades.iter().map(|t| t.pnl).sum();
        let avg_trade_pnl = if total > 0 {
            sum_pnl / total as f64
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
        let actual_win_rate = if total > 0 {
            winners.len() as f64 / total as f64
        } else {
            0.0
        };
        let actual_loss_rate = if total > 0 {
            losers.len() as f64 / total as f64
        } else {
            0.0
        };
        let expectancy = (actual_win_rate * avg_winner) + (actual_loss_rate * avg_loser);

        let mut max_consecutive_losses = 0usize;
        let mut streak = 0usize;
        for t in &trades {
            if t.pnl < 0.0 {
                streak += 1;
                max_consecutive_losses = max_consecutive_losses.max(streak);
            } else {
                streak = 0;
            }
        }

        BacktestResult {
            trade_count: total,
            total_pnl: sum_pnl,
            metrics: PerformanceMetrics {
                sharpe,
                sortino: sharpe * 1.2,
                max_drawdown,
                win_rate: actual_win_rate,
                profit_factor,
                calmar,
                var_95: 0.03,
                total_return_pct: 0.0,
                cagr: 0.0,
                avg_trade_pnl,
                avg_winner,
                avg_loser,
                avg_days_held,
                max_consecutive_losses,
                expectancy,
            },
            equity_curve: equity,
            trade_log: trades,
            quality: crate::engine::types::BacktestQualityStats::default(),
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
        assert!((summary.avg_pnl - 0.0).abs() < f64::EPSILON);
        assert!((summary.avg_winner - 0.0).abs() < f64::EPSILON);
        assert!((summary.avg_loser - 0.0).abs() < f64::EPSILON);
        assert!((summary.avg_days_held - 0.0).abs() < f64::EPSILON);
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
        let metrics = PerformanceMetrics {
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
    fn format_compare_empty_results() {
        let response = format_compare(vec![], &[]);
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
                display_name: "Alpha".to_string(),
                trades: 10,
                pnl: 500.0,
                sharpe: 0.8,
                sortino: 1.0,
                max_dd: 0.05,
                win_rate: 0.6,
                profit_factor: 1.5,
                calmar: 1.0,
                total_return_pct: 5.0,
                trade_log: vec![],
                error: None,
            },
            CompareResult {
                strategy: "beta".to_string(),
                display_name: "Beta".to_string(),
                trades: 20,
                pnl: 300.0,
                sharpe: 1.5,
                sortino: 2.0,
                max_dd: 0.03,
                win_rate: 0.7,
                profit_factor: 2.5,
                calmar: 2.0,
                total_return_pct: 3.0,
                trade_log: vec![],
                error: None,
            },
            CompareResult {
                strategy: "gamma".to_string(),
                display_name: "Gamma".to_string(),
                trades: 15,
                pnl: 1000.0,
                sharpe: 1.2,
                sortino: 1.5,
                max_dd: 0.08,
                win_rate: 0.65,
                profit_factor: 2.0,
                calmar: 1.5,
                total_return_pct: 10.0,
                trade_log: vec![],
                error: None,
            },
        ];

        let labeled_entries = vec![
            CompareEntry {
                name: "alpha".to_string(),
                leg_deltas: vec![],
                entry_dte: DteRange {
                    target: 45,
                    min: 30,
                    max: 60,
                },
                exit_dte: 7,
                slippage: crate::engine::types::Slippage::Mid,
                commission: None,
            },
            CompareEntry {
                name: "beta".to_string(),
                leg_deltas: vec![],
                entry_dte: DteRange {
                    target: 45,
                    min: 30,
                    max: 60,
                },
                exit_dte: 7,
                slippage: crate::engine::types::Slippage::Mid,
                commission: None,
            },
            CompareEntry {
                name: "gamma".to_string(),
                leg_deltas: vec![],
                entry_dte: DteRange {
                    target: 45,
                    min: 30,
                    max: 60,
                },
                exit_dte: 7,
                slippage: crate::engine::types::Slippage::Mid,
                commission: None,
            },
        ];

        let response = format_compare(results, &labeled_entries);
        assert_eq!(response.ranking_by_sharpe, vec!["beta", "gamma", "alpha"]);
        assert_eq!(response.ranking_by_pnl, vec!["gamma", "alpha", "beta"]);
        assert_eq!(response.best_overall, Some("beta".to_string()));
        assert_eq!(response.strategies_compared.len(), 3);
        assert!(response.summary.contains("beta"));
        assert!(response.summary.contains("gamma"));
    }

    #[test]
    fn format_backtest_excellent_sharpe() {
        let trades = vec![
            make_trade(200.0, 10, ExitType::Expiration),
            make_trade(150.0, 5, ExitType::TakeProfit),
        ];
        let result = make_backtest_result(350.0, 1.8, 0.05, 3.0, 2.0, trades, vec![]);
        let params = make_backtest_params("short_put", 100_000.0);
        let response = format_backtest(result, &params, vec![]);

        assert!(response.summary.contains("excellent"));
        assert_eq!(response.assessment, "excellent");
        assert!(response.summary.contains("short_put"));
        assert!(response.summary.contains("2 trades"));
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
        let result = make_backtest_result(-250.0, -0.5, 0.25, 0.3, -0.2, trades, vec![]);
        let params = make_backtest_params("long_call", 10_000.0);
        let response = format_backtest(result, &params, vec![]);

        assert!(response.summary.contains("poor"));
        assert_eq!(response.assessment, "poor");
        assert!(response
            .suggested_next_steps
            .iter()
            .any(|s| s.contains("stop_loss") || s.contains("risk")));
        assert!(response
            .suggested_next_steps
            .iter()
            .any(|s| s.contains("drawdown") || s.contains("risk management")));
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
        let result = make_backtest_result(300.0, 1.2, 0.02, 999.99, 5.0, trades, vec![]);
        let params = make_backtest_params("bull_call_spread", 50_000.0);
        let response = format_backtest(result, &params, vec![]);

        assert_eq!(response.assessment, "strong");
        assert!(response
            .key_findings
            .iter()
            .any(|f| f.contains("no losing trades")));
        assert_eq!(response.trade_summary.losers, 0);
        assert_eq!(response.trade_summary.winners, 2);
    }

    #[test]
    fn format_backtest_zero_trades() {
        let result = make_backtest_result(0.0, 0.0, 0.0, 0.0, 0.0, vec![], vec![]);
        let params = make_backtest_params("iron_condor", 100_000.0);
        let response = format_backtest(result, &params, vec![]);

        assert!(response.summary.contains("no trades"));
        assert_eq!(response.assessment, "N/A");
        assert!(response
            .key_findings
            .iter()
            .any(|f| f.contains("No trades matched")));
        assert!(!response
            .key_findings
            .iter()
            .any(|f| f.contains("Win rate") || f.contains("drawdown")));
        assert!(response
            .suggested_next_steps
            .iter()
            .any(|s| s.contains("Widen") || s.contains("DTE")));
    }
}
