use std::collections::HashMap;

use crate::engine::types::{
    BacktestParams, BacktestQualityStats, BacktestResult, CompareParams, CompareResult, ExitType,
    TradeRecord,
};

use crate::data::eodhd::DownloadSummary;

use super::response_types::{
    BacktestDataQuality, BacktestParamsSummary, BacktestResponse, CompareResponse,
    CompareStrategyEntry, DateRange, DownloadResponse, LoadDataResponse, PriceBar,
    RawPricesResponse, StrategiesResponse, StrategyInfo, TradeStat, TradeSummary,
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
        ExitType::Signal => "Signal",
    }
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

fn build_backtest_quality(quality: &BacktestQualityStats) -> BacktestDataQuality {
    let price_data_coverage_pct = if quality.trading_days_total > 0 {
        (quality.trading_days_with_data as f64 / quality.trading_days_total as f64) * 100.0
    } else {
        0.0
    };

    let fill_rate_pct = if quality.total_candidates > 0 {
        (quality.positions_opened as f64 / quality.total_candidates as f64) * 100.0
    } else {
        0.0
    };

    let median_entry_spread_pct = if quality.entry_spread_pcts.is_empty() {
        None
    } else {
        let mut sorted = quality.entry_spread_pcts.clone();
        sorted.sort_by(|a: &f64, b: &f64| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median_idx = sorted.len() / 2;
        Some(if sorted.len().is_multiple_of(2) {
            f64::midpoint(sorted[median_idx - 1], sorted[median_idx])
        } else {
            sorted[median_idx]
        })
    };

    let mut warnings = Vec::new();
    if price_data_coverage_pct < 80.0 {
        warnings.push(format!(
            "Price data missing for {:.0}% of trading days. Mark-to-market accuracy may be reduced.",
            100.0 - price_data_coverage_pct
        ));
    }
    if quality.total_candidates > 0 && fill_rate_pct < 50.0 {
        warnings.push(format!(
            "Only {fill_rate_pct:.0}% of entry candidates were opened. Consider increasing max_positions."
        ));
    }

    BacktestDataQuality {
        trading_days_total: quality.trading_days_total,
        trading_days_with_price_data: quality.trading_days_with_data,
        price_data_coverage_pct,
        total_entry_candidates: quality.total_candidates,
        total_positions_opened: quality.positions_opened,
        fill_rate_pct,
        median_entry_spread_pct,
        warnings,
    }
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
        if m.max_consecutive_losses == 0 && m.win_rate > 0.0 {
            " — no losing trades"
        } else if m.win_rate == 0.0 && m.avg_loser == 0.0 {
            " — all scratch trades"
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

#[allow(clippy::too_many_lines)]
pub fn format_backtest(result: BacktestResult, params: &BacktestParams) -> BacktestResponse {
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
                "No trades matched the entry criteria during the backtest period".to_string(),
            ],
            parameters: BacktestParamsSummary {
                strategy: params.strategy.clone(),
                leg_deltas: params.leg_deltas.clone(),
                entry_dte: params.entry_dte.clone(),
                exit_dte: params.exit_dte,
                slippage: params.slippage.clone(),
                commission: params.commission.clone(),
                capital: params.capital,
                quantity: params.quantity,
                multiplier: params.multiplier,
                max_positions: params.max_positions,
                stop_loss: params.stop_loss,
                take_profit: params.take_profit,
                max_hold_days: params.max_hold_days,
                selector: params.selector.clone(),
                entry_signal: params
                    .entry_signal
                    .as_ref()
                    .map(|s| serde_json::to_value(s).unwrap_or(serde_json::Value::Null)),
                exit_signal: params
                    .exit_signal
                    .as_ref()
                    .map(|s| serde_json::to_value(s).unwrap_or(serde_json::Value::Null)),
            },
            metrics: result.metrics,
            trade_summary,
            trade_log: result.trade_log,
            data_quality,
            suggested_next_steps: vec![
                    "[RETRY] Widen DTE range or delta targets to capture more entry opportunities".to_string(),
                format!(
                    "[RETRY] Try broader parameters (wider entry_dte or leg_deltas) for {}",
                    params.strategy
                ),
                "[CHECK] Verify the loaded dataset covers the expected date range via get_loaded_symbol".to_string(),
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
            "[Phase 6 → NEXT] Use compare_strategies to benchmark {} against similar strategies or parameter variations",
            params.strategy
        ),
    ];

    if m.sharpe < 1.0 {
        suggested_next_steps.push(
            "[Phase 5 → ITERATE] Adjust stop_loss/take_profit thresholds and re-run run_backtest to improve risk-adjusted returns"
                .to_string(),
        );
    }
    if m.max_drawdown > 0.15 {
        suggested_next_steps.push(
            "[Phase 5 → ITERATE] Max drawdown is significant — tighten risk management and re-run run_backtest"
                .to_string(),
        );
    }

    BacktestResponse {
        summary,
        assessment: assessment.to_string(),
        key_findings,
        parameters: BacktestParamsSummary {
            strategy: params.strategy.clone(),
            leg_deltas: params.leg_deltas.clone(),
            entry_dte: params.entry_dte.clone(),
            exit_dte: params.exit_dte,
            slippage: params.slippage.clone(),
            commission: params.commission.clone(),
            capital: params.capital,
            quantity: params.quantity,
            multiplier: params.multiplier,
            max_positions: params.max_positions,
            stop_loss: params.stop_loss,
            take_profit: params.take_profit,
            max_hold_days: params.max_hold_days,
            selector: params.selector.clone(),
            entry_signal: params
                .entry_signal
                .as_ref()
                .map(|s| serde_json::to_value(s).unwrap_or(serde_json::Value::Null)),
            exit_signal: params
                .exit_signal
                .as_ref()
                .map(|s| serde_json::to_value(s).unwrap_or(serde_json::Value::Null)),
        },
        metrics: result.metrics,
        trade_summary,
        trade_log: result.trade_log,
        data_quality,
        suggested_next_steps,
    }
}

pub fn format_compare(results: Vec<CompareResult>, params: &CompareParams) -> CompareResponse {
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
            "[Phase 5 → DRILL DOWN] Use run_backtest on {best} for detailed trade-level analysis",
        ));
        suggested_next_steps.push(format!(
            "[Phase 6 → ITERATE] Use compare_strategies with parameter variations of {best} to further optimize",
        ));
    }

    let strategies_compared = params
        .strategies
        .iter()
        .map(|entry| CompareStrategyEntry {
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

pub fn format_load_data(
    symbol: &str,
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
        symbol: symbol.to_string(),
        rows,
        symbols,
        date_range,
        columns,
        suggested_next_steps: vec![
            "[Phase 2 → NEXT] Call list_strategies() to browse available strategies and choose one to analyze".to_string(),
            "[Phase 3 → THEN] Call suggest_parameters({ strategy, risk_preference: \"moderate\" }) for data-driven parameter recommendations".to_string(),
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
            "[NEXT] Call suggest_parameters({ strategy: \"<chosen_strategy>\", risk_preference: \"moderate\" }) to get data-driven parameters".to_string(),
            "[THEN] Call run_backtest with the chosen strategy for full simulation".to_string(),
        ],
    }
}

pub fn format_download(summary: DownloadSummary) -> DownloadResponse {
    let api_calls = summary.api_requests * crate::data::eodhd::API_CALLS_PER_REQUEST;

    let status = if summary.was_resumed && summary.new_rows > 0 {
        format!(
            "Resumed download for {}: added {} new rows to {} previously cached. Total: {} records.",
            summary.symbol, summary.new_rows, summary.cached_rows, summary.total_rows,
        )
    } else if summary.was_resumed && summary.new_rows == 0 {
        format!(
            "Cache for {} is already up to date ({} records, no new data fetched).",
            summary.symbol, summary.total_rows,
        )
    } else {
        format!(
            "Downloaded {} options records for {}.",
            summary.total_rows, summary.symbol,
        )
    };

    let date_info = match (&summary.date_min, &summary.date_max) {
        (Some(min), Some(max)) => format!(" Date range: {min} to {max}."),
        _ => String::new(),
    };

    let mut full_summary = format!(
        "{status}{date_info} API usage: {} requests ({api_calls} API calls).",
        summary.api_requests,
    );

    if !summary.errors.is_empty() {
        use std::fmt::Write;
        let _ = write!(
            full_summary,
            " WARNING: partial errors occurred: {}. Run download_options_data again to retry.",
            summary.errors.join("; "),
        );
    }

    DownloadResponse {
        summary: full_summary,
        symbol: summary.symbol.clone(),
        new_rows: summary.new_rows,
        total_rows: summary.total_rows,
        was_resumed: summary.was_resumed,
        api_requests: summary.api_requests,
        date_range: DateRange {
            start: summary.date_min,
            end: summary.date_max,
        },
        suggested_next_steps: vec![
            format!(
                "[Phase 1 → NEXT] Call load_data({{ symbol: \"{}\" }}) to load the downloaded data into memory",
                summary.symbol,
            ),
            format!(
                "[Phase 2 → THEN] Call list_strategies() to browse available strategies"
            ),
        ],
    }
}

pub fn format_raw_prices(
    symbol: &str,
    total_rows: usize,
    returned_rows: usize,
    sampled: bool,
    date_range: DateRange,
    prices: Vec<PriceBar>,
) -> RawPricesResponse {
    let summary = if sampled {
        format!(
            "Returning {returned_rows} sampled price bars for {symbol} (from {total_rows} total). \
             Use these data points directly to generate charts or perform analysis."
        )
    } else {
        format!(
            "Returning {returned_rows} price bars for {symbol}. \
             Use these data points directly to generate charts or perform analysis."
        )
    };

    RawPricesResponse {
        summary,
        symbol: symbol.to_string(),
        total_rows,
        returned_rows,
        sampled,
        date_range,
        prices,
        suggested_next_steps: vec![
            "[Phase 7 → COMPLETE] Use the prices array to generate a line chart (close prices), candlestick chart (OHLC), or area chart.".to_string(),
            "[Phase 7 → COMPLETE] Combine with backtest trade_log data to overlay strategy performance on price action.".to_string(),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::types::{DteRange, EquityPoint, TradeSelector};
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
    fn format_compare_empty_results() {
        let params = CompareParams {
            strategies: vec![],
            sim_params: crate::engine::types::SimParams {
                capital: 10000.0,
                quantity: 1,
                multiplier: 100,
                max_positions: 5,
                selector: TradeSelector::default(),
                stop_loss: None,
                take_profit: None,
                max_hold_days: None,
            },
        };
        let response = format_compare(vec![], &params);
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

        let params = CompareParams {
            strategies: vec![
                crate::engine::types::CompareEntry {
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
                crate::engine::types::CompareEntry {
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
                crate::engine::types::CompareEntry {
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
            ],
            sim_params: crate::engine::types::SimParams {
                capital: 10000.0,
                quantity: 1,
                multiplier: 100,
                max_positions: 5,
                selector: TradeSelector::default(),
                stop_loss: None,
                take_profit: None,
                max_hold_days: None,
            },
        };

        let response = format_compare(results, &params);
        assert_eq!(response.ranking_by_sharpe, vec!["beta", "gamma", "alpha"]);
        assert_eq!(response.ranking_by_pnl, vec!["gamma", "alpha", "beta"]);
        assert_eq!(response.best_overall, Some("beta".to_string()));
        assert_eq!(response.strategies_compared.len(), 3);
        assert!(response.summary.contains("beta"));
        assert!(response.summary.contains("gamma"));
    }

    #[test]
    fn format_strategies_category_counts() {
        let strategies = vec![
            StrategyInfo {
                name: "long_call".to_string(),
                category: "Singles".to_string(),
                legs: 1,
                description: "Buy a call".to_string(),
                default_deltas: vec![],
            },
            StrategyInfo {
                name: "short_put".to_string(),
                category: "Singles".to_string(),
                legs: 1,
                description: "Sell a put".to_string(),
                default_deltas: vec![],
            },
            StrategyInfo {
                name: "bull_call_spread".to_string(),
                category: "Spreads".to_string(),
                legs: 2,
                description: "Bullish spread".to_string(),
                default_deltas: vec![],
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
            "SPY",
            1000,
            vec!["SPY".to_string()],
            DateRange {
                start: None,
                end: None,
            },
            vec!["col1".to_string()],
        );
        assert_eq!(response.rows, 1000);
        assert_eq!(response.symbol, "SPY");
        assert!(response.summary.contains("unknown"));
    }

    #[test]
    fn format_load_data_empty_symbols_shows_unknown() {
        let response = format_load_data(
            "QQQ",
            500,
            vec![],
            DateRange {
                start: Some("2024-01-01".to_string()),
                end: Some("2024-12-31".to_string()),
            },
            vec!["col1".to_string()],
        );
        assert_eq!(response.symbol, "QQQ");
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
            "SPY",
            5000,
            vec!["SPY".to_string(), "QQQ".to_string()],
            DateRange {
                start: Some("2024-01-01".to_string()),
                end: Some("2024-12-31".to_string()),
            },
            vec!["col1".to_string(), "col2".to_string()],
        );
        assert_eq!(response.rows, 5000);
        assert_eq!(response.symbol, "SPY");
        assert!(response.summary.contains("SPY, QQQ"));
        assert!(response.summary.contains("2024-01-01"));
        assert!(response.summary.contains("2024-12-31"));
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
        // Derive trade-level fields from the trades to match production logic.
        // Note: sharpe, max_drawdown, calmar, profit_factor are still provided
        // as parameters since they depend on equity curve or are hard to compute here.
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
        // Derive win_rate and loss_rate from trades to match production
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
                // Zero-PnL (scratch) and winners both reset the streak,
                // matching production behavior in compute_trade_metrics.
                streak = 0;
            }
        }

        BacktestResult {
            trade_count: total,
            total_pnl: sum_pnl,
            metrics: crate::engine::types::PerformanceMetrics {
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
    fn format_backtest_excellent_sharpe() {
        let trades = vec![
            make_trade(200.0, 10, ExitType::Expiration),
            make_trade(150.0, 5, ExitType::TakeProfit),
        ];
        let result = make_backtest_result(350.0, 1.8, 0.05, 3.0, 2.0, trades, vec![]);
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
        let result = make_backtest_result(-250.0, -0.5, 0.25, 0.3, -0.2, trades, vec![]);
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
        let result = make_backtest_result(300.0, 1.2, 0.02, 999.99, 5.0, trades, vec![]);
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
    fn format_backtest_zero_trades() {
        let result = make_backtest_result(0.0, 0.0, 0.0, 0.0, 0.0, vec![], vec![]);
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
