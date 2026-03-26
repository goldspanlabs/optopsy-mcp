//! Handler for the `run_script` MCP tool.

use std::sync::Arc;

use anyhow::Result;

use crate::scripting::engine::{CachedDataLoader, ScriptBacktestResult};
use crate::server::OptopsyServer;
use crate::tools::run_script::{RunScriptParams, RunScriptResponse};

/// Execute a Rhai backtest script.
pub async fn execute(server: &OptopsyServer, params: RunScriptParams) -> Result<RunScriptResponse> {
    let start = std::time::Instant::now();

    // 1. Resolve script source (inline or file)
    let source = crate::tools::run_script::resolve_script_source(&params)?;

    // 2. Create data loader backed by server's CachedStore
    let loader = CachedDataLoader {
        cache: Arc::clone(&server.cache),
    };

    // 3. Run the script backtest (params injected into scope as `params` map)
    let ScriptBacktestResult {
        result,
        metadata: _,
        indicator_data,
        ..
    } = crate::scripting::engine::run_script_backtest(&source, &params.params, &loader).await?;

    let elapsed = start.elapsed().as_millis() as u64;

    // 4. Format response
    let metrics_json = serde_json::to_value(&result.metrics)?;

    let summary = format!(
        "{} trades over the backtest period. Total P&L: ${:.2}. Sharpe: {:.2}.",
        result.trade_count, result.total_pnl, result.metrics.sharpe,
    );

    let mut findings = vec![];
    if result.metrics.sharpe > 1.0 {
        findings.push("Strategy shows positive risk-adjusted returns (Sharpe > 1.0)".to_string());
    }
    if result.metrics.max_drawdown > 0.20 {
        findings.push(format!(
            "Significant max drawdown of {:.1}%",
            result.metrics.max_drawdown * 100.0
        ));
    }
    if result.metrics.win_rate > 0.6 {
        findings.push(format!(
            "High win rate of {:.1}%",
            result.metrics.win_rate * 100.0
        ));
    }

    Ok(RunScriptResponse {
        trade_count: result.trade_count,
        total_pnl: result.total_pnl,
        metrics: metrics_json,
        equity_curve_length: result.equity_curve.len(),
        warnings: result.warnings,
        execution_time_ms: elapsed,
        indicator_data,
        summary,
        key_findings: findings,
        suggested_next_steps: vec![
            "Use monte_carlo to assess tail risk".to_string(),
            "Try different parameters and compare results".to_string(),
        ],
    })
}
