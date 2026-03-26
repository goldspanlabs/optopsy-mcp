//! Handler for the `run_script` MCP tool.

use std::sync::Arc;

use anyhow::Result;

use crate::scripting::engine::{CachedDataLoader, ScriptBacktestResult};
use crate::server::OptopsyServer;
use crate::tools::run_script::{format_indicator_data, RunScriptParams, RunScriptResponse};

/// Execute a Rhai backtest script.
pub async fn execute(server: &OptopsyServer, params: RunScriptParams) -> Result<RunScriptResponse> {
    let start = std::time::Instant::now();

    let source = crate::tools::run_script::resolve_script_source(&params)?;

    let loader = CachedDataLoader {
        cache: Arc::clone(&server.cache),
    };

    let ScriptBacktestResult {
        result,
        metadata: _,
        indicator_data,
        ..
    } = crate::scripting::engine::run_script_backtest(&source, &params.params, &loader).await?;

    // Convert raw indicator arrays to compact IndicatorData format
    let formatted_indicators = format_indicator_data(&indicator_data);

    Ok(RunScriptResponse {
        result,
        indicator_data: formatted_indicators,
        execution_time_ms: start.elapsed().as_millis() as u64,
    })
}
